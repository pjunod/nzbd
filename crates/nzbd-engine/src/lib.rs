//! The download engine (ARCHITECTURE.md §8).
//!
//! Architecture: **single-owner queue task + message passing**. One task
//! owns all queue state (jobs/files/segments/leases) and is the only place
//! it mutates; connection tasks (one per NNTP connection, pull model) ask it
//! for leased segments, decode article bodies incrementally and hand the
//! bytes to per-file writer tasks; readers get lock-free `arc-swap`
//! snapshots. Crash safety: an append-only segment journal + debounced
//! atomic queue snapshots (`nzbd-state`) — kill -9 loses at most the
//! last un-fsynced second.
//!
//! Public surface: [`Engine::spawn`] → [`EngineHandle`].

pub mod events;
pub mod failover;
pub mod queue;
pub mod rate;
pub mod snapshot;

mod owner;
mod pool;
mod writer;

pub use events::Event;
pub use snapshot::{new_shared_snapshot, JobSummary, QueueSnapshot, SharedSnapshot};

use nzbd_nntp::transport::{tls_client_config, TlsClientConfig};
use nzbd_types::{JobId, ServerDef, TlsMode};
use owner::{EngineMsg, Owner, QueueCommand};
use rate::{RateLimiter, SpeedMeter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("invalid NZB: {0}")]
    Nzb(#[from] nzbd_nzb::NzbError),
    #[error("state: {0}")]
    State(#[from] nzbd_state::StateError),
    #[error("tls: {0}")]
    Tls(String),
    #[error("engine is shutting down")]
    Closed,
}

/// Behavioral knobs, defaulting to NZBGet's values (ARCHITECTURE.md §3.3).
#[derive(Debug, Clone)]
pub struct Tuning {
    pub article_retries: u8,
    pub retry_interval: Duration,
    pub article_timeout: Duration,
    pub connect_timeout: Duration,
    /// How long an idle connection is kept before being closed.
    pub idle_hold: Duration,
    pub propagation_delay: Duration,
    /// Queue `*.volNNN+MM.par2` files paused (delayed-par download).
    pub pause_extra_pars: bool,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            article_retries: 3,
            retry_interval: Duration::from_secs(10),
            article_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(30),
            idle_hold: Duration::from_secs(5),
            propagation_delay: Duration::ZERO,
            pause_extra_pars: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub servers: Vec<ServerDef>,
    /// Journal + snapshots live here.
    pub state_dir: PathBuf,
    /// Completed jobs are written to `<dest_dir>/<job name>/`.
    pub dest_dir: PathBuf,
    pub tuning: Tuning,
    pub speed_limit_bps: Option<u64>,
}

pub struct Engine;

impl Engine {
    /// Recover state, spawn the owner task and one connection task per
    /// configured connection, and return the handle.
    pub async fn spawn(cfg: EngineConfig) -> Result<EngineHandle, EngineError> {
        let shared = new_shared_snapshot();
        let (events, _) = broadcast::channel(512);
        let (epoch_tx, epoch_rx) = watch::channel(0u64);
        let (engine_tx, engine_rx) = mpsc::channel::<EngineMsg>(1024);
        let meter = Arc::new(SpeedMeter::new());
        let limiter = Arc::new(RateLimiter::new(cfg.speed_limit_bps));
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let servers = Arc::new(cfg.servers.clone());

        // TLS configs once per server.
        let mut tls_by_server: Vec<Option<TlsClientConfig>> = Vec::new();
        for s in servers.iter() {
            tls_by_server.push(match s.tls {
                TlsMode::Tls => Some(
                    tls_client_config(s.cert_verification)
                        .map_err(|e| EngineError::Tls(e.to_string()))?,
                ),
                TlsMode::None => None,
            });
        }

        let owner = Owner::recover(
            &cfg.state_dir,
            cfg.dest_dir.clone(),
            servers.clone(),
            cfg.tuning.clone(),
            shared.clone(),
            events.clone(),
            epoch_tx,
            meter.clone(),
            limiter.clone(),
            engine_tx.clone(),
            tracker.clone(),
            cancel.clone(),
        )?;
        tracker.spawn(owner.run(engine_rx));

        // Connection tasks: `max_connections` per active server. Tasks are
        // cheap when parked; sockets only exist while there is work.
        for (i, server) in servers.iter().enumerate() {
            if !server.active {
                continue;
            }
            for _ in 0..server.max_connections.max(1) {
                tracker.spawn(pool::connection_task(pool::ConnCtx {
                    server: server.clone(),
                    tls: tls_by_server[i].clone(),
                    engine_tx: engine_tx.clone(),
                    epoch: epoch_rx.clone(),
                    limiter: limiter.clone(),
                    meter: meter.clone(),
                    cancel: cancel.clone(),
                    connect_timeout: cfg.tuning.connect_timeout,
                    read_timeout: cfg.tuning.article_timeout,
                    idle_hold: cfg.tuning.idle_hold,
                }));
            }
        }
        tracker.close();

        Ok(EngineHandle {
            cmd_tx: engine_tx,
            shared,
            events,
            cancel,
            tracker,
        })
    }
}

/// Cloneable handle to a running engine.
#[derive(Clone)]
pub struct EngineHandle {
    cmd_tx: mpsc::Sender<EngineMsg>,
    shared: SharedSnapshot,
    events: broadcast::Sender<Event>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl EngineHandle {
    /// Parse and enqueue an NZB. Parsing happens on the caller's task so a
    /// large or hostile NZB never stalls the queue owner.
    pub async fn add_nzb(
        &self,
        name: &str,
        content: &[u8],
        category: Option<String>,
        priority: i32,
    ) -> Result<JobId, EngineError> {
        let parsed = nzbd_nzb::parse(content)?;
        let name = name
            .trim()
            .trim_end_matches(".nzb")
            .trim_end_matches(".NZB");
        let name = if name.is_empty() {
            parsed
                .meta
                .title
                .clone()
                .unwrap_or_else(|| "download".to_string())
        } else {
            name.to_string()
        };
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AddParsed {
            name,
            parsed: Box::new(parsed),
            category,
            priority,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    pub async fn pause_job(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Pause { job, reply })
            .await
    }

    pub async fn resume_job(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Resume { job, reply })
            .await
    }

    pub async fn delete_job(&self, job: JobId, delete_files: bool) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Delete {
            job,
            delete_files,
            reply,
        })
        .await
    }

    pub async fn set_priority(&self, job: JobId, priority: i32) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetPriority {
            job,
            priority,
            reply,
        })
        .await
    }

    pub async fn pause_all(&self) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::PauseAll { reply })
            .await
    }

    pub async fn resume_all(&self) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::ResumeAll { reply })
            .await
    }

    pub async fn set_speed_limit(&self, bytes_per_sec: Option<u64>) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::SetSpeedLimit {
            bytes_per_sec,
            reply,
        })
        .await
    }

    /// Lock-free snapshot of the queue (never blocks the engine).
    pub fn snapshot(&self) -> Arc<QueueSnapshot> {
        self.shared.load_full()
    }

    pub fn shared_snapshot(&self) -> SharedSnapshot {
        self.shared.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Graceful shutdown: stop leasing, flush journal + snapshot, clear the
    /// unclean marker, wait for every task.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        self.tracker.wait().await;
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), EngineError> {
        self.cmd_tx
            .send(EngineMsg::Command(cmd))
            .await
            .map_err(|_| EngineError::Closed)
    }

    async fn roundtrip_bool(
        &self,
        make: impl FnOnce(oneshot::Sender<bool>) -> QueueCommand,
    ) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    async fn roundtrip_unit(
        &self,
        make: impl FnOnce(oneshot::Sender<()>) -> QueueCommand,
    ) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        rx.await.map_err(|_| EngineError::Closed)
    }
}
