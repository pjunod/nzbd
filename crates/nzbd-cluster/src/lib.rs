//! Cluster runtime (CLUSTERING.md): elected leader + workers over a shared
//! work volume. Every node runs the same subsystems — an engine in
//! worker mode, the election observer, the node registry, the worker loop
//! and the (dormant until elected) leader scheduler — plus an API layer
//! that answers locally when leader and proxies to the leader otherwise.
//!
//! Phase C1 distributes whole-job downloads; PP leases (C2) reuse the same
//! protocol when phase 2 lands.

pub mod election;
pub mod http;
pub mod layout;
pub mod leader;
pub mod proto;
pub mod proxy;
pub mod registry;
pub mod worker;

use axum::routing::get;
use axum::{middleware, Json, Router};
use election::{persist_guard, spawn_election, ElectionCfg, LeaderView};
use http::ClusterClient;
use layout::SharedLayout;
use leader::{spawn_leader_task, LeaderShared};
use nzbd_engine::{Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_types::ServerDef;
use proxy::{proxy_to_leader, ProxyState};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use worker::{spawn_worker, ActiveLeases};

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine: {0}")]
    Engine(#[from] nzbd_engine::EngineError),
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_name: String,
    pub shared_dir: PathBuf,
    /// How peers reach this node's API (scheme + host + port).
    pub advertise_url: String,
    pub secret: String,
    pub coordinator: bool,
    pub priority: u32,
    pub download: bool,
    pub max_download_jobs: u32,
    pub post_process: bool,
    /// Concurrent PP pipelines this node may run (C2).
    pub pp_slots: u32,
    pub lease_interval: Duration,
    pub takeover_after: Duration,
    pub worker_ttl: Duration,
}

/// Post-processing wiring for a cluster node (C2): the PP pipeline config
/// and the history store (SQLite local, JSONL on the shared volume).
#[derive(Clone)]
pub struct PpSetup {
    pub post: nzbd_post::manager::PostConfig,
    pub history: std::sync::Arc<nzbd_state::history::HistoryDb>,
}

pub struct ClusterRuntime {
    pub engine: EngineHandle,
    cfg: ClusterConfig,
    view: watch::Receiver<LeaderView>,
    leader_shared: Arc<LeaderShared>,
    client: ClusterClient,
    pp: Option<PpSetup>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl ClusterRuntime {
    /// Boot the node: engine in worker mode (empty queue; per-node fenced
    /// journals on the shared volume), election, registry, worker loop,
    /// leader task. Queue authority is adopted if/when this node wins.
    pub async fn start(
        cfg: ClusterConfig,
        servers: Vec<ServerDef>,
        tuning: Tuning,
        dest_dir: PathBuf,
        speed_limit_bps: Option<u64>,
        pp: Option<PpSetup>,
    ) -> Result<ClusterRuntime, ClusterError> {
        let layout = SharedLayout::new(&cfg.shared_dir, &cfg.node_name)?;
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        let view = spawn_election(
            layout.clone(),
            ElectionCfg {
                node: cfg.node_name.clone(),
                api_url: cfg.advertise_url.clone(),
                eligible: cfg.coordinator,
                priority: cfg.priority,
                lease_interval: cfg.lease_interval,
                takeover_after: cfg.takeover_after,
            },
            cancel.clone(),
            &tracker,
        );

        let guard = persist_guard(layout.clone(), view.clone(), cfg.node_name.clone());
        let engine = Engine::spawn(EngineConfig {
            servers: servers.clone(),
            state_dir: layout.state_dir(),
            dest_dir: dest_dir.clone(),
            tuning,
            speed_limit_bps,
            persist_queue: false, // adopted on taking office
            journal_suffix: cfg.node_name.clone(),
            persist_guard: Some(guard),
        })
        .await?;

        // A non-downloading node must never open provider connections —
        // even for the moment a queued job sits local before the scheduler
        // delegates it. Zero budgets park every connection task.
        if !cfg.download {
            let zero: std::collections::HashMap<_, _> =
                servers.iter().map(|s| (s.id, 0u16)).collect();
            let _ = engine.set_server_budgets(zero).await;
        }

        let client = ClusterClient::new(cfg.secret.clone());
        let leader_shared = LeaderShared::new(
            engine.clone(),
            layout.clone(),
            cfg.clone(),
            servers.clone(),
            view.clone(),
        );
        spawn_leader_task(leader_shared.clone(), cancel.clone(), &tracker);
        registry::spawn_registry(
            layout.clone(),
            cfg.clone(),
            engine.clone(),
            cancel.clone(),
            &tracker,
        );

        let active: ActiveLeases = Default::default();
        spawn_worker(
            cfg.clone(),
            servers.clone(),
            engine.clone(),
            view.clone(),
            client.clone(),
            active.clone(),
            pp.clone(),
            dest_dir.clone(),
            cancel.clone(),
            &tracker,
        );
        let history = pp.as_ref().map(|s| s.history.clone());
        let _ = &history; // (kept alongside pp in the runtime below)

        // Leader-local PP manager (C2): processes only jobs the scheduler
        // assigned to THIS node, and only while it holds authority. Health-
        // failed jobs (no PP) are history-recorded by the leader.
        if let Some(setup) = &pp {
            if cfg.post_process && cfg.pp_slots > 0 {
                let gate: nzbd_post::manager::PpGate = Some(std::sync::Arc::new({
                    let view = view.clone();
                    let engine = engine.clone();
                    let me = cfg.node_name.clone();
                    move |job_id: nzbd_types::JobId| {
                        if !view.borrow().is_me {
                            return false;
                        }
                        let snap = engine.snapshot();
                        match snap.jobs.iter().find(|j| j.id == job_id) {
                            Some(j) if matches!(j.status, nzbd_types::JobStatus::Failed) => true,
                            Some(j) => j.assigned_node.as_deref() == Some(me.as_str()),
                            None => false,
                        }
                    }
                }));
                let mut post = setup.post.clone();
                post.slots = cfg.pp_slots.max(1) as usize;
                nzbd_post::manager::spawn_post_manager(
                    engine.clone(),
                    post,
                    setup.history.clone(),
                    dest_dir.clone(),
                    gate,
                    cancel.clone(),
                    &tracker,
                );
            }
        }

        // Crash-only demotion: on losing leadership, keep only the jobs we
        // still execute as a worker; drop authority state.
        {
            let engine = engine.clone();
            let mut view_rx = view.clone();
            let active = active.clone();
            let demote_cancel = cancel.clone();
            tracker.spawn(async move {
                let mut was_me = view_rx.borrow().is_me;
                loop {
                    tokio::select! {
                        _ = demote_cancel.cancelled() => break,
                        changed = view_rx.changed() => {
                            if changed.is_err() { break }
                        }
                    }
                    let is_me = view_rx.borrow().is_me;
                    if was_me && !is_me {
                        let keep: Vec<_> =
                            active.lock().unwrap().values().map(|st| st.job).collect();
                        tracing::warn!(kept = keep.len(), "demoted: dropping authority state");
                        let _ = engine.retain_jobs(keep).await;
                    }
                    was_me = is_me;
                }
            });
        }
        tracker.close();

        Ok(ClusterRuntime {
            engine,
            cfg,
            view,
            leader_shared,
            client,
            pp,
            cancel,
            tracker,
        })
    }

    pub fn is_leader(&self) -> bool {
        self.view.borrow().is_me
    }

    pub fn leader_view(&self) -> LeaderView {
        self.view.borrow().clone()
    }

    /// A cheap "am I the leader right now?" probe for gating work (e.g. the
    /// post-processing manager runs only on the queue authority until C2).
    pub fn leader_gate(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let view = self.view.clone();
        move || view.borrow().is_me
    }

    /// Where the authoritative history JSONL lives on the shared volume.
    pub fn history_dir(&self) -> PathBuf {
        SharedLayout::new(&self.cfg.shared_dir, &self.cfg.node_name)
            .expect("layout exists")
            .history_dir()
    }

    /// The full node router: cluster endpoints (answered locally) + the
    /// native API and compat shim (proxied to the leader from non-leaders).
    pub fn router(&self, compat_version: &str, options: Vec<(String, String)>) -> Router {
        self.router_with_auth(compat_version, options, Default::default())
    }

    /// [`ClusterRuntime::router`] with HTTP auth on the API + compat
    /// surface. Cluster peer endpoints keep their own shared-secret auth
    /// and are never behind user credentials.
    pub fn router_with_auth(
        &self,
        compat_version: &str,
        options: Vec<(String, String)>,
        auth: nzbd_api::AuthConfig,
    ) -> Router {
        self.router_full(compat_version, options, auth, None, None)
    }

    /// Full router: auth + daemon log ring + watch-dir scan notify.
    pub fn router_full(
        &self,
        compat_version: &str,
        options: Vec<(String, String)>,
        auth: nzbd_api::AuthConfig,
        log: Option<Arc<nzbd_api::LogBuffer>>,
        scan_notify: Option<Arc<tokio::sync::Notify>>,
    ) -> Router {
        let history = self.pp.as_ref().map(|s| s.history.clone());
        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: compat_version.to_string(),
            }),
            engine: self.engine.clone(),
            history: history.clone(),
            options: Arc::new(options),
            log: log.clone(),
            scan_notify,
        };
        let proxied = nzbd_api::require_auth(
            nzbd_api::router_with(nzbd_api::ApiState {
                engine: self.engine.clone(),
                history,
                log,
            })
            .merge(nzbd_compat::router(compat_state)),
            auth,
        )
        .layer(middleware::from_fn_with_state(
            ProxyState {
                node: self.cfg.node_name.clone(),
                view: self.view.clone(),
                client: self.client.clone(),
            },
            proxy_to_leader,
        ));

        let info = ClusterInfoState {
            node: self.cfg.node_name.clone(),
            layout: SharedLayout::new(&self.cfg.shared_dir, &self.cfg.node_name)
                .expect("layout exists"),
            view: self.view.clone(),
        };
        Router::new()
            .merge(leader::router(self.leader_shared.clone()))
            .route("/api/v1/cluster", get(cluster_info).with_state(info))
            .merge(proxied)
    }

    /// Stop cluster tasks, then flush the engine.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        self.tracker.wait().await;
        self.engine.shutdown().await;
    }
}

#[derive(Clone)]
struct ClusterInfoState {
    node: String,
    layout: SharedLayout,
    view: watch::Receiver<LeaderView>,
}

/// Local (unproxied) cluster diagnostics: this node's view of leadership
/// and membership.
async fn cluster_info(
    axum::extract::State(s): axum::extract::State<ClusterInfoState>,
) -> Json<serde_json::Value> {
    let v = s.view.borrow().clone();
    let nodes = registry::read_nodes(&s.layout);
    Json(serde_json::json!({
        "self": s.node,
        "is_leader": v.is_me,
        "epoch": v.epoch(),
        "leader": v.record.as_ref().map(|r| serde_json::json!({
            "node": r.node,
            "api_url": r.api_url,
        })),
        "nodes": nodes,
    }))
}
