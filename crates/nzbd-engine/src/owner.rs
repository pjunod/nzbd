//! The queue-owner task (ARCHITECTURE.md §8.1): the single serialization
//! point for all queue mutation. Inputs: a bounded command/event channel and
//! a 1 Hz tick. Outputs: leases granted to connection tasks (pull model),
//! `arc-swap` snapshots for lock-free readers, broadcast events, journal
//! appends and debounced snapshot saves.
//!
//! Every handler is synchronous — the owner never awaits while reasoning
//! about state. Sends toward writer tasks use `try_send` with a
//! retry-on-tick fallback so owner ⇄ writer backpressure can never deadlock.

use crate::events::Event;
use crate::failover::{AttemptOutcome, Ladder, SegmentAttempt, Verdict};
use crate::queue::{
    final_status, next_for_server, pick_par_files, recompute_job_totals, sanitize_name,
    vol_par_blocks, QueueState, SegRef, SelectionCtx,
};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::snapshot::{JobSummary, QueueSnapshot, SharedSnapshot};
use crate::writer::{spawn_writer, WriteCmd, WriterHandle};
use crate::Tuning;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{FsJournal, JobJournals, JournalRecord, SnapshotStore, UncleanMarker};
use nzbd_types::{FileId, Health, Job, JobId, JobStatus, SegmentState, ServerDef, ServerId};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// External commands (wrapped by `EngineHandle` methods).
#[derive(Debug)]
pub(crate) enum QueueCommand {
    AddParsed {
        name: String,
        parsed: Box<ParsedNzb>,
        category: Option<String>,
        priority: i32,
        dupe: Option<nzbd_types::DupeInfo>,
        paused: bool,
        reply: oneshot::Sender<JobId>,
    },
    AddUrl {
        name: String,
        url: String,
        category: Option<String>,
        priority: i32,
        dupe: Option<nzbd_types::DupeInfo>,
        paused: bool,
        reply: oneshot::Sender<JobId>,
    },
    CompleteUrlFetch {
        job: JobId,
        parsed: Box<ParsedNzb>,
        reply: oneshot::Sender<bool>,
    },
    FailUrlFetch {
        job: JobId,
        error: String,
        reply: oneshot::Sender<()>,
    },
    SetFilePaused {
        job: JobId,
        file: FileId,
        paused: bool,
        reply: oneshot::Sender<bool>,
    },
    DeleteFile {
        job: JobId,
        file: FileId,
        reply: oneshot::Sender<bool>,
    },
    Pause {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    Resume {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    Delete {
        job: JobId,
        delete_files: bool,
        reply: oneshot::Sender<bool>,
    },
    SetPriority {
        job: JobId,
        priority: i32,
        reply: oneshot::Sender<bool>,
    },
    /// Reorder within the queue vec — the scheduling tiebreaker inside a
    /// priority band, and the order the UI displays.
    Move {
        job: JobId,
        op: MoveOp,
        reply: oneshot::Sender<bool>,
    },
    PauseAll {
        reply: oneshot::Sender<()>,
    },
    ResumeAll {
        reply: oneshot::Sender<()>,
    },
    SetSpeedLimit {
        bytes_per_sec: Option<u64>,
        reply: oneshot::Sender<()>,
    },

    // -- cluster commands (CLUSTERING.md §7) --------------------------------
    /// Insert (or replace) a job with its ids preserved — the cluster grant
    /// / completion path. Optionally folds the job's shared journals
    /// (cross-node resume).
    ImportJob {
        job: Box<Job>,
        fold_journals: bool,
        emit_finished: bool,
        reply: oneshot::Sender<()>,
    },
    /// Clone a job's full current state out (for grants and completion
    /// reports).
    ExportJob {
        job: JobId,
        reply: oneshot::Sender<Option<Box<Job>>>,
    },
    /// Remove a job from this engine without touching disk artifacts,
    /// history or events — the executor-side handoff cleanup.
    RemoveJobSilent {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    /// Mark a job as executing on another node: the local scheduler skips
    /// it; summaries carry the assignee.
    SetDelegated {
        job: JobId,
        node: Option<String>,
        reply: oneshot::Sender<bool>,
    },
    /// Overlay remote progress counters onto a delegated job's summary.
    MirrorProgress {
        job: JobId,
        stats: MirrorStats,
    },
    /// Union-fold the job's shared journal files into local state (reclaim
    /// after a worker died, or adoption after taking office).
    FoldJobJournals {
        job: JobId,
        reply: oneshot::Sender<()>,
    },
    /// Cap connection concurrency per server (cluster-wide provider
    /// account budgets). Absent entry = local config limit.
    SetServerBudgets {
        budgets: HashMap<ServerId, u16>,
        reply: oneshot::Sender<()>,
    },
    /// Become the queue authority: load the shared snapshot (local jobs
    /// win on conflict — the executor copy is fresher), fold all journals,
    /// enable persistence.
    AdoptAuthority {
        reply: oneshot::Sender<()>,
    },
    /// Crash-only demotion: drop authority persistence and every job not
    /// in `keep` (the leases this node still executes).
    RetainJobs {
        keep: Vec<JobId>,
        reply: oneshot::Sender<()>,
    },

    // -- post-processing hooks (phase 2) ------------------------------------
    /// Post-processing state transitions (PostQueued / Post{stage} /
    /// terminal). Only meaningful on jobs whose download already finished.
    SetJobStatus {
        job: JobId,
        status: JobStatus,
        reply: oneshot::Sender<bool>,
    },
    /// Delayed-par download (§3.2): unpause the smallest set of paused
    /// `*.volXX+NN.par2` files covering `blocks`. Replies with the number
    /// of recovery blocks now downloading (0 = nothing left to unpause).
    UnpauseParBlocks {
        job: JobId,
        blocks: u32,
        reply: oneshot::Sender<u32>,
    },
}

/// Queue reorder operations (NZBGet GroupMoveTop/Up/Down/Bottom).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOp {
    Top,
    Up,
    Down,
    Bottom,
}

/// Sliding per-job rate from downloaded-byte deltas between snapshots.
struct JobRateMeter {
    last_bytes: u64,
    last_at: std::time::Instant,
    ema_bps: f64,
}

impl JobRateMeter {
    /// EMA over ~5s; ignores sub-250ms deltas (snapshot bursts).
    fn update(&mut self, bytes_now: u64) -> u64 {
        let now = std::time::Instant::now();
        let dt = now.duration_since(self.last_at).as_secs_f64();
        if dt >= 0.25 {
            let delta = bytes_now.saturating_sub(self.last_bytes) as f64;
            let inst = delta / dt;
            let alpha = (dt / 5.0).min(1.0);
            self.ema_bps += alpha * (inst - self.ema_bps);
            self.last_bytes = bytes_now;
            self.last_at = now;
        }
        self.ema_bps.max(0.0) as u64
    }
}

/// Remote progress counters mirrored into a delegated job's summary.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct MirrorStats {
    pub done_articles: u32,
    pub failed_articles: u32,
    pub downloaded_bytes: u64,
    pub health: u16,
}

/// A granted segment lease: everything a connection task needs.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    pub r: SegRef,
    pub message_id: String,
    pub writer: mpsc::Sender<WriteCmd>,
}

/// Everything that reaches the owner task.
#[derive(Debug)]
pub(crate) enum EngineMsg {
    Command(QueueCommand),
    WorkRequest {
        server: ServerId,
        max: usize,
        reply: oneshot::Sender<Vec<Lease>>,
    },
    SegmentWritten {
        job: JobId,
        file: FileId,
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
        server: ServerId,
    },
    SegmentFailed {
        job: JobId,
        file: FileId,
        seg_number: u32,
        server: ServerId,
        outcome: AttemptOutcome,
    },
    ConnectFailed {
        server: ServerId,
    },
    WriterFinalized {
        job: JobId,
        file: FileId,
        ok: bool,
        final_path: Option<PathBuf>,
        combined_crc: Option<u32>,
    },
    WriterError {
        job: JobId,
        file: FileId,
        error: String,
    },
}

// ---------------------------------------------------------------------------
// Owner
// ---------------------------------------------------------------------------

pub(crate) struct Owner {
    state: QueueState,
    attempts: HashMap<SegRef, SegmentAttempt>,
    blocked: HashMap<ServerId, Instant>,
    writers: HashMap<FileId, WriterHandle>,
    finalize_sent: HashSet<FileId>,
    pending_finalize: Vec<(JobId, FileId)>,
    file_sizes: HashMap<FileId, u64>,
    /// Jobs executing on another node (job → node name).
    delegated: HashMap<JobId, String>,
    mirror: HashMap<JobId, MirrorStats>,
    /// Per-job download-rate EMA, fed from downloaded-byte deltas at
    /// snapshot time (job id → meter).
    job_rates: HashMap<u32, JobRateMeter>,

    state_dir: PathBuf,
    journal: JobJournals,
    snap_store: SnapshotStore,
    marker: UncleanMarker,
    /// Queue-authority persistence (snapshot save/compact). Worker-mode
    /// engines run with this off; journals stay on regardless.
    persist: bool,
    persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    budget_tx: watch::Sender<HashMap<ServerId, u16>>,

    shared: SharedSnapshot,
    events: broadcast::Sender<Event>,
    epoch_tx: watch::Sender<u64>,
    meter: Arc<SpeedMeter>,
    limiter: Arc<RateLimiter>,

    servers: Arc<Vec<ServerDef>>,
    tuning: Tuning,
    dest_dir: PathBuf,

    engine_tx: mpsc::Sender<EngineMsg>,
    tracker: TaskTracker,
    cancel: CancellationToken,

    up_since_unix: i64,
    dirty: bool,
    last_save: Instant,

    /// Per-server volume counters + quota/disk guards.
    volumes: crate::volumes::VolumeBook,
    quota_reached: bool,
    disk_low: bool,
    guard_tick: u32,
}

#[allow(clippy::too_many_arguments)]
impl Owner {
    /// Synchronous construction incl. crash recovery. Authority mode
    /// (`persist = true`): load snapshot, union-replay every per-job
    /// journal (plus a legacy phase-1 global journal, once), fold. Worker
    /// mode: start empty — jobs arrive as leases and fold their own
    /// journals on import.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover(
        state_dir: &Path,
        dest_dir: PathBuf,
        servers: Arc<Vec<ServerDef>>,
        tuning: Tuning,
        persist: bool,
        journal_suffix: &str,
        persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        budget_tx: watch::Sender<HashMap<ServerId, u16>>,
        shared: SharedSnapshot,
        events: broadcast::Sender<Event>,
        epoch_tx: watch::Sender<u64>,
        meter: Arc<SpeedMeter>,
        limiter: Arc<RateLimiter>,
        config_speed_limit: Option<u64>,
        engine_tx: mpsc::Sender<EngineMsg>,
        tracker: TaskTracker,
        cancel: CancellationToken,
    ) -> Result<Owner, nzbd_state::StateError> {
        let marker = UncleanMarker::new(state_dir, journal_suffix);
        let was_unclean = marker.check_and_arm()?;
        let snap_store = SnapshotStore::open(state_dir)?;
        let journal = JobJournals::open(state_dir, journal_suffix)?;

        let mut state = QueueState::default();
        let mut file_sizes = HashMap::new();
        let mut replay_count = 0usize;

        if persist {
            if let Some(doc) = snap_store.load()? {
                state = QueueState::from_doc(doc);
            }

            // Legacy phase-1 global journal: fold once, then retire it.
            let legacy = FsJournal::open(state_dir)?;
            let mut replayed = legacy.replay()?;
            drop(legacy);
            if !replayed.is_empty() {
                tracing::info!(records = replayed.len(), "migrating legacy global journal");
                let _ = std::fs::remove_file(state_dir.join("segments.journal"));
            }
            replayed.extend(JobJournals::replay_all(state_dir)?);
            replay_count = replayed.len();
            for rec in replayed {
                let r = SegRef {
                    job: rec.job,
                    file: rec.file,
                    seg_number: rec.segment_number,
                };
                if rec.file_size > 0 {
                    file_sizes.insert(rec.file, rec.file_size);
                }
                if let Some(seg) = state.segment_mut(r) {
                    if !matches!(seg.state, SegmentState::Done { .. }) {
                        seg.state = SegmentState::Done {
                            offset: rec.offset,
                            len: rec.len,
                            crc: rec.crc32,
                        };
                    }
                }
            }
            state.recompute_all_totals();
        }

        if was_unclean || replay_count > 0 {
            tracing::info!(
                was_unclean,
                journal_records = replay_count,
                jobs = state.jobs.len(),
                "recovered queue state"
            );
        }

        // Speed limit: the config wins whenever it sets one (a config
        // edit must take effect on reload); the runtime-set persisted
        // value applies only when the config is silent.
        if config_speed_limit.is_some() {
            state.speed_limit_bps = config_speed_limit;
        }
        limiter.set(state.speed_limit_bps);

        Ok(Owner {
            state,
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            finalize_sent: HashSet::new(),
            pending_finalize: Vec::new(),
            file_sizes,
            delegated: HashMap::new(),
            mirror: HashMap::new(),
            job_rates: HashMap::new(),
            state_dir: state_dir.to_path_buf(),
            journal,
            snap_store,
            marker,
            persist,
            persist_guard,
            budget_tx,
            shared,
            events,
            epoch_tx,
            meter,
            limiter,
            servers,
            tuning,
            dest_dir,
            engine_tx,
            tracker,
            cancel,
            up_since_unix: unix_now(),
            dirty: false,
            last_save: Instant::now(),
            volumes: crate::volumes::VolumeBook::load(state_dir, journal_suffix),
            quota_reached: false,
            disk_low: false,
            guard_tick: 0,
        })
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<EngineMsg>) {
        // Fold replayed journal into a fresh snapshot, then finish anything
        // that completed just before the crash.
        self.save_snapshot();
        self.startup_pass();
        self.publish_snapshot(0);
        self.bump_epoch();

        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tick.tick() => self.on_tick(),
                msg = rx.recv() => match msg {
                    Some(m) => self.on_msg(m),
                    None => break,
                },
            }
        }

        // Graceful shutdown = fast crash with a flush (§4.6).
        if let Err(e) = self.journal.sync() {
            tracing::warn!(error = %e, "journal sync at shutdown failed");
        }
        self.volumes.save_if_dirty();
        self.save_snapshot();
        if let Err(e) = self.marker.disarm() {
            tracing::warn!(error = %e, "could not clear unclean marker");
        }
        self.writers.clear(); // writers drain and exit
        tracing::info!("queue owner stopped");
    }

    fn startup_pass(&mut self) {
        let mut to_finalize = Vec::new();
        let mut to_check = Vec::new();
        for job in &self.state.jobs {
            if !matches!(job.status, JobStatus::Queued | JobStatus::Downloading) {
                continue;
            }
            for file in &job.files {
                if file.is_terminal() && file.has_any_done() && !file.finalized {
                    to_finalize.push((job.id, file.id));
                }
            }
            to_check.push(job.id);
        }
        for (j, f) in to_finalize {
            self.send_finalize(j, f);
        }
        for j in to_check {
            self.check_job_complete(j);
        }
    }

    // -- message dispatch ----------------------------------------------------

    fn on_msg(&mut self, msg: EngineMsg) {
        match msg {
            EngineMsg::Command(cmd) => self.on_command(cmd),
            EngineMsg::WorkRequest { server, max, reply } => {
                let leases = self.grant_work(server, max.max(1));
                let _ = reply.send(leases);
            }
            EngineMsg::SegmentWritten {
                job,
                file,
                seg_number,
                offset,
                len,
                crc,
                file_size,
                server,
            } => {
                tracing::trace!(
                    job = job.0,
                    file = file.0,
                    seg = seg_number,
                    server = server.0,
                    len,
                    "segment written"
                );
                self.on_segment_written(job, file, seg_number, offset, len, crc, file_size, server)
            }
            EngineMsg::SegmentFailed {
                job,
                file,
                seg_number,
                server,
                outcome,
            } => self.on_segment_failed(
                SegRef {
                    job,
                    file,
                    seg_number,
                },
                server,
                outcome,
            ),
            EngineMsg::ConnectFailed { server } => self.block_server(server),
            EngineMsg::WriterFinalized {
                job,
                file,
                ok,
                final_path,
                combined_crc,
            } => self.on_writer_finalized(job, file, ok, final_path, combined_crc),
            EngineMsg::WriterError { job, file, error } => {
                tracing::warn!(job = job.0, file = file.0, %error, "writer error; failing file");
                self.fail_whole_file(job, file);
            }
        }
    }

    fn on_command(&mut self, cmd: QueueCommand) {
        match cmd {
            QueueCommand::AddParsed {
                name,
                parsed,
                category,
                priority,
                dupe,
                paused,
                reply,
            } => {
                let id = self.state.admit_nzb(
                    name.clone(),
                    &parsed,
                    category,
                    priority,
                    self.tuning.pause_extra_pars,
                );
                if let Some(j) = self.state.job_mut(id) {
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.status = JobStatus::Paused;
                    }
                }
                tracing::info!(job = id.0, %name, "job added");
                self.save_snapshot(); // adds are durable immediately
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send(id);
            }
            QueueCommand::AddUrl {
                name,
                url,
                category,
                priority,
                dupe,
                paused,
                reply,
            } => {
                let id = self.state.admit_url(name.clone(), &url, category, priority);
                if let Some(j) = self.state.job_mut(id) {
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.params.push(("*AddPaused".into(), "yes".into()));
                    }
                }
                tracing::info!(job = id.0, %name, %url, "url job added (fetching)");
                self.save_snapshot();
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send(id);
            }
            QueueCommand::CompleteUrlFetch { job, parsed, reply } => {
                let ok = self
                    .state
                    .complete_url_fetch(job, &parsed, self.tuning.pause_extra_pars);
                if ok {
                    if let Some(j) = self.state.job_mut(job) {
                        if let Some(pos) = j.params.iter().position(|(k, _)| k == "*AddPaused") {
                            j.params.remove(pos);
                            j.status = JobStatus::Paused;
                        }
                    }
                    tracing::info!(job = job.0, "url fetch complete; queued");
                    self.save_snapshot();
                    self.publish_now();
                    self.bump_epoch();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::FailUrlFetch { job, error, reply } => {
                let name = match self.state.job_mut(job) {
                    Some(j) if matches!(j.status, JobStatus::Fetching) => {
                        j.status = JobStatus::Failed;
                        Some(j.name.clone())
                    }
                    _ => None,
                };
                if let Some(name) = name {
                    tracing::warn!(job = job.0, %error, "url fetch failed");
                    self.save_snapshot();
                    self.publish_now();
                    self.emit(Event::JobFinished {
                        job,
                        name,
                        status: JobStatus::Failed,
                        health: 0,
                    });
                    self.bump_epoch();
                }
                let _ = reply.send(());
            }
            QueueCommand::SetFilePaused {
                job,
                file,
                paused,
                reply,
            } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => match j.files.iter_mut().find(|f| f.id == file) {
                        Some(f) => {
                            f.paused = paused;
                            true
                        }
                        None => false,
                    },
                    None => false,
                };
                if ok {
                    if let Some(j) = self.state.job_mut(job) {
                        recompute_job_totals(j);
                    }
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                    self.check_job_complete(job);
                }
                let _ = reply.send(ok);
            }
            QueueCommand::DeleteFile { job, file, reply } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        let before = j.files.len();
                        j.files.retain(|f| f.id != file);
                        if j.files.len() != before {
                            recompute_job_totals(j);
                            true
                        } else {
                            false
                        }
                    }
                    None => false,
                };
                if ok {
                    self.writers.remove(&file);
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                    self.check_job_complete(job);
                }
                let _ = reply.send(ok);
            }
            QueueCommand::Pause { job, reply } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) if matches!(j.status, JobStatus::Queued | JobStatus::Downloading) => {
                        j.status = JobStatus::Paused;
                        true
                    }
                    _ => false,
                };
                if ok {
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::Resume { job, reply } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) if matches!(j.status, JobStatus::Paused) => {
                        j.status = JobStatus::Queued;
                        true
                    }
                    _ => false,
                };
                if ok {
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::Delete {
                job,
                delete_files,
                reply,
            } => {
                let ok = self.delete_job(job, delete_files);
                let _ = reply.send(ok);
            }
            QueueCommand::SetPriority {
                job,
                priority,
                reply,
            } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        j.priority = priority;
                        true
                    }
                    None => false,
                };
                if ok {
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::Move { job, op, reply } => {
                let ok = self.move_job(job, op);
                let _ = reply.send(ok);
            }
            QueueCommand::PauseAll { reply } => {
                self.state.download_paused = true;
                self.dirty = true;
                self.emit(Event::QueuePauseChanged { paused: true });
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::ResumeAll { reply } => {
                self.state.download_paused = false;
                self.dirty = true;
                self.emit(Event::QueuePauseChanged { paused: false });
                self.bump_epoch();
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::SetSpeedLimit {
                bytes_per_sec,
                reply,
            } => {
                self.state.speed_limit_bps = bytes_per_sec;
                self.limiter.set(bytes_per_sec);
                self.dirty = true;
                self.emit(Event::SpeedLimitChanged { bytes_per_sec });
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::ImportJob {
                job,
                fold_journals,
                emit_finished,
                reply,
            } => {
                self.import_job(*job, fold_journals, emit_finished);
                let _ = reply.send(());
            }
            QueueCommand::ExportJob { job, reply } => {
                let _ = reply.send(self.state.job(job).cloned().map(Box::new));
            }
            QueueCommand::RemoveJobSilent { job, reply } => {
                let _ = reply.send(self.remove_job_silent(job));
            }
            QueueCommand::SetDelegated { job, node, reply } => {
                let ok = self.state.job(job).is_some();
                if ok {
                    match &node {
                        Some(n) => {
                            self.delegated.insert(job, n.clone());
                        }
                        None => {
                            self.delegated.remove(&job);
                            self.mirror.remove(&job);
                        }
                    }
                    self.emit(Event::JobAssigned {
                        job,
                        node: node.clone(),
                    });
                    self.bump_epoch();
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::MirrorProgress { job, stats } => {
                if self.delegated.contains_key(&job) {
                    self.mirror.insert(job, stats);
                    self.publish_now();
                }
            }
            QueueCommand::FoldJobJournals { job, reply } => {
                self.fold_job_journals(job);
                let _ = reply.send(());
            }
            QueueCommand::SetServerBudgets { budgets, reply } => {
                tracing::info!(?budgets, "connection budgets updated");
                let _ = self.budget_tx.send(budgets);
                self.bump_epoch();
                let _ = reply.send(());
            }
            QueueCommand::AdoptAuthority { reply } => {
                self.adopt_authority();
                let _ = reply.send(());
            }
            QueueCommand::SetJobStatus { job, status, reply } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        j.status = status;
                        true
                    }
                    None => false,
                };
                if ok {
                    self.dirty = true;
                    if self.persist
                        && matches!(
                            status,
                            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                        )
                    {
                        self.save_snapshot();
                    }
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::UnpauseParBlocks { job, blocks, reply } => {
                let unpaused = self.unpause_par_blocks(job, blocks);
                let _ = reply.send(unpaused);
            }
            QueueCommand::RetainJobs { keep, reply } => {
                self.persist = false;
                let keep: HashSet<JobId> = keep.into_iter().collect();
                let drop_ids: Vec<JobId> = self
                    .state
                    .jobs
                    .iter()
                    .map(|j| j.id)
                    .filter(|id| !keep.contains(id))
                    .collect();
                for id in drop_ids {
                    self.remove_job_silent(id);
                }
                self.delegated.clear();
                self.mirror.clear();
                self.publish_now();
                self.bump_epoch();
                let _ = reply.send(());
            }
        }
    }

    // -- cluster: import / export / delegation / adoption --------------------

    fn import_job(&mut self, mut job: Job, fold_journals: bool, emit_finished: bool) {
        // Normalize transient state from the wire.
        for f in &mut job.files {
            for s in &mut f.segments {
                if matches!(s.state, SegmentState::Leased { .. }) {
                    s.state = SegmentState::Pending;
                }
            }
        }
        if matches!(job.status, JobStatus::Downloading) {
            job.status = JobStatus::Queued;
        }
        let job_id = job.id;
        let max_file = job.files.iter().map(|f| f.id.0).max().unwrap_or(0);
        self.state.next_job_id = self.state.next_job_id.max(job_id.0);
        self.state.next_file_id = self.state.next_file_id.max(max_file);

        // Replace any existing copy (idempotent re-grant / completion).
        if self.state.job(job_id).is_some() {
            self.remove_job_silent(job_id);
        }
        let terminal = matches!(
            job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
        );
        let name = job.name.clone();
        let status = job.status;
        let health = Health::calc(&job.totals).0;
        self.state.jobs.push(job);
        if let Some(j) = self.state.job_mut(job_id) {
            recompute_job_totals(j);
        }
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);

        if fold_journals {
            self.fold_job_journals(job_id);
        }
        tracing::info!(job = job_id.0, %name, ?status, "job imported");
        self.dirty = true;
        if self.persist {
            self.save_snapshot();
        }
        self.publish_now();
        if terminal && emit_finished {
            self.emit(Event::JobFinished {
                job: job_id,
                name,
                status,
                health,
            });
        }
        self.bump_epoch();
    }

    fn remove_job_silent(&mut self, job_id: JobId) -> bool {
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let job = self.state.jobs.remove(idx);
        for f in &job.files {
            self.writers.remove(&f.id);
            self.file_sizes.remove(&f.id);
            self.finalize_sent.remove(&f.id);
        }
        self.pending_finalize.retain(|(j, _)| *j != job_id);
        self.attempts.retain(|r, _| r.job != job_id);
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);
        self.publish_now();
        self.bump_epoch();
        true
    }

    fn fold_job_journals(&mut self, job_id: JobId) {
        let recs = match JobJournals::replay_job(&self.state_dir, job_id) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(job = job_id.0, error = %e, "journal fold failed");
                return;
            }
        };
        let mut touched: HashSet<FileId> = HashSet::new();
        let mut applied = 0usize;
        for rec in recs {
            if rec.job != job_id {
                continue;
            }
            let r = SegRef {
                job: rec.job,
                file: rec.file,
                seg_number: rec.segment_number,
            };
            if rec.file_size > 0 {
                self.file_sizes.insert(rec.file, rec.file_size);
            }
            let Some(seg) = self.state.segment_mut(r) else {
                continue;
            };
            if !matches!(seg.state, SegmentState::Done { .. }) {
                seg.state = SegmentState::Done {
                    offset: rec.offset,
                    len: rec.len,
                    crc: rec.crc32,
                };
                self.attempts.remove(&r);
                touched.insert(rec.file);
                applied += 1;
            }
        }
        if applied > 0 {
            tracing::info!(job = job_id.0, applied, "folded journal records");
            if let Some(j) = self.state.job_mut(job_id) {
                recompute_job_totals(j);
            }
            for file in touched {
                self.after_file_change(job_id, file);
            }
            self.bump_epoch();
        }
    }

    fn adopt_authority(&mut self) {
        self.persist = true;
        match self.snap_store.load() {
            Ok(Some(doc)) => {
                let snapshot_state = QueueState::from_doc(doc);
                self.state.next_job_id = self.state.next_job_id.max(snapshot_state.next_job_id);
                self.state.next_file_id = self.state.next_file_id.max(snapshot_state.next_file_id);
                self.state.download_paused = snapshot_state.download_paused;
                if self.state.speed_limit_bps.is_none() {
                    self.state.speed_limit_bps = snapshot_state.speed_limit_bps;
                    self.limiter.set(snapshot_state.speed_limit_bps);
                }
                for job in snapshot_state.jobs {
                    // Local executor copies are fresher than the late
                    // leader's snapshot — keep them.
                    if self.state.job(job.id).is_none() {
                        self.state.jobs.push(job);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, "authority snapshot unreadable; adopting journals only");
            }
        }

        // Fold every job's journals (union across lease files).
        let ids: Vec<JobId> = self.state.jobs.iter().map(|j| j.id).collect();
        for id in &ids {
            self.fold_job_journals(*id);
        }
        self.state.recompute_all_totals();

        // Finish anything that completed right before the takeover.
        self.startup_pass();
        self.save_snapshot();
        self.publish_now();
        self.bump_epoch();
        tracing::info!(jobs = self.state.jobs.len(), "adopted queue authority");
    }

    // -- scheduling ----------------------------------------------------------

    fn grant_work(&mut self, server_id: ServerId, max: usize) -> Vec<Lease> {
        if self.disk_low || self.is_blocked(server_id) {
            return Vec::new();
        }
        let servers = self.servers.clone();
        let Some(server) = servers.iter().find(|s| s.id == server_id) else {
            return Vec::new();
        };
        let now = Instant::now();
        let blocked_now: HashSet<ServerId> = self
            .blocked
            .iter()
            .filter(|(_, until)| **until > now)
            .map(|(id, _)| *id)
            .collect();
        let is_blocked = move |id: ServerId| blocked_now.contains(&id);

        let mut leases = Vec::new();
        for _ in 0..max {
            let ladder = Ladder::new(&servers);
            let mut ctx = SelectionCtx {
                ladder: &ladder,
                attempts: &mut self.attempts,
                is_blocked: &is_blocked,
                delegated: &self.delegated,
                article_retries: self.tuning.article_retries,
                now_unix: unix_now(),
                propagation_delay_secs: self.tuning.propagation_delay.as_secs() as i64,
                soft_hold: self.quota_reached,
            };
            let result = next_for_server(&self.state, server, &mut ctx);
            let exhausted = result.exhausted;
            let lease = result.lease;
            for r in exhausted {
                self.fail_segment(r);
            }
            let Some(r) = lease else { break };

            let (message_id, writer) = {
                let Some(seg) = self.state.segment_mut(r) else {
                    break;
                };
                seg.state = SegmentState::Leased { server: server_id };
                let msgid = seg.message_id.to_string();
                (msgid, self.writer_for(r.job, r.file))
            };
            if let Some(job) = self.state.job_mut(r.job) {
                if matches!(job.status, JobStatus::Queued) {
                    job.status = JobStatus::Downloading;
                }
            }
            leases.push(Lease {
                r,
                message_id,
                writer,
            });
        }
        leases
    }

    // -- outcomes ------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn on_segment_written(
        &mut self,
        job: JobId,
        file: FileId,
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
        server: ServerId,
    ) {
        self.volumes
            .add(server, len as u64, unix_now(), self.tuning.quota_start_day);
        let r = SegRef {
            job,
            file,
            seg_number,
        };
        if file_size > 0 {
            self.file_sizes.insert(file, file_size);
        }
        let Some(seg) = self.state.segment_mut(r) else {
            return; // job deleted while the write was in flight
        };
        if matches!(seg.state, SegmentState::Done { .. }) {
            return; // duplicate (e.g. recovery overlap)
        }
        seg.state = SegmentState::Done { offset, len, crc };
        if let Err(e) = self.journal.append(&JournalRecord {
            job,
            file,
            segment_number: seg_number,
            offset,
            len,
            crc32: crc,
            file_size,
        }) {
            tracing::error!(error = %e, "journal append failed");
        }
        self.attempts.remove(&r);
        if let Some(j) = self.state.job_mut(job) {
            recompute_job_totals(j);
        }
        self.after_file_change(job, file);
    }

    fn on_segment_failed(&mut self, r: SegRef, server: ServerId, outcome: AttemptOutcome) {
        let servers = self.servers.clone();
        let ladder = Ladder::new(&servers);
        let att = self
            .attempts
            .entry(r)
            .or_insert_with(|| SegmentAttempt::new(self.tuning.article_retries));
        let verdict = ladder.on_outcome(att, server, outcome);

        let age_days = self
            .state
            .job(r.job)
            .and_then(|j| j.files.iter().find(|f| f.id == r.file))
            .and_then(|f| f.date)
            .map(|d| ((unix_now() - d).max(0) / 86_400) as u32);

        let exhausted = match verdict {
            Verdict::NextServer => {
                let att = self.attempts.get_mut(&r).unwrap();
                ladder.is_exhausted(att, age_days)
            }
            Verdict::Failed => true,
            _ => false,
        };

        // Release the lease back to pending.
        if let Some(seg) = self.state.segment_mut(r) {
            if matches!(seg.state, SegmentState::Leased { .. }) {
                seg.state = SegmentState::Pending;
            }
        }

        match verdict {
            Verdict::RetrySame { block_server } if block_server => self.block_server(server),
            _ => {}
        }
        if exhausted {
            self.fail_segment(r);
        }
        self.bump_epoch(); // work may now be eligible for other servers
    }

    fn fail_segment(&mut self, r: SegRef) {
        let Some(seg) = self.state.segment_mut(r) else {
            return;
        };
        if matches!(seg.state, SegmentState::Done { .. } | SegmentState::Failed) {
            return;
        }
        seg.state = SegmentState::Failed;
        self.attempts.remove(&r);
        if let Some(j) = self.state.job_mut(r.job) {
            recompute_job_totals(j);
        }
        tracing::debug!(
            job = r.job.0,
            file = r.file.0,
            seg = r.seg_number,
            "segment exhausted"
        );
        self.emit(Event::SegmentExhausted {
            job: r.job,
            file: r.file,
            segment: r.seg_number,
        });
        self.after_file_change(r.job, r.file);
        self.maybe_abort_unhealthy(r.job);
    }

    /// NZBGet-style critical-health abort: once enough articles have
    /// failed that the job can't be repaired even with every par2 block
    /// (`health < critical_health`), stop wasting bandwidth. Pending
    /// segments are failed outright; leased ones finish their in-flight
    /// attempt honestly. The job then completes as Failed through the
    /// normal path, and the PP health gate parks/deletes per policy.
    fn maybe_abort_unhealthy(&mut self, job_id: JobId) {
        if !self.tuning.health_abort {
            return;
        }
        let Some(job) = self.state.job(job_id) else {
            return;
        };
        if !matches!(
            job.status,
            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
        ) {
            return;
        }
        let health = Health::calc(&job.totals);
        let critical = Health::calc_critical(&job.totals, true);
        if health.0 >= critical.0 {
            return;
        }
        let name = job.name.clone();
        let pending: Vec<(FileId, Vec<u32>)> = job
            .files
            .iter()
            .filter(|f| !f.is_terminal())
            .map(|f| {
                (
                    f.id,
                    f.segments
                        .iter()
                        .filter(|s| matches!(s.state, SegmentState::Pending))
                        .map(|s| s.number)
                        .collect(),
                )
            })
            .collect();
        tracing::warn!(
            job = job_id.0,
            %name,
            health = health.0,
            critical = critical.0,
            "aborting download: health below critical (unrepairable even with all par2)"
        );
        for (file, segs) in pending {
            for seg_number in segs {
                let r = SegRef {
                    job: job_id,
                    file,
                    seg_number,
                };
                if let Some(seg) = self.state.segment_mut(r) {
                    seg.state = SegmentState::Failed;
                }
                self.attempts.remove(&r);
            }
            if let Some(j) = self.state.job_mut(job_id) {
                recompute_job_totals(j);
            }
            self.after_file_change(job_id, file);
        }
        self.bump_epoch();
    }

    /// Writer reported an unrecoverable disk error: fail every non-done
    /// segment of the file.
    fn fail_whole_file(&mut self, job: JobId, file: FileId) {
        let refs: Vec<SegRef> = match self.state.file_mut(job, file) {
            Some(f) => f
                .segments
                .iter()
                .filter(|s| !matches!(s.state, SegmentState::Done { .. }))
                .map(|s| SegRef {
                    job,
                    file,
                    seg_number: s.number,
                })
                .collect(),
            None => return,
        };
        for r in refs {
            self.fail_segment(r);
        }
    }

    // -- completion cascade --------------------------------------------------

    fn after_file_change(&mut self, job: JobId, file: FileId) {
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        if !f.is_terminal() || f.finalized {
            self.check_job_complete(job);
            return;
        }
        if f.has_any_done() {
            if !self.finalize_sent.contains(&file) {
                self.send_finalize(job, file);
            }
        } else {
            // Nothing on disk; complete trivially.
            f.finalized = true;
            let filename = f.filename.clone();
            self.emit(Event::FileFinished {
                job,
                file,
                filename,
                ok: false,
            });
            self.dirty = true;
            self.check_job_complete(job);
        }
    }

    fn send_finalize(&mut self, job: JobId, file: FileId) {
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        // Combined whole-file CRC when coverage is complete and contiguous.
        let mut segs: Vec<(u64, u32, u32)> = Vec::with_capacity(f.segments.len());
        let mut all_done = true;
        for s in &f.segments {
            match s.state {
                SegmentState::Done { offset, len, crc } => segs.push((offset, len, crc)),
                _ => all_done = false,
            }
        }
        segs.sort_by_key(|(off, _, _)| *off);
        let combined_crc = if all_done { combine_crcs(&segs) } else { None };

        let file_size = self
            .file_sizes
            .get(&file)
            .copied()
            .filter(|s| *s > 0)
            .unwrap_or_else(|| {
                segs.iter()
                    .map(|(o, l, _)| o + *l as u64)
                    .max()
                    .unwrap_or(0)
            });

        let tx = self.writer_for(job, file);
        match tx.try_send(WriteCmd::Finalize {
            file_size,
            combined_crc,
        }) {
            Ok(()) => {
                self.finalize_sent.insert(file);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.pending_finalize.push((job, file)); // retried on tick
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.writers.remove(&file);
                self.pending_finalize.push((job, file)); // respawn on retry
            }
        }
    }

    fn on_writer_finalized(
        &mut self,
        job: JobId,
        file: FileId,
        ok: bool,
        final_path: Option<PathBuf>,
        combined_crc: Option<u32>,
    ) {
        self.writers.remove(&file);
        self.finalize_sent.remove(&file);
        self.file_sizes.remove(&file);
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        f.finalized = true;
        if ok {
            f.crc32 = combined_crc;
        }
        let filename = f.filename.clone();
        tracing::info!(job = job.0, file = file.0, %filename, ok, path = ?final_path, "file finished");
        self.emit(Event::FileFinished {
            job,
            file,
            filename,
            ok,
        });
        self.dirty = true;
        self.check_job_complete(job);
    }

    fn check_job_complete(&mut self, job_id: JobId) {
        if self.delegated.contains_key(&job_id) {
            return; // completes via the executor's report, not locally
        }
        let Some(job) = self.state.job_mut(job_id) else {
            return;
        };
        if !matches!(
            job.status,
            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
        ) {
            return;
        }
        let complete = !job.files.is_empty()
            && job
                .files
                .iter()
                .all(|f| f.paused || (f.is_terminal() && (!f.has_any_done() || f.finalized)));
        if !complete {
            return;
        }
        let (status, health) = final_status(job);
        job.status = status;
        let name = job.name.clone();
        let file_ids: Vec<FileId> = job.files.iter().map(|f| f.id).collect();
        tracing::info!(job = job_id.0, %name, ?status, health = health.0, "job finished");
        self.attempts.retain(|r, _| r.job != job_id);
        for fid in &file_ids {
            self.writers.remove(fid);
            self.file_sizes.remove(fid);
        }
        // Persist and publish BEFORE emitting: an event subscriber that
        // immediately reads the snapshot must see the terminal state.
        self.save_snapshot();
        self.publish_now();
        self.emit(Event::JobFinished {
            job: job_id,
            name,
            status,
            health: health.0,
        });
    }

    // -- servers -------------------------------------------------------------

    fn is_blocked(&self, server: ServerId) -> bool {
        self.blocked
            .get(&server)
            .is_some_and(|until| *until > Instant::now())
    }

    fn block_server(&mut self, server: ServerId) {
        let until = Instant::now() + self.tuning.retry_interval;
        let newly = self
            .blocked
            .insert(server, until)
            .is_none_or(|prev| prev <= Instant::now());
        if newly {
            tracing::warn!(
                server = server.0,
                secs = self.tuning.retry_interval.as_secs(),
                "server blocked after connection failure — retrying on a timer"
            );
            self.emit(Event::ServerBlocked {
                server,
                seconds: self.tuning.retry_interval.as_secs(),
            });
        }
    }

    // -- writers -------------------------------------------------------------

    fn writer_for(&mut self, job: JobId, file: FileId) -> mpsc::Sender<WriteCmd> {
        if let Some(h) = self.writers.get(&file) {
            if !h.tx.is_closed() {
                return h.tx.clone();
            }
        }
        let (job_name, filename) = match self.state.job(job) {
            Some(j) => (
                j.name.clone(),
                j.files
                    .iter()
                    .find(|f| f.id == file)
                    .map(|f| f.filename.clone())
                    .unwrap_or_else(|| format!("file-{}", file.0)),
            ),
            None => (format!("job-{}", job.0), format!("file-{}", file.0)),
        };
        let dir = self.dest_dir.join(sanitize_name(&job_name));
        let h = spawn_writer(
            &self.tracker,
            job,
            file,
            dir,
            filename,
            self.engine_tx.clone(),
        );
        let tx = h.tx.clone();
        self.writers.insert(file, h);
        tx
    }

    // -- jobs ----------------------------------------------------------------

    /// Delayed-par: unpause the smallest covering set; returns blocks freed.
    fn unpause_par_blocks(&mut self, job_id: JobId, blocks: u32) -> u32 {
        let Some(job) = self.state.job_mut(job_id) else {
            return 0;
        };
        let candidates: Vec<(FileId, u32)> = job
            .files
            .iter()
            .filter(|f| f.paused && f.is_par2)
            .filter_map(|f| vol_par_blocks(&f.filename).map(|b| (f.id, b)))
            .collect();
        if candidates.is_empty() {
            return 0;
        }
        let picked = pick_par_files(&candidates, blocks.max(1));
        let mut freed = 0u32;
        for (id, b) in &candidates {
            if picked.contains(id) {
                if let Some(f) = job.files.iter_mut().find(|f| f.id == *id) {
                    f.paused = false;
                    freed += b;
                }
            }
        }
        if freed > 0 {
            // The job likely finished its download phase; make it
            // schedulable again for the par files.
            if matches!(
                job.status,
                JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
            ) {
                job.status = JobStatus::Queued;
            }
            recompute_job_totals(job);
            tracing::info!(job = job_id.0, freed, "delayed par files unpaused");
            self.dirty = true;
            self.bump_epoch();
            self.publish_now();
        }
        freed
    }

    fn delete_job(&mut self, job_id: JobId, delete_files: bool) -> bool {
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let job = self.state.jobs.remove(idx);
        for f in &job.files {
            self.writers.remove(&f.id); // dropped senders stop the writers
            self.file_sizes.remove(&f.id);
            self.finalize_sent.remove(&f.id);
        }
        self.pending_finalize.retain(|(j, _)| *j != job_id);
        self.attempts.retain(|r, _| r.job != job_id);
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);
        if self.persist {
            let _ = self.journal.remove_job(job_id);
        }
        if delete_files {
            let dir = self.dest_dir.join(sanitize_name(&job.name));
            tokio::spawn(async move {
                if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(dir = %dir.display(), error = %e, "delete files failed");
                    }
                }
            });
        }
        tracing::info!(job = job_id.0, name = %job.name, delete_files, "job deleted");
        self.save_snapshot();
        self.publish_now();
        self.emit(Event::JobDeleted { job: job_id });
        self.bump_epoch();
        true
    }

    // -- tick / snapshot -----------------------------------------------------

    /// Disk + quota guards, evaluated every 10 s (cluster-aware quota:
    /// peers' volume files on the shared state dir are summed in).
    /// Reposition a job in the queue vec. Position is the scheduler's
    /// tiebreaker within a priority band and persists via the snapshot.
    fn move_job(&mut self, job_id: JobId, op: MoveOp) -> bool {
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let last = self.state.jobs.len() - 1;
        let target = match op {
            MoveOp::Top => 0,
            MoveOp::Up => idx.saturating_sub(1),
            MoveOp::Down => (idx + 1).min(last),
            MoveOp::Bottom => last,
        };
        if target != idx {
            let job = self.state.jobs.remove(idx);
            self.state.jobs.insert(target, job);
            self.dirty = true;
            self.bump_epoch();
            self.publish_now();
        }
        true
    }

    fn update_guards(&mut self) {
        if self.tuning.min_free_disk_bytes > 0 {
            let free = crate::volumes::free_space(&self.dest_dir);
            let was = self.disk_low;
            self.disk_low = free < self.tuning.min_free_disk_bytes;
            if self.disk_low != was {
                if self.disk_low {
                    tracing::warn!(
                        free,
                        floor = self.tuning.min_free_disk_bytes,
                        "destination volume low on space — downloads held"
                    );
                } else {
                    tracing::info!(free, "disk space recovered — downloads resume");
                }
                self.publish_now();
            }
        }
        if self.tuning.daily_quota_bytes > 0 || self.tuning.monthly_quota_bytes > 0 {
            let (day, month) = self
                .volumes
                .cluster_totals(unix_now(), self.tuning.quota_start_day);
            let was = self.quota_reached;
            self.quota_reached = (self.tuning.daily_quota_bytes > 0
                && day >= self.tuning.daily_quota_bytes)
                || (self.tuning.monthly_quota_bytes > 0
                    && month >= self.tuning.monthly_quota_bytes);
            if self.quota_reached != was {
                tracing::warn!(
                    day,
                    month,
                    reached = self.quota_reached,
                    "download quota state changed"
                );
                self.publish_now();
            }
        }
    }

    fn on_tick(&mut self) {
        self.guard_tick = self.guard_tick.wrapping_add(1);
        // NOT `is_multiple_of`: stabilized in 1.87, MSRV is 1.85.
        if self.guard_tick % 10 == 0 {
            self.update_guards();
        }
        if self.guard_tick % 30 == 0 {
            self.volumes.save_if_dirty();
        }
        let rate = self.meter.tick();

        let now = Instant::now();
        let before = self.blocked.len();
        self.blocked.retain(|_, until| *until > now);
        if self.blocked.len() != before {
            self.bump_epoch(); // blocked servers came back: hand out work
        }

        if let Err(e) = self.journal.sync() {
            tracing::error!(error = %e, "journal fsync failed");
        }

        let pending = std::mem::take(&mut self.pending_finalize);
        for (j, f) in pending {
            if !self.finalize_sent.contains(&f) {
                self.send_finalize(j, f);
            }
        }

        self.publish_snapshot(rate);

        if self.dirty && self.last_save.elapsed() > Duration::from_secs(2) {
            self.save_snapshot();
        }
    }

    fn publish_now(&mut self) {
        let rate = self.shared.load().download_rate_bps;
        self.publish_snapshot(rate);
    }

    fn publish_snapshot(&mut self, rate: u64) {
        let jobs = self
            .state
            .jobs
            .iter()
            .map(|j| {
                let health = Health::calc(&j.totals);
                let critical = Health::calc_critical(&j.totals, true);
                let remaining: u64 = j
                    .files
                    .iter()
                    .filter(|f| !f.paused)
                    .flat_map(|f| &f.segments)
                    .filter(|s| {
                        matches!(s.state, SegmentState::Pending | SegmentState::Leased { .. })
                    })
                    .map(|s| s.size as u64)
                    .sum();
                let mut summary = JobSummary {
                    id: j.id,
                    name: j.name.clone(),
                    status: j.status,
                    category: j.category.clone(),
                    priority: j.priority,
                    size_bytes: j.totals.size,
                    downloaded_bytes: j.totals.success_size,
                    failed_bytes: j.totals.failed_size,
                    remaining_bytes: remaining,
                    total_articles: j.totals.total_articles,
                    done_articles: j.totals.success_articles,
                    failed_articles: j.totals.failed_articles,
                    files_total: j.files.len() as u32,
                    files_done: j.files.iter().filter(|f| f.is_terminal()).count() as u32,
                    health: health.0,
                    critical_health: critical.0,
                    assigned_node: self.delegated.get(&j.id).cloned(),
                    pp_done: j.params.iter().any(|(k, _)| k == nzbd_types::PP_DONE_PARAM),
                    dupe_key: j.dupe.key.clone(),
                    dupe_score: j.dupe.score,
                    rate_bps: 0,
                };
                // Delegated jobs progress remotely; overlay heartbeat stats.
                if let Some(m) = self.mirror.get(&j.id) {
                    summary.done_articles = m.done_articles;
                    summary.failed_articles = m.failed_articles;
                    summary.downloaded_bytes = m.downloaded_bytes;
                    summary.remaining_bytes = summary
                        .size_bytes
                        .saturating_sub(m.downloaded_bytes)
                        .saturating_sub(summary.failed_bytes);
                    summary.health = m.health;
                    if m.done_articles > 0 && summary.status == JobStatus::Queued {
                        summary.status = JobStatus::Downloading;
                    }
                }
                summary
            })
            .collect::<Vec<_>>();
        let jobs: Vec<JobSummary> = {
            let mut jobs = jobs;
            for summary in &mut jobs {
                let meter = self
                    .job_rates
                    .entry(summary.id.0)
                    .or_insert_with(|| JobRateMeter {
                        last_bytes: summary.downloaded_bytes,
                        last_at: std::time::Instant::now(),
                        ema_bps: 0.0,
                    });
                summary.rate_bps = if summary.status == JobStatus::Downloading {
                    meter.update(summary.downloaded_bytes)
                } else {
                    // Reset the baseline so a resume doesn't spike.
                    meter.last_bytes = summary.downloaded_bytes;
                    meter.ema_bps = 0.0;
                    0
                };
            }
            let live: HashSet<u32> = jobs.iter().map(|s| s.id.0).collect();
            self.job_rates.retain(|id, _| live.contains(id));
            jobs
        };
        let now_block = Instant::now();
        let mut blocked_servers: Vec<u32> = self
            .blocked
            .iter()
            .filter(|(_, until)| **until > now_block)
            .map(|(id, _)| id.0)
            .collect();
        blocked_servers.sort_unstable();
        let snap = QueueSnapshot {
            up_since_unix: self.up_since_unix,
            download_paused: self.state.download_paused,
            quota_reached: self.quota_reached,
            disk_low: self.disk_low,
            blocked_servers,
            health_abort: self.tuning.health_abort,
            server_volumes: {
                let now_day = unix_now().div_euclid(86_400);
                let mut v: Vec<crate::snapshot::ServerVolume> = self
                    .volumes
                    .doc()
                    .servers
                    .iter()
                    .map(|(id, w)| crate::snapshot::ServerVolume {
                        server: *id,
                        total_bytes: w.total_bytes,
                        day_bytes: if w.day_key == now_day { w.day_bytes } else { 0 },
                        month_bytes: w.month_bytes,
                    })
                    .collect();
                v.sort_by_key(|x| x.server);
                v
            },
            speed_limit_bps: self.state.speed_limit_bps,
            download_rate_bps: rate,
            session_downloaded_bytes: self.meter.total(),
            remaining_bytes: self.state.remaining_bytes(),
            jobs,
        };
        self.shared.store(Arc::new(snap));
    }

    fn save_snapshot(&mut self) {
        if !self.persist {
            self.dirty = false;
            return;
        }
        let doc = self.state.to_doc();
        let result = match &self.persist_guard {
            Some(g) => {
                let g = g.clone();
                self.snap_store.save_guarded(&doc, &move || g())
            }
            None => self.snap_store.save(&doc),
        };
        if let Err(e) = result {
            tracing::error!(error = %e, "snapshot save failed (fenced or io); demoting persistence until re-adopted");
            if matches!(e, nzbd_state::StateError::Corrupt(_)) {
                self.persist = false; // deposed: stop writing authority state
            }
            return;
        }
        // The snapshot now embodies every folded segment: compact journals
        // of jobs we own outright, plus orphaned job dirs (deleted jobs,
        // stale zombies). Delegated jobs have live foreign writers — skip.
        let known: HashSet<JobId> = self.state.jobs.iter().map(|j| j.id).collect();
        if let Ok(entries) = std::fs::read_dir(self.journal.jobs_dir()) {
            for entry in entries.flatten() {
                let Some(id) = entry
                    .file_name()
                    .to_string_lossy()
                    .parse::<u32>()
                    .ok()
                    .map(JobId)
                else {
                    continue;
                };
                // Known & not delegated: folded into the snapshot just
                // written. Unknown: orphan from a deleted job or a stale
                // lease. Delegated: live foreign writer — leave alone.
                let _ = known; // clarity: both known and orphan compact
                if !self.delegated.contains_key(&id) {
                    if let Err(e) = self.journal.remove_job(id) {
                        tracing::debug!(job = id.0, error = %e, "journal compact skip");
                    }
                }
            }
        }
        self.dirty = false;
        self.last_save = Instant::now();
    }

    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    fn bump_epoch(&self) {
        self.epoch_tx.send_modify(|v| *v += 1);
    }
}

/// Combine per-segment CRCs into the whole-file CRC. Requires contiguous
/// coverage from offset 0 (yEnc parts are contiguous by construction).
fn combine_crcs(sorted: &[(u64, u32, u32)]) -> Option<u32> {
    let mut expect = 0u64;
    let mut acc: Option<u32> = None;
    for (off, len, crc) in sorted {
        if *off != expect {
            return None;
        }
        expect += *len as u64;
        acc = Some(match acc {
            None => *crc,
            Some(prev) => nzbd_yenc::crc32_combine(prev, *crc, *len as u64),
        });
    }
    acc
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
