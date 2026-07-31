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
    final_status, job_dir_name, next_for_server, pick_par_files, recompute_job_totals, QueueState,
    SegRef, SelectionCtx,
};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::snapshot::{JobSummary, QueueSnapshot, SharedSnapshot};
use crate::writer::{spawn_writer, WriteCmd, WriterHandle};
use crate::Tuning;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{FsJournal, JobJournals, JournalRecord, SnapshotStore, UncleanMarker};
use nzbd_types::{
    FileId, Health, Job, JobId, JobStatus, PostStage, SegmentState, ServerDef, ServerId, StageSpan,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
        /// Caller-supplied job params, applied in this same turn.
        params: Vec<(String, String)>,
        reply: oneshot::Sender<JobId>,
    },
    AddUrl {
        name: String,
        url: String,
        category: Option<String>,
        priority: i32,
        dupe: Option<nzbd_types::DupeInfo>,
        paused: bool,
        params: Vec<(String, String)>,
        /// Replies `(id, created)`. `created == false` means the same URL is
        /// already fetching — the caller must NOT spawn a second fetch task.
        reply: oneshot::Sender<(JobId, bool)>,
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
        /// Requesting client (UA / "web-ui") — logged and carried on the
        /// event so pause flapping is attributable.
        source: String,
        reply: oneshot::Sender<()>,
    },
    ResumeAll {
        source: String,
        reply: oneshot::Sender<()>,
    },
    SetSpeedLimit {
        bytes_per_sec: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    /// How many jobs may download at once. Replies with the value
    /// actually applied, which is the request clamped into range — the
    /// caller learns what happened rather than having to re-read.
    SetMaxActiveDownloads { n: u32, reply: oneshot::Sender<u32> },
    /// Operator-set per-server connection counts. Replies with the values
    /// actually applied after clamping to each server's spawned ceiling.
    SetServerConnectionCaps {
        caps: HashMap<ServerId, u16>,
        reply: oneshot::Sender<HashMap<ServerId, u16>>,
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
    MirrorProgress { job: JobId, stats: MirrorStats },
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
    AdoptAuthority { reply: oneshot::Sender<()> },
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
    /// Enter a post-processing stage: set `Post { stage }` AND append the
    /// stage span, closing the previous one. Separate from `SetJobStatus`
    /// because the status and the timeline must not be settable apart —
    /// see [`close_span`].
    EnterPostStage {
        job: JobId,
        stage: PostStage,
        at_unix: i64,
        /// The manager's monotonic measurement of the stage being left.
        prev_ms: Option<u64>,
        reply: oneshot::Sender<bool>,
    },
    /// Close the running stage span without opening another — the pipeline
    /// ended (success, failure, or an early return). Fire-and-forget: the
    /// job is on its way to history and nothing waits on the timing.
    ClosePostStage {
        job: JobId,
        at_unix: i64,
        ms: Option<u64>,
    },
    /// Delayed-par download (§3.2): unpause the smallest set of paused
    /// par2 files covering `blocks`. Replies with the number of recovery
    /// blocks now downloading (0 = nothing left to unpause).
    ///
    /// `block_size` is the recovery-set slice size read from the job's own
    /// par2 index, and is what lets a hash-named volume — no `.volXX+NN`
    /// marker anywhere — be priced by its size.
    UnpauseParBlocks {
        job: JobId,
        blocks: u32,
        block_size: Option<u64>,
        reply: oneshot::Sender<u32>,
    },
}

/// Bank the running stage's duration onto the job's timeline.
///
/// `measured` is the post manager's monotonic figure and is preferred
/// whenever it exists: wall-clock subtraction is subject to NTP steps, and
/// a repair that reads as negative time is worse than no figure at all.
/// The wall-clock fallback covers the one case the manager cannot measure
/// — a span that outlived the process, where the `Instant` is gone but
/// `started_at_unix` survived in the snapshot.
///
/// Idempotent: a span that already has a duration keeps it, so a close
/// following a close cannot overwrite a good measurement with a stale one.
fn close_span(job: &mut Job, measured: Option<u64>, at_unix: i64) {
    if let Some(prev) = job.stages.last_mut() {
        if prev.ms.is_none() {
            prev.ms = Some(measured.unwrap_or_else(|| {
                at_unix.saturating_sub(prev.started_at_unix).max(0) as u64 * 1000
            }));
        }
    }
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
    /// A real write ran out of space (ENOSPC/EDQUOT). Reported from
    /// wherever it happened — writer, finalize, post-processing — and
    /// latches the disk guard immediately.
    OutOfSpace {
        whence: String,
    },
    /// A completed par2 file told us what this job really is. Cosmetic and
    /// best-effort: dropped freely under load, and ignored if the job has
    /// meanwhile acquired a real name by any other route.
    LearnedJobName {
        job: JobId,
        name: String,
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
    /// Jobs whose bytes did not survive the trip to disk, and why.
    ///
    /// Health cannot express this. Health is computed from segment state —
    /// how much of the article set arrived off the wire — and a file whose
    /// segments all downloaded perfectly and then failed to `set_len`,
    /// `sync_data` or `rename` has a PERFECT health score and a wrong file on
    /// disk. On NFS in particular, a deferred writeback error (ENOSPC,
    /// EDQUOT, EIO) surfaces at `sync_data()` and nowhere else, so this is
    /// the ONLY place the daemon can learn the download did not land.
    ///
    /// Before this existed, `ok: false` from the writer set `finalized =
    /// true`, touched no totals, and the job completed as SUCCESS with 500 MiB
    /// of a 48 GiB remux on disk. A downloader may report that it failed. It
    /// may not report that it succeeded when it did not.
    write_failures: HashMap<JobId, String>,
    /// Jobs executing on another node (job → node name).
    delegated: HashMap<JobId, String>,
    mirror: HashMap<JobId, MirrorStats>,
    /// Per-job download-rate EMA, fed from downloaded-byte deltas at
    /// snapshot time (job id → meter).
    job_rates: HashMap<u32, JobRateMeter>,
    /// Wire-byte EMA per job (B/s), folded from the meter's time-stamped
    /// drains — the same measurement as the header rate.
    job_wire_ema: HashMap<u32, f64>,
    /// The same, per server. Same counters, same drains, same alpha — and
    /// the header rate IS the sum of these, so the chips sum to the tile
    /// exactly instead of to a second, differently-derived figure.
    server_wire_ema: HashMap<u32, f64>,
    /// Article retries per job (any failed attempt that goes back on the
    /// ladder). Surfaced in the snapshot: this is the gap between wire
    /// throughput and completed bytes, and it must be visible.
    retry_counts: HashMap<u32, u32>,

    state_dir: PathBuf,
    journal: JobJournals,
    snap_store: SnapshotStore,
    /// How long the last snapshot write took — feeds [`save_spacing`], so a
    /// slow state volume stretches the save cadence instead of consuming
    /// the owner loop.
    last_save_ms: u64,
    /// Destination free space, updated by a dedicated prober task (engine
    /// spawn wires it). The owner NEVER calls statvfs itself: on a
    /// write-saturated FUSE/network destination that syscall blocks for
    /// seconds, and it used to run inline every 10th tick — starving lease
    /// handout and cutting throughput 30% on a sawtooth (field report
    /// 2026-07-26). `u64::MAX` = not yet measured; never trips the guard.
    disk_free: Arc<AtomicU64>,
    marker: UncleanMarker,
    /// Queue-authority persistence (snapshot save/compact). Worker-mode
    /// engines run with this off; journals stay on regardless.
    persist: bool,
    persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    budget_tx: watch::Sender<HashMap<ServerId, u16>>,
    /// Cluster's share-out of a provider's account-wide connection limit.
    cluster_budgets: HashMap<ServerId, u16>,
    /// The operator's per-server connection count, changed from Settings
    /// without a restart.
    ///
    /// Kept apart from the cluster budget rather than both writing
    /// `budget_tx`, because they answer different questions — "how many
    /// may this NODE open" and "how many do I WANT open" — and whichever
    /// wrote last would erase the other's answer. What the connection
    /// tasks see is the smaller of the two.
    user_conn_caps: HashMap<ServerId, u16>,

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
    /// Latched by an out-of-space error observed on a real write. statvfs
    /// is the forecast; this is the ground truth, and it wins.
    enospc_latched: bool,
    enospc_observed: u64,
    enospc_where: Option<String>,
    guard_tick: u32,
    /// Round-robin cursor over the active set, advanced per granted
    /// lease. See `SelectionCtx::rotate`.
    rotate: usize,
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
        config_max_active: Option<u32>,
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
        // Same rule for concurrency: a value in the file is the operator
        // stating an intent, and it outranks whatever the last runtime
        // nudge happened to leave in the snapshot.
        if let Some(n) = config_max_active {
            state.max_active_downloads = crate::queue::clamp_active_downloads(n);
        }

        Ok(Owner {
            state,
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            finalize_sent: HashSet::new(),
            pending_finalize: Vec::new(),
            file_sizes,
            write_failures: HashMap::new(),
            delegated: HashMap::new(),
            mirror: HashMap::new(),
            job_rates: HashMap::new(),
            job_wire_ema: HashMap::new(),
            server_wire_ema: HashMap::new(),
            retry_counts: HashMap::new(),
            state_dir: state_dir.to_path_buf(),
            journal,
            snap_store,
            marker,
            persist,
            persist_guard,
            budget_tx,
            cluster_budgets: HashMap::new(),
            user_conn_caps: HashMap::new(),
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
            last_save_ms: 0,
            disk_free: Arc::new(AtomicU64::new(u64::MAX)),
            volumes: crate::volumes::VolumeBook::load(state_dir, journal_suffix),
            quota_reached: false,
            disk_low: false,
            enospc_latched: false,
            enospc_observed: 0,
            enospc_where: None,
            rotate: 0,
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

    /// Return jobs to `Queued` once they are neither in the active set
    /// nor holding a lease.
    ///
    /// `Downloading` is set the moment a job receives its first lease and
    /// was never once set back, so a job that caught a single segment
    /// during the spill-over at the tail of another job stayed labelled
    /// `Downloading` forever while receiving no work — a permanent claim
    /// made on the strength of one article. Harmless while the UI showed
    /// a flat list; the queue now groups rows by what they are doing, so
    /// the label is a heading a job sits under, and a job sitting under
    /// DOWNLOADING with no rate is the page telling a lie.
    ///
    /// Scans segments only for jobs currently claiming to download, and
    /// only until the first lease is found, so the expensive full scan
    /// happens once per stale job — after which it is `Queued` and no
    /// longer a candidate.
    fn settle_download_labels(&mut self) {
        let demoted =
            crate::queue::jobs_to_requeue(&self.state, &self.delegated, self.quota_reached);
        if demoted.is_empty() {
            return;
        }
        for id in demoted {
            if let Some(job) = self.state.job_mut(id) {
                job.status = JobStatus::Queued;
            }
        }
        self.dirty = true;
    }

    /// Publish the effective per-server connection allowance: the smaller
    /// of what the cluster grants this node and what the operator asked
    /// for. An absent entry on either side means "no opinion".
    fn publish_conn_budgets(&mut self) {
        let mut out: HashMap<ServerId, u16> = self.cluster_budgets.clone();
        for (id, want) in &self.user_conn_caps {
            let eff = match out.get(id) {
                Some(cluster) => (*cluster).min(*want),
                None => *want,
            };
            out.insert(*id, eff);
        }
        let _ = self.budget_tx.send(out);
        // Raising an allowance unparks tasks asleep on the budget
        // channel; they also need telling there may be work.
        self.bump_epoch();
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
            EngineMsg::LearnedJobName { job, name } => self.adopt_learned_name(job, name),
            EngineMsg::OutOfSpace { whence } => self.observe_out_of_space(&whence),
            EngineMsg::WriterError { job, file, error } => {
                tracing::warn!(job = job.0, file = file.0, %error, "writer error; failing file");
                if crate::is_out_of_space(&error) {
                    self.observe_out_of_space(&error);
                }
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
                params,
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
                    j.params.extend(params);
                }
                // Nothing has been written yet, so a better name may take
                // the directory with it.
                let name = self.name_from_requestor(id, true).unwrap_or(name);
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
                params,
                reply,
            } => {
                // Same URL already mid-fetch? Return that job instead of
                // piling up identical downloads — a client that retries an
                // add because the fetch looks slow (or a daemon restart hid
                // its progress) would otherwise queue N copies of the same
                // 50 GiB download. Failed fetches are NOT matched: re-adding
                // one is a deliberate retry.
                let existing = self
                    .state
                    .jobs
                    .iter()
                    .find(|j| {
                        matches!(j.status, JobStatus::Fetching)
                            && j.params.iter().any(|(k, v)| k == "*URL" && *v == url)
                    })
                    .map(|j| j.id);
                if let Some(id) = existing {
                    tracing::info!(
                        job = id.0,
                        %url,
                        "url add deduplicated: same URL is already fetching"
                    );
                    // The retry still gets its params applied. A client
                    // that re-adds because the fetch looked stalled is the
                    // same client that will later look for its tracking id
                    // on the job; dropping the params here loses the id on
                    // exactly the adds most likely to be retried, and the
                    // caller has no way to tell it happened.
                    if !params.is_empty() {
                        if let Some(j) = self.state.job_mut(id) {
                            for (k, v) in params {
                                match j.params.iter_mut().find(|(pk, _)| *pk == k) {
                                    Some(slot) => slot.1 = v,
                                    None => j.params.push((k, v)),
                                }
                            }
                        }
                        self.save_snapshot();
                        self.publish_now();
                    }
                    let _ = reply.send((id, false));
                    return;
                }
                let id = self.state.admit_url(name.clone(), &url, category, priority);
                if let Some(j) = self.state.job_mut(id) {
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.params.push(("*AddPaused".into(), "yes".into()));
                    }
                    j.params.extend(params);
                }
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                tracing::info!(job = id.0, %name, %url, "url job added (fetching)");
                self.save_snapshot();
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send((id, true));
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
                    // `complete_url_fetch` has just re-run the evidence
                    // pass against the fetched NZB. If that still came up
                    // with nothing, fall back to who asked. Still no files
                    // on disk, so the directory follows the name.
                    self.name_from_requestor(job, true);
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
            QueueCommand::PauseAll { source, reply } => {
                // Always log WHO, at info: "the queue keeps unpausing
                // itself" is always some client sending these — make the
                // culprit readable straight from `docker logs`.
                tracing::info!(
                    %source,
                    was_paused = self.state.download_paused,
                    "queue pause requested"
                );
                self.state.download_paused = true;
                self.dirty = true;
                self.emit(Event::QueuePauseChanged {
                    paused: true,
                    source,
                });
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::ResumeAll { source, reply } => {
                tracing::info!(
                    %source,
                    was_paused = self.state.download_paused,
                    "queue resume requested"
                );
                self.state.download_paused = false;
                // An operator resuming the queue is the operator override
                // for the out-of-space latch: they have seen the banner.
                self.clear_enospc_latch();
                self.dirty = true;
                self.emit(Event::QueuePauseChanged {
                    paused: false,
                    source,
                });
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
            QueueCommand::SetMaxActiveDownloads { n, reply } => {
                let n = crate::queue::clamp_active_downloads(n);
                self.state.max_active_downloads = n;
                self.dirty = true;
                // Lowering the cap leaves jobs labelled Downloading that
                // are no longer in the set; settle them now rather than
                // up to a second later, so the queue redraws once with
                // the right answer instead of twice.
                self.settle_download_labels();
                self.emit(Event::MaxActiveDownloadsChanged { n });
                // Wake the parked connection tasks: raising the cap makes
                // work available that the last scan said did not exist,
                // and nothing else would tell them for up to `idle_hold`.
                self.bump_epoch();
                self.publish_now();
                let _ = reply.send(n);
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
                self.cluster_budgets = budgets;
                self.publish_conn_budgets();
                let _ = reply.send(());
            }
            QueueCommand::SetServerConnectionCaps { caps, reply } => {
                // Clamp to what was actually spawned at boot: there are
                // `max_connections` tasks per server and no more, so a
                // higher number here would be a promise nothing keeps.
                // The caller is told the effective values, so the UI can
                // say "restart to go above this" instead of showing a
                // figure the daemon is quietly ignoring.
                let mut applied = HashMap::new();
                for (id, want) in caps {
                    let ceiling = self
                        .servers
                        .iter()
                        .find(|s| s.id == id)
                        .map(|s| s.max_connections)
                        .unwrap_or(0);
                    if ceiling == 0 {
                        continue;
                    }
                    applied.insert(id, want.clamp(1, ceiling));
                }
                tracing::info!(?applied, "connection counts updated");
                self.user_conn_caps = applied.clone();
                self.publish_conn_budgets();
                let _ = reply.send(applied);
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
            QueueCommand::EnterPostStage {
                job,
                stage,
                at_unix,
                prev_ms,
                reply,
            } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        close_span(j, prev_ms, at_unix);
                        j.stages.push(StageSpan {
                            stage,
                            started_at_unix: at_unix,
                            ms: None,
                        });
                        // Status and timeline move together, under the
                        // owner task's single mutable borrow. That is what
                        // makes "one stage at a time" structural: a caller
                        // cannot enter a stage without leaving the previous
                        // one, because it never gets to do only half of it.
                        j.status = JobStatus::Post { stage };
                        true
                    }
                    None => false,
                };
                if ok {
                    self.dirty = true;
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::ClosePostStage { job, at_unix, ms } => {
                if let Some(j) = self.state.job_mut(job) {
                    close_span(j, ms, at_unix);
                    self.dirty = true;
                    self.publish_now();
                }
            }
            QueueCommand::UnpauseParBlocks {
                job,
                blocks,
                block_size,
                reply,
            } => {
                let unpaused = self.unpause_par_blocks(job, blocks, block_size);
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
                rotate: self.rotate,
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
            // Advance the cursor for the NEXT lease, whichever connection
            // asks for it. One counter for the whole owner, not one per
            // server: the active set is a queue-wide notion, and per-
            // server cursors would let two servers settle into lockstep
            // on the same job.
            self.rotate = self.rotate.wrapping_add(1);
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
        // Every failed attempt is a retry the wire pays for again — count
        // it per job so "header rate ≫ row progress" is explained on the
        // dashboard instead of looking like a stale page.
        *self.retry_counts.entry(r.job.0).or_insert(0) += 1;
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

        // The yEnc-declared size, or nothing.
        //
        // This used to fall back to "the highest offset we actually wrote",
        // which finalize then applied with set_len — a silent truncation to
        // whatever happened to arrive, performed by the daemon itself, with
        // health and totals left untouched. It is reachable whenever
        // `file_sizes` has no entry for this file: the map is in-memory and
        // rebuilt from journal records that carry a size, so a restart or an
        // authority takeover mid-file lands squarely in it.
        //
        // A size we do not know is a size we must not invent. Zero means "do
        // not resize", which leaves the preallocated length alone rather than
        // cutting the file down to the part that made it.
        let file_size = self
            .file_sizes
            .get(&file)
            .copied()
            .filter(|s| *s > 0)
            .unwrap_or(0);

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
        if !ok {
            // The bytes did not land. Record it against the job so that
            // check_job_complete cannot call this a success — see the field's
            // comment for why health is incapable of catching it.
            tracing::error!(job = job.0, file = file.0, %filename,
                "finalize failed; the job cannot be reported as successful");
            self.write_failures
                .entry(job)
                .or_insert_with(|| format!("could not write {filename} to disk"));
        }
        tracing::info!(job = job.0, file = file.0, %filename, ok, path = ?final_path, "file finished");
        // A completed file may be the recovery index that knows what this
        // whole job actually is. Only worth asking when the job still has
        // no real name.
        if ok {
            if let Some(path) = final_path.clone() {
                self.maybe_learn_name_from(job, path);
            }
        }
        self.emit(Event::FileFinished {
            job,
            file,
            filename,
            ok,
        });
        self.dirty = true;
        self.check_job_complete(job);
    }

    /// Ask a just-completed file whether it is a par2 index, and if so
    /// whether it names the job.
    ///
    /// This is the "as soon as possible" half of naming an obfuscated
    /// post. The evidence exists from the moment the recovery index lands
    /// — for the 4.8 GiB job #182 that was **one minute** into the
    /// download — and waiting for post-processing to discover it means the
    /// queue shows a 40-character hash for the entire hour it runs.
    ///
    /// Content-sniffed, never extension-matched: an obfuscated post hides
    /// its par2 files exactly like everything else (#182's index arrived
    /// as `LKKp171CWZ3IrtvUyiLuNWIqWtos`), so `is_par2` — which is a guess
    /// off the NZB subject — is `false` for precisely the files this needs
    /// to read. The read happens on a blocking thread: the owner loop is
    /// the single writer for the whole queue and must never sit on a
    /// network mount's I/O.
    fn maybe_learn_name_from(&mut self, job: JobId, path: PathBuf) {
        let Some(j) = self.state.job(job) else { return };
        if !crate::queue::name_is_open(j) {
            return; // already named by something real
        }
        let tx = self.engine_tx.clone();
        self.tracker.spawn_blocking(move || {
            let Some(name) = read_par2_name(&path) else {
                return;
            };
            // Best effort: a full channel means the owner is busy, and a
            // cosmetic rename is not worth blocking a writer thread for.
            let _ = tx.try_send(EngineMsg::LearnedJobName { job, name });
        });
    }

    /// Fall back to naming a job by who asked for it, when its own
    /// documents named it nothing. Returns the new name if it changed.
    fn name_from_requestor(&mut self, job: JobId, storage_too: bool) -> Option<String> {
        let j = self.state.job_mut(job)?;
        if !crate::queue::name_is_open(j) {
            return None;
        }
        let better = crate::queue::requestor_name(j)?;
        let was = j.name.clone();
        // Provisional: this says who asked, not what arrived. The job's own
        // par2 metadata supersedes it the moment that lands.
        //
        // Through `set_job_name`, not the free function: a requestor name
        // is built from the client and the indexer and so is *shared* by
        // every job that client adds — exactly the shape that must never
        // be allowed to become a shared download directory.
        self.state
            .set_job_name(job, better.clone(), storage_too, true);
        tracing::info!(job = job.0, from = %was, to = %better,
            "job named from its requestor — the NZB carried no name of its own");
        Some(better)
    }

    /// Adopt a name recovered from par2 metadata mid-download.
    ///
    /// The DISPLAY name only: files are already on disk under the storage
    /// name, and moving that out from under the writers is the bug the
    /// `name`/`dir_name` split exists to prevent. Post-processing moves
    /// the finished job into a directory named from the display name, so
    /// what lands in the destination still reads correctly.
    fn adopt_learned_name(&mut self, job: JobId, name: String) {
        let Some(j) = self.state.job_mut(job) else {
            return;
        };
        // The race this closes: between the blocking read and this turn,
        // the job may have been named by its own NZB metadata or by a
        // second par2 file. First real name wins; a rename that flickers
        // is worse than one that is late.
        if !crate::queue::name_is_open(j) {
            return;
        }
        let was = j.name.clone();
        // Final: the recovery set is the document naming itself.
        self.state.set_job_name(job, name.clone(), false, false);
        tracing::info!(job = job.0, from = %was, to = %name,
            "job named from its own par2 metadata");
        self.dirty = true;
        self.publish_now();
        self.bump_epoch();
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
        let (mut status, health) = final_status(job);
        // A write that did not land overrides a healthy article set. The two
        // measure different things: health says the bytes arrived off the
        // wire, this says they reached the disk. Reporting SUCCESS on the
        // strength of the first while the second failed is how a consumer
        // ends up importing 500 MiB of a 48 GiB file.
        if let Some(reason) = self.write_failures.remove(&job_id) {
            status = JobStatus::Failed;
            job.status = status;
            tracing::error!(job = job_id.0, %reason,
                "job failed: the download completed but the files did not");
        } else {
            job.status = status;
        }
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
        let (job_dir, filename) = match self.state.job(job) {
            Some(j) => (
                job_dir_name(j),
                j.files
                    .iter()
                    .find(|f| f.id == file)
                    .map(|f| f.filename.clone())
                    .unwrap_or_else(|| format!("file-{}", file.0)),
            ),
            None => (format!("job-{}", job.0), format!("file-{}", file.0)),
        };
        // `job_dir`, never `job.name`: a job that renamed itself from its
        // par2 metadata mid-download must not split its files across two
        // directories.
        let dir = self.dest_dir.join(job_dir);
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
    ///
    /// Returning 0 here is what repair reads as "nothing left to fetch",
    /// so every path that can return 0 while paused recovery data is
    /// sitting in the queue says why — that branch was silent through
    /// months of PAR_FAILUREs on jobs with 5 GB of recovery blocks on hand.
    fn unpause_par_blocks(&mut self, job_id: JobId, blocks: u32, block_size: Option<u64>) -> u32 {
        let Some(job) = self.state.job_mut(job_id) else {
            tracing::warn!(
                job = job_id.0,
                "delayed par blocks requested for a job that is no longer in the queue"
            );
            return 0;
        };
        let paused_pars = job.files.iter().filter(|f| f.paused && f.is_par2).count();
        if paused_pars == 0 {
            tracing::info!(
                job = job_id.0,
                blocks,
                "no paused par2 files left — every recovery volume this job has is already fetched"
            );
            return 0;
        }
        let (priced, unpriceable) = crate::queue::price_paused_pars(job, block_size);
        let (candidates, last_resort) = if priced.is_empty() {
            // Nothing carried a vol marker and no block size was available:
            // unpause the cheapest volume rather than give up. One round
            // costs one file; the next round prices better or escalates.
            tracing::warn!(
                job = job_id.0,
                blocks,
                paused_pars,
                "no paused par2 file could be priced in recovery blocks (no .volXX+NN marker, \
                 no par2 block size) — unpausing the smallest one as a probe"
            );
            (Vec::new(), crate::queue::smallest_paused_par(job))
        } else {
            if !unpriceable.is_empty() {
                tracing::warn!(
                    job = job_id.0,
                    unpriced = unpriceable.len(),
                    "some paused par2 files could not be priced and were left paused"
                );
            }
            let pairs: Vec<(FileId, u32)> = priced.iter().map(|c| (c.id, c.blocks)).collect();
            (pick_par_files(&pairs, blocks.max(1)), None)
        };
        let estimated = priced.iter().any(|c| c.estimated);
        let mut freed = 0u32;
        for c in &priced {
            if candidates.contains(&c.id) {
                if let Some(f) = job.files.iter_mut().find(|f| f.id == c.id) {
                    f.paused = false;
                    freed += c.blocks;
                }
            }
        }
        if let Some(id) = last_resort {
            if let Some(f) = job.files.iter_mut().find(|f| f.id == id) {
                f.paused = false;
                freed = freed.max(1);
            }
        }
        if freed == 0 {
            tracing::warn!(
                job = job_id.0,
                blocks,
                paused_pars,
                priced = priced.len(),
                "repair asked for recovery blocks and nothing could be unpaused — the paused par2 \
                 files were priced but none were selected"
            );
        } else if estimated {
            tracing::info!(
                job = job_id.0,
                freed,
                block_size,
                "delayed par blocks priced by file size — this post's recovery volumes carry no \
                 .volXX+NN marker"
            );
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
            // A delete whose journal removal AND snapshot save both fail
            // is resurrected by the next restart's recovery. The save
            // failure already logs loudly; this one must too — if a
            // deleted job "came back", this line is the receipt.
            if let Err(e) = self.journal.remove_job(job_id) {
                tracing::error!(
                    job = job_id.0, error = %e,
                    "could not remove the deleted job's journal — if the snapshot save also fails, this delete will not survive a restart"
                );
            }
        }
        if delete_files {
            let dir = self.dest_dir.join(job_dir_name(&job));
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

    /// The prober task's write side, handed out at spawn (lib.rs) so the
    /// statvfs syscall lives on a blocking thread, never in this loop.
    pub(crate) fn disk_free_handle(&self) -> Arc<AtomicU64> {
        self.disk_free.clone()
    }

    /// A write path hit ENOSPC/EDQUOT. Hold intake NOW.
    ///
    /// The statvfs prober is a forecast, and on a quota-backed mount it
    /// can be wrong for hours: nuc3 answered `disk_low: false` and kept
    /// downloading at wire speed while writers, finalize and PP were all
    /// being told there was no space — 725 GB burned in a day. A failed
    /// write is not a forecast, so it does not go through the threshold:
    /// it latches the guard directly and only a statvfs reading of twice
    /// the floor (or an operator resume) clears it.
    pub(crate) fn observe_out_of_space(&mut self, whence: &str) {
        self.enospc_observed += 1;
        self.enospc_where = Some(whence.to_string());
        if !self.enospc_latched {
            self.enospc_latched = true;
            tracing::error!(
                whence,
                observed = self.enospc_observed,
                "out of space on the destination volume — holding all downloads (observed from a \
                 write, not from the free-space probe)"
            );
        }
        if !self.disk_low {
            self.disk_low = true;
            self.publish_now();
        }
    }

    /// Operator resume clears the latch: they have been told what
    /// happened, and only they can know the volume is usable again when
    /// statvfs is the thing that is lying.
    pub(crate) fn clear_enospc_latch(&mut self) {
        if self.enospc_latched {
            self.enospc_latched = false;
            tracing::info!(
                observed = self.enospc_observed,
                "out-of-space latch cleared by the operator"
            );
            self.update_guards();
            self.publish_now();
        }
    }

    fn update_guards(&mut self) {
        // Hysteresis: a mount that lied once has to prove itself with room
        // to spare before intake resumes, so a volume hovering at the
        // floor cannot flap the whole fleet. With no floor configured the
        // latch is the operator's to clear.
        if self.enospc_latched {
            let free = self.disk_free.load(Ordering::Relaxed);
            let clear_at = self.tuning.min_free_disk_bytes.saturating_mul(2);
            if clear_at > 0 && free >= clear_at {
                self.enospc_latched = false;
                tracing::info!(
                    free,
                    clear_at,
                    "out-of-space latch cleared — the volume reports twice the floor free"
                );
            }
        }
        let below_floor = self.tuning.min_free_disk_bytes > 0
            && self.disk_free.load(Ordering::Relaxed) < self.tuning.min_free_disk_bytes;
        let was = self.disk_low;
        self.disk_low = self.enospc_latched || below_floor;
        if self.disk_low != was {
            if self.disk_low {
                tracing::warn!(
                    free = self.disk_free.load(Ordering::Relaxed),
                    floor = self.tuning.min_free_disk_bytes,
                    observed_enospc = self.enospc_observed,
                    "destination volume low on space — downloads held"
                );
            } else {
                tracing::info!(
                    free = self.disk_free.load(Ordering::Relaxed),
                    "disk space recovered — downloads resume"
                );
            }
            self.publish_now();
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
        let tick_started = Instant::now();
        self.guard_tick = self.guard_tick.wrapping_add(1);
        self.settle_download_labels();
        // Guards on the FIRST tick and every 10th after (== 1, not == 0:
        // a fresh boot should know it is out of disk within a second, not
        // ten). NOT `is_multiple_of`: stabilized in 1.87, MSRV is 1.85.
        let mut guards_ms = 0u64;
        if self.guard_tick % 10 == 1 {
            let t = Instant::now();
            self.update_guards();
            guards_ms = t.elapsed().as_millis() as u64;
        }
        let mut volumes_ms = 0u64;
        if self.guard_tick % 30 == 0 {
            let t = Instant::now();
            self.volumes.save_if_dirty();
            volumes_ms = t.elapsed().as_millis() as u64;
        }

        // ONE wire measurement. The drain is stamped with the wall time it
        // covers, per-job and per-server EMAs fold the same bytes at the
        // same alpha, and the header rate is the SUM of the per-server
        // EMAs — so the tile, the chips and the rows can only disagree by
        // integer rounding, at any tick cadence (field report 2026-07-26:
        // 24.8 MiB/s header vs 9.9 rows was the old ring-vs-EMA split,
        // inflated by fsync-delayed ticks counting >1 s of bytes as 1 s).
        let drained = self.meter.drain();
        crate::rate::fold_wire_ema(&mut self.job_wire_ema, &drained.per_job, drained.secs);
        crate::rate::fold_wire_ema(&mut self.server_wire_ema, &drained.per_server, drained.secs);
        // Ghost entries (removed servers) decay; drop them below display
        // resolution so they cannot linger in the snapshot forever.
        self.server_wire_ema.retain(|_, v| *v > 0.4);
        let rate: u64 = self
            .server_wire_ema
            .values()
            .map(|e| e.max(0.0) as u64)
            .sum();

        let now = Instant::now();
        let before = self.blocked.len();
        self.blocked.retain(|_, until| *until > now);
        if self.blocked.len() != before {
            self.bump_epoch(); // blocked servers came back: hand out work
        }

        // Publish BEFORE the durability work. Journal fsync and snapshot
        // saves can block for seconds on a slow state volume, and the read
        // model the dashboard lives on must not queue behind them — that
        // was the "page shows nothing for a while, then everything jumps"
        // field report.
        let t = Instant::now();
        self.publish_snapshot(rate);
        let publish_ms = t.elapsed().as_millis() as u64;

        let sync_started = Instant::now();
        if let Err(e) = self.journal.sync() {
            tracing::error!(error = %e, "journal fsync failed");
        }
        let sync_ms = sync_started.elapsed().as_millis() as u64;

        let t = Instant::now();
        let pending = std::mem::take(&mut self.pending_finalize);
        for (j, f) in pending {
            if !self.finalize_sent.contains(&f) {
                self.send_finalize(j, f);
            }
        }
        let finalize_ms = t.elapsed().as_millis() as u64;

        let mut save_ms = 0u64;
        if self.dirty && self.last_save.elapsed() > save_spacing(self.last_save_ms) {
            let save_started = Instant::now();
            self.save_snapshot();
            save_ms = save_started.elapsed().as_millis() as u64;
        }

        // A tick that runs long stalls everything behind this loop:
        // commands answer late (a delete can time out client-side and its
        // row spring back), lease handout starves (throughput dives), and
        // publishes queue (the UI goes quiet, then jumps). Every section is
        // timed separately — the first field report under this warn had the
        // stall hiding in the ONE untimed section (a statvfs against a
        // saturated destination volume), so nothing here goes unmeasured.
        let total_ms = tick_started.elapsed().as_millis() as u64;
        if total_ms > 1500 {
            tracing::warn!(
                total_ms,
                guards_ms,
                volumes_ms,
                publish_ms,
                journal_sync_ms = sync_ms,
                finalize_ms,
                snapshot_save_ms = save_ms,
                "engine tick ran long — the breakdown names the stall; commands and UI updates queue behind this"
            );
        }
    }

    fn publish_now(&mut self) {
        let rate = self.shared.load().download_rate_bps;
        self.publish_snapshot(rate);
    }

    /// Publish the recovered queue into the shared snapshot *before* the
    /// async run loop is scheduled, so the very first API read reflects
    /// real state — otherwise `/api/v1/jobs` can serve the empty initial
    /// snapshot in the startup window and the UI flashes "queue is empty".
    pub(crate) fn seed_snapshot(&mut self) {
        self.publish_snapshot(0);
    }

    /// URL jobs recovered in `Fetching`: their fetch tasks lived in the
    /// previous process and died with it, so without a re-spawn they sit at
    /// "FETCHING · 0 B of 0 B" forever. [`crate::Engine::spawn`] feeds this
    /// list through [`plan_url_refetches`] right after recovery.
    pub(crate) fn pending_url_fetches(&self) -> Vec<(JobId, String)> {
        self.state
            .jobs
            .iter()
            .filter(|j| matches!(j.status, JobStatus::Fetching))
            .filter_map(|j| {
                j.params
                    .iter()
                    .find(|(k, _)| k == "*URL")
                    .map(|(_, url)| (j.id, url.clone()))
            })
            .collect()
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
                    params: j
                        .params
                        .iter()
                        .filter(|(k, _)| !k.starts_with('*'))
                        .cloned()
                        .collect(),
                    rate_bps: 0,
                    retried_articles: 0,
                    stages: j.stages.clone(),
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
                    // Local jobs: wire-fed EMA — the SAME bytes the header
                    // meter counts, attributed per job, so the row rate and
                    // the header rate can never structurally disagree
                    // (completed-article deltas lag the wire by whatever is
                    // being retried, which read as a stale, slower row).
                    // Delegated jobs have no local wire; their heartbeat
                    // byte deltas keep using the completed-bytes meter.
                    match self.job_wire_ema.get(&summary.id.0) {
                        Some(ema) if summary.assigned_node.is_none() => ema.max(0.0) as u64,
                        _ => meter.update(summary.downloaded_bytes),
                    }
                } else {
                    // Reset the baseline so a resume doesn't spike.
                    meter.last_bytes = summary.downloaded_bytes;
                    meter.ema_bps = 0.0;
                    0
                };
                summary.retried_articles =
                    self.retry_counts.get(&summary.id.0).copied().unwrap_or(0);
            }
            let live: HashSet<u32> = jobs.iter().map(|s| s.id.0).collect();
            self.job_rates.retain(|id, _| live.contains(id));
            self.job_wire_ema.retain(|id, _| live.contains(id));
            self.retry_counts.retain(|id, _| live.contains(id));
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
            enospc_observed: self.enospc_observed,
            enospc_where: self.enospc_where.clone(),
            blocked_servers,
            health_abort: self.tuning.health_abort,
            server_volumes: {
                let now_day = unix_now().div_euclid(86_400);
                let name_of = |id: u32| {
                    self.servers
                        .iter()
                        .find(|s| s.id.0 == id)
                        .map(|s| s.name.clone())
                        .unwrap_or_else(|| format!("server {id}"))
                };
                // Every configured server appears, even one that has never
                // moved a byte: "which provider is quiet?" is exactly the
                // question these rows exist to answer.
                let mut ids: std::collections::BTreeSet<u32> =
                    self.servers.iter().map(|s| s.id.0).collect();
                ids.extend(self.volumes.doc().servers.keys().copied());
                ids.extend(self.server_wire_ema.keys().copied());
                let mut v: Vec<crate::snapshot::ServerVolume> = ids
                    .into_iter()
                    .map(|id| {
                        let w = self.volumes.doc().servers.get(&id).cloned();
                        crate::snapshot::ServerVolume {
                            server: id,
                            name: name_of(id),
                            total_bytes: w.as_ref().map(|w| w.total_bytes).unwrap_or(0),
                            day_bytes: w
                                .as_ref()
                                .filter(|w| w.day_key == now_day)
                                .map(|w| w.day_bytes)
                                .unwrap_or(0),
                            month_bytes: w.as_ref().map(|w| w.month_bytes).unwrap_or(0),
                            rate_bps: self
                                .server_wire_ema
                                .get(&id)
                                .map(|e| e.max(0.0) as u64)
                                .unwrap_or(0),
                        }
                    })
                    .collect();
                v.sort_by_key(|x| x.server);
                v
            },
            speed_limit_bps: self.state.speed_limit_bps,
            max_active_downloads: self.state.max_active_downloads,
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
        let write_started = Instant::now();
        let result = match &self.persist_guard {
            Some(g) => {
                let g = g.clone();
                self.snap_store.save_guarded(&doc, &move || g())
            }
            None => self.snap_store.save(&doc),
        };
        let bytes = match result {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "snapshot save failed (fenced or io); demoting persistence until re-adopted");
                if matches!(e, nzbd_state::StateError::Corrupt(_)) {
                    self.persist = false; // deposed: stop writing authority state
                }
                return;
            }
        };
        // Duration and throughput, remembered for the adaptive spacing and
        // said out loud when slow — "how fast is the state volume really?"
        // must be answerable from the log, with numbers.
        let ms = write_started.elapsed().as_millis() as u64;
        self.last_save_ms = ms;
        let mib_s = (bytes as f64 / (1 << 20) as f64) / (ms.max(1) as f64 / 1000.0);
        if ms > 1000 {
            tracing::warn!(
                bytes,
                ms,
                throughput_mib_s = format!("{mib_s:.1}"),
                "snapshot save is slow — saves are spaced out to compensate (the journal still protects progress)"
            );
        } else {
            tracing::debug!(bytes, ms, "snapshot saved");
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

/// How long after a save the next debounced save may run: 10× the last
/// save's duration, floored at the historical 2 s, capped at 5 min. A
/// fast volume keeps the old cadence; a slow one gets its saves spaced so
/// the owner loop spends at most ~10% of wall time blocked in them
/// (field report 2026-07-26: 65 s saves fired back-to-back and consumed
/// the loop — commands, publishes and the dashboard all queued behind
/// storage). Stretching the cadence risks no data: every completed
/// segment is in the fsync'd journal, and recovery is snapshot + replay.
/// Structural saves (delete, import) stay immediate — they don't come
/// through the debounce.
fn save_spacing(last_save_ms: u64) -> Duration {
    Duration::from_millis(last_save_ms.saturating_mul(10).clamp(2_000, 300_000))
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

/// A recovered-`Fetching` job that duplicates another job's URL: `job`
/// should fail as a duplicate of `original`.
pub(crate) type DupeOf = (JobId, JobId);

/// Split recovered `Fetching` jobs into fetches to re-spawn and duplicates
/// to fail. The first job (queue order) of each URL refetches; later
/// same-URL jobs — the pile-up left by clients re-adding while the original
/// fetch was invisible after a restart — fail as duplicates of it, instead
/// of N copies of the same download racing to completion.
pub(crate) fn plan_url_refetches(
    pending: Vec<(JobId, String)>,
) -> (Vec<(JobId, String)>, Vec<DupeOf>) {
    let mut first_for_url: HashMap<String, JobId> = HashMap::new();
    let mut refetch = Vec::new();
    let mut duplicates = Vec::new();
    for (job, url) in pending {
        match first_for_url.get(&url) {
            None => {
                first_for_url.insert(url.clone(), job);
                refetch.push((job, url));
            }
            Some(original) => duplicates.push((job, *original)),
        }
    }
    (refetch, duplicates)
}

/// The recovery-set name a par2 file implies, or `None` if this file is
/// not a par2 file or its packets name nothing useful.
///
/// Reads the head first so that the overwhelmingly common case — a payload
/// file, not a par2 file — costs 8 bytes rather than a whole rar volume.
fn read_par2_name(path: &Path) -> Option<String> {
    use std::io::Read as _;
    let mut f = std::fs::File::open(path).ok()?;
    let mut head = [0u8; 8];
    let mut got = 0;
    while got < head.len() {
        match f.read(&mut head[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => return None,
        }
    }
    if !nzbd_par2::is_par2(&head[..got]) {
        return None;
    }
    // par2 index files are small (job #182's was 20.5 KiB). Cap the read
    // anyway: a recovery VOLUME carries the same FileDesc packets up front
    // and can be hundreds of MiB, and we only ever want its header.
    const MAX: u64 = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(MAX)
        .read_to_end(&mut bytes)
        .ok()?;
    crate::queue::name_from_par2(&nzbd_par2::scan(&bytes).descs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bare_job() -> Job {
        Job {
            id: JobId(1),
            kind: nzbd_types::JobKind::Nzb,
            name: "x".into(),
            dir_name: String::new(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![],
            files: vec![],
            totals: Default::default(),
            status: JobStatus::PostQueued,
            stages: vec![],
        }
    }

    /// The monotonic figure the post manager measured is what lands on the
    /// span. Wall-clock is a fallback, not the primary — an NTP step
    /// during a long repair must not produce a duration nobody can trust.
    #[test]
    fn close_span_prefers_the_measured_duration() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::ParRepair,
            started_at_unix: 1_000,
            ms: None,
        });
        // Wall clock says 60s; the manager measured 4500ms.
        close_span(&mut j, Some(4500), 1_060);
        assert_eq!(j.stages[0].ms, Some(4500));
    }

    /// A span that outlived the process has no `Instant` left to consult,
    /// so the seconds that survived in the snapshot are all there is.
    #[test]
    fn close_span_falls_back_to_wall_clock() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::Unpack,
            started_at_unix: 1_000,
            ms: None,
        });
        close_span(&mut j, None, 1_007);
        assert_eq!(j.stages[0].ms, Some(7_000));
    }

    /// `Stages::finish` closes the span and `Drop` closes it again. The
    /// second close must not overwrite the first — otherwise every job's
    /// last stage would report the few milliseconds between the two.
    #[test]
    fn close_span_is_idempotent() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::Move,
            started_at_unix: 1_000,
            ms: None,
        });
        close_span(&mut j, Some(9_000), 1_009);
        close_span(&mut j, Some(3), 1_009);
        assert_eq!(j.stages[0].ms, Some(9_000));
    }

    /// A clock that went backwards between entering and leaving a stage
    /// yields 0, never a wrapped-around u64.
    #[test]
    fn close_span_survives_a_backwards_clock() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::ParVerify,
            started_at_unix: 2_000,
            ms: None,
        });
        close_span(&mut j, None, 1_900);
        assert_eq!(j.stages[0].ms, Some(0));
    }

    #[test]
    fn close_span_on_a_job_that_never_post_processed() {
        let mut j = bare_job();
        close_span(&mut j, Some(5), 1_000);
        assert!(j.stages.is_empty());
    }

    #[test]
    fn url_refetch_plan_keeps_first_fails_dupes() {
        let a = "https://indexer.example/getnzb/aaa.nzb&i=1&r=k".to_string();
        let b = "https://indexer.example/getnzb/bbb.nzb&i=1&r=k".to_string();
        let pending = vec![
            (JobId(4), a.clone()),
            (JobId(5), a.clone()),
            (JobId(7), b.clone()),
            (JobId(9), a.clone()),
        ];
        let (refetch, dupes) = plan_url_refetches(pending);
        assert_eq!(refetch, vec![(JobId(4), a), (JobId(7), b)]);
        assert_eq!(dupes, vec![(JobId(5), JobId(4)), (JobId(9), JobId(4))]);
    }

    #[test]
    fn url_refetch_plan_empty() {
        let (refetch, dupes) = plan_url_refetches(Vec::new());
        assert!(refetch.is_empty());
        assert!(dupes.is_empty());
    }

    #[test]
    fn save_spacing_scales_with_save_cost() {
        // Fast volume: the historical 2 s cadence.
        assert_eq!(save_spacing(0), Duration::from_secs(2));
        assert_eq!(save_spacing(120), Duration::from_secs(2));
        // A 1 s save gets 10 s between saves (~10% duty cycle)…
        assert_eq!(save_spacing(1_000), Duration::from_secs(10));
        // …the field report's 65 s save gets spaced way out…
        assert_eq!(save_spacing(65_000), Duration::from_secs(300));
        // …and the cap holds even for absurd values.
        assert_eq!(save_spacing(u64::MAX / 20), Duration::from_secs(300));
    }
}
