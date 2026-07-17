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
    final_status, next_for_server, recompute_job_totals, sanitize_name, QueueState, SegRef,
    SelectionCtx,
};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::snapshot::{JobSummary, QueueSnapshot, SharedSnapshot};
use crate::writer::{spawn_writer, WriteCmd, WriterHandle};
use crate::Tuning;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{FsJournal, JournalRecord, SnapshotStore, UncleanMarker};
use nzbd_types::{
    FileId, Health, JobId, JobStatus, SegmentState, ServerDef, ServerId,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
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
        reply: oneshot::Sender<JobId>,
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

    journal: FsJournal,
    snap_store: SnapshotStore,
    marker: UncleanMarker,

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
}

#[allow(clippy::too_many_arguments)]
impl Owner {
    /// Synchronous construction incl. crash recovery: load snapshot, replay
    /// journal, fold, arm the unclean marker.
    pub(crate) fn recover(
        state_dir: &std::path::Path,
        dest_dir: PathBuf,
        servers: Arc<Vec<ServerDef>>,
        tuning: Tuning,
        shared: SharedSnapshot,
        events: broadcast::Sender<Event>,
        epoch_tx: watch::Sender<u64>,
        meter: Arc<SpeedMeter>,
        limiter: Arc<RateLimiter>,
        engine_tx: mpsc::Sender<EngineMsg>,
        tracker: TaskTracker,
        cancel: CancellationToken,
    ) -> Result<Owner, nzbd_state::StateError> {
        let marker = UncleanMarker::new(state_dir);
        let was_unclean = marker.check_and_arm()?;
        let snap_store = SnapshotStore::open(state_dir)?;
        let journal = FsJournal::open(state_dir)?;

        let mut state = match snap_store.load()? {
            Some(doc) => QueueState::from_doc(doc),
            None => QueueState::default(),
        };

        let mut file_sizes = HashMap::new();
        let replayed = journal.replay()?;
        let replay_count = replayed.len();
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

        if was_unclean || replay_count > 0 {
            tracing::info!(
                was_unclean,
                journal_records = replay_count,
                jobs = state.jobs.len(),
                "recovered queue state"
            );
        }

        // Restore the persisted speed limit.
        limiter.set(state.speed_limit_bps);

        Ok(Owner {
            state,
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            finalize_sent: HashSet::new(),
            pending_finalize: Vec::new(),
            file_sizes,
            journal,
            snap_store,
            marker,
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
                self.on_segment_written(job, file, seg_number, offset, len, crc, file_size)
            }
            EngineMsg::SegmentFailed {
                job,
                file,
                seg_number,
                server,
                outcome,
            } => self.on_segment_failed(SegRef { job, file, seg_number }, server, outcome),
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
                reply,
            } => {
                let id = self.state.admit_nzb(
                    name.clone(),
                    &parsed,
                    category,
                    priority,
                    self.tuning.pause_extra_pars,
                );
                tracing::info!(job = id.0, %name, "job added");
                self.save_snapshot(); // adds are durable immediately
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send(id);
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
        }
    }

    // -- scheduling ----------------------------------------------------------

    fn grant_work(&mut self, server_id: ServerId, max: usize) -> Vec<Lease> {
        if self.is_blocked(server_id) {
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
                article_retries: self.tuning.article_retries,
                now_unix: unix_now(),
                propagation_delay_secs: self.tuning.propagation_delay.as_secs() as i64,
            };
            let result = next_for_server(&self.state, server, &mut ctx);
            let exhausted = result.exhausted;
            let lease = result.lease;
            for r in exhausted {
                self.fail_segment(r);
            }
            let Some(r) = lease else { break };

            let (message_id, writer) = {
                let Some(seg) = self.state.segment_mut(r) else { break };
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

    fn on_segment_written(
        &mut self,
        job: JobId,
        file: FileId,
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
    ) {
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
        tracing::debug!(job = r.job.0, file = r.file.0, seg = r.seg_number, "segment exhausted");
        self.emit(Event::SegmentExhausted {
            job: r.job,
            file: r.file,
            segment: r.seg_number,
        });
        self.after_file_change(r.job, r.file);
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
            .unwrap_or_else(|| segs.iter().map(|(o, l, _)| o + *l as u64).max().unwrap_or(0));

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
            && job.files.iter().all(|f| {
                f.paused || (f.is_terminal() && (!f.has_any_done() || f.finalized))
            });
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
            tracing::debug!(server = server.0, secs = self.tuning.retry_interval.as_secs(), "server blocked");
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

    fn on_tick(&mut self) {
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
                JobSummary {
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
                }
            })
            .collect();
        let snap = QueueSnapshot {
            up_since_unix: self.up_since_unix,
            download_paused: self.state.download_paused,
            speed_limit_bps: self.state.speed_limit_bps,
            download_rate_bps: rate,
            session_downloaded_bytes: self.meter.total(),
            remaining_bytes: self.state.remaining_bytes(),
            jobs,
        };
        self.shared.store(Arc::new(snap));
    }

    fn save_snapshot(&mut self) {
        if let Err(e) = self.snap_store.save(&self.state.to_doc()) {
            tracing::error!(error = %e, "snapshot save failed");
            return;
        }
        if let Err(e) = self.journal.compact() {
            tracing::error!(error = %e, "journal compact failed");
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
