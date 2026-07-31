//! The post-processing orchestrator + manager (ARCHITECTURE.md §9).
//!
//! The manager watches the engine for finished downloads and drives each
//! job through the stage graph: par verify (native quick path first) →
//! repair (with delayed-par fetching) → unpack → cleanup → scripts —
//! recording the outcome in history and stamping the job so restarts never
//! re-process. Stage parallelism follows `PostStrategy`.

use crate::rename::{par_rename, rar_rename};
use crate::script::{discover, ScriptHost};
use crate::tools::{detect_archives, Extractors, Par2Tool};
use crate::{par2, DownloadEvidence, PostError, RepairResult, VerifyResult};
use nzbd_engine::{EngineHandle, Event};
use nzbd_state::history::HistoryDb;
use nzbd_state::HistoryEntry;
use nzbd_types::metrics::PpStageStats;
use nzbd_types::{Health, Job, JobId, JobStatus, PostStage};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Param stamped onto a job once post-processing finished (defined in
/// `nzbd-types` so the engine snapshot and cluster scheduler see it too).
pub use nzbd_types::PP_DONE_PARAM;

/// What to do with the FILES of a job that ended in a terminal failure —
/// par failure, unpack failure, script failure, health abort, post crash.
///
/// Was `HealthAction` and governed the health gate alone, which left
/// every *other* terminal failure's directory sitting in the category
/// tree an importer watches, forever. Nothing ever removed them: ~523 GB
/// of known-bad downloads in one field report
/// (docs/REGRAB_LOOP_PLAN.md D2).
///
/// `Delete` is the default. The bytes are known-bad by definition, and
/// the job's NZB is parked with its history entry, so `requeue` fetches
/// them again — deleting loses nothing but the disk space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailureAction {
    /// Leave the files where they are. What produced the terabyte.
    None,
    Park,
    #[default]
    Delete,
}

impl FailureAction {
    pub fn parse(s: &str) -> FailureAction {
        match s.to_ascii_lowercase().as_str() {
            "none" | "keep" | "" => FailureAction::None,
            "park" | "pause" => FailureAction::Park,
            _ => FailureAction::Delete,
        }
    }
}

/// What a `[[category]]` block actually changes about post-processing.
///
/// These keys were parsed and advertised to compat clients as
/// `CategoryN.DestDir` / `CategoryN.Unpack` long before anything applied
/// them, so an operator who set a per-category destination got files
/// somewhere else and an *arr that path-mapped off the advertised value
/// found nothing. Advertised must equal actual; that is what this type
/// carries into the pipeline.
#[derive(Debug, Clone, Default)]
pub struct CategoryRule {
    pub name: String,
    /// Completed files are moved here (under `<dest_dir>/<job name>`)
    /// instead of the global destination.
    pub dest_dir: Option<PathBuf>,
    /// Per-category override of `[post] unpack`.
    pub unpack: Option<bool>,
    /// Extension scripts to run for this category, by file name or stem.
    /// Empty = every discovered script, which is the global behavior.
    pub extensions: Vec<String>,
}

#[derive(Clone)]
pub struct PostConfig {
    pub par2_cmd: String,
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    pub scripts_dir: Option<PathBuf>,
    pub unpack: bool,
    pub cleanup: bool,
    /// Rename still-obfuscated files to the job name after unpack
    /// (SABnzbd-style; fully obfuscated season packs get `<job> - NN`).
    pub deobfuscate_final: bool,
    /// What happens to the files of a job that ends in a terminal
    /// failure (par, unpack, script, health abort, crash).
    pub failure_action: FailureAction,
    /// Where `FailureAction::Park` moves a failed job's directory.
    /// `None` falls back to a `.failed` dir beside the downloads, which
    /// is at least out of an importer's way (dot-prefixed).
    pub failed_dir: Option<PathBuf>,
    /// Concurrent PP jobs (PostStrategy: sequential=1, balanced=2,
    /// aggressive=3, rocket=6).
    pub slots: usize,
    pub tool_timeout: Duration,
    pub script_timeout: Duration,
    /// How long to wait for delayed par files to download.
    pub par_fetch_timeout: Duration,
    /// `[[category]]` rules, matched against the job's category name.
    pub categories: Vec<CategoryRule>,
    /// Stage-duration counters for `/metrics`, when the daemon wired one.
    pub stats: Option<Arc<PpStageStats>>,
}

impl Default for PostConfig {
    fn default() -> Self {
        PostConfig {
            par2_cmd: "par2".into(),
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            scripts_dir: None,
            unpack: true,
            cleanup: true,
            deobfuscate_final: true,
            failure_action: FailureAction::default(),
            failed_dir: None,
            slots: 1,
            tool_timeout: Duration::from_secs(3600),
            script_timeout: Duration::from_secs(3600),
            par_fetch_timeout: Duration::from_secs(600),
            categories: Vec::new(),
            stats: None,
        }
    }
}

impl PostConfig {
    /// The `[[category]]` rule for a job's category, if one is configured.
    /// Matched case-insensitively, like the compat shim's category
    /// handling — an *arr sending "TV" must land on `name = "tv"`.
    fn rule_for(&self, category: Option<&str>) -> Option<&CategoryRule> {
        let want = category?.trim();
        if want.is_empty() {
            return None;
        }
        self.categories
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(want))
    }
}

pub fn strategy_slots(name: &str) -> usize {
    match name {
        "balanced" => 2,
        "aggressive" => 3,
        "rocket" => 6,
        _ => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PpFinal {
    Success,
    ParFailure,
    UnpackFailure,
    ScriptFailure,
}

impl PpFinal {
    pub fn as_str(&self) -> &'static str {
        match self {
            PpFinal::Success => "SUCCESS",
            PpFinal::ParFailure => "PAR_FAILURE",
            PpFinal::UnpackFailure => "UNPACK_FAILURE",
            PpFinal::ScriptFailure => "SCRIPT_FAILURE",
        }
    }
}

/// Per-job claim gate. `None` = process everything (single node). In
/// cluster mode the closure consults election state + the PP assignment
/// the leader scheduler made (C2 anti-affinity): a node only processes
/// jobs assigned to it, and only the leader records health-failures.
pub type PpGate = Option<Arc<dyn Fn(JobId) -> bool + Send + Sync>>;

/// Fencing context for one PP execution (CLUSTERING.md §6.4): `tag` names
/// the staging dir (`.pp.<tag>/`) unpack extracts into, and `commit_ok`
/// is re-checked immediately before every commit rename and before the
/// final stamp — a lease that was cancelled or reclaimed must not publish.
#[derive(Clone)]
pub struct PpCtx {
    pub tag: String,
    pub commit_ok: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl Default for PpCtx {
    fn default() -> Self {
        PpCtx {
            tag: "local".into(),
            commit_ok: Arc::new(|| true),
        }
    }
}

/// How often the manager rescans the queue for un-processed finished jobs
/// (covers leadership takeover, lagged event streams, crashed PP attempts
/// on a prior run — the `*PP:done` stamp keeps it idempotent).
const RESCAN_INTERVAL: Duration = Duration::from_secs(30);

pub fn spawn_post_manager(
    engine: EngineHandle,
    cfg: PostConfig,
    history: Arc<HistoryDb>,
    dest_dir: PathBuf,
    gate: PpGate,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    let t2 = tracker.clone();
    tracker.spawn(manager_task(
        engine, cfg, history, dest_dir, gate, cancel, t2,
    ));
}

async fn manager_task(
    engine: EngineHandle,
    cfg: PostConfig,
    history: Arc<HistoryDb>,
    dest_dir: PathBuf,
    gate: PpGate,
    cancel: CancellationToken,
    tracker: TaskTracker,
) {
    let mut rx = engine.subscribe();
    let mut queued: HashSet<JobId> = HashSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.slots.max(1)));
    let claim = |g: &PpGate, job: JobId| g.as_ref().map(|f| f(job)).unwrap_or(true);

    // Startup scan + periodic rescan: finished downloads that were never
    // post-processed (crash between completion and PP, or authority adopted
    // from a dead leader mid-way).
    let mut rescan = tokio::time::interval(RESCAN_INTERVAL);
    rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = rescan.tick() => {
                scan_queue(&tracker, &engine, &cfg, &history, &dest_dir, &gate, &sem, &cancel, &mut queued).await;
            }
            ev = rx.recv() => match ev {
                Ok(Event::JobFinished { job, status, name, health }) => match status {
                    JobStatus::Completed if claim(&gate, job) => {
                        if let Ok(Some(exported)) = engine.export_job(job).await {
                            if !pp_done(&exported) && queued.insert(job) {
                                spawn_job(
                                    &tracker,
                                    &engine,
                                    &cfg,
                                    &history,
                                    &dest_dir,
                                    &sem,
                                    &cancel,
                                    job,
                                    gate.is_none(),
                                );
                            }
                        }
                    }
                    JobStatus::Failed if claim(&gate, job) => {
                        // Below critical health: no PP; record history and
                        // stamp so the retire sweep moves the job out.
                        let exported = engine.export_job(job).await.ok().flatten();
                        if exported.as_ref().is_some_and(pp_done) {
                            continue; // already handled (rescan overlap)
                        }
                        let fetch_failure = exported
                            .as_ref()
                            .map(|j| matches!(j.kind, nzbd_types::JobKind::Url) && j.files.is_empty())
                            .unwrap_or(false);
                        let fail_status = if fetch_failure {
                            "FAILURE/FETCH"
                        } else {
                            "FAILURE/HEALTH"
                        };
                        // Same disposition as every other terminal
                        // failure: the health gate is not a special case,
                        // it is just the earliest one.
                        let disposition = match exported.as_ref() {
                            Some(j) if !j.files.is_empty() => {
                                let dir_name = nzbd_engine::queue::job_dir_name(j);
                                Some(dispose_failed(
                                    &cfg,
                                    job,
                                    &dest_dir.join(&dir_name),
                                    &dest_dir,
                                    &dir_name,
                                    "local",
                                ).await)
                            }
                            _ => None,
                        };
                        let mut fail_params =
                            exported.as_ref().map(user_params).unwrap_or_default();
                        if let Some(d) = disposition.as_ref() {
                            fail_params.push(("Failure:Files".into(), d.note.clone()));
                        }
                        let entry = HistoryEntry {
                            job,
                            name: name.clone(),
                            category: exported.as_ref().and_then(|j| j.category.clone()),
                            final_dir: disposition
                                .as_ref()
                                .and_then(|d| d.files_at.as_ref())
                                .map(|p| p.to_string_lossy().into_owned()),
                            status: fail_status.into(),
                            size: exported.as_ref().map(|j| j.totals.size).unwrap_or(0),
                            health,
                            params: fail_params.clone(),
                            dupe_key: exported
                                .as_ref()
                                .map(|j| j.dupe.key.clone())
                                .unwrap_or_default(),
                            dupe_score: exported.as_ref().map(|j| j.dupe.score).unwrap_or(0),
                            completed_at_unix: now(),
                            hidden: false,
                            first_seen_at_unix: None,
                            last_seen_at_unix: None,
                            seen_count: 0,
                            removed_at_unix: None,
                            picked_up_by: None,
                            record: exported.as_ref().map(nzbd_state::JobRecord::from_job),
                            stages: exported
                                .as_ref()
                                .map(|j| j.stages.clone())
                                .unwrap_or_default(),
                            seq: 0,
                        };
                        let h = history.clone();
                        let record = entry.clone();
                        let history_seq =
                            tokio::task::spawn_blocking(move || h.record_seq(&record))
                                .await
                                .ok()
                                .and_then(|r| r.ok())
                                .unwrap_or(0);
                        // These jobs never enter the stage pipeline, but they
                        // ARE terminal, and a consumer waiting for
                        // `job_pp_finished` must not wait forever on a
                        // download that died at the health gate. The status
                        // string is the history one verbatim
                        // (`FAILURE/HEALTH` / `FAILURE/FETCH`) — same
                        // FAILURE prefix consumers already switch on.
                        engine.emit(Event::JobPpFinished {
                            job,
                            name,
                            category: entry.category.clone(),
                            pp_status: fail_status.into(),
                            final_dir: None,
                            size_bytes: entry.size,
                            health,
                            params: entry.params.clone(),
                            history_seq,
                        });
                        if let Some(mut fin) = exported {
                            if let Some(d) = disposition.as_ref() {
                                fin.params.push(("Failure:Files".into(), d.note.clone()));
                            }
                            fin.params.push((PP_DONE_PARAM.into(), fail_status.into()));
                            let _ = engine.import_job(fin, false, false).await;
                        }
                    }
                    _ => {}
                },
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            },
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_queue(
    tracker: &TaskTracker,
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    gate: &PpGate,
    sem: &Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
    queued: &mut HashSet<JobId>,
) {
    let claim = |job: JobId| gate.as_ref().map(|f| f(job)).unwrap_or(true);
    for j in engine.snapshot().jobs.iter() {
        // NZBGet parity: finished-and-processed jobs live in history, not
        // the queue. Single-node the manager IS the authority; in cluster
        // mode the leader sweep retires (the gate stays out of hygiene).
        if gate.is_none()
            && j.pp_done
            && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
        {
            let _ = engine.remove_job_silent(j.id).await;
            queued.remove(&j.id);
            continue;
        }
        let needs_pp = !j.pp_done
            && matches!(
                j.status,
                JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
            );
        if needs_pp && claim(j.id) && queued.insert(j.id) {
            spawn_job(
                tracker,
                engine,
                cfg,
                history,
                dest_dir,
                sem,
                cancel,
                j.id,
                gate.is_none(),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_job(
    tracker: &TaskTracker,
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    sem: &Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
    job: JobId,
    retire_local: bool,
) {
    let engine = engine.clone();
    let cfg = cfg.clone();
    let history = history.clone();
    let dest = dest_dir.to_path_buf();
    let sem = sem.clone();
    let cancel = cancel.clone();
    tracker.spawn(async move {
        // Cancel-aware at both await points. Without this, one PP job —
        // a par2 repair, an unpack, an extension script — kept the PP
        // tracker's shutdown wait (and with it the whole daemon restart)
        // hostage until it finished: "clicked restart, page never came
        // back" (field report 2026-07-25, second of the day). Aborting is
        // safe by the pipeline's own crash model: no `*PP:done` stamp has
        // been written, subprocesses are kill-on-drop, and the next pass's
        // rescan re-runs the job from scratch.
        let permit = tokio::select! {
            _ = cancel.cancelled() => return, // never START work mid-shutdown
            p = sem.acquire() => match p {
                Ok(p) => p,
                Err(_) => return,
            },
        };
        let _permit = permit;
        let outcome = tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!(
                    job = job.0,
                    "post-processing aborted by shutdown/restart; it will re-run on the next pass"
                );
                return;
            }
            r = process_job(&engine, &cfg, &history, &dest, job) => r,
        };
        match outcome {
            Ok(outcome) => {
                tracing::info!(
                    job = job.0,
                    outcome = outcome.as_str(),
                    "post-processing finished"
                );
                if retire_local {
                    // NZBGet parity: the finished job's record IS the
                    // history entry — move it out of the queue now.
                    let _ = engine.remove_job_silent(job).await;
                }
            }
            Err(e) => {
                // PP writes as much as the downloader does — unpack,
                // move, script output. An ENOSPC here is the same ground
                // truth about the volume and must stop intake too, not
                // just fail this job.
                if nzbd_engine::is_out_of_space(&e.to_string()) {
                    engine.report_out_of_space(format!("post-processing job {}: {e}", job.0));
                }
                tracing::error!(job = job.0, error = %e, "post-processing crashed");
            }
        }
    });
}

fn pp_done(job: &Job) -> bool {
    job.params.iter().any(|(k, _)| k == PP_DONE_PARAM)
}

/// Parameters exposed in history: everything except `*`-internal ones.
fn user_params(job: &Job) -> Vec<(String, String)> {
    job.params
        .iter()
        .filter(|(k, _)| !k.starts_with('*'))
        .cloned()
        .collect()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Evidence for quick verification straight from the engine's export,
/// with paths remapped through any de-obfuscation renames.
fn evidence_of(
    job: &Job,
    dir: &Path,
    renames: &std::collections::HashMap<PathBuf, PathBuf>,
) -> Vec<DownloadEvidence> {
    job.files
        .iter()
        .map(|f| {
            let orig = dir.join(&f.filename);
            let path = renames.get(&orig).cloned().unwrap_or(orig);
            DownloadEvidence {
                path,
                crc32: f.crc32,
                segment_crcs: Vec::new(),
            }
        })
        .collect()
}

/// Drives one job's stage transitions: sets the queue status (as before),
/// publishes `job_pp_stage` on the engine stream, and times each stage for
/// `/metrics`.
///
/// This is the one place stage changes happen, because the three things
/// have to stay in lockstep — a status the UI shows, an event a consumer
/// reacts to and a timing an operator graphs must never disagree about
/// which stage the job is in.
struct Stages<'a> {
    engine: &'a EngineHandle,
    job: JobId,
    name: String,
    stats: Option<Arc<PpStageStats>>,
    current: Option<(PostStage, std::time::Instant)>,
}

impl<'a> Stages<'a> {
    fn new(engine: &'a EngineHandle, job: JobId, name: String, cfg: &PostConfig) -> Stages<'a> {
        Stages {
            engine,
            job,
            name,
            stats: cfg.stats.clone(),
            current: None,
        }
    }

    async fn enter(&mut self, stage: PostStage) {
        let prev_ms = self.close();
        // Status, timeline and event in that order. The first two are one
        // command precisely so a consumer that reacts to the event can
        // read the job back and find the stage already on it.
        let _ = self
            .engine
            .enter_post_stage(self.job, stage, now(), prev_ms)
            .await;
        self.engine.emit(Event::JobPpStage {
            job: self.job,
            name: self.name.clone(),
            stage,
        });
        self.current = Some((stage, std::time::Instant::now()));
    }

    /// Bank the running stage's duration into the process histogram and
    /// hand the same measurement back for the job's own timeline.
    ///
    /// Returning it is the whole point: this figure used to be computed
    /// here, averaged into `PpStageStats`, and dropped — so "how long does
    /// unpack take across every job" was answerable and "how long did
    /// unpack take for THIS job" was not, from the same measurement.
    fn close(&mut self) -> Option<u64> {
        let (stage, started) = self.current.take()?;
        let elapsed = started.elapsed();
        if let Some(stats) = self.stats.as_ref() {
            stats.record(stage, elapsed);
        }
        Some(elapsed.as_millis() as u64)
    }

    /// End the pipeline cleanly: close the last span on the job and wait
    /// for the engine to have applied it.
    ///
    /// Called before the finalize export, so the `Job` that becomes the
    /// history entry carries a *closed* final stage. Leaving it to `Drop`
    /// would be too late by exactly one export — history would show the
    /// last stage of every successful job as still running, which is the
    /// one entry you are most likely to be reading.
    ///
    /// Commands reach the owner task on one ordered channel, so the export
    /// queued after this observes the close. Safe to call more than once.
    async fn finish(&mut self) {
        let ms = self.close();
        self.engine.close_post_stage(self.job, now(), ms).await;
    }
}

impl Drop for Stages<'_> {
    /// A job that returns early (lease lost, tool error, cancelled) still
    /// spent time in its last stage. Closing on drop means "how long does
    /// unpack take" is not silently biased toward the runs that succeeded
    /// — and now that the span is on the job too, an abandoned job's last
    /// stage does not sit in history looking like it never ended.
    ///
    /// Drop cannot await, so the engine half is spawned. It is ordered
    /// behind nothing: the span is already closed in the histogram, and
    /// `close_span` on the owner side is idempotent, so a late arrival
    /// cannot overwrite a good measurement.
    fn drop(&mut self) {
        let ms = self.close();
        let (engine, job) = (self.engine.clone(), self.job);
        tokio::spawn(async move { engine.close_post_stage(job, now(), ms).await });
    }
}

// ---------------------------------------------------------------------------
// The stage pipeline for one job
// ---------------------------------------------------------------------------

pub async fn process_job(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
) -> Result<PpFinal, PostError> {
    process_job_ctx(engine, cfg, history, dest_dir, job_id, &PpCtx::default()).await
}

/// The stage pipeline with explicit fencing (cluster PP leases pass their
/// lease id as the staging tag and a live lease check as `commit_ok`).
pub async fn process_job_ctx(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
    ctx: &PpCtx,
) -> Result<PpFinal, PostError> {
    let _ = engine.set_job_status(job_id, JobStatus::PostQueued).await;
    let Some(job) = engine
        .export_job(job_id)
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
    else {
        return Err(PostError::Subprocess("job vanished".into()));
    };
    // The SOURCE directory is the job's storage name, which never changes;
    // `job.name` is a display name that a job can improve on itself (par2
    // metadata naming an obfuscated post). Deriving the input directory
    // from a mutable name is how the pipeline would come looking for a
    // directory the engine never wrote.
    let sanitized = nzbd_engine::queue::job_dir_name(&job);
    // The engine always writes downloads under the global destination;
    // a `[[category]] dest_dir` is a *move* at the end of the pipeline
    // (PostStage::Move), not a different download target.
    let mut dir = dest_dir.join(&sanitized);
    let rule = cfg.rule_for(job.category.as_deref()).cloned();
    // Re-run after a crash between the move and the `*PP:done` stamp: the
    // files are already at the category destination and the global path is
    // gone. Adopt what is on disk. Without this the whole pipeline runs
    // against a directory that no longer exists — par2 load fails with
    // ENOENT, `process_job` errors, and the job is wedged forever: never
    // stamped, never in history, never announced, never retired from the
    // queue. The stage graph is idempotent, and this is what keeps that
    // true across the one step that moves its own input.
    if let Some(moved) = rule.as_ref().and_then(|r| r.dest_dir.as_ref()) {
        let moved = moved.join(&sanitized);
        if !dir.exists() && moved.is_dir() {
            tracing::info!(
                job = job_id.0,
                dir = %moved.display(),
                "resuming post-processing at the category destination (already moved)"
            );
            dir = moved;
        }
    }
    let unpack_enabled = rule.as_ref().and_then(|r| r.unpack).unwrap_or(cfg.unpack);
    // Superseded staging dirs (a reclaimed lease's leftovers) are garbage
    // by definition — this lease is now the only live executor.
    let staging = dir.join(format!(".pp.{}", ctx.tag));
    remove_stale_staging(&dir, &staging);
    let mut stages = Stages::new(engine, job_id, job.name.clone(), cfg);

    // ---- RENAME stage (par-rename, then rar-rename) ------------------------
    // Obfuscated posts get their real names back before anything verifies
    // or unpacks. Whole-file CRCs are content-addressed, so download
    // evidence just needs its paths remapped.
    stages.enter(PostStage::ParRename).await;
    let mut renames = par_rename(&dir);
    if unpack_enabled {
        stages.enter(PostStage::RarRename).await;
        renames.extend(rar_rename(&dir));
    }
    let rename_map: std::collections::HashMap<PathBuf, PathBuf> = renames.into_iter().collect();

    // ---- PAR stage ---------------------------------------------------------
    let par_tool = Par2Tool {
        cmd: cfg.par2_cmd.clone(),
        timeout: cfg.tool_timeout,
    };
    let mut par_ok = true;
    let mut par_did_repair = false;
    // Names the par2 set vouches for: proven correct, off-limits to the
    // heuristic deobfuscation pass at the end.
    let mut par2_names: std::collections::HashSet<String> = Default::default();
    if let Some(set) = par2::load_dir(&dir)? {
        par2_names = set.files.iter().map(|f| f.name.clone()).collect();
        stages.enter(PostStage::ParVerify).await;
        let quick = par2::quick_verify(&set, &evidence_of(&job, &dir, &rename_map));
        if quick == VerifyResult::Intact {
            tracing::info!(job = job_id.0, "par quick-verify: intact (no data re-read)");
        } else if let Some(main) = set.main_path.clone() {
            par_ok = repair_loop(engine, cfg, &par_tool, &mut stages, job_id, &main).await?;
            par_did_repair = par_ok;
        } else {
            par_ok = false;
        }
    }

    // ---- UNPACK stage ------------------------------------------------------
    let mut unpack_ok = true;
    let mut unpacked_any = false;
    if unpack_enabled {
        let archives = detect_archives(&dir);
        if !archives.is_empty() {
            stages.enter(PostStage::Unpack).await;
            let ex = Extractors {
                unrar_cmd: cfg.unrar_cmd.clone(),
                sevenzip_cmd: cfg.sevenzip_cmd.clone(),
                timeout: cfg.tool_timeout,
            };
            let password = job
                .params
                .iter()
                .find(|(k, _)| k == "*Unpack:Password")
                .map(|(_, v)| v.as_str());
            for (archive, kind) in &archives {
                // Extraction is fenced: everything lands in the lease's
                // staging dir and is renamed into place only on success
                // with the lease still live (double-unpack can't happen).
                let _ = std::fs::remove_dir_all(&staging);
                let mut r = ex.extract(archive, *kind, &staging, password).await?;
                if !r.success && !par_did_repair && par_ok {
                    // The unpack↔repair loop: a broken archive that quick
                    // verification couldn't see; force a repair and retry once.
                    if let Some(set) = par2::load_dir(&dir)? {
                        if let Some(main) = set.main_path.clone() {
                            tracing::warn!(
                                job = job_id.0,
                                "unpack failed; forcing par repair + retry"
                            );
                            stages.enter(PostStage::ParRepair).await;
                            if repair_loop(engine, cfg, &par_tool, &mut stages, job_id, &main)
                                .await?
                            {
                                par_did_repair = true;
                                stages.enter(PostStage::Unpack).await;
                                let _ = std::fs::remove_dir_all(&staging);
                                r = ex.extract(archive, *kind, &staging, password).await?;
                            }
                        }
                    }
                }
                // "It said OK" is not the same as "it read the whole set".
                // Checked BEFORE the commit, so a short extraction is thrown
                // away with the staging dir instead of being renamed over the
                // top of the volumes it failed to read.
                let short = if r.success {
                    crate::tools::unpack_shortfall(&dir, archive, &staging)
                } else {
                    None
                };
                if let Some(why) = &short {
                    tracing::error!(
                        job = job_id.0,
                        archive = %archive.display(),
                        reason = %why,
                        "unpack claimed success without consuming the volume set"
                    );
                }
                if r.success && short.is_none() {
                    if !(ctx.commit_ok)() {
                        let _ = std::fs::remove_dir_all(&staging);
                        return Err(PostError::Subprocess("pp lease lost before commit".into()));
                    }
                    commit_staging(&staging, &dir)?;
                    unpacked_any = true;
                } else {
                    tracing::warn!(
                        job = job_id.0,
                        archive = %archive.display(),
                        password_error = r.password_error,
                        "unpack failed"
                    );
                    unpack_ok = false;
                }
                let _ = std::fs::remove_dir_all(&staging);
            }
        }
    }

    // ---- CLEANUP stage -----------------------------------------------------
    if cfg.cleanup && par_ok && unpack_ok && unpacked_any {
        stages.enter(PostStage::Cleanup).await;
        cleanup_dir(&dir);
    }

    // ---- DEOBFUSCATE stage -------------------------------------------------
    // Anything still meaninglessly named after par-rename, rar-rename and
    // unpack has no recovery evidence left; the job name (from the NZB /
    // indexer) is the last source of truth. Scripts run after this, so
    // they see the final names. Discrete status: the queue shows the
    // PostUnpackRename stage (compat: "RENAMING") while the pass runs, and
    // the applied renames are recorded on the job as `Deobfuscate:*`
    // parameters, which persist into history.
    let mut deobfuscated: Vec<(PathBuf, PathBuf)> = Vec::new();
    if cfg.deobfuscate_final && par_ok && unpack_ok {
        stages.enter(PostStage::PostUnpackRename).await;
        deobfuscated = crate::deobfuscate::deobfuscate_dir(&dir, &sanitized, &par2_names);
        for (from, to) in &deobfuscated {
            tracing::info!(
                job = job_id.0,
                from = %from.display(),
                to = %to.display(),
                "deobfuscate: renamed"
            );
        }
        if !deobfuscated.is_empty() {
            tracing::info!(
                job = job_id.0,
                count = deobfuscated.len(),
                "deobfuscate: pass applied renames"
            );
        }
    }

    // ---- MOVE stage --------------------------------------------------------
    // `[[category]] dest_dir` finally means something. The download landed
    // under the global destination (that is where the engine writes); if
    // the category names another root, the finished folder moves there
    // now, before scripts run and before anything is reported — so the
    // path in the event, in history, and in compat `FinalDir` is the path
    // the files are actually at. Moving earlier would fight the unpack
    // staging; moving later would report a path that is about to change.
    if let Some(target_root) = rule.as_ref().and_then(|r| r.dest_dir.as_ref()) {
        let target = target_root.join(&sanitized);
        if target != dir && dir.exists() {
            let _ = std::fs::remove_dir_all(&staging); // never move the lease's scratch
            stages.enter(PostStage::Move).await;
            if !(ctx.commit_ok)() {
                return Err(PostError::Subprocess("pp lease lost before move".into()));
            }
            let from = dir.clone();
            let to = target.clone();
            let tag = ctx.tag.clone();
            let moved = tokio::task::spawn_blocking(move || move_dir(&from, &to, &tag))
                .await
                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
            match moved {
                Ok(()) => {
                    tracing::info!(
                        job = job_id.0,
                        category = job.category.as_deref().unwrap_or(""),
                        to = %target.display(),
                        "moved to the category destination"
                    );
                    dir = target;
                }
                Err(e) => {
                    // Report where the files ARE, not where they were
                    // meant to go. A wrong path here is the silent import
                    // failure this whole change exists to remove.
                    tracing::error!(
                        job = job_id.0,
                        to = %target.display(),
                        error = %e,
                        "category move failed; leaving the files in the global destination"
                    );
                    if nzbd_engine::is_out_of_space(&e.to_string()) {
                        engine.report_out_of_space(format!(
                            "category move to {}: {e}",
                            target.display()
                        ));
                    }
                }
            }
        }
    }

    // ---- SCRIPT stage ------------------------------------------------------
    let mut script_ok = true;
    let mut final_dir = dir.to_string_lossy().into_owned();
    if let Some(scripts_dir) = &cfg.scripts_dir {
        let scripts = select_scripts(
            discover(scripts_dir),
            rule.as_ref()
                .map(|r| r.extensions.as_slice())
                .unwrap_or(&[]),
        );
        if !scripts.is_empty() {
            stages.enter(PostStage::Script).await;
            let host = ScriptHost {
                timeout: cfg.script_timeout,
            };
            let env = script_env(&job, &dir, par_ok, par_did_repair, unpack_ok, unpacked_any);
            for script in scripts {
                match host.run(&script, &dir, &env).await {
                    Ok(out) => {
                        for (k, v) in &out.commands {
                            if k == "FINALDIR" || k == "DIRECTORY" {
                                final_dir = v.clone();
                            }
                        }
                        match out.exit_code {
                            crate::script_exit::SUCCESS | crate::script_exit::NONE => {}
                            crate::script_exit::PAR_CHECK => {
                                if let Some(set) = par2::load_dir(&dir)? {
                                    if let Some(main) = set.main_path {
                                        let _ = repair_loop(
                                            engine,
                                            cfg,
                                            &par_tool,
                                            &mut stages,
                                            job_id,
                                            &main,
                                        )
                                        .await;
                                    }
                                }
                            }
                            _ => script_ok = false,
                        }
                    }
                    Err(e) => {
                        tracing::error!(job = job_id.0, error = %e, "script failed to run");
                        script_ok = false;
                    }
                }
            }
        }
    }

    // ---- finalize ----------------------------------------------------------
    let outcome = if !par_ok {
        PpFinal::ParFailure
    } else if !unpack_ok {
        PpFinal::UnpackFailure
    } else if !script_ok {
        PpFinal::ScriptFailure
    } else {
        PpFinal::Success
    };

    if !(ctx.commit_ok)() {
        return Err(PostError::Subprocess(
            "pp lease lost before finalize".into(),
        ));
    }
    // ---- disposition of a terminal failure ---------------------------------
    // A failed job's bytes are known-bad, and until this existed nothing
    // ever removed them: the directory stayed in the very tree the
    // importer watches, one ~90 GB corpse per failed grab, until the
    // volume filled (docs/REGRAB_LOOP_PLAN.md D2). Before the history row
    // is written, so the row can say where the files went.
    let disposition = if outcome == PpFinal::Success {
        None
    } else {
        Some(dispose_failed(cfg, job_id, &dir, dest_dir, &sanitized, &ctx.tag).await)
    };

    // Close the last stage span BEFORE the export below reads the job:
    // the timeline that lands in history has to be complete, and the
    // final stage is the one an operator looks at first.
    stages.finish().await;
    // Stamp + set final status in one import (replaces the job atomically).
    if let Ok(Some(mut fin)) = engine.export_job(job_id).await {
        fin.params
            .push((PP_DONE_PARAM.into(), outcome.as_str().into()));
        // Durable deobfuscation record: plain (non-`*`) params survive
        // into history and the compat `Parameters` array.
        if !deobfuscated.is_empty() {
            fin.params
                .push(("Deobfuscate:Count".into(), deobfuscated.len().to_string()));
            let base = |p: &PathBuf| {
                p.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let mut list = deobfuscated
                .iter()
                .take(10)
                .map(|(f, t)| format!("{} → {}", base(f), base(t)))
                .collect::<Vec<_>>()
                .join("; ");
            if deobfuscated.len() > 10 {
                list.push_str("; …");
            }
            fin.params.push(("Deobfuscate:Files".into(), list));
        }
        fin.status = if outcome == PpFinal::Success {
            JobStatus::Completed
        } else {
            JobStatus::Failed
        };
        // Plain param: it survives into history and the compat
        // `Parameters` array, so the row itself answers "where did the
        // files go" — the question every one of these corpses raised.
        if let Some(d) = disposition.as_ref() {
            fin.params.push(("Failure:Files".into(), d.note.clone()));
        }
        let health = Health::calc(&fin.totals).0;
        // A row must not point at a directory that is no longer there:
        // deleted means no final dir, parked means the parked one.
        let final_dir = match disposition.as_ref() {
            None => Some(final_dir),
            Some(d) => d
                .files_at
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
        };
        let entry = HistoryEntry {
            job: job_id,
            name: fin.name.clone(),
            category: fin.category.clone(),
            final_dir: final_dir.clone(),
            status: outcome.as_str().into(),
            size: fin.totals.size,
            health,
            params: user_params(&fin),
            dupe_key: fin.dupe.key.clone(),
            dupe_score: fin.dupe.score,
            completed_at_unix: now(),
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            // The job as it was, kept with the job: which files, how many
            // articles, where it came from, who asked. Those questions are
            // asked AFTER it leaves the queue, which is the one moment the
            // old summary stopped being able to answer them.
            record: Some(nzbd_state::JobRecord::from_job(&fin)),
            stages: fin.stages.clone(),
            seq: 0,
        };
        // Park the NZB with the entry, so a finished job can be put back.
        //
        // Until now `spool_nzb` had exactly ONE caller — the delete path —
        // so a job that completed normally kept no requeue source and its
        // history row reported `can_requeue: false` forever. "That nzb for
        // that job can be downloaded again if necessary" (field report
        // 2026-07-29) was simply not true of any job that succeeded.
        // Gzipped, and reaped with its entry by retention, so keeping one
        // per job stays bounded.
        if !fin.files.is_empty() {
            let h = history.clone();
            let nzb = nzbd_engine::queue::job_to_nzb(&fin).into_bytes();
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = h.spool_nzb(job_id, &nzb) {
                    // Not worth failing the job over: the files are
                    // downloaded and post-processed either way. It costs
                    // the undo, and it says so.
                    tracing::warn!(job = job_id.0, error = %e,
                        "could not park the finished job's NZB — it will not be re-downloadable from history");
                }
            })
            .await;
        }
        // History first, stamp second: a crash in between re-runs PP (the
        // stages are idempotent) — the reverse would lose the entry forever.
        let h = history.clone();
        let history_seq = tokio::task::spawn_blocking(move || h.record_seq(&entry))
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or(0);
        let _ = engine.import_job(fin.clone(), false, false).await;
        // Only now — after the history row is durably written — does the
        // completion become public. A consumer may react to this event by
        // reading `?since_seq=history_seq-1` and is guaranteed to find the
        // row; announcing first would hand every consumer an invisible
        // race against our own write.
        stages.close();
        engine.emit(Event::JobPpFinished {
            job: job_id,
            name: fin.name.clone(),
            category: fin.category.clone(),
            pp_status: outcome.as_str().into(),
            final_dir,
            size_bytes: fin.totals.size,
            health,
            params: user_params(&fin),
            history_seq,
        });
    }
    Ok(outcome)
}

/// Move a finished job folder to another root, across filesystems if need
/// be.
///
/// `rename` is the fast path and the only atomic one. A category
/// destination on a different volume — the usual homelab shape, download
/// on the SSD, library on the NAS — answers `EXDEV`, and then the copy
/// goes to a **sibling scratch directory** and is renamed into place only
/// once every byte is on disk. Copying straight into `to` would leave a
/// half-populated library folder if the process died mid-copy: the next
/// attempt's `rename` would fail `ENOTEMPTY` forever, and in the meantime
/// a truncated file would be sitting where a consumer expects a finished
/// one. This way an interrupted move leaves only `<to>.pp-move`, which
/// the next attempt removes, and the source is untouched until the commit
/// succeeds — so a re-run is always either "not moved yet" or "moved",
/// never "half moved".
fn move_dir(from: &Path, to: &Path, tag: &str) -> std::io::Result<()> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Clear any leftovers from an attempt that died mid-copy first, and
    // unconditionally: whichever path this attempt takes, a completed move
    // must not leave scratch behind in the library.
    let scratch = to.with_file_name(format!(
        "{}.pp-move.{tag}",
        to.file_name().unwrap_or_default().to_string_lossy()
    ));
    let _ = std::fs::remove_dir_all(&scratch);
    match std::fs::rename(from, to) {
        Ok(()) => return Ok(()),
        Err(e) if e.raw_os_error() == Some(18) => {} // EXDEV: different volume
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {}
        Err(e) => return Err(e),
    }
    copy_dir_all(from, &scratch)?;
    std::fs::rename(&scratch, to)?;
    std::fs::remove_dir_all(from)
}

/// Recursive copy, fsyncing each file before it counts as written. The
/// fsync is the difference between "the kernel accepted these bytes" and
/// "these bytes survive a power cut", and the source is deleted right
/// after — so without it a badly-timed crash loses the download outright.
fn copy_dir_all(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
            if let Ok(f) = std::fs::File::open(&target) {
                let _ = f.sync_all();
            }
        }
    }
    if let Ok(d) = std::fs::File::open(to) {
        let _ = d.sync_all(); // the directory entries themselves
    }
    Ok(())
}

/// Narrow the discovered post-processing scripts to a category's
/// `extensions` list. An empty list means "all of them" — the global
/// behavior, and what every category without the key gets. Matching is on
/// the file name or its stem, case-insensitively, so both
/// `Extensions=Clean.py` and `Extensions=clean` name the same script.
fn select_scripts(found: Vec<PathBuf>, extensions: &[String]) -> Vec<PathBuf> {
    if extensions.is_empty() {
        return found;
    }
    found
        .into_iter()
        .filter(|p| {
            let name = |f: Option<&std::ffi::OsStr>| {
                f.map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let file = name(p.file_name());
            let stem = name(p.file_stem());
            extensions
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&file) || e.eq_ignore_ascii_case(&stem))
        })
        .collect()
}

/// verify_full → (unpause delayed pars → wait → re-verify)* → repair.
async fn repair_loop(
    engine: &EngineHandle,
    cfg: &PostConfig,
    par: &Par2Tool,
    stages: &mut Stages<'_>,
    job_id: JobId,
    main: &Path,
) -> Result<bool, PostError> {
    for round in 0..8 {
        match par.verify_full(main).await? {
            VerifyResult::Intact => return Ok(true),
            VerifyResult::Repairable { .. } => {
                stages.enter(PostStage::ParRepair).await;
                return Ok(par.repair(main).await? == RepairResult::Repaired);
            }
            VerifyResult::NeedMoreBlocks { blocks_needed } => {
                let block_size = par2_block_size(main).await;
                tracing::info!(
                    job = job_id.0,
                    blocks_needed,
                    round,
                    block_size,
                    "requesting delayed par blocks"
                );
                let freed = engine
                    .unpause_par_blocks(job_id, blocks_needed, block_size)
                    .await
                    .unwrap_or(0);
                if freed == 0 {
                    // Not "nothing left to fetch" until it says so out
                    // loud: this branch was silent, and a repair that
                    // never started reads exactly like a repair that ran
                    // and failed.
                    tracing::warn!(
                        job = job_id.0,
                        blocks_needed,
                        round,
                        block_size,
                        "repair needs recovery blocks and none could be unpaused — giving up; \
                         the engine log names which paused par2 files it could not price"
                    );
                    return Ok(false);
                }
                if !wait_par_files(engine, job_id, cfg.par_fetch_timeout).await {
                    return Ok(false);
                }
            }
            VerifyResult::Unrepairable => return Ok(false),
        }
    }
    Ok(false)
}

/// What became of a failed job's files: what to tell the history row, and
/// where the bytes are now (`None` = gone).
#[derive(Debug, Clone)]
pub struct Disposition {
    pub note: String,
    pub files_at: Option<PathBuf>,
}

/// Dispose of the files of a job that ended in a terminal failure, per
/// `[post] failure_action`, and describe what happened for the history row.
///
/// The one-dir-one-job invariant is what makes this safe to do wholesale:
/// a job's directory contains that job's files and nothing else, so
/// removing or moving the tree cannot take a neighbour's download with it.
///
/// Deleting is not destructive in the way it looks: the job's NZB is
/// spooled beside its history entry, so `requeue` re-downloads exactly
/// what was thrown away. Keeping it is what filled a terabyte.
pub async fn dispose_failed(
    cfg: &PostConfig,
    job_id: JobId,
    dir: &Path,
    dest_dir: &Path,
    dir_name: &str,
    tag: &str,
) -> Disposition {
    let kept = |note: &str| Disposition {
        note: note.into(),
        files_at: Some(dir.to_path_buf()),
    };
    if !dir.exists() {
        return Disposition {
            note: "already gone".into(),
            files_at: None,
        };
    }
    match cfg.failure_action {
        FailureAction::None => kept("kept"),
        FailureAction::Delete => {
            let d = dir.to_path_buf();
            let res = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&d)).await;
            match res {
                Ok(Ok(())) => {
                    tracing::warn!(job = job_id.0, dir = %dir.display(),
                        "failed job: deleting its files (requeue re-downloads them)");
                    Disposition {
                        note: "deleted".into(),
                        files_at: None,
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!(job = job_id.0, dir = %dir.display(), error = %e,
                        "failed job: could not delete its files — they stay on the volume");
                    kept(&format!("delete failed: {e}"))
                }
                Err(e) => kept(&format!("delete failed: {e}")),
            }
        }
        FailureAction::Park => {
            let root = cfg
                .failed_dir
                .clone()
                .unwrap_or_else(|| dest_dir.join(".failed"));
            let target = root.join(dir_name);
            let from = dir.to_path_buf();
            let to = target.clone();
            let tag = tag.to_string();
            let res = tokio::task::spawn_blocking(move || {
                if let Some(parent) = to.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                move_dir(&from, &to, &tag)
            })
            .await;
            match res {
                Ok(Ok(())) => {
                    tracing::warn!(job = job_id.0, to = %target.display(),
                        "failed job: parking its files off the destination tree");
                    Disposition {
                        note: format!("parked at {}", target.display()),
                        files_at: Some(target),
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!(job = job_id.0, to = %target.display(), error = %e,
                        "failed job: could not park its files — they stay where they are");
                    kept(&format!("park failed: {e}"))
                }
                Err(e) => kept(&format!("park failed: {e}")),
            }
        }
    }
}

/// The recovery-set slice size from the job's main par2 index, if it can
/// be read.
///
/// This is what prices a hash-named recovery volume: no `.volXX+NN` marker
/// exists on an obfuscated post, but `bytes / slice_size` is a good enough
/// block count to pick with. The index is small (tens of KiB) and the read
/// is capped and off the async threads regardless.
async fn par2_block_size(main: &Path) -> Option<u64> {
    let path = main.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Read as _;
        const MAX: u64 = 8 * 1024 * 1024;
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .ok()?
            .take(MAX)
            .read_to_end(&mut bytes)
            .ok()?;
        let slice = nzbd_par2::scan(&bytes).slice_size;
        (slice > 0).then_some(slice)
    })
    .await
    .ok()
    .flatten()
}

/// Wait until every non-paused par2 file of the job is terminal again
/// (the freshly unpaused vol files finished downloading).
async fn wait_par_files(engine: &EngineHandle, job_id: JobId, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        match engine.export_job(job_id).await {
            Ok(Some(job)) => {
                let pending = job
                    .files
                    .iter()
                    .any(|f| f.is_par2 && !f.paused && !f.is_terminal());
                if !pending {
                    // Give writers a beat to finalize renames.
                    let all_finalized = job
                        .files
                        .iter()
                        .filter(|f| f.is_par2 && !f.paused && f.has_any_done())
                        .all(|f| f.finalized);
                    if all_finalized {
                        return true;
                    }
                }
            }
            _ => return false,
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Remove every `.pp.*` staging dir except this lease's own.
fn remove_stale_staging(dir: &Path, own: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() && p != own && e.file_name().to_string_lossy().starts_with(".pp.") {
            tracing::info!(dir = %p.display(), "removing superseded pp staging dir");
            let _ = std::fs::remove_dir_all(&p);
        }
    }
}

/// Publish staged extraction output: rename each entry into the job dir,
/// replacing existing targets (identical content by construction — same
/// archive, same extractor).
fn commit_staging(staging: &Path, dir: &Path) -> std::io::Result<()> {
    for e in std::fs::read_dir(staging)?.flatten() {
        let target = dir.join(e.file_name());
        if target.is_dir() {
            std::fs::remove_dir_all(&target)?;
        }
        std::fs::rename(e.path(), &target)?;
    }
    Ok(())
}

fn cleanup_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_lowercase();
        let junk = name.ends_with(".par2")
            || name.ends_with(".rar")
            || name.ends_with(".zip")
            || name.ends_with(".7z")
            || name.ends_with(".sfv")
            || name
                .split('.')
                .next_back()
                .map(|e| e.chars().all(|c| c.is_ascii_digit()) && e.len() == 3)
                .unwrap_or(false);
        if junk {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// The NZBGet-compatible script environment (the adoption-critical subset).
fn script_env(
    job: &Job,
    dir: &Path,
    par_ok: bool,
    par_repaired: bool,
    unpack_ok: bool,
    unpacked_any: bool,
) -> Vec<(String, String)> {
    let par_status = if !par_ok {
        "4" // repair failed
    } else if par_repaired {
        "2" // repaired
    } else {
        "1" // checked, no repair needed (0 = not checked)
    };
    let unpack_status = if !unpack_ok {
        "1" // failed
    } else if unpacked_any {
        "2" // unpacked
    } else {
        "0" // nothing to unpack
    };
    let total = if par_ok && unpack_ok {
        "SUCCESS"
    } else {
        "FAILURE"
    };
    let mut env = vec![
        ("NZBPP_DIRECTORY".into(), dir.to_string_lossy().into_owned()),
        ("NZBPP_FINALDIR".into(), String::new()),
        ("NZBPP_NZBNAME".into(), job.name.clone()),
        ("NZBPP_NZBFILENAME".into(), format!("{}.nzb", job.name)),
        (
            "NZBPP_CATEGORY".into(),
            job.category.clone().unwrap_or_default(),
        ),
        ("NZBPP_PARSTATUS".into(), par_status.into()),
        ("NZBPP_UNPACKSTATUS".into(), unpack_status.into()),
        ("NZBPP_TOTALSTATUS".into(), total.into()),
        ("NZBPP_STATUS".into(), format!("{total}/ALL")),
        (
            "NZBPP_HEALTH".into(),
            Health::calc(&job.totals).0.to_string(),
        ),
        ("NZBPP_NZBID".into(), job.id.0.to_string()),
        (
            "NZBOP_DESTDIR".into(),
            dir.parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        ("NZBOP_VERSION".into(), env!("CARGO_PKG_VERSION").into()),
    ];
    for (k, v) in &job.params {
        if !k.starts_with('*') {
            env.push((format!("NZBPR_{k}"), v.clone()));
        }
    }
    env
}
