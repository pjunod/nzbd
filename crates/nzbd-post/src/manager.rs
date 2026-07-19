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

/// What to do with a job that failed its health gate (NZBGet
/// `HealthCheck`): keep the partial files (`None`/`Park` — the history
/// entry records the failure either way) or delete them from disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthAction {
    #[default]
    None,
    Park,
    Delete,
}

impl HealthAction {
    pub fn parse(s: &str) -> HealthAction {
        match s.to_ascii_lowercase().as_str() {
            "delete" => HealthAction::Delete,
            "park" | "pause" => HealthAction::Park,
            _ => HealthAction::None,
        }
    }
}

#[derive(Debug, Clone)]
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
    /// Action for health-gated failures (files on disk).
    pub health_action: HealthAction,
    /// Concurrent PP jobs (PostStrategy: sequential=1, balanced=2,
    /// aggressive=3, rocket=6).
    pub slots: usize,
    pub tool_timeout: Duration,
    pub script_timeout: Duration,
    /// How long to wait for delayed par files to download.
    pub par_fetch_timeout: Duration,
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
            health_action: HealthAction::None,
            slots: 1,
            tool_timeout: Duration::from_secs(3600),
            script_timeout: Duration::from_secs(3600),
            par_fetch_timeout: Duration::from_secs(600),
        }
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
                scan_queue(&tracker, &engine, &cfg, &history, &dest_dir, &gate, &sem, &mut queued).await;
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
                        let entry = HistoryEntry {
                            job,
                            name,
                            category: exported.as_ref().and_then(|j| j.category.clone()),
                            final_dir: None,
                            status: fail_status.into(),
                            size: exported.as_ref().map(|j| j.totals.size).unwrap_or(0),
                            health,
                            params: exported.as_ref().map(user_params).unwrap_or_default(),
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
                        };
                        let h = history.clone();
                        let _ = tokio::task::spawn_blocking(move || h.record(&entry)).await;
                        if let Some(mut fin) = exported {
                            if cfg.health_action == HealthAction::Delete {
                                let dir = dest_dir
                                    .join(nzbd_engine::queue::sanitize_name(&fin.name));
                                tracing::warn!(job = job.0, dir = %dir.display(),
                                    "health check action: deleting failed download");
                                let _ = tokio::task::spawn_blocking(move || {
                                    std::fs::remove_dir_all(&dir)
                                })
                                .await;
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
    job: JobId,
    retire_local: bool,
) {
    let engine = engine.clone();
    let cfg = cfg.clone();
    let history = history.clone();
    let dest = dest_dir.to_path_buf();
    let sem = sem.clone();
    tracker.spawn(async move {
        let Ok(_permit) = sem.acquire().await else {
            return;
        };
        match process_job(&engine, &cfg, &history, &dest, job).await {
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
            Err(e) => tracing::error!(job = job.0, error = %e, "post-processing crashed"),
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

async fn set_stage(engine: &EngineHandle, job: JobId, stage: PostStage) {
    let _ = engine.set_job_status(job, JobStatus::Post { stage }).await;
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
    let dir = dest_dir.join(nzbd_engine::queue::sanitize_name(&job.name));
    // Superseded staging dirs (a reclaimed lease's leftovers) are garbage
    // by definition — this lease is now the only live executor.
    let staging = dir.join(format!(".pp.{}", ctx.tag));
    remove_stale_staging(&dir, &staging);

    // ---- RENAME stage (par-rename, then rar-rename) ------------------------
    // Obfuscated posts get their real names back before anything verifies
    // or unpacks. Whole-file CRCs are content-addressed, so download
    // evidence just needs its paths remapped.
    set_stage(engine, job_id, PostStage::ParRename).await;
    let mut renames = par_rename(&dir);
    if cfg.unpack {
        set_stage(engine, job_id, PostStage::RarRename).await;
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
        set_stage(engine, job_id, PostStage::ParVerify).await;
        let quick = par2::quick_verify(&set, &evidence_of(&job, &dir, &rename_map));
        if quick == VerifyResult::Intact {
            tracing::info!(job = job_id.0, "par quick-verify: intact (no data re-read)");
        } else if let Some(main) = set.main_path.clone() {
            par_ok = repair_loop(engine, cfg, &par_tool, job_id, &main).await?;
            par_did_repair = par_ok;
        } else {
            par_ok = false;
        }
    }

    // ---- UNPACK stage ------------------------------------------------------
    let mut unpack_ok = true;
    let mut unpacked_any = false;
    if cfg.unpack {
        let archives = detect_archives(&dir);
        if !archives.is_empty() {
            set_stage(engine, job_id, PostStage::Unpack).await;
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
                            set_stage(engine, job_id, PostStage::ParRepair).await;
                            if repair_loop(engine, cfg, &par_tool, job_id, &main).await? {
                                par_did_repair = true;
                                set_stage(engine, job_id, PostStage::Unpack).await;
                                let _ = std::fs::remove_dir_all(&staging);
                                r = ex.extract(archive, *kind, &staging, password).await?;
                            }
                        }
                    }
                }
                if r.success {
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
        set_stage(engine, job_id, PostStage::Cleanup).await;
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
        set_stage(engine, job_id, PostStage::PostUnpackRename).await;
        deobfuscated = crate::deobfuscate::deobfuscate_dir(
            &dir,
            &nzbd_engine::queue::sanitize_name(&job.name),
            &par2_names,
        );
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

    // ---- SCRIPT stage ------------------------------------------------------
    let mut script_ok = true;
    let mut final_dir = dir.to_string_lossy().into_owned();
    if let Some(scripts_dir) = &cfg.scripts_dir {
        let scripts = discover(scripts_dir);
        if !scripts.is_empty() {
            set_stage(engine, job_id, PostStage::Script).await;
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
                                        let _ = repair_loop(engine, cfg, &par_tool, job_id, &main)
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
        let health = Health::calc(&fin.totals).0;
        let entry = HistoryEntry {
            job: job_id,
            name: fin.name.clone(),
            category: fin.category.clone(),
            final_dir: Some(final_dir),
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
        };
        // History first, stamp second: a crash in between re-runs PP (the
        // stages are idempotent) — the reverse would lose the entry forever.
        let h = history.clone();
        let _ = tokio::task::spawn_blocking(move || h.record(&entry)).await;
        let _ = engine.import_job(fin, false, false).await;
    }
    Ok(outcome)
}

/// verify_full → (unpause delayed pars → wait → re-verify)* → repair.
async fn repair_loop(
    engine: &EngineHandle,
    cfg: &PostConfig,
    par: &Par2Tool,
    job_id: JobId,
    main: &Path,
) -> Result<bool, PostError> {
    for round in 0..8 {
        match par.verify_full(main).await? {
            VerifyResult::Intact => return Ok(true),
            VerifyResult::Repairable { .. } => {
                set_stage(engine, job_id, PostStage::ParRepair).await;
                return Ok(par.repair(main).await? == RepairResult::Repaired);
            }
            VerifyResult::NeedMoreBlocks { blocks_needed } => {
                tracing::info!(
                    job = job_id.0,
                    blocks_needed,
                    round,
                    "requesting delayed par blocks"
                );
                let freed = engine
                    .unpause_par_blocks(job_id, blocks_needed)
                    .await
                    .unwrap_or(0);
                if freed == 0 {
                    return Ok(false); // nothing left to fetch
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
