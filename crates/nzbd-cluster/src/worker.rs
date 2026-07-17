//! Worker role (CLUSTERING.md §6.2): poll the leader for download and
//! post-processing leases, execute them on the local engine (downloads
//! journal to the shared per-job files; PP runs the stage pipeline fenced
//! in `.pp.<lease>/` staging), heartbeat progress, report completions.
//! Leases survive leader failover — the next heartbeat to the new leader
//! adopts them.

use crate::election::LeaderView;
use crate::http::ClusterClient;
use crate::proto::*;
use crate::{ClusterConfig, PpSetup};
use nzbd_engine::{EngineHandle, MirrorStats};
use nzbd_post::manager::{process_job_ctx, PpCtx};
use nzbd_types::{JobId, JobStatus, ServerDef};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone)]
pub struct LeaseState {
    pub job: JobId,
    pub kind: LeaseKind,
    /// PP leases only: the pipeline finished locally; the stamped job is
    /// ready to hand to the leader.
    pub pp_ready: bool,
}

/// Lease-id → state map, shared with the demotion path (`retain_jobs`).
pub type ActiveLeases = Arc<Mutex<HashMap<String, LeaseState>>>;

#[allow(clippy::too_many_arguments)]
pub fn spawn_worker(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    pp: Option<PpSetup>,
    dest_dir: PathBuf,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    let t2 = tracker.clone();
    tracker.spawn(worker_task(
        cfg, servers, engine, view, client, active, pp, dest_dir, cancel, t2,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn worker_task(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    pp: Option<PpSetup>,
    dest_dir: PathBuf,
    cancel: CancellationToken,
    tracker: TaskTracker,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let v = view.borrow().clone();

        if v.is_me {
            // We are the leader: granted leases dissolve into local jobs
            // (adopt_authority kept them); the scheduler takes over.
            active.lock().unwrap().clear();
        } else if let Some(url) = v.leader_url().map(|s| s.to_string()) {
            heartbeat_and_cancel(&cfg, &servers, &engine, &client, &active, &url).await;
            report_completions(&cfg, &engine, &client, &active, &url).await;
            poll_for_work(
                &cfg, &servers, &engine, &client, &active, &pp, &dest_dir, &tracker, &url,
            )
            .await;
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(cfg.lease_interval) => {}
        }
    }
}

fn progress_of(engine: &EngineHandle, job: JobId) -> MirrorStats {
    engine
        .snapshot()
        .jobs
        .iter()
        .find(|j| j.id == job)
        .map(|j| MirrorStats {
            done_articles: j.done_articles,
            failed_articles: j.failed_articles,
            downloaded_bytes: j.downloaded_bytes,
            health: j.health,
        })
        .unwrap_or_default()
}

async fn heartbeat_and_cancel(
    cfg: &ClusterConfig,
    servers: &[ServerDef],
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    leader_url: &str,
) {
    let leases: Vec<LeaseProgress> = active
        .lock()
        .unwrap()
        .iter()
        .map(|(id, st)| LeaseProgress {
            lease_id: id.clone(),
            job: st.job,
            stats: progress_of(engine, st.job),
        })
        .collect();
    if leases.is_empty() {
        return;
    }
    let req = HeartbeatRequest {
        node: cfg.node_name.clone(),
        leases,
    };
    match client
        .post_json::<_, HeartbeatResponse>(leader_url, "/cluster/v1/work/heartbeat", &req)
        .await
    {
        Ok(resp) => {
            for lease_id in resp.cancel {
                let st = active.lock().unwrap().remove(&lease_id);
                if let Some(st) = st {
                    tracing::info!(job = st.job.0, %lease_id, "lease cancelled by leader");
                    let _ = engine.remove_job_silent(st.job).await;
                }
            }
            if let Some(budgets) = resp.server_budgets {
                apply_budgets(engine, servers, &budgets).await;
            }
        }
        Err(e) => tracing::debug!(error = %e, "heartbeat failed (election in progress?)"),
    }
}

async fn report_completions(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    leader_url: &str,
) {
    let snapshot = engine.snapshot();
    let finished: Vec<(String, JobId)> = active
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, st)| match st.kind {
            LeaseKind::Download => snapshot.jobs.iter().any(|j| {
                j.id == st.job && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            }),
            // A PP job arrives already Completed — only hand it back once
            // the pipeline stamped it.
            LeaseKind::Post => st.pp_ready,
        })
        .map(|(id, st)| (id.clone(), st.job))
        .collect();

    for (lease_id, job_id) in finished {
        let Ok(Some(job)) = engine.export_job(job_id).await else {
            continue;
        };
        let req = CompleteRequest {
            node: cfg.node_name.clone(),
            lease_id: lease_id.clone(),
            job,
        };
        match client
            .post_json::<_, CompleteResponse>(leader_url, "/cluster/v1/work/complete", &req)
            .await
        {
            Ok(resp) if resp.ok => {
                tracing::info!(job = job_id.0, %lease_id, "completion handed to leader");
                active.lock().unwrap().remove(&lease_id);
                let _ = engine.remove_job_silent(job_id).await;
            }
            Ok(_) | Err(_) => {
                // Leader unreachable or refused: retry next tick; a
                // reclaimed lease resolves via the journals either way.
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn poll_for_work(
    cfg: &ClusterConfig,
    servers: &[ServerDef],
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    dest_dir: &std::path::Path,
    tracker: &TaskTracker,
    leader_url: &str,
) {
    let (dl_held, pp_held) = {
        let a = active.lock().unwrap();
        (
            a.values().filter(|s| s.kind == LeaseKind::Download).count() as u32,
            a.values().filter(|s| s.kind == LeaseKind::Post).count() as u32,
        )
    };
    let free_dl = if cfg.download {
        cfg.max_download_jobs.saturating_sub(dl_held)
    } else {
        0
    };
    let free_pp = if cfg.post_process && pp.is_some() {
        cfg.pp_slots.saturating_sub(pp_held)
    } else {
        0
    };
    if free_dl == 0 && free_pp == 0 {
        return;
    }
    let req = PollRequest {
        node: cfg.node_name.clone(),
        free_download_slots: free_dl,
        free_pp_slots: free_pp,
    };
    let resp = match client
        .post_json::<_, PollResponse>(leader_url, "/cluster/v1/work/poll", &req)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "work poll failed");
            return;
        }
    };
    for grant in resp.grants {
        let job_id = grant.job.id;
        match grant.kind {
            LeaseKind::Download => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "download lease received");
                apply_budgets(engine, servers, &grant.server_budgets).await;
                // Fold shared journals on import: resume work another node did.
                if engine.import_job(grant.job, true, false).await.is_ok() {
                    active.lock().unwrap().insert(
                        grant.lease_id,
                        LeaseState {
                            job: job_id,
                            kind: LeaseKind::Download,
                            pp_ready: false,
                        },
                    );
                }
            }
            LeaseKind::Post => {
                let Some(setup) = pp else { continue };
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "pp lease received");
                if engine.import_job(grant.job, false, false).await.is_ok() {
                    active.lock().unwrap().insert(
                        grant.lease_id.clone(),
                        LeaseState {
                            job: job_id,
                            kind: LeaseKind::Post,
                            pp_ready: false,
                        },
                    );
                    run_pp_lease(
                        engine.clone(),
                        setup.clone(),
                        dest_dir.to_path_buf(),
                        active.clone(),
                        grant.lease_id,
                        job_id,
                        tracker,
                    );
                }
            }
        }
    }
}

/// Execute one PP lease: the stage pipeline fenced by the lease id, with a
/// commit check against the live lease map (a cancelled/reclaimed lease
/// must never publish results or stamp the job).
fn run_pp_lease(
    engine: EngineHandle,
    setup: PpSetup,
    dest_dir: PathBuf,
    active: ActiveLeases,
    lease_id: String,
    job_id: JobId,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let ctx = PpCtx {
            tag: lease_id.clone(),
            commit_ok: Arc::new({
                let active = active.clone();
                let lease_id = lease_id.clone();
                move || active.lock().unwrap().contains_key(&lease_id)
            }),
        };
        match process_job_ctx(&engine, &setup.post, &setup.history, &dest_dir, job_id, &ctx).await {
            Ok(outcome) => {
                tracing::info!(job = job_id.0, lease = %lease_id, outcome = outcome.as_str(), "pp lease finished");
                if let Some(st) = active.lock().unwrap().get_mut(&lease_id) {
                    st.pp_ready = true;
                }
            }
            Err(e) => {
                tracing::warn!(job = job_id.0, lease = %lease_id, error = %e, "pp lease aborted");
                // Drop the local copy; the leader reclaims and reschedules.
                if active.lock().unwrap().remove(&lease_id).is_some() {
                    let _ = engine.remove_job_silent(job_id).await;
                }
            }
        }
    });
}

async fn apply_budgets(
    engine: &EngineHandle,
    servers: &[ServerDef],
    by_name: &HashMap<String, u16>,
) {
    let by_id: HashMap<nzbd_types::ServerId, u16> = servers
        .iter()
        .filter_map(|s| by_name.get(&s.name).map(|b| (s.id, *b)))
        .collect();
    if !by_id.is_empty() {
        let _ = engine.set_server_budgets(by_id).await;
    }
}
