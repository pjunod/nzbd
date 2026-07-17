//! Worker role (CLUSTERING.md §6.2): poll the leader for download-job
//! leases, execute them on the local engine (journaling to the shared
//! per-job files), heartbeat progress, report completions. Leases survive
//! leader failover — the next heartbeat to the new leader adopts them.

use crate::election::LeaderView;
use crate::http::ClusterClient;
use crate::proto::*;
use crate::ClusterConfig;
use nzbd_engine::{EngineHandle, MirrorStats};
use nzbd_types::{JobId, JobStatus, ServerDef};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Lease-id → job map, shared with the demotion path (`retain_jobs`).
pub type ActiveLeases = Arc<Mutex<HashMap<String, JobId>>>;

#[allow(clippy::too_many_arguments)]
pub fn spawn_worker(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(worker_task(
        cfg, servers, engine, view, client, active, cancel,
    ));
}

async fn worker_task(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    cancel: CancellationToken,
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
            poll_for_work(&cfg, &servers, &engine, &client, &active, &url).await;
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
        .map(|(id, job)| LeaseProgress {
            lease_id: id.clone(),
            job: *job,
            stats: progress_of(engine, *job),
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
                let job = active.lock().unwrap().remove(&lease_id);
                if let Some(job) = job {
                    tracing::info!(job = job.0, %lease_id, "lease cancelled by leader");
                    let _ = engine.remove_job_silent(job).await;
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
        .filter(|(_, job)| {
            snapshot.jobs.iter().any(|j| {
                j.id == **job && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            })
        })
        .map(|(id, job)| (id.clone(), *job))
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

async fn poll_for_work(
    cfg: &ClusterConfig,
    servers: &[ServerDef],
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    leader_url: &str,
) {
    if !cfg.download {
        return;
    }
    let held = active.lock().unwrap().len() as u32;
    let free = cfg.max_download_jobs.saturating_sub(held);
    if free == 0 {
        return;
    }
    let req = PollRequest {
        node: cfg.node_name.clone(),
        free_download_slots: free,
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
        tracing::info!(
            job = grant.job.id.0,
            lease = %grant.lease_id,
            "download lease received"
        );
        apply_budgets(engine, servers, &grant.server_budgets).await;
        let job_id = grant.job.id;
        // Fold shared journals on import: resume work another node did.
        if engine.import_job(grant.job, true, false).await.is_ok() {
            active.lock().unwrap().insert(grant.lease_id, job_id);
        }
    }
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
