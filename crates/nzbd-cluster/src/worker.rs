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
    let disk_low = engine.snapshot().disk_low;
    let (dl_held, pp_held) = {
        let a = active.lock().unwrap();
        (
            a.values().filter(|s| s.kind == LeaseKind::Download).count() as u32,
            a.values().filter(|s| s.kind == LeaseKind::Post).count() as u32,
        )
    };
    let free_dl = if cfg.download && !disk_low {
        cfg.max_download_jobs.saturating_sub(dl_held)
    } else {
        0
    };
    let free_pp = if cfg.post_process && pp.is_some() && !disk_low {
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
        if engine.snapshot().disk_low {
            reject_remote_grant(cfg, client, leader_url, &grant.lease_id).await;
            continue;
        }
        match grant.kind {
            LeaseKind::Download => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "download lease received");
                apply_budgets(engine, servers, &grant.server_budgets).await;
                // Fold shared journals on import: resume work another node did.
                if engine.import_job(grant.job, true, false).await.is_ok() {
                    if engine.snapshot().disk_low {
                        let _ = engine.remove_job_silent(job_id).await;
                        reject_remote_grant(cfg, client, leader_url, &grant.lease_id).await;
                        continue;
                    }
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
                    if engine.snapshot().disk_low {
                        let _ = engine.remove_job_silent(job_id).await;
                        reject_remote_grant(cfg, client, leader_url, &grant.lease_id).await;
                        continue;
                    }
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
                        cfg.node_name.clone(),
                        client.clone(),
                        leader_url.to_string(),
                    );
                }
            }
        }
    }
}

async fn reject_remote_grant(
    cfg: &ClusterConfig,
    client: &ClusterClient,
    leader_url: &str,
    lease_id: &str,
) {
    let req = RejectRequest {
        node: cfg.node_name.clone(),
        lease_id: lease_id.to_string(),
    };
    match client
        .post_json::<_, RejectResponse>(leader_url, "/cluster/v1/work/reject", &req)
        .await
    {
        Ok(response) if response.released => {
            tracing::info!(%lease_id, "rejected stale grant after disk guard changed")
        }
        Ok(_) => tracing::debug!(%lease_id, "stale grant was already released"),
        Err(error) => tracing::warn!(
            %lease_id,
            %error,
            "could not reject stale grant; it will expire without being started"
        ),
    }
}

/// Execute one PP lease: the stage pipeline fenced by the lease id, with a
/// commit check against the live lease map (a cancelled/reclaimed lease
/// must never publish results or stamp the job).
#[allow(clippy::too_many_arguments)]
fn run_pp_lease(
    engine: EngineHandle,
    setup: PpSetup,
    dest_dir: PathBuf,
    active: ActiveLeases,
    lease_id: String,
    job_id: JobId,
    tracker: &TaskTracker,
    node: String,
    client: ClusterClient,
    leader_url: String,
) {
    tracker.spawn(async move {
        if engine.snapshot().disk_low {
            active.lock().unwrap().remove(&lease_id);
            let _ = engine.remove_job_silent(job_id).await;
            let req = RejectRequest {
                node,
                lease_id: lease_id.clone(),
            };
            let _ = client
                .post_json::<_, RejectResponse>(
                    &leader_url,
                    "/cluster/v1/work/reject",
                    &req,
                )
                .await;
            return;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_types::{DupeInfo, Job, JobKind, JobTotals};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct DelayedLeader {
        poll_seen: Arc<tokio::sync::Semaphore>,
        release_response: Arc<tokio::sync::Semaphore>,
        rejected: Arc<AtomicUsize>,
        job: Job,
    }

    async fn delayed_poll(State(state): State<DelayedLeader>) -> Json<PollResponse> {
        state.poll_seen.add_permits(1);
        let _ = state.release_response.acquire().await;
        Json(PollResponse {
            grants: vec![Grant {
                lease_id: "transition-lease".into(),
                epoch: 1,
                kind: LeaseKind::Download,
                job: state.job,
                server_budgets: HashMap::new(),
            }],
        })
    }

    async fn record_reject(
        State(state): State<DelayedLeader>,
        Json(req): Json<RejectRequest>,
    ) -> Json<RejectResponse> {
        assert_eq!(req.node, "worker");
        assert_eq!(req.lease_id, "transition-lease");
        state.rejected.fetch_add(1, Ordering::SeqCst);
        Json(RejectResponse { released: true })
    }

    fn queued_test_job() -> Job {
        Job {
            id: JobId(77),
            kind: JobKind::Nzb,
            name: "transition".into(),
            dir_name: "transition".into(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: JobTotals::default(),
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        }
    }

    #[tokio::test]
    async fn healthy_poll_then_disk_hold_rejects_grant_without_importing_it() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        assert!(!engine.snapshot().disk_low);

        let state = DelayedLeader {
            poll_seen: Arc::new(tokio::sync::Semaphore::new(0)),
            release_response: Arc::new(tokio::sync::Semaphore::new(0)),
            rejected: Arc::new(AtomicUsize::new(0)),
            job: queued_test_job(),
        };
        let app = Router::new()
            .route("/cluster/v1/work/poll", post(delayed_poll))
            .route("/cluster/v1/work/reject", post(record_reject))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cfg = ClusterConfig {
            node_name: "worker".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://worker.invalid".into(),
            secret: "secret".into(),
            coordinator: false,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            disk_guard_roots: Vec::new(),
        };
        let client = ClusterClient::new("secret".into());
        let active: ActiveLeases = Default::default();
        let tracker = TaskTracker::new();
        let dest = tmp.path().join("dest");

        let poll = poll_for_work(
            &cfg,
            &[],
            &engine,
            &client,
            &active,
            &None,
            &dest,
            &tracker,
            &leader_url,
        );
        let transition = async {
            let _ = state.poll_seen.acquire().await.unwrap();
            engine.report_out_of_space("injected between poll and response");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while !engine.snapshot().disk_low {
                assert!(tokio::time::Instant::now() < deadline);
                tokio::task::yield_now().await;
            }
            state.release_response.add_permits(1);
        };
        tokio::join!(poll, transition);

        assert_eq!(state.rejected.load(Ordering::SeqCst), 1);
        assert!(active.lock().unwrap().is_empty());
        assert!(engine.export_job(JobId(77)).await.unwrap().is_none());

        tracker.close();
        tracker.wait().await;
        server.abort();
        engine.shutdown().await;
    }
}
