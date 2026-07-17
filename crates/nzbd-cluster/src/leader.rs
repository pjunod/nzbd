//! Leader role (CLUSTERING.md §6): the work-lease endpoints, the lease
//! table with TTL reclaim, the assignment scheduler and connection-budget
//! partitioning. Active only while this node's election view says `is_me`;
//! handlers reject otherwise (workers re-resolve the leader and retry).

use crate::election::LeaderView;
use crate::http::secret_matches;
use crate::proto::*;
use crate::registry::read_nodes;
use crate::{ClusterConfig, SharedLayout};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use nzbd_engine::EngineHandle;
use nzbd_types::{JobId, JobStatus, ServerDef};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone)]
struct LeaseInfo {
    job: JobId,
    node: String,
    kind: LeaseKind,
    last_hb: Instant,
}

pub struct LeaderShared {
    pub engine: EngineHandle,
    pub layout: SharedLayout,
    pub cfg: ClusterConfig,
    pub servers: Vec<ServerDef>,
    pub view: watch::Receiver<LeaderView>,
    leases: Mutex<HashMap<String, LeaseInfo>>,
    lease_counter: std::sync::atomic::AtomicU64,
    /// Node liveness by observed seq progression: name → (seq, last change).
    node_seen: Mutex<HashMap<String, (u64, Instant)>>,
}

impl LeaderShared {
    pub fn new(
        engine: EngineHandle,
        layout: SharedLayout,
        cfg: ClusterConfig,
        servers: Vec<ServerDef>,
        view: watch::Receiver<LeaderView>,
    ) -> Arc<LeaderShared> {
        Arc::new(LeaderShared {
            engine,
            layout,
            cfg,
            servers,
            view,
            leases: Mutex::new(HashMap::new()),
            lease_counter: std::sync::atomic::AtomicU64::new(0),
            node_seen: Mutex::new(HashMap::new()),
        })
    }

    fn is_leader(&self) -> bool {
        self.view.borrow().is_me
    }

    fn epoch(&self) -> u64 {
        self.view.borrow().epoch()
    }

    fn next_lease_id(&self) -> String {
        let n = self
            .lease_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("L{}-{}", self.epoch(), n)
    }

    /// Distinct nodes currently downloading (download leases only — PP
    /// leases open no provider connections) plus the leader itself — the
    /// divisor for per-account connection budgets. Counting the leader
    /// unconditionally is conservative: budgets never exceed the account
    /// cap, at worst they under-use it while the leader idles.
    fn budget_divisor(&self) -> u32 {
        let nodes: HashSet<String> = self
            .leases
            .lock()
            .unwrap()
            .values()
            .filter(|l| l.kind == LeaseKind::Download)
            .map(|l| l.node.clone())
            .collect();
        1 + nodes.len() as u32
    }

    fn budgets_by_name(&self) -> HashMap<String, u16> {
        let n = self.budget_divisor().max(1) as u16;
        self.servers
            .iter()
            .map(|s| (s.name.clone(), (s.max_connections / n).max(1)))
            .collect()
    }

    async fn apply_local_budgets(&self) {
        // A non-downloading node keeps zero budgets no matter what the
        // divisor says — its engine must never open provider connections.
        let by_id = if self.cfg.download {
            let by_name = self.budgets_by_name();
            self.servers
                .iter()
                .filter_map(|s| by_name.get(&s.name).map(|b| (s.id, *b)))
                .collect()
        } else {
            self.servers.iter().map(|s| (s.id, 0u16)).collect()
        };
        let _ = self.engine.set_server_budgets(by_id).await;
    }

    /// Live nodes (seq progressed within 3 lease intervals), self excluded.
    fn live_workers(&self) -> Vec<NodeRecord> {
        let now = Instant::now();
        let ttl = self.cfg.lease_interval * 3;
        let mut seen = self.node_seen.lock().unwrap();
        let mut out = Vec::new();
        for rec in read_nodes(&self.layout) {
            if rec.name == self.cfg.node_name {
                continue;
            }
            let entry = seen.entry(rec.name.clone()).or_insert((rec.seq, now));
            if rec.seq != entry.0 {
                *entry = (rec.seq, now);
            }
            if now.duration_since(entry.1) <= ttl {
                out.push(rec);
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// HTTP endpoints (mounted on every node; answer only while leader)
// ---------------------------------------------------------------------------

pub fn router(shared: Arc<LeaderShared>) -> Router {
    Router::new()
        .route("/cluster/v1/leader", get(leader_info))
        .route("/cluster/v1/work/poll", post(work_poll))
        .route("/cluster/v1/work/heartbeat", post(work_heartbeat))
        .route("/cluster/v1/work/complete", post(work_complete))
        .with_state(shared)
}

fn authed(shared: &LeaderShared, headers: &HeaderMap) -> bool {
    secret_matches(
        headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()),
        &shared.cfg.secret,
    )
}

fn not_leader() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error": "not the leader"})),
    )
        .into_response()
}

fn denied() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error": "bad cluster secret"})),
    )
        .into_response()
}

async fn leader_info(State(s): State<Arc<LeaderShared>>) -> Response {
    let v = s.view.borrow().clone();
    Json(serde_json::json!({
        "leader": v.record.as_ref().map(|r| &r.node),
        "api_url": v.record.as_ref().map(|r| &r.api_url),
        "epoch": v.epoch(),
        "is_me": v.is_me,
    }))
    .into_response()
}

async fn work_poll(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<PollRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }

    // Jobs delegated to this node without an active lease → grants.
    let snap = s.engine.snapshot();
    let assigned: Vec<JobId> = snap
        .jobs
        .iter()
        .filter(|j| j.assigned_node.as_deref() == Some(req.node.as_str()))
        .filter(|j| {
            !matches!(
                j.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        })
        .map(|j| j.id)
        .collect();

    let leased_jobs: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();

    let mut grants = Vec::new();
    for job_id in assigned {
        if grants.len() as u32 >= req.free_download_slots {
            break;
        }
        if leased_jobs.contains(&job_id) {
            continue;
        }
        let Ok(Some(job)) = s.engine.export_job(job_id).await else {
            continue;
        };
        let lease_id = s.next_lease_id();
        s.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job_id,
                node: req.node.clone(),
                kind: LeaseKind::Download,
                last_hb: Instant::now(),
            },
        );
        tracing::info!(job = job_id.0, node = %req.node, %lease_id, "download lease granted");
        // Budgets AFTER inserting the lease: the divisor must count the
        // node this grant goes to, or the first grant hands out the whole
        // account cap until a heartbeat corrects it.
        grants.push(Grant {
            lease_id,
            epoch: s.epoch(),
            kind: LeaseKind::Download,
            job,
            server_budgets: s.budgets_by_name(),
        });
    }

    // PP grants (C2): completed jobs the scheduler assigned to this node
    // for post-processing, not yet leased, PP not yet done.
    let mut pp_granted = 0u32;
    let pp_candidates: Vec<JobId> = snap
        .jobs
        .iter()
        .filter(|j| {
            matches!(j.status, JobStatus::Completed)
                && !j.pp_done
                && j.assigned_node.as_deref() == Some(req.node.as_str())
        })
        .map(|j| j.id)
        .collect();
    for job_id in pp_candidates {
        if pp_granted >= req.free_pp_slots {
            break;
        }
        let already = s.leases.lock().unwrap().values().any(|l| l.job == job_id);
        if already {
            continue;
        }
        let Ok(Some(job)) = s.engine.export_job(job_id).await else {
            continue;
        };
        let lease_id = s.next_lease_id();
        s.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job_id,
                node: req.node.clone(),
                kind: LeaseKind::Post,
                last_hb: Instant::now(),
            },
        );
        tracing::info!(job = job_id.0, node = %req.node, %lease_id, "pp lease granted");
        pp_granted += 1;
        grants.push(Grant {
            lease_id,
            epoch: s.epoch(),
            kind: LeaseKind::Post,
            job,
            server_budgets: HashMap::new(),
        });
    }

    if !grants.is_empty() {
        s.apply_local_budgets().await;
    }
    Json(PollResponse { grants }).into_response()
}

async fn work_heartbeat(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<HeartbeatRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }

    let mut cancel = Vec::new();
    let mut adopted = Vec::new();
    let snap = s.engine.snapshot();
    {
        let mut leases = s.leases.lock().unwrap();
        for lp in &req.leases {
            match leases.get_mut(&lp.lease_id) {
                Some(info) if info.node == req.node => {
                    info.last_hb = Instant::now();
                    if !snap.jobs.iter().any(|j| j.id == lp.job) {
                        cancel.push(lp.lease_id.clone()); // job deleted
                        leases.remove(&lp.lease_id);
                    }
                }
                Some(_) => cancel.push(lp.lease_id.clone()), // someone else's id?!
                None => {
                    // Adoption (CLUSTERING.md §6.2): new leader, live worker.
                    // A running download lease is adoptable while the job is
                    // non-terminal; a running PP lease while the job is
                    // Completed with PP still pending.
                    let job = snap.jobs.iter().find(|j| j.id == lp.job);
                    let unassigned_or_mine = |j: &nzbd_engine::JobSummary| {
                        j.assigned_node.is_none()
                            || j.assigned_node.as_deref() == Some(req.node.as_str())
                    };
                    let kind = job.and_then(|j| {
                        if !unassigned_or_mine(j) || leases.values().any(|l| l.job == lp.job) {
                            None
                        } else if matches!(j.status, JobStatus::Completed) && !j.pp_done {
                            Some(LeaseKind::Post)
                        } else if !matches!(
                            j.status,
                            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                        ) {
                            Some(LeaseKind::Download)
                        } else {
                            None
                        }
                    });
                    if let Some(kind) = kind {
                        leases.insert(
                            lp.lease_id.clone(),
                            LeaseInfo {
                                job: lp.job,
                                node: req.node.clone(),
                                kind,
                                last_hb: Instant::now(),
                            },
                        );
                        adopted.push(lp.job);
                        tracing::info!(job = lp.job.0, node = %req.node, lease = %lp.lease_id, ?kind, "lease adopted");
                    } else {
                        cancel.push(lp.lease_id.clone());
                    }
                }
            }
        }
    }
    for job in adopted {
        let _ = s.engine.set_delegated(job, Some(req.node.clone())).await;
    }
    for lp in &req.leases {
        if !cancel.contains(&lp.lease_id) {
            s.engine.mirror_progress(lp.job, lp.stats);
        }
    }
    Json(HeartbeatResponse {
        cancel,
        server_budgets: Some(s.budgets_by_name()),
    })
    .into_response()
}

async fn work_complete(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<CompleteRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    let job_id = req.job.id;
    let known = {
        let mut leases = s.leases.lock().unwrap();
        match leases.get(&req.lease_id) {
            Some(info) if info.node == req.node && info.job == job_id => {
                leases.remove(&req.lease_id);
                true
            }
            _ => {
                // Accept anyway if the job is assigned to this node — the
                // lease may have been reclaimed a moment ago; a completed
                // job is a completed job.
                s.engine.snapshot().jobs.iter().any(|j| {
                    j.id == job_id && j.assigned_node.as_deref() == Some(req.node.as_str())
                })
            }
        }
    };
    if !known {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "unknown lease"})),
        )
            .into_response();
    }
    tracing::info!(job = job_id.0, node = %req.node, "job completed remotely");
    let _ = s.engine.import_job(req.job, false, true).await;
    s.apply_local_budgets().await;
    Json(CompleteResponse { ok: true }).into_response()
}

// ---------------------------------------------------------------------------
// Sweeper + scheduler task
// ---------------------------------------------------------------------------

pub fn spawn_leader_task(
    shared: Arc<LeaderShared>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let mut was_leader = false;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let is_leader = shared.is_leader();
            if is_leader && !was_leader {
                // Taking office: authority adoption; leases arrive via
                // worker heartbeats (adoption) or fresh grants.
                shared.leases.lock().unwrap().clear();
                let _ = shared.engine.adopt_authority().await;
                tracing::info!(epoch = shared.epoch(), "leader task active");
            }
            was_leader = is_leader;

            if is_leader {
                sweep_expired(&shared).await;
                schedule(&shared).await;
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(shared.cfg.lease_interval) => {}
            }
        }
    });
}

async fn sweep_expired(s: &Arc<LeaderShared>) {
    let ttl = s.cfg.worker_ttl;
    let expired: Vec<(String, LeaseInfo)> = {
        let mut leases = s.leases.lock().unwrap();
        let now = Instant::now();
        let dead: Vec<String> = leases
            .iter()
            .filter(|(_, l)| now.duration_since(l.last_hb) > ttl)
            .map(|(id, _)| id.clone())
            .collect();
        dead.into_iter()
            .filter_map(|id| leases.remove(&id).map(|l| (id, l)))
            .collect()
    };
    for (lease_id, info) in expired {
        tracing::warn!(job = info.job.0, node = %info.node, %lease_id, "lease expired; reclaiming");
        // Fold whatever the worker journaled, release the delegation; the
        // job re-enters scheduling (locally or re-delegated).
        let _ = s.engine.fold_job_journals(info.job).await;
        let _ = s.engine.set_delegated(info.job, None).await;
    }
    s.apply_local_budgets().await;
}

async fn schedule(s: &Arc<LeaderShared>) {
    let workers = s.live_workers();
    let snap = s.engine.snapshot();

    // Reconcile: a job assigned to a node that is no longer live and holds
    // no lease for it was delegated into the void (node died between
    // assignment and poll, or vanished entirely). Release it.
    {
        let live: HashSet<&str> = workers.iter().map(|w| w.name.as_str()).collect();
        let leased: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
        for j in snap.jobs.iter() {
            if let Some(node) = j.assigned_node.as_deref() {
                if node != s.cfg.node_name
                    && !live.contains(node)
                    && !leased.contains(&j.id)
                    && !matches!(j.status, JobStatus::Deleted)
                {
                    tracing::warn!(job = j.id.0, %node, "assigned node is gone; releasing delegation");
                    let _ = s.engine.set_delegated(j.id, None).await;
                }
            }
        }
    }

    let (leases_by_node, pp_leases_by_node): (HashMap<String, u32>, HashMap<String, u32>) = {
        let leases = s.leases.lock().unwrap();
        let mut dl = HashMap::new();
        let mut pp = HashMap::new();
        for l in leases.values() {
            match l.kind {
                LeaseKind::Download => *dl.entry(l.node.clone()).or_insert(0) += 1,
                LeaseKind::Post => *pp.entry(l.node.clone()).or_insert(0) += 1,
            }
        }
        (dl, pp)
    };

    // Free download slots per worker (our lease count is fresher than the
    // registry's self-reported load).
    let mut free: Vec<(String, u32)> = workers
        .iter()
        .filter(|w| w.download && w.max_download_jobs > 0)
        .map(|w| {
            let held = leases_by_node.get(&w.name).copied().unwrap_or(0);
            (w.name.clone(), w.max_download_jobs.saturating_sub(held))
        })
        .filter(|(_, f)| *f > 0)
        .collect();
    free.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    let self_active = snap
        .jobs
        .iter()
        .filter(|j| j.assigned_node.is_none() && matches!(j.status, JobStatus::Downloading))
        .count() as u32;
    let self_capacity = if s.cfg.download {
        s.cfg.max_download_jobs.saturating_sub(self_active)
    } else {
        0
    };
    let mut self_free = self_capacity;

    // Assign untouched queued jobs: keep local while we have capacity,
    // then spread to the freest workers. Jobs with local progress stay
    // local (no mid-download migration in C1).
    for job in snap.jobs.iter() {
        if job.assigned_node.is_some() || !matches!(job.status, JobStatus::Queued) {
            continue;
        }
        if self_free > 0 {
            self_free -= 1; // stays local: the engine schedules it itself
            continue;
        }
        let Some(slot) = free.iter_mut().find(|(_, f)| *f > 0) else {
            break; // everyone is saturated
        };
        slot.1 -= 1;
        let node = slot.0.clone();
        tracing::info!(job = job.id.0, %node, "delegating job");
        let _ = s.engine.set_delegated(job.id, Some(node)).await;
    }

    // ---- PP assignment (C2, CLUSTERING.md §13) ----------------------------
    // Anti-affinity: a node busy downloading is the LAST choice for par
    // repair / unpack — prefer idle PP-capable nodes so the same box never
    // runs both when the cluster has spare hands.
    let leased_jobs: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
    let mut pp_targets: Vec<(String, u32, bool)> = Vec::new(); // (node, free_pp, downloading)
    for w in workers.iter().filter(|w| w.post_process && w.pp_slots > 0) {
        let pp_held = pp_leases_by_node.get(&w.name).copied().unwrap_or(0)
            + assigned_pp_backlog(&snap, &w.name, &leased_jobs);
        let free = w.pp_slots.saturating_sub(pp_held);
        if free > 0 {
            let downloading = leases_by_node.get(&w.name).copied().unwrap_or(0) > 0
                || w.active_download_jobs > 0;
            pp_targets.push((w.name.clone(), free, downloading));
        }
    }
    if s.cfg.post_process && s.cfg.pp_slots > 0 {
        let held = assigned_pp_backlog(&snap, &s.cfg.node_name, &leased_jobs);
        let free = s.cfg.pp_slots.saturating_sub(held);
        if free > 0 {
            pp_targets.push((s.cfg.node_name.clone(), free, self_active > 0));
        }
    }
    // Idle nodes first, then most free slots, then name for determinism.
    pp_targets.sort_by(|a, b| a.2.cmp(&b.2).then(b.1.cmp(&a.1)).then(a.0.cmp(&b.0)));

    for job in snap.jobs.iter() {
        if !matches!(job.status, JobStatus::Completed) || job.pp_done || job.assigned_node.is_some()
        {
            continue;
        }
        let Some(slot) = pp_targets.iter_mut().find(|(_, f, _)| *f > 0) else {
            break;
        };
        slot.1 -= 1;
        let node = slot.0.clone();
        tracing::info!(job = job.id.0, %node, "assigning post-processing");
        let _ = s.engine.set_delegated(job.id, Some(node)).await;
    }
}

/// Completed-but-unprocessed jobs already assigned to `node` and not yet
/// leased count against its PP capacity (assignment-to-poll in flight).
fn assigned_pp_backlog(
    snap: &nzbd_engine::QueueSnapshot,
    node: &str,
    leased: &HashSet<JobId>,
) -> u32 {
    snap.jobs
        .iter()
        .filter(|j| {
            matches!(j.status, JobStatus::Completed)
                && !j.pp_done
                && !leased.contains(&j.id)
                && j.assigned_node.as_deref() == Some(node)
        })
        .count() as u32
}
