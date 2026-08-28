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

    /// Every remote executor that may currently use provider connections,
    /// plus the leader as a conservative reserved share. PP leases count
    /// because delayed PAR recovery can open NNTP connections. Remote nodes
    /// come first so a scarce remainder is not stranded on an idle leader.
    fn budget_nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self
            .leases
            .lock()
            .unwrap()
            .values()
            .map(|l| l.node.clone())
            .collect();
        nodes.sort();
        nodes.dedup();
        nodes.retain(|node| node != &self.cfg.node_name);
        nodes.push(self.cfg.node_name.clone());
        nodes
    }

    /// This executor's exact share. Remainders go to the first stable node
    /// names and later nodes may receive zero: unlike `.max(1)`, the issued
    /// shares always sum to at most the provider account cap.
    fn budgets_for_node(&self, node: &str) -> HashMap<String, u16> {
        let nodes = self.budget_nodes();
        let position = nodes.iter().position(|candidate| candidate == node);
        let count = nodes.len() as u16;
        self.servers
            .iter()
            .map(|server| {
                let share = match (position, count) {
                    (Some(position), count) if count > 0 => {
                        let base = server.max_connections / count;
                        let remainder = server.max_connections % count;
                        base + u16::from((position as u16) < remainder)
                    }
                    _ => 0,
                };
                (server.name.clone(), share)
            })
            .collect()
    }

    async fn apply_local_budgets(&self) {
        // A PP-only leader may need NNTP for delayed PAR recovery. Its
        // engine independently disables ordinary queued downloads, so these
        // budgets authorize capacity without broadening file eligibility.
        let by_id = if self.cfg.download || self.cfg.post_process {
            let by_name = self.budgets_for_node(&self.cfg.node_name);
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
        .route("/cluster/v1/work/reject", post(work_reject))
        .with_state(shared)
}

fn authed(shared: &LeaderShared, headers: &HeaderMap) -> bool {
    secret_matches(
        headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()),
        &shared.cfg.secret,
    )
}

fn worker_admits_new_work(worker: &NodeRecord) -> bool {
    worker.disk_guard_capable && !worker.disk_low
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

async fn leader_info(State(s): State<Arc<LeaderShared>>, headers: HeaderMap) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
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
    // The registry is the leader's independent admission fact. Do not trust
    // slot counts from a poll whose node is already known to be held.
    if read_nodes(&s.layout)
        .into_iter()
        .find(|node| node.name == req.node)
        .is_none_or(|node| !worker_admits_new_work(&node))
    {
        return Json(PollResponse::default()).into_response();
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
            server_budgets: s.budgets_for_node(&req.node),
            post_fetch_budgeted: true,
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
            // The divisor already includes this PP executor. It may not use
            // the allowance for ordinary files; the engine's explicit
            // delayed-PAR lane enforces that separate authorization.
            server_budgets: s.budgets_for_node(&req.node),
            post_fetch_budgeted: true,
        });
    }

    if !grants.is_empty() {
        s.apply_local_budgets().await;
    }
    Json(PollResponse { grants }).into_response()
}

async fn work_reject(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<RejectRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    let released = {
        let mut leases = s.leases.lock().unwrap();
        leases
            .get(&req.lease_id)
            .is_some_and(|lease| lease.node == req.node)
            .then(|| leases.remove(&req.lease_id))
            .flatten()
    };
    if let Some(lease) = released {
        tracing::info!(
            job = lease.job.0,
            node = %req.node,
            lease = %req.lease_id,
            "worker rejected grant after its local admission state changed"
        );
        let _ = s.engine.set_delegated(lease.job, None).await;
        s.apply_local_budgets().await;
        Json(RejectResponse { released: true }).into_response()
    } else {
        Json(RejectResponse { released: false }).into_response()
    }
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
            s.engine.mirror_progress(lp.job, lp.stats.clone());
        }
    }
    Json(HeartbeatResponse {
        cancel,
        server_budgets: Some(s.budgets_for_node(&req.node)),
        post_fetch_budgeted: true,
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
        let mut authority_ready = false;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let is_leader = shared.is_leader();
            if is_leader && !was_leader {
                // Taking office: discard leases inherited from the old view.
                // New leases arrive via worker heartbeats or fresh grants.
                shared.leases.lock().unwrap().clear();
            }
            if is_leader && !authority_ready {
                // Retry a refused adoption while this node remains leader.
                // The engine leaves both local and shared state unchanged on
                // refusal, so an operator can repair the snapshot in place
                // without restarting the daemon or forcing an election flap.
                match shared.engine.adopt_authority().await {
                    Ok(()) => {
                        authority_ready = true;
                        tracing::info!(epoch = shared.epoch(), "leader task active");
                    }
                    Err(error) => {
                        authority_ready = false;
                        tracing::error!(
                            epoch = shared.epoch(),
                            error = %error,
                            "leader scheduling disabled because queue authority adoption failed"
                        );
                    }
                }
            } else if !is_leader {
                authority_ready = false;
            }
            was_leader = is_leader;

            if is_leader && authority_ready {
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

    // Retire: post-processed terminal jobs move out of the queue — their
    // record of existence is the history store (NZBGet parity). Applies to
    // jobs PP'd remotely (imported stamped via work/complete) and locally.
    for j in snap.jobs.iter() {
        if j.pp_done && matches!(j.status, JobStatus::Completed | JobStatus::Failed) {
            tracing::info!(job = j.id.0, "retiring finished job to history");
            let _ = s.engine.remove_job_silent(j.id).await;
        }
    }

    // Reconcile: a job assigned to a node that is no longer live and holds
    // no lease for it was delegated into the void (node died between
    // assignment and poll, or vanished entirely). Release it.
    {
        let live: HashSet<&str> = workers.iter().map(|w| w.name.as_str()).collect();
        let disk_held: HashSet<&str> = workers
            .iter()
            .filter(|worker| !worker_admits_new_work(worker))
            .map(|worker| worker.name.as_str())
            .collect();
        let leased: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
        for j in snap.jobs.iter() {
            if let Some(node) = j.assigned_node.as_deref() {
                if node != s.cfg.node_name
                    && (!live.contains(node) || disk_held.contains(node))
                    && !leased.contains(&j.id)
                    && !matches!(j.status, JobStatus::Deleted)
                {
                    tracing::warn!(
                        job = j.id.0,
                        %node,
                        disk_low = disk_held.contains(node),
                        "assigned node is unavailable; releasing delegation"
                    );
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
        .filter(|w| w.download && w.max_download_jobs > 0 && worker_admits_new_work(w))
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
    let self_capacity = if s.cfg.download && !snap.disk_low {
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
    for w in workers
        .iter()
        .filter(|w| w.post_process && w.pp_slots > 0 && worker_admits_new_work(w))
    {
        let pp_held = pp_leases_by_node.get(&w.name).copied().unwrap_or(0)
            + assigned_pp_backlog(&snap, &w.name, &leased_jobs);
        let free = w.pp_slots.saturating_sub(pp_held);
        if free > 0 {
            let downloading =
                leases_by_node.get(&w.name).copied().unwrap_or(0) > 0 || w.active_download_jobs > 0;
            pp_targets.push((w.name.clone(), free, downloading));
        }
    }
    if s.cfg.post_process && s.cfg.pp_slots > 0 && !snap.disk_low {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::LeaderRecord;
    use axum::extract::State;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_types::{CertLevel, DupeInfo, Job, JobKind, JobTotals, ServerDef, ServerId, TlsMode};

    #[test]
    fn legacy_worker_without_disk_guard_capability_is_excluded() {
        let old = serde_json::json!({
            "name": "old",
            "api_url": "http://old",
            "download": true,
            "post_process": true,
            "max_download_jobs": 1,
            "active_download_jobs": 0,
            "pp_slots": 1,
            "rate_bps": 0,
            "seq": 1
        });
        let mut record: NodeRecord = serde_json::from_value(old).unwrap();
        assert!(!worker_admits_new_work(&record));
        record.disk_guard_capable = true;
        assert!(worker_admits_new_work(&record));
        record.disk_low = true;
        assert!(!worker_admits_new_work(&record));
    }

    #[tokio::test]
    async fn post_leases_share_the_provider_account_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
        let provider = ServerDef {
            id: ServerId(1),
            name: "provider".into(),
            host: "127.0.0.1".into(),
            port: 119,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 9,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        };
        let mut scarce = provider.clone();
        scarce.id = ServerId(2);
        scarce.name = "scarce".into();
        scarce.max_connections = 1;
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![provider.clone(), scarce.clone()],
            layout.state_dir(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let (_view_tx, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 1,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let cfg = ClusterConfig {
            node_name: "leader".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: true,
            pp_slots: 1,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            disk_guard_roots: Vec::new(),
        };
        let shared = LeaderShared::new(engine.clone(), layout, cfg, vec![provider, scarce], view);
        for (lease, node) in [("pp-a", "worker-a"), ("pp-b", "worker-b")] {
            shared.leases.lock().unwrap().insert(
                lease.into(),
                LeaseInfo {
                    job: JobId(if node == "worker-a" { 1 } else { 2 }),
                    node: node.into(),
                    kind: LeaseKind::Post,
                    last_hb: Instant::now(),
                },
            );
        }

        assert_eq!(shared.budget_nodes().len(), 3);
        let shares: Vec<_> = ["leader", "worker-a", "worker-b"]
            .iter()
            .map(|node| shared.budgets_for_node(node))
            .collect();
        assert_eq!(shares.iter().map(|share| share["provider"]).sum::<u16>(), 9);
        assert_eq!(shares.iter().map(|share| share["scarce"]).sum::<u16>(), 1);
        assert_eq!(
            shares.iter().filter(|share| share["scarce"] == 0).count(),
            2,
            "a one-connection account cannot issue one connection per executor"
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn rejected_transition_grant_releases_lease_and_delegation() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            layout.state_dir(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let job = Job {
            id: JobId(91),
            kind: JobKind::Nzb,
            name: "reject".into(),
            dir_name: "reject".into(),
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
        };
        engine.import_job(job, false, false).await.unwrap();
        assert!(engine
            .set_delegated(JobId(91), Some("worker".into()))
            .await
            .unwrap());

        let (_view_tx, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 3,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let cfg = ClusterConfig {
            node_name: "leader".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            disk_guard_roots: Vec::new(),
        };
        let shared = LeaderShared::new(engine.clone(), layout, cfg, Vec::new(), view);
        shared.leases.lock().unwrap().insert(
            "transition-lease".into(),
            LeaseInfo {
                job: JobId(91),
                node: "worker".into(),
                kind: LeaseKind::Download,
                last_hb: Instant::now(),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        let response = work_reject(
            State(shared.clone()),
            headers,
            Json(RejectRequest {
                node: "worker".into(),
                lease_id: "transition-lease".into(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(shared.leases.lock().unwrap().is_empty());
        assert_eq!(
            engine
                .snapshot()
                .jobs
                .iter()
                .find(|job| job.id == JobId(91))
                .unwrap()
                .assigned_node,
            None
        );
        engine.shutdown().await;
    }
}
