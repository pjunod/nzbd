//! Leader role (CLUSTERING.md §6): the work-lease endpoints, the lease
//! table with TTL reclaim, the assignment scheduler and connection-budget
//! partitioning. Active only while this node's election view says `is_me`;
//! handlers reject otherwise (workers re-resolve the leader and retry).

use crate::control::{ControlStore, LeaseClaim, LeaseToken, MutationOutcome};
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
    token: LeaseToken,
    job_revision: u64,
    control_revision: u64,
    last_hb: Instant,
}

pub struct LeaderShared {
    pub engine: EngineHandle,
    pub layout: SharedLayout,
    pub cfg: ClusterConfig,
    pub servers: Vec<ServerDef>,
    pub view: watch::Receiver<LeaderView>,
    pub control: Option<ControlStore>,
    pub owner_incarnation: String,
    leases: Mutex<HashMap<String, LeaseInfo>>,
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
        control: Option<ControlStore>,
        owner_incarnation: String,
    ) -> Arc<LeaderShared> {
        Arc::new(LeaderShared {
            engine,
            layout,
            cfg,
            servers,
            view,
            control,
            owner_incarnation,
            leases: Mutex::new(HashMap::new()),
            node_seen: Mutex::new(HashMap::new()),
        })
    }

    fn is_leader(&self) -> bool {
        self.view.borrow().is_me
    }

    fn epoch(&self) -> u64 {
        self.view.borrow().epoch()
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

    async fn grant_job(
        &self,
        node: &str,
        owner_incarnation: &str,
        kind: LeaseKind,
        job: nzbd_types::Job,
        scope: serde_json::Value,
    ) -> Option<Grant> {
        use sha2::{Digest, Sha256};

        let control = self.control.as_ref()?;
        let intent_json = serde_json::to_string(&job).ok()?;
        let proposed_incarnation = format!("job-{:x}", Sha256::digest(intent_json.as_bytes()));
        if let Err(error) = control
            .seed_job(job.id.0.into(), &proposed_incarnation, &intent_json)
            .await
        {
            tracing::warn!(job = job.id.0, %error, "could not seed replicated job");
            return None;
        }
        let (job_incarnation, job_revision) = match control.job_identity(job.id.0.into()).await {
            Ok(Some(identity)) => identity,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(job = job.id.0, %error, "could not read replicated job identity");
                return None;
            }
        };
        let kind_name = match kind {
            LeaseKind::Download => "download",
            LeaseKind::Post => "post",
            LeaseKind::Segment => "segment",
            LeaseKind::Assemble => "assemble",
        };
        let resource = format!("work/job/{}/{kind_name}", job.id.0);
        let scope_json = serde_json::to_string(&scope).ok()?;
        let token = match control
            .acquire(
                &resource,
                &self.cfg.cluster_id,
                job.id.0.into(),
                &job_incarnation,
                node,
                owner_incarnation,
                kind_name,
                &scope_json,
                job_revision,
                self.cfg.worker_ttl,
            )
            .await
        {
            Ok(LeaseClaim::Acquired(token)) => token,
            Ok(LeaseClaim::Held { .. }) => return None,
            Err(error) => {
                tracing::warn!(job = job.id.0, %error, "replicated work lease acquire failed");
                return None;
            }
        };
        let lease_id = format!("{}@{}", token.resource, token.fence);
        self.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job.id,
                node: node.to_owned(),
                kind,
                token: token.clone(),
                job_revision,
                control_revision: job_revision,
                last_hb: Instant::now(),
            },
        );
        Some(Grant {
            lease_id,
            token,
            job_incarnation,
            job_revision,
            control_revision: job_revision,
            scope,
            epoch: self.epoch(),
            kind,
            job,
            server_budgets: self.budgets_for_node(node),
            post_fetch_budgeted: true,
        })
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
        if let Some(grant) = s
            .grant_job(
                &req.node,
                &req.owner_incarnation,
                LeaseKind::Download,
                job,
                serde_json::json!({"whole_job": true}),
            )
            .await
        {
            tracing::info!(job = job_id.0, node = %req.node, lease = %grant.lease_id, "download lease granted");
            grants.push(grant);
        }
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
        if let Some(grant) = s
            .grant_job(
                &req.node,
                &req.owner_incarnation,
                LeaseKind::Post,
                job,
                serde_json::json!({"whole_job": true}),
            )
            .await
        {
            tracing::info!(job = job_id.0, node = %req.node, lease = %grant.lease_id, "pp lease granted");
            pp_granted += 1;
            grants.push(grant);
        }
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
    let candidate = s.leases.lock().unwrap().get(&req.lease_id).cloned();
    let release_ok = if let Some(lease) = &candidate {
        if lease.node != req.node || lease.token != req.token {
            false
        } else if let Some(control) = &s.control {
            control.release(&req.token).await.unwrap_or(false)
        } else {
            true
        }
    } else {
        false
    };
    let released = if release_ok {
        s.leases.lock().unwrap().remove(&req.lease_id)
    } else {
        None
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
    let mut renewed = Vec::new();
    let mut controls = HashMap::new();
    let snap = s.engine.snapshot();
    for lp in &req.leases {
        let candidate = s.leases.lock().unwrap().get(&lp.lease_id).cloned();
        let Some(info) = candidate else {
            // A new leader may rebuild this process-local projection only from
            // the exact live replicated token, never from the worker's claim.
            let stored = match &s.control {
                Some(control) => control
                    .current_lease(&lp.token.resource)
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            if stored.as_ref() != Some(&lp.token) {
                cancel.push(lp.lease_id.clone());
            }
            continue;
        };
        if info.node != req.node
            || info.job != lp.job
            || info.token != lp.token
            || !snap.jobs.iter().any(|job| job.id == lp.job)
        {
            cancel.push(lp.lease_id.clone());
            continue;
        }
        let next = match &s.control {
            Some(control) => control.renew(&lp.token, s.cfg.worker_ttl).await,
            None => Ok(None),
        };
        match next {
            Ok(Some(next)) => {
                let mut leases = s.leases.lock().unwrap();
                if let Some(current) = leases.get_mut(&lp.lease_id) {
                    if current.token == lp.token {
                        current.token = next.clone();
                        current.last_hb = Instant::now();
                        controls.insert(lp.lease_id.clone(), current.control_revision);
                        renewed.push(next);
                    }
                }
            }
            Ok(None) | Err(_) => cancel.push(lp.lease_id.clone()),
        }
    }
    for lp in &req.leases {
        if !cancel.contains(&lp.lease_id) {
            s.engine.mirror_progress(lp.job, lp.stats.clone());
        }
    }
    Json(HeartbeatResponse {
        cancel,
        renewed,
        controls,
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
    let candidate = s.leases.lock().unwrap().get(&req.lease_id).cloned();
    let Some(info) = candidate else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "unknown lease"})),
        )
            .into_response();
    };
    if info.node != req.node
        || info.job != job_id
        || info.token != req.token
        || info.job_revision != req.expected_job_revision
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stale lease token or job revision"})),
        )
            .into_response();
    }
    let outcome = match &s.control {
        Some(control) => {
            control
                .publish_result(
                    &req.receipt_id,
                    &req.token,
                    req.expected_job_revision,
                    &req.result_id,
                    &req.result_ref,
                )
                .await
        }
        None => Err("replicated control unavailable".into()),
    };
    match outcome {
        Ok(MutationOutcome::Applied { .. } | MutationOutcome::Duplicate { .. }) => {}
        Ok(MutationOutcome::Conflict) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "publication fence rejected"})),
            )
                .into_response();
        }
        Err(error) => {
            tracing::warn!(job = job_id.0, %error, "durable completion outcome unknown");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "completion outcome unknown"})),
            )
                .into_response();
        }
    }
    tracing::info!(job = job_id.0, node = %req.node, "job completed remotely");
    if let Err(error) = s.engine.import_job(req.job, false, true).await {
        tracing::error!(job = job_id.0, %error, "durable result accepted but local projection failed");
    }
    s.leases.lock().unwrap().remove(&req.lease_id);
    s.apply_local_budgets().await;
    Json(CompleteResponse {
        ok: true,
        durable_receipt: Some(req.receipt_id),
    })
    .into_response()
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
                        let replicated = match &shared.control {
                            Some(control) => control.load_jobs().await,
                            None => Err("replicated control unavailable".into()),
                        };
                        match replicated {
                            Ok(rows) => {
                                let jobs = rows.into_iter().map(|(job, _, _)| job).collect();
                                match shared.engine.adopt_replicated_authority(jobs).await {
                                    Ok(()) => {
                                        authority_ready = true;
                                        tracing::info!(epoch = shared.epoch(), "leader task active");
                                    }
                                    Err(error) => {
                                        authority_ready = false;
                                        tracing::error!(%error, "replicated queue projection failed");
                                    }
                                }
                            }
                            Err(error) => {
                                authority_ready = false;
                                tracing::error!(%error, "replicated queue authority unavailable");
                            }
                        }
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
                LeaseKind::Download | LeaseKind::Segment | LeaseKind::Assemble => {
                    *dl.entry(l.node.clone()).or_insert(0) += 1
                }
                LeaseKind::Post => *pp.entry(l.node.clone()).or_insert(0) += 1,
            }
        }
        (dl, pp)
    };

    // Assigned-but-not-polled jobs count as load. Otherwise a slow poller can
    // accumulate the whole queue before its first lease exists.
    let mut download_targets: Vec<(String, u32, u32, u32, bool)> = workers
        .iter()
        .filter(|w| w.download && w.max_download_jobs > 0 && worker_admits_new_work(w))
        .map(|w| {
            let held = leases_by_node.get(&w.name).copied().unwrap_or(0);
            let backlog = snap
                .jobs
                .iter()
                .filter(|job| job.assigned_node.as_deref() == Some(w.name.as_str()))
                .count() as u32;
            (
                w.name.clone(),
                w.max_download_jobs.saturating_sub(backlog.max(held)),
                backlog.max(held),
                w.download_weight.max(1),
                false,
            )
        })
        .collect();

    let self_active = snap
        .jobs
        .iter()
        .filter(|j| j.assigned_node.is_none() && matches!(j.status, JobStatus::Downloading))
        .count() as u32;
    if s.cfg.download && !snap.disk_low {
        download_targets.push((
            s.cfg.node_name.clone(),
            s.cfg.max_download_jobs.saturating_sub(self_active),
            self_active,
            s.cfg.download_weight.max(1),
            true,
        ));
    }

    // Assign untouched queued jobs: keep local while we have capacity,
    // then spread to the freest workers. Jobs with local progress stay
    // local (no mid-download migration in C1).
    for job in snap.jobs.iter() {
        if job.assigned_node.is_some() || !matches!(job.status, JobStatus::Queued) {
            continue;
        }
        download_targets.sort_by(|left, right| {
            (u64::from(left.2) * u64::from(right.3))
                .cmp(&(u64::from(right.2) * u64::from(left.3)))
                .then(left.0.cmp(&right.0))
        });
        let Some(slot) = download_targets.iter_mut().find(|target| target.1 > 0) else {
            break; // everyone is saturated
        };
        slot.1 -= 1;
        slot.2 += 1;
        let node = slot.0.clone();
        if !slot.4 {
            tracing::info!(job = job.id.0, %node, "delegating job by weighted load");
            let _ = s.engine.set_delegated(job.id, Some(node)).await;
        }
    }

    // ---- PP assignment (C2, CLUSTERING.md §13) ----------------------------
    // Anti-affinity: a node busy downloading is the LAST choice for par
    // repair / unpack — prefer idle PP-capable nodes so the same box never
    // runs both when the cluster has spare hands.
    let leased_jobs: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
    // (node, free_pp, downloading, assigned, weight)
    let mut pp_targets: Vec<(String, u32, bool, u32, u32)> = Vec::new();
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
            pp_targets.push((
                w.name.clone(),
                free,
                downloading,
                pp_held,
                w.pp_weight.max(1),
            ));
        }
    }
    if s.cfg.post_process && s.cfg.pp_slots > 0 && !snap.disk_low {
        let held = assigned_pp_backlog(&snap, &s.cfg.node_name, &leased_jobs);
        let free = s.cfg.pp_slots.saturating_sub(held);
        if free > 0 {
            pp_targets.push((
                s.cfg.node_name.clone(),
                free,
                self_active > 0,
                held,
                s.cfg.pp_weight.max(1),
            ));
        }
    }
    // Idle nodes first, then lowest assigned/weight, then stable name.
    pp_targets.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then(
                (u64::from(left.3) * u64::from(right.4))
                    .cmp(&(u64::from(right.3) * u64::from(left.4))),
            )
            .then(left.0.cmp(&right.0))
    });

    for job in snap.jobs.iter() {
        if !matches!(job.status, JobStatus::Completed) || job.pp_done || job.assigned_node.is_some()
        {
            continue;
        }
        let Some(slot) = pp_targets.iter_mut().find(|target| target.1 > 0) else {
            break;
        };
        slot.1 -= 1;
        slot.3 += 1;
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

    fn test_token(resource: &str) -> LeaseToken {
        LeaseToken {
            resource: resource.into(),
            owner_node_id: "worker".into(),
            owner_incarnation: "test-incarnation".into(),
            fence: 1,
            revision: 1,
            expires_at_unix_ms: i64::MAX,
        }
    }

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
            cluster_id: "test".into(),
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
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38110".into(),
            control_api_bind: "127.0.0.1:38210".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            cfg,
            vec![provider, scarce],
            view,
            None,
            "test-incarnation".into(),
        );
        for (lease, node) in [("pp-a", "worker-a"), ("pp-b", "worker-b")] {
            shared.leases.lock().unwrap().insert(
                lease.into(),
                LeaseInfo {
                    job: JobId(if node == "worker-a" { 1 } else { 2 }),
                    node: node.into(),
                    kind: LeaseKind::Post,
                    token: test_token(lease),
                    job_revision: 1,
                    control_revision: 1,
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
            cluster_id: "test".into(),
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
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38111".into(),
            control_api_bind: "127.0.0.1:38211".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            cfg,
            Vec::new(),
            view,
            None,
            "test-incarnation".into(),
        );
        shared.leases.lock().unwrap().insert(
            "transition-lease".into(),
            LeaseInfo {
                job: JobId(91),
                node: "worker".into(),
                kind: LeaseKind::Download,
                token: test_token("transition-lease"),
                job_revision: 1,
                control_revision: 1,
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
                token: test_token("transition-lease"),
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
