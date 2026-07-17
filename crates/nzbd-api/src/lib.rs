//! Native REST API (`/api/v1`): status, job CRUD, queue controls,
//! history, SSE events, Prometheus `/metrics` and HTTP auth
//! (ARCHITECTURE.md §10.1). OpenAPI + roles are the remaining items.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine as _;
use nzbd_engine::{EngineHandle, JobSummary, QueueSnapshot};
use nzbd_state::history::HistoryDb;
use nzbd_types::{JobId, JobStatus};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio_stream::StreamExt as _;

/// Router state: the engine plus optional stores wired by the daemon.
#[derive(Clone)]
pub struct ApiState {
    pub engine: EngineHandle,
    pub history: Option<Arc<HistoryDb>>,
}

/// HTTP auth requirements (NZBGet `ControlUsername`/`ControlPassword`
/// parity plus a bearer token). Enforced only when a password or token is
/// configured; `/healthz` is always open.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub username: String,
    pub password: Option<String>,
    pub token: Option<String>,
}

impl AuthConfig {
    pub fn required(&self) -> bool {
        self.password.is_some() || self.token.is_some()
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().min(b.len()).max(1) {
        let (x, y) = (
            a.get(i).copied().unwrap_or(0),
            b.get(i).copied().unwrap_or(0),
        );
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

fn authorized(auth: &AuthConfig, header: Option<&str>) -> bool {
    if !auth.required() {
        return true;
    }
    let Some(header) = header else { return false };
    if let Some(token) = header.strip_prefix("Bearer ") {
        if let Some(want) = &auth.token {
            return constant_time_eq(token.trim(), want);
        }
        return false;
    }
    if let Some(b64) = header.strip_prefix("Basic ") {
        let Some(want_pw) = &auth.password else {
            return false;
        };
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
            return false;
        };
        let decoded = String::from_utf8_lossy(&decoded);
        let Some((user, pass)) = decoded.split_once(':') else {
            return false;
        };
        return constant_time_eq(user, &auth.username) & constant_time_eq(pass, want_pw);
    }
    false
}

/// Wrap a router with auth enforcement. `/healthz` stays open; everything
/// else answers 401 (with a Basic challenge, which NZBGet clients expect)
/// until credentials match.
pub fn require_auth(router: Router, auth: AuthConfig) -> Router {
    if !auth.required() {
        return router;
    }
    let auth = Arc::new(auth);
    router.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let auth = auth.clone();
            async move {
                if req.uri().path() == "/healthz" {
                    return next.run(req).await;
                }
                let header = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                if authorized(&auth, header.as_deref()) {
                    next.run(req).await
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        [(axum::http::header::WWW_AUTHENTICATE, "Basic realm=\"nzbd\"")],
                        "unauthorized",
                    )
                        .into_response()
                }
            }
        },
    ))
}

#[derive(Debug, Serialize)]
pub struct StatusDto {
    pub version: &'static str,
    pub up_since_unix: i64,
    pub download_rate_bps: u64,
    pub remaining_bytes: u64,
    pub session_downloaded_bytes: u64,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
    pub jobs_queued: u32,
    pub jobs_downloading: u32,
    pub jobs_finished: u32,
}

pub fn status_dto(snap: &QueueSnapshot) -> StatusDto {
    let count =
        |pred: &dyn Fn(&JobSummary) -> bool| snap.jobs.iter().filter(|j| pred(j)).count() as u32;
    StatusDto {
        version: env!("CARGO_PKG_VERSION"),
        up_since_unix: snap.up_since_unix,
        download_rate_bps: snap.download_rate_bps,
        remaining_bytes: snap.remaining_bytes,
        session_downloaded_bytes: snap.session_downloaded_bytes,
        download_paused: snap.download_paused,
        speed_limit_bps: snap.speed_limit_bps,
        jobs_queued: count(&|j| matches!(j.status, JobStatus::Queued | JobStatus::Paused)),
        jobs_downloading: count(&|j| matches!(j.status, JobStatus::Downloading)),
        jobs_finished: count(&|j| {
            matches!(
                j.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        }),
    }
}

async fn get_status(State(st): State<ApiState>) -> Json<StatusDto> {
    Json(status_dto(&st.engine.snapshot()))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_jobs(State(st): State<ApiState>) -> Response {
    let snap = st.engine.snapshot();
    Json(json!({ "jobs": snap.jobs })).into_response()
}

async fn get_job(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    let snap = st.engine.snapshot();
    match snap.jobs.iter().find(|j| j.id == JobId(id)) {
        Some(job) => Json(job.clone()).into_response(),
        None => not_found(),
    }
}

#[derive(Debug, Deserialize)]
struct AddJobQuery {
    name: Option<String>,
    category: Option<String>,
    priority: Option<i32>,
}

/// `POST /api/v1/jobs` with the raw NZB document as the request body.
/// (Multipart and `{url}` forms arrive in phase 3.)
async fn add_job(
    State(st): State<ApiState>,
    Query(q): Query<AddJobQuery>,
    body: axum::body::Bytes,
) -> Response {
    if body.is_empty() {
        return error(StatusCode::BAD_REQUEST, "empty body; POST the NZB document");
    }
    let name = q.name.unwrap_or_default();
    match st
        .engine
        .add_nzb(&name, &body, q.category, q.priority.unwrap_or(0))
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
    }
}

async fn job_action(
    State(st): State<ApiState>,
    Path((id, action)): Path<(u32, String)>,
) -> Response {
    let engine = &st.engine;
    let job = JobId(id);
    let result = match action.as_str() {
        "pause" => engine.pause_job(job).await,
        "resume" => engine.resume_job(job).await,
        "delete" => engine.delete_job(job, false).await,
        "delete-files" => engine.delete_job(job, true).await,
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "unknown action (pause|resume|delete|delete-files)",
            )
        }
    };
    match result {
        Ok(true) => Json(json!({ "ok": true })).into_response(),
        Ok(false) => not_found(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

async fn queue_action(State(st): State<ApiState>, Path(action): Path<String>) -> Response {
    let engine = &st.engine;
    let result = match action.as_str() {
        "pause" => engine.pause_all().await,
        "resume" => engine.resume_all().await,
        _ => return error(StatusCode::BAD_REQUEST, "unknown action (pause|resume)"),
    };
    match result {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct SpeedLimitBody {
    bytes_per_sec: Option<u64>,
}

async fn set_speed_limit(State(st): State<ApiState>, Json(body): Json<SpeedLimitBody>) -> Response {
    match st.engine.set_speed_limit(body.bytes_per_sec).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

/// `GET /api/v1/events` — engine events as SSE (`event:` = variant name,
/// `data:` = JSON payload). Lagged consumers observe a `lagged` event and
/// should resync from `/api/v1/status`.
async fn sse_events(State(st): State<ApiState>) -> Response {
    let rx = st.engine.subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).map(|ev| match ev {
        Ok(ev) => {
            let (name, data) = event_wire(&ev);
            Ok::<SseEvent, std::convert::Infallible>(SseEvent::default().event(name).data(data))
        }
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
            Ok(SseEvent::default()
                .event("lagged")
                .data(json!({ "skipped": n }).to_string()))
        }
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn event_wire(ev: &nzbd_engine::Event) -> (&'static str, String) {
    use nzbd_engine::Event as E;
    match ev {
        E::JobAdded { job, name } => ("job_added", json!({"job": job.0, "name": name}).to_string()),
        E::JobFinished {
            job,
            name,
            status,
            health,
        } => (
            "job_finished",
            json!({"job": job.0, "name": name, "status": status, "health": health}).to_string(),
        ),
        E::JobDeleted { job } => ("job_deleted", json!({"job": job.0}).to_string()),
        E::FileFinished {
            job,
            file,
            filename,
            ok,
        } => (
            "file_finished",
            json!({"job": job.0, "file": file.0, "filename": filename, "ok": ok}).to_string(),
        ),
        E::SegmentExhausted { job, file, segment } => (
            "segment_exhausted",
            json!({"job": job.0, "file": file.0, "segment": segment}).to_string(),
        ),
        E::ServerBlocked { server, seconds } => (
            "server_blocked",
            json!({"server": server.0, "seconds": seconds}).to_string(),
        ),
        other => ("event", json!({"debug": format!("{other:?}")}).to_string()),
    }
}

/// `GET /metrics` — Prometheus text exposition from the queue snapshot.
async fn metrics(State(st): State<ApiState>) -> Response {
    let snap = st.engine.snapshot();
    let mut by_status: std::collections::BTreeMap<&'static str, u32> = Default::default();
    for j in snap.jobs.iter() {
        let k = match j.status {
            JobStatus::Queued => "queued",
            JobStatus::Downloading => "downloading",
            JobStatus::Paused => "paused",
            JobStatus::Fetching => "fetching",
            JobStatus::PostQueued | JobStatus::Post { .. } => "post_processing",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Deleted => "deleted",
        };
        *by_status.entry(k).or_insert(0) += 1;
    }
    use std::fmt::Write;
    let mut out = String::with_capacity(1024);
    let m = &mut out;
    let _ = writeln!(m, "# TYPE nzbd_download_rate_bytes_per_second gauge");
    let _ = writeln!(
        m,
        "nzbd_download_rate_bytes_per_second {}",
        snap.download_rate_bps
    );
    let _ = writeln!(m, "# TYPE nzbd_remaining_bytes gauge");
    let _ = writeln!(m, "nzbd_remaining_bytes {}", snap.remaining_bytes);
    let _ = writeln!(m, "# TYPE nzbd_session_downloaded_bytes counter");
    let _ = writeln!(
        m,
        "nzbd_session_downloaded_bytes {}",
        snap.session_downloaded_bytes
    );
    let _ = writeln!(m, "# TYPE nzbd_download_paused gauge");
    let _ = writeln!(m, "nzbd_download_paused {}", snap.download_paused as u8);
    let _ = writeln!(m, "# TYPE nzbd_speed_limit_bytes_per_second gauge");
    let _ = writeln!(
        m,
        "nzbd_speed_limit_bytes_per_second {}",
        snap.speed_limit_bps.unwrap_or(0)
    );
    let _ = writeln!(m, "# TYPE nzbd_jobs gauge");
    for (k, v) in by_status {
        let _ = writeln!(m, "nzbd_jobs{{status=\"{k}\"}} {v}");
    }
    let _ = writeln!(m, "# TYPE nzbd_up_since_seconds gauge");
    let _ = writeln!(m, "nzbd_up_since_seconds {}", snap.up_since_unix);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
}

/// `GET /api/v1/history` — completed/failed jobs (NZBGet parity: finished
/// jobs leave the queue and live here).
async fn get_history(State(st): State<ApiState>, Query(q): Query<HistoryQuery>) -> Response {
    let Some(db) = &st.history else {
        return error(StatusCode::NOT_IMPLEMENTED, "history store not configured");
    };
    let db = db.clone();
    let limit = q.limit.unwrap_or(200).min(10_000);
    let entries = tokio::task::spawn_blocking(move || {
        let _ = db.refresh(); // pick up other nodes' appends (throttled)
        db.list(limit)
    })
    .await;
    match entries {
        Ok(Ok(entries)) => Json(json!({ "entries": entries })).into_response(),
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "no such job")
}

fn error(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

pub fn router(engine: EngineHandle) -> Router {
    router_with(ApiState {
        engine,
        history: None,
    })
}

pub fn router_with(state: ApiState) -> Router {
    Router::new()
        .route("/api/v1/status", get(get_status))
        .route("/api/v1/jobs", get(list_jobs).post(add_job))
        .route("/api/v1/jobs/{id}", get(get_job))
        .route("/api/v1/jobs/{id}/actions/{action}", post(job_action))
        .route("/api/v1/queue/actions/{action}", post(queue_action))
        .route("/api/v1/queue/speed-limit", put(set_speed_limit))
        .route("/api/v1/history", get(get_history))
        .route("/api/v1/events", get(sse_events))
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .with_state(state)
}

/// Re-exported so the daemon can hand the same snapshot to the compat shim.
pub fn snapshot(engine: &EngineHandle) -> Arc<QueueSnapshot> {
    engine.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use tower::util::ServiceExt;

    async fn test_engine(tmp: &tempfile::TempDir) -> EngineHandle {
        Engine::spawn(EngineConfig::single_node(
            vec![], // no connections; queue logic only
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap()
    }

    const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="1000" number="1">m1@x</segment></segments>
</file></nzb>"#;

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn add_list_status_and_actions() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        // Add.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=myjob&priority=50")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let v = body_json(resp).await;
        let id = v["id"].as_u64().unwrap();

        // List.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/jobs")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert_eq!(v["jobs"][0]["name"], "myjob");
        assert_eq!(v["jobs"][0]["priority"], 50);

        // Status.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert_eq!(v["jobs_queued"], 1);
        assert_eq!(v["remaining_bytes"], 1000);

        // Pause + resume + delete.
        for (action, expect) in [("pause", true), ("resume", true), ("delete", true)] {
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::post(format!("/api/v1/jobs/{id}/actions/{action}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{action}");
            assert_eq!(body_json(resp).await["ok"], expect, "{action}");
        }

        // Gone now.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/jobs/{id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_bad_nzb_and_bad_actions() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .body(axum::body::Body::from("<html>nope</html>"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/1/actions/explode")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        engine.shutdown().await;
    }

    #[test]
    fn auth_matrix() {
        use base64::Engine as _;
        let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);

        // No credentials configured: everything passes.
        let open = AuthConfig::default();
        assert!(authorized(&open, None));

        let auth = AuthConfig {
            username: "paul".into(),
            password: Some("s3cret".into()),
            token: Some("tok123".into()),
        };
        assert!(!authorized(&auth, None));
        assert!(!authorized(&auth, Some("Basic definitely-not-b64!")));
        assert!(authorized(
            &auth,
            Some(&format!("Basic {}", b64("paul:s3cret")))
        ));
        assert!(!authorized(
            &auth,
            Some(&format!("Basic {}", b64("paul:wrong")))
        ));
        assert!(!authorized(
            &auth,
            Some(&format!("Basic {}", b64("eve:s3cret")))
        ));
        assert!(authorized(&auth, Some("Bearer tok123")));
        assert!(!authorized(&auth, Some("Bearer nope")));

        // Password-only config rejects bearer attempts.
        let basic_only = AuthConfig {
            username: "paul".into(),
            password: Some("pw".into()),
            token: None,
        };
        assert!(!authorized(&basic_only, Some("Bearer pw")));
        assert!(authorized(
            &basic_only,
            Some(&format!("Basic {}", b64("paul:pw")))
        ));
    }

    #[tokio::test]
    async fn auth_layer_guards_routes_but_not_healthz() {
        use base64::Engine as _;
        use tower::util::ServiceExt;
        let tmp = tempfile::tempdir().unwrap();
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let app = require_auth(
            router(engine.clone()),
            AuthConfig {
                username: "u".into(),
                password: Some("p".into()),
                token: None,
            },
        );

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(resp.headers().contains_key("www-authenticate"));

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "healthz stays open");

        let creds = base64::engine::general_purpose::STANDARD.encode("u:p");
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .header("authorization", format!("Basic {creds}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_exposition_shape() {
        use tower::util::ServiceExt;
        let tmp = tempfile::tempdir().unwrap();
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let app = router(engine.clone());
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("nzbd_download_rate_bytes_per_second"));
        assert!(text.contains("# TYPE nzbd_jobs gauge"));
        assert!(text.contains("nzbd_remaining_bytes 0"));
        engine.shutdown().await;
    }
}
