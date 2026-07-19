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

pub mod logbuf;
pub use logbuf::{LogBuffer, LogBufferLayer};
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
    pub log: Option<Arc<LogBuffer>>,
    /// First-run setup mode: present when the daemon booted with a
    /// `--config` path that doesn't exist yet. The UI offers a setup form;
    /// `POST /api/v1/setup` writes the file and asks the daemon to reload.
    pub setup: Option<Arc<SetupHandle>>,
}

/// Shared handle between the setup endpoint and the daemon's run loop.
pub struct SetupHandle {
    /// Where the config file will be written.
    pub config_path: std::path::PathBuf,
    /// The effective listen address (recorded into the written config so
    /// a later bare `nzbd run --config …` binds the same way).
    pub bind: String,
    /// Signals the run loop to tear down and re-run with the new config.
    pub reload: tokio::sync::Notify,
    /// True once a config has been written (the run loop turns this into
    /// a reload instead of an exit).
    pub applied: std::sync::atomic::AtomicBool,
}

impl SetupHandle {
    pub fn new(config_path: std::path::PathBuf, bind: String) -> Self {
        SetupHandle {
            config_path,
            bind,
            reload: tokio::sync::Notify::new(),
            applied: std::sync::atomic::AtomicBool::new(false),
        }
    }
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
                if auth_exempt(req.uri().path()) {
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

/// The embedded web UI (phase 4): one self-contained page, no build step.
async fn ui_index() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../ui/index.html"),
    )
        .into_response()
}

// PWA assets, all compiled into the binary. Cache header keeps phones
// from re-fetching icons on every open; the shell itself (/ and sw.js)
// stays revalidated so UI updates land immediately.
fn asset(ctype: &'static str, cache: bool, bytes: &'static [u8]) -> Response {
    let cache_control = if cache {
        "public, max-age=86400"
    } else {
        "no-cache"
    };
    (
        [
            (axum::http::header::CONTENT_TYPE, ctype),
            (axum::http::header::CACHE_CONTROL, cache_control),
        ],
        bytes,
    )
        .into_response()
}

async fn pwa_manifest() -> Response {
    asset(
        "application/manifest+json",
        false,
        include_bytes!("../ui/manifest.webmanifest"),
    )
}
async fn pwa_sw() -> Response {
    asset("text/javascript", false, include_bytes!("../ui/sw.js"))
}
async fn icon_192() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-192.png"),
    )
}
async fn icon_512() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-512.png"),
    )
}
async fn icon_maskable() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-maskable-512.png"),
    )
}
async fn apple_touch_icon() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/apple-touch-icon.png"),
    )
}

/// Paths that must work without credentials: health for probes, and the
/// PWA identity assets — browsers fetch the manifest, icons and service
/// worker updates without sending Authorization, and a 401 there breaks
/// install/updates. They carry no user data.
fn auth_exempt(path: &str) -> bool {
    matches!(
        path,
        "/healthz" | "/manifest.webmanifest" | "/sw.js" | "/apple-touch-icon.png"
    ) || path.starts_with("/icons/")
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
    /// Fetch the NZB from this URL instead of the request body.
    url: Option<String>,
    paused: Option<bool>,
    dupe_key: Option<String>,
    dupe_score: Option<i32>,
}

/// `POST /api/v1/jobs` with the raw NZB document as the request body.
/// (Multipart and `{url}` forms arrive in phase 3.)
async fn add_job(
    State(st): State<ApiState>,
    Query(q): Query<AddJobQuery>,
    body: axum::body::Bytes,
) -> Response {
    let name = q.name.unwrap_or_default();
    let opts = nzbd_engine::AddOpts {
        category: q.category,
        priority: q.priority.unwrap_or(0),
        paused: q.paused.unwrap_or(false),
        dupe: q.dupe_key.map(|key| nzbd_types::DupeInfo {
            key,
            score: q.dupe_score.unwrap_or(0),
            mode: None,
        }),
    };
    if let Some(url) = &q.url {
        return match st.engine.add_url(&name, url, opts).await {
            Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
            Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
        };
    }
    if body.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "empty body; POST the NZB document (or pass ?url=)",
        );
    }
    match st.engine.add_nzb_opts(&name, &body, opts).await {
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
struct LogsQuery {
    after: Option<u64>,
    limit: Option<usize>,
}

/// `GET /api/v1/logs` — recent daemon log entries from the in-memory ring.
async fn get_logs(State(st): State<ApiState>, Query(q): Query<LogsQuery>) -> Response {
    let Some(buf) = &st.log else {
        return error(StatusCode::NOT_IMPLEMENTED, "log buffer not configured");
    };
    let limit = q.limit.unwrap_or(200).min(2000);
    let entries = match q.after {
        Some(after) => buf.since(after, limit),
        None => buf.tail(limit),
    };
    Json(json!({ "entries": entries })).into_response()
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

/// `GET /api/v1/openapi.json` — a compact machine-readable surface
/// summary (full schema docs are generated in a later pass).
async fn openapi() -> Response {
    Json(json!({
        "openapi": "3.0.3",
        "info": { "title": "nzbd native API", "version": env!("CARGO_PKG_VERSION") },
        "paths": {
            "/api/v1/status": { "get": { "summary": "Queue + rate + guard status" } },
            "/api/v1/jobs": {
                "get": { "summary": "List queue jobs" },
                "post": { "summary": "Add a job (NZB body, or ?url=)",
                          "parameters": [
                              {"name": "name", "in": "query"},
                              {"name": "category", "in": "query"},
                              {"name": "priority", "in": "query"},
                              {"name": "url", "in": "query"},
                              {"name": "paused", "in": "query"},
                              {"name": "dupe_key", "in": "query"},
                              {"name": "dupe_score", "in": "query"}
                          ] }
            },
            "/api/v1/jobs/{id}": { "get": { "summary": "Job detail" } },
            "/api/v1/jobs/{id}/actions/{action}": { "post": { "summary": "pause|resume|delete|delete-files" } },
            "/api/v1/queue/actions/{action}": { "post": { "summary": "pause|resume" } },
            "/api/v1/queue/speed-limit": { "put": { "summary": "Set speed limit (bytes_per_sec)" } },
            "/api/v1/history": { "get": { "summary": "Finished jobs" } },
            "/api/v1/logs": { "get": { "summary": "Recent daemon log entries" } },
            "/api/v1/events": { "get": { "summary": "Engine events (SSE)" } },
            "/metrics": { "get": { "summary": "Prometheus exposition" } },
            "/healthz": { "get": { "summary": "Liveness" } }
        }
    }))
    .into_response()
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
        log: None,
        setup: None,
    })
}

// ---------------------------------------------------------------------------
// First-run setup
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct SetupServerReq {
    #[serde(default)]
    name: Option<String>,
    host: String,
    #[serde(default = "default_nntp_port")]
    port: u16,
    #[serde(default = "default_true")]
    tls: bool,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    connections: Option<u16>,
}

fn default_nntp_port() -> u16 {
    563
}
fn default_true() -> bool {
    true
}

#[derive(serde::Deserialize)]
struct SetupReq {
    main_dir: String,
    dest_dir: String,
    server: SetupServerReq,
    #[serde(default)]
    api_password: Option<String>,
}

async fn get_setup(State(st): State<ApiState>) -> Response {
    match &st.setup {
        Some(s) => Json(json!({
            "setup_mode": !s.applied.load(std::sync::atomic::Ordering::Relaxed),
            "config_path": s.config_path.display().to_string(),
        }))
        .into_response(),
        None => Json(json!({ "setup_mode": false })).into_response(),
    }
}

async fn post_setup(State(st): State<ApiState>, Json(req): Json<SetupReq>) -> Response {
    let Some(setup) = &st.setup else {
        return error(StatusCode::NOT_FOUND, "not in setup mode");
    };
    if setup.applied.load(std::sync::atomic::Ordering::Relaxed) {
        return error(StatusCode::CONFLICT, "setup already applied; reloading");
    }
    if req.main_dir.trim().is_empty()
        || req.dest_dir.trim().is_empty()
        || req.server.host.is_empty()
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "main_dir, dest_dir and server.host are required",
        );
    }

    let mut cfg = nzbd_config::Config::default();
    cfg.paths.main_dir = req.main_dir.trim().into();
    cfg.paths.dest_dir = req.dest_dir.trim().into();
    cfg.api.bind = setup.bind.clone();
    cfg.api.password = req
        .api_password
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(String::from);
    cfg.servers.push(nzbd_config::ServerConfig {
        name: req
            .server
            .name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| "primary".into()),
        host: req.server.host.clone(),
        port: req.server.port,
        tls: req.server.tls,
        username: req.server.username.clone().filter(|s| !s.is_empty()),
        password: req.server.password.clone().filter(|s| !s.is_empty()),
        connections: req.server.connections.unwrap_or(8).max(1),
        ..nzbd_config::ServerConfig::default()
    });

    let toml_text = match nzbd_config::to_toml(&cfg) {
        Ok(t) => t,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    // Round-trip through the strict parser so we never write a config the
    // next boot would refuse.
    if let Err(e) = nzbd_config::Config::from_toml(&toml_text) {
        return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }
    if let Some(parent) = setup.config_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("create {}: {e}", parent.display()),
            );
        }
    }
    if let Err(e) = std::fs::write(&setup.config_path, &toml_text) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("write {}: {e}", setup.config_path.display()),
        );
    }
    tracing::info!(path = %setup.config_path.display(), "setup: configuration written; reloading");
    setup
        .applied
        .store(true, std::sync::atomic::Ordering::Relaxed);
    setup.reload.notify_one();
    Json(json!({ "written": setup.config_path.display().to_string(), "reloading": true }))
        .into_response()
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
        .route("/api/v1/logs", get(get_logs))
        .route("/metrics", get(metrics))
        .route("/api/v1/openapi.json", get(openapi))
        .route("/api/v1/setup", get(get_setup).post(post_setup))
        .route("/healthz", get(healthz))
        .route("/", get(ui_index))
        .route("/manifest.webmanifest", get(pwa_manifest))
        .route("/sw.js", get(pwa_sw))
        .route("/icons/icon-192.png", get(icon_192))
        .route("/icons/icon-512.png", get(icon_512))
        .route("/icons/icon-maskable-512.png", get(icon_maskable))
        .route("/apple-touch-icon.png", get(apple_touch_icon))
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
