//! File lifecycle API. Reads use the cached inventory; scans and copies are jobs.
use crate::{error, ApiState};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use nzbd_state::artifacts::{Error, Inventory, Receipt, Settings};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/artifacts", get(list))
        .route(
            "/api/v1/artifacts/settings",
            get(settings).put(put_settings),
        )
        .route("/api/v1/artifacts/scan", post(scan))
        .route("/api/v1/artifacts/{id}", get(detail))
        .route("/api/v1/artifacts/{id}/files", get(files))
        .route("/api/v1/artifacts/{id}/events", get(events))
        .route("/api/v1/artifacts/{id}/inspect", post(inspect))
        .route("/api/v1/artifacts/{id}/adopt", post(adopt))
        .route("/api/v1/artifacts/{id}/retention", post(retention))
        .route("/api/v1/artifacts/{id}/delete", post(delete))
        .route("/api/v1/artifact-operations/{id}", get(operation))
        .route("/api/v1/artifact-operations/{id}/cancel", post(cancel))
        .route("/api/v1/artifacts/{id}/recoveries", post(stage))
        .route("/api/v1/recoveries", get(recoveries))
        .route("/api/v1/recoveries/{id}", get(recovery))
        .route("/api/v1/recoveries/{id}/claim", post(claim))
        .route("/api/v1/recoveries/{id}/receipt", post(receipt))
        .route("/api/v1/recoveries/{id}/cancel", post(cancel_recovery))
        .route("/api/v1/recoveries/{id}/cancel-ack", post(cancel_ack))
}
fn failure(e: Error) -> Response {
    let status = match e {
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    error(status, &e.to_string())
}
async fn work<T: serde::Serialize + Send + 'static>(
    f: impl FnOnce() -> nzbd_state::artifacts::Result<T> + Send + 'static,
) -> Response {
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => failure(e),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}
#[derive(Default, Deserialize)]
struct Page {
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    after: i64,
}
async fn list(State(st): State<ApiState>, Query(p): Query<Page>) -> Response {
    let db = st.engine.artifacts();
    work(move||{let mut rows=db.list(p.offset,100)?;let now=nzbd_state::artifacts::now();let mut out=Vec::new();for a in &mut rows {let total=a.files.len();let bytes=a.files.iter().filter(|f|!f.identity.directory).map(|f|f.identity.bytes).sum::<u64>();a.files.clear();out.push(json!({"artifact":a,"files":total,"bytes":bytes,"earliest_expiry":a.earliest_expiry(now)}));}Ok(json!({"entries":out,"offset":p.offset,"limit":100}))}).await
}
async fn detail(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || {
        let mut a = db.get(&id)?;
        a.files.clear();
        Ok(a)
    })
    .await
}
async fn files(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Query(p): Query<Page>,
) -> Response {
    let db = st.engine.artifacts();
    work(move||{let a=db.get(&id)?;Ok(json!({"revision":a.revision,"total":a.files.len(),"files":a.files.into_iter().skip(p.offset).take(200).collect::<Vec<_>>()}))}).await
}
async fn events(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Query(p): Query<Page>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.events(&id, p.after)).await
}
async fn operation(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.operation(&id)).await
}
async fn cancel(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.cancel_delete(&id)).await
}
async fn inspect(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = db.inspect(&id) {
            tracing::warn!(artifact=%id,error=%e,"artifact inspection failed");
        }
    });
    (
        StatusCode::ACCEPTED,
        Json(json!({"state":"inspection_requested"})),
    )
        .into_response()
}
#[derive(Deserialize)]
struct Revision {
    revision: u64,
}
async fn adopt(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Revision>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.adopt(&id, body.revision)).await
}
#[derive(Deserialize)]
struct Retention {
    revision: u64,
    keep: bool,
    seconds: Option<u64>,
}
async fn retention(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Retention>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.retention(&id, body.revision, body.keep, body.seconds)).await
}
#[derive(Deserialize)]
struct Delete {
    revision: u64,
    idempotency_key: String,
    #[serde(default)]
    undo_seconds: u64,
}
async fn delete(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Delete>,
) -> Response {
    let db = st.engine.artifacts();
    match tokio::task::spawn_blocking(move || {
        db.request_delete(&id, body.revision, &body.idempotency_key, body.undo_seconds)
    })
    .await
    {
        Ok(Ok(op)) => (StatusCode::ACCEPTED, Json(op)).into_response(),
        Ok(Err(e)) => failure(e),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

fn config(st: &ApiState) -> Option<nzbd_config::Config> {
    st.setup.as_ref().map(|s| s.current.lock().unwrap().clone())
}
async fn settings(State(st): State<ApiState>) -> Response {
    let db = st.engine.artifacts();
    let cfg = config(&st);
    work(move||{let mut s=db.settings()?;let credential=!s.consumer_token.is_empty();s.consumer_token.clear();let mut advisory=Vec::new();advisory.push(json!({"requirement":"Consumer credential for claims and receipts","met":credential}));advisory.push(json!({"requirement":"Dedicated published recovery directory mounted read-only in Curator","met":null,"detail":"Configure /recovery separately from completed downloads; verify on the Curator host."}));if let Some(cfg)=cfg{advisory.push(json!({"requirement":"Single authoritative lifecycle writer","met":!cfg.cluster.enabled}));if s.recovery_root.as_os_str().is_empty(){s.recovery_root=nzbd_config::expand_home(&cfg.paths.main_dir).join("recovery");}}Ok(json!({"settings":s,"advisory":advisory,"installation":db.installation}))}).await
}
#[derive(Deserialize)]
struct SettingsUpdate {
    enabled: bool,
    failed_retention_days: u32,
    recovery_root: PathBuf,
    consumer_token: Option<String>,
}
async fn put_settings(State(st): State<ApiState>, Json(body): Json<SettingsUpdate>) -> Response {
    let db = st.engine.artifacts();
    work(move || {
        let current = db.settings()?;
        let next = Settings {
            enabled: body.enabled,
            failed_retention_days: body.failed_retention_days,
            recovery_root: body.recovery_root,
            consumer_token: body
                .consumer_token
                .filter(|s| !s.is_empty())
                .unwrap_or(current.consumer_token),
        };
        db.set_settings(&next)?;
        Ok(json!({"ok":true,"enabled":next.enabled}))
    })
    .await
}
async fn scan(State(st): State<ApiState>) -> Response {
    let Some(cfg) = config(&st) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "configuration unavailable");
    };
    let db = st.engine.artifacts();
    let active: Vec<PathBuf> = st
        .engine
        .snapshot()
        .jobs
        .iter()
        .map(|j| j.name.clone())
        .map(|n| cfg.dest_dir().join(n))
        .collect();
    tokio::task::spawn_blocking(move || {
        let mut roots = vec![
            nzbd_config::expand_home(&cfg.paths.main_dir),
            cfg.dest_dir(),
            cfg.post
                .failed_dir
                .clone()
                .unwrap_or_else(|| cfg.dest_dir().join(".failed")),
        ];
        if let Some(p) = &cfg.paths.inter_dir {
            roots.push(nzbd_config::expand_home(p));
        }
        roots.sort();
        roots.dedup();
        let excluded = cfg.storage_roots();
        let mut count = 0;
        for root in roots {
            let entries = match std::fs::read_dir(&root) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(path=%root.display(),error=%e,"artifact scan root unavailable");
                    continue;
                }
            };
            for entry in entries {
                if count >= 2000 {
                    return;
                }
                let Ok(entry) = entry else {
                    continue;
                };
                let path = entry.path();
                if excluded.iter().any(|r| r.path == path)
                    || path.file_name().is_some_and(|n| n == "recovery")
                {
                    continue;
                }
                if !entry
                    .file_type()
                    .is_ok_and(|t| t.is_dir() && !t.is_symlink())
                {
                    continue;
                }
                count += 1;
                if let Err(e) = db.discover(&root, &path, active.contains(&path)) {
                    tracing::warn!(path=%path.display(),error=%e,"artifact discovery failed");
                }
            }
        }
    });
    (
        StatusCode::ACCEPTED,
        Json(json!({"state":"scan_requested","max_directories":2000})),
    )
        .into_response()
}
#[derive(Deserialize)]
struct Stage {
    revision: u64,
    idempotency_key: String,
    files: Vec<String>,
}
async fn stage(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Stage>,
) -> Response {
    let db = st.engine.artifacts();
    let Some(cfg) = config(&st) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "configuration unavailable");
    };
    let settings = match db.settings() {
        Ok(s) => s,
        Err(e) => return failure(e),
    };
    let root = if settings.recovery_root.as_os_str().is_empty() {
        nzbd_config::expand_home(&cfg.paths.main_dir).join("recovery")
    } else {
        settings.recovery_root
    };
    if cfg
        .storage_roots()
        .iter()
        .any(|r| r.label != "working" && (root.starts_with(&r.path) || r.path.starts_with(&root)))
    {
        return error(
            StatusCode::CONFLICT,
            "recovery root overlaps a download, state, watch or category role",
        );
    }
    let key = body.idempotency_key.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = db.stage_recovery(
            &id,
            body.revision,
            &body.idempotency_key,
            &body.files,
            &root,
        ) {
            tracing::warn!(artifact=%id,error=%e,"recovery staging refused");
        }
    });
    (
        StatusCode::ACCEPTED,
        Json(json!({"request_id":key,"state":"staging_requested"})),
    )
        .into_response()
}
async fn recoveries(State(st): State<ApiState>, Query(p): Query<Page>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.recoveries(p.offset)).await
}
async fn recovery(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.recovery(&id)).await
}
fn consumer(db: &Arc<Inventory>, headers: &HeaderMap) -> Result<String, Response> {
    let s = db.settings().map_err(failure)?;
    let supplied = headers
        .get("x-recovery-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if s.consumer_token.is_empty() || supplied.as_bytes() != s.consumer_token.as_bytes() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "recovery consumer credential required",
        ));
    }
    Ok("curator".into())
}
#[derive(Deserialize)]
struct Claim {
    import_id: String,
    manifest_digest: String,
}
async fn claim(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Claim>,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.claim_recovery(&id, &who, &body.import_id, &body.manifest_digest)).await
}
async fn receipt(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Receipt>,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.recovery_receipt(&id, &who, body)).await
}
async fn cancel_recovery(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.cancel_recovery(&id, None, false)).await
}
async fn cancel_ack(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.cancel_recovery(&id, Some(&who), true)).await
}
