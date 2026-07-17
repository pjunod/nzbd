//! Native REST API (`/api/v1`), phase 1: status, job CRUD and queue
//! controls wired to the engine. SSE events, auth, OpenAPI and the full
//! surface of ARCHITECTURE.md §10.1 land in phase 3.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use nzbd_engine::{EngineHandle, JobSummary, QueueSnapshot};
use nzbd_types::{JobId, JobStatus};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

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
    let count = |pred: &dyn Fn(&JobSummary) -> bool| {
        snap.jobs.iter().filter(|j| pred(j)).count() as u32
    };
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

async fn get_status(State(engine): State<EngineHandle>) -> Json<StatusDto> {
    Json(status_dto(&engine.snapshot()))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_jobs(State(engine): State<EngineHandle>) -> Response {
    let snap = engine.snapshot();
    Json(json!({ "jobs": snap.jobs })).into_response()
}

async fn get_job(State(engine): State<EngineHandle>, Path(id): Path<u32>) -> Response {
    let snap = engine.snapshot();
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
    State(engine): State<EngineHandle>,
    Query(q): Query<AddJobQuery>,
    body: axum::body::Bytes,
) -> Response {
    if body.is_empty() {
        return error(StatusCode::BAD_REQUEST, "empty body; POST the NZB document");
    }
    let name = q.name.unwrap_or_default();
    match engine
        .add_nzb(&name, &body, q.category, q.priority.unwrap_or(0))
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
    }
}

async fn job_action(
    State(engine): State<EngineHandle>,
    Path((id, action)): Path<(u32, String)>,
) -> Response {
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

async fn queue_action(
    State(engine): State<EngineHandle>,
    Path(action): Path<String>,
) -> Response {
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

async fn set_speed_limit(
    State(engine): State<EngineHandle>,
    Json(body): Json<SpeedLimitBody>,
) -> Response {
    match engine.set_speed_limit(body.bytes_per_sec).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "no such job")
}

fn error(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

pub fn router(engine: EngineHandle) -> Router {
    Router::new()
        .route("/api/v1/status", get(get_status))
        .route("/api/v1/jobs", get(list_jobs).post(add_job))
        .route("/api/v1/jobs/{id}", get(get_job))
        .route("/api/v1/jobs/{id}/actions/{action}", post(job_action))
        .route("/api/v1/queue/actions/{action}", post(queue_action))
        .route("/api/v1/queue/speed-limit", put(set_speed_limit))
        .route("/healthz", get(healthz))
        .with_state(engine)
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
        Engine::spawn(EngineConfig {
            servers: vec![], // no connections; queue logic only
            state_dir: tmp.path().join("state"),
            dest_dir: tmp.path().join("dest"),
            tuning: Tuning::default(),
            speed_limit_bps: None,
        })
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
                    axum::http::Request::post(format!(
                        "/api/v1/jobs/{id}/actions/{action}"
                    ))
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
}
