//! Native REST API (`/api/v1`). Phase 0: status/healthz skeleton over a
//! shared snapshot; jobs/history/servers/config/SSE land with the engine.

use arc_swap::ArcSwap;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;

/// Read model published by the engine (arc-swap'd, lock-free for readers).
#[derive(Debug, Default, Clone, Serialize)]
pub struct Snapshot {
    pub download_rate_bps: u64,
    pub remaining_bytes: u64,
    pub queued_jobs: u32,
    pub download_paused: bool,
    pub post_paused: bool,
    pub up_since_unix: i64,
}

pub type SharedSnapshot = Arc<ArcSwap<Snapshot>>;

pub fn new_shared_snapshot() -> SharedSnapshot {
    Arc::new(ArcSwap::from_pointee(Snapshot::default()))
}

#[derive(Debug, Serialize)]
pub struct StatusDto {
    pub version: &'static str,
    pub download_rate_bps: u64,
    pub remaining_bytes: u64,
    pub queued_jobs: u32,
    pub download_paused: bool,
    pub post_paused: bool,
}

pub fn status_dto(snap: &Snapshot) -> StatusDto {
    StatusDto {
        version: env!("CARGO_PKG_VERSION"),
        download_rate_bps: snap.download_rate_bps,
        remaining_bytes: snap.remaining_bytes,
        queued_jobs: snap.queued_jobs,
        download_paused: snap.download_paused,
        post_paused: snap.post_paused,
    }
}

async fn get_status(State(snap): State<SharedSnapshot>) -> Json<StatusDto> {
    Json(status_dto(&snap.load()))
}

async fn healthz() -> &'static str {
    "ok"
}

pub fn router(snapshot: SharedSnapshot) -> Router {
    Router::new()
        .route("/api/v1/status", get(get_status))
        .route("/healthz", get(healthz))
        .with_state(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_dto_reflects_snapshot() {
        let snap = Snapshot {
            download_rate_bps: 1_000_000,
            remaining_bytes: 42,
            queued_jobs: 3,
            download_paused: true,
            ..Default::default()
        };
        let dto = status_dto(&snap);
        assert_eq!(dto.download_rate_bps, 1_000_000);
        assert_eq!(dto.remaining_bytes, 42);
        assert_eq!(dto.queued_jobs, 3);
        assert!(dto.download_paused);
        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["queued_jobs"], 3);
    }
}
