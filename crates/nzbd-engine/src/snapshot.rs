//! The lock-free read model. The queue-owner task publishes an immutable
//! [`QueueSnapshot`] via `arc-swap` (debounced to its 1 Hz tick plus
//! structural changes); API handlers load it without ever blocking the
//! engine (ARCHITECTURE.md §8.1).

use arc_swap::ArcSwap;
use nzbd_types::{JobId, JobStatus};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub id: JobId,
    pub name: String,
    pub status: JobStatus,
    pub category: Option<String>,
    pub priority: i32,
    pub size_bytes: u64,
    pub downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub remaining_bytes: u64,
    pub total_articles: u32,
    pub done_articles: u32,
    pub failed_articles: u32,
    pub files_total: u32,
    pub files_done: u32,
    /// Per-mille (NZBGet scale: 1000 = 100.0%).
    pub health: u16,
    pub critical_health: u16,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueSnapshot {
    pub up_since_unix: i64,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
    pub download_rate_bps: u64,
    pub session_downloaded_bytes: u64,
    /// Bytes still to fetch across active jobs (non-paused files).
    pub remaining_bytes: u64,
    pub jobs: Vec<JobSummary>,
}

pub type SharedSnapshot = Arc<ArcSwap<QueueSnapshot>>;

pub fn new_shared_snapshot() -> SharedSnapshot {
    Arc::new(ArcSwap::from_pointee(QueueSnapshot::default()))
}
