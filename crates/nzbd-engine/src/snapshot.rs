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
    /// This job's current download rate (EMA, bytes/sec; 0 unless
    /// actively downloading).
    pub rate_bps: u64,
    /// Cluster: node currently executing this job remotely (None = local).
    pub assigned_node: Option<String>,
    /// Post-processing already finished (the `*PP:done` stamp is present).
    pub pp_done: bool,
    /// Duplicate-detection metadata (empty key = no dupe tracking).
    pub dupe_key: String,
    pub dupe_score: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerVolume {
    pub server: u32,
    pub total_bytes: u64,
    pub day_bytes: u64,
    pub month_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueSnapshot {
    pub up_since_unix: i64,
    pub download_paused: bool,
    /// Daily/monthly quota exhausted (force-priority jobs still run).
    pub quota_reached: bool,
    /// Free space on the destination volume is below the floor — all
    /// downloading is held.
    pub disk_low: bool,
    /// Per-server session/day/month volume counters (this node).
    pub server_volumes: Vec<ServerVolume>,
    /// Servers currently blocked after connect failures (retrying on a
    /// timer). Surfaced so the UI can explain a stalled queue.
    pub blocked_servers: Vec<u32>,
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
