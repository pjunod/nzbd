//! The lock-free read model. The queue-owner task publishes an immutable
//! [`QueueSnapshot`] via `arc-swap` (debounced to its 1 Hz tick plus
//! structural changes); API handlers load it without ever blocking the
//! engine (ARCHITECTURE.md §8.1).

use arc_swap::ArcSwap;
use nzbd_types::{JobId, JobKind, JobStatus, StageSpan};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub id: JobId,
    pub kind: JobKind,
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
    /// actively downloading). For local jobs this is WIRE bytes — the
    /// same measurement as the queue-wide rate, so the two never
    /// structurally disagree.
    pub rate_bps: u64,
    /// Article download attempts that failed and were retried — the gap
    /// between wire throughput and completed bytes, made visible.
    pub retried_articles: u32,
    /// Cluster: node currently executing this job remotely (None = local).
    pub assigned_node: Option<String>,
    /// Post-processing already finished (the `*PP:done` stamp is present).
    pub pp_done: bool,
    /// Protocol-neutral content readiness. For Usenet this is the durable
    /// post-processing completion stamp; for torrents it is set only after
    /// selected payload bytes pass piece verification.
    pub ready: bool,
    pub ready_at_unix: Option<i64>,
    /// Duplicate-detection metadata (empty key = no dupe tracking).
    pub dupe_key: String,
    pub dupe_score: i32,
    /// The job's non-internal parameters — a consumer's own tracking id
    /// (`drone`, `monarr-transfer`) among them. Carried on the snapshot so
    /// the queue UI and `GET /api/v1/jobs/{id}` can show it without an
    /// export round-trip: the whole value of a transfer id is that you can
    /// SEE it on the job it belongs to. `*`-internal params stay out.
    pub params: Vec<(String, String)>,
    /// Post-processing stages this job has passed through, in order, with
    /// the wall time each took (`ms` absent = the stage is still running).
    /// Rides the existing 1 Hz tick, so the queue row's stage timer and
    /// the detail pipeline cost no extra request.
    pub stages: Vec<StageSpan>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerVolume {
    pub server: u32,
    /// Configured display name, so the UI's per-provider chips can say
    /// "eweka" rather than "#0".
    pub name: String,
    pub total_bytes: u64,
    pub day_bytes: u64,
    pub month_bytes: u64,
    /// This server's current share of the wire rate (EMA, bytes/sec),
    /// computed from the SAME counters as `download_rate_bps` — so the
    /// per-server rates sum to the header rate instead of to some other
    /// number that also calls itself throughput.
    pub rate_bps: u64,
}

/// The enforcing disk guard's cached evidence for one filesystem.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StorageVolumeSnapshot {
    pub label: String,
    pub path: String,
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    /// False when the current cycle could not measure this row. Non-None
    /// capacity is conservative last-known data; None means no successful
    /// reading has ever been observed.
    pub current: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueSnapshot {
    pub up_since_unix: i64,
    pub download_paused: bool,
    /// Daily/monthly quota exhausted (force-priority jobs still run).
    pub quota_reached: bool,
    /// Intake is held by a below-floor reading, an observed write failure, an
    /// initial fail-safe measurement, or incomplete evidence after a prior
    /// hold.
    pub disk_low: bool,
    /// Lowest measured free space across every configured write root. None
    /// means the enforcing probe has no usable reading or is disabled.
    #[serde(default)]
    pub disk_guard_free_bytes: Option<u64>,
    /// Operator-facing role of the limiting configured root.
    #[serde(default)]
    pub disk_guard_label: Option<String>,
    /// Configured path whose containing filesystem currently limits intake.
    #[serde(default)]
    pub disk_guard_path: Option<String>,
    /// The current hold was latched by an observed ENOSPC/EDQUOT write,
    /// rather than solely by the cached capacity forecast.
    #[serde(default)]
    pub disk_guard_write_latched: bool,
    /// True only when the enforcing cycle measured every configured root.
    /// False with `disk_low` can mean a prior hold is being retained because
    /// incomplete evidence is not recovery proof.
    #[serde(default)]
    pub disk_guard_all_roots_known: bool,
    /// The same per-filesystem evidence used by the enforcing guard. The API
    /// renders this cache directly; it never launches an independent probe.
    #[serde(default)]
    pub storage_volumes: Vec<StorageVolumeSnapshot>,
    /// Cumulative out-of-space errors (ENOSPC/EDQUOT) reported by write
    /// paths since start. Use `disk_guard_write_latched`, not this historical
    /// count, to identify the cause of the current hold.
    #[serde(default)]
    pub enospc_observed: u64,
    /// What the write path was doing when it last ran out of space
    /// (operation and path, as the fsx layer stamped it).
    #[serde(default)]
    pub enospc_where: Option<String>,
    /// Per-server session/day/month volume counters (this node).
    pub server_volumes: Vec<ServerVolume>,
    /// Servers currently blocked after connect failures (retrying on a
    /// timer). Surfaced so the UI can explain a stalled queue.
    pub blocked_servers: Vec<u32>,
    /// Whether critical-health abort is armed (`[post] health_action` is
    /// park/delete). Surfaced so the UI can tell the user whether a doomed
    /// download will be cut off early or just run to completion.
    pub health_abort: bool,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once.
    pub max_active_downloads: u32,
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
