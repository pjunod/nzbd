//! Cluster work-lease wire types (CLUSTERING.md §6.1). Server credentials
//! never cross this channel — budgets are keyed by server *name*, resolved
//! against each node's local `[[server]]` config.

use nzbd_engine::MirrorStats;
use nzbd_types::{Job, JobId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SECRET_HEADER: &str = "x-nzbd-cluster-secret";

#[derive(Debug, Serialize, Deserialize)]
pub struct PollRequest {
    pub node: String,
    pub free_download_slots: u32,
    /// Free post-processing slots (C2). Absent/0 = not a PP executor.
    #[serde(default)]
    pub free_pp_slots: u32,
}

/// What a lease authorizes (C2 adds stage-level PP work).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LeaseKind {
    #[default]
    Download,
    /// Post-processing: par verify/repair → unpack → cleanup → scripts,
    /// fenced in `.pp.<lease_id>/` staging (CLUSTERING.md §6.4).
    Post,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Grant {
    pub lease_id: String,
    pub epoch: u64,
    #[serde(default)]
    pub kind: LeaseKind,
    pub job: Job,
    /// Per-server-name connection allowance (cluster-wide account cap
    /// partitioning, §6.3). Empty for PP grants — PP opens no provider
    /// connections.
    pub server_budgets: HashMap<String, u16>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PollResponse {
    pub grants: Vec<Grant>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseProgress {
    pub lease_id: String,
    pub job: JobId,
    pub stats: MirrorStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node: String,
    pub leases: Vec<LeaseProgress>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HeartbeatResponse {
    /// Leases the worker must abort (job deleted / reassigned / unknown).
    pub cancel: Vec<String>,
    /// Refreshed connection budgets (membership changed since the grant).
    pub server_budgets: Option<HashMap<String, u16>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub node: String,
    pub lease_id: String,
    /// The finished job's full final state (ids preserved).
    pub job: Job,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteResponse {
    pub ok: bool,
}

/// Node presence record (registry file on the shared volume).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub name: String,
    pub api_url: String,
    pub download: bool,
    pub post_process: bool,
    pub max_download_jobs: u32,
    pub active_download_jobs: u32,
    /// PP executor capacity (C2 anti-affinity scheduling input).
    #[serde(default)]
    pub pp_slots: u32,
    pub rate_bps: u64,
    pub seq: u64,
}
