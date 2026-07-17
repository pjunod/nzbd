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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Grant {
    pub lease_id: String,
    pub epoch: u64,
    pub job: Job,
    /// Per-server-name connection allowance (cluster-wide account cap
    /// partitioning, §6.3).
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
    pub rate_bps: u64,
    pub seq: u64,
}
