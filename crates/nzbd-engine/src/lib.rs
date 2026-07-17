//! Download engine.
//!
//! Phase 0 ships the crown-jewel semantic as pure, scenario-tested logic:
//! the **server failover ladder** (NZBGet `ArticleDownloader::Run` +
//! `ServerPool`, see ARCHITECTURE.md §3.2 / §8.2). The async queue-owner
//! task, connection pools, rate limiter and disk writers land in phase 1 and
//! *consume* this module — the semantics stay a pure function of inputs.

pub mod failover;

/// Commands accepted by the queue-owner task (phase 1).
/// Sketched now so API/compat crates can develop against the vocabulary.
#[derive(Debug)]
pub enum QueueCommand {
    AddNzb { name: String, content: Vec<u8> },
    AddUrl { url: String },
    PauseJob { job: nzbd_types::JobId },
    ResumeJob { job: nzbd_types::JobId },
    DeleteJob { job: nzbd_types::JobId, final_delete: bool },
    SetPriority { job: nzbd_types::JobId, priority: i32 },
    PauseAll,
    ResumeAll,
    SetSpeedLimit { bytes_per_sec: Option<u64> },
}
