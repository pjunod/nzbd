//! Engine broadcast events (mirrored to the API's SSE stream in phase 3).

use nzbd_types::{FileId, JobId, JobStatus, ServerId};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    JobAdded {
        job: JobId,
        name: String,
    },
    JobFinished {
        job: JobId,
        name: String,
        status: JobStatus,
        health: u16,
    },
    JobDeleted {
        job: JobId,
    },
    FileFinished {
        job: JobId,
        file: FileId,
        filename: String,
        ok: bool,
    },
    /// A segment failed on every server at every tier.
    SegmentExhausted {
        job: JobId,
        file: FileId,
        segment: u32,
    },
    ServerBlocked {
        server: ServerId,
        seconds: u64,
    },
    QueuePauseChanged {
        paused: bool,
        /// Who asked (client UA / "web-ui" / "cli") — surfaced in the UI
        /// and logs so a queue that keeps (un)pausing itself is a
        /// one-glance diagnosis instead of whack-a-mole: SOME client sent
        /// this; the engine never flips the flag on its own.
        source: String,
    },
    SpeedLimitChanged {
        bytes_per_sec: Option<u64>,
    },
    /// Cluster: job (un)delegated to a node.
    JobAssigned {
        job: JobId,
        node: Option<String>,
    },
}
