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
    },
    SpeedLimitChanged {
        bytes_per_sec: Option<u64>,
    },
}
