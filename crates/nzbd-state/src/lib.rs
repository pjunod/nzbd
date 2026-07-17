//! Persistence boundaries (phase 0). See ARCHITECTURE.md §8.6.
//!
//! Two artifacts:
//! 1. Queue **snapshot** (atomic tmp+rename, debounced) + append-only
//!    **segment journal** (`fileId, segNo, offset, len, crc`, fsync'd on a
//!    short interval, compacted into the snapshot opportunistically).
//!    Recovery = load snapshot, replay journal, re-lease everything else.
//! 2. **History** in SQLite (rusqlite, WAL) — queryable, unbounded-friendly.

use nzbd_types::{FileId, JobId};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt state: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub job: JobId,
    pub file: FileId,
    pub segment_number: u32,
    pub offset: u64,
    pub len: u32,
    pub crc32: u32,
}

pub trait SegmentJournal {
    fn append(&mut self, rec: &JournalRecord) -> Result<(), StateError>;
    fn sync(&mut self) -> Result<(), StateError>;
    fn replay(&mut self) -> Result<Vec<JournalRecord>, StateError>;
    /// Truncate after the records have been folded into a snapshot.
    fn compact(&mut self) -> Result<(), StateError>;
}

/// Terminal record of a job for the history store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub job: JobId,
    pub name: String,
    pub category: Option<String>,
    pub final_dir: Option<String>,
    pub status: String,
    pub size: u64,
    pub completed_at_unix: i64,
}

pub trait HistoryStore {
    fn record(&mut self, entry: &HistoryEntry) -> Result<(), StateError>;
    fn list(&self, limit: usize) -> Result<Vec<HistoryEntry>, StateError>;
    fn delete(&mut self, job: JobId) -> Result<bool, StateError>;
}
