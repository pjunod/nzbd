//! Protocol-neutral transfer-backend contract.
//!
//! This is deliberately dormant in M1b: the production daemon still admits
//! and executes only Usenet jobs. The contract exists so an eventual torrent
//! engine cannot become a second queue owner or flood the queue owner's FIFO
//! with peer-rate updates. Structural facts are reliable and bounded;
//! progress is a latest-value snapshot per job.

use nzbd_types::{JobId, TorrentPhase, TorrentRecord};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, watch};

/// A torrent that has made no payload progress and seen no useful peer for
/// this long yields its shared download slot. It remains live in the backend
/// and competes again as soon as a later fact advances `last_activity_unix`.
pub const STALLED_SLOT_YIELD_SECS: i64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendCommand {
    Start { job: JobId },
    Pause { job: JobId },
    Resume { job: JobId },
    Remove { job: JobId, delete_data: bool },
    SetPriority { job: JobId, priority: i32 },
    SetDownloadLimit { bytes_per_sec: Option<u64> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransferProgress {
    pub downloaded_bytes: u64,
    pub verified_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_bps: u64,
    pub upload_bps: u64,
    pub useful_peers: u32,
    /// Wall-clock time of the latest payload progress or useful-peer fact.
    pub last_activity_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentMetadata {
    pub info_hash_v1: String,
    pub name: String,
    pub total_bytes: u64,
    pub selected_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    Paused,
    SeedPolicyReached,
    Removed,
    Shutdown,
}

/// A display-safe backend failure. The adapter must redact passkeys, query
/// strings, peer lists, and paths outside the job root before construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeError(String);

impl SafeError {
    pub const MAX_BYTES: usize = 2 * 1024;

    pub fn from_redacted(message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > Self::MAX_BYTES {
            let mut boundary = Self::MAX_BYTES;
            while !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            message.truncate(boundary);
        }
        Self(message)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Non-coalescible facts. Delivery may apply backpressure to the backend but
/// these facts are never silently replaced by later progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendFact {
    MetadataReady {
        job: JobId,
        torrent: TorrentMetadata,
    },
    Ready {
        job: JobId,
        content_path: PathBuf,
    },
    Stopped {
        job: JobId,
        reason: StopReason,
    },
    Failed {
        job: JobId,
        error: SafeError,
    },
}

/// Queue-owner side of a backend connection.
pub struct BackendOwnerPort {
    command_tx: mpsc::Sender<BackendCommand>,
    structural_rx: mpsc::Receiver<BackendFact>,
    progress_rx: watch::Receiver<BTreeMap<JobId, TransferProgress>>,
}

impl BackendOwnerPort {
    /// Queue-owner compatible send: never await while holding queue state.
    /// A full command channel is retained by the caller and retried on the
    /// next owner tick, matching the existing NNTP writer contract.
    pub fn try_command(
        &self,
        command: BackendCommand,
    ) -> Result<(), mpsc::error::TrySendError<BackendCommand>> {
        self.command_tx.try_send(command)
    }

    pub fn try_structural(&mut self) -> Result<BackendFact, mpsc::error::TryRecvError> {
        self.structural_rx.try_recv()
    }

    /// Return one latest progress value per job and mark this version seen.
    pub fn latest_progress(&mut self) -> BTreeMap<JobId, TransferProgress> {
        self.progress_rx.borrow_and_update().clone()
    }
}

/// Adapter side of a backend connection.
pub struct BackendAdapterPort {
    command_rx: mpsc::Receiver<BackendCommand>,
    structural_tx: mpsc::Sender<BackendFact>,
    progress_tx: watch::Sender<BTreeMap<JobId, TransferProgress>>,
}

impl BackendAdapterPort {
    pub async fn next_command(&mut self) -> Option<BackendCommand> {
        self.command_rx.recv().await
    }

    pub async fn structural(
        &self,
        fact: BackendFact,
    ) -> Result<(), mpsc::error::SendError<BackendFact>> {
        self.structural_tx.send(fact).await
    }

    /// Replace this job's pending progress without consuming structural FIFO
    /// capacity. Thousands of peer ticks therefore cost one map entry.
    pub fn progress(&self, job: JobId, progress: TransferProgress) {
        self.progress_tx.send_modify(|latest| {
            latest.insert(job, progress);
        });
    }

    pub fn forget_progress(&self, job: JobId) {
        self.progress_tx.send_modify(|latest| {
            latest.remove(&job);
        });
    }
}

pub fn backend_channel(
    command_capacity: usize,
    structural_capacity: usize,
) -> (BackendOwnerPort, BackendAdapterPort) {
    assert!(
        command_capacity > 0,
        "backend command capacity must be nonzero"
    );
    assert!(
        structural_capacity > 0,
        "backend structural capacity must be nonzero"
    );
    let (command_tx, command_rx) = mpsc::channel(command_capacity);
    let (structural_tx, structural_rx) = mpsc::channel(structural_capacity);
    let (progress_tx, progress_rx) = watch::channel(BTreeMap::new());
    (
        BackendOwnerPort {
            command_tx,
            structural_rx,
            progress_rx,
        },
        BackendAdapterPort {
            command_rx,
            structural_tx,
            progress_tx,
        },
    )
}

/// Whether a torrent currently competes for one shared active-download slot.
/// Seeding never consumes a download slot. A stalled live download yields
/// without being stopped, so Usenet can progress and the torrent can later
/// reacquire a slot when a useful-peer/progress fact updates its activity.
pub fn torrent_wants_download_slot(
    torrent: &TorrentRecord,
    queued_at_unix: i64,
    now_unix: i64,
) -> bool {
    if !torrent.phase.wants_download_slot() {
        return false;
    }
    if torrent.phase != TorrentPhase::Downloading {
        return true;
    }
    let last_activity = torrent.last_activity_unix.unwrap_or(queued_at_unix);
    now_unix.saturating_sub(last_activity) < STALLED_SLOT_YIELD_SECS
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{SeedPolicy, TorrentSource};

    fn torrent(phase: TorrentPhase, last_activity_unix: Option<i64>) -> TorrentRecord {
        TorrentRecord {
            info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
            source: TorrentSource::Magnet,
            metadata_file: "meta/example.torrent".into(),
            phase,
            files: Vec::new(),
            total_bytes: 100,
            selected_bytes: 100,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: SeedPolicy::default(),
            last_activity_unix,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn progress_flood_cannot_delay_a_control_command() {
        let (owner, mut adapter) = backend_channel(1, 1);
        for n in 0..50_000 {
            adapter.progress(
                JobId(7),
                TransferProgress {
                    downloaded_bytes: n,
                    ..Default::default()
                },
            );
        }

        owner
            .try_command(BackendCommand::Remove {
                job: JobId(7),
                delete_data: false,
            })
            .unwrap();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Remove {
                job: JobId(7),
                delete_data: false,
            })
        );
    }

    #[tokio::test]
    async fn progress_is_latest_per_job_and_structural_facts_are_separate() {
        let (mut owner, adapter) = backend_channel(1, 1);
        adapter.progress(
            JobId(1),
            TransferProgress {
                downloaded_bytes: 1,
                ..Default::default()
            },
        );
        adapter.progress(
            JobId(1),
            TransferProgress {
                downloaded_bytes: 99,
                ..Default::default()
            },
        );
        adapter
            .structural(BackendFact::Ready {
                job: JobId(1),
                content_path: "/payload/example".into(),
            })
            .await
            .unwrap();

        assert_eq!(owner.latest_progress()[&JobId(1)].downloaded_bytes, 99);
        assert!(matches!(
            owner.try_structural(),
            Ok(BackendFact::Ready { job: JobId(1), .. })
        ));
    }

    #[test]
    fn stalled_download_yields_and_new_activity_reacquires() {
        let mut record = torrent(TorrentPhase::Downloading, Some(100));
        assert!(torrent_wants_download_slot(&record, 90, 159));
        assert!(!torrent_wants_download_slot(&record, 90, 160));
        record.last_activity_unix = Some(161);
        assert!(torrent_wants_download_slot(&record, 90, 161));
        record.phase = TorrentPhase::Seeding;
        assert!(!torrent_wants_download_slot(&record, 90, 161));
    }

    #[test]
    fn safe_error_is_utf8_safe_and_bounded() {
        let error = SafeError::from_redacted("é".repeat(2_000));
        assert!(error.as_str().len() <= SafeError::MAX_BYTES);
        assert!(error.as_str().is_char_boundary(error.as_str().len()));
    }
}
