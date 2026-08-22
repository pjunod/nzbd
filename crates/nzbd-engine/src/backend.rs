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
    /// The write path hit ENOSPC or EDQUOT. This is a live hold, not a
    /// terminal engine failure.
    StorageFull,
    /// Previously admitted payload is absent or no longer matches metadata.
    /// The job remains recoverable through restore/recheck.
    MissingContent,
    /// Discovery or peer availability temporarily cannot make progress.
    Transient,
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
        /// Canonical payload path. Emitting this fact is the adapter's
        /// assertion that all selected pieces are hash-verified and the
        /// payload plus containing directory have completed their durability
        /// barrier. The owner independently checks verified byte counts.
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
/// Seeding never consumes a download slot. Active source/metadata/checking or
/// payload work yields when stalled, so a dead magnet cannot monopolize the
/// default single slot before it reaches `Downloading`. `Queued` stays
/// eligible without a clock: yielding a job that has not started would deny
/// it the only transition capable of producing new activity.
pub fn torrent_wants_download_slot(
    torrent: &TorrentRecord,
    queued_at_unix: i64,
    now_unix: i64,
) -> bool {
    if !torrent.phase.wants_download_slot() {
        return false;
    }
    if torrent.phase == TorrentPhase::Queued {
        return true;
    }
    if torrent.last_activity_unix.is_none() && torrent.phase != TorrentPhase::Downloading {
        // Source, metadata, and checking work with no activity stamp has not
        // had a chance to run yet. Its queue age is not a phase-entry clock;
        // treating an old queued timestamp as activity would make the job
        // yield the slot before it can produce the first backend fact.
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
        owner
            .try_command(BackendCommand::Pause { job: JobId(7) })
            .unwrap();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(7) })
        );
        owner
            .try_command(BackendCommand::SetPriority {
                job: JobId(7),
                priority: 100,
            })
            .unwrap();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::SetPriority {
                job: JobId(7),
                priority: 100,
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

        adapter.forget_progress(JobId(1));
        assert!(owner.latest_progress().is_empty());
    }

    #[test]
    fn stalled_active_phases_yield_and_new_activity_reacquires() {
        for phase in [
            TorrentPhase::FetchingSource,
            TorrentPhase::FetchingMetadata,
            TorrentPhase::Checking,
            TorrentPhase::Downloading,
        ] {
            let record = torrent(phase, Some(100));
            assert!(torrent_wants_download_slot(&record, 90, 159), "{phase:?}");
            assert!(!torrent_wants_download_slot(&record, 90, 160), "{phase:?}");
        }

        let mut record = torrent(TorrentPhase::Downloading, Some(100));
        record.last_activity_unix = Some(161);
        assert!(torrent_wants_download_slot(&record, 90, 161));
        record.phase = TorrentPhase::Queued;
        assert!(
            torrent_wants_download_slot(&record, 90, 10_000),
            "a not-yet-started job must not permanently yield itself"
        );
        record.phase = TorrentPhase::Seeding;
        assert!(!torrent_wants_download_slot(&record, 90, 161));
    }

    #[test]
    fn old_queued_age_cannot_starve_new_pre_download_work() {
        for phase in [
            TorrentPhase::FetchingSource,
            TorrentPhase::FetchingMetadata,
            TorrentPhase::Checking,
        ] {
            let record = torrent(phase, None);
            assert!(
                torrent_wants_download_slot(&record, 100, 10_000),
                "{phase:?} must run once before it can be called stalled"
            );
        }

        let record = torrent(TorrentPhase::Downloading, None);
        assert!(
            !torrent_wants_download_slot(&record, 100, 10_000),
            "an already-downloading legacy row still needs a bounded fallback"
        );
    }

    #[test]
    fn safe_error_is_utf8_safe_and_bounded() {
        let error = SafeError::from_redacted("é".repeat(2_000));
        assert!(error.as_str().len() <= SafeError::MAX_BYTES);
        assert!(error.as_str().is_char_boundary(error.as_str().len()));

        // 2,048 is inside a three-byte code point here, so truncation must
        // walk back to a real UTF-8 boundary rather than panic or emit junk.
        let error = SafeError::from_redacted("€".repeat(1_000));
        assert_eq!(error.as_str().len(), 2_046);
        assert!(error.as_str().ends_with('€'));
    }
}
