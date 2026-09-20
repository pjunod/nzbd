use crate::log_store_impl::deserialize;
use crate::metadata::Metadata;
use crate::wal::WalFileSet;
use openraft::LogId;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_ERROR_CHARS: usize = 512;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WalRuntimeState {
    Open,
    Syncing,
    Compacting,
    Stopping,
    Stopped,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalRecoveryObservation {
    pub observed_at_unix_ms: u64,
    pub operation: String,
    pub outcome: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BoundedWalError {
    pub observed_at_unix_ms: u64,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalStatusSnapshot {
    pub state: WalRuntimeState,
    pub lock_owned: bool,
    pub unclean_start_observed: bool,
    pub sync_policy: String,
    pub segment_size_bytes: u64,
    pub segment_count: u64,
    pub allocated_bytes: u64,
    pub first_retained_index: Option<u64>,
    pub last_log_index: Option<u64>,
    pub last_purged_index: Option<u64>,
    pub last_durable_index: Option<u64>,
    pub last_sync_unix_ms: Option<u64>,
    pub last_compaction_unix_ms: Option<u64>,
    pub last_recovery: Option<WalRecoveryObservation>,
    pub last_error: Option<BoundedWalError>,
}

#[derive(Debug)]
struct WalStatusRuntime {
    state: WalRuntimeState,
    lock_owned: bool,
    last_durable_index: Option<u64>,
    last_sync_unix_ms: Option<u64>,
    last_compaction_unix_ms: Option<u64>,
    last_recovery: Option<WalRecoveryObservation>,
    last_error: Option<BoundedWalError>,
}

/// Cloneable, local-only view of the WAL owned by the live log store.
///
/// Layout values are sampled through the writer's existing `WalFileSet` lock;
/// this handle never reopens, mmaps, or walks WAL files.
#[derive(Clone, Debug)]
pub struct WalStatusHandle {
    runtime: Arc<RwLock<WalStatusRuntime>>,
    wal: Arc<RwLock<WalFileSet>>,
    metadata: Arc<RwLock<Metadata>>,
    sync_policy: String,
    segment_size_bytes: u64,
    unclean_start_observed: bool,
}

impl WalStatusHandle {
    pub(crate) fn new(
        wal: Arc<RwLock<WalFileSet>>,
        metadata: Arc<RwLock<Metadata>>,
        sync_policy: String,
        segment_size_bytes: u64,
        unclean_start_observed: bool,
    ) -> Self {
        // Anything present after startup integrity validation was already
        // recovered from durable storage. Starting this at `None` would make
        // a quiet, healthy node look unsafe until its next write happens to
        // trigger a sync.
        let last_durable_index = wal
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .files
            .back()
            .map(|file| file.id_until)
            .filter(|index| *index != 0);
        Self {
            runtime: Arc::new(RwLock::new(WalStatusRuntime {
                state: WalRuntimeState::Open,
                lock_owned: true,
                last_durable_index,
                last_sync_unix_ms: None,
                last_compaction_unix_ms: None,
                last_recovery: unclean_start_observed.then(|| WalRecoveryObservation {
                    observed_at_unix_ms: unix_ms(),
                    operation: "startup_integrity_check".to_owned(),
                    outcome: "completed".to_owned(),
                }),
                last_error: None,
            })),
            wal,
            metadata,
            sync_policy,
            segment_size_bytes,
            unclean_start_observed,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> WalStatusSnapshot {
        let runtime = self
            .runtime
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let wal = self
            .wal
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let metadata = self
            .metadata
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let first_retained_index = wal
            .files
            .front()
            .map(|file| file.id_from)
            .filter(|index| *index != 0);
        let last_log_index = wal
            .files
            .back()
            .map(|file| file.id_until)
            .filter(|index| *index != 0);
        let mut state = runtime.state;
        let mut last_error = runtime.last_error.clone();
        let last_purged_index = match metadata
            .last_purged_log_id
            .as_deref()
            .map(deserialize::<LogId<u64>>)
            .transpose()
        {
            Ok(value) => value.map(|value| value.index),
            Err(error) => {
                state = WalRuntimeState::Error;
                last_error.get_or_insert_with(|| bounded_error(&error));
                None
            }
        };
        let segment_count = u64::try_from(wal.files.len()).unwrap_or(u64::MAX);

        WalStatusSnapshot {
            state,
            lock_owned: runtime.lock_owned,
            unclean_start_observed: self.unclean_start_observed,
            sync_policy: self.sync_policy.clone(),
            segment_size_bytes: self.segment_size_bytes,
            segment_count,
            allocated_bytes: segment_count.saturating_mul(self.segment_size_bytes),
            first_retained_index,
            last_log_index,
            last_purged_index,
            last_durable_index: runtime.last_durable_index,
            last_sync_unix_ms: runtime.last_sync_unix_ms,
            last_compaction_unix_ms: runtime.last_compaction_unix_ms,
            last_recovery: runtime.last_recovery.clone(),
            last_error,
        }
    }

    pub(crate) fn set_state(&self, state: WalRuntimeState) {
        self.write_runtime().state = state;
    }

    pub(crate) fn record_sync(&self, durable_index: Option<u64>) {
        let mut runtime = self.write_runtime();
        runtime.state = WalRuntimeState::Open;
        runtime.last_durable_index = durable_index;
        runtime.last_sync_unix_ms = Some(unix_ms());
    }

    pub(crate) fn record_compaction(&self, durable_index: Option<u64>) {
        let mut runtime = self.write_runtime();
        runtime.state = WalRuntimeState::Open;
        runtime.last_durable_index = durable_index;
        runtime.last_compaction_unix_ms = Some(unix_ms());
    }

    pub(crate) fn record_stopped(&self, durable_index: Option<u64>) {
        let mut runtime = self.write_runtime();
        runtime.state = WalRuntimeState::Stopped;
        runtime.lock_owned = false;
        runtime.last_durable_index = durable_index;
        runtime.last_sync_unix_ms = Some(unix_ms());
    }

    pub(crate) fn record_error(&self, error: &impl std::fmt::Display) {
        let mut runtime = self.write_runtime();
        runtime.state = WalRuntimeState::Error;
        runtime.last_error = Some(bounded_error(error));
    }

    fn write_runtime(&self) -> std::sync::RwLockWriteGuard<'_, WalStatusRuntime> {
        self.runtime
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn bounded_error(error: &impl std::fmt::Display) -> BoundedWalError {
    let mut message = error.to_string();
    if message.chars().count() > MAX_ERROR_CHARS {
        message = message.chars().take(MAX_ERROR_CHARS).collect();
    }
    BoundedWalError {
        observed_at_unix_ms: unix_ms(),
        message,
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
