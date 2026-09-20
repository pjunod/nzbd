use crate::app_state::AppState;
use crate::{Error, NodeId};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use stream::{ClientStreamControl, ClientStreamReq};
use tokio::sync::{RwLock, oneshot, watch};
use tokio::task::JoinHandle;

#[cfg(feature = "backup")]
mod backup;
#[cfg(feature = "sqlite")]
mod batch;
#[cfg(feature = "cache")]
mod cache;
mod create;
#[cfg(feature = "dlock")]
pub mod dlock;
#[cfg(feature = "sqlite")]
mod execute;
mod helpers;
#[cfg(feature = "listen_notify_local")]
mod listen_notify;
mod mgmt;
#[cfg(feature = "sqlite")]
pub use mgmt::{
    DB_LOCAL_READ_PROTOCOL_VERSION, DbQuorumWatermark, LocalDbRaftMetrics, LocalDbRaftSnapshot,
};
#[cfg(feature = "sqlite")]
pub(crate) use mgmt::{
    DB_QUORUM_WATERMARK_COMPAT_PROBE, DB_QUORUM_WATERMARK_MARKER, db_quorum_watermark_local,
};
#[cfg(feature = "sqlite")]
mod migrate;
#[cfg(feature = "sqlite")]
mod query;
mod rate_limit;
#[cfg(feature = "shutdown-handle")]
mod shutdown_handle;
pub mod stream;
#[cfg(feature = "sqlite")]
mod transaction;

/// The main database client.
///
/// It will handle all things you need to work with the Database / Cache / Event Bus /
/// Distributed Locks. It wraps all inner data inside an internal `Arc<_>`, which means it's very
/// cheap to clone directly.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<DbClient>,
}

pub(crate) struct DbClient {
    pub(crate) state: Option<Arc<AppState>>,
    #[cfg(feature = "cache")]
    pub(crate) leader_cache: Arc<RwLock<(NodeId, String)>>,
    #[cfg(feature = "cache")]
    pub(crate) leader_recovery_cache: Arc<LeaderRecovery>,
    #[cfg(feature = "sqlite")]
    pub(crate) leader_db: Arc<RwLock<(NodeId, String)>>,
    #[cfg(feature = "sqlite")]
    pub(crate) leader_recovery_db: Arc<LeaderRecovery>,
    pub(crate) nodes: Vec<String>,
    /// Keep remote proxy endpoints authoritative across reconnects. Without
    /// this, discovery and ForwardToLeader handling replace them with the
    /// cluster's directly advertised node addresses.
    pub(crate) proxy_mode: bool,
    pub(crate) client: Option<reqwest::Client>,
    #[cfg(feature = "cache")]
    pub(crate) tx_client_cache: flume::Sender<ClientStreamReq>,
    #[cfg(feature = "cache")]
    pub(crate) tx_leader_cache: flume::Sender<ClientStreamControl>,
    #[cfg(feature = "sqlite")]
    pub(crate) tx_client_db: flume::Sender<ClientStreamReq>,
    #[cfg(feature = "sqlite")]
    pub(crate) tx_leader_db: flume::Sender<ClientStreamControl>,
    pub(crate) tls_config: Option<Arc<rustls::ClientConfig>>,
    #[cfg(feature = "cache")]
    pub(crate) tls_no_verify: bool,
    pub(crate) api_secret: Option<String>,
    pub(crate) request_id: AtomicUsize,
    pub(crate) tx_shutdown: Option<watch::Sender<bool>>,
    pub(crate) stream_shutdown: watch::Sender<bool>,
    pub(crate) background_handles: Mutex<Vec<JoinHandle<()>>>,
    #[cfg(feature = "listen_notify_local")]
    pub(crate) app_start: i64,
    #[cfg(feature = "listen_notify_local")]
    pub(crate) rx_notify: Option<flume::Receiver<(i64, Vec<u8>)>>,
    #[cfg(feature = "cache")]
    pub(crate) rate_limit_cache: Option<AtomicU32>,
    #[cfg(feature = "cache")]
    pub(crate) rate_limit_cache_await:
        crossbeam::channel::Sender<oneshot::Sender<Result<(), Error>>>,
    #[cfg(feature = "sqlite")]
    pub(crate) rate_limit_db: Option<AtomicU32>,
    #[cfg(feature = "sqlite")]
    pub(crate) rate_limit_db_await: crossbeam::channel::Sender<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Default)]
pub(crate) struct LeaderRecovery {
    state: tokio::sync::Mutex<LeaderRecoveryState>,
}

#[derive(Default)]
struct LeaderRecoveryState {
    generation: u64,
    in_flight: Option<(u64, watch::Receiver<Option<bool>>)>,
}

impl LeaderRecovery {
    pub(crate) async fn join_or_begin(
        &self,
    ) -> (
        u64,
        watch::Receiver<Option<bool>>,
        Option<watch::Sender<Option<bool>>>,
    ) {
        let mut state = self.state.lock().await;
        if let Some((generation, receiver)) = state.in_flight.as_ref() {
            (*generation, receiver.clone(), None)
        } else {
            state.generation = state.generation.saturating_add(1);
            let generation = state.generation;
            let (sender, receiver) = watch::channel(None);
            state.in_flight = Some((generation, receiver.clone()));
            (generation, receiver, Some(sender))
        }
    }

    pub(crate) async fn complete(
        &self,
        generation: u64,
        sender: watch::Sender<Option<bool>>,
        succeeded: bool,
    ) {
        sender.send_replace(Some(succeeded));
        let mut state = self.state.lock().await;
        if state
            .in_flight
            .as_ref()
            .is_some_and(|(active, _)| *active == generation)
        {
            state.in_flight = None;
        }
    }

    pub(crate) async fn wait(mut receiver: watch::Receiver<Option<bool>>) -> bool {
        loop {
            if let Some(succeeded) = *receiver.borrow() {
                return succeeded;
            }
            if receiver.changed().await.is_err() {
                return false;
            }
        }
    }
}
