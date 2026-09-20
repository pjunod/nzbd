use crate::app_state::AppState;
use crate::client::stream::ClientStreamReq;
use crate::helpers::deserialize;
use crate::network::HEADER_NAME_SECRET;
use crate::{Client, Error};
use openraft::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, info};

// Multi-node shutdown deliberately waits 9.5 seconds for readiness propagation
// and may then wait another five seconds for a leader. Keep enough time after
// those waits for Raft, WAL, SQL, and client-stream teardown on a loaded host.
pub(crate) const RAFT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "sqlite")]
const SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[cfg(feature = "sqlite")]
pub(crate) const DB_QUORUM_WATERMARK_MARKER: &str =
    "/* hiqlite-internal:db-quorum-watermark:v1 */ THIS IS NOT SQL";

#[cfg(feature = "sqlite")]
pub(crate) const DB_QUORUM_WATERMARK_COMPAT_PROBE: &str =
    "SELECT 1 AS hiqlite_watermark_stream_compat_v1";

#[cfg(feature = "cache")]
use crate::network::management::{self, ClusterLeaveReq};
#[cfg(feature = "sqlite")]
use crate::store::state_machine::sqlite::state_machine::QueryWrite;
#[cfg(feature = "sqlite")]
use crate::store::state_machine::sqlite::writer::WriterRequest;
#[cfg(any(feature = "sqlite", feature = "cache"))]
use crate::{Node, NodeId};
#[cfg(any(feature = "sqlite", feature = "cache"))]
use openraft::RaftMetrics;
#[cfg(any(feature = "sqlite", feature = "cache"))]
use std::clone::Clone;
#[cfg(any(feature = "sqlite", feature = "cache"))]
use std::sync::atomic::Ordering;

/// Privacy-safe, local-only projection of the database Raft watch channel.
///
/// The projection deliberately omits node addresses and exposes no method
/// that can issue an HTTP request. Consumers can therefore sample local Raft
/// progress without accidentally falling back to the management API.
#[cfg(feature = "sqlite")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDbRaftSnapshot {
    pub running: bool,
    pub node_id: u64,
    pub current_term: u64,
    pub current_leader: Option<u64>,
    pub last_applied_term: Option<u64>,
    pub last_applied_index: Option<u64>,
}

/// A leader-issued database commit watermark backed by a quorum heartbeat.
///
/// The term and leader identity describe the leadership proof, not the term
/// that originally appended the committed entry. Callers must bind this tuple
/// to a fresh local Raft observation before using it for a bounded read.
///
/// The protocol scalar advertises that the watermark source implements the
/// same local-read contract as the receiver. A newer receiver therefore fails
/// closed while an older leader is active during a rolling upgrade.
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DbQuorumWatermark {
    pub term: u64,
    pub leader_id: u64,
    pub committed_index: u64,
    pub local_read_protocol_version: u64,
}

/// First protocol that binds one local database query to a quorum watermark.
#[cfg(feature = "sqlite")]
pub const DB_LOCAL_READ_PROTOCOL_VERSION: u64 = 1;

/// Receiver for the in-process database Raft metrics watch channel.
///
/// This type can only be constructed for a [`Client`] backed by a local
/// Hiqlite node. A remote client returns an error instead of silently turning
/// a passive observation into network traffic.
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct LocalDbRaftMetrics {
    receiver: watch::Receiver<RaftMetrics<NodeId, Node>>,
}

#[cfg(feature = "sqlite")]
impl LocalDbRaftMetrics {
    /// Copy the latest in-process Raft observation without Store or network IO.
    #[must_use]
    pub fn snapshot(&self) -> LocalDbRaftSnapshot {
        let metrics = self.receiver.borrow();
        LocalDbRaftSnapshot {
            running: metrics.running_state.is_ok(),
            node_id: metrics.id,
            current_term: metrics.current_term,
            current_leader: metrics.current_leader,
            last_applied_term: metrics.last_applied.as_ref().map(|log| log.leader_id.term),
            last_applied_index: metrics.last_applied.as_ref().map(|log| log.index),
        }
    }

    /// Wait for a new in-process observation. `false` means the Raft task
    /// closed its watch channel and no future sample can become fresh.
    pub async fn wait_for_change(&mut self) -> bool {
        self.receiver.changed().await.is_ok()
    }
}

#[cfg(feature = "sqlite")]
pub(crate) async fn db_quorum_watermark_local(
    state: &Arc<AppState>,
) -> Result<DbQuorumWatermark, Error> {
    let before = state.raft_db.raft.metrics().borrow().clone();
    before.running_state?;

    let committed = tokio::time::timeout(
        Duration::from_secs(1),
        state.raft_db.raft.ensure_linearizable(),
    )
    .await
    .map_err(|_| Error::Timeout("database quorum watermark proof timed out".into()))??
    .ok_or_else(|| Error::LeaderChange("database leader has no read index".into()))?;
    let after = state.raft_db.raft.metrics().borrow().clone();
    after.running_state?;
    if after.state != ServerState::Leader
        || after.current_term != before.current_term
        || after.current_term != committed.leader_id.term
        || after.current_leader != Some(state.id)
        || committed.leader_id.node_id != state.id
    {
        return Err(Error::LeaderChange(
            "database leadership changed during quorum watermark proof".into(),
        ));
    }

    Ok(DbQuorumWatermark {
        term: after.current_term,
        leader_id: state.id,
        committed_index: committed.index,
        local_read_protocol_version: DB_LOCAL_READ_PROTOCOL_VERSION,
    })
}

impl Client {
    /// Ask this embedded database voter to materialize its current state as a
    /// Raft snapshot, returning the applied index the snapshot must cover.
    ///
    /// The no-op write is intentional. A restored database may predate the
    /// rebuilt Raft log, so its writer metadata needs one applied entry before
    /// OpenRaft can describe the preserved image in a transferable snapshot.
    /// The trigger returns after the command is accepted; callers that need
    /// publication must observe [`Self::metrics_db`].
    #[cfg(feature = "sqlite")]
    pub async fn trigger_db_snapshot(&self) -> Result<u64, Error> {
        let state = self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("database snapshot trigger requires a local node client".to_owned())
        })?;
        let applied = state
            .raft_db
            .raft
            .client_write(QueryWrite::RTT)
            .await?
            .log_id
            .index;
        state.raft_db.raft.trigger().snapshot().await?;
        Ok(applied)
    }

    /// Subscribe to database Raft metrics only when this client owns the local
    /// node. Remote clients return an error; this method never performs IO.
    #[cfg(feature = "sqlite")]
    pub fn local_db_raft_metrics(&self) -> Result<LocalDbRaftMetrics, Error> {
        let state = self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("local database Raft metrics require a local node client".to_owned())
        })?;
        Ok(LocalDbRaftMetrics {
            receiver: state.raft_db.raft.metrics(),
        })
    }

    /// Obtain the process-local database snapshot instrumentation handle.
    ///
    /// Remote clients fail immediately. Reading this handle performs no
    /// management request, storage operation, allocation, or lock acquisition.
    #[cfg(feature = "sqlite")]
    pub fn local_db_snapshot_metrics(&self) -> Result<crate::LocalDbSnapshotMetrics, Error> {
        self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("local database snapshot metrics require a local node client".to_owned())
        })?;
        Ok(crate::LocalDbSnapshotMetrics::new())
    }

    /// Obtain this embedded node's process-local snapshot transport handle.
    /// Reading it performs no Store or network operation.
    #[cfg(feature = "sqlite")]
    pub fn local_snapshot_transport_status(
        &self,
    ) -> Result<crate::LocalSnapshotTransportStatus, Error> {
        let state = self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("local snapshot transport status requires a local node client".into())
        })?;
        Ok(state.snapshot_transport.clone())
    }

    /// Read one node from this embedded node's current committed Raft membership.
    /// A 404 is expected during rolling upgrades and means unavailable.
    #[cfg(feature = "sqlite")]
    pub async fn snapshot_transport_status_sqlite(
        &self,
        peer_node_id: NodeId,
    ) -> Result<Option<crate::SnapshotTransportStatus>, Error> {
        let state = self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("peer transport status requires a local node client".into())
        })?;
        let peer = snapshot_transport_peer_from_current_membership(
            &state.raft_db.raft.metrics(),
            peer_node_id,
        )?;
        request_snapshot_transport_status_sqlite(
            self.inner.client.as_ref().ok_or_else(|| {
                Error::Connect("snapshot transport HTTP client is unavailable".into())
            })?,
            self.inner.api_secret.as_deref().ok_or_else(|| {
                Error::Connect("snapshot transport API secret is unavailable".into())
            })?,
            self.inner.tls_config.is_some(),
            &peer,
        )
        .await
    }

    /// Obtain the process-local database WAL status handle.
    ///
    /// The handle reads only the live log store's owned locks and never opens
    /// or walks WAL files. Remote clients fail immediately.
    #[cfg(feature = "sqlite")]
    pub fn local_db_wal_status(&self) -> Result<hiqlite_wal::WalStatusHandle, Error> {
        let state = self.inner.state.as_ref().ok_or_else(|| {
            Error::Connect("local database WAL status requires a local node client".to_owned())
        })?;
        Ok(state.raft_db.wal_status.clone())
    }

    /// Obtain a commit watermark after the database leader has confirmed its
    /// current term with a quorum and applied through the returned read index.
    ///
    /// A follower forwards this narrow request over Hiqlite's authenticated
    /// leader stream. The method performs no SQL or state-machine mutation.
    #[cfg(feature = "sqlite")]
    pub async fn db_quorum_watermark(&self) -> Result<DbQuorumWatermark, Error> {
        self.retry_db_after_leader_change(|| self.db_quorum_watermark_req())
            .await
    }

    #[cfg(feature = "sqlite")]
    async fn db_quorum_watermark_req(&self) -> Result<DbQuorumWatermark, Error> {
        if let Some(state) = self.is_leader_db_with_state().await {
            db_quorum_watermark_local(state).await
        } else {
            let mut rows = self
                .query_remote_req(
                    crate::store::state_machine::sqlite::state_machine::Query {
                        sql: DB_QUORUM_WATERMARK_MARKER.into(),
                        params: Vec::new(),
                    },
                    true,
                )
                .await?;
            if rows.len() != 1 {
                return Err(Error::Connect(
                    "database quorum watermark returned an invalid response".into(),
                ));
            }
            rows.swap_remove(0).into_db_quorum_watermark()
        }
    }

    /// Get cluster metrics for the database Raft.
    #[cfg(feature = "sqlite")]
    pub async fn metrics_db(&self) -> Result<RaftMetrics<NodeId, Node>, Error> {
        if let Some(state) = &self.inner.state {
            let metrics = state.raft_db.raft.metrics().borrow().clone();
            Ok(metrics)
        } else {
            let url = self
                .build_addr("/cluster/metrics/sqlite", &self.inner.leader_db)
                .await;
            self.get_metrics_remote(url).await
        }
    }

    /// Get cluster metrics for the cache Raft.
    #[cfg(feature = "cache")]
    pub async fn metrics_cache(&self) -> Result<RaftMetrics<NodeId, Node>, Error> {
        if let Some(state) = &self.inner.state {
            let metrics = state.raft_cache.raft.metrics().borrow().clone();
            Ok(metrics)
        } else {
            let url = self
                .build_addr("/cluster/metrics/cache", &self.inner.leader_cache)
                .await;
            self.get_metrics_remote(url).await
        }
    }

    // This is separated from the `self.send_with_retry_db()` to avoid recursion on leader unreachable
    pub(crate) async fn get_metrics_remote(
        &self,
        url: String,
    ) -> Result<RaftMetrics<NodeId, Node>, Error> {
        // Ordinary local metrics remain in-process. Missing-leader recovery may
        // deliberately query a configured peer while the resumed local watch
        // has not republished the current leader.
        debug_assert!(
            self.inner.api_secret.is_some(),
            "api_secret should always exist for remote clients"
        );

        let res = self
            .inner
            .client
            .as_ref()
            .unwrap()
            .get(url)
            .header(HEADER_NAME_SECRET, self.inner.api_secret.as_ref().unwrap())
            .send()
            .await?;

        if res.status().is_success() {
            let bytes = res.bytes().await?;
            let resp = deserialize(bytes.as_ref())?;
            Ok(resp)
        } else {
            let err = res.json::<Error>().await?;
            Err(err)
        }
    }

    /// Check the cluster health state for the database Raft.
    #[cfg(feature = "sqlite")]
    pub async fn is_healthy_db(&self) -> Result<(), Error> {
        let metrics = self.metrics_db().await?;
        metrics.running_state?;
        if metrics.current_leader.is_some() {
            if metrics.state == ServerState::Learner
                || metrics.state == ServerState::Follower
                || metrics.state == ServerState::Leader
            {
                Ok(())
            } else {
                Err(Error::Connect(format!(
                    "The DB leader voting process has not finished yet - server state: {:?}",
                    metrics.state
                )))
            }
        } else {
            // tracing::error!("Unhealthy DB");
            Err(Error::LeaderChange(
                "The DB leader voting process has not finished yet".into(),
            ))
        }
    }

    /// Check the cluster health state for the cache Raft.
    #[cfg(feature = "cache")]
    pub async fn is_healthy_cache(&self) -> Result<(), Error> {
        let metrics = self.metrics_cache().await?;
        metrics.running_state?;
        if metrics.current_leader.is_some() {
            if metrics.state == ServerState::Learner
                || metrics.state == ServerState::Follower
                || metrics.state == ServerState::Leader
            {
                Ok(())
            } else {
                Err(Error::Connect(format!(
                    "The cache leader voting process has not finished yet - server state: {:?}",
                    metrics.state
                )))
            }
        } else {
            // tracing::error!("Unhealthy cache");
            Err(Error::LeaderChange(
                "The cache leader voting process has not finished yet".into(),
            ))
        }
    }

    /// Wait until the database Raft is healthy.
    #[cfg(feature = "sqlite")]
    pub async fn wait_until_healthy_db(&self) {
        loop {
            match self.is_healthy_db().await {
                Ok(_) => {
                    return;
                }
                Err(err) => {
                    debug!("Waiting for healthy Raft DB: {:?}", err);
                    // tracing::warn!("Waiting for healthy Raft DB");
                    info!("Waiting for healthy Raft DB");
                    time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Wait until the cache Raft is healthy.
    #[cfg(feature = "cache")]
    pub async fn wait_until_healthy_cache(&self) {
        loop {
            match self.is_healthy_cache().await {
                Ok(_) => {
                    return;
                }
                Err(err) => {
                    debug!("Waiting for healthy Raft cache: {:?}", err);
                    info!("Waiting for healthy Raft cache");
                    time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Perform a graceful shutdown for a local Raft node, or close the owned
    /// streams and rate ticker for a remote client without stopping servers.
    ///
    /// The shutdown adds a 10 delay on purpose for smoothing out Kubernetes rolling releases and
    /// make the whole process more graceful, because a whole new leader election might be necessary.
    ///
    /// In future versions, there will be the possibility to trigger a graceful leader election
    /// upfront, but this has not been stabilized in this version.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let primary = if let Some(state) = &self.inner.state {
            match tokio::time::timeout(
                RAFT_SHUTDOWN_TIMEOUT,
                Self::shutdown_execute(
                    state,
                    #[cfg(feature = "cache")]
                    self.inner.tls_config.is_some(),
                    #[cfg(feature = "cache")]
                    self.inner.tls_no_verify,
                    #[cfg(feature = "cache")]
                    &self.inner.tx_client_cache,
                    #[cfg(feature = "sqlite")]
                    &self.inner.tx_client_db,
                    &self.inner.tx_shutdown,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(Error::Error(
                    "Timeout reached while shutting down Raft".into(),
                )),
            }
        } else {
            Ok(())
        };

        self.inner.stream_shutdown.send_replace(true);
        let handles = self
            .inner
            .background_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
            .collect::<Vec<_>>();
        let mut cleanup_error = None;
        for mut handle in handles {
            match tokio::time::timeout(Duration::from_secs(2), &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if cleanup_error.is_none() {
                        cleanup_error = Some(Error::Error(error.to_string().into()));
                    }
                }
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    if cleanup_error.is_none() {
                        cleanup_error = Some(Error::Error(
                            "Timeout reached while closing a client background task".into(),
                        ));
                    }
                }
            }
        }
        if let Err(error) = primary {
            Err(error)
        } else if let Some(error) = cleanup_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    #[allow(unused_assignments)]
    #[allow(unused_variables)]
    pub(crate) async fn shutdown_execute(
        state: &Arc<AppState>,
        #[cfg(feature = "cache")] with_tls: bool,
        #[cfg(feature = "cache")] tls_no_verify: bool,
        #[cfg(feature = "cache")] tx_client_cache: &flume::Sender<ClientStreamReq>,
        #[cfg(feature = "sqlite")] tx_client_db: &flume::Sender<ClientStreamReq>,
        tx_shutdown: &Option<watch::Sender<bool>>,
    ) -> Result<(), Error> {
        info!("Starting Node shutdown");

        #[allow(unused_mut)]
        let mut is_single_instance: bool;
        #[cfg(feature = "cache")]
        {
            let node_count = state
                .raft_cache
                .raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .count();
            is_single_instance = node_count == 1;
        }
        #[cfg(feature = "sqlite")]
        {
            let node_count = state
                .raft_db
                .raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .count();
            is_single_instance = node_count == 1;
        }

        state.is_shutting_down.store(true, Ordering::Relaxed);

        // This pre-shutdown delay is not strictly necessary, but it makes rolling releases
        // smoother, especially with ephemeral storage. It also allows to set a ready check
        // interval of 3 seconds while it will still catch it before it actually starts the
        // shutdown, so services can stop sending requests to this node.
        if !is_single_instance {
            time::sleep(Duration::from_millis(9500)).await;
        }

        #[cfg(feature = "cache")]
        {
            let mut metrics = state.raft_cache.raft.metrics().borrow().clone();

            for _ in 0..5 {
                if metrics.current_leader.is_some() {
                    break;
                }
                info!("Delaying cache cluster leave because of no existing leader");
                time::sleep(Duration::from_secs(1)).await;
                metrics = state.raft_cache.raft.metrics().borrow().clone();
            }

            if !state.raft_cache.cache_storage_disk {
                // If we run an entirely in-memory cache and therefore lose the Raft state
                // and membership between restarts, we should always leave the cluster cleanly
                // before doing a shutdown.
                info!("Leaving in-memory-only cache cluster");

                let client = crate::http_client::build_http_client(tls_no_verify);
                let scheme = if with_tls { "https" } else { "http" };

                if metrics.current_leader == Some(state.id) {
                    if let Err(err) = management::leave_cluster_exec(
                        state,
                        &crate::app_state::RaftType::Cache,
                        ClusterLeaveReq {
                            node_id: state.id,
                            stay_as_learner: false,
                        },
                    )
                    .await
                    {
                        tracing::error!("Error leaving the Cache cluster: {:?}", err);
                    }
                } else if let Err(err) = crate::init::leave_remote_cluster(
                    state,
                    &crate::app_state::RaftType::Cache,
                    &client,
                    scheme,
                    state.id,
                    &state.nodes,
                    0,
                    false,
                )
                .await
                {
                    tracing::error!("Error leaving the Cache cluster: {:?}", err);
                }

                info!("Left in-memory-only cache cluster successfully");
            }

            info!("Shutting down raft cache layer");

            // TODO as soon openraft-0.10 is out, we will be able to trigger a pre-emptive
            //  leader switch, if this node is the leader. This will smoth out things even more.

            state
                .raft_cache
                .is_raft_stopped
                .store(true, Ordering::Relaxed);
            state.raft_cache.snapshot_executor.request_shutdown();
            if !state
                .raft_cache
                .snapshot_executor
                .wait_for_shutdown(Duration::from_secs(5))
                .await
            {
                return Err(Error::Error(
                    "cache snapshot executor still owns work during shutdown".into(),
                ));
            }
            // Keep the Raft core and state-machine worker alive until every
            // accepted snapshot operation has relinquished ownership. Core
            // shutdown can close the install response before the inner
            // durable apply has finished, which would make a later WAL/cache
            // shutdown race that still-running work.
            state.raft_cache.raft.shutdown().await?;
            if let Some(handle) = &state.raft_cache.shutdown_handle {
                handle.shutdown().await?;
            }
            let _ = tx_client_cache.send_async(ClientStreamReq::Shutdown).await;
        };

        #[cfg(feature = "sqlite")]
        {
            info!("Shutting down raft sqlite layer");

            for _ in 0..5 {
                if state
                    .raft_db
                    .raft
                    .metrics()
                    .borrow()
                    .current_leader
                    .is_some()
                {
                    break;
                }
                info!("Delaying sqlite raft shutdown because of no existing leader");
                time::sleep(Duration::from_secs(1)).await;
            }

            // TODO as soon openraft-0.10 is out, we will be able to trigger a pre-emptive
            //  leader switch, if this node is the leader. This will smoth out things even more.

            state.raft_db.is_raft_stopped.store(true, Ordering::Relaxed);

            state.raft_db.snapshot_executor.request_shutdown();
            if !state
                .raft_db
                .snapshot_executor
                .wait_for_shutdown(Duration::from_secs(5))
                .await
            {
                return Err(Error::Error(
                    "sqlite snapshot executor still owns work during shutdown".into(),
                ));
            }
            state.raft_db.raft.shutdown().await?;
            info!("Shutting down sqlite logs writer");
            state.raft_db.shutdown_handle.shutdown().await?;

            info!("Shutting down sqlite writer");
            let (tx_sm, rx_sm) = tokio::sync::oneshot::channel();
            state
                .raft_db
                .sql_writer
                .send_async(WriterRequest::Shutdown(tx_sm))
                .await
                .expect("The state machine writer to always be listening");
            rx_sm
                .await
                .expect("To always get an answer from SQL writer");

            let _ = tx_client_db.send_async(ClientStreamReq::Shutdown).await;
        }

        if let Some(tx) = tx_shutdown {
            tx.send(true)
                .expect("The global Hiqlite shutdown handler to always listen");
        }

        info!("Shutdown complete");
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
fn snapshot_transport_peer_from_current_membership(
    metrics: &watch::Receiver<RaftMetrics<NodeId, Node>>,
    peer_node_id: NodeId,
) -> Result<Node, Error> {
    metrics
        .borrow()
        .membership_config
        .membership()
        .get_node(&peer_node_id)
        .cloned()
        .ok_or_else(|| {
            Error::Config("transport status peer is absent from the current Raft membership".into())
        })
}

#[cfg(feature = "sqlite")]
async fn read_snapshot_transport_status_body(
    mut response: reqwest::Response,
) -> Result<Vec<u8>, Error> {
    let declared_length = response.content_length();
    if declared_length
        .is_some_and(|length| length > SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES as u64)
    {
        return Err(Error::Request(format!(
            "snapshot transport status response exceeds the {} byte limit",
            SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES
        )));
    }

    let initial_capacity = declared_length
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES);
    let mut body = Vec::with_capacity(initial_capacity);
    while let Some(chunk) = response.chunk().await? {
        let Some(next_length) = body.len().checked_add(chunk.len()) else {
            return Err(Error::Request(
                "snapshot transport status response length overflowed".to_owned(),
            ));
        };
        if next_length > SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES {
            return Err(Error::Request(format!(
                "snapshot transport status response exceeds the {} byte limit",
                SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(feature = "sqlite")]
async fn request_snapshot_transport_status_sqlite(
    client: &reqwest::Client,
    api_secret: &str,
    tls: bool,
    peer: &Node,
) -> Result<Option<crate::SnapshotTransportStatus>, Error> {
    let scheme = if tls { "https" } else { "http" };
    let response = client
        .get(format!(
            "{scheme}://{}/cluster/transport/sqlite",
            peer.addr_api
        ))
        .header(HEADER_NAME_SECRET, api_secret)
        .timeout(Duration::from_secs(1))
        .send()
        .await?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let body = read_snapshot_transport_status_body(response).await?;
    if status.is_success() {
        return Ok(Some(serde_json::from_slice(&body)?));
    }
    Err(serde_json::from_slice::<Error>(&body)?)
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::{
        RAFT_SHUTDOWN_TIMEOUT, SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES,
        request_snapshot_transport_status_sqlite, snapshot_transport_peer_from_current_membership,
    };
    use crate::Node;
    use openraft::{Membership, RaftMetrics, StoredMembership};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn serve_transport_response(
        status: &'static str,
        body: String,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind private transport test listener");
        let address = listener.local_addr().expect("read test listener address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0_u8; 4096];
            let read = socket.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]).to_ascii_lowercase();
            assert!(request.starts_with("get /cluster/transport/sqlite http/1.1\r\n"));
            assert!(request.contains("x-api-secret: exact-test-secret\r\n"));
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            socket.flush().await.expect("flush response");
        });
        (address.to_string(), task)
    }

    async fn serve_transport_response_without_length(
        body: Vec<u8>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind private transport test listener");
        let address = listener.local_addr().expect("read test listener address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0_u8; 4096];
            let read = socket.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]).to_ascii_lowercase();
            assert!(request.starts_with("get /cluster/transport/sqlite http/1.1\r\n"));
            assert!(request.contains("x-api-secret: exact-test-secret\r\n"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n",
                )
                .await
                .expect("write response headers");
            // The client is expected to close as soon as it detects the limit,
            // so a reset while writing the deliberately oversized tail is valid.
            let _ = socket.write_all(&body).await;
            let _ = socket.flush().await;
        });
        (address.to_string(), task)
    }

    #[tokio::test]
    async fn production_transport_client_uses_exact_authenticated_route_and_accepts_404() {
        let expected = crate::SnapshotTransportStatus {
            schema_version: 1,
            observing_node_id: 2,
            observed_at_unix_ms: 17,
            owned_async_tasks: 0,
            observations: Vec::new(),
            local_request_started_at: None,
            local_receipt_at: None,
        };
        let (address, server) = serve_transport_response(
            "200 OK",
            serde_json::to_string(&expected).expect("serialize status"),
        )
        .await;
        let peer = Node {
            id: 2,
            addr_raft: "127.0.0.1:1".to_owned(),
            addr_api: address,
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build test client");
        let actual =
            request_snapshot_transport_status_sqlite(&client, "exact-test-secret", false, &peer)
                .await
                .expect("read status")
                .expect("status exists");
        server.await.expect("join status server");
        assert_eq!(actual, expected);

        let (address, server) = serve_transport_response("404 Not Found", String::new()).await;
        let peer = Node {
            addr_api: address,
            ..peer
        };
        assert!(
            request_snapshot_transport_status_sqlite(&client, "exact-test-secret", false, &peer,)
                .await
                .expect("404 is compatible")
                .is_none()
        );
        server.await.expect("join 404 server");
    }

    #[test]
    fn snapshot_transport_peer_tracks_current_raft_membership_for_new_learner() {
        let voter = Node {
            id: 1,
            addr_raft: "127.0.0.1:8101".to_owned(),
            addr_api: "127.0.0.1:8201".to_owned(),
        };
        let learner = Node {
            id: 3,
            addr_raft: "127.0.0.1:8103".to_owned(),
            addr_api: "127.0.0.1:8203".to_owned(),
        };
        let mut metrics = RaftMetrics::<u64, Node>::new_initial(1);
        metrics.membership_config = Arc::new(StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([voter.id])],
                BTreeMap::from([(voter.id, voter)]),
            ),
        ));
        let (sender, receiver) = tokio::sync::watch::channel(metrics.clone());
        assert!(snapshot_transport_peer_from_current_membership(&receiver, learner.id).is_err());

        metrics.membership_config = Arc::new(StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([
                    (
                        1,
                        Node {
                            id: 1,
                            addr_raft: "127.0.0.1:8101".to_owned(),
                            addr_api: "127.0.0.1:8201".to_owned(),
                        },
                    ),
                    (learner.id, learner.clone()),
                ]),
            ),
        ));
        sender.send_replace(metrics);

        assert_eq!(
            snapshot_transport_peer_from_current_membership(&receiver, learner.id)
                .expect("newly committed learner is visible"),
            learner
        );
    }

    #[tokio::test]
    async fn production_transport_client_rejects_oversized_unframed_response_body() {
        let oversized_body = vec![b'x'; SNAPSHOT_TRANSPORT_STATUS_MAX_RESPONSE_BYTES + 1];
        let (address, server) = serve_transport_response_without_length(oversized_body).await;
        let peer = Node {
            id: 2,
            addr_raft: "127.0.0.1:1".to_owned(),
            addr_api: address,
        };
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build test client");
        let error =
            request_snapshot_transport_status_sqlite(&client, "exact-test-secret", false, &peer)
                .await
                .expect_err("oversized body must be rejected before JSON decode");
        server.await.expect("join oversized response server");
        assert!(error.to_string().contains("exceeds the 262144 byte limit"));
    }

    #[test]
    fn shutdown_timeout_leaves_room_after_deliberate_cluster_waits() {
        let deliberate_waits = Duration::from_millis(9_500) + Duration::from_secs(5);
        assert!(
            RAFT_SHUTDOWN_TIMEOUT >= deliberate_waits + Duration::from_secs(10),
            "shutdown must retain time for Raft and durable-writer drains after cluster waits"
        );
    }

    #[test]
    fn local_watch_accessor_has_no_remote_fallback() {
        let source = include_str!("mgmt.rs");
        let accessor = source
            .split_once("pub fn local_db_raft_metrics")
            .expect("local watch accessor")
            .1
            .split_once("/// Obtain a commit watermark")
            .expect("end of local watch accessor")
            .0;
        assert!(accessor.contains("self.inner.state.as_ref().ok_or_else"));
        assert!(!accessor.contains("build_addr"));
        assert!(!accessor.contains("get_metrics_remote"));
        assert!(!accessor.contains(".await"));
    }

    #[test]
    fn quorum_watermark_reuses_the_existing_consistent_query_wire_variant() {
        let source = include_str!("mgmt.rs");
        let accessor = source
            .split_once("async fn db_quorum_watermark_req")
            .expect("quorum watermark request")
            .1
            .split_once("/// Get cluster metrics for the database Raft.")
            .expect("end of quorum watermark request")
            .0;
        assert!(accessor.contains("query_remote_req"));
        assert!(accessor.contains("DB_QUORUM_WATERMARK_MARKER"));
        assert!(!accessor.contains("ApiStreamRequestPayload"));

        let stream = include_str!("../network/api.rs");
        assert!(stream.contains("ApiStreamRequestPayload::QueryConsistent"));
        assert!(!stream.contains("ApiStreamRequestPayload::QuorumWatermark"));
    }

    #[test]
    fn quorum_watermark_row_round_trips_full_u64_values() {
        let expected = super::DbQuorumWatermark {
            term: u64::MAX,
            leader_id: u64::MAX - 1,
            committed_index: u64::MAX - 2,
            local_read_protocol_version: super::DB_LOCAL_READ_PROTOCOL_VERSION,
        };
        let actual = crate::query::rows::RowOwned::from_db_quorum_watermark(expected)
            .into_db_quorum_watermark()
            .expect("watermark row");
        assert_eq!(actual, expected);
    }

    #[test]
    fn old_three_column_watermark_preserves_readiness_but_disables_local_reads() {
        use crate::query::rows::{ColumnOwned, RowOwned, ValueOwned};

        let actual = RowOwned {
            columns: vec![
                ColumnOwned {
                    name: "term".to_owned(),
                    value: ValueOwned::Text("7".to_owned()),
                },
                ColumnOwned {
                    name: "leader_id".to_owned(),
                    value: ValueOwned::Text("2".to_owned()),
                },
                ColumnOwned {
                    name: "committed_index".to_owned(),
                    value: ValueOwned::Text("41".to_owned()),
                },
            ],
        }
        .into_db_quorum_watermark()
        .expect("P3a watermark remains a valid quorum proof");

        assert_eq!(actual.term, 7);
        assert_eq!(actual.leader_id, 2);
        assert_eq!(actual.committed_index, 41);
        assert_eq!(actual.local_read_protocol_version, 0);
    }

    #[test]
    fn malformed_present_local_read_protocol_is_rejected() {
        use crate::query::rows::{ColumnOwned, RowOwned, ValueOwned};

        let error = RowOwned {
            columns: vec![
                ColumnOwned {
                    name: "term".to_owned(),
                    value: ValueOwned::Text("7".to_owned()),
                },
                ColumnOwned {
                    name: "leader_id".to_owned(),
                    value: ValueOwned::Text("2".to_owned()),
                },
                ColumnOwned {
                    name: "committed_index".to_owned(),
                    value: ValueOwned::Text("41".to_owned()),
                },
                ColumnOwned {
                    name: "local_read_protocol_version".to_owned(),
                    value: ValueOwned::Text("not-a-version".to_owned()),
                },
            ],
        }
        .into_db_quorum_watermark()
        .expect_err("a malformed advertised protocol is not an old leader");

        assert!(
            error
                .to_string()
                .contains("invalid local_read_protocol_version")
        );
    }

    #[tokio::test]
    async fn remote_shutdown_joins_streams_with_every_endpoint_unavailable() {
        let client = crate::Client::remote(
            vec!["127.0.0.1:0".to_owned()],
            false,
            false,
            "remote-shutdown-test".to_owned(),
            true,
            #[cfg(feature = "cache")]
            Some(crate::config::RateLimitConfig { rps: 0, burst: 0 }),
            Some(crate::config::RateLimitConfig { rps: 0, burst: 0 }),
        )
        .await
        .expect("create remote client");
        let expected_background_tasks = if cfg!(feature = "listen_notify") {
            4
        } else if cfg!(feature = "cache") {
            3
        } else {
            2
        };
        assert_eq!(
            client
                .inner
                .background_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            expected_background_tasks
        );

        // Let the ticker consume its immediate first tick, then queue both a
        // stream request and rate waiters so shutdown must resolve them.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let operation_client = client.clone();
        let operation = tokio::spawn(async move { operation_client.db_quorum_watermark().await });
        let db_rate_client = client.clone();
        let db_rate = tokio::spawn(async move { db_rate_client.rate_limit_db().await });
        #[cfg(feature = "cache")]
        let cache_rate_client = client.clone();
        #[cfg(feature = "cache")]
        let cache_rate = tokio::spawn(async move { cache_rate_client.rate_limit_cache().await });
        tokio::task::yield_now().await;

        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .expect("remote shutdown deadline")
            .expect("remote shutdown");
        assert!(
            operation
                .await
                .expect("stream operation did not panic")
                .is_err()
        );
        assert!(
            db_rate
                .await
                .expect("DB rate waiter did not panic")
                .is_err()
        );
        #[cfg(feature = "cache")]
        assert!(
            cache_rate
                .await
                .expect("cache rate waiter did not panic")
                .is_err()
        );
        assert!(*client.inner.stream_shutdown.borrow());
        assert!(
            client
                .inner
                .background_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }
}
