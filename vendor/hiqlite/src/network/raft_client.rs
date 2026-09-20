use crate::Node;
use crate::NodeId;
use crate::app_state::RaftType;
use crate::helpers::{deserialize, serialize};
use crate::network::frame_io::{
    CLOSE_WRITE_TIMEOUT, write_close_frame_flushed, write_frame_flushed,
};
use crate::network::raft_server::{
    RaftStreamRequest, RaftStreamResponse, RaftStreamResponsePayload,
};
use crate::network::web_socket_connect;
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, WebSocketWrite};
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf};
use tokio::sync::watch;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::{select, task, time};
use tracing::{debug, error, info};

#[cfg(feature = "validation-test-helpers")]
static VALIDATION_RAFT_PARTITIONED: AtomicBool = AtomicBool::new(false);

/// Isolate this validation process from every Raft peer without changing its
/// authenticated client API. The feature is absent from production builds.
#[cfg(feature = "validation-test-helpers")]
pub fn validation_set_raft_partitioned(partitioned: bool) {
    VALIDATION_RAFT_PARTITIONED.store(partitioned, Ordering::Release);
}

#[cfg(feature = "validation-test-helpers")]
pub(crate) fn validation_raft_partitioned() -> bool {
    VALIDATION_RAFT_PARTITIONED.load(Ordering::Acquire)
}

#[cfg(feature = "cache")]
use crate::store::state_machine::memory::TypeConfigKV;

#[cfg(feature = "sqlite")]
use crate::store::state_machine::sqlite::TypeConfigSqlite;

#[cfg(any(feature = "cache", feature = "sqlite"))]
use crate::Error;
#[cfg(any(feature = "cache", feature = "sqlite"))]
use openraft::{
    OptionalSend, Snapshot, Vote,
    error::{Fatal, InstallSnapshotError, RaftError, ReplicationClosed, StreamingError, Timeout},
    network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
};

#[cfg(any(feature = "cache", feature = "sqlite"))]
use openraft::network::snapshot_transport::{Chunked, SnapshotTransport};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotRpcBudgets {
    chunk: Duration,
    transfer: Duration,
    install: Duration,
}

impl SnapshotRpcBudgets {
    pub(crate) fn from_node_config(config: &crate::NodeConfig) -> Self {
        Self {
            chunk: config.snapshot_chunk_timeout,
            transfer: config.snapshot_transfer_timeout,
            install: Duration::from_millis(config.raft_config.install_snapshot_timeout),
        }
    }
}

pub struct NetworkStreaming {
    pub node_id: NodeId,
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    pub secret_raft: Vec<u8>,
    pub raft_type: RaftType,
    pub heartbeat_interval: u64,
    pub is_raft_stopped: Arc<AtomicBool>,
    pub is_startup_finished: Arc<AtomicBool>,
    pub(crate) snapshot_budgets: SnapshotRpcBudgets,
    pub(crate) snapshot_transport: crate::LocalSnapshotTransportStatus,
    // pub sender: flume::Sender<RaftRequest>,
}

#[cfg(feature = "cache")]
impl RaftNetworkFactory<TypeConfigKV> for NetworkStreaming {
    type Network = NetworkConnectionStreaming;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        debug!("Building new Raft Cache client with target {}", node);

        let (sender, rx) = flume::bounded(1);
        let reset = Arc::new(ConnectionResetState::default());
        let shutdown = Arc::new(ConnectionShutdownState::default());
        let snapshot_attempt = Arc::new(StdMutex::new(None));
        let transport_connection_id = self.snapshot_transport.next_outbound_connection_id();

        let task_status = self.snapshot_transport.clone();
        let handler = Self::ws_handler(
            self.node_id,
            self.raft_type.clone(),
            node.clone(),
            self.tls_config.clone(),
            self.secret_raft.clone(),
            rx,
            self.heartbeat_interval,
            self.is_raft_stopped.clone(),
            self.is_startup_finished.clone(),
            Arc::clone(&reset),
            Arc::clone(&shutdown),
            self.snapshot_transport.clone(),
            "cache",
            transport_connection_id,
        );
        let task = tokio::task::spawn(Box::pin(async move {
            let _task_guard = task_status.owned_async_task();
            handler.await;
        }));

        NetworkConnectionStreaming {
            node: node.clone(),
            sender,
            reset,
            shutdown,
            local_node_id: self.node_id,
            raft_group: "cache",
            snapshot_budgets: self.snapshot_budgets,
            snapshot_transport: self.snapshot_transport.clone(),
            snapshot_attempt,
            transport_connection_id,
            runtime: tokio::runtime::Handle::current(),
            task: Some(task),
        }
    }
}

#[cfg(feature = "sqlite")]
impl RaftNetworkFactory<TypeConfigSqlite> for NetworkStreaming {
    type Network = NetworkConnectionStreaming;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        debug!("Building new Raft DB client with target {}", node);

        let (sender, rx) = flume::bounded(1);
        let reset = Arc::new(ConnectionResetState::default());
        let shutdown = Arc::new(ConnectionShutdownState::default());
        let snapshot_attempt = Arc::new(StdMutex::new(None));
        let transport_connection_id = self.snapshot_transport.next_outbound_connection_id();

        let task_status = self.snapshot_transport.clone();
        let handler = Self::ws_handler(
            self.node_id,
            self.raft_type.clone(),
            node.clone(),
            self.tls_config.clone(),
            self.secret_raft.clone(),
            rx,
            self.heartbeat_interval,
            self.is_raft_stopped.clone(),
            self.is_startup_finished.clone(),
            Arc::clone(&reset),
            Arc::clone(&shutdown),
            self.snapshot_transport.clone(),
            "sqlite",
            transport_connection_id,
        );
        let task = tokio::task::spawn(Box::pin(async move {
            let _task_guard = task_status.owned_async_task();
            handler.await;
        }));

        NetworkConnectionStreaming {
            node: node.clone(),
            sender,
            reset,
            shutdown,
            local_node_id: self.node_id,
            raft_group: "sqlite",
            snapshot_budgets: self.snapshot_budgets,
            snapshot_transport: self.snapshot_transport.clone(),
            snapshot_attempt,
            transport_connection_id,
            runtime: tokio::runtime::Handle::current(),
            task: Some(task),
        }
    }
}

enum RaftRequest {
    #[cfg(feature = "sqlite")]
    AppendDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            AppendEntriesRequest<TypeConfigSqlite>,
        ),
    ),
    #[cfg(feature = "sqlite")]
    VoteDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            VoteRequest<u64>,
        ),
    ),
    #[cfg(feature = "sqlite")]
    SnapshotDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            InstallSnapshotRequest<TypeConfigSqlite>,
        ),
    ),

    #[cfg(feature = "cache")]
    AppendCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            AppendEntriesRequest<TypeConfigKV>,
        ),
    ),
    #[cfg(feature = "cache")]
    VoteCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            VoteRequest<u64>,
        ),
    ),
    #[cfg(feature = "cache")]
    SnapshotCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            InstallSnapshotRequest<TypeConfigKV>,
        ),
    ),

    StreamResponse(RaftStreamResponse),

    Shutdown,
}

impl RaftRequest {
    fn kind(&self) -> &'static str {
        match self {
            #[cfg(feature = "sqlite")]
            Self::AppendDB(_) => "append_db",
            #[cfg(feature = "sqlite")]
            Self::VoteDB(_) => "vote_db",
            #[cfg(feature = "sqlite")]
            Self::SnapshotDB(_) => "snapshot_db",
            #[cfg(feature = "cache")]
            Self::AppendCache(_) => "append_cache",
            #[cfg(feature = "cache")]
            Self::VoteCache(_) => "vote_cache",
            #[cfg(feature = "cache")]
            Self::SnapshotCache(_) => "snapshot_cache",
            Self::StreamResponse(_) => "stream_response",
            Self::Shutdown => "shutdown",
        }
    }

    fn snapshot_chunk_bytes(&self) -> Option<usize> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SnapshotDB((_, request)) => Some(request.data.len()),
            #[cfg(feature = "cache")]
            Self::SnapshotCache((_, request)) => Some(request.data.len()),
            _ => None,
        }
    }

    fn outbound_disposition(
        &self,
        socket_epoch: u64,
        reset_epoch: u64,
    ) -> Option<OutboundDisposition> {
        let response_is_closed = match self {
            #[cfg(feature = "sqlite")]
            Self::AppendDB((ack, _)) => ack.is_closed(),
            #[cfg(feature = "sqlite")]
            Self::VoteDB((ack, _)) => ack.is_closed(),
            #[cfg(feature = "sqlite")]
            Self::SnapshotDB((ack, _)) => ack.is_closed(),
            #[cfg(feature = "cache")]
            Self::AppendCache((ack, _)) => ack.is_closed(),
            #[cfg(feature = "cache")]
            Self::VoteCache((ack, _)) => ack.is_closed(),
            #[cfg(feature = "cache")]
            Self::SnapshotCache((ack, _)) => ack.is_closed(),
            Self::StreamResponse(_) | Self::Shutdown => return None,
        };

        if response_is_closed {
            Some(OutboundDisposition::DropCancelled)
        } else if reset_epoch != socket_epoch {
            Some(OutboundDisposition::Reconnect)
        } else {
            Some(OutboundDisposition::Send)
        }
    }

    fn fail_outbound(self, error: Error) {
        let ack = match self {
            #[cfg(feature = "sqlite")]
            Self::AppendDB((ack, _)) => Some(ack),
            #[cfg(feature = "sqlite")]
            Self::VoteDB((ack, _)) => Some(ack),
            #[cfg(feature = "sqlite")]
            Self::SnapshotDB((ack, _)) => Some(ack),
            #[cfg(feature = "cache")]
            Self::AppendCache((ack, _)) => Some(ack),
            #[cfg(feature = "cache")]
            Self::VoteCache((ack, _)) => Some(ack),
            #[cfg(feature = "cache")]
            Self::SnapshotCache((ack, _)) => Some(ack),
            Self::StreamResponse(_) | Self::Shutdown => None,
        };
        if let Some(ack) = ack {
            let _ = ack.send(Err(error));
        }
    }
}

#[derive(Debug)]
enum WritePayload {
    Payload(Vec<u8>),
    Close,
}

#[derive(Default)]
struct ConnectionResetState {
    epoch: AtomicU64,
    socket_epoch: AtomicU64,
    connection_attempt_sequence: AtomicU64,
    connected: AtomicBool,
    notify: Notify,
}

#[derive(Default)]
struct ConnectionShutdownState {
    requested: AtomicBool,
    notify: Notify,
}

impl ConnectionShutdownState {
    fn request(&self) {
        if !self.requested.swap(true, Ordering::AcqRel) {
            self.notify.notify_one();
        }
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    async fn requested(&self) {
        while !self.is_requested() {
            self.notify.notified().await;
        }
    }
}

impl ConnectionResetState {
    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn socket_epoch(&self) -> u64 {
        self.socket_epoch.load(Ordering::Acquire)
    }

    fn begin_socket(&self) -> u64 {
        let previous = self
            .socket_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                Some(epoch.saturating_add(1))
            })
            .expect("socket epoch update cannot be rejected");
        previous.saturating_add(1)
    }

    fn begin_connection_attempt(&self) -> u64 {
        self.connection_attempt_sequence
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn connection_attempt_sequence(&self) -> u64 {
        self.connection_attempt_sequence.load(Ordering::Acquire)
    }

    fn mark_connected(&self) {
        self.connected.store(true, Ordering::Release);
    }

    fn mark_disconnected(&self) {
        self.connected.store(false, Ordering::Release);
    }

    fn request_reset(&self, observed_epoch: u64) {
        if self
            .epoch
            .compare_exchange(
                observed_epoch,
                observed_epoch.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.notify.notify_one();
        }
    }

    async fn changed_since(&self, observed_epoch: u64) {
        while self.epoch() == observed_epoch {
            self.notify.notified().await;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum OutboundDisposition {
    Send,
    DropCancelled,
    Reconnect,
}

#[derive(Debug)]
enum WriteEnqueueError {
    Reset,
    Shutdown,
    ReaderFinished(Result<(), String>),
    WriterFinished(Result<(), String>),
    Disconnected(flume::SendError<WritePayload>),
}

// Boxing the request variant would add an allocation to every Raft RPC only to
// reduce this short-lived supervisor value's stack size.
#[allow(clippy::large_enum_variant)]
enum ConnectedEvent {
    Reset,
    Shutdown,
    ReaderFinished(Result<(), String>),
    WriterFinished(Result<(), String>),
    Request(Result<RaftRequest, flume::RecvError>),
}

async fn next_connected_event(
    reset: &ConnectionResetState,
    socket_epoch: u64,
    rx_read: &flume::Receiver<RaftRequest>,
    rx: &flume::Receiver<RaftRequest>,
    shutdown: &ConnectionShutdownState,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
) -> ConnectedEvent {
    select! {
        biased;
        _ = shutdown.requested() => ConnectedEvent::Shutdown,
        _ = reset.changed_since(socket_epoch) => ConnectedEvent::Reset,
        result = reader_finished => ConnectedEvent::ReaderFinished(
            result.unwrap_or_else(|_| Err("Raft reader task exited without reporting an outcome".into()))
        ),
        result = writer_finished => ConnectedEvent::WriterFinished(
            result.unwrap_or_else(|_| Err("Raft writer task exited without reporting an outcome".into()))
        ),
        result = rx_read.recv_async() => ConnectedEvent::Request(result),
        result = rx.recv_async() => ConnectedEvent::Request(result),
    }
}

async fn enqueue_write_or_reset(
    tx_write: &flume::Sender<WritePayload>,
    mut payload: WritePayload,
    reset: &ConnectionResetState,
    socket_epoch: u64,
    shutdown: &ConnectionShutdownState,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
) -> Result<(), WriteEnqueueError> {
    loop {
        // Preserve first-terminal ownership: do not hand a frame to the
        // writer after reset/shutdown/task completion was already latched.
        // These synchronous checks run before every ownership-transfer
        // attempt; the select below handles state that changes while full.
        if shutdown.is_requested() {
            return Err(WriteEnqueueError::Shutdown);
        }
        if reset.epoch() != socket_epoch {
            return Err(WriteEnqueueError::Reset);
        }
        match reader_finished.try_recv() {
            Ok(outcome) => return Err(WriteEnqueueError::ReaderFinished(outcome)),
            Err(oneshot::error::TryRecvError::Closed) => {
                return Err(WriteEnqueueError::ReaderFinished(Err(
                    "Raft reader task exited without reporting an outcome".into(),
                )));
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
        }
        match writer_finished.try_recv() {
            Ok(outcome) => return Err(WriteEnqueueError::WriterFinished(outcome)),
            Err(oneshot::error::TryRecvError::Closed) => {
                return Err(WriteEnqueueError::WriterFinished(Err(
                    "Raft writer task exited without reporting an outcome".into(),
                )));
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
        }
        match tx_write.try_send(payload) {
            Ok(()) => return Ok(()),
            Err(flume::TrySendError::Disconnected(payload)) => {
                return Err(WriteEnqueueError::Disconnected(flume::SendError(payload)));
            }
            Err(flume::TrySendError::Full(pending)) => payload = pending,
        }
        select! {
            biased;
            _ = shutdown.requested() => return Err(WriteEnqueueError::Shutdown),
            _ = reset.changed_since(socket_epoch) => return Err(WriteEnqueueError::Reset),
            result = &mut *reader_finished => return Err(WriteEnqueueError::ReaderFinished(
                result.unwrap_or_else(|_| Err("Raft reader task exited without reporting an outcome".into()))
            )),
            result = &mut *writer_finished => return Err(WriteEnqueueError::WriterFinished(
                result.unwrap_or_else(|_| Err("Raft writer task exited without reporting an outcome".into()))
            )),
            () = time::sleep(Duration::from_millis(1)) => {}
        }
    }
}

async fn stop_stream_tasks(
    tx_write: &flume::Sender<WritePayload>,
    mut handle_write: JoinHandle<()>,
    handle_read: JoinHandle<()>,
    forced_reset: bool,
) {
    // Cleanup must never queue behind a blocked socket write. A reset is a
    // forced transport boundary, so abort both split tasks immediately after
    // a best-effort close. Other reconnects retain the short graceful window.
    let writer_finished = if forced_reset {
        false
    } else {
        time::timeout(CLOSE_WRITE_TIMEOUT, async {
            tx_write.send_async(WritePayload::Close).await.ok()?;
            Some((&mut handle_write).await)
        })
        .await
        .ok()
        .flatten()
        .is_some()
    };

    if !writer_finished {
        handle_write.abort();
        let _ = handle_write.await;
    }
    handle_read.abort();
    let _ = handle_read.await;
}

#[allow(clippy::type_complexity)]
impl NetworkStreaming {
    #[allow(clippy::too_many_arguments)]
    async fn ws_handler(
        this_node: NodeId,
        raft_type: RaftType,
        node: Node,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        secret: Vec<u8>,
        rx: flume::Receiver<RaftRequest>,
        heartbeat_interval: u64,
        is_raft_stopped: Arc<AtomicBool>,
        is_startup_finished: Arc<AtomicBool>,
        reset: Arc<ConnectionResetState>,
        connection_shutdown: Arc<ConnectionShutdownState>,
        snapshot_transport: crate::LocalSnapshotTransportStatus,
        raft_group: &'static str,
        transport_connection_id: u64,
    ) {
        let mut request_id = 0usize;
        // TODO probably, a Vec<_> is faster here since we would never have too many in flight reqs
        // for raft internal replication and voting? -> check
        // maybe feature-gate an alternative impl, even though it might not make the biggest difference
        let mut in_flight: HashMap<
            usize,
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
        > = HashMap::with_capacity(4);
        let mut shutdown = false;

        'outer: loop {
            if connection_shutdown.is_requested() {
                break;
            }
            if is_raft_stopped.load(Ordering::Relaxed) {
                if !is_startup_finished.load(Ordering::Relaxed) {
                    debug!("Raft is still starting up - skipping initial connection");
                    select! {
                        biased;
                        _ = connection_shutdown.requested() => break 'outer,
                        () = time::sleep(Duration::from_secs(1)) => {}
                    }
                    continue;
                }

                debug!("Raft is stopped - exiting NetworkStreaming::ws_handler()");
                break;
            }

            let connection_attempt_sequence = reset.begin_connection_attempt();
            snapshot_transport.connecting_owned(
                raft_group,
                node.id,
                transport_connection_id,
                reset.socket_epoch(),
                connection_attempt_sequence,
            );
            info!("Trying to open WebSocket stream");
            let (socket, reset_epoch) = {
                let connection = web_socket_connect::try_connect(
                    this_node,
                    &node.addr_raft,
                    &raft_type,
                    tls_config.clone(),
                    &secret,
                );
                tokio::pin!(connection);
                let connection = select! {
                    biased;
                    _ = connection_shutdown.requested() => break 'outer,
                    result = &mut connection => result,
                };
                match connection {
                    Ok(socket) => {
                        info!("WebSocket connected successfully");
                        let socket_epoch = reset.begin_socket();
                        reset.mark_connected();
                        snapshot_transport.connected_owned(
                            raft_group,
                            node.id,
                            transport_connection_id,
                            socket_epoch,
                            connection_attempt_sequence,
                        );
                        (socket, reset.epoch())
                    }
                    Err(err) => {
                        error!("Socket connect error to node {}: {:?}", node.id, err);

                        for _ in 0..3 {
                            // if there is a network error, no reason to try too hard to connect
                            select! {
                                biased;
                                _ = connection_shutdown.requested() => break 'outer,
                                () = time::sleep(Duration::from_millis(heartbeat_interval)) => {}
                            }

                            // make sure channel is always free
                            match rx.try_recv() {
                                Ok(req) => {
                                    let ack = match req {
                                        #[cfg(feature = "sqlite")]
                                        RaftRequest::AppendDB((ack, _)) => Some(ack),
                                        #[cfg(feature = "sqlite")]
                                        RaftRequest::VoteDB((ack, _)) => Some(ack),
                                        #[cfg(feature = "sqlite")]
                                        RaftRequest::SnapshotDB((ack, _)) => Some(ack),
                                        #[cfg(feature = "cache")]
                                        RaftRequest::AppendCache((ack, _)) => Some(ack),
                                        #[cfg(feature = "cache")]
                                        RaftRequest::VoteCache((ack, _)) => Some(ack),
                                        #[cfg(feature = "cache")]
                                        RaftRequest::SnapshotCache((ack, _)) => Some(ack),
                                        RaftRequest::StreamResponse(_) => None,
                                        RaftRequest::Shutdown => {
                                            connection_shutdown.request();
                                            break 'outer;
                                        }
                                    };

                                    if let Some(ack) = ack {
                                        let _ = ack.send(Err(Error::Connect(err.to_string())));
                                    }
                                }
                                // `Drop` normally queues `Shutdown`, but a request may already
                                // occupy the bounded channel. Once the last sender disappears,
                                // disconnection is the equally authoritative shutdown signal.
                                Err(flume::TryRecvError::Disconnected) => break 'outer,
                                Err(flume::TryRecvError::Empty) => {}
                            }
                        }

                        continue;
                    }
                }
            };
            assert!(
                in_flight.is_empty(),
                "raft in flight buffer should always be empty when restoring a connection"
            );
            let (tx_write, rx_write) = flume::bounded(1);
            let (tx_read, rx_read) = flume::bounded(1);

            // TODO splitting needs `unstable-split` feature right now but is about to be stabilized soon
            let (read, write) = socket.split(tokio::io::split);
            // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
            let read = FragmentCollectorRead::new(read);

            let (tx_reader_finished, mut rx_reader_finished) = oneshot::channel();
            let reader_task_status = snapshot_transport.clone();
            let handle_read = task::spawn(Box::pin(async move {
                let _task_guard = reader_task_status.owned_async_task();
                let outcome = Self::stream_reader(read, tx_read).await;
                let _ = tx_reader_finished.send(outcome);
            }));
            let (tx_writer_finished, mut rx_writer_finished) = oneshot::channel();
            let writer_task_status = snapshot_transport.clone();
            let handle_write = task::spawn(Box::pin(async move {
                let _task_guard = writer_task_status.owned_async_task();
                Self::stream_writer(write, rx_write, tx_writer_finished).await;
            }));

            let mut forced_reset = false;
            'connected: loop {
                let res = match next_connected_event(
                    &reset,
                    reset_epoch,
                    &rx_read,
                    &rx,
                    &connection_shutdown,
                    &mut rx_reader_finished,
                    &mut rx_writer_finished,
                )
                .await
                {
                    ConnectedEvent::Reset => {
                        debug!("RPC future was cancelled - reconnecting Raft stream");
                        forced_reset = true;
                        break;
                    }
                    ConnectedEvent::Shutdown => {
                        debug!("Raft connection shutdown requested");
                        shutdown = true;
                        break;
                    }
                    ConnectedEvent::ReaderFinished(outcome) => {
                        match outcome {
                            Ok(()) => debug!("Raft WebSocket reader exited"),
                            Err(err) => error!("Raft WebSocket reader failed: {err}"),
                        }
                        forced_reset = true;
                        break;
                    }
                    ConnectedEvent::WriterFinished(outcome) => {
                        match outcome {
                            Ok(()) => error!("Raft WebSocket writer exited while connected"),
                            Err(err) => error!("Raft WebSocket writer failed: {err}"),
                        }
                        forced_reset = true;
                        break;
                    }
                    ConnectedEvent::Request(res) => res,
                };

                let req = match res {
                    Ok(r) => r,
                    Err(err) => {
                        error!("Client stream reader error: {}", err,);

                        if rx.is_disconnected() {
                            debug!("Raft tx dropped - exiting Stream Reader");
                            shutdown = true;
                        }
                        if rx_read.is_disconnected() {
                            debug!("Client Stream reader exited - initiating shutdown + reconnect");
                        }

                        break;
                    }
                };

                match req.outbound_disposition(reset_epoch, reset.epoch()) {
                    Some(OutboundDisposition::DropCancelled) => {
                        debug!("Dropping cancelled Raft request before transport write");
                        continue;
                    }
                    Some(OutboundDisposition::Reconnect) => {
                        req.fail_outbound(Error::Connect(
                            "Raft transport reset before request write".into(),
                        ));
                        forced_reset = true;
                        break;
                    }
                    Some(OutboundDisposition::Send) | None => {}
                }

                let stream_req = match req {
                    #[cfg(feature = "sqlite")]
                    RaftRequest::AppendDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::AppendDB((request_id, req))))
                    }
                    #[cfg(feature = "sqlite")]
                    RaftRequest::VoteDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::VoteDB((request_id, req))))
                    }
                    #[cfg(feature = "sqlite")]
                    RaftRequest::SnapshotDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::SnapshotDB((request_id, req))))
                    }

                    #[cfg(feature = "cache")]
                    RaftRequest::AppendCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::AppendCache((request_id, req))))
                    }
                    #[cfg(feature = "cache")]
                    RaftRequest::VoteCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::VoteCache((request_id, req))))
                    }
                    #[cfg(feature = "cache")]
                    RaftRequest::SnapshotCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::SnapshotCache((request_id, req))))
                    }

                    RaftRequest::StreamResponse(resp) => {
                        match in_flight.remove(&resp.request_id) {
                            None => {
                                error!("client ack for RaftStreamResponse missing");
                            }
                            Some(ack) => {
                                if ack.send(Ok(resp.payload)).is_err() {
                                    error!("sending back stream response from raft server");
                                }
                            }
                        }
                        None
                    }

                    RaftRequest::Shutdown => {
                        debug!("RaftRequest::Shutdown");
                        connection_shutdown.request();
                        shutdown = true;
                        break;
                    }
                };

                if let Some((ack, payload)) = stream_req {
                    let bytes = serialize(&payload).unwrap();

                    match enqueue_write_or_reset(
                        &tx_write,
                        WritePayload::Payload(bytes),
                        &reset,
                        reset_epoch,
                        &connection_shutdown,
                        &mut rx_reader_finished,
                        &mut rx_writer_finished,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(WriteEnqueueError::Reset) => {
                            let _ = ack.send(Err(Error::Connect(
                                "Raft transport reset during request write".into(),
                            )));
                            forced_reset = true;
                            break 'connected;
                        }
                        Err(WriteEnqueueError::Shutdown) => {
                            let _ = ack.send(Err(Error::Connect(
                                "Raft connection shutdown during request write".into(),
                            )));
                            shutdown = true;
                            break 'connected;
                        }
                        Err(WriteEnqueueError::ReaderFinished(outcome)) => {
                            let detail = outcome
                                .err()
                                .unwrap_or_else(|| "reader exited while connected".into());
                            let _ = ack.send(Err(Error::Connect(format!(
                                "Raft reader ended during request write: {detail}"
                            ))));
                            forced_reset = true;
                            break 'connected;
                        }
                        Err(WriteEnqueueError::WriterFinished(outcome)) => {
                            let detail = outcome
                                .err()
                                .unwrap_or_else(|| "writer exited while connected".into());
                            let _ = ack.send(Err(Error::Connect(format!(
                                "Raft writer ended during request write: {detail}"
                            ))));
                            forced_reset = true;
                            break 'connected;
                        }
                        Err(WriteEnqueueError::Disconnected(err)) => {
                            let _ = ack.send(Err(Error::Connect(format!(
                                "Error sending Write Request to WebSocket writer: {err}"
                            ))));
                            break 'connected;
                        }
                    }

                    in_flight.insert(request_id, ack);
                    request_id += 1;
                }
            }

            reset.mark_disconnected();
            stop_stream_tasks(&tx_write, handle_write, handle_read, forced_reset).await;

            for (_, ack) in in_flight.drain() {
                let _ = ack.send(Err(Error::Connect("Raft WebSocket stream ended".into())));
            }
            // reset to a reasonable size for the next start to keep memory usage under control
            in_flight = HashMap::with_capacity(4);

            if shutdown {
                break;
            }
        }

        debug!("Raft Client shut down, tx closed, exiting WsHandler");
    }

    async fn stream_reader<S>(
        mut read: FragmentCollectorRead<ReadHalf<S>>,
        tx: flume::Sender<RaftRequest>,
    ) -> Result<(), String>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            let frame = read
                .read_frame(&mut |frame| async move {
                    // TODO obligated sends should be auto ping / pong / close ? -> verify!
                    debug!(
                        opcode = ?frame.opcode,
                        payload_len = frame.payload.len(),
                        "received obligated send in Raft stream client"
                    );
                    Ok::<(), Error>(())
                })
                .await
                .map_err(|err| err.to_string())?;
            match frame.opcode {
                OpCode::Continuation => {}
                OpCode::Text => {}
                OpCode::Binary => {
                    let bytes = frame.payload.deref();
                    let payload = deserialize::<RaftStreamResponse>(bytes)
                        .map_err(|err| format!("invalid Raft stream response: {err}"))?;
                    tx.send_async(RaftRequest::StreamResponse(payload))
                        .await
                        .map_err(|err| format!("Raft reader outcome channel closed: {err}"))?;
                }
                OpCode::Close => break,
                OpCode::Ping => {}
                OpCode::Pong => {}
            }
        }

        debug!("Exiting Client Stream Reader");
        Ok(())
    }

    async fn stream_writer<S>(
        mut write: WebSocketWrite<S>,
        rx: flume::Receiver<WritePayload>,
        finished: oneshot::Sender<Result<(), String>>,
    ) where
        S: AsyncWrite + Unpin,
    {
        let outcome = loop {
            let payload = match rx.recv_async().await {
                Ok(payload) => payload,
                Err(_) => break Ok(()),
            };
            match payload {
                WritePayload::Payload(bytes) => {
                    if let Err(err) = write_raft_request_frame(&mut write, bytes).await {
                        error!("Client Stream error: {:?}", err);
                        break Err(err.to_string());
                    }
                }
                WritePayload::Close => {
                    debug!("Received Close request in Client Stream Writer");
                    let _ =
                        write_close_frame_flushed(&mut write, Frame::close(1000, b"go away")).await;
                    break Ok(());
                }
            }
        };

        let _ = finished.send(outcome);
        debug!("Exiting Client Stream Writer");
    }
}

async fn write_raft_request_frame<S>(
    write: &mut WebSocketWrite<S>,
    bytes: Vec<u8>,
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: AsyncWrite + Unpin,
{
    write_frame_flushed(write, Frame::binary(Payload::from(bytes))).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotAttemptPhase {
    Transfer,
    FinalInstall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotStageDeadline {
    phase: SnapshotAttemptPhase,
    deadline: time::Instant,
}

#[derive(Debug, Clone, Copy)]
struct FinalInstallWindow {
    start: time::Instant,
    deadline: time::Instant,
}

#[derive(Debug)]
struct SnapshotAttempt {
    id: u64,
    start: time::Instant,
    snapshot_id: String,
    transfer_deadline: time::Instant,
    final_install: StdMutex<Option<FinalInstallWindow>>,
    stage: watch::Sender<SnapshotStageDeadline>,
    transport_attempt: Option<crate::transport_status::OutboundSnapshotAttempt>,
}

impl SnapshotAttempt {
    #[cfg(test)]
    fn new(id: u64, snapshot_id: String, budgets: SnapshotRpcBudgets) -> Arc<Self> {
        Self::new_with_transport(id, &snapshot_id, budgets, None)
    }

    fn tracked(
        transport_attempt: crate::transport_status::OutboundSnapshotAttempt,
        snapshot_id: &str,
        budgets: SnapshotRpcBudgets,
    ) -> Arc<Self> {
        Self::new_with_transport(
            transport_attempt.attempt_id,
            snapshot_id,
            budgets,
            Some(transport_attempt),
        )
    }

    fn new_with_transport(
        id: u64,
        snapshot_id: &str,
        budgets: SnapshotRpcBudgets,
        transport_attempt: Option<crate::transport_status::OutboundSnapshotAttempt>,
    ) -> Arc<Self> {
        let start = time::Instant::now();
        let transfer_deadline = start + budgets.transfer;
        let (stage, _) = watch::channel(SnapshotStageDeadline {
            phase: SnapshotAttemptPhase::Transfer,
            deadline: transfer_deadline,
        });
        Arc::new(Self {
            id,
            start,
            snapshot_id: crate::transport_status::retained_snapshot_id(snapshot_id),
            transfer_deadline,
            final_install: StdMutex::new(None),
            stage,
            transport_attempt,
        })
    }

    fn rpc_deadline(
        &self,
        done: bool,
        hard_ttl: Duration,
        budgets: SnapshotRpcBudgets,
    ) -> Option<time::Instant> {
        let now = time::Instant::now();
        if !done {
            self.stage.send_replace(SnapshotStageDeadline {
                phase: SnapshotAttemptPhase::Transfer,
                deadline: self.transfer_deadline,
            });
            return Some(
                (now + budgets.chunk)
                    .min(self.transfer_deadline)
                    .min(now + hard_ttl),
            );
        }

        let mut final_install = self
            .final_install
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let returning_from_transfer = self.stage.borrow().phase == SnapshotAttemptPhase::Transfer;
        if returning_from_transfer && now >= self.transfer_deadline {
            return None;
        }
        if final_install.is_none() {
            let attempt_end = self.start + budgets.transfer + budgets.install;
            *final_install = Some(FinalInstallWindow {
                start: now,
                deadline: (now + budgets.install).min(attempt_end).min(now + hard_ttl),
            });
        }
        let final_window = final_install.expect("final install window was just initialized");
        debug_assert!(final_window.start >= self.start);
        self.stage.send_replace(SnapshotStageDeadline {
            phase: SnapshotAttemptPhase::FinalInstall,
            deadline: final_window.deadline,
        });
        Some(final_window.deadline)
    }

    fn return_to_transfer(&self) {
        self.stage.send_replace(SnapshotStageDeadline {
            phase: SnapshotAttemptPhase::Transfer,
            deadline: self.transfer_deadline,
        });
    }
}

struct SnapshotAttemptGuard {
    attempt_id: u64,
    slot: Arc<StdMutex<Option<Arc<SnapshotAttempt>>>>,
    snapshot_transport: crate::LocalSnapshotTransportStatus,
    transport_attempt: crate::transport_status::OutboundSnapshotAttempt,
    terminal_armed: bool,
}

impl SnapshotAttemptGuard {
    fn new(
        attempt_id: u64,
        slot: Arc<StdMutex<Option<Arc<SnapshotAttempt>>>>,
        snapshot_transport: crate::LocalSnapshotTransportStatus,
        transport_attempt: crate::transport_status::OutboundSnapshotAttempt,
    ) -> Self {
        Self {
            attempt_id,
            slot,
            snapshot_transport,
            transport_attempt,
            terminal_armed: true,
        }
    }

    fn disarm(&mut self) {
        self.terminal_armed = false;
    }
}

impl Drop for SnapshotAttemptGuard {
    fn drop(&mut self) {
        if self.terminal_armed {
            self.snapshot_transport
                .outbound_failed_owned(&self.transport_attempt, "snapshot_attempt_ended");
        }
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot
            .as_ref()
            .is_some_and(|attempt| attempt.id == self.attempt_id)
        {
            *slot = None;
        }
    }
}

async fn wait_for_snapshot_deadline(
    mut stage: watch::Receiver<SnapshotStageDeadline>,
) -> SnapshotStageDeadline {
    loop {
        let current = *stage.borrow_and_update();
        tokio::select! {
            biased;
            changed = stage.changed() => {
                if changed.is_err() {
                    return current;
                }
            }
            _ = time::sleep_until(current.deadline) => {
                // A phase update racing the old timer owns the decision. In
                // particular, final dispatch just before T must switch to its
                // latched install deadline instead of losing to a stale,
                // simultaneously-ready transfer sleep.
                if stage.has_changed().unwrap_or(false) {
                    continue;
                }
                return current;
            }
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct NetworkConnectionStreaming {
    node: Node,
    sender: flume::Sender<RaftRequest>,
    reset: Arc<ConnectionResetState>,
    shutdown: Arc<ConnectionShutdownState>,
    local_node_id: NodeId,
    raft_group: &'static str,
    snapshot_budgets: SnapshotRpcBudgets,
    snapshot_transport: crate::LocalSnapshotTransportStatus,
    snapshot_attempt: Arc<StdMutex<Option<Arc<SnapshotAttempt>>>>,
    transport_connection_id: u64,
    runtime: tokio::runtime::Handle,
    task: Option<JoinHandle<()>>,
}

struct ConnectionResetGuard {
    reset: Option<(Arc<ConnectionResetState>, u64)>,
}

impl ConnectionResetGuard {
    fn new(reset: Arc<ConnectionResetState>) -> Self {
        let epoch = reset.epoch();
        Self {
            reset: Some((reset, epoch)),
        }
    }

    fn disarm(&mut self) {
        self.reset = None;
    }
}

impl Drop for ConnectionResetGuard {
    fn drop(&mut self) {
        if let Some((reset, epoch)) = self.reset.take() {
            // A reset must not share the bounded request queue. The queue may
            // still contain the RPC whose future OpenRaft just dropped; a
            // best-effort `try_send` can then lose the only instruction that
            // tears down its stale WebSocket. The epoch is durable state, so
            // cancellation cannot be missed or applied to a replacement
            // socket created after this request began.
            reset.request_reset(epoch);
        }
    }
}

impl Drop for NetworkConnectionStreaming {
    fn drop(&mut self) {
        self.shutdown.request();
        let _ = self.sender.try_send(RaftRequest::Shutdown);
        let Some(task) = self.task.take() else {
            return;
        };
        // Drop cannot await and may run on a non-runtime thread. The handle
        // captured when this connection was constructed remains the cleanup
        // owner for its supervisor and split socket tasks.
        self.runtime.spawn(async move {
            let _ = task.await;
        });
    }
}

impl NetworkConnectionStreaming {
    fn snapshot_rpc_ttl(&self, done: bool, option: &RPCOption) -> Option<Duration> {
        let now = time::Instant::now();
        let attempt = self
            .snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let deadline = match attempt {
            Some(attempt) => attempt.rpc_deadline(done, option.hard_ttl(), self.snapshot_budgets),
            None => Some(
                (now + if done {
                    self.snapshot_budgets.install
                } else {
                    self.snapshot_budgets.chunk
                })
                .min(now + option.hard_ttl()),
            ),
        };
        deadline
            .filter(|deadline| *deadline > now)
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    fn snapshot_timeout_error<Err>(&self) -> RPCError<NodeId, Node, Err>
    where
        Err: std::error::Error,
    {
        RPCError::Timeout(Timeout {
            action: RPCTypes::InstallSnapshot,
            id: self.local_node_id,
            target: self.node.id,
            timeout: Duration::ZERO,
        })
    }

    fn snapshot_transfer_deadline(&self) -> Option<time::Instant> {
        self.snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|attempt| attempt.transfer_deadline)
    }

    fn snapshot_stage_deadline(&self) -> Option<SnapshotStageDeadline> {
        self.snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|attempt| *attempt.stage.borrow())
    }

    fn snapshot_transport_attempt(
        &self,
    ) -> Option<crate::transport_status::OutboundSnapshotAttempt> {
        self.snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|attempt| attempt.transport_attempt)
    }

    fn restore_transfer_after_snapshot_mismatch(
        &self,
        response: &Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ) {
        if matches!(
            &response,
            Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(
                _
            )))
        ) && let Some(attempt) = self
            .snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            // OpenRaft seeks and rereads before issuing the offset-zero retry.
            // Restore the original T boundary while returning the typed error,
            // not on the next RPC, or that source work inherits the longer I
            // deadline. This is also required when the whole image is one
            // final chunk and the retry has no non-final RPC at all.
            attempt.return_to_transfer();
        }
    }

    #[inline(always)]
    async fn send<Err>(
        &mut self,
        req: RaftRequest,
        rx: oneshot::Receiver<Result<RaftStreamResponsePayload, Error>>,
        soft_ttl: Duration,
    ) -> Result<RaftStreamResponsePayload, RPCError<NodeId, Node, Err>>
    where
        Err: std::error::Error + 'static + Clone,
    {
        #[cfg(feature = "validation-test-helpers")]
        if validation_raft_partitioned() {
            let error = std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "validation Raft partition",
            );
            return Err(RPCError::Unreachable(Unreachable::new(&error)));
        }
        tracing::debug!(
            request_kind = req.kind(),
            snapshot_chunk_bytes = ?req.snapshot_chunk_bytes(),
            "sending rpc request to {}",
            self.node.addr_raft
        );

        // OpenRaft enforces `RPCOption::hard_ttl()` by dropping this future.
        // Keep a cancellation guard alive across both enqueue and response so
        // that drop also tears down a half-open WebSocket and lets the next
        // snapshot/append attempt establish a clean stream.
        let mut reset = ConnectionResetGuard::new(Arc::clone(&self.reset));
        let result = tokio::time::timeout(soft_ttl, async {
            self.sender.send_async(req).await.map_err(|err| {
                error!(
                    "NetworkConnectionStreaming::send to node {}: {}",
                    self.node.id,
                    err.to_string()
                );
                RPCError::Unreachable(Unreachable::new(&err))
            })?;

            rx.await
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
        })
        .await;
        match result {
            Ok(result) => {
                reset.disarm();
                result
            }
            Err(error) => Err(RPCError::Unreachable(Unreachable::new(&error))),
        }
    }
}

#[cfg(any(feature = "cache", feature = "sqlite"))]
fn catch_up_outbound_snapshot_connection(network: &NetworkConnectionStreaming) {
    network.snapshot_transport.connecting_owned(
        network.raft_group,
        network.node.id,
        network.transport_connection_id,
        network.reset.socket_epoch(),
        network.reset.connection_attempt_sequence(),
    );
}

#[cfg(any(feature = "cache", feature = "sqlite"))]
async fn bounded_full_snapshot<C>(
    network: &mut NetworkConnectionStreaming,
    vote: Vote<NodeId>,
    snapshot: Snapshot<C>,
    cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    option: RPCOption,
) -> Result<SnapshotResponse<NodeId>, StreamingError<C, Fatal<NodeId>>>
where
    C: openraft::RaftTypeConfig<NodeId = NodeId, Node = Node>,
    C::SnapshotData: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin,
    NetworkConnectionStreaming: RaftNetwork<C>,
{
    let mut transport_attempt = network.snapshot_transport.next_outbound_attempt(
        network.raft_group,
        network.node.id,
        network.transport_connection_id,
    );
    transport_attempt.connection_attempt_sequence = network.reset.connection_attempt_sequence();
    let attempt_id = transport_attempt.attempt_id;
    let attempt = SnapshotAttempt::tracked(
        transport_attempt,
        &snapshot.meta.snapshot_id,
        network.snapshot_budgets,
    );
    network.snapshot_transport.begin_owned_outbound_attempt(
        &transport_attempt,
        &snapshot.meta.snapshot_id,
        crate::transport_status::OutboundSnapshotSocket {
            epoch: network.reset.socket_epoch(),
            connected: network.reset.is_connected(),
        },
        attempt.transfer_deadline,
    );
    catch_up_outbound_snapshot_connection(network);
    {
        let mut slot = network
            .snapshot_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = Some(Arc::clone(&attempt));
    }
    let mut attempt_guard = SnapshotAttemptGuard::new(
        attempt_id,
        Arc::clone(&network.snapshot_attempt),
        network.snapshot_transport.clone(),
        transport_attempt,
    );
    let stage = attempt.stage.subscribe();
    let local_node_id = network.local_node_id;
    let target_node_id = network.node.id;
    let driver = Chunked::send_snapshot(
        network,
        vote,
        snapshot,
        std::future::pending::<ReplicationClosed>(),
        option,
    );

    let result = supervise_snapshot_driver(
        attempt,
        local_node_id,
        target_node_id,
        stage,
        cancel,
        driver,
    )
    .await;
    if result.is_err() {
        network
            .snapshot_transport
            .outbound_failed_owned(&transport_attempt, "snapshot_attempt_ended");
    }
    attempt_guard.disarm();
    result
}

#[cfg(any(feature = "cache", feature = "sqlite"))]
async fn supervise_snapshot_driver<C, Cancel, Driver>(
    attempt: Arc<SnapshotAttempt>,
    local_node_id: NodeId,
    target_node_id: NodeId,
    stage: watch::Receiver<SnapshotStageDeadline>,
    cancel: Cancel,
    driver: Driver,
) -> Result<SnapshotResponse<NodeId>, StreamingError<C, Fatal<NodeId>>>
where
    C: openraft::RaftTypeConfig<NodeId = NodeId, Node = Node>,
    Cancel: Future<Output = ReplicationClosed> + OptionalSend + 'static,
    Driver: Future<Output = Result<SnapshotResponse<NodeId>, StreamingError<C, Fatal<NodeId>>>>,
{
    let attempt_id = attempt.id;
    let snapshot_id = attempt.snapshot_id.clone();
    let mut deadline = std::pin::pin!(wait_for_snapshot_deadline(stage));
    let mut caller_cancel = std::pin::pin!(cancel);
    let mut driver = std::pin::pin!(driver);

    tokio::select! {
        biased;
        closed = &mut caller_cancel => {
            info!(attempt_id, %snapshot_id, target_node_id, "snapshot attempt cancelled by replication owner");
            Err(StreamingError::Closed(closed))
        }
        expired = &mut deadline => {
            error!(attempt_id, %snapshot_id, target_node_id, phase = ?expired.phase,
                "snapshot attempt reached its absolute phase deadline");
            Err(StreamingError::Timeout(Timeout {
                action: RPCTypes::InstallSnapshot,
                id: local_node_id,
                target: target_node_id,
                timeout: expired.deadline.saturating_duration_since(attempt.start),
            }))
        }
        result = &mut driver => result,
    }
}

/// Preserve errors returned by the peer as remote Raft errors.
///
/// In particular, OpenRaft's chunked snapshot transport recognizes a remote
/// `SnapshotMismatch` and restarts the transfer at offset zero. Flattening the
/// peer response into `Unreachable` hides that recovery signal and makes every
/// later retry resume at the rejected nonzero offset.
#[cfg(any(feature = "cache", feature = "sqlite"))]
fn remote_raft_error<Err>(node: &Node, error: Err) -> RPCError<NodeId, Node, Err>
where
    Err: std::error::Error,
{
    RPCError::RemoteError(RemoteError::new_with_node(node.id, node.clone(), error))
}

/// AppendEntries performs the durable follower write Raft is waiting for.
///
/// OpenRaft already drops the network future at `hard_ttl`; cancelling the
/// same RPC at its 3/4 soft deadline turns a response that completes inside
/// the caller's accepted bound into a false outage and resets its stream.
#[cfg(any(feature = "cache", feature = "sqlite"))]
fn append_response_ttl(option: &RPCOption) -> Duration {
    option.hard_ttl()
}

#[cfg(feature = "sqlite")]
impl RaftNetwork<TypeConfigSqlite> for NetworkConnectionStreaming {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfigSqlite>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self
            .send(
                RaftRequest::AppendDB((ack, req)),
                rx,
                append_response_ttl(&option),
            )
            .await?
        {
            RaftStreamResponsePayload::AppendDB(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfigSqlite>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let request_vote = req.vote;
        let done = req.done;
        let acknowledged_offset = req.offset.saturating_add(req.data.len() as u64);
        let rpc_ttl = self
            .snapshot_rpc_ttl(done, &option)
            .ok_or_else(|| self.snapshot_timeout_error())?;
        let final_install_deadline = if done {
            self.snapshot_stage_deadline().map(|stage| stage.deadline)
        } else {
            None
        };
        let transport_attempt = self.snapshot_transport_attempt();
        if let Some(attempt) = transport_attempt.as_ref() {
            self.snapshot_transport.outbound_chunk_owned(
                attempt,
                req.offset,
                req.data.len(),
                done,
                time::Instant::now() + rpc_ttl,
            );
        }
        let (ack, rx) = oneshot::channel();
        let payload = match self
            .send(RaftRequest::SnapshotDB((ack, req)), rx, rpc_ttl)
            .await
        {
            Ok(payload) => payload,
            Err(error) => {
                if let Some(attempt) = transport_attempt.as_ref() {
                    self.snapshot_transport.outbound_retry_owned(
                        attempt,
                        "transport_unavailable",
                        final_install_deadline.or_else(|| self.snapshot_transfer_deadline()),
                    );
                }
                return Err(error);
            }
        };
        match payload {
            RaftStreamResponsePayload::SnapshotDB(resp) => {
                self.restore_transfer_after_snapshot_mismatch(&resp);
                match &resp {
                    Ok(response) if response.vote <= request_vote => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport.outbound_acknowledged_owned(
                                attempt,
                                acknowledged_offset,
                                done,
                                self.snapshot_transfer_deadline(),
                            );
                        }
                    }
                    Ok(_) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport
                                .outbound_failed_owned(attempt, "higher_vote");
                        }
                    }
                    Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(_))) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport.outbound_retry_owned(
                                attempt,
                                "snapshot_mismatch",
                                self.snapshot_transfer_deadline(),
                            );
                        }
                    }
                    Err(_) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport
                                .outbound_failed_owned(attempt, "remote_snapshot_error");
                        }
                    }
                }
                resp.map_err(|error| remote_raft_error(&self.node, error))
            }
            _ => unreachable!(),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfigSqlite>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfigSqlite, Fatal<NodeId>>> {
        bounded_full_snapshot(self, vote, snapshot, cancel, option).await
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self
            .send(RaftRequest::VoteDB((ack, req)), rx, option.soft_ttl())
            .await?
        {
            RaftStreamResponsePayload::VoteDB(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "cache")]
impl RaftNetwork<TypeConfigKV> for NetworkConnectionStreaming {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfigKV>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self
            .send(
                RaftRequest::AppendCache((ack, req)),
                rx,
                append_response_ttl(&option),
            )
            .await?
        {
            RaftStreamResponsePayload::AppendCache(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfigKV>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let request_vote = req.vote;
        let done = req.done;
        let acknowledged_offset = req.offset.saturating_add(req.data.len() as u64);
        let rpc_ttl = self
            .snapshot_rpc_ttl(done, &option)
            .ok_or_else(|| self.snapshot_timeout_error())?;
        let final_install_deadline = if done {
            self.snapshot_stage_deadline().map(|stage| stage.deadline)
        } else {
            None
        };
        let transport_attempt = self.snapshot_transport_attempt();
        if let Some(attempt) = transport_attempt.as_ref() {
            self.snapshot_transport.outbound_chunk_owned(
                attempt,
                req.offset,
                req.data.len(),
                done,
                time::Instant::now() + rpc_ttl,
            );
        }
        let (ack, rx) = oneshot::channel();
        let payload = match self
            .send(RaftRequest::SnapshotCache((ack, req)), rx, rpc_ttl)
            .await
        {
            Ok(payload) => payload,
            Err(error) => {
                if let Some(attempt) = transport_attempt.as_ref() {
                    self.snapshot_transport.outbound_retry_owned(
                        attempt,
                        "transport_unavailable",
                        final_install_deadline.or_else(|| self.snapshot_transfer_deadline()),
                    );
                }
                return Err(error);
            }
        };
        match payload {
            RaftStreamResponsePayload::SnapshotCache(resp) => {
                self.restore_transfer_after_snapshot_mismatch(&resp);
                match &resp {
                    Ok(response) if response.vote <= request_vote => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport.outbound_acknowledged_owned(
                                attempt,
                                acknowledged_offset,
                                done,
                                self.snapshot_transfer_deadline(),
                            );
                        }
                    }
                    Ok(_) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport
                                .outbound_failed_owned(attempt, "higher_vote");
                        }
                    }
                    Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(_))) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport.outbound_retry_owned(
                                attempt,
                                "snapshot_mismatch",
                                self.snapshot_transfer_deadline(),
                            );
                        }
                    }
                    Err(_) => {
                        if let Some(attempt) = transport_attempt.as_ref() {
                            self.snapshot_transport
                                .outbound_failed_owned(attempt, "remote_snapshot_error");
                        }
                    }
                }
                resp.map_err(|error| remote_raft_error(&self.node, error))
            }
            _ => unreachable!(),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfigKV>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfigKV, Fatal<NodeId>>> {
        bounded_full_snapshot(self, vote, snapshot, cancel, option).await
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self
            .send(RaftRequest::VoteCache((ack, req)), rx, option.soft_ttl())
            .await?
        {
            RaftStreamResponsePayload::VoteCache(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "sqlite")]
    use axum::{Router, extract::State, response::IntoResponse, routing::get};
    use fastwebsockets::{Role, WebSocket};
    use openraft::error::{InstallSnapshotError, RaftError, SnapshotMismatch};
    use openraft::{SnapshotMeta, SnapshotSegmentId, Vote};

    use super::*;

    #[cfg(feature = "sqlite")]
    #[derive(Clone)]
    struct ReconnectServerState {
        accepted: flume::Sender<u64>,
        connection_sequence: Arc<AtomicU64>,
        release_first: Arc<Notify>,
        release_second: Arc<Notify>,
    }

    #[cfg(feature = "sqlite")]
    async fn reconnecting_raft_server(
        State(state): State<ReconnectServerState>,
        ws: fastwebsockets::upgrade::IncomingUpgrade,
    ) -> Result<impl IntoResponse, Error> {
        let (response, socket) = ws.upgrade()?;
        tokio::spawn(async move {
            let mut socket = socket.await.expect("upgrade reconnect test WebSocket");
            socket.set_auto_close(true);
            crate::network::handshake::HandshakeSecret::server(&mut socket, b"test-secret")
                .await
                .expect("authenticate reconnect test WebSocket");
            let sequence = state
                .connection_sequence
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1);
            state
                .accepted
                .send_async(sequence)
                .await
                .expect("report authenticated test socket");
            if sequence == 1 {
                state.release_first.notified().await;
                let _ = crate::network::frame_io::write_socket_close_frame_flushed(
                    &mut socket,
                    Frame::close(1000, b"reconnect"),
                )
                .await;
            } else {
                state.release_second.notified().await;
            }
        });
        Ok(response)
    }

    fn test_node() -> Node {
        Node {
            id: 7,
            addr_raft: "127.0.0.1:32401".to_owned(),
            addr_api: "127.0.0.1:32402".to_owned(),
        }
    }

    fn test_snapshot_meta() -> SnapshotMeta<NodeId, Node> {
        SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "snapshot".to_owned(),
        }
    }

    fn mismatch_at(offset: u64) -> RaftError<NodeId, InstallSnapshotError> {
        RaftError::APIError(InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
            expect: SnapshotSegmentId::from(("snapshot", 0)),
            got: SnapshotSegmentId::from(("snapshot", offset)),
        }))
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn raft_transport_debug_logging_exposes_only_bounded_metadata() {
        let (ack, _response) = oneshot::channel();
        let request = RaftRequest::SnapshotDB((
            ack,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 0,
                data: b"private snapshot and SQL parameter bytes".to_vec(),
                done: false,
            },
        ));
        assert_eq!(request.kind(), "snapshot_db");
        assert_eq!(request.snapshot_chunk_bytes(), Some(40));

        let full_request_formatter = ["debug", "(&req)"].concat();
        let obligated_callback = ["read_frame(&mut |frame| async move ", "{"].concat();
        for source in [
            include_str!("raft_client.rs"),
            include_str!("raft_server.rs"),
        ] {
            assert!(
                !source.contains(&full_request_formatter),
                "full Raft requests must never be formatted into logs"
            );
            for callback in source.split(&obligated_callback).skip(1) {
                let obligated_log = callback
                    .split("Ok::<(), Error>(())")
                    .next()
                    .expect("obligated WebSocket frame callback");
                assert_eq!(
                    obligated_log.matches("frame.payload").count(),
                    1,
                    "obligated-frame logs may inspect only one bounded payload field"
                );
                assert!(obligated_log.contains("payload_len = frame.payload.len()"));
            }
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_raft_request_writer_flushes_a_snapshot_chunk_through_tls() {
        let bytes = serialize(&RaftStreamRequest::SnapshotDB((
            9,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 0,
                data: vec![42; 3 * 1024 * 1024],
                done: false,
            },
        )))
        .expect("serialize Raft snapshot request");
        crate::network::frame_io::tests::exercise_gated_tls_writer(
            Role::Client,
            bytes,
            true,
            |mut write, bytes| async move { write_raft_request_frame(&mut write, bytes).await },
        )
        .await;
    }

    #[tokio::test]
    async fn production_raft_writer_reports_flush_failure_to_its_supervisor() {
        let write = crate::network::frame_io::tests::split_writer(
            crate::network::frame_io::tests::TestIo::failing_flush(),
        );
        let (tx, rx) = flume::bounded(1);
        let (finished, outcome) = oneshot::channel();
        tx.send_async(WritePayload::Payload(b"raft request".to_vec()))
            .await
            .expect("queue Raft request");

        NetworkStreaming::stream_writer(write, rx, finished).await;

        let error = outcome
            .await
            .expect("writer terminal outcome")
            .expect_err("flush failure must terminate the writer");
        assert!(error.contains("injected flush failure"));
    }

    #[tokio::test]
    async fn production_raft_reader_reports_malformed_frames() {
        let (client_io, server_io) = tokio::io::duplex(4 * 1024);
        let client = WebSocket::after_handshake(client_io, Role::Client);
        let (read, _write) = client.split(tokio::io::split);
        let read = FragmentCollectorRead::new(read);
        let (tx, _rx) = flume::bounded(1);
        let (finished, result) = oneshot::channel();
        tokio::spawn(async move {
            let _ = finished.send(NetworkStreaming::stream_reader(read, tx).await);
        });
        let mut server = WebSocket::after_handshake(server_io, Role::Server);

        crate::network::frame_io::write_socket_frame_flushed(
            &mut server,
            Frame::binary(Payload::Borrowed(b"not a Raft response")),
        )
        .await
        .expect("send malformed server frame");

        let outcome = tokio::time::timeout(Duration::from_secs(1), result)
            .await
            .expect("reader must report promptly")
            .expect("reader task must report an outcome");
        assert!(matches!(outcome, Err(ref err) if err.contains("invalid Raft stream response")));
    }

    #[tokio::test]
    async fn dropping_connection_off_runtime_keeps_cleanup_owned() {
        let (sender, receiver) = flume::bounded(1);
        let release = Arc::new(Notify::new());
        let stopped = Arc::new(Notify::new());
        let task = tokio::spawn({
            let release = Arc::clone(&release);
            let stopped = Arc::clone(&stopped);
            async move {
                release.notified().await;
                assert!(matches!(
                    receiver.recv_async().await,
                    Ok(RaftRequest::Shutdown)
                ));
                stopped.notify_one();
            }
        });
        let connection = NetworkConnectionStreaming {
            node: Node {
                id: 2,
                addr_raft: "127.0.0.1:32401".to_owned(),
                addr_api: "127.0.0.1:32402".to_owned(),
            },
            sender,
            reset: Arc::new(ConnectionResetState::default()),
            shutdown: Arc::new(ConnectionShutdownState::default()),
            local_node_id: 1,
            raft_group: "sqlite",
            snapshot_budgets: SnapshotRpcBudgets {
                chunk: Duration::from_secs(30),
                transfer: Duration::from_secs(1_200),
                install: Duration::from_secs(120),
            },
            snapshot_transport: crate::LocalSnapshotTransportStatus::new(
                1,
                std::collections::BTreeSet::from([2]),
            ),
            snapshot_attempt: Arc::new(StdMutex::new(None)),
            transport_connection_id: 1,
            runtime: tokio::runtime::Handle::current(),
            task: Some(task),
        };

        std::thread::spawn(move || drop(connection))
            .join()
            .expect("drop connection off runtime");
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(1), stopped.notified())
            .await
            .expect("the captured runtime must reap the handler after off-runtime drop");
    }

    #[tokio::test]
    async fn repeated_connection_failures_return_supervised_tasks_to_baseline() {
        struct ActiveTask(Arc<AtomicU64>);

        impl Drop for ActiveTask {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let active = Arc::new(AtomicU64::new(0));
        for _ in 0..100 {
            let (client_io, server_io) = tokio::io::duplex(4 * 1024);
            let client = WebSocket::after_handshake(client_io, Role::Client);
            let (read, write) = client.split(tokio::io::split);
            let read = FragmentCollectorRead::new(read);
            let (tx_read, rx_read) = flume::bounded(1);
            let (_request_sender, request_receiver) = flume::bounded(1);
            let (reader_finished, mut reader_result) = oneshot::channel();
            let (writer_finished, mut writer_result) = oneshot::channel();
            let (tx_write, rx_write) = flume::bounded(1);

            let active_reader = Arc::clone(&active);
            let handle_read = tokio::spawn(async move {
                active_reader.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveTask(active_reader);
                let outcome = NetworkStreaming::stream_reader(read, tx_read).await;
                let _ = reader_finished.send(outcome);
            });
            let active_writer = Arc::clone(&active);
            let handle_write = tokio::spawn(async move {
                active_writer.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveTask(active_writer);
                NetworkStreaming::stream_writer(write, rx_write, writer_finished).await;
            });
            let peer = tokio::spawn(async move {
                let mut server = WebSocket::after_handshake(server_io, Role::Server);
                crate::network::frame_io::write_socket_frame_flushed(
                    &mut server,
                    Frame::binary(Payload::Borrowed(b"not a Raft response")),
                )
                .await
                .expect("inject malformed peer response");
            });

            let reset = ConnectionResetState::default();
            let shutdown = ConnectionShutdownState::default();
            let event = tokio::time::timeout(
                Duration::from_secs(1),
                next_connected_event(
                    &reset,
                    reset.epoch(),
                    &rx_read,
                    &request_receiver,
                    &shutdown,
                    &mut reader_result,
                    &mut writer_result,
                ),
            )
            .await
            .expect("real reader failure must wake the supervisor");
            assert!(matches!(
                event,
                ConnectedEvent::ReaderFinished(Err(ref error))
                    if error.contains("invalid Raft stream response")
            ));

            stop_stream_tasks(&tx_write, handle_write, handle_read, true).await;
            peer.await.expect("peer task must quiesce");
            assert_eq!(active.load(Ordering::SeqCst), 0);
        }

        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_connection_supervisor_does_not_invent_snapshot_work() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind handshake observer");
        let address = listener.local_addr().expect("read endpoint");
        let transport =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([2]));
        let mut factory = NetworkStreaming {
            node_id: 1,
            tls_config: None,
            secret_raft: b"test-secret".to_vec(),
            raft_type: RaftType::Sqlite,
            heartbeat_interval: 1,
            is_raft_stopped: Arc::new(AtomicBool::new(false)),
            is_startup_finished: Arc::new(AtomicBool::new(true)),
            snapshot_budgets: test_snapshot_budgets(),
            snapshot_transport: transport.clone(),
        };
        let node = Node {
            id: 2,
            addr_raft: address.to_string(),
            addr_api: "127.0.0.1:1".to_owned(),
        };
        let connection = <NetworkStreaming as RaftNetworkFactory<TypeConfigSqlite>>::new_client(
            &mut factory,
            node.id,
            &node,
        )
        .await;
        let (attempted_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("production supervisor must attempt the observed connection")
            .expect("accept production connection attempt");
        assert!(transport.snapshot().observations.is_empty());
        drop(attempted_socket);
        drop(connection);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_supervisor_start_before_snapshot_attempt_counts_first_reconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind handshake observer");
        let address = listener.local_addr().expect("read endpoint");
        let transport =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([2]));
        let mut factory = NetworkStreaming {
            node_id: 1,
            tls_config: None,
            secret_raft: b"test-secret".to_vec(),
            raft_type: RaftType::Sqlite,
            heartbeat_interval: 0,
            is_raft_stopped: Arc::new(AtomicBool::new(false)),
            is_startup_finished: Arc::new(AtomicBool::new(true)),
            snapshot_budgets: test_snapshot_budgets(),
            snapshot_transport: transport.clone(),
        };
        let node = Node {
            id: 2,
            addr_raft: address.to_string(),
            addr_api: "127.0.0.1:1".to_owned(),
        };
        let connection = <NetworkStreaming as RaftNetworkFactory<TypeConfigSqlite>>::new_client(
            &mut factory,
            node.id,
            &node,
        )
        .await;
        let (first_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("supervisor must start before the snapshot attempt")
            .expect("accept initial connection attempt");
        assert_eq!(connection.reset.connection_attempt_sequence(), 1);

        let mut attempt =
            transport.next_outbound_attempt("sqlite", node.id, connection.transport_connection_id);
        attempt.connection_attempt_sequence = connection.reset.connection_attempt_sequence();
        let deadline = time::Instant::now() + Duration::from_secs(120);
        transport.begin_owned_outbound_attempt(
            &attempt,
            "race-snapshot",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: connection.reset.socket_epoch(),
                connected: connection.reset.is_connected(),
            },
            deadline,
        );
        assert_eq!(transport.snapshot().observations[0].reconnect_count, 0);

        drop(first_socket);
        let (second_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("failed initial handshake must start the first reconnect")
            .expect("accept first reconnect");
        let observation = &transport.snapshot().observations[0];
        assert_eq!(connection.reset.connection_attempt_sequence(), 2);
        assert_eq!(observation.reconnect_count, 1);
        assert_eq!(observation.snapshot_id.as_deref(), Some("race-snapshot"));
        assert!(observation.operation_owns_work);

        drop(second_socket);
        drop(connection);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn production_snapshot_catch_up_preserves_physical_socket_epoch_during_interleaving() {
        let (network, _receiver) = test_network(test_snapshot_budgets());
        let reset = Arc::clone(&network.reset);
        let physical_socket_epoch = reset.begin_socket();
        let first_connection_attempt = reset.begin_connection_attempt();
        let mut transport_attempt = network.snapshot_transport.next_outbound_attempt(
            network.raft_group,
            network.node.id,
            network.transport_connection_id,
        );
        transport_attempt.connection_attempt_sequence = first_connection_attempt;
        network.snapshot_transport.begin_owned_outbound_attempt(
            &transport_attempt,
            "catch-up-interleaving",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: physical_socket_epoch,
                connected: false,
            },
            time::Instant::now() + Duration::from_secs(120),
        );

        // Model reset churn plus a replacement connection attempt landing
        // between snapshot registration and its production catch-up call.
        // Logical reset epochs and physical socket epochs intentionally diverge.
        for _ in 0..3 {
            reset.request_reset(reset.epoch());
        }
        let replacement_connection_attempt = reset.begin_connection_attempt();
        assert_ne!(reset.epoch(), physical_socket_epoch);
        catch_up_outbound_snapshot_connection(&network);

        let caught_up = network.snapshot_transport.snapshot().observations.remove(0);
        assert_eq!(caught_up.socket_epoch, physical_socket_epoch);
        assert_eq!(caught_up.reconnect_count, 1);

        let replacement_socket_epoch = reset.begin_socket();
        network.snapshot_transport.connected_owned(
            network.raft_group,
            network.node.id,
            network.transport_connection_id,
            replacement_socket_epoch,
            replacement_connection_attempt,
        );
        assert_eq!(
            network.snapshot_transport.snapshot().observations[0].socket_epoch,
            replacement_socket_epoch,
            "catch-up must not poison monotonic physical socket publication"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_natural_reconnect_advances_physical_socket_epoch_without_reset() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authenticated reconnect server");
        let address = listener.local_addr().expect("read reconnect endpoint");
        let (accepted, accepted_rx) = flume::bounded(2);
        let release_first = Arc::new(Notify::new());
        let release_second = Arc::new(Notify::new());
        let app = Router::new()
            .route("/stream/sqlite", get(reconnecting_raft_server))
            .with_state(ReconnectServerState {
                accepted,
                connection_sequence: Arc::new(AtomicU64::new(0)),
                release_first: Arc::clone(&release_first),
                release_second: Arc::clone(&release_second),
            });
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve authenticated reconnect route");
        });

        let transport =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([2]));
        let mut factory = NetworkStreaming {
            node_id: 1,
            tls_config: None,
            secret_raft: b"test-secret".to_vec(),
            raft_type: RaftType::Sqlite,
            heartbeat_interval: 0,
            is_raft_stopped: Arc::new(AtomicBool::new(false)),
            is_startup_finished: Arc::new(AtomicBool::new(true)),
            snapshot_budgets: test_snapshot_budgets(),
            snapshot_transport: transport.clone(),
        };
        let node = Node {
            id: 2,
            addr_raft: address.to_string(),
            addr_api: "127.0.0.1:1".to_owned(),
        };
        let connection = <NetworkStreaming as RaftNetworkFactory<TypeConfigSqlite>>::new_client(
            &mut factory,
            node.id,
            &node,
        )
        .await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), accepted_rx.recv_async())
                .await
                .expect("first socket must authenticate")
                .expect("first socket report"),
            1
        );

        let reset_epoch = connection.reset.epoch();
        let first_socket_epoch = connection.reset.socket_epoch();
        let mut attempt =
            transport.next_outbound_attempt("sqlite", node.id, connection.transport_connection_id);
        attempt.connection_attempt_sequence = connection.reset.connection_attempt_sequence();
        transport.begin_owned_outbound_attempt(
            &attempt,
            "natural-reconnect",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: first_socket_epoch,
                connected: connection.reset.is_connected(),
            },
            time::Instant::now() + Duration::from_secs(120),
        );

        release_first.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), accepted_rx.recv_async())
                .await
                .expect("natural disconnect must reconnect")
                .expect("replacement socket report"),
            2
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while connection.reset.socket_epoch() == first_socket_epoch {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("client must publish replacement physical socket epoch");

        let observation = transport.snapshot().observations.remove(0);
        assert!(observation.socket_epoch > first_socket_epoch);
        assert_eq!(observation.socket_epoch, connection.reset.socket_epoch());
        assert_eq!(observation.reconnect_count, 1);
        assert_eq!(
            connection.reset.epoch(),
            reset_epoch,
            "natural reconnect must not mutate cancellation/reset state"
        );

        drop(connection);
        release_second.notify_one();
        server.abort();
        let _ = server.await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn two_client_churn_cannot_overwrite_the_snapshot_connection_owner() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind two-client handshake observer");
        let address = listener.local_addr().expect("read endpoint");
        let transport =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([2]));
        let mut factory = NetworkStreaming {
            node_id: 1,
            tls_config: None,
            secret_raft: b"test-secret".to_vec(),
            raft_type: RaftType::Sqlite,
            heartbeat_interval: 1,
            is_raft_stopped: Arc::new(AtomicBool::new(false)),
            is_startup_finished: Arc::new(AtomicBool::new(true)),
            snapshot_budgets: test_snapshot_budgets(),
            snapshot_transport: transport.clone(),
        };
        let node = Node {
            id: 2,
            addr_raft: address.to_string(),
            addr_api: "127.0.0.1:1".to_owned(),
        };
        let snapshot_client =
            <NetworkStreaming as RaftNetworkFactory<TypeConfigSqlite>>::new_client(
                &mut factory,
                node.id,
                &node,
            )
            .await;
        let (snapshot_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("snapshot client must attempt a connection")
            .expect("accept snapshot client connection");

        let attempt = transport.next_outbound_attempt(
            "sqlite",
            node.id,
            snapshot_client.transport_connection_id,
        );
        let deadline = time::Instant::now() + Duration::from_secs(120);
        transport.begin_owned_outbound_attempt(
            &attempt,
            "owned-snapshot",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: snapshot_client.reset.socket_epoch(),
                connected: snapshot_client.reset.is_connected(),
            },
            deadline,
        );
        transport.outbound_chunk_owned(&attempt, 0, 64, true, deadline);

        let ordinary_client =
            <NetworkStreaming as RaftNetworkFactory<TypeConfigSqlite>>::new_client(
                &mut factory,
                node.id,
                &node,
            )
            .await;
        assert_ne!(
            snapshot_client.transport_connection_id,
            ordinary_client.transport_connection_id
        );
        let (ordinary_socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("ordinary client must attempt a connection")
            .expect("accept ordinary client connection");

        let observation = transport.snapshot().observations.remove(0);
        assert_eq!(observation.attempt_id, attempt.attempt_id);
        assert_eq!(observation.snapshot_id.as_deref(), Some("owned-snapshot"));
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Installing);
        assert_eq!(observation.reconnect_count, 0);
        assert!(observation.operation_owns_work);

        drop(ordinary_socket);
        drop(snapshot_socket);
        drop(ordinary_client);
        drop(snapshot_client);
    }

    #[tokio::test]
    async fn handler_coordinator_consumes_retained_reset_when_request_queue_is_full() {
        let (sender, receiver) = flume::bounded(1);
        let (_reader_sender, reader_receiver) = flume::bounded(1);
        sender
            .try_send(RaftRequest::Shutdown)
            .expect("fill the bounded request queue");
        let reset = Arc::new(ConnectionResetState::default());
        let socket_epoch = reset.epoch();
        let guard = ConnectionResetGuard::new(Arc::clone(&reset));
        let shutdown = ConnectionShutdownState::default();
        let (_reader_finished, mut reader_finished) = oneshot::channel();
        let (_writer_finished, mut writer_finished) = oneshot::channel();

        drop(guard);

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            next_connected_event(
                &reset,
                socket_epoch,
                &reader_receiver,
                &receiver,
                &shutdown,
                &mut reader_finished,
                &mut writer_finished,
            ),
        )
        .await
        .expect("the handler's reconnect consumer must observe the retained reset");
        assert!(matches!(event, ConnectedEvent::Reset));
        assert!(matches!(
            receiver.recv_async().await,
            Ok(RaftRequest::Shutdown)
        ));
    }

    #[tokio::test]
    async fn handler_coordinator_observes_writer_failure_while_reader_is_pending() {
        let (_request_sender, request_receiver) = flume::bounded(1);
        let (_reader_sender, reader_receiver) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (_reader_finished, mut reader_result) = oneshot::channel();
        let (writer_finished, mut writer_result) = oneshot::channel();

        writer_finished
            .send(Err("injected flush failure".to_owned()))
            .expect("writer outcome receiver must remain open");

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            next_connected_event(
                &reset,
                socket_epoch,
                &reader_receiver,
                &request_receiver,
                &shutdown,
                &mut reader_result,
                &mut writer_result,
            ),
        )
        .await
        .expect("writer failure must wake the connection supervisor");
        assert!(matches!(
            event,
            ConnectedEvent::WriterFinished(Err(ref err)) if err == "injected flush failure"
        ));
    }

    #[tokio::test]
    async fn handler_coordinator_observes_reader_failure_while_writer_is_pending() {
        let (_request_sender, request_receiver) = flume::bounded(1);
        let (_reader_sender, reader_receiver) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (reader_finished, mut reader_result) = oneshot::channel();
        let (_writer_finished, mut writer_result) = oneshot::channel();

        reader_finished
            .send(Err("invalid Raft response".to_owned()))
            .expect("reader outcome receiver must remain open");

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            next_connected_event(
                &reset,
                socket_epoch,
                &reader_receiver,
                &request_receiver,
                &shutdown,
                &mut reader_result,
                &mut writer_result,
            ),
        )
        .await
        .expect("reader failure must wake the connection supervisor");
        assert!(matches!(
            event,
            ConnectedEvent::ReaderFinished(Err(ref err)) if err == "invalid Raft response"
        ));
    }

    #[tokio::test]
    async fn handler_coordinator_treats_reader_panic_as_terminal() {
        let (_request_sender, request_receiver) = flume::bounded(1);
        let (_reader_sender, reader_receiver) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (reader_finished, mut reader_result) = oneshot::channel::<Result<(), String>>();
        let (_writer_finished, mut writer_result) = oneshot::channel();
        drop(reader_finished);

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            next_connected_event(
                &reset,
                socket_epoch,
                &reader_receiver,
                &request_receiver,
                &shutdown,
                &mut reader_result,
                &mut writer_result,
            ),
        )
        .await
        .expect("dropped reader outcome must wake the connection supervisor");
        assert!(matches!(
            event,
            ConnectedEvent::ReaderFinished(Err(ref err))
                if err.contains("without reporting an outcome")
        ));
    }

    #[tokio::test]
    async fn handler_coordinator_treats_writer_panic_as_terminal() {
        let (_request_sender, request_receiver) = flume::bounded(1);
        let (_reader_sender, reader_receiver) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (_reader_finished, mut reader_result) = oneshot::channel();
        let (writer_finished, mut writer_result) = oneshot::channel::<Result<(), String>>();
        drop(writer_finished);

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            next_connected_event(
                &reset,
                socket_epoch,
                &reader_receiver,
                &request_receiver,
                &shutdown,
                &mut reader_result,
                &mut writer_result,
            ),
        )
        .await
        .expect("dropped writer outcome must wake the connection supervisor");
        assert!(matches!(
            event,
            ConnectedEvent::WriterFinished(Err(ref err))
                if err.contains("without reporting an outcome")
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn replacement_socket_drops_cancelled_request_after_consuming_reset() {
        let reset = Arc::new(ConnectionResetState::default());
        let stale_socket_epoch = reset.epoch();
        let (cancelled_ack, cancelled_rx) = oneshot::channel();
        let cancelled = RaftRequest::SnapshotDB((
            cancelled_ack,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 3,
                data: b"old".to_vec(),
                done: false,
            },
        ));
        drop(cancelled_rx);

        let guard = ConnectionResetGuard::new(Arc::clone(&reset));
        drop(guard);
        reset.changed_since(stale_socket_epoch).await;

        let replacement_socket_epoch = reset.epoch();
        assert_eq!(
            cancelled.outbound_disposition(replacement_socket_epoch, reset.epoch()),
            Some(OutboundDisposition::DropCancelled)
        );

        let (live_ack, _live_rx) = oneshot::channel();
        let live = RaftRequest::SnapshotDB((
            live_ack,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 0,
                data: b"new".to_vec(),
                done: false,
            },
        ));
        assert_eq!(
            live.outbound_disposition(replacement_socket_epoch, reset.epoch()),
            Some(OutboundDisposition::Send)
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn live_request_on_stale_socket_requires_reconnect() {
        let reset = ConnectionResetState::default();
        let socket_epoch = reset.epoch();
        reset.request_reset(socket_epoch);
        let (ack, _rx) = oneshot::channel();
        let request = RaftRequest::SnapshotDB((
            ack,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 0,
                data: Vec::new(),
                done: true,
            },
        ));

        assert_eq!(
            request.outbound_disposition(socket_epoch, reset.epoch()),
            Some(OutboundDisposition::Reconnect)
        );
    }

    #[tokio::test]
    async fn reset_interrupts_write_enqueue_under_backpressure() {
        let (tx_write, _rx_write) = flume::bounded(1);
        tx_write
            .try_send(WritePayload::Payload(b"blocked".to_vec()))
            .expect("fill writer queue");
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (_reader_finished, mut reader_result) = oneshot::channel();
        let (_writer_finished, mut writer_result) = oneshot::channel();
        let enqueue = enqueue_write_or_reset(
            &tx_write,
            WritePayload::Payload(b"waiting".to_vec()),
            &reset,
            socket_epoch,
            &shutdown,
            &mut reader_result,
            &mut writer_result,
        );
        tokio::pin!(enqueue);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(enqueue.as_mut(), &mut context).is_pending(),
            "the write enqueue must be blocked before reset"
        );
        reset.request_reset(socket_epoch);

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), &mut enqueue).await,
            Ok(Err(WriteEnqueueError::Reset))
        ));
    }

    #[tokio::test]
    async fn latched_reset_prevents_write_queue_ownership_transfer() {
        let (tx_write, rx_write) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        reset.request_reset(socket_epoch);
        let (_reader_finished, mut reader_result) = oneshot::channel();
        let (_writer_finished, mut writer_result) = oneshot::channel();

        let result = enqueue_write_or_reset(
            &tx_write,
            WritePayload::Payload(b"stale".to_vec()),
            &reset,
            socket_epoch,
            &shutdown,
            &mut reader_result,
            &mut writer_result,
        )
        .await;

        assert!(matches!(result, Err(WriteEnqueueError::Reset)));
        assert!(rx_write.is_empty());
    }

    #[tokio::test]
    async fn latched_reader_failure_prevents_write_queue_ownership_transfer() {
        let (tx_write, rx_write) = flume::bounded(1);
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (reader_finished, mut reader_result) = oneshot::channel();
        let (_writer_finished, mut writer_result) = oneshot::channel();
        reader_finished
            .send(Err("reader stopped".into()))
            .expect("retain reader result receiver");

        let result = enqueue_write_or_reset(
            &tx_write,
            WritePayload::Payload(b"stale".to_vec()),
            &reset,
            socket_epoch,
            &shutdown,
            &mut reader_result,
            &mut writer_result,
        )
        .await;

        assert!(matches!(
            result,
            Err(WriteEnqueueError::ReaderFinished(Err(ref error)))
                if error == "reader stopped"
        ));
        assert!(rx_write.is_empty());
    }

    #[tokio::test]
    async fn shutdown_interrupts_write_enqueue_under_backpressure() {
        let (tx_write, _rx_write) = flume::bounded(1);
        tx_write
            .try_send(WritePayload::Payload(b"blocked".to_vec()))
            .expect("fill writer queue");
        let reset = ConnectionResetState::default();
        let shutdown = ConnectionShutdownState::default();
        let socket_epoch = reset.epoch();
        let (_reader_finished, mut reader_result) = oneshot::channel();
        let (_writer_finished, mut writer_result) = oneshot::channel();
        let enqueue = enqueue_write_or_reset(
            &tx_write,
            WritePayload::Payload(b"waiting".to_vec()),
            &reset,
            socket_epoch,
            &shutdown,
            &mut reader_result,
            &mut writer_result,
        );
        tokio::pin!(enqueue);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(enqueue.as_mut(), &mut context).is_pending(),
            "the write enqueue must be blocked before shutdown"
        );
        shutdown.request();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), &mut enqueue).await,
            Ok(Err(WriteEnqueueError::Shutdown))
        ));
    }

    #[tokio::test]
    async fn forced_reset_cleanup_does_not_wait_for_full_writer_queue() {
        let (tx_write, _rx_write) = flume::bounded(1);
        tx_write
            .try_send(WritePayload::Payload(b"blocked".to_vec()))
            .expect("fill writer queue");
        let handle_write = tokio::spawn(std::future::pending());
        let handle_read = tokio::spawn(std::future::pending());

        tokio::time::timeout(
            Duration::from_secs(1),
            stop_stream_tasks(&tx_write, handle_write, handle_read, true),
        )
        .await
        .expect("forced reset cleanup must not queue behind the writer");
    }

    #[test]
    fn append_response_uses_the_callers_whole_accepted_deadline() {
        let option = RPCOption::new(Duration::from_millis(800));

        assert_eq!(option.soft_ttl(), Duration::from_millis(600));
        assert_eq!(append_response_ttl(&option), Duration::from_millis(800));
    }

    #[test]
    fn outbound_snapshot_attempt_bounds_identity_before_supervisor_logging() {
        let oversized = "outbound-snapshot-metadata".repeat(64);
        let expected = crate::transport_status::retained_snapshot_id(&oversized);
        let attempt = SnapshotAttempt::new(1, oversized.clone(), test_snapshot_budgets());

        assert_eq!(attempt.snapshot_id, expected);
        assert_ne!(attempt.snapshot_id, oversized);
    }

    fn test_snapshot_budgets() -> SnapshotRpcBudgets {
        SnapshotRpcBudgets {
            chunk: Duration::from_secs(30),
            transfer: Duration::from_secs(120),
            install: Duration::from_secs(60),
        }
    }

    fn test_network(
        budgets: SnapshotRpcBudgets,
    ) -> (NetworkConnectionStreaming, flume::Receiver<RaftRequest>) {
        let (sender, receiver) = flume::bounded(1);
        (
            NetworkConnectionStreaming {
                node: test_node(),
                sender,
                reset: Arc::new(ConnectionResetState::default()),
                shutdown: Arc::new(ConnectionShutdownState::default()),
                local_node_id: 1,
                raft_group: "sqlite",
                snapshot_budgets: budgets,
                snapshot_transport: crate::LocalSnapshotTransportStatus::new(
                    1,
                    std::collections::BTreeSet::from([2]),
                ),
                snapshot_attempt: Arc::new(StdMutex::new(None)),
                transport_connection_id: 1,
                runtime: tokio::runtime::Handle::current(),
                task: None,
            },
            receiver,
        )
    }

    fn test_snapshot_file() -> tokio::fs::File {
        tokio::fs::File::from_std(
            std::fs::File::open(std::env::current_exe().expect("test executable path"))
                .expect("open a stable test snapshot source"),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_chunk_deadlines_advance_without_renewing_the_transfer_window() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(1, "snapshot".into(), budgets);
        let start = attempt.start;

        assert_eq!(
            attempt.rpc_deadline(false, Duration::from_secs(300), budgets),
            Some(start + Duration::from_secs(30))
        );
        time::advance(Duration::from_secs(25)).await;
        assert_eq!(
            attempt.rpc_deadline(false, Duration::from_secs(300), budgets),
            Some(start + Duration::from_secs(55))
        );
        time::advance(Duration::from_secs(55)).await;
        assert_eq!(
            attempt.rpc_deadline(false, Duration::from_secs(300), budgets),
            Some(start + Duration::from_secs(110))
        );
        time::advance(Duration::from_secs(15)).await;
        assert_eq!(
            attempt.rpc_deadline(false, Duration::from_secs(300), budgets),
            Some(start + Duration::from_secs(120)),
            "the last chunk receives only the transfer time that remains"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn non_final_snapshot_rpc_uses_hard_ttl_when_it_is_shorter_than_chunk_budget() {
        let budgets = test_snapshot_budgets();
        let (mut network, _receiver) = test_network(budgets);
        let reset = Arc::clone(&network.reset);
        let socket_epoch = reset.epoch();
        network.snapshot_attempt = Arc::new(StdMutex::new(Some(SnapshotAttempt::new(
            11,
            "snapshot".into(),
            budgets,
        ))));

        let rpc = tokio::spawn(async move {
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"chunk".to_vec(),
                    done: false,
                },
                RPCOption::new(Duration::from_secs(10)),
            )
            .await
        });
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;
        assert!(!rpc.is_finished());
        time::advance(Duration::from_secs(1)).await;

        assert!(matches!(
            rpc.await.expect("join hard-capped chunk RPC"),
            Err(RPCError::Unreachable(_))
        ));
        assert_ne!(reset.epoch(), socket_epoch);
    }

    #[tokio::test(start_paused = true)]
    async fn final_install_deadline_latches_once_and_mismatch_restores_transfer_deadline() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(2, "snapshot".into(), budgets);
        let start = attempt.start;
        let mut stage = attempt.stage.subscribe();
        time::advance(Duration::from_secs(100)).await;

        let final_deadline = attempt
            .rpc_deadline(true, Duration::from_secs(300), budgets)
            .expect("final stage begins before transfer expiry");
        assert_eq!(final_deadline, start + Duration::from_secs(160));
        time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            attempt.rpc_deadline(true, Duration::from_secs(300), budgets),
            Some(final_deadline),
            "a final retry must not renew the install window"
        );

        assert_eq!(
            attempt.rpc_deadline(false, Duration::from_secs(300), budgets),
            Some(start + Duration::from_secs(120)),
            "a mismatch returning to offset zero keeps the original transfer boundary"
        );
        assert_eq!(
            *stage.borrow_and_update(),
            SnapshotStageDeadline {
                phase: SnapshotAttemptPhase::Transfer,
                deadline: start + Duration::from_secs(120),
            }
        );
        assert_eq!(
            attempt.rpc_deadline(true, Duration::from_secs(300), budgets),
            Some(final_deadline),
            "re-entering final installation must reuse the first deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn final_install_respects_the_first_rpc_hard_cap_and_transfer_expiry() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(3, "snapshot".into(), budgets);
        let start = attempt.start;
        time::advance(Duration::from_secs(80)).await;
        let hard_capped = attempt
            .rpc_deadline(true, Duration::from_secs(40), budgets)
            .expect("final stage begins with transfer time remaining");
        assert_eq!(hard_capped, start + Duration::from_secs(120));
        time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            attempt.rpc_deadline(true, Duration::from_secs(300), budgets),
            Some(hard_capped),
            "a retry cannot replace the first RPC's hard cap"
        );

        let expired = SnapshotAttempt::new(4, "expired".into(), budgets);
        time::advance(Duration::from_secs(120)).await;
        assert_eq!(
            expired.rpc_deadline(true, Duration::from_secs(300), budgets),
            None,
            "final installation cannot begin after transfer expiry"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mismatch_cannot_reenter_final_install_after_transfer_expiry() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(5, "snapshot".into(), budgets);
        time::advance(Duration::from_secs(100)).await;
        attempt
            .rpc_deadline(true, Duration::from_secs(300), budgets)
            .expect("begin final install before transfer expiry");
        attempt
            .rpc_deadline(false, Duration::from_secs(300), budgets)
            .expect("mismatch returns to the original transfer window");
        time::advance(Duration::from_secs(20)).await;

        assert_eq!(
            attempt.rpc_deadline(true, Duration::from_secs(300), budgets),
            None,
            "a final retry cannot outrun the restored transfer deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn watchable_snapshot_deadline_switches_to_the_active_phase() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(5, "snapshot".into(), budgets);
        let start = attempt.start;
        let stage = attempt.stage.subscribe();
        time::advance(Duration::from_secs(100)).await;
        attempt
            .rpc_deadline(true, Duration::from_secs(300), budgets)
            .expect("begin final install");
        attempt
            .rpc_deadline(false, Duration::from_secs(300), budgets)
            .expect("return to transfer after mismatch");

        let deadline = tokio::spawn(wait_for_snapshot_deadline(stage));
        time::advance(Duration::from_secs(19)).await;
        tokio::task::yield_now().await;
        assert!(!deadline.is_finished());
        time::advance(Duration::from_secs(1)).await;
        let expired = deadline.await.expect("join deadline watcher");
        assert_eq!(expired.phase, SnapshotAttemptPhase::Transfer);
        assert_eq!(expired.deadline, start + Duration::from_secs(120));
    }

    #[tokio::test(start_paused = true)]
    async fn simultaneous_final_phase_update_wins_over_stale_transfer_timer() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(12, "snapshot".into(), budgets);
        let start = attempt.start;
        let watcher = tokio::spawn(wait_for_snapshot_deadline(attempt.stage.subscribe()));
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(119)).await;
        let final_deadline = attempt
            .rpc_deadline(true, Duration::from_secs(300), budgets)
            .expect("dispatch final RPC before transfer expiry");
        assert_eq!(final_deadline, start + Duration::from_secs(179));

        // Do not yield between the watch update and T: both the old sleep and
        // stage change are ready when the watcher resumes.
        time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            !watcher.is_finished(),
            "the pending final-stage update must supersede the stale T timer"
        );
        time::advance(Duration::from_secs(59)).await;
        let expired = watcher.await.expect("join deadline watcher");
        assert_eq!(
            expired,
            SnapshotStageDeadline {
                phase: SnapshotAttemptPhase::FinalInstall,
                deadline: final_deadline,
            }
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn production_final_mismatch_restores_transfer_deadline_before_reread() {
        let budgets = test_snapshot_budgets();
        let (mut network, receiver) = test_network(budgets);
        let transport = network.snapshot_transport.clone();
        let transport_attempt = transport.next_outbound_attempt(
            "sqlite",
            network.node.id,
            network.transport_connection_id,
        );
        let attempt = SnapshotAttempt::tracked(transport_attempt, "snapshot", budgets);
        let start = attempt.start;
        transport.begin_owned_outbound_attempt(
            &transport_attempt,
            "snapshot",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: network.reset.socket_epoch(),
                connected: network.reset.is_connected(),
            },
            attempt.transfer_deadline,
        );
        network.snapshot_attempt = Arc::new(StdMutex::new(Some(Arc::clone(&attempt))));
        time::advance(Duration::from_secs(100)).await;

        let responder = tokio::spawn(async move {
            let (ack, request) = match receiver
                .recv_async()
                .await
                .expect("receive final snapshot request")
            {
                RaftRequest::SnapshotDB(request) => request,
                request => panic!("unexpected SQLite Raft request: {}", request.kind()),
            };
            assert!(request.done, "one-chunk retry remains a final RPC");
            ack.send(Ok(RaftStreamResponsePayload::SnapshotDB(Err(mismatch_at(
                request.offset,
            )))))
            .expect("return typed final mismatch");
        });

        let result =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 4,
                    data: b"only-final-chunk".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_secs(300)),
            )
            .await;
        responder.await.expect("join mismatch responder");
        assert!(matches!(
            result,
            Err(RPCError::RemoteError(RemoteError {
                source: RaftError::APIError(InstallSnapshotError::SnapshotMismatch(_)),
                ..
            }))
        ));
        assert_eq!(
            *attempt.stage.borrow(),
            SnapshotStageDeadline {
                phase: SnapshotAttemptPhase::Transfer,
                deadline: start + Duration::from_secs(120),
            }
        );
        let observation = transport.snapshot().observations.remove(0);
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(observation.active_deadline_remaining_ms, Some(20_000));
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("snapshot_mismatch")
        );

        let reread = tokio::spawn(supervise_snapshot_driver::<TypeConfigSqlite, _, _>(
            Arc::clone(&attempt),
            1,
            7,
            attempt.stage.subscribe(),
            std::future::pending::<ReplicationClosed>(),
            std::future::pending(),
        ));
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(19)).await;
        tokio::task::yield_now().await;
        assert!(!reread.is_finished());
        time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            reread.await.expect("join stalled reread"),
            Err(StreamingError::Timeout(Timeout { timeout, .. }))
                if timeout == Duration::from_secs(120)
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn production_final_transport_error_retains_final_install_deadline_in_status() {
        let budgets = test_snapshot_budgets();
        let (mut network, receiver) = test_network(budgets);
        let transport = network.snapshot_transport.clone();
        let transport_attempt = transport.next_outbound_attempt(
            "sqlite",
            network.node.id,
            network.transport_connection_id,
        );
        let attempt = SnapshotAttempt::tracked(transport_attempt, "final-transport-error", budgets);
        let start = attempt.start;
        transport.begin_owned_outbound_attempt(
            &transport_attempt,
            &attempt.snapshot_id,
            crate::transport_status::OutboundSnapshotSocket {
                epoch: network.reset.socket_epoch(),
                connected: network.reset.is_connected(),
            },
            attempt.transfer_deadline,
        );
        network.snapshot_attempt = Arc::new(StdMutex::new(Some(Arc::clone(&attempt))));
        time::advance(Duration::from_secs(100)).await;

        let responder = tokio::spawn(async move {
            let (ack, request) = match receiver
                .recv_async()
                .await
                .expect("receive final snapshot request")
            {
                RaftRequest::SnapshotDB(request) => request,
                request => panic!("unexpected SQLite Raft request: {}", request.kind()),
            };
            assert!(request.done);
            drop(ack);
        });

        let result =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"only-final-chunk".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_secs(300)),
            )
            .await;
        responder.await.expect("join dropped-response peer");

        assert!(matches!(result, Err(RPCError::Unreachable(_))));
        assert_eq!(
            *attempt.stage.borrow(),
            SnapshotStageDeadline {
                phase: SnapshotAttemptPhase::FinalInstall,
                deadline: start + Duration::from_secs(160),
            }
        );
        let observation = transport.snapshot().observations.remove(0);
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(observation.attempt_count, 1);
        assert_eq!(observation.retry_count, 0);
        assert_eq!(observation.active_deadline_remaining_ms, Some(60_000));
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("transport_unavailable")
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_higher_vote_response_never_advances_snapshot_acknowledgement() {
        let budgets = test_snapshot_budgets();
        let (mut network, receiver) = test_network(budgets);
        let transport = network.snapshot_transport.clone();
        let transport_attempt = transport.next_outbound_attempt(
            "sqlite",
            network.node.id,
            network.transport_connection_id,
        );
        let tracked_attempt =
            SnapshotAttempt::tracked(transport_attempt, "snapshot-higher-vote", budgets);
        transport.begin_owned_outbound_attempt(
            &transport_attempt,
            "snapshot-higher-vote",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: network.reset.socket_epoch(),
                connected: network.reset.is_connected(),
            },
            tracked_attempt.transfer_deadline,
        );
        network.snapshot_attempt = Arc::new(StdMutex::new(Some(tracked_attempt)));
        let responder = tokio::spawn(async move {
            let (ack, request) = match receiver.recv_async().await.expect("receive request") {
                RaftRequest::SnapshotDB(request) => request,
                request => panic!("unexpected SQLite Raft request: {}", request.kind()),
            };
            ack.send(Ok(RaftStreamResponsePayload::SnapshotDB(Ok(
                InstallSnapshotResponse {
                    vote: Vote::new_committed(request.vote.leader_id.term + 1, 7),
                },
            ))))
            .expect("return higher vote");
        });
        let response =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"final".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_secs(30)),
            )
            .await
            .expect("higher vote remains a typed Raft response");
        responder.await.expect("join responder");
        assert_eq!(response.vote.leader_id.term, 2);
        let observation = transport.snapshot().observations.remove(0);
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Failed);
        assert_eq!(observation.acknowledged_offset, None);
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("higher_vote")
        );
        assert!(!observation.operation_owns_work);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_full_snapshot_enters_bounded_wrapper() {
        let (mut network, _receiver) = test_network(test_snapshot_budgets());
        let transport = network.snapshot_transport.clone();
        let slot = Arc::clone(&network.snapshot_attempt);
        let result = <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::full_snapshot(
            &mut network,
            Vote::new_committed(1, 1),
            Snapshot {
                meta: test_snapshot_meta(),
                snapshot: Box::new(test_snapshot_file()),
            },
            std::future::ready(ReplicationClosed::new("test cancellation")),
            RPCOption::new(Duration::from_secs(300)),
        )
        .await;

        assert!(matches!(result, Err(StreamingError::Closed(_))));
        assert_eq!(transport.outbound_attempt_count(), 1);
        assert!(slot.lock().expect("snapshot attempt slot").is_none());
    }

    #[cfg(feature = "cache")]
    #[tokio::test]
    async fn cache_full_snapshot_enters_bounded_wrapper() {
        let (mut network, _receiver) = test_network(test_snapshot_budgets());
        let transport = network.snapshot_transport.clone();
        let slot = Arc::clone(&network.snapshot_attempt);
        let result = <NetworkConnectionStreaming as RaftNetwork<TypeConfigKV>>::full_snapshot(
            &mut network,
            Vote::new_committed(1, 1),
            Snapshot {
                meta: test_snapshot_meta(),
                snapshot: Box::new(test_snapshot_file()),
            },
            std::future::ready(ReplicationClosed::new("test cancellation")),
            RPCOption::new(Duration::from_secs(300)),
        )
        .await;

        assert!(matches!(result, Err(StreamingError::Closed(_))));
        assert_eq!(transport.outbound_attempt_count(), 1);
        assert!(slot.lock().expect("snapshot attempt slot").is_none());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn advancing_transfer_may_exceed_one_chunk_window_and_finish_before_transfer_expiry() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(6, "advancing".into(), budgets);
        let stage = attempt.stage.subscribe();
        let driver_attempt = Arc::clone(&attempt);
        let transfer = tokio::spawn(supervise_snapshot_driver::<TypeConfigSqlite, _, _>(
            attempt,
            1,
            7,
            stage,
            std::future::pending::<ReplicationClosed>(),
            async move {
                for _ in 0..3 {
                    driver_attempt
                        .rpc_deadline(false, Duration::from_secs(300), budgets)
                        .expect("an advancing chunk starts before transfer expiry");
                    time::sleep(Duration::from_secs(30)).await;
                }
                Ok(SnapshotResponse::new(Vote::new(1, 1)))
            },
        ));
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            !transfer.is_finished(),
            "the chunk watchdog is per RPC, not a whole-transfer deadline"
        );
        time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(30)).await;

        assert!(matches!(
            transfer.await.expect("join advancing transfer"),
            Ok(response) if response.vote == Vote::new(1, 1)
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn unanswered_non_final_rpc_expires_at_chunk_budget_and_resets_socket() {
        let (sender, _receiver) = flume::bounded(1);
        let reset = Arc::new(ConnectionResetState::default());
        let socket_epoch = reset.epoch();
        let attempt = SnapshotAttempt::new(9, "snapshot".into(), test_snapshot_budgets());
        let mut network = NetworkConnectionStreaming {
            node: test_node(),
            sender,
            reset: Arc::clone(&reset),
            shutdown: Arc::new(ConnectionShutdownState::default()),
            local_node_id: 1,
            raft_group: "sqlite",
            snapshot_budgets: test_snapshot_budgets(),
            snapshot_transport: crate::LocalSnapshotTransportStatus::new(
                1,
                std::collections::BTreeSet::from([2]),
            ),
            snapshot_attempt: Arc::new(StdMutex::new(Some(attempt))),
            transport_connection_id: 1,
            runtime: tokio::runtime::Handle::current(),
            task: None,
        };
        let mut rpc = tokio::spawn(async move {
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"chunk".to_vec(),
                    done: false,
                },
                RPCOption::new(Duration::from_secs(300)),
            )
            .await
        });
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(!rpc.is_finished());
        time::advance(Duration::from_secs(1)).await;

        assert!(matches!(
            (&mut rpc).await.expect("join chunk RPC"),
            Err(RPCError::Unreachable(_))
        ));
        assert_ne!(
            reset.epoch(),
            socket_epoch,
            "the expired active RPC must request connection reset"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn repeated_mismatch_style_resets_expire_at_the_original_transfer_deadline() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(7, "mismatches".into(), budgets);
        let stage = attempt.stage.subscribe();
        let driver_attempt = Arc::clone(&attempt);
        let transfer = tokio::spawn(supervise_snapshot_driver::<TypeConfigSqlite, _, _>(
            attempt,
            1,
            7,
            stage,
            std::future::pending::<ReplicationClosed>(),
            async move {
                loop {
                    // A mismatch makes the upstream driver return to a
                    // non-final offset. Repeated calls update the active phase
                    // but must never replace the attempt's original deadline.
                    driver_attempt
                        .rpc_deadline(false, Duration::from_secs(300), budgets)
                        .expect("transfer remains active before its deadline");
                    time::sleep(Duration::from_secs(1)).await;
                }
                #[allow(unreachable_code)]
                Ok(SnapshotResponse::new(Vote::new(1, 1)))
            },
        ));
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(119)).await;
        tokio::task::yield_now().await;
        assert!(!transfer.is_finished());
        time::advance(Duration::from_secs(1)).await;

        assert!(matches!(
            transfer.await.expect("join bounded mismatch loop"),
            Err(StreamingError::Timeout(Timeout { timeout, .. }))
                if timeout == Duration::from_secs(120)
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn final_install_may_exceed_chunk_window_but_cannot_renew_install_window() {
        let budgets = test_snapshot_budgets();
        let attempt = SnapshotAttempt::new(8, "final".into(), budgets);
        time::advance(Duration::from_secs(10)).await;
        let final_deadline = attempt
            .rpc_deadline(true, Duration::from_secs(300), budgets)
            .expect("begin final install before transfer expiry");
        let stage = attempt.stage.subscribe();
        let retry_attempt = Arc::clone(&attempt);
        let install = tokio::spawn(supervise_snapshot_driver::<TypeConfigSqlite, _, _>(
            attempt,
            1,
            7,
            stage,
            std::future::pending::<ReplicationClosed>(),
            async {
                time::sleep(Duration::from_secs(45)).await;
                Ok(SnapshotResponse::new(Vote::new(1, 1)))
            },
        ));
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert!(
            !install.is_finished(),
            "a healthy final restore may run longer than the 30-second chunk window"
        );
        assert_eq!(
            retry_attempt.rpc_deadline(true, Duration::from_secs(300), budgets),
            Some(final_deadline),
            "a final retry cannot renew the first install deadline"
        );
        time::advance(Duration::from_secs(14)).await;

        assert!(matches!(
            install.await.expect("join final install"),
            Ok(response) if response.vote == Vote::new(1, 1)
        ));
    }

    #[test]
    fn stale_snapshot_guard_cannot_clear_a_newer_attempt() {
        let slot = Arc::new(StdMutex::new(None));
        let budgets = test_snapshot_budgets();
        let status =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([7]));
        let first_transport = status.next_outbound_attempt("sqlite", 7, 1);
        let first = SnapshotAttempt::new(first_transport.attempt_id, "first".into(), budgets);
        status.begin_owned_outbound_attempt(
            &first_transport,
            "first",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: 1,
                connected: true,
            },
            first.transfer_deadline,
        );
        *slot.lock().expect("snapshot attempt slot") = Some(Arc::clone(&first));
        let first_guard =
            SnapshotAttemptGuard::new(first.id, Arc::clone(&slot), status.clone(), first_transport);
        let second_transport = status.next_outbound_attempt("sqlite", 7, 1);
        let second = SnapshotAttempt::new(second_transport.attempt_id, "second".into(), budgets);
        status.begin_owned_outbound_attempt(
            &second_transport,
            "second",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: 1,
                connected: true,
            },
            second.transfer_deadline,
        );
        *slot.lock().expect("snapshot attempt slot") = Some(Arc::clone(&second));

        drop(first_guard);

        assert_eq!(
            slot.lock()
                .expect("snapshot attempt slot")
                .as_ref()
                .map(|attempt| attempt.id),
            Some(second.id)
        );
        assert_eq!(status.snapshot().observations[0].attempt_id, second.id);
        assert!(status.snapshot().observations[0].operation_owns_work);
        drop(SnapshotAttemptGuard::new(
            second.id,
            Arc::clone(&slot),
            status.clone(),
            second_transport,
        ));
        assert!(slot.lock().expect("snapshot attempt slot").is_none());
        assert_eq!(
            status.snapshot().observations[0].phase,
            crate::SnapshotTransportPhase::Failed
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn caller_cancellation_drops_the_active_snapshot_rpc_guard_immediately() {
        let reset = Arc::new(ConnectionResetState::default());
        let socket_epoch = reset.epoch();
        let status =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([7]));
        let transport_attempt = status.next_outbound_attempt("sqlite", 7, 1);
        let attempt = SnapshotAttempt::new(
            transport_attempt.attempt_id,
            "snapshot".into(),
            test_snapshot_budgets(),
        );
        status.begin_owned_outbound_attempt(
            &transport_attempt,
            "snapshot",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: 1,
                connected: true,
            },
            attempt.transfer_deadline,
        );
        let attempts = Arc::new(StdMutex::new(Some(Arc::clone(&attempt))));
        let attempt_guard = SnapshotAttemptGuard::new(
            attempt.id,
            Arc::clone(&attempts),
            status.clone(),
            transport_attempt,
        );
        let (cancel, cancelled) = oneshot::channel();
        let active_rpc = {
            let reset = Arc::clone(&reset);
            async move {
                let _reset_guard = ConnectionResetGuard::new(reset);
                std::future::pending::<
                    Result<
                        SnapshotResponse<NodeId>,
                        StreamingError<TypeConfigSqlite, Fatal<NodeId>>,
                    >,
                >()
                .await
            }
        };
        let transfer = tokio::spawn({
            let attempt = Arc::clone(&attempt);
            async move {
                let result = supervise_snapshot_driver(
                    Arc::clone(&attempt),
                    1,
                    7,
                    attempt.stage.subscribe(),
                    async move {
                        cancelled
                            .await
                            .expect("test cancellation sender remains open")
                    },
                    active_rpc,
                )
                .await;
                drop(attempt_guard);
                result
            }
        });
        tokio::task::yield_now().await;
        cancel
            .send(ReplicationClosed::new("test leader change"))
            .expect("active RPC still waits for cancellation");

        assert!(matches!(
            transfer.await.expect("join snapshot wrapper"),
            Err(StreamingError::Closed(_))
        ));
        assert!(
            attempts.lock().expect("snapshot attempt slot").is_none(),
            "the attempt guard must clear on cancellation"
        );
        let terminal = &status.snapshot().observations[0];
        assert_eq!(terminal.phase, crate::SnapshotTransportPhase::Failed);
        assert_eq!(
            terminal.last_error_category.as_deref(),
            Some("snapshot_attempt_ended")
        );
        assert!(!terminal.operation_owns_work);
        assert_ne!(
            reset.epoch(),
            socket_epoch,
            "dropping the active RPC guard must request connection reset"
        );
    }

    #[tokio::test]
    async fn aborting_snapshot_owner_publishes_attempt_guarded_terminal_status() {
        let status =
            crate::LocalSnapshotTransportStatus::new(1, std::collections::BTreeSet::from([7]));
        let transport_attempt = status.next_outbound_attempt("sqlite", 7, 1);
        let attempt = SnapshotAttempt::new(
            transport_attempt.attempt_id,
            "aborted".into(),
            test_snapshot_budgets(),
        );
        status.begin_owned_outbound_attempt(
            &transport_attempt,
            "aborted",
            crate::transport_status::OutboundSnapshotSocket {
                epoch: 1,
                connected: true,
            },
            attempt.transfer_deadline,
        );
        let slot = Arc::new(StdMutex::new(Some(Arc::clone(&attempt))));
        let started = Arc::new(tokio::sync::Notify::new());
        let owner = tokio::spawn({
            let status = status.clone();
            let slot = Arc::clone(&slot);
            let started = Arc::clone(&started);
            async move {
                let _guard = SnapshotAttemptGuard::new(attempt.id, slot, status, transport_attempt);
                started.notify_one();
                std::future::pending::<()>().await;
            }
        });
        started.notified().await;

        owner.abort();
        assert!(owner.await.expect_err("owner is aborted").is_cancelled());
        assert!(slot.lock().expect("snapshot attempt slot").is_none());
        let terminal = &status.snapshot().observations[0];
        assert_eq!(terminal.attempt_id, transport_attempt.attempt_id);
        assert_eq!(terminal.phase, crate::SnapshotTransportPhase::Failed);
        assert_eq!(
            terminal.last_error_category.as_deref(),
            Some("snapshot_attempt_ended")
        );
        assert!(!terminal.operation_owns_work);
    }

    #[test]
    fn snapshot_mismatch_remains_a_remote_api_error() {
        let node = test_node();
        let mismatch = SnapshotMismatch {
            expect: SnapshotSegmentId::from(("snapshot", 0)),
            got: SnapshotSegmentId::from(("snapshot", 6_291_456)),
        };

        let error: RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>> =
            remote_raft_error(
                &node,
                RaftError::APIError(InstallSnapshotError::SnapshotMismatch(mismatch.clone())),
            );

        assert!(matches!(
            error,
            RPCError::RemoteError(RemoteError {
                target: 7,
                target_node: Some(target_node),
                source: RaftError::APIError(InstallSnapshotError::SnapshotMismatch(actual)),
            }) if target_node == node && actual == mismatch
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_install_snapshot_preserves_mismatch_for_offset_reset() {
        let (sender, receiver) = flume::bounded(1);
        let reset = Arc::new(ConnectionResetState::default());
        let socket_epoch = reset.epoch();
        let mut network = NetworkConnectionStreaming {
            node: test_node(),
            sender,
            reset: Arc::clone(&reset),
            shutdown: Arc::new(ConnectionShutdownState::default()),
            local_node_id: 1,
            raft_group: "sqlite",
            snapshot_budgets: SnapshotRpcBudgets {
                chunk: Duration::from_secs(30),
                transfer: Duration::from_secs(1_200),
                install: Duration::from_secs(120),
            },
            snapshot_transport: crate::LocalSnapshotTransportStatus::new(
                1,
                std::collections::BTreeSet::from([2]),
            ),
            snapshot_attempt: Arc::new(StdMutex::new(None)),
            transport_connection_id: 1,
            runtime: tokio::runtime::Handle::current(),
            task: None,
        };
        let responder = tokio::spawn(async move {
            for expected_offset in [4, 0] {
                let (ack, request) = match receiver
                    .recv_async()
                    .await
                    .expect("receive SQLite snapshot request")
                {
                    RaftRequest::SnapshotDB(request) => request,
                    request => panic!("unexpected SQLite Raft request: {}", request.kind()),
                };
                assert_eq!(request.offset, expected_offset);
                let response = if expected_offset == 4 {
                    Err(mismatch_at(request.offset))
                } else {
                    Ok(InstallSnapshotResponse {
                        vote: Vote::new_committed(1, 7),
                    })
                };
                ack.send(Ok(RaftStreamResponsePayload::SnapshotDB(response)))
                    .expect("return SQLite snapshot response");
            }
        });

        let error =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 4,
                    data: b"efgh".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_millis(500)),
            )
            .await;
        assert!(matches!(
            error,
            Err(RPCError::RemoteError(RemoteError {
                target: 7,
                target_node: Some(target_node),
                source: RaftError::APIError(InstallSnapshotError::SnapshotMismatch(
                    SnapshotMismatch { expect, got },
                )),
            })) if target_node == test_node()
                && expect == SnapshotSegmentId::from(("snapshot", 0))
                && got == SnapshotSegmentId::from(("snapshot", 4))
        ));
        let recovered =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigSqlite>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"abcd".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_millis(500)),
            )
            .await
            .expect("offset-zero retry must succeed on the same connection");
        responder.await.expect("join SQLite snapshot responder");
        assert_eq!(recovered.vote, Vote::new_committed(1, 7));
        assert_eq!(reset.epoch(), socket_epoch);
    }

    #[cfg(feature = "cache")]
    #[tokio::test]
    async fn cache_install_snapshot_preserves_mismatch_for_offset_reset() {
        let (sender, receiver) = flume::bounded(1);
        let reset = Arc::new(ConnectionResetState::default());
        let socket_epoch = reset.epoch();
        let mut network = NetworkConnectionStreaming {
            node: test_node(),
            sender,
            reset: Arc::clone(&reset),
            shutdown: Arc::new(ConnectionShutdownState::default()),
            local_node_id: 1,
            raft_group: "cache",
            snapshot_budgets: SnapshotRpcBudgets {
                chunk: Duration::from_secs(30),
                transfer: Duration::from_secs(1_200),
                install: Duration::from_secs(120),
            },
            snapshot_transport: crate::LocalSnapshotTransportStatus::new(
                1,
                std::collections::BTreeSet::from([2]),
            ),
            snapshot_attempt: Arc::new(StdMutex::new(None)),
            transport_connection_id: 1,
            runtime: tokio::runtime::Handle::current(),
            task: None,
        };
        let responder = tokio::spawn(async move {
            for expected_offset in [4, 0] {
                let (ack, request) = match receiver
                    .recv_async()
                    .await
                    .expect("receive cache snapshot request")
                {
                    RaftRequest::SnapshotCache(request) => request,
                    request => panic!("unexpected cache Raft request: {}", request.kind()),
                };
                assert_eq!(request.offset, expected_offset);
                let response = if expected_offset == 4 {
                    Err(mismatch_at(request.offset))
                } else {
                    Ok(InstallSnapshotResponse {
                        vote: Vote::new_committed(1, 7),
                    })
                };
                ack.send(Ok(RaftStreamResponsePayload::SnapshotCache(response)))
                    .expect("return cache snapshot response");
            }
        });

        let error = <NetworkConnectionStreaming as RaftNetwork<TypeConfigKV>>::install_snapshot(
            &mut network,
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 1),
                meta: test_snapshot_meta(),
                offset: 4,
                data: b"efgh".to_vec(),
                done: true,
            },
            RPCOption::new(Duration::from_millis(500)),
        )
        .await;
        assert!(matches!(
            error,
            Err(RPCError::RemoteError(RemoteError {
                target: 7,
                target_node: Some(target_node),
                source: RaftError::APIError(InstallSnapshotError::SnapshotMismatch(
                    SnapshotMismatch { expect, got },
                )),
            })) if target_node == test_node()
                && expect == SnapshotSegmentId::from(("snapshot", 0))
                && got == SnapshotSegmentId::from(("snapshot", 4))
        ));
        let recovered =
            <NetworkConnectionStreaming as RaftNetwork<TypeConfigKV>>::install_snapshot(
                &mut network,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: test_snapshot_meta(),
                    offset: 0,
                    data: b"abcd".to_vec(),
                    done: true,
                },
                RPCOption::new(Duration::from_millis(500)),
            )
            .await
            .expect("offset-zero retry must succeed on the same connection");
        responder.await.expect("join cache snapshot responder");
        assert_eq!(recovered.vote, Vote::new_committed(1, 7));
        assert_eq!(reset.epoch(), socket_epoch);
    }
}
