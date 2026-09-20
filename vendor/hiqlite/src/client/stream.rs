use crate::app_state::RaftType;
use crate::helpers::deserialize;
use crate::network::api::{ApiStreamResponse, ApiStreamResponsePayload};
use crate::network::frame_io::{
    CLOSE_WRITE_TIMEOUT, write_close_frame_flushed, write_frame_flushed,
};
use crate::network::{serialize_network, web_socket_connect};
use crate::{Client, Error, Node, NodeId};
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, WebSocket, WebSocketWrite};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tokio::{select, task, time};
use tracing::{debug, error, info};

#[cfg(any(feature = "sqlite", feature = "cache"))]
use crate::network::api::{ApiStreamRequest, ApiStreamRequestPayload};
#[cfg(feature = "cache")]
use crate::store::state_machine::memory::state_machine::CacheRequest;
#[cfg(feature = "sqlite")]
use crate::{migration::Migration, store::state_machine::sqlite::state_machine::Query};

#[derive(Debug)]
pub(crate) enum ClientStreamReq {
    // coming from the `DbClient`
    #[cfg(feature = "sqlite")]
    Execute(ClientExecutePayload),
    #[cfg(feature = "sqlite")]
    ExecuteReturning(ClientExecutePayload),
    #[cfg(feature = "sqlite")]
    Transaction(ClientTransactionPayload),
    #[cfg(feature = "sqlite")]
    Query(ClientQueryPayload),
    #[cfg(feature = "sqlite")]
    QueryConsistent(ClientQueryPayload),
    #[cfg(feature = "sqlite")]
    Batch(ClientBatchPayload),
    #[cfg(feature = "sqlite")]
    Migrate(ClientMigratePayload),

    #[cfg(feature = "backup")]
    Backup(ClientBackupPayload),

    #[cfg(feature = "cache")]
    KV(ClientKVPayload),
    #[cfg(feature = "cache")]
    KVGet(ClientKVPayload),

    #[cfg(feature = "dlock")]
    LockAwait(ClientKVPayload),

    #[cfg(feature = "listen_notify_local")]
    Notify(ClientKVPayload),

    Shutdown,

    // coming from the WebSocket reader
    StreamResponse(ApiStreamResponse),
    CleanupBuffer,
}

/// Priority control message consumed even while the manager is opening its
/// current WebSocket. Keeping leader handoff off the one-slot request queue
/// prevents application traffic or a slow obsolete handshake from consuming
/// the bounded recovery window.
#[derive(Debug)]
pub(crate) struct ClientLeaderChange {
    pub(crate) leader_id: NodeId,
    pub(crate) node: Node,
    pub(crate) ready: Option<oneshot::Sender<()>>,
}

/// Recovery controls have a dedicated queue so application FIFO traffic
/// cannot hide a handoff while the writer queue is full.
#[derive(Debug)]
pub(crate) enum ClientStreamControl {
    Leader(ClientLeaderChange),
    #[cfg(feature = "dashboard")]
    DashboardLeader((Option<NodeId>, Option<Node>), Option<oneshot::Sender<()>>),
}

#[derive(Default)]
struct PendingLeaderReady {
    target: Option<(NodeId, String)>,
    waiters: Vec<oneshot::Sender<()>>,
}

impl PendingLeaderReady {
    fn register(&mut self, target: (NodeId, String), ready: Option<oneshot::Sender<()>>) -> bool {
        self.prune_closed();
        if ready.as_ref().is_some_and(oneshot::Sender::is_closed) {
            // Its bounded caller expired while this control message waited to
            // be received. It no longer has authority to redirect the stream.
            return false;
        }
        if self.target.as_ref() != Some(&target) {
            // Dropping superseded senders makes the old recovery generation
            // fail instead of acknowledging it on a stream to another node.
            self.waiters.clear();
            self.target = Some(target);
        }
        if let Some(ready) = ready {
            self.waiters.push(ready);
        }
        true
    }

    fn prune_closed(&mut self) {
        self.waiters.retain(|ready| !ready.is_closed());
        if self.waiters.is_empty() {
            self.target = None;
        }
    }

    fn resolve_connected_target(&mut self, connected: &(NodeId, String)) {
        self.prune_closed();
        if self
            .target
            .as_ref()
            .is_some_and(|target| target != connected)
        {
            // The requested target failed and authenticated discovery selected
            // another live leader. Fail the obsolete handoff rather than
            // dropping the valid stream forever or falsely acknowledging it.
            self.waiters.clear();
            self.target = None;
        }
    }

    fn acknowledge(&mut self, connected: &(NodeId, String)) -> bool {
        if self.target.as_ref() != Some(connected) {
            return false;
        }
        for ready in self.waiters.drain(..) {
            let _ = ready.send(());
        }
        self.target = None;
        true
    }
}

fn leader_handoff_restarts_connection(
    connecting: &(NodeId, String),
    incoming: &(NodeId, String),
) -> bool {
    connecting != incoming
}

impl ClientStreamReq {
    fn fail_on_shutdown(self) {
        let error = || Error::Connect("client stream manager stopped".into());
        match self {
            #[cfg(feature = "sqlite")]
            Self::Execute(payload) | Self::ExecuteReturning(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "sqlite")]
            Self::Transaction(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "sqlite")]
            Self::Query(payload) | Self::QueryConsistent(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "sqlite")]
            Self::Batch(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "sqlite")]
            Self::Migrate(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "backup")]
            Self::Backup(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "cache")]
            Self::KV(payload) | Self::KVGet(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "dlock")]
            Self::LockAwait(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            #[cfg(feature = "listen_notify_local")]
            Self::Notify(payload) => {
                let _ = payload.ack.send(Err(error()));
            }
            Self::Shutdown | Self::StreamResponse(_) | Self::CleanupBuffer => {}
        }
    }
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct ClientExecutePayload {
    pub request_id: usize,
    pub sql: Query,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct ClientTransactionPayload {
    pub request_id: usize,
    pub queries: Vec<Query>,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct ClientQueryPayload {
    pub request_id: usize,
    pub query: Query,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct ClientBatchPayload {
    pub request_id: usize,
    pub sql: std::borrow::Cow<'static, str>,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct ClientMigratePayload {
    pub request_id: usize,
    pub migrations: Vec<Migration>,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "backup")]
#[derive(Debug)]
pub struct ClientBackupPayload {
    pub request_id: usize,
    pub node_id: NodeId,
    pub ts: i64,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[cfg(feature = "cache")]
#[derive(Debug)]
pub struct ClientKVPayload {
    pub request_id: usize,
    pub cache_req: CacheRequest,
    pub ack: oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
}

#[derive(Debug)]
enum WritePayload {
    Payload(Vec<u8>),
    Close,
}

enum TryWriterEnqueue {
    Sent,
    Full(WritePayload),
    Disconnected,
}

fn try_writer_enqueue(
    writer: &flume::Sender<WritePayload>,
    payload: WritePayload,
) -> TryWriterEnqueue {
    match writer.try_send(payload) {
        Ok(()) => TryWriterEnqueue::Sent,
        Err(flume::TrySendError::Full(payload)) => TryWriterEnqueue::Full(payload),
        Err(flume::TrySendError::Disconnected(_)) => TryWriterEnqueue::Disconnected,
    }
}

enum ClientConnectedEvent {
    Shutdown,
    ReaderFinished(Result<(), String>),
    WriterFinished(Result<(), String>),
    Control(Result<ClientStreamControl, flume::RecvError>),
    Incoming(Result<ClientStreamReq, flume::RecvError>),
}

enum ClientEnqueueEvent {
    Shutdown,
    Reader(Result<ClientStreamReq, flume::RecvError>),
    Control(Result<ClientStreamControl, flume::RecvError>),
    ReaderFinished(Result<(), String>),
    WriterFinished(Result<(), String>),
    Retry,
}

fn latched_client_enqueue_event(
    stream_shutdown: &tokio::sync::watch::Receiver<bool>,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
    reader: &flume::Receiver<ClientStreamReq>,
    controls: &flume::Receiver<ClientStreamControl>,
) -> Option<ClientEnqueueEvent> {
    if *stream_shutdown.borrow() {
        return Some(ClientEnqueueEvent::Shutdown);
    }
    match reader_finished.try_recv() {
        Ok(outcome) => return Some(ClientEnqueueEvent::ReaderFinished(outcome)),
        Err(oneshot::error::TryRecvError::Closed) => {
            return Some(ClientEnqueueEvent::ReaderFinished(Err(
                "API reader task exited without reporting an outcome".into(),
            )));
        }
        Err(oneshot::error::TryRecvError::Empty) => {}
    }
    match writer_finished.try_recv() {
        Ok(outcome) => return Some(ClientEnqueueEvent::WriterFinished(outcome)),
        Err(oneshot::error::TryRecvError::Closed) => {
            return Some(ClientEnqueueEvent::WriterFinished(Err(
                "API writer task exited without reporting an outcome".into(),
            )));
        }
        Err(oneshot::error::TryRecvError::Empty) => {}
    }
    match reader.try_recv() {
        Ok(request) => return Some(ClientEnqueueEvent::Reader(Ok(request))),
        Err(flume::TryRecvError::Disconnected) => {
            return Some(ClientEnqueueEvent::Reader(Err(
                flume::RecvError::Disconnected,
            )));
        }
        Err(flume::TryRecvError::Empty) => {}
    }
    match controls.try_recv() {
        Ok(control) => Some(ClientEnqueueEvent::Control(Ok(control))),
        Err(flume::TryRecvError::Disconnected) => Some(ClientEnqueueEvent::Control(Err(
            flume::RecvError::Disconnected,
        ))),
        Err(flume::TryRecvError::Empty) => None,
    }
}

async fn next_client_connected_event(
    stream_shutdown: &mut tokio::sync::watch::Receiver<bool>,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
    controls: &flume::Receiver<ClientStreamControl>,
    reader: &flume::Receiver<ClientStreamReq>,
    requests: &flume::Receiver<ClientStreamReq>,
) -> ClientConnectedEvent {
    select! {
        biased;
        _ = stream_shutdown.changed() => ClientConnectedEvent::Shutdown,
        result = reader_finished => ClientConnectedEvent::ReaderFinished(
            result.unwrap_or_else(|_| Err(
                "API reader task exited without reporting an outcome".into()
            ))
        ),
        result = writer_finished => ClientConnectedEvent::WriterFinished(
            result.unwrap_or_else(|_| Err(
                "API writer task exited without reporting an outcome".into()
            ))
        ),
        result = reader.recv_async() => ClientConnectedEvent::Incoming(result),
        control = controls.recv_async() => ClientConnectedEvent::Control(control),
        result = requests.recv_async() => ClientConnectedEvent::Incoming(result),
    }
}

const CLIENT_STREAM_RETRY_DELAY: Duration = Duration::from_secs(1);

fn reconnect_delay(
    previous_leader: &(NodeId, String),
    current_leader: &(NodeId, String),
) -> Duration {
    if previous_leader == current_leader {
        CLIENT_STREAM_RETRY_DELAY
    } else {
        Duration::ZERO
    }
}

async fn apply_disconnected_control(
    leader: &Arc<RwLock<(NodeId, String)>>,
    pending_leader_ready: &mut PendingLeaderReady,
    connecting_target: Option<&(NodeId, String)>,
    control: ClientStreamControl,
) -> bool {
    match control {
        ClientStreamControl::Leader(ClientLeaderChange {
            leader_id,
            node,
            ready,
        }) => {
            let target = (leader_id, node.addr_api.clone());
            if !pending_leader_ready.register(target.clone(), ready) {
                return false;
            }
            if connecting_target.is_some_and(|connecting_target| {
                !leader_handoff_restarts_connection(connecting_target, &target)
            }) {
                // A duplicate for the stream already being opened shares that
                // handshake instead of resetting its five-second attempt near
                // the recovery deadline.
                return false;
            }
            update_leader(leader, Some(leader_id), Some(node)).await;
            true
        }
        #[cfg(feature = "dashboard")]
        ClientStreamControl::DashboardLeader((node_id, node), ready) => {
            let target = node_id
                .zip(node.as_ref())
                .map(|(node_id, node)| (node_id, node.addr_api.clone()));
            if let Some(target) = target {
                if !pending_leader_ready.register(target.clone(), ready) {
                    return false;
                }
                if connecting_target.is_some_and(|connecting_target| {
                    !leader_handoff_restarts_connection(connecting_target, &target)
                }) {
                    return false;
                }
            }
            update_leader(leader, node_id, node).await;
            true
        }
    }
}

impl Client {
    pub(crate) fn open_stream(
        &self,
        secret: Vec<u8>,
        leader: Arc<RwLock<(NodeId, String)>>,
        rx_client_stream: flume::Receiver<ClientStreamReq>,
        rx_control: flume::Receiver<ClientStreamControl>,
        raft_type: RaftType,
    ) {
        let handle = task::spawn(Box::pin(client_stream(
            self.clone(),
            secret,
            leader,
            rx_client_stream,
            rx_control,
            raft_type,
            self.inner.stream_shutdown.subscribe(),
        )));
        self.inner
            .background_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(handle);
    }
}

/// Manager task which handles connection creation, split into sender / receiver, keeps the state,
/// handles reconnects and leader switches.
async fn client_stream(
    client: Client,
    secret: Vec<u8>,
    leader: Arc<RwLock<(NodeId, String)>>,
    rx_req: flume::Receiver<ClientStreamReq>,
    rx_control: flume::Receiver<ClientStreamControl>,
    raft_type: RaftType,
    mut stream_shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut in_flight: HashMap<usize, oneshot::Sender<Result<ApiStreamResponsePayload, Error>>> =
        HashMap::with_capacity(8);
    let mut in_flight_buf: HashMap<
        usize,
        oneshot::Sender<Result<ApiStreamResponsePayload, Error>>,
    > = HashMap::new();
    let mut shutdown = false;
    let mut pending_leader_ready = PendingLeaderReady::default();
    // DB and cache managers each own their cursor. An index, rather than an
    // address lookup, keeps duplicate configured endpoints from pinning a
    // stream forever.
    let mut proxy_index = 0;

    'manager: loop {
        pending_leader_ready.prune_closed();
        let connecting_target = leader.read().await.clone();
        let connection = try_connect(
            &leader,
            &raft_type,
            client.inner.tls_config.clone(),
            &secret,
        );
        tokio::pin!(connection);
        let connection = loop {
            select! {
                _ = stream_shutdown.changed() => {
                    fail_client_stream_shutdown(
                        &mut in_flight,
                        &mut in_flight_buf,
                        &rx_req,
                    );
                    return;
                }
                control = rx_control.recv_async() => {
                    let Ok(control) = control else {
                        fail_client_stream_shutdown(
                            &mut in_flight,
                            &mut in_flight_buf,
                            &rx_req,
                        );
                        return;
                    };
                    if apply_disconnected_control(
                        &leader,
                        &mut pending_leader_ready,
                        Some(&connecting_target),
                        control,
                    ).await {
                        continue 'manager;
                    }
                }
                connection = &mut connection => break connection,
            }
        };
        let (ws, connected_leader) = match connection {
            Ok((ws, connected_leader)) => {
                info!(
                    "Client API WebSocket to {} opened successfully",
                    connected_leader.1
                );
                (ws, connected_leader)
            }
            Err(err) => {
                let previous_leader = leader.read().await.clone();
                let mut retry_delay = CLIENT_STREAM_RETRY_DELAY;
                if client.inner.proxy_mode {
                    // No request was dispatched, so every handshake/TLS/API
                    // error is safe to recover at the next configured proxy.
                    rotate_proxy_endpoint(&client, &leader, &mut proxy_index).await;
                } else if let Error::Connect(_) = &err {
                    let discovery = client.find_set_active_leader();
                    tokio::pin!(discovery);
                    loop {
                        select! {
                            biased;
                            _ = stream_shutdown.changed() => {
                                fail_client_stream_shutdown(
                                    &mut in_flight,
                                    &mut in_flight_buf,
                                    &rx_req,
                                );
                                return;
                            }
                            control = rx_control.recv_async() => {
                                let Ok(control) = control else {
                                    fail_client_stream_shutdown(
                                        &mut in_flight,
                                        &mut in_flight_buf,
                                        &rx_req,
                                    );
                                    return;
                                };
                                if apply_disconnected_control(
                                    &leader,
                                    &mut pending_leader_ready,
                                    None,
                                    control,
                                ).await {
                                    continue 'manager;
                                }
                            }
                            () = &mut discovery => break,
                        }
                    }
                    let current_leader = leader.read().await.clone();
                    retry_delay = reconnect_delay(&previous_leader, &current_leader);
                }

                if !retry_delay.is_zero() {
                    let delay = time::sleep(retry_delay);
                    tokio::pin!(delay);
                    loop {
                        select! {
                            biased;
                            _ = stream_shutdown.changed() => {
                                fail_client_stream_shutdown(
                                    &mut in_flight,
                                    &mut in_flight_buf,
                                    &rx_req,
                                );
                                return;
                            }
                            control = rx_control.recv_async() => {
                                let Ok(control) = control else {
                                    fail_client_stream_shutdown(
                                        &mut in_flight,
                                        &mut in_flight_buf,
                                        &rx_req,
                                    );
                                    return;
                                };
                                if apply_disconnected_control(
                                    &leader,
                                    &mut pending_leader_ready,
                                    None,
                                    control,
                                ).await {
                                    continue 'manager;
                                }
                            }
                            () = &mut delay => break,
                        }
                    }
                }
                error!(
                    "Could not connect Client API WebSocket to {}: {}",
                    leader.read().await.1,
                    err
                );
                continue;
            }
        };

        pending_leader_ready.resolve_connected_target(&connected_leader);

        let (tx_write, rx_write) = flume::bounded(1);
        let (tx_read, rx_read) = flume::bounded(1);
        let pending_reader_response = Arc::new(Mutex::new(None));

        // TODO splitting needs `unstable-split` feature right now but is about to be stabilized soon
        let (rx, write) = ws.split(tokio::io::split);
        // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
        let read = FragmentCollectorRead::new(rx);

        let (tx_reader_finished, mut rx_reader_finished) = oneshot::channel();
        let reader_tx = tx_read.clone();
        let reader_pending = pending_reader_response.clone();
        let handle_read = task::spawn(async move {
            let outcome = stream_reader(read, reader_tx, reader_pending).await;
            let _ = tx_reader_finished.send(outcome);
        });
        let (tx_writer_finished, mut rx_writer_finished) = oneshot::channel();
        let handle_write = task::spawn(stream_writer(write, rx_write, tx_writer_finished));

        pending_leader_ready.acknowledge(&connected_leader);

        let handle_buf = cleanup_buffer_timeout(tx_read, 10);
        let mut awaiting_timeout = true;
        let mut rotate_after_disconnect = false;
        let mut terminal_transport_failure = false;
        let mut force_writer_abort = false;
        let mut leader_handoff = false;
        let mut proxy_handoff = false;

        'connected: loop {
            let event = next_client_connected_event(
                &mut stream_shutdown,
                &mut rx_reader_finished,
                &mut rx_writer_finished,
                &rx_control,
                &rx_read,
                &rx_req,
            )
            .await;
            let res = match event {
                ClientConnectedEvent::Shutdown => {
                    shutdown = true;
                    None
                }
                ClientConnectedEvent::ReaderFinished(result) => {
                    match result {
                        Ok(()) => debug!("API WebSocket reader exited"),
                        Err(err) => error!("API WebSocket reader failed: {err}"),
                    }
                    terminal_transport_failure = true;
                    rotate_after_disconnect = client.inner.proxy_mode;
                    None
                }
                ClientConnectedEvent::WriterFinished(result) => {
                    match result {
                        Ok(()) => error!("API WebSocket writer exited while connected"),
                        Err(err) => error!("API WebSocket writer failed: {err}"),
                    }
                    terminal_transport_failure = true;
                    force_writer_abort = true;
                    rotate_after_disconnect = client.inner.proxy_mode;
                    None
                }
                ClientConnectedEvent::Control(control) => {
                    let Ok(control) = control else {
                        let _ = tx_write.try_send(WritePayload::Close);
                        shutdown = true;
                        break;
                    };
                    match control {
                        ClientStreamControl::Leader(ClientLeaderChange {
                            leader_id,
                            node,
                            ready,
                        }) => {
                            let target = (leader_id, node.addr_api.clone());
                            if target == connected_leader {
                                if let Some(ready) = ready {
                                    let _ = ready.send(());
                                }
                                continue;
                            }
                            if !pending_leader_ready.register(target, ready) {
                                continue;
                            }
                            let _ = tx_write.try_send(WritePayload::Close);
                            update_leader(&leader, Some(leader_id), Some(node)).await;
                            leader_handoff = true;
                            break;
                        }
                        #[cfg(feature = "dashboard")]
                        ClientStreamControl::DashboardLeader((node_id, node), ready) => {
                            if leader_change_matches_connection(
                                &connected_leader,
                                node_id,
                                node.as_ref(),
                            ) {
                                if let Some(ready) = ready {
                                    let _ = ready.send(());
                                }
                                continue;
                            }
                            let _ = tx_write.try_send(WritePayload::Close);
                            let ready_target = node_id
                                .zip(node.as_ref())
                                .map(|(node_id, node)| (node_id, node.addr_api.clone()));
                            update_leader(&leader, node_id, node).await;
                            if let (Some(target), Some(ready)) = (ready_target, ready) {
                                let _ = pending_leader_ready.register(target, Some(ready));
                            }
                            leader_handoff = true;
                            break;
                        }
                    }
                }
                ClientConnectedEvent::Incoming(result) => Some(result),
            };
            let Some(res) = res else {
                let _ = tx_write.try_send(WritePayload::Close);
                break;
            };
            let req = match res {
                Ok(req) => req,
                Err(err) => {
                    error!("Client stream reader error: {}", err,);
                    if rx_req.is_disconnected() {
                        let _ = tx_write.try_send(WritePayload::Close);
                        shutdown = true;
                    } else if client.inner.proxy_mode {
                        rotate_after_disconnect = true;
                    }
                    break;
                }
            };

            let payload = match req {
                #[cfg(feature = "sqlite")]
                ClientStreamReq::Execute(ClientExecutePayload {
                    request_id,
                    sql,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Execute(sql),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        req.request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::ExecuteReturning(ClientExecutePayload {
                    request_id,
                    sql,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::ExecuteReturning(sql),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::Transaction(ClientTransactionPayload {
                    request_id,
                    queries,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Transaction(queries),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::Query(ClientQueryPayload {
                    request_id,
                    query,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Query(query),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::QueryConsistent(ClientQueryPayload {
                    request_id,
                    query,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::QueryConsistent(query),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::Batch(ClientBatchPayload {
                    request_id,
                    sql,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Batch(sql),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "sqlite")]
                ClientStreamReq::Migrate(ClientMigratePayload {
                    request_id,
                    migrations,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Migrate(migrations),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "backup")]
                ClientStreamReq::Backup(ClientBackupPayload {
                    request_id,
                    node_id,
                    ts,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Backup((node_id, ts)),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "cache")]
                ClientStreamReq::KV(ClientKVPayload {
                    request_id,
                    cache_req,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::KV(cache_req),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "cache")]
                ClientStreamReq::KVGet(ClientKVPayload {
                    request_id,
                    cache_req,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::KVGet(cache_req),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "dlock")]
                ClientStreamReq::LockAwait(ClientKVPayload {
                    request_id,
                    cache_req,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::LockAwait(cache_req),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                #[cfg(feature = "listen_notify_local")]
                ClientStreamReq::Notify(ClientKVPayload {
                    request_id,
                    cache_req,
                    ack,
                }) => {
                    let req = ApiStreamRequest {
                        request_id,
                        payload: ApiStreamRequestPayload::Notify(cache_req),
                    };
                    Some((
                        WritePayload::Payload(serialize_network(&req)),
                        request_id,
                        ack,
                    ))
                }

                ClientStreamReq::StreamResponse(resp) => {
                    let proxy_refused = try_forward_response(
                        &mut in_flight,
                        &mut in_flight_buf,
                        awaiting_timeout,
                        resp,
                    )
                    .await;
                    if client.inner.proxy_mode && proxy_refused {
                        // This response belongs to the current socket. Claim
                        // its one handoff before yielding so the exact retry
                        // cannot return through the refusing proxy.
                        proxy_handoff = true;
                        break;
                    }
                    None
                }

                ClientStreamReq::CleanupBuffer => {
                    for (_, ack) in in_flight_buf {
                        let _ = ack.send(Err(Error::Connect("request timed out".to_string())));
                    }
                    in_flight_buf = HashMap::new();
                    awaiting_timeout = false;
                    None
                }

                ClientStreamReq::Shutdown => {
                    shutdown = true;
                    break;
                }
            };

            if let Some((payload, request_id, ack)) = payload {
                // `flume::SendFut` is not cancellation safe at the ownership
                // boundary: it can transfer the item before being repolled
                // Ready. Retain `Full(payload)` explicitly so any terminal
                // branch below can truthfully classify this request as not
                // dispatched. Once `try_send` succeeds, its application
                // outcome is treated as unknown until a response arrives.
                let mut pending_payload = payload;
                loop {
                    let enqueue = if let Some(event) = latched_client_enqueue_event(
                        &stream_shutdown,
                        &mut rx_reader_finished,
                        &mut rx_writer_finished,
                        &rx_read,
                        &rx_control,
                    ) {
                        event
                    } else {
                        match try_writer_enqueue(&tx_write, pending_payload) {
                            TryWriterEnqueue::Sent => {
                                in_flight.insert(request_id, ack);
                                break;
                            }
                            TryWriterEnqueue::Disconnected => {
                                terminal_transport_failure = true;
                                force_writer_abort = true;
                                rotate_after_disconnect = client.inner.proxy_mode;
                                let _ = ack.send(Err(Error::RequestNotDispatched(
                                    "API transport ended before request dispatch".into(),
                                )));
                                break 'connected;
                            }
                            TryWriterEnqueue::Full(payload) => {
                                pending_payload = payload;
                            }
                        }

                        select! {
                            biased;
                            _ = stream_shutdown.changed() => ClientEnqueueEvent::Shutdown,
                            result = &mut rx_reader_finished => ClientEnqueueEvent::ReaderFinished(
                                result.unwrap_or_else(|_| Err(
                                    "API reader task exited without reporting an outcome".into()
                                ))
                            ),
                            result = &mut rx_writer_finished => ClientEnqueueEvent::WriterFinished(
                                result.unwrap_or_else(|_| Err(
                                    "API writer task exited without reporting an outcome".into()
                                ))
                            ),
                            result = rx_read.recv_async() => ClientEnqueueEvent::Reader(result),
                            control = rx_control.recv_async() => ClientEnqueueEvent::Control(control),
                            () = time::sleep(Duration::from_millis(1)) => ClientEnqueueEvent::Retry,
                        }
                    };
                    match enqueue {
                        ClientEnqueueEvent::Retry => continue,
                        ClientEnqueueEvent::Shutdown => {
                            shutdown = true;
                            let _ = ack
                                .send(Err(Error::Connect("client stream manager stopped".into())));
                            break 'connected;
                        }
                        ClientEnqueueEvent::Reader(result) => {
                            let Ok(reader_request) = result else {
                                terminal_transport_failure = true;
                                rotate_after_disconnect = client.inner.proxy_mode;
                                let _ = ack.send(Err(Error::RequestNotDispatched(
                                    "API transport ended before request dispatch".into(),
                                )));
                                break 'connected;
                            };
                            match reader_request {
                                ClientStreamReq::StreamResponse(response) => {
                                    let proxy_refused = try_forward_response(
                                        &mut in_flight,
                                        &mut in_flight_buf,
                                        awaiting_timeout,
                                        response,
                                    )
                                    .await;
                                    if client.inner.proxy_mode && proxy_refused {
                                        proxy_handoff = true;
                                        let _ = ack.send(Err(Error::RequestNotDispatched(
                                            "API request was not dispatched before proxy handoff"
                                                .into(),
                                        )));
                                        break 'connected;
                                    }
                                }
                                ClientStreamReq::CleanupBuffer => {
                                    for (_, buffered_ack) in in_flight_buf.drain() {
                                        let _ = buffered_ack.send(Err(Error::Connect(
                                            "request timed out".to_string(),
                                        )));
                                    }
                                    awaiting_timeout = false;
                                }
                                _ => unreachable!(
                                    "API WebSocket reader emitted a non-response request"
                                ),
                            }
                            continue;
                        }
                        ClientEnqueueEvent::Control(Ok(ClientStreamControl::Leader(
                            ClientLeaderChange {
                                leader_id,
                                node,
                                ready,
                            },
                        ))) => {
                            let target = (leader_id, node.addr_api.clone());
                            if target == connected_leader {
                                if let Some(ready) = ready {
                                    let _ = ready.send(());
                                }
                                continue;
                            }
                            if !pending_leader_ready.register(target, ready) {
                                continue;
                            }
                            update_leader(&leader, Some(leader_id), Some(node)).await;
                            leader_handoff = true;
                            let _ = ack.send(Err(Error::RequestNotDispatched(
                                "API request was not dispatched before stream handoff".into(),
                            )));
                            break 'connected;
                        }
                        #[cfg(feature = "dashboard")]
                        ClientEnqueueEvent::Control(Ok(ClientStreamControl::DashboardLeader(
                            (node_id, node),
                            ready,
                        ))) => {
                            if leader_change_matches_connection(
                                &connected_leader,
                                node_id,
                                node.as_ref(),
                            ) {
                                if let Some(ready) = ready {
                                    let _ = ready.send(());
                                }
                                continue;
                            }
                            let ready_target = node_id
                                .zip(node.as_ref())
                                .map(|(node_id, node)| (node_id, node.addr_api.clone()));
                            update_leader(&leader, node_id, node).await;
                            if let (Some(target), Some(ready)) = (ready_target, ready) {
                                let _ = pending_leader_ready.register(target, Some(ready));
                            }
                            leader_handoff = true;
                            let _ = ack.send(Err(Error::RequestNotDispatched(
                                "API request was not dispatched before stream handoff".into(),
                            )));
                            break 'connected;
                        }
                        ClientEnqueueEvent::Control(Err(_)) => {
                            shutdown = true;
                            let _ = ack.send(Err(Error::Connect(
                                "client control channel closed before dispatch".into(),
                            )));
                            break 'connected;
                        }
                        ClientEnqueueEvent::ReaderFinished(outcome) => {
                            if let Err(err) = outcome {
                                error!("API WebSocket reader failed while enqueueing: {err}");
                            }
                            terminal_transport_failure = true;
                            rotate_after_disconnect = client.inner.proxy_mode;
                            let _ = ack.send(Err(Error::RequestNotDispatched(
                                "API transport ended before request dispatch".into(),
                            )));
                            break 'connected;
                        }
                        ClientEnqueueEvent::WriterFinished(outcome) => {
                            if let Err(err) = outcome {
                                error!("API WebSocket writer failed while enqueueing: {err}");
                            }
                            terminal_transport_failure = true;
                            force_writer_abort = true;
                            rotate_after_disconnect = client.inner.proxy_mode;
                            let _ = ack.send(Err(Error::RequestNotDispatched(
                                "API transport ended before request dispatch".into(),
                            )));
                            break 'connected;
                        }
                    }
                }
            }
        }

        handle_buf.abort();
        let _ = handle_buf.await;
        let mut handle_write = handle_write;
        let writer_finished = if force_writer_abort {
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

        proxy_handoff |= drain_client_reader_responses(
            &rx_read,
            &pending_reader_response,
            &mut in_flight,
            &mut in_flight_buf,
            client.inner.proxy_mode,
        )
        .await;

        if shutdown {
            fail_client_stream_shutdown(&mut in_flight, &mut in_flight_buf, &rx_req);
            debug!("Shutting down Client stream receiver");
            break;
        }

        finalize_client_stream_disconnect(
            &client.inner.nodes,
            &leader,
            &mut proxy_index,
            &mut in_flight,
            &mut in_flight_buf,
            ClientDisconnectState {
                proxy_handoff,
                leader_handoff,
                terminal_transport_failure,
                rotate_after_disconnect,
            },
        )
        .await;

        debug!("client stream tasks killed - re-connecting now");
    }
}

fn fail_client_stream_shutdown(
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    rx_req: &flume::Receiver<ClientStreamReq>,
) {
    for (_, ack) in in_flight.drain().chain(in_flight_buf.drain()) {
        let _ = ack.send(Err(Error::Connect("client stream manager stopped".into())));
    }
    while let Ok(request) = rx_req.try_recv() {
        request.fail_on_shutdown();
    }
}

fn fail_client_stream_leader_handoff(
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
) {
    for (_, ack) in in_flight.drain().chain(in_flight_buf.drain()) {
        let _ = ack.send(Err(Error::LeaderChange(
            "Action not allowed, Raft leader has changed".into(),
        )));
    }
}

fn fail_client_stream_proxy_handoff(
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
) {
    for (_, ack) in in_flight.drain().chain(in_flight_buf.drain()) {
        let _ = ack.send(Err(Error::LeaderChange(
            "Action not allowed, proxy endpoint has changed".into(),
        )));
    }
}

async fn drain_client_reader_responses(
    rx_read: &flume::Receiver<ClientStreamReq>,
    pending_reader_response: &Mutex<Option<ApiStreamResponse>>,
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    proxy_mode: bool,
) -> bool {
    let mut proxy_handoff = false;
    debug!("make sure reader rx is empty and closed");
    while let Ok(req) = rx_read.recv_async().await {
        debug!("Answer from reader into buffer: {:?}", req);
        match req {
            ClientStreamReq::StreamResponse(response) => {
                let proxy_refused =
                    try_forward_response(in_flight, in_flight_buf, false, response).await;
                proxy_handoff |= proxy_mode && proxy_refused;
            }
            ClientStreamReq::CleanupBuffer => {
                // Ignore the expiry marker while reconnecting.
            }
            _ => unreachable!("API WebSocket reader emitted a non-response request"),
        }
    }
    let pending_response = {
        pending_reader_response
            .lock()
            .expect("client reader response mutex poisoned")
            .take()
    };
    if let Some(response) = pending_response {
        debug!("Answer retained by reader during teardown: {:?}", response);
        let proxy_refused = try_forward_response(in_flight, in_flight_buf, false, response).await;
        proxy_handoff |= proxy_mode && proxy_refused;
    }
    proxy_handoff
}

#[derive(Debug, Clone, Copy)]
struct ClientDisconnectState {
    proxy_handoff: bool,
    leader_handoff: bool,
    terminal_transport_failure: bool,
    rotate_after_disconnect: bool,
}

async fn finalize_client_stream_disconnect(
    nodes: &[String],
    leader: &Arc<RwLock<(NodeId, String)>>,
    proxy_index: &mut usize,
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    state: ClientDisconnectState,
) {
    let rotate_proxy = if state.proxy_handoff {
        fail_client_stream_proxy_handoff(in_flight, in_flight_buf);
        true
    } else if state.leader_handoff {
        fail_client_stream_leader_handoff(in_flight, in_flight_buf);
        false
    } else if state.terminal_transport_failure {
        for (_, ack) in in_flight.drain().chain(in_flight_buf.drain()) {
            let _ = ack.send(Err(Error::Connect(
                "API connection ended after dispatch; outcome unknown and request was not replayed"
                    .into(),
            )));
        }
        state.rotate_after_disconnect
    } else if state.rotate_after_disconnect {
        for (_, ack) in in_flight.drain().chain(in_flight_buf.drain()) {
            let _ = ack.send(Err(Error::Connect(
                "Connection to proxy endpoint lost".into(),
            )));
        }
        true
    } else {
        for (request_id, ack) in in_flight.drain() {
            in_flight_buf.insert(request_id, ack);
        }
        false
    };

    if rotate_proxy {
        rotate_configured_proxy_endpoint(nodes, leader, proxy_index).await;
    }
    assert!(in_flight.is_empty());
}

#[inline(always)]
fn api_response_is_forward_to_leader(payload: &ApiStreamResponsePayload) -> bool {
    match payload {
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::Execute(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::ExecuteReturning(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::Transaction(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::Query(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::QueryConsistent(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::Batch(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "sqlite")]
        ApiStreamResponsePayload::Migrate(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "backup")]
        ApiStreamResponsePayload::Backup(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "cache")]
        ApiStreamResponsePayload::KV(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
        #[cfg(feature = "dlock")]
        ApiStreamResponsePayload::Lock(_) => false,
        #[cfg(feature = "listen_notify_local")]
        ApiStreamResponsePayload::Notify(result) => result
            .as_ref()
            .is_err_and(|err| err.is_forward_to_leader().is_some()),
    }
}

#[inline(always)]
async fn try_forward_response(
    in_flight: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    in_flight_buf: &mut HashMap<usize, Sender<Result<ApiStreamResponsePayload, Error>>>,
    awaiting_timeout: bool,
    response: ApiStreamResponse,
) -> bool {
    let proxy_refused = api_response_is_forward_to_leader(&response.result);
    let recognized = match in_flight.remove(&response.request_id) {
        None => {
            if awaiting_timeout {
                match in_flight_buf.remove(&response.request_id) {
                    None => {
                        error!("client ack for ApiStreamResponse missing");
                        false
                    }
                    Some(ack) => {
                        if ack.send(Ok(response.result)).is_err() {
                            error!(
                                request_id = response.request_id,
                                "client acknowledgement receiver dropped"
                            );
                        } else {
                            debug!("ApiStreamResponse sent to client from in_flight_buf");
                        }
                        true
                    }
                }
            } else {
                error!("client ack for ApiStreamResponse missing");
                false
            }
        }

        Some(ack) => {
            if ack.send(Ok(response.result)).is_err() {
                error!(
                    request_id = response.request_id,
                    "client acknowledgement receiver dropped"
                );
            } else {
                debug!("ApiStreamResponse sent to client");
            }
            true
        }
    };

    // A refusal belongs to this socket when its request id was recognized.
    // Caller cancellation can make acknowledgement delivery fail, but it must
    // not leave the refusing proxy selected for later requests.
    proxy_refused && recognized
}

async fn update_leader(
    leader: &Arc<RwLock<(NodeId, String)>>,
    node_id: Option<u64>,
    node: Option<Node>,
) {
    if let Some(leader_id) = node_id
        && let Some(node) = node
    {
        let api_addr = node.addr_api.clone();
        info!(
            "API Client received a Leader Change: {} / {}",
            leader_id, api_addr
        );
        {
            let mut lock = leader.write().await;
            *lock = (leader_id, api_addr);
        }
    }
}

fn next_configured_proxy(nodes: &[String], proxy_index: &mut usize) -> Option<String> {
    if nodes.is_empty() {
        return None;
    }
    *proxy_index = (*proxy_index + 1) % nodes.len();
    Some(nodes[*proxy_index].clone())
}

async fn rotate_proxy_endpoint(
    client: &Client,
    leader: &Arc<RwLock<(NodeId, String)>>,
    proxy_index: &mut usize,
) {
    debug_assert!(client.inner.proxy_mode);
    rotate_configured_proxy_endpoint(&client.inner.nodes, leader, proxy_index).await;
}

async fn rotate_configured_proxy_endpoint(
    nodes: &[String],
    leader: &Arc<RwLock<(NodeId, String)>>,
    proxy_index: &mut usize,
) {
    let mut lock = leader.write().await;
    if let Some(endpoint) = next_configured_proxy(nodes, proxy_index) {
        // Preserve this stream's synthetic/current id and replace only the
        // address with a member of the original configured trust boundary.
        let node_id = lock.0;
        *lock = (node_id, endpoint);
    }
}

fn cleanup_buffer_timeout(tx: flume::Sender<ClientStreamReq>, seconds: u64) -> JoinHandle<()> {
    task::spawn(async move {
        time::sleep(Duration::from_secs(seconds)).await;
        let _ = tx.send_async(ClientStreamReq::CleanupBuffer).await;
    })
}

async fn stream_reader<S>(
    mut read: FragmentCollectorRead<ReadHalf<S>>,
    tx: flume::Sender<ClientStreamReq>,
    pending_response: Arc<Mutex<Option<ApiStreamResponse>>>,
) -> Result<(), String>
where
    S: AsyncRead + Unpin,
{
    loop {
        let frame = read
            .read_frame(&mut |frame| async move {
                // TODO obligated sends should be auto ping / pong / close ? -> verify!
                debug!(
                    "Received obligated send in stream client: OpCode: {:?}: {:?}",
                    frame.opcode.clone(),
                    frame.payload
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
                let payload = deserialize::<ApiStreamResponse>(bytes)
                    .map_err(|err| format!("invalid API stream response: {err}"))?;
                send_client_reader_response(&tx, &pending_response, payload).await?;
            }
            OpCode::Close => break,
            OpCode::Ping => {}
            OpCode::Pong => {}
        }
    }

    debug!("Exiting Client Stream Reader");
    Ok(())
}

async fn send_client_reader_response(
    tx: &flume::Sender<ClientStreamReq>,
    pending_response: &Mutex<Option<ApiStreamResponse>>,
    response: ApiStreamResponse,
) -> Result<(), String> {
    *pending_response
        .lock()
        .expect("client reader response mutex poisoned") = Some(response);

    loop {
        let response = pending_response
            .lock()
            .expect("client reader response mutex poisoned")
            .take()
            .expect("pending client reader response missing");
        match tx.try_send(ClientStreamReq::StreamResponse(response)) {
            Ok(()) => return Ok(()),
            Err(flume::TrySendError::Full(ClientStreamReq::StreamResponse(response))) => {
                *pending_response
                    .lock()
                    .expect("client reader response mutex poisoned") = Some(response);
                task::yield_now().await;
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                return Err("API reader outcome channel closed".to_owned());
            }
            Err(flume::TrySendError::Full(_)) => {
                unreachable!("client reader emitted a non-response request")
            }
        }
    }
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
                if let Err(err) = write_api_request_frame(&mut write, bytes).await {
                    error!("Client Stream error: {:?}", err);
                    break Err(err.to_string());
                }
            }
            WritePayload::Close => {
                debug!("Received Close request in Client Stream Writer");
                let _ = write_close_frame_flushed(&mut write, Frame::close(1000, b"go away")).await;
                break Ok(());
            }
        }
    };

    let _ = finished.send(outcome);
    debug!("Exiting Client Stream Writer");
}

async fn write_api_request_frame<S>(
    write: &mut WebSocketWrite<S>,
    bytes: Vec<u8>,
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: AsyncWrite + Unpin,
{
    write_frame_flushed(write, Frame::binary(Payload::from(bytes))).await
}

async fn try_connect(
    leader: &Arc<RwLock<(NodeId, String)>>,
    raft_type: &RaftType,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    secret: &[u8],
) -> Result<(WebSocket<TokioIo<Upgraded>>, (NodeId, String)), Error> {
    let (node_id, addr) = {
        let lock = leader.read().await;
        (lock.0, lock.1.clone())
    };
    let socket =
        web_socket_connect::try_connect(node_id, &addr, raft_type, tls_config, secret).await?;
    Ok((socket, (node_id, addr)))
}

#[cfg(any(feature = "dashboard", test))]
fn leader_change_matches_connection(
    connected: &(NodeId, String),
    node_id: Option<NodeId>,
    node: Option<&Node>,
) -> bool {
    node_id == Some(connected.0) && node.is_some_and(|node| node.addr_api == connected.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::Role;
    use std::io::{self, Write};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().expect("captured log lock").clone())
                .expect("captured logs are UTF-8")
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("captured log lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "sqlite")]
    fn forward_to_leader_error() -> Error {
        use openraft::error::{CheckIsLeaderError, ForwardToLeader, RaftError};

        Error::CheckIsLeaderError(Box::new(RaftError::APIError(
            CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_id: None,
                leader_node: None,
            }),
        )))
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_api_request_writer_flushes_serialized_request_through_tls() {
        let request = ApiStreamRequest {
            request_id: 17,
            payload: ApiStreamRequestPayload::Query(Query {
                sql: "SELECT 1".into(),
                params: Vec::new(),
            }),
        };
        let bytes = serialize_network(&request);
        crate::network::frame_io::tests::exercise_gated_tls_writer(
            Role::Client,
            bytes,
            true,
            |mut write, bytes| async move { write_api_request_frame(&mut write, bytes).await },
        )
        .await;
    }

    #[tokio::test]
    async fn production_api_writer_reports_flush_failure_to_its_supervisor() {
        let write = crate::network::frame_io::tests::split_writer(
            crate::network::frame_io::tests::TestIo::failing_flush(),
        );
        let (tx, rx) = flume::bounded(1);
        let (finished, outcome) = oneshot::channel();
        tx.send_async(WritePayload::Payload(b"API request".to_vec()))
            .await
            .expect("queue API request");

        stream_writer(write, rx, finished).await;

        let error = outcome
            .await
            .expect("writer terminal outcome")
            .expect_err("flush failure must terminate the writer");
        assert!(error.contains("injected flush failure"));
    }

    #[test]
    fn full_writer_queue_retains_mutation_until_ownership_transfer_is_certain() {
        let (tx, rx) = flume::bounded(1);
        tx.try_send(WritePayload::Close).expect("fill writer queue");
        let pending = match try_writer_enqueue(
            &tx,
            WritePayload::Payload(b"non-idempotent mutation".to_vec()),
        ) {
            TryWriterEnqueue::Full(payload) => payload,
            _ => panic!("full queue must return ownership to the manager"),
        };

        assert!(matches!(rx.try_recv(), Ok(WritePayload::Close)));
        assert!(matches!(
            try_writer_enqueue(&tx, pending),
            TryWriterEnqueue::Sent
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(WritePayload::Payload(bytes)) if bytes == b"non-idempotent mutation"
        ));
    }

    #[tokio::test]
    async fn queued_leader_change_wins_before_queued_api_request() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (_reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (leader_tx, leader_rx) = flume::bounded(1);
        let (_socket_reader_tx, socket_reader_rx) = flume::bounded(1);
        let (request_tx, request_rx) = flume::bounded(1);
        leader_tx
            .send_async(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                node: Node {
                    id: 8,
                    addr_raft: "node-eight:21000".into(),
                    addr_api: "node-eight:21001".into(),
                },
                ready: None,
            }))
            .await
            .expect("queue leader change");
        request_tx
            .send_async(ClientStreamReq::Shutdown)
            .await
            .expect("queue API request");

        let event = next_client_connected_event(
            &mut shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &leader_rx,
            &socket_reader_rx,
            &request_rx,
        )
        .await;

        assert!(matches!(
            event,
            ClientConnectedEvent::Control(Ok(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                ..
            })))
        ));
        assert!(matches!(
            request_rx.try_recv(),
            Ok(ClientStreamReq::Shutdown)
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn queued_api_response_wins_before_queued_leader_change() {
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (_reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (leader_tx, leader_rx) = flume::bounded(1);
        let (socket_reader_tx, socket_reader_rx) = flume::bounded(1);
        let (_request_tx, request_rx) = flume::bounded(1);
        socket_reader_tx
            .send_async(ClientStreamReq::StreamResponse(ApiStreamResponse {
                request_id: 41,
                result: ApiStreamResponsePayload::Execute(Ok(1)),
            }))
            .await
            .expect("queue decoded API response");
        leader_tx
            .send_async(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                node: Node {
                    id: 8,
                    addr_raft: "node-eight:21000".into(),
                    addr_api: "node-eight:21001".into(),
                },
                ready: None,
            }))
            .await
            .expect("queue leader change");

        let event = next_client_connected_event(
            &mut shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &leader_rx,
            &socket_reader_rx,
            &request_rx,
        )
        .await;

        assert!(matches!(
            event,
            ClientConnectedEvent::Incoming(Ok(ClientStreamReq::StreamResponse(
                ApiStreamResponse { request_id: 41, .. }
            )))
        ));
        assert!(matches!(
            leader_rx.try_recv(),
            Ok(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                ..
            }))
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn queued_api_response_wins_before_leader_during_writer_backpressure() {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (_reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (leader_tx, leader_rx) = flume::bounded(1);
        let (socket_reader_tx, socket_reader_rx) = flume::bounded(1);
        socket_reader_tx
            .send_async(ClientStreamReq::StreamResponse(ApiStreamResponse {
                request_id: 42,
                result: ApiStreamResponsePayload::Execute(Ok(1)),
            }))
            .await
            .expect("queue decoded API response");
        leader_tx
            .send_async(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 9,
                node: Node {
                    id: 9,
                    addr_raft: "node-nine:21000".into(),
                    addr_api: "node-nine:21001".into(),
                },
                ready: None,
            }))
            .await
            .expect("queue leader change");

        let event = latched_client_enqueue_event(
            &shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &socket_reader_rx,
            &leader_rx,
        )
        .expect("a queued response must wake backpressured admission");

        assert!(matches!(
            event,
            ClientEnqueueEvent::Reader(Ok(ClientStreamReq::StreamResponse(ApiStreamResponse {
                request_id: 42,
                ..
            })))
        ));
        assert!(matches!(
            leader_rx.try_recv(),
            Ok(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 9,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn dedicated_leader_control_bypasses_application_backlog_during_writer_backpressure() {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (_reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (control_tx, control_rx) = flume::bounded(1);
        let (_socket_reader_tx, socket_reader_rx) = flume::bounded(1);
        let (request_tx, request_rx) = flume::bounded(1);
        request_tx
            .send_async(ClientStreamReq::Shutdown)
            .await
            .expect("fill application queue");
        control_tx
            .send_async(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                node: Node {
                    id: 8,
                    addr_raft: "node-eight:21000".into(),
                    addr_api: "node-eight:21001".into(),
                },
                ready: None,
            }))
            .await
            .expect("queue priority leader handoff");

        let (writer_tx, writer_rx) = flume::bounded(1);
        writer_tx
            .try_send(WritePayload::Close)
            .expect("fill writer queue");
        let pending_payload = WritePayload::Payload(b"pending mutation".to_vec());

        let event = latched_client_enqueue_event(
            &shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &socket_reader_rx,
            &control_rx,
        )
        .expect("queued leader handoff must wake backpressured admission");

        assert!(matches!(
            event,
            ClientEnqueueEvent::Control(Ok(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id: 8,
                ..
            })))
        ));
        assert!(matches!(
            request_rx.try_recv(),
            Ok(ClientStreamReq::Shutdown)
        ));
        assert!(matches!(writer_rx.try_recv(), Ok(WritePayload::Close)));
        assert!(matches!(
            pending_payload,
            WritePayload::Payload(bytes) if bytes == b"pending mutation"
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn leader_handoff_settles_decoded_response_before_failing_unresolved_requests() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let (settled_tx, settled_rx) = oneshot::channel();
        let (unresolved_tx, unresolved_rx) = oneshot::channel();
        in_flight.insert(51, settled_tx);
        in_flight.insert(52, unresolved_tx);

        assert!(
            !try_forward_response(
                &mut in_flight,
                &mut in_flight_buf,
                false,
                ApiStreamResponse {
                    request_id: 51,
                    result: ApiStreamResponsePayload::Execute(Ok(1)),
                },
            )
            .await
        );
        fail_client_stream_leader_handoff(&mut in_flight, &mut in_flight_buf);

        assert!(matches!(
            settled_rx.await.expect("settled request acknowledgement"),
            Ok(ApiStreamResponsePayload::Execute(Ok(1)))
        ));
        assert!(matches!(
            unresolved_rx
                .await
                .expect("unresolved request acknowledgement"),
            Err(Error::LeaderChange(_))
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn proxy_handoff_settles_decoded_response_before_failing_unresolved_requests() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let (settled_tx, settled_rx) = oneshot::channel();
        let (unresolved_tx, unresolved_rx) = oneshot::channel();
        in_flight.insert(61, settled_tx);
        in_flight.insert(62, unresolved_tx);

        assert!(
            !try_forward_response(
                &mut in_flight,
                &mut in_flight_buf,
                false,
                ApiStreamResponse {
                    request_id: 61,
                    result: ApiStreamResponsePayload::Execute(Ok(1)),
                },
            )
            .await
        );
        fail_client_stream_proxy_handoff(&mut in_flight, &mut in_flight_buf);

        assert!(matches!(
            settled_rx.await.expect("settled request acknowledgement"),
            Ok(ApiStreamResponsePayload::Execute(Ok(1)))
        ));
        assert!(matches!(
            unresolved_rx
                .await
                .expect("unresolved request acknowledgement"),
            Err(Error::LeaderChange(_))
        ));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn dropped_caller_still_allows_its_proxy_refusal_to_claim_handoff() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let (ack, dropped_response) = oneshot::channel();
        drop(dropped_response);
        in_flight.insert(63, ack);

        assert!(
            try_forward_response(
                &mut in_flight,
                &mut in_flight_buf,
                false,
                ApiStreamResponse {
                    request_id: 63,
                    result: ApiStreamResponsePayload::Execute(Err(forward_to_leader_error())),
                },
            )
            .await,
            "the current socket owns recovery even after caller cancellation"
        );
        assert!(in_flight.is_empty());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_client_acknowledgements_never_log_response_payloads() {
        use crate::query::rows::{ColumnOwned, RowOwned, ValueOwned};

        const SECRET: &str = "sentinel-service-credential-never-log";

        let sensitive_payload = || {
            ApiStreamResponsePayload::QueryConsistent(Ok(vec![RowOwned {
                columns: vec![ColumnOwned {
                    name: "value".to_owned(),
                    value: ValueOwned::Text(SECRET.to_owned()),
                }],
            }]))
        };
        assert!(
            format!("{:?}", sensitive_payload()).contains(SECRET),
            "the fixture must prove that debug-formatting the payload would disclose it"
        );

        let captured = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .without_time()
            .with_ansi(false)
            .with_writer(captured.clone())
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);

        for (request_id, buffered) in [(81, false), (82, true)] {
            let mut in_flight = HashMap::new();
            let mut in_flight_buf = HashMap::new();
            let (ack, caller) = oneshot::channel();
            drop(caller);
            if buffered {
                in_flight_buf.insert(request_id, ack);
            } else {
                in_flight.insert(request_id, ack);
            }

            assert!(
                !try_forward_response(
                    &mut in_flight,
                    &mut in_flight_buf,
                    buffered,
                    ApiStreamResponse {
                        request_id,
                        result: sensitive_payload(),
                    },
                )
                .await
            );
        }

        let output = captured.contents();
        assert_eq!(
            output.matches("client acknowledgement receiver dropped").count(),
            2,
            "both cancellation paths must retain a payload-free diagnostic: {output}"
        );
        assert!(
            !output.contains(SECRET),
            "cancelled acknowledgement disclosed its response payload: {output}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn concurrent_proxy_refusals_coalesce_into_one_endpoint_advance() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let mut acknowledgements = Vec::new();
        let (reader_tx, reader_rx) = flume::bounded(1);
        let pending_reader_response = Arc::new(Mutex::new(None));
        for request_id in [71, 72] {
            let (ack, response) = oneshot::channel();
            in_flight.insert(request_id, ack);
            acknowledgements.push(response);
        }
        let reader_pending = pending_reader_response.clone();
        let mut reader = tokio::spawn(async move {
            for request_id in [71, 72] {
                send_client_reader_response(
                    &reader_tx,
                    &reader_pending,
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Execute(Err(forward_to_leader_error())),
                    },
                )
                .await
                .expect("retain refusing response");
            }
        });

        time::timeout(Duration::from_secs(1), async {
            loop {
                if pending_reader_response
                    .lock()
                    .expect("client reader response mutex poisoned")
                    .is_some()
                {
                    break;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect("second response must block behind the one-slot queue");
        reader.abort();
        let _ = (&mut reader).await;

        let proxy_handoff = drain_client_reader_responses(
            &reader_rx,
            &pending_reader_response,
            &mut in_flight,
            &mut in_flight_buf,
            true,
        )
        .await;

        let nodes = vec!["proxy-a:21000".to_owned(), "proxy-b:21000".to_owned()];
        let leader = Arc::new(RwLock::new((0, nodes[0].clone())));
        let mut proxy_index = 0;
        finalize_client_stream_disconnect(
            &nodes,
            &leader,
            &mut proxy_index,
            &mut in_flight,
            &mut in_flight_buf,
            ClientDisconnectState {
                proxy_handoff,
                leader_handoff: false,
                terminal_transport_failure: false,
                rotate_after_disconnect: false,
            },
        )
        .await;

        for response in acknowledgements {
            let payload = response.await.expect("proxy refusal acknowledgement");
            assert!(matches!(
                payload,
                Ok(ApiStreamResponsePayload::Execute(Err(ref err)))
                    if err.is_forward_to_leader().is_some()
            ));
        }
        assert!(proxy_handoff, "the refusing socket must claim one handoff");
        assert_eq!(leader.read().await.1, "proxy-b:21000");
        assert_eq!(proxy_index, 1, "two refusals must rotate only once");
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn teardown_drained_proxy_refusal_claims_the_socket_handoff() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let (ack, response) = oneshot::channel();
        in_flight.insert(81, ack);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (_control_tx, control_rx) = flume::bounded(1);
        let (reader_tx, reader_rx) = flume::bounded(1);
        let pending_reader_response = Mutex::new(None);
        let (_request_tx, request_rx) = flume::bounded(1);
        reader_tx
            .send_async(ClientStreamReq::StreamResponse(ApiStreamResponse {
                request_id: 81,
                result: ApiStreamResponsePayload::Execute(Err(forward_to_leader_error())),
            }))
            .await
            .expect("queue decoded refusal");
        drop(reader_tx);
        reader_finished_tx
            .send(Ok(()))
            .expect("queue reader EOF outcome");

        let terminal = next_client_connected_event(
            &mut shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &control_rx,
            &reader_rx,
            &request_rx,
        )
        .await;
        assert!(matches!(
            terminal,
            ClientConnectedEvent::ReaderFinished(Ok(()))
        ));

        // The same production teardown seam drains the response that lost the
        // biased race to EOF, then lets its exact-socket refusal supersede the
        // generic terminal rotation.
        let proxy_handoff = drain_client_reader_responses(
            &reader_rx,
            &pending_reader_response,
            &mut in_flight,
            &mut in_flight_buf,
            true,
        )
        .await;
        let nodes = vec!["proxy-a:21000".to_owned(), "proxy-b:21000".to_owned()];
        let leader = Arc::new(RwLock::new((0, nodes[0].clone())));
        let mut proxy_index = 0;
        finalize_client_stream_disconnect(
            &nodes,
            &leader,
            &mut proxy_index,
            &mut in_flight,
            &mut in_flight_buf,
            ClientDisconnectState {
                proxy_handoff,
                leader_handoff: false,
                terminal_transport_failure: true,
                rotate_after_disconnect: true,
            },
        )
        .await;

        assert!(proxy_handoff);
        assert!(matches!(
            response.await.expect("drained refusal acknowledgement"),
            Ok(ApiStreamResponsePayload::Execute(Err(ref err)))
                if err.is_forward_to_leader().is_some()
        ));
        assert_eq!(leader.read().await.1, "proxy-b:21000");
        assert_eq!(
            proxy_index, 1,
            "response-owned and EOF recovery must rotate only once"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn teardown_drained_non_proxy_refusal_preserves_terminal_ambiguity() {
        let mut in_flight = HashMap::new();
        let mut in_flight_buf = HashMap::new();
        let (settled_ack, settled_response) = oneshot::channel();
        let (unresolved_ack, unresolved_response) = oneshot::channel();
        in_flight.insert(91, settled_ack);
        in_flight.insert(92, unresolved_ack);
        let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (reader_finished_tx, mut reader_finished_rx) = oneshot::channel();
        let (_writer_finished_tx, mut writer_finished_rx) = oneshot::channel();
        let (_control_tx, control_rx) = flume::bounded(1);
        let (reader_tx, reader_rx) = flume::bounded(1);
        let pending_reader_response = Mutex::new(None);
        let (_request_tx, request_rx) = flume::bounded(1);
        reader_tx
            .send_async(ClientStreamReq::StreamResponse(ApiStreamResponse {
                request_id: 91,
                result: ApiStreamResponsePayload::Execute(Err(forward_to_leader_error())),
            }))
            .await
            .expect("queue decoded leader refusal");
        drop(reader_tx);
        reader_finished_tx
            .send(Ok(()))
            .expect("queue reader EOF outcome");

        let terminal = next_client_connected_event(
            &mut shutdown_rx,
            &mut reader_finished_rx,
            &mut writer_finished_rx,
            &control_rx,
            &reader_rx,
            &request_rx,
        )
        .await;
        assert!(matches!(
            terminal,
            ClientConnectedEvent::ReaderFinished(Ok(()))
        ));

        let proxy_handoff = drain_client_reader_responses(
            &reader_rx,
            &pending_reader_response,
            &mut in_flight,
            &mut in_flight_buf,
            false,
        )
        .await;
        let nodes = vec!["node-a:21000".to_owned(), "node-b:21000".to_owned()];
        let leader = Arc::new(RwLock::new((1, nodes[0].clone())));
        let mut proxy_index = 0;
        finalize_client_stream_disconnect(
            &nodes,
            &leader,
            &mut proxy_index,
            &mut in_flight,
            &mut in_flight_buf,
            ClientDisconnectState {
                proxy_handoff,
                leader_handoff: false,
                terminal_transport_failure: true,
                rotate_after_disconnect: false,
            },
        )
        .await;

        assert!(!proxy_handoff, "ordinary clients do not own proxy recovery");
        assert!(matches!(
            settled_response.await.expect("drained refusal acknowledgement"),
            Ok(ApiStreamResponsePayload::Execute(Err(ref err)))
                if err.is_forward_to_leader().is_some()
        ));
        assert!(matches!(
            unresolved_response
                .await
                .expect("unresolved acknowledgement"),
            Err(Error::Connect(ref message)) if message.contains("outcome unknown")
        ));
        assert_eq!(leader.read().await.1, "node-a:21000");
        assert_eq!(proxy_index, 0);
    }

    #[tokio::test]
    async fn production_api_reader_reports_malformed_frames() {
        let (client_io, server_io) = tokio::io::duplex(4 * 1024);
        let client = WebSocket::after_handshake(client_io, Role::Client);
        let (read, _write) = client.split(tokio::io::split);
        let read = FragmentCollectorRead::new(read);
        let (tx, _rx) = flume::bounded(1);
        let pending_reader_response = Arc::new(Mutex::new(None));
        let (finished, result) = oneshot::channel();
        tokio::spawn(async move {
            let _ = finished.send(stream_reader(read, tx, pending_reader_response).await);
        });
        let mut server = WebSocket::after_handshake(server_io, Role::Server);

        crate::network::frame_io::write_socket_frame_flushed(
            &mut server,
            Frame::binary(Payload::Borrowed(b"not an API response")),
        )
        .await
        .expect("send malformed server frame");

        let outcome = tokio::time::timeout(Duration::from_secs(1), result)
            .await
            .expect("reader must report promptly")
            .expect("reader task must report an outcome");
        assert!(matches!(outcome, Err(ref err) if err.contains("invalid API stream response")));
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_near_deadline_shares_the_connecting_target() {
        let target = (9, "node-nine:21000".to_owned());
        let mut pending = PendingLeaderReady::default();
        let (first_tx, first_rx) = oneshot::channel();
        pending.register(target.clone(), Some(first_tx));

        time::advance(Duration::from_millis(4_900)).await;
        let (duplicate_tx, duplicate_rx) = oneshot::channel();
        assert!(!leader_handoff_restarts_connection(&target, &target));
        pending.register(target.clone(), Some(duplicate_tx));

        assert!(pending.acknowledge(&target));
        first_rx.await.expect("first same-target waiter");
        duplicate_rx.await.expect("duplicate same-target waiter");
    }

    #[tokio::test]
    async fn a_new_target_fails_superseded_waiters_and_acks_only_itself() {
        let target_b = (2, "node-two:21000".to_owned());
        let target_c = (3, "node-three:21000".to_owned());
        let mut pending = PendingLeaderReady::default();
        let (b_tx, b_rx) = oneshot::channel();
        pending.register(target_b.clone(), Some(b_tx));
        let (c_tx, c_rx) = oneshot::channel();
        assert!(leader_handoff_restarts_connection(&target_b, &target_c));
        pending.register(target_c.clone(), Some(c_tx));

        assert!(b_rx.await.is_err(), "B must fail when C supersedes it");
        assert!(!pending.acknowledge(&target_b));
        assert!(pending.acknowledge(&target_c));
        c_rx.await.expect("C waiter");
    }

    #[tokio::test]
    async fn failed_target_yields_to_the_leader_selected_by_discovery() {
        let target_c = (3, "node-three:21000".to_owned());
        let discovered_b = (2, "node-two:21000".to_owned());
        let mut pending = PendingLeaderReady::default();
        let (c_tx, c_rx) = oneshot::channel();
        pending.register(target_c, Some(c_tx));

        pending.resolve_connected_target(&discovered_b);
        assert!(
            pending.target.is_none(),
            "obsolete C must not make the manager reject B forever"
        );
        assert!(c_rx.await.is_err(), "C handoff must fail on discovered B");
        assert!(!pending.acknowledge(&discovered_b));
    }

    #[tokio::test(start_paused = true)]
    async fn a_handoff_expired_before_receipt_cannot_redirect_the_manager() {
        let stale_target = (3, "node-three:21000".to_owned());
        let mut pending = PendingLeaderReady::default();
        let (stale_tx, stale_rx) = oneshot::channel();

        time::advance(crate::LEADER_STREAM_HANDOFF_TIMEOUT).await;
        drop(stale_rx);
        assert!(!pending.register(stale_target, Some(stale_tx)));
        assert!(pending.target.is_none());
        assert!(pending.waiters.is_empty());
    }

    #[test]
    fn proxy_failover_cycles_only_through_configured_endpoints() {
        let nodes = vec![
            "proxy-a".to_owned(),
            "proxy-b".to_owned(),
            "proxy-c".to_owned(),
        ];
        let mut proxy_index = 0;
        assert_eq!(
            next_configured_proxy(&nodes, &mut proxy_index).as_deref(),
            Some("proxy-b")
        );
        assert_eq!(
            next_configured_proxy(&nodes, &mut proxy_index).as_deref(),
            Some("proxy-c")
        );
        assert_eq!(
            next_configured_proxy(&nodes, &mut proxy_index).as_deref(),
            Some("proxy-a")
        );

        let duplicates = vec![
            "proxy-a".to_owned(),
            "proxy-a".to_owned(),
            "proxy-b".to_owned(),
        ];
        let mut duplicate_index = 0;
        assert_eq!(
            next_configured_proxy(&duplicates, &mut duplicate_index).as_deref(),
            Some("proxy-a")
        );
        assert_eq!(
            next_configured_proxy(&duplicates, &mut duplicate_index).as_deref(),
            Some("proxy-b")
        );

        let mut singleton_index = 0;
        assert_eq!(
            next_configured_proxy(&["only-proxy".to_owned()], &mut singleton_index).as_deref(),
            Some("only-proxy")
        );
        let mut empty_index = 0;
        assert_eq!(next_configured_proxy(&[], &mut empty_index), None);
    }

    #[test]
    fn stale_recovery_for_the_connected_leader_does_not_require_reconnect() {
        let connected = (7, "node-seven:21000".to_owned());
        let same = Node {
            id: 7,
            addr_raft: "node-seven:21001".to_owned(),
            addr_api: "node-seven:21000".to_owned(),
        };
        assert!(leader_change_matches_connection(
            &connected,
            Some(7),
            Some(&same)
        ));

        let replacement = Node {
            id: 8,
            addr_raft: "node-eight:21001".to_owned(),
            addr_api: "node-eight:21000".to_owned(),
        };
        assert!(!leader_change_matches_connection(
            &connected,
            Some(8),
            Some(&replacement)
        ));
    }

    #[test]
    fn a_discovered_leader_reconnects_without_the_failure_backoff() {
        let previous = (1, "node-1".to_owned());
        let replacement = (2, "node-2".to_owned());

        assert_eq!(reconnect_delay(&previous, &replacement), Duration::ZERO);
        assert_eq!(
            reconnect_delay(&replacement, &replacement),
            CLIENT_STREAM_RETRY_DELAY
        );
    }
}
