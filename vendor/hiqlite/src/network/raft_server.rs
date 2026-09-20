use crate::network::frame_io::{
    CLOSE_WRITE_TIMEOUT, write_close_frame_flushed, write_frame_flushed,
    write_socket_close_frame_flushed,
};
use crate::network::handshake::HandshakeSecret;
use crate::network::{AppStateExt, Error, serialize_network};
use axum::response::IntoResponse;
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, upgrade};
use openraft::error::{Fatal, InstallSnapshotError, RaftError};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task;
use tokio::time;
use tracing::{debug, error, warn};

#[cfg(feature = "cache")]
use crate::app_state::RaftType;
#[cfg(feature = "cache")]
use crate::helpers;
#[cfg(feature = "cache")]
use crate::store::state_machine::memory::TypeConfigKV;
#[cfg(feature = "cache")]
use std::collections::BTreeSet;

#[cfg(feature = "sqlite")]
use crate::store::state_machine::sqlite::TypeConfigSqlite;

use crate::helpers::deserialize;
#[cfg(any(feature = "cache", feature = "sqlite"))]
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftStreamRequest {
    #[cfg(feature = "sqlite")]
    AppendDB((usize, AppendEntriesRequest<TypeConfigSqlite>)),
    #[cfg(feature = "sqlite")]
    VoteDB((usize, VoteRequest<u64>)),
    #[cfg(feature = "sqlite")]
    SnapshotDB((usize, InstallSnapshotRequest<TypeConfigSqlite>)),

    #[cfg(feature = "cache")]
    AppendCache((usize, AppendEntriesRequest<TypeConfigKV>)),
    #[cfg(feature = "cache")]
    VoteCache((usize, VoteRequest<u64>)),
    #[cfg(feature = "cache")]
    SnapshotCache((usize, InstallSnapshotRequest<TypeConfigKV>)),
    #[cfg(feature = "cache")]
    RemoveMembershipCache(u64),
}

impl From<&[u8]> for RaftStreamRequest {
    #[inline]
    fn from(value: &[u8]) -> Self {
        deserialize(value).unwrap()
    }
}

impl From<Vec<u8>> for RaftStreamRequest {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        deserialize(&value).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaftStreamResponse {
    pub request_id: usize,
    pub payload: RaftStreamResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum RaftStreamResponsePayload {
    #[cfg(feature = "sqlite")]
    AppendDB(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    #[cfg(feature = "sqlite")]
    VoteDB(Result<VoteResponse<u64>, RaftError<u64>>),
    #[cfg(feature = "sqlite")]
    SnapshotDB(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),

    #[cfg(feature = "cache")]
    AppendCache(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    #[cfg(feature = "cache")]
    VoteCache(Result<VoteResponse<u64>, RaftError<u64>>),
    #[cfg(feature = "cache")]
    SnapshotCache(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),
}

#[derive(Debug)]
pub(crate) enum WsWriteMsg {
    Payload(Vec<u8>),
    Break,
}

enum RaftWorkSelection<T> {
    WriterFinished(Result<Result<(), String>, oneshot::error::RecvError>),
    ReaderFinished(Result<Result<(), String>, oneshot::error::RecvError>),
    Work(T),
}

enum RaftRequestTermination {
    WriterFinished(Result<Result<(), String>, oneshot::error::RecvError>),
    ReaderFinished(Result<Result<(), String>, oneshot::error::RecvError>),
    RequestChannelClosed,
}

async fn select_raft_request(
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    requests: &flume::Receiver<PreparedRaftRequest>,
) -> Result<PreparedRaftRequest, RaftRequestTermination> {
    tokio::select! {
        biased;
        writer = writer_finished => Err(RaftRequestTermination::WriterFinished(writer)),
        reader = reader_finished => Err(RaftRequestTermination::ReaderFinished(reader)),
        request = requests.recv_async() => request.map_err(|_| RaftRequestTermination::RequestChannelClosed),
    }
}

async fn select_raft_work<T, Work>(
    writer_finished: &mut oneshot::Receiver<Result<(), String>>,
    reader_finished: &mut oneshot::Receiver<Result<(), String>>,
    work: Work,
) -> RaftWorkSelection<T>
where
    Work: Future<Output = T>,
{
    tokio::pin!(work);
    tokio::select! {
        biased;
        writer = writer_finished => RaftWorkSelection::WriterFinished(writer),
        reader = reader_finished => RaftWorkSelection::ReaderFinished(reader),
        result = &mut work => RaftWorkSelection::Work(result),
    }
}

impl From<Vec<u8>> for RaftStreamResponse {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        deserialize(&value).unwrap()
    }
}

pub async fn stream_cache(
    state: AppStateExt,
    ws: upgrade::IncomingUpgrade,
) -> Result<impl IntoResponse, Error> {
    tracing::info!("Incoming WebSocket stream for Cache");

    #[cfg(feature = "cache")]
    {
        if !state.raft_cache.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Cache Raft still starting up - rejecting streaming connection");
            return Err(Error::BadRequest("Raft is still starting up".into()));
        }
        if state.raft_cache.is_raft_stopped.load(Ordering::Relaxed) {
            warn!("Cache Raft has been stopped - rejecting streaming connection");
            return Err(Error::BadRequest("Raft has been stopped".into()));
        }
    }

    tracing::info!("WebSocket cache stream request accepted");

    let (response, socket) = ws.upgrade()?;
    let task_status = state.snapshot_transport.clone();
    tokio::task::spawn(Box::pin(async move {
        let _task_guard = task_status.owned_async_task();
        if let Err(err) = handle_socket(state, socket).await {
            debug!("Cache WebSocket stream closed: {}", err);
        }
    }));

    Ok(response)
}

pub async fn stream_sqlite(
    state: AppStateExt,
    ws: upgrade::IncomingUpgrade,
) -> Result<impl IntoResponse, Error> {
    tracing::info!("Incoming WebSocket stream for SQLite");

    #[cfg(feature = "sqlite")]
    {
        if !state.raft_db.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Sqlite Raft still starting up - rejecting streaming connection");
            return Err(Error::BadRequest("Raft is still starting up".into()));
        }
        if state.raft_db.is_raft_stopped.load(Ordering::Relaxed) {
            warn!("Sqlite Raft has been stopped - rejecting streaming connection");
            return Err(Error::BadRequest("Raft has been stopped".into()));
        }
    }

    tracing::info!("WebSocket sqlite stream request accepted");

    let (response, socket) = ws.upgrade()?;
    let task_status = state.snapshot_transport.clone();
    tokio::task::spawn(Box::pin(async move {
        let _task_guard = task_status.owned_async_task();
        if let Err(err) = handle_socket(state, socket).await {
            debug!("SQLite WebSocket stream closed: {}", err);
        }
    }));

    Ok(response)
}

async fn handle_socket(
    state: AppStateExt,
    socket: upgrade::UpgradeFut,
) -> Result<(), fastwebsockets::WebSocketError> {
    let mut ws = socket.await?;
    ws.set_auto_close(true);

    let peer_node_id = match HandshakeSecret::server(&mut ws, state.secret_raft.as_bytes()).await {
        Ok(peer_node_id) => peer_node_id,
        Err(err) => {
            error!("Error during WebSocket handshake: {}", err);
            write_socket_close_frame_flushed(&mut ws, Frame::close(1000, b"Invalid Handshake"))
                .await?;
            return Ok(());
        }
    };
    let socket_epoch = state.snapshot_transport.next_inbound_socket_epoch();

    let (tx_write, rx_write) = flume::bounded::<WsWriteMsg>(1);
    let (rx, write) = ws.split(tokio::io::split);
    // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
    let mut read = FragmentCollectorRead::new(rx);

    let (tx_connection_closed, mut rx_connection_closed) = watch::channel(false);
    let (tx_writer_finished, mut rx_writer_finished) = oneshot::channel();
    let writer_connection_closed = tx_connection_closed.clone();
    let writer_task_status = state.snapshot_transport.clone();
    let handle_write = task::spawn(async move {
        let _task_guard = writer_task_status.owned_async_task();
        raft_response_writer(
            write,
            rx_write,
            writer_connection_closed,
            tx_writer_finished,
        )
        .await;
    });

    let (tx_read, rx_read) = flume::bounded(1);
    let (tx_reader_finished, mut rx_reader_finished) = oneshot::channel();
    let reader_connection_closed = tx_connection_closed.clone();
    let reader_snapshot_transport = state.snapshot_transport.clone();
    let reader_admission_budgets = SnapshotAdmissionBudgets::from_state(&state);
    let reader_task_status = state.snapshot_transport.clone();
    let handle_read = task::spawn(async move {
        let _task_guard = reader_task_status.owned_async_task();
        let outcome = loop {
            let frame = match read
                .read_frame(&mut |frame| async move {
                    // TODO obligated sends should be auto ping / pong / close ? -> verify!
                    debug!(
                        opcode = ?frame.opcode,
                        payload_len = frame.payload.len(),
                        "received obligated send in Raft stream server"
                    );
                    Ok::<(), Error>(())
                })
                .await
            {
                Ok(frame) => frame,
                Err(err) => break Err(err.to_string()),
            };
            let request = match frame.opcode {
                OpCode::Close => {
                    debug!("received Close frame in server stream");
                    break Ok(());
                }
                OpCode::Binary => {
                    let bytes = frame.payload.deref();
                    match deserialize::<RaftStreamRequest>(bytes) {
                        Ok(req) => req,
                        Err(err) => break Err(format!("invalid Raft stream request: {err}")),
                    }
                }
                _ => break Err("non-binary Raft stream payload".to_owned()),
            };
            // Preparation is synchronous and precedes both ownership transfer
            // to the bounded queue and publication of reader completion. The
            // queued value therefore owns status cleanup even when the biased
            // socket supervisor observes EOF before dequeuing it.
            let req = PreparedRaftRequest::new(
                &reader_snapshot_transport,
                reader_admission_budgets,
                peer_node_id,
                socket_epoch,
                request,
            );

            tokio::select! {
                _ = rx_connection_closed.changed() => break Ok(()),
                result = tx_read.send_async(req) => {
                    if result.is_err() {
                        break Ok(());
                    }
                }
            }
        };
        reader_connection_closed.send_replace(true);
        let _ = tx_reader_finished.send(outcome);
    });

    let mut writer_failed = false;
    loop {
        let req =
            match select_raft_request(&mut rx_writer_finished, &mut rx_reader_finished, &rx_read)
                .await
            {
                Err(RaftRequestTermination::WriterFinished(writer)) => {
                    match writer {
                        Ok(Ok(())) => error!("Raft server WebSocket writer exited while connected"),
                        Ok(Err(err)) => error!("Raft server WebSocket writer failed: {err}"),
                        Err(_) => error!("Raft server WebSocket writer panicked or was cancelled"),
                    }
                    writer_failed = true;
                    break;
                }
                Err(RaftRequestTermination::ReaderFinished(reader)) => {
                    match reader {
                        Ok(Ok(())) => debug!("Raft server WebSocket reader exited"),
                        Ok(Err(err)) => error!("Raft server WebSocket reader failed: {err}"),
                        Err(_) => error!("Raft server WebSocket reader panicked or was cancelled"),
                    }
                    break;
                }
                Err(RaftRequestTermination::RequestChannelClosed) => break,
                Ok(req) => req,
            };

        #[cfg(feature = "validation-test-helpers")]
        if crate::network::raft_client::validation_raft_partitioned() {
            break;
        }

        let mut work_connection_closed = tx_connection_closed.subscribe();
        let work = execute_raft_request(&state, req, &mut work_connection_closed);
        let work_result =
            match select_raft_work(&mut rx_writer_finished, &mut rx_reader_finished, work).await {
                RaftWorkSelection::WriterFinished(writer) => {
                    match writer {
                        Ok(Ok(())) => error!("Raft server WebSocket writer exited while connected"),
                        Ok(Err(err)) => error!("Raft server WebSocket writer failed: {err}"),
                        Err(_) => error!("Raft server WebSocket writer panicked or was cancelled"),
                    }
                    writer_failed = true;
                    None
                }
                RaftWorkSelection::ReaderFinished(reader) => {
                    match reader {
                        Ok(Ok(())) => debug!("Raft server WebSocket reader exited"),
                        Ok(Err(err)) => error!("Raft server WebSocket reader failed: {err}"),
                        Err(_) => error!("Raft server WebSocket reader panicked or was cancelled"),
                    }
                    None
                }
                RaftWorkSelection::Work(result) => Some(result),
            };
        let Some(work_result) = work_result else {
            break;
        };
        let (request_id, payload) = match work_result {
            Ok(Some(response)) => response,
            Ok(None) => break,
            Err(err) => {
                error!("Raft snapshot request was not admitted or lost its response: {err:?}");
                break;
            }
        };

        let response = WsWriteMsg::Payload(serialize_network(&RaftStreamResponse {
            request_id,
            payload,
        }));
        tokio::select! {
            biased;
            writer = &mut rx_writer_finished => {
                match writer {
                    Ok(Ok(())) => error!("Raft server WebSocket writer exited while connected"),
                    Ok(Err(err)) => error!("Raft server WebSocket writer failed: {err}"),
                    Err(_) => error!("Raft server WebSocket writer panicked or was cancelled"),
                }
                writer_failed = true;
                break;
            }
            _ = &mut rx_reader_finished => break,
            result = tx_write.send_async(response) => {
                if let Err(err) = result {
                    error!("Error forwarding raft response to WebSocket writer: {err}");
                    break;
                }
            }
        }
    }

    tx_connection_closed.send_replace(true);
    let mut handle_write = handle_write;
    let writer_finished = if writer_failed {
        false
    } else {
        tokio::time::timeout(CLOSE_WRITE_TIMEOUT, async {
            tx_write.send_async(WsWriteMsg::Break).await.ok()?;
            Some((&mut handle_write).await)
        })
        .await
        .ok()
        .flatten()
        .is_some()
    };
    drop(tx_write);
    if !writer_finished {
        handle_write.abort();
        let _ = handle_write.await;
    }
    handle_read.abort();
    let _ = handle_read.await;

    debug!("handle_socket exiting");

    Ok(())
}

async fn write_raft_response_frame<S>(
    write: &mut fastwebsockets::WebSocketWrite<S>,
    bytes: Vec<u8>,
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    write_frame_flushed(write, Frame::binary(Payload::Owned(bytes))).await
}

async fn raft_response_writer<S>(
    mut write: fastwebsockets::WebSocketWrite<S>,
    rx_write: flume::Receiver<WsWriteMsg>,
    connection_closed: watch::Sender<bool>,
    finished: oneshot::Sender<Result<(), String>>,
) where
    S: tokio::io::AsyncWrite + Unpin,
{
    let outcome = loop {
        let req = match rx_write.recv_async().await {
            Ok(req) => req,
            Err(_) => break Ok(()),
        };
        match req {
            WsWriteMsg::Payload(bytes) => {
                if let Err(err) = write_raft_response_frame(&mut write, bytes).await {
                    error!("Error during WebSocket write: {}", err);
                    break Err(err.to_string());
                }
            }
            WsWriteMsg::Break => {
                debug!("handle_socket -> server stream break message");
                break Ok(());
            }
        }
    };

    debug!("handle_socket -> Raft server WebSocket writer exiting");
    let _ = write_close_frame_flushed(&mut write, Frame::close(1000, b"go away")).await;
    connection_closed.send_replace(true);
    let _ = finished.send(outcome);
}

// `handle_socket` may drop request work when a biased reader/writer branch wins.
// Keep synchronous ownership of the receipt until the executor publishes a
// result so cancellation cannot leave an unowned observation looking active.
struct InboundSnapshotStatusGuard {
    snapshot_transport: crate::LocalSnapshotTransportStatus,
    status_attempt: Option<crate::transport_status::InboundSnapshotAttempt>,
    admission_timeout: Duration,
}

#[derive(Clone, Copy)]
struct SnapshotAdmissionBudget {
    timeout: Duration,
}

#[derive(Clone, Copy)]
struct SnapshotAdmissionBudgets {
    #[cfg(feature = "sqlite")]
    sqlite: SnapshotAdmissionBudget,
    #[cfg(feature = "cache")]
    cache: SnapshotAdmissionBudget,
}

impl SnapshotAdmissionBudgets {
    fn from_state(state: &AppStateExt) -> Self {
        Self {
            #[cfg(feature = "sqlite")]
            sqlite: SnapshotAdmissionBudget {
                timeout: state.raft_db.snapshot_executor.admission_timeout(),
            },
            #[cfg(feature = "cache")]
            cache: SnapshotAdmissionBudget {
                timeout: state.raft_cache.snapshot_executor.admission_timeout(),
            },
        }
    }

    #[cfg(test)]
    fn uniform(timeout: Duration) -> Self {
        Self {
            #[cfg(feature = "sqlite")]
            sqlite: SnapshotAdmissionBudget { timeout },
            #[cfg(feature = "cache")]
            cache: SnapshotAdmissionBudget { timeout },
        }
    }
}

struct PreparedInboundSnapshotStatus {
    status_attempt: Option<crate::transport_status::InboundSnapshotAttempt>,
    status_guard: InboundSnapshotStatusGuard,
}

struct PreparedRaftRequest {
    request: RaftStreamRequest,
    inbound_snapshot_status: Option<PreparedInboundSnapshotStatus>,
}

impl PreparedRaftRequest {
    fn new(
        snapshot_transport: &crate::LocalSnapshotTransportStatus,
        admission_budgets: SnapshotAdmissionBudgets,
        peer_node_id: u64,
        socket_epoch: u64,
        request: RaftStreamRequest,
    ) -> Self {
        let inbound_snapshot_status = match &request {
            #[cfg(feature = "sqlite")]
            RaftStreamRequest::SnapshotDB((_, request)) => Some(Self::prepare_snapshot_status(
                snapshot_transport,
                admission_budgets.sqlite,
                "sqlite",
                peer_node_id,
                socket_epoch,
                request.meta.snapshot_id.as_str(),
                request.offset,
                request.data.len(),
                request.done,
            )),
            #[cfg(feature = "cache")]
            RaftStreamRequest::SnapshotCache((_, request)) => Some(Self::prepare_snapshot_status(
                snapshot_transport,
                admission_budgets.cache,
                "cache",
                peer_node_id,
                socket_epoch,
                request.meta.snapshot_id.as_str(),
                request.offset,
                request.data.len(),
                request.done,
            )),
            _ => None,
        };
        Self {
            request,
            inbound_snapshot_status,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_snapshot_status(
        snapshot_transport: &crate::LocalSnapshotTransportStatus,
        admission_budget: SnapshotAdmissionBudget,
        raft_group: &'static str,
        peer_node_id: u64,
        socket_epoch: u64,
        snapshot_id: &str,
        offset: u64,
        len: usize,
        done: bool,
    ) -> PreparedInboundSnapshotStatus {
        let status_attempt =
            snapshot_transport.inbound_received(crate::transport_status::InboundSnapshotChunk {
                raft_group,
                peer_node_id,
                snapshot_id,
                offset,
                len,
                done,
                socket_epoch,
                // A connection may remain open indefinitely. Give each
                // decoded request its own admission window instead of
                // retaining the socket-accept instant as a deadline.
                deadline: time::Instant::now() + admission_budget.timeout,
            });
        PreparedInboundSnapshotStatus {
            status_guard: InboundSnapshotStatusGuard::new(
                snapshot_transport,
                status_attempt.clone(),
                admission_budget.timeout,
            ),
            status_attempt,
        }
    }
}

impl InboundSnapshotStatusGuard {
    fn new(
        snapshot_transport: &crate::LocalSnapshotTransportStatus,
        status_attempt: Option<crate::transport_status::InboundSnapshotAttempt>,
        admission_timeout: Duration,
    ) -> Self {
        Self {
            snapshot_transport: snapshot_transport.clone(),
            status_attempt,
            admission_timeout,
        }
    }

    fn disarm(&mut self) {
        self.status_attempt = None;
    }
}

impl Drop for InboundSnapshotStatusGuard {
    fn drop(&mut self) {
        let Some(status_attempt) = self.status_attempt.as_ref() else {
            return;
        };
        self.snapshot_transport.inbound_request_ended(
            status_attempt,
            (!status_attempt.done).then(|| time::Instant::now() + self.admission_timeout),
            crate::transport_status::InboundSnapshotDisposition::Retrying(
                "snapshot_connection_closed",
            ),
        );
    }
}

fn record_inbound_submit_error<C: openraft::RaftTypeConfig>(
    snapshot_transport: &crate::LocalSnapshotTransportStatus,
    status_attempt: Option<&crate::transport_status::InboundSnapshotAttempt>,
    error: &crate::network::snapshot_executor::SubmitError,
    executor: &crate::network::snapshot_executor::SnapshotExecutor<C>,
) {
    let Some(status_attempt) = status_attempt else {
        return;
    };
    let disposition = match error {
        crate::network::snapshot_executor::SubmitError::AdmissionTimeout => {
            crate::transport_status::InboundSnapshotDisposition::Retrying(
                "snapshot_admission_timeout",
            )
        }
        crate::network::snapshot_executor::SubmitError::AdmissionBusy => {
            crate::transport_status::InboundSnapshotDisposition::Retrying("snapshot_admission_busy")
        }
        crate::network::snapshot_executor::SubmitError::ConnectionClosed => {
            crate::transport_status::InboundSnapshotDisposition::Retrying(
                "snapshot_connection_closed",
            )
        }
        crate::network::snapshot_executor::SubmitError::ExecutorClosed => {
            crate::transport_status::InboundSnapshotDisposition::Failed("snapshot_executor_closed")
        }
        crate::network::snapshot_executor::SubmitError::ResponseClosed => {
            crate::transport_status::InboundSnapshotDisposition::Failed(
                "snapshot_executor_response_closed",
            )
        }
    };
    snapshot_transport.inbound_request_ended(
        status_attempt,
        (!status_attempt.done).then(|| executor.admission_deadline()),
        disposition,
    );
}

async fn execute_raft_request(
    state: &AppStateExt,
    prepared_request: PreparedRaftRequest,
    connection_closed: &mut watch::Receiver<bool>,
) -> Result<
    Option<(usize, RaftStreamResponsePayload)>,
    crate::network::snapshot_executor::SubmitError,
> {
    let PreparedRaftRequest {
        request,
        mut inbound_snapshot_status,
    } = prepared_request;
    let response = match request {
        #[cfg(feature = "sqlite")]
        RaftStreamRequest::AppendDB((request_id, request)) => {
            let result = state.raft_db.raft.append_entries(request).await;
            if let Err(RaftError::Fatal(Fatal::Stopped)) = &result {
                debug!("Raft DB stopped - exiting");
                state.raft_db.is_raft_stopped.store(true, Ordering::Relaxed);
                return Ok(None);
            }
            (request_id, RaftStreamResponsePayload::AppendDB(result))
        }
        #[cfg(feature = "sqlite")]
        RaftStreamRequest::VoteDB((request_id, request)) => {
            let result = state.raft_db.raft.vote(request).await;
            (request_id, RaftStreamResponsePayload::VoteDB(result))
        }
        #[cfg(feature = "sqlite")]
        RaftStreamRequest::SnapshotDB((request_id, request)) => {
            let mut prepared_status = inbound_snapshot_status
                .take()
                .expect("SQLite snapshot status is prepared before execution");
            let status_attempt = prepared_status.status_attempt.clone();
            let result = state
                .raft_db
                .snapshot_executor
                .submit(
                    crate::network::snapshot_executor::SnapshotExecutorRequest {
                        status_attempt: status_attempt.clone(),
                        request,
                    },
                    connection_closed,
                )
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    record_inbound_submit_error(
                        &state.snapshot_transport,
                        status_attempt.as_ref(),
                        &error,
                        &state.raft_db.snapshot_executor,
                    );
                    prepared_status.status_guard.disarm();
                    return Err(error);
                }
            };
            prepared_status.status_guard.disarm();
            (request_id, RaftStreamResponsePayload::SnapshotDB(result))
        }
        #[cfg(feature = "cache")]
        RaftStreamRequest::AppendCache((request_id, request)) => {
            let result = state.raft_cache.raft.append_entries(request).await;
            if let Err(RaftError::Fatal(Fatal::Stopped)) = &result {
                debug!("Raft Cache stopped - exiting");
                state
                    .raft_cache
                    .is_raft_stopped
                    .store(true, Ordering::Relaxed);
                return Ok(None);
            }
            (request_id, RaftStreamResponsePayload::AppendCache(result))
        }
        #[cfg(feature = "cache")]
        RaftStreamRequest::VoteCache((request_id, request)) => {
            let result = state.raft_cache.raft.vote(request).await;
            (request_id, RaftStreamResponsePayload::VoteCache(result))
        }
        #[cfg(feature = "cache")]
        RaftStreamRequest::SnapshotCache((request_id, request)) => {
            let mut prepared_status = inbound_snapshot_status
                .take()
                .expect("cache snapshot status is prepared before execution");
            let status_attempt = prepared_status.status_attempt.clone();
            let result = state
                .raft_cache
                .snapshot_executor
                .submit(
                    crate::network::snapshot_executor::SnapshotExecutorRequest {
                        status_attempt: status_attempt.clone(),
                        request,
                    },
                    connection_closed,
                )
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    record_inbound_submit_error(
                        &state.snapshot_transport,
                        status_attempt.as_ref(),
                        &error,
                        &state.raft_cache.snapshot_executor,
                    );
                    prepared_status.status_guard.disarm();
                    return Err(error);
                }
            };
            prepared_status.status_guard.disarm();
            (request_id, RaftStreamResponsePayload::SnapshotCache(result))
        }
        #[cfg(feature = "cache")]
        RaftStreamRequest::RemoveMembershipCache(node_id) => {
            debug!("Node drop membership request for Node: {}\n", node_id);
            let _lock = state.raft_lock.lock().await;
            let metrics = helpers::get_raft_metrics(state, &RaftType::Cache).await;
            let members = metrics.membership_config;
            let mut nodes_set = BTreeSet::new();
            for (id, _node) in members.nodes() {
                if *id != node_id {
                    nodes_set.insert(*id);
                }
            }
            if let Err(err) =
                helpers::change_membership(state, &RaftType::Cache, nodes_set, false).await
            {
                error!("Error removing remote Cache Member: {:?}", err);
            }
            return Ok(None);
        }
    };

    Ok(Some(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::Role;
    use openraft::Vote;
    use openraft::raft::InstallSnapshotResponse;

    fn track_inbound_attempt(
        status: &crate::LocalSnapshotTransportStatus,
        snapshot_id: &str,
        offset: u64,
        len: usize,
        done: bool,
        socket_epoch: u64,
    ) -> crate::transport_status::InboundSnapshotAttempt {
        status
            .inbound_received(crate::transport_status::InboundSnapshotChunk {
                raft_group: "sqlite",
                peer_node_id: 1,
                snapshot_id,
                offset,
                len,
                done,
                socket_epoch,
                deadline: time::Instant::now() + Duration::from_secs(30),
            })
            .expect("track inbound snapshot")
    }

    #[cfg(feature = "sqlite")]
    async fn drop_queued_snapshot_after_biased_reader_eof(
        status: &crate::LocalSnapshotTransportStatus,
        snapshot_id: &str,
        offset: u64,
    ) {
        let prepared_request = PreparedRaftRequest::new(
            status,
            SnapshotAdmissionBudgets::uniform(Duration::from_secs(1)),
            1,
            1,
            RaftStreamRequest::SnapshotDB((
                7,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: openraft::SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: snapshot_id.to_owned(),
                    },
                    offset,
                    data: vec![0; 64],
                    done: false,
                },
            )),
        );
        let (requests, queued_requests) = flume::bounded(1);
        assert!(requests.send(prepared_request).is_ok());
        drop(requests);
        let (keep_writer_open, mut writer_finished) = oneshot::channel();
        let (reader_done, mut reader_finished) = oneshot::channel();
        reader_done.send(Ok(())).expect("signal reader close");

        let selected =
            select_raft_request(&mut writer_finished, &mut reader_finished, &queued_requests).await;
        assert!(matches!(
            selected,
            Err(RaftRequestTermination::ReaderFinished(Ok(Ok(()))))
        ));
        drop(keep_writer_open);
        drop(queued_requests);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn first_snapshot_identity_reports_terminal_status_when_reader_eof_wins() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        drop_queued_snapshot_after_biased_reader_eof(&status, "first-socket-close", 0).await;

        let observation = &status.snapshot().observations[0];
        assert_ne!(observation.attempt_id, 0);
        assert_eq!(
            observation.snapshot_id.as_deref(),
            Some("first-socket-close")
        );
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("snapshot_connection_closed")
        );
        assert_eq!(observation.retry_count, 1);
        assert!(!observation.operation_owns_work);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn changed_snapshot_identity_reports_terminal_status_when_reader_eof_wins() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let old = track_inbound_attempt(&status, "old-snapshot", 0, 64, true, 1);
        status.inbound_admitted(&old, true, time::Instant::now() + Duration::from_secs(30));
        status.inbound_finished(
            &old,
            64,
            true,
            None,
            crate::transport_status::InboundSnapshotDisposition::Succeeded,
        );
        let old_attempt_id = status.snapshot().observations[0].attempt_id;

        drop_queued_snapshot_after_biased_reader_eof(&status, "replacement-snapshot", 0).await;

        let observation = &status.snapshot().observations[0];
        assert!(observation.attempt_id > old_attempt_id);
        assert_eq!(
            observation.snapshot_id.as_deref(),
            Some("replacement-snapshot")
        );
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("snapshot_connection_closed")
        );
        assert_eq!(observation.retry_count, 1);
        assert!(!observation.operation_owns_work);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn queued_snapshot_status_is_prepared_before_biased_reader_eof() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let first = track_inbound_attempt(&status, "queued-socket-close", 0, 64, false, 1);
        status.inbound_admitted(
            &first,
            false,
            time::Instant::now() + Duration::from_secs(30),
        );
        status.inbound_finished(
            &first,
            64,
            false,
            Some(time::Instant::now() + Duration::from_secs(30)),
            crate::transport_status::InboundSnapshotDisposition::Succeeded,
        );

        drop_queued_snapshot_after_biased_reader_eof(&status, "queued-socket-close", 64).await;

        let observation = &status.snapshot().observations[0];
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("snapshot_connection_closed")
        );
        assert_eq!(observation.retry_count, 1);
        assert!(!observation.operation_owns_work);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn long_lived_socket_derives_a_fresh_admission_deadline_per_request() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let admission_timeout = Duration::from_secs(30);
        let budgets = SnapshotAdmissionBudgets::uniform(admission_timeout);
        let first = track_inbound_attempt(&status, "long-lived-socket", 0, 64, false, 1);
        status.inbound_admitted(&first, false, time::Instant::now() + admission_timeout);
        status.inbound_finished(
            &first,
            64,
            false,
            Some(time::Instant::now() + admission_timeout),
            crate::transport_status::InboundSnapshotDisposition::Succeeded,
        );

        time::advance(admission_timeout + Duration::from_secs(1)).await;
        let _prepared = PreparedRaftRequest::new(
            &status,
            budgets,
            1,
            1,
            RaftStreamRequest::SnapshotDB((
                8,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: openraft::SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: "long-lived-socket".to_owned(),
                    },
                    offset: 64,
                    data: vec![0; 64],
                    done: false,
                },
            )),
        );

        let observation = &status.snapshot().observations[0];
        assert_eq!(
            observation.phase,
            crate::SnapshotTransportPhase::Transferring
        );
        assert_eq!(
            observation.active_deadline_remaining_ms,
            Some(admission_timeout.as_millis() as u64)
        );
        assert_eq!(observation.last_error_category, None);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn production_prepares_snapshot_status_before_biased_socket_close_drops_unpolled_work() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let first = track_inbound_attempt(&status, "socket-close", 0, 64, false, 1);
        status.inbound_admitted(
            &first,
            false,
            time::Instant::now() + Duration::from_secs(30),
        );
        status.inbound_finished(
            &first,
            64,
            false,
            Some(time::Instant::now() + Duration::from_secs(30)),
            crate::transport_status::InboundSnapshotDisposition::Succeeded,
        );
        let prepared_request = PreparedRaftRequest::new(
            &status,
            SnapshotAdmissionBudgets::uniform(Duration::from_secs(1)),
            1,
            1,
            RaftStreamRequest::SnapshotDB((
                7,
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: openraft::SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: "socket-close".to_owned(),
                    },
                    offset: 64,
                    data: vec![0; 64],
                    done: false,
                },
            )),
        );
        let prepared = &status.snapshot().observations[0];
        assert_eq!(prepared.attempted_offset, Some(128));
        assert_eq!(prepared.phase, crate::SnapshotTransportPhase::Transferring);

        let (keep_writer_open, mut writer_finished) = oneshot::channel();
        let (reader_done, mut reader_finished) = oneshot::channel();
        reader_done.send(Ok(())).expect("signal reader close");

        let work = async move {
            let _prepared_request = prepared_request;
            std::future::pending::<()>().await;
        };
        let selected = select_raft_work(&mut writer_finished, &mut reader_finished, work).await;
        assert!(matches!(
            selected,
            RaftWorkSelection::ReaderFinished(Ok(Ok(())))
        ));
        drop(keep_writer_open);

        let observation = &status.snapshot().observations[0];
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Retrying);
        assert_eq!(
            observation.last_error_category.as_deref(),
            Some("snapshot_connection_closed")
        );
        assert!(!observation.operation_owns_work);
    }

    #[tokio::test(start_paused = true)]
    async fn inbound_snapshot_status_guard_preserves_worker_owned_and_completed_results() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let attempt = track_inbound_attempt(&status, "worker-owned", 0, 64, true, 1);
        let worker_started = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let finish_worker = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let executor = std::sync::Arc::new(
            crate::network::snapshot_executor::NodeOwnedExecutor::start(Duration::from_secs(1), {
                let worker_status = status.clone();
                let worker_attempt = attempt.clone();
                let worker_started = std::sync::Arc::clone(&worker_started);
                let finish_worker = std::sync::Arc::clone(&finish_worker);
                move |()| {
                    let worker_status = worker_status.clone();
                    let worker_attempt = worker_attempt.clone();
                    let worker_started = std::sync::Arc::clone(&worker_started);
                    let finish_worker = std::sync::Arc::clone(&finish_worker);
                    async move {
                        worker_status.inbound_admitted(
                            &worker_attempt,
                            true,
                            time::Instant::now() + Duration::from_secs(30),
                        );
                        worker_started.add_permits(1);
                        finish_worker
                            .acquire()
                            .await
                            .expect("finish-worker gate")
                            .forget();
                        worker_status.inbound_finished(
                            &worker_attempt,
                            64,
                            true,
                            None,
                            crate::transport_status::InboundSnapshotDisposition::Succeeded,
                        );
                    }
                }
            }),
        );

        let (_connection, mut connection_closed) = watch::channel(false);
        let socket_work = tokio::spawn({
            let executor = std::sync::Arc::clone(&executor);
            let status_guard = InboundSnapshotStatusGuard::new(
                &status,
                Some(attempt.clone()),
                Duration::from_secs(1),
            );
            async move {
                let _status_guard = status_guard;
                executor.submit((), &mut connection_closed).await
            }
        });
        worker_started
            .acquire()
            .await
            .expect("worker-start gate")
            .forget();

        socket_work.abort();
        assert!(
            socket_work
                .await
                .expect_err("socket work should be cancelled")
                .is_cancelled()
        );
        let installing = &status.snapshot().observations[0];
        assert_eq!(installing.phase, crate::SnapshotTransportPhase::Installing);
        assert!(installing.operation_owns_work);
        assert_eq!(installing.last_error_category, None);

        finish_worker.add_permits(1);
        while status.snapshot().observations[0].phase != crate::SnapshotTransportPhase::Complete {
            tokio::task::yield_now().await;
        }
        drop(InboundSnapshotStatusGuard::new(
            &status,
            Some(attempt),
            Duration::from_secs(1),
        ));
        let completed = &status.snapshot().observations[0];
        assert_eq!(completed.phase, crate::SnapshotTransportPhase::Complete);
        assert!(!completed.operation_owns_work);
        assert_eq!(completed.last_error_category, None);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_raft_response_writer_flushes_serialized_response_through_tls() {
        let response = RaftStreamResponse {
            request_id: 9,
            payload: RaftStreamResponsePayload::SnapshotDB(Ok(InstallSnapshotResponse {
                vote: Vote::new_committed(1, 1),
            })),
        };
        let bytes = serialize_network(&response);
        crate::network::frame_io::tests::exercise_gated_tls_writer(
            Role::Server,
            bytes,
            true,
            |mut write, bytes| async move { write_raft_response_frame(&mut write, bytes).await },
        )
        .await;
    }

    #[tokio::test]
    async fn production_raft_response_writer_reports_flush_failure_and_closes() {
        let write = crate::network::frame_io::tests::split_writer(
            crate::network::frame_io::tests::TestIo::failing_flush(),
        );
        let (tx, rx) = flume::bounded(1);
        let (closed, closed_rx) = watch::channel(false);
        let (finished, outcome) = oneshot::channel();
        tx.send_async(WsWriteMsg::Payload(b"Raft response".to_vec()))
            .await
            .expect("queue Raft response");

        raft_response_writer(write, rx, closed, finished).await;

        assert!(*closed_rx.borrow());
        let error = outcome
            .await
            .expect("writer terminal outcome")
            .expect_err("flush failure must terminate the writer");
        assert!(error.contains("injected flush failure"));
    }
}
