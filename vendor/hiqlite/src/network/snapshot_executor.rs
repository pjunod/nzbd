use openraft::Raft;
use openraft::RaftTypeConfig;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{InstallSnapshotRequest, InstallSnapshotResponse};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{error, warn};

type SnapshotResult<C> = Result<
    InstallSnapshotResponse<<C as RaftTypeConfig>::NodeId>,
    RaftError<<C as RaftTypeConfig>::NodeId, InstallSnapshotError>,
>;

// The executor itself still owns at most one running and one queued job.
// Bound socket-owned callers waiting to claim the single queue slot as well.
const MAX_ADMISSION_WAITERS: usize = 2;

struct Job<Req, Resp> {
    request: Req,
    response: oneshot::Sender<Resp>,
}

#[derive(Default)]
struct AdmissionOrderState {
    serving: u64,
    // A cancelled ticket retains its waiter permit until the serving cursor
    // reaches it. Otherwise one descheduled head waiter plus rapid churn in a
    // second slot could accumulate an unbounded set of cancelled ticket IDs.
    cancelled: BTreeMap<u64, tokio::sync::OwnedSemaphorePermit>,
}

struct AdmissionOrder {
    next: AtomicU64,
    state: StdMutex<AdmissionOrderState>,
    changed: Notify,
    waiter_slots: Arc<tokio::sync::Semaphore>,
}

impl AdmissionOrder {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            state: StdMutex::new(AdmissionOrderState::default()),
            changed: Notify::new(),
            waiter_slots: Arc::new(tokio::sync::Semaphore::new(MAX_ADMISSION_WAITERS)),
        }
    }

    fn reserve(self: &Arc<Self>) -> Option<AdmissionTicket> {
        let waiter_slot = Arc::clone(&self.waiter_slots).try_acquire_owned().ok()?;
        Some(AdmissionTicket {
            order: Arc::clone(self),
            ticket: self.next.fetch_add(1, Ordering::AcqRel),
            active: true,
            waiter_slot: Some(waiter_slot),
        })
    }

    async fn wait_until_serving(&self, ticket: u64) {
        loop {
            // Register before inspecting the state so advancing the queue
            // cannot land between the check and the notification subscription.
            let changed = self.changed.notified();
            let serving = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .serving;
            if serving == ticket {
                return;
            }
            changed.await;
        }
    }

    fn release(&self, ticket: u64, waiter_slot: tokio::sync::OwnedSemaphorePermit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ticket == state.serving {
            state.serving = state.serving.wrapping_add(1);
            let mut serving = state.serving;
            while state.cancelled.remove(&serving).is_some() {
                state.serving = state.serving.wrapping_add(1);
                serving = state.serving;
            }
        } else if ticket > state.serving {
            state.cancelled.insert(ticket, waiter_slot);
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

struct AdmissionTicket {
    order: Arc<AdmissionOrder>,
    ticket: u64,
    active: bool,
    waiter_slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl AdmissionTicket {
    async fn wait_until_serving(&self) {
        self.order.wait_until_serving(self.ticket).await;
    }

    fn release(&mut self) {
        if self.active {
            self.active = false;
            if let Some(waiter_slot) = self.waiter_slot.take() {
                self.order.release(self.ticket, waiter_slot);
            }
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        self.release();
    }
}

/// One node-owned worker plus one queued request.
///
/// A socket owns admission and the response receiver only. Once the worker
/// starts a request, dropping that receiver cannot cancel the operation.
pub(crate) struct NodeOwnedExecutor<Req, Resp> {
    tx: flume::Sender<Job<Req, Resp>>,
    admission_timeout: Duration,
    admission_order: Arc<AdmissionOrder>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<Req, Resp> NodeOwnedExecutor<Req, Resp> {
    pub(crate) fn admission_timeout(&self) -> Duration {
        self.admission_timeout
    }

    pub(crate) fn admission_deadline(&self) -> time::Instant {
        time::Instant::now() + self.admission_timeout
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SubmitError {
    AdmissionTimeout,
    AdmissionBusy,
    ConnectionClosed,
    ExecutorClosed,
    ResponseClosed,
}

impl<Req, Resp> NodeOwnedExecutor<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    #[cfg(test)]
    pub(crate) fn start<Execute, ExecuteFuture>(
        admission_timeout: Duration,
        execute: Execute,
    ) -> Self
    where
        Execute: FnMut(Req) -> ExecuteFuture + Send + 'static,
        ExecuteFuture: Future<Output = Resp> + Send + 'static,
    {
        Self::start_inner(admission_timeout, None, execute)
    }

    fn start_tracked<Execute, ExecuteFuture>(
        admission_timeout: Duration,
        task_status: crate::LocalSnapshotTransportStatus,
        execute: Execute,
    ) -> Self
    where
        Execute: FnMut(Req) -> ExecuteFuture + Send + 'static,
        ExecuteFuture: Future<Output = Resp> + Send + 'static,
    {
        Self::start_inner(admission_timeout, Some(task_status), execute)
    }

    fn start_inner<Execute, ExecuteFuture>(
        admission_timeout: Duration,
        task_status: Option<crate::LocalSnapshotTransportStatus>,
        mut execute: Execute,
    ) -> Self
    where
        Execute: FnMut(Req) -> ExecuteFuture + Send + 'static,
        ExecuteFuture: Future<Output = Resp> + Send + 'static,
    {
        let (tx, rx) = flume::bounded::<Job<Req, Resp>>(1);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _task_guard = task_status.map(|status| status.owned_async_task());
            loop {
                let job = tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => break,
                    job = rx.recv_async() => match job {
                        Ok(job) => job,
                        Err(_) => break,
                    },
                };

                if job.response.is_closed() {
                    continue;
                }

                let result = execute(job.request).await;
                let _ = job.response.send(result);
            }
        });

        Self {
            tx,
            admission_timeout,
            admission_order: Arc::new(AdmissionOrder::new()),
            shutdown,
            task: Mutex::new(Some(task)),
        }
    }

    pub(crate) fn submit<'a>(
        &'a self,
        request: Req,
        connection_closed: &'a mut watch::Receiver<bool>,
    ) -> impl Future<Output = Result<Resp, SubmitError>> + 'a {
        // Reserve synchronously, before this future can be polled, so callers
        // that reach the executor retain FIFO order under reverse polling.
        // Status ownership is assigned separately at actual worker start.
        // Dropping the future releases or skips its bounded ticket.
        let admission_ticket = self.admission_order.reserve();
        let admission_deadline = time::Instant::now() + self.admission_timeout;
        async move {
            let Some(mut admission_ticket) = admission_ticket else {
                return Err(SubmitError::AdmissionBusy);
            };
            if *connection_closed.borrow() {
                return Err(SubmitError::ConnectionClosed);
            }
            // Subscribe before inspecting the value so a concurrent shutdown
            // is either observed here or wakes the admission/response select.
            let mut shutdown = self.shutdown.subscribe();
            if *shutdown.borrow() {
                return Err(SubmitError::ExecutorClosed);
            }

            tokio::select! {
                biased;
                _ = connection_closed.changed() => return Err(SubmitError::ConnectionClosed),
                _ = shutdown.changed() => return Err(SubmitError::ExecutorClosed),
                _ = time::sleep_until(admission_deadline) => {
                    return Err(SubmitError::AdmissionTimeout);
                }
                () = admission_ticket.wait_until_serving() => {}
            }

            let (response, response_rx) = oneshot::channel();
            let mut pending = Job { request, response };
            loop {
                if *connection_closed.borrow() {
                    return Err(SubmitError::ConnectionClosed);
                }
                if *shutdown.borrow() {
                    return Err(SubmitError::ExecutorClosed);
                }
                // A successful `try_send` is the only point after which
                // execution is permitted, so timeout cannot race accepted work.
                if time::Instant::now() >= admission_deadline {
                    return Err(SubmitError::AdmissionTimeout);
                }
                match self.tx.try_send(pending) {
                    Ok(()) => {
                        admission_ticket.release();
                        break;
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        return Err(SubmitError::ExecutorClosed);
                    }
                    Err(flume::TrySendError::Full(job)) => pending = job,
                }
                tokio::select! {
                    biased;
                    _ = connection_closed.changed() => {
                        return Err(SubmitError::ConnectionClosed);
                    }
                    _ = shutdown.changed() => return Err(SubmitError::ExecutorClosed),
                    _ = time::sleep_until(admission_deadline) => {
                        return Err(SubmitError::AdmissionTimeout);
                    }
                    () = time::sleep(Duration::from_millis(1)) => {}
                }
            }

            tokio::select! {
                biased;
                _ = connection_closed.changed() => Err(SubmitError::ConnectionClosed),
                _ = shutdown.changed() => Err(SubmitError::ExecutorClosed),
                result = response_rx => result.map_err(|_| SubmitError::ResponseClosed),
            }
        }
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    /// Wait without detaching or aborting an accepted operation.
    pub(crate) async fn wait_for_shutdown(&self, timeout: Duration) -> bool {
        self.request_shutdown();
        let mut task_slot = self.task.lock().await;
        let Some(task) = task_slot.as_mut() else {
            return true;
        };

        match time::timeout(timeout, task).await {
            Ok(Ok(())) => {
                task_slot.take();
                true
            }
            Ok(Err(join_error)) => {
                error!("snapshot executor task failed during shutdown: {join_error}");
                task_slot.take();
                false
            }
            Err(_) => {
                warn!("snapshot executor still owns work after bounded shutdown wait");
                false
            }
        }
    }
}

pub(crate) struct SnapshotExecutorRequest<C: RaftTypeConfig> {
    pub(crate) status_attempt: Option<crate::transport_status::InboundSnapshotAttempt>,
    pub(crate) request: InstallSnapshotRequest<C>,
}

pub(crate) type SnapshotExecutor<C> =
    NodeOwnedExecutor<SnapshotExecutorRequest<C>, SnapshotResult<C>>;

fn record_inbound_snapshot_result<C>(
    snapshot_transport: &crate::LocalSnapshotTransportStatus,
    status_attempt: Option<&crate::transport_status::InboundSnapshotAttempt>,
    locally_received_offset: u64,
    done: bool,
    next_chunk_deadline: Option<time::Instant>,
    request_vote: openraft::Vote<u64>,
    result: &SnapshotResult<C>,
) where
    C: RaftTypeConfig<NodeId = u64>,
{
    let Some(status_attempt) = status_attempt else {
        return;
    };
    let disposition = match result {
        Ok(response) if response.vote <= request_vote => {
            crate::transport_status::InboundSnapshotDisposition::Succeeded
        }
        Ok(_) => crate::transport_status::InboundSnapshotDisposition::Failed("higher_vote"),
        Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(_))) => {
            crate::transport_status::InboundSnapshotDisposition::Retrying("snapshot_mismatch")
        }
        Err(RaftError::Fatal(_)) => {
            crate::transport_status::InboundSnapshotDisposition::Failed("snapshot_install_fatal")
        }
    };
    snapshot_transport.inbound_finished(
        status_attempt,
        locally_received_offset,
        done,
        next_chunk_deadline,
        disposition,
    );
}

pub(crate) fn start_snapshot_executor<C>(
    raft: Raft<C>,
    admission_timeout: Duration,
    install_timeout: Duration,
    _raft_group: &'static str,
    snapshot_transport: crate::LocalSnapshotTransportStatus,
) -> SnapshotExecutor<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::SnapshotData: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin,
{
    start_snapshot_executor_with_installer(
        admission_timeout,
        install_timeout,
        snapshot_transport,
        move |request| {
            let raft = raft.clone();
            async move { raft.install_snapshot(request).await }
        },
    )
}

fn start_snapshot_executor_with_installer<C, Install, InstallFuture>(
    admission_timeout: Duration,
    install_timeout: Duration,
    snapshot_transport: crate::LocalSnapshotTransportStatus,
    mut install: Install,
) -> SnapshotExecutor<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    Install: FnMut(InstallSnapshotRequest<C>) -> InstallFuture + Send + 'static,
    InstallFuture: Future<Output = SnapshotResult<C>> + Send + 'static,
{
    NodeOwnedExecutor::start_tracked(
        admission_timeout,
        snapshot_transport.clone(),
        move |job: SnapshotExecutorRequest<C>| {
            let snapshot_transport = snapshot_transport.clone();
            let request_vote = job.request.vote;
            let done = job.request.done;
            let acknowledged_offset = job
                .request
                .offset
                .saturating_add(job.request.data.len() as u64);
            let install = install(job.request);
            async move {
                if let Some(status_attempt) = job.status_attempt.as_ref() {
                    snapshot_transport.inbound_admitted(
                        status_attempt,
                        done,
                        time::Instant::now()
                            + if done {
                                install_timeout
                            } else {
                                admission_timeout
                            },
                    );
                }
                let result = install.await;
                record_inbound_snapshot_result::<C>(
                    &snapshot_transport,
                    job.status_attempt.as_ref(),
                    acknowledged_offset,
                    done,
                    (!done).then(|| time::Instant::now() + admission_timeout),
                    request_vote,
                    &result,
                );
                result
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use tokio::sync::{Notify, Semaphore};

    #[tokio::test]
    async fn production_snapshot_executor_worker_is_counted_until_joined() {
        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let executor = NodeOwnedExecutor::start_tracked(
            Duration::from_secs(1),
            status.clone(),
            |request: usize| async move { request },
        );
        while status.owned_async_task_count() == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(status.owned_async_task_count(), 1);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
        assert_eq!(status.owned_async_task_count(), 0);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_snapshot_executor_worker_records_status_around_injected_installer() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use openraft::{SnapshotMeta, Vote};

        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let status_attempt = status
            .inbound_received(crate::transport_status::InboundSnapshotChunk {
                raft_group: "sqlite",
                peer_node_id: 1,
                snapshot_id: "executor-ordering",
                offset: 0,
                len: 64,
                done: true,
                socket_epoch: 1,
                deadline: time::Instant::now() + Duration::from_secs(30),
            })
            .expect("track inbound snapshot");
        let installer_status = status.clone();
        let executor = start_snapshot_executor_with_installer::<TypeConfigSqlite, _, _>(
            Duration::from_secs(1),
            Duration::from_secs(30),
            status.clone(),
            move |request| {
                let installer_status = installer_status.clone();
                async move {
                    let installing = &installer_status.snapshot().observations[0];
                    assert_eq!(installing.phase, crate::SnapshotTransportPhase::Installing);
                    assert!(installing.operation_owns_work);
                    Ok(InstallSnapshotResponse { vote: request.vote })
                }
            },
        );
        let (_connection, mut connection_closed) = watch::channel(false);
        let response = executor
            .submit(
                SnapshotExecutorRequest {
                    status_attempt: Some(status_attempt),
                    request: InstallSnapshotRequest {
                        vote: Vote::new_committed(1, 1),
                        meta: SnapshotMeta {
                            last_log_id: None,
                            last_membership: Default::default(),
                            snapshot_id: "executor-ordering".to_owned(),
                        },
                        offset: 0,
                        data: vec![0; 64],
                        done: true,
                    },
                },
                &mut connection_closed,
            )
            .await
            .expect("executor response");
        assert!(response.is_ok());
        let observation = &status.snapshot().observations[0];
        assert_eq!(observation.locally_received_bytes, Some(64));
        assert_eq!(observation.acknowledged_offset, None);
        assert_eq!(observation.phase, crate::SnapshotTransportPhase::Complete);
        assert!(!observation.operation_owns_work);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_snapshot_executor_fifo_status_ignores_later_admission_waiter() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use crate::transport_status::{
            InboundSnapshotAttempt, InboundSnapshotChunk, InboundSnapshotDisposition,
        };
        use openraft::{SnapshotMeta, Vote};

        fn track_attempt(
            status: &crate::LocalSnapshotTransportStatus,
            snapshot_id: &str,
            socket_epoch: u64,
        ) -> InboundSnapshotAttempt {
            status
                .inbound_received(InboundSnapshotChunk {
                    raft_group: "sqlite",
                    peer_node_id: 1,
                    snapshot_id,
                    offset: 0,
                    len: 64,
                    done: true,
                    socket_epoch,
                    deadline: time::Instant::now() + Duration::from_secs(30),
                })
                .expect("track inbound snapshot")
        }

        fn request(
            status_attempt: InboundSnapshotAttempt,
            snapshot_id: &str,
        ) -> SnapshotExecutorRequest<TypeConfigSqlite> {
            SnapshotExecutorRequest {
                status_attempt: Some(status_attempt),
                request: InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: snapshot_id.to_owned(),
                    },
                    offset: 0,
                    data: vec![0; 64],
                    done: true,
                },
            }
        }

        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let first_attempt = track_attempt(&status, "fifo-a", 1);
        let first_gate = Arc::new(Semaphore::new(0));
        let second_gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = Arc::new(start_snapshot_executor_with_installer::<
            TypeConfigSqlite,
            _,
            _,
        >(
            Duration::from_secs(1),
            Duration::from_secs(30),
            status.clone(),
            {
                let first_gate = Arc::clone(&first_gate);
                let second_gate = Arc::clone(&second_gate);
                let started = Arc::clone(&started);
                move |request| {
                    let gate = match request.meta.snapshot_id.as_str() {
                        "fifo-a" => Arc::clone(&first_gate),
                        "fifo-b" => Arc::clone(&second_gate),
                        other => panic!("unexpected admitted snapshot {other}"),
                    };
                    let started = Arc::clone(&started);
                    async move {
                        started.lock().await.push(request.meta.snapshot_id.clone());
                        gate.acquire().await.expect("test gate").forget();
                        Ok(InstallSnapshotResponse { vote: request.vote })
                    }
                }
            },
        ));

        let (_close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .submit(request(first_attempt, "fifo-a"), &mut first_closed)
                    .await
            }
        });
        while started.lock().await.as_slice() != ["fifo-a"] {
            tokio::task::yield_now().await;
        }
        let active = &status.snapshot().observations[0];
        assert_eq!(active.snapshot_id.as_deref(), Some("fifo-a"));
        assert!(active.operation_owns_work);

        let second_attempt = track_attempt(&status, "fifo-b", 2);
        let (_close_second, mut second_closed) = watch::channel(false);
        let second = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .submit(request(second_attempt, "fifo-b"), &mut second_closed)
                    .await
            }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        let third_attempt = track_attempt(&status, "fifo-c", 3);
        let (close_third, mut third_closed) = watch::channel(false);
        let third = tokio::spawn({
            let executor = Arc::clone(&executor);
            let third_attempt = third_attempt.clone();
            async move {
                executor
                    .submit(request(third_attempt, "fifo-c"), &mut third_closed)
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !third.is_finished(),
            "third request must still be waiting behind the full FIFO"
        );
        let still_first = &status.snapshot().observations[0];
        assert_eq!(still_first.snapshot_id.as_deref(), Some("fifo-a"));
        assert!(still_first.operation_owns_work);

        close_third.send_replace(true);
        assert_eq!(
            third.await.expect("third caller task"),
            Err(SubmitError::ConnectionClosed)
        );
        status.inbound_request_ended(
            &third_attempt,
            None,
            InboundSnapshotDisposition::Retrying("snapshot_connection_closed"),
        );
        assert_eq!(
            status.snapshot().observations[0].snapshot_id.as_deref(),
            Some("fifo-a")
        );

        first_gate.add_permits(1);
        assert!(
            first
                .await
                .expect("first caller task")
                .expect("first response")
                .is_ok()
        );
        while !started.lock().await.iter().any(|value| value == "fifo-b") {
            tokio::task::yield_now().await;
        }
        let actual_fifo_owner = &status.snapshot().observations[0];
        assert_eq!(actual_fifo_owner.snapshot_id.as_deref(), Some("fifo-b"));
        assert!(actual_fifo_owner.operation_owns_work);
        assert_eq!(actual_fifo_owner.last_error_category, None);

        second_gate.add_permits(1);
        assert!(
            second
                .await
                .expect("second caller task")
                .expect("second response")
                .is_ok()
        );
        let completed = &status.snapshot().observations[0];
        assert_eq!(completed.snapshot_id.as_deref(), Some("fifo-b"));
        assert_eq!(completed.phase, crate::SnapshotTransportPhase::Complete);
        assert!(!completed.operation_owns_work);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_snapshot_status_follows_worker_start_after_pre_ticket_deschedule() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use crate::transport_status::{InboundSnapshotAttempt, InboundSnapshotChunk};
        use openraft::{SnapshotMeta, Vote};

        fn track_attempt(
            status: &crate::LocalSnapshotTransportStatus,
            snapshot_id: &str,
            socket_epoch: u64,
        ) -> InboundSnapshotAttempt {
            status
                .inbound_received(InboundSnapshotChunk {
                    raft_group: "sqlite",
                    peer_node_id: 1,
                    snapshot_id,
                    offset: 0,
                    len: 64,
                    done: true,
                    socket_epoch,
                    deadline: time::Instant::now() + Duration::from_secs(30),
                })
                .expect("track inbound snapshot")
        }

        fn request(
            status_attempt: InboundSnapshotAttempt,
            snapshot_id: &str,
        ) -> SnapshotExecutorRequest<TypeConfigSqlite> {
            SnapshotExecutorRequest {
                status_attempt: Some(status_attempt),
                request: InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: snapshot_id.to_owned(),
                    },
                    offset: 0,
                    data: vec![0; 64],
                    done: true,
                },
            }
        }

        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        // The first socket is deliberately descheduled after receipt but
        // before calling submit, while the later receipt reaches the executor.
        let earlier_receipt = track_attempt(&status, "descheduled-a", 1);
        let later_receipt = track_attempt(&status, "descheduled-b", 2);
        let gates = Arc::new([Semaphore::new(0), Semaphore::new(0)]);
        let started = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = Arc::new(start_snapshot_executor_with_installer::<
            TypeConfigSqlite,
            _,
            _,
        >(
            Duration::from_secs(1),
            Duration::from_secs(30),
            status.clone(),
            {
                let gates = Arc::clone(&gates);
                let started = Arc::clone(&started);
                move |request| {
                    let gate_index = match request.meta.snapshot_id.as_str() {
                        "descheduled-a" => 0,
                        "descheduled-b" => 1,
                        other => panic!("unexpected admitted snapshot {other}"),
                    };
                    let gates = Arc::clone(&gates);
                    let started = Arc::clone(&started);
                    async move {
                        started.lock().await.push(request.meta.snapshot_id.clone());
                        gates[gate_index]
                            .acquire()
                            .await
                            .expect("test gate")
                            .forget();
                        Ok(InstallSnapshotResponse { vote: request.vote })
                    }
                }
            },
        ));

        let (_close_later, mut later_closed) = watch::channel(false);
        let later = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .submit(request(later_receipt, "descheduled-b"), &mut later_closed)
                    .await
            }
        });
        while started.lock().await.as_slice() != ["descheduled-b"] {
            tokio::task::yield_now().await;
        }
        let later_owner = &status.snapshot().observations[0];
        assert_eq!(later_owner.snapshot_id.as_deref(), Some("descheduled-b"));
        assert!(later_owner.operation_owns_work);
        let later_status_attempt_id = later_owner.attempt_id;

        let (_close_earlier, mut earlier_closed) = watch::channel(false);
        let earlier = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .submit(
                        request(earlier_receipt, "descheduled-a"),
                        &mut earlier_closed,
                    )
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(started.lock().await.as_slice(), ["descheduled-b"]);

        gates[1].add_permits(1);
        assert!(
            later
                .await
                .expect("later receipt caller")
                .expect("later receipt response")
                .is_ok()
        );
        while started.lock().await.as_slice() != ["descheduled-b", "descheduled-a"] {
            tokio::task::yield_now().await;
        }
        let actual_later_owner = &status.snapshot().observations[0];
        assert_eq!(
            actual_later_owner.snapshot_id.as_deref(),
            Some("descheduled-a")
        );
        assert!(actual_later_owner.operation_owns_work);
        assert!(actual_later_owner.attempt_id > later_status_attempt_id);

        gates[0].add_permits(1);
        assert!(
            earlier
                .await
                .expect("earlier receipt caller")
                .expect("earlier receipt response")
                .is_ok()
        );
        let completed = &status.snapshot().observations[0];
        assert_eq!(completed.snapshot_id.as_deref(), Some("descheduled-a"));
        assert_eq!(completed.phase, crate::SnapshotTransportPhase::Complete);
        assert!(!completed.operation_owns_work);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_snapshot_executor_ticket_order_survives_reverse_waiter_polling() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use crate::transport_status::{InboundSnapshotAttempt, InboundSnapshotChunk};
        use openraft::{SnapshotMeta, Vote};

        fn track_attempt(
            status: &crate::LocalSnapshotTransportStatus,
            snapshot_id: &str,
            socket_epoch: u64,
        ) -> InboundSnapshotAttempt {
            status
                .inbound_received(InboundSnapshotChunk {
                    raft_group: "sqlite",
                    peer_node_id: 1,
                    snapshot_id,
                    offset: 0,
                    len: 64,
                    done: true,
                    socket_epoch,
                    deadline: time::Instant::now() + Duration::from_secs(30),
                })
                .expect("track inbound snapshot")
        }

        fn request(
            status_attempt: InboundSnapshotAttempt,
            snapshot_id: &str,
        ) -> SnapshotExecutorRequest<TypeConfigSqlite> {
            SnapshotExecutorRequest {
                status_attempt: Some(status_attempt),
                request: InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: snapshot_id.to_owned(),
                    },
                    offset: 0,
                    data: vec![0; 64],
                    done: true,
                },
            }
        }

        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
        let gates = Arc::new([
            Semaphore::new(0),
            Semaphore::new(0),
            Semaphore::new(0),
            Semaphore::new(0),
        ]);
        let started = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = Arc::new(start_snapshot_executor_with_installer::<
            TypeConfigSqlite,
            _,
            _,
        >(
            Duration::from_secs(5),
            Duration::from_secs(30),
            status.clone(),
            {
                let gates = Arc::clone(&gates);
                let started = Arc::clone(&started);
                move |request| {
                    let gate_index = match request.meta.snapshot_id.as_str() {
                        "reverse-a" => 0,
                        "reverse-b" => 1,
                        "reverse-c" => 2,
                        "reverse-d" => 3,
                        other => panic!("unexpected admitted snapshot {other}"),
                    };
                    let gates = Arc::clone(&gates);
                    let started = Arc::clone(&started);
                    async move {
                        started.lock().await.push(request.meta.snapshot_id.clone());
                        gates[gate_index]
                            .acquire()
                            .await
                            .expect("test gate")
                            .forget();
                        Ok(InstallSnapshotResponse { vote: request.vote })
                    }
                }
            },
        ));

        let (_close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            let first_attempt = track_attempt(&status, "reverse-a", 1);
            async move {
                executor
                    .submit(request(first_attempt, "reverse-a"), &mut first_closed)
                    .await
            }
        });
        while started.lock().await.as_slice() != ["reverse-a"] {
            tokio::task::yield_now().await;
        }

        let (_close_second, mut second_closed) = watch::channel(false);
        let second = tokio::spawn({
            let executor = Arc::clone(&executor);
            let second_attempt = track_attempt(&status, "reverse-b", 2);
            async move {
                executor
                    .submit(request(second_attempt, "reverse-b"), &mut second_closed)
                    .await
            }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        let third_attempt = track_attempt(&status, "reverse-c", 3);
        let fourth_attempt = track_attempt(&status, "reverse-d", 4);
        let (_close_third, mut third_closed) = watch::channel(false);
        let (_close_fourth, mut fourth_closed) = watch::channel(false);
        // Reserving C before D is the production receipt order. The biased
        // join deliberately polls D first on every wake to exercise the
        // scheduler interleaving that used to reverse the two waiters.
        let third = executor.submit(request(third_attempt, "reverse-c"), &mut third_closed);
        let fourth = executor.submit(request(fourth_attempt, "reverse-d"), &mut fourth_closed);
        let controller = tokio::spawn({
            let gates = Arc::clone(&gates);
            let started = Arc::clone(&started);
            async move {
                gates[0].add_permits(1);
                while started.lock().await.len() < 2 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(started.lock().await.as_slice(), ["reverse-a", "reverse-b"]);
                gates[1].add_permits(1);
                while started.lock().await.len() < 3 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    started.lock().await.as_slice(),
                    ["reverse-a", "reverse-b", "reverse-c"]
                );
                gates[2].add_permits(1);
                while started.lock().await.len() < 4 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    started.lock().await.as_slice(),
                    ["reverse-a", "reverse-b", "reverse-c", "reverse-d"]
                );
                gates[3].add_permits(1);
            }
        });

        let (fourth_result, third_result) = tokio::join!(biased; fourth, third);
        assert!(third_result.expect("third response").is_ok());
        assert!(fourth_result.expect("fourth response").is_ok());
        assert!(
            first
                .await
                .expect("first caller task")
                .expect("first response")
                .is_ok()
        );
        assert!(
            second
                .await
                .expect("second caller task")
                .expect("second response")
                .is_ok()
        );
        controller.await.expect("release controller");
        let completed = &status.snapshot().observations[0];
        assert_eq!(completed.snapshot_id.as_deref(), Some("reverse-d"));
        assert_eq!(completed.phase, crate::SnapshotTransportPhase::Complete);
        assert!(!completed.operation_owns_work);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_shared_executor_retains_abandoned_other_peer_terminal_status() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use crate::transport_status::{
            InboundSnapshotAttempt, InboundSnapshotChunk, InboundSnapshotDisposition,
        };
        use openraft::{SnapshotMeta, Vote};

        fn track_attempt(
            status: &crate::LocalSnapshotTransportStatus,
            peer_node_id: u64,
            snapshot_id: &str,
        ) -> InboundSnapshotAttempt {
            status
                .inbound_received(InboundSnapshotChunk {
                    raft_group: "sqlite",
                    peer_node_id,
                    snapshot_id,
                    offset: 0,
                    len: 64,
                    done: true,
                    socket_epoch: 1,
                    deadline: time::Instant::now() + Duration::from_secs(30),
                })
                .expect("track inbound snapshot")
        }

        fn request(
            status_attempt: InboundSnapshotAttempt,
            snapshot_id: &str,
        ) -> SnapshotExecutorRequest<TypeConfigSqlite> {
            SnapshotExecutorRequest {
                status_attempt: Some(status_attempt),
                request: InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: snapshot_id.to_owned(),
                    },
                    offset: 0,
                    data: vec![0; 64],
                    done: true,
                },
            }
        }

        let status =
            crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1, 3]));
        let running_attempt = track_attempt(&status, 1, "running");
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = Arc::new(start_snapshot_executor_with_installer::<
            TypeConfigSqlite,
            _,
            _,
        >(
            Duration::from_secs(1),
            Duration::from_secs(30),
            status.clone(),
            {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                move |request| {
                    let gate = Arc::clone(&gate);
                    let started = Arc::clone(&started);
                    async move {
                        started.lock().await.push(request.meta.snapshot_id.clone());
                        gate.acquire().await.expect("test gate").forget();
                        Ok(InstallSnapshotResponse { vote: request.vote })
                    }
                }
            },
        ));

        let (_running_close, mut running_closed) = watch::channel(false);
        let running = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .submit(request(running_attempt, "running"), &mut running_closed)
                    .await
            }
        });
        while started.lock().await.as_slice() != ["running"] {
            tokio::task::yield_now().await;
        }

        let abandoned_attempt = track_attempt(&status, 3, "abandoned");
        assert_eq!(status.snapshot().observations.len(), 1);
        let (abandoned_close, mut abandoned_closed) = watch::channel(false);
        let abandoned = tokio::spawn({
            let executor = Arc::clone(&executor);
            let abandoned_attempt = abandoned_attempt.clone();
            async move {
                executor
                    .submit(
                        request(abandoned_attempt, "abandoned"),
                        &mut abandoned_closed,
                    )
                    .await
            }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }
        abandoned_close.send_replace(true);
        assert_eq!(
            abandoned.await.expect("abandoned caller task"),
            Err(SubmitError::ConnectionClosed)
        );
        status.inbound_request_ended(
            &abandoned_attempt,
            None,
            InboundSnapshotDisposition::Retrying("snapshot_connection_closed"),
        );
        let running_and_terminal = status.snapshot();
        assert_eq!(running_and_terminal.observations.len(), 2);
        let running_status = &running_and_terminal.observations[0];
        let abandoned_status = &running_and_terminal.observations[1];
        assert_eq!(running_status.peer_node_id, 1);
        assert!(running_status.operation_owns_work);
        assert_eq!(abandoned_status.peer_node_id, 3);
        assert_eq!(abandoned_status.snapshot_id.as_deref(), Some("abandoned"));
        assert_eq!(
            abandoned_status.phase,
            crate::SnapshotTransportPhase::Retrying
        );
        assert_eq!(
            abandoned_status.last_error_category.as_deref(),
            Some("snapshot_connection_closed")
        );
        assert_eq!(abandoned_status.retry_count, 1);
        assert!(!abandoned_status.operation_owns_work);
        assert!(abandoned_status.attempt_id > running_status.attempt_id);

        gate.add_permits(1);
        assert!(
            running
                .await
                .expect("running caller task")
                .expect("running response")
                .is_ok()
        );
        while executor.tx.is_full() {
            tokio::task::yield_now().await;
        }
        assert_eq!(started.lock().await.as_slice(), ["running"]);
        let completed = status.snapshot();
        assert_eq!(completed.observations.len(), 2);
        assert_eq!(completed.observations[0].peer_node_id, 1);
        assert_eq!(
            completed.observations[0].phase,
            crate::SnapshotTransportPhase::Complete
        );
        assert_eq!(&completed.observations[1], abandoned_status);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test(start_paused = true)]
    async fn inbound_result_disposition_distinguishes_mismatch_higher_vote_and_fatal() {
        use crate::store::state_machine::sqlite::TypeConfigSqlite;
        use openraft::error::{Fatal, SnapshotMismatch};
        use openraft::{SnapshotSegmentId, Vote};

        fn tracked_status(
            snapshot_id: &'static str,
        ) -> (
            crate::LocalSnapshotTransportStatus,
            crate::transport_status::InboundSnapshotAttempt,
        ) {
            let status =
                crate::LocalSnapshotTransportStatus::new(2, std::collections::BTreeSet::from([1]));
            let attempt = status
                .inbound_received(crate::transport_status::InboundSnapshotChunk {
                    raft_group: "sqlite",
                    peer_node_id: 1,
                    snapshot_id,
                    offset: 0,
                    len: 64,
                    done: true,
                    socket_epoch: 1,
                    deadline: time::Instant::now() + Duration::from_secs(30),
                })
                .expect("track inbound snapshot");
            status.inbound_admitted(
                &attempt,
                true,
                time::Instant::now() + Duration::from_secs(30),
            );
            (status, attempt)
        }

        let request_vote = Vote::new_committed(1, 1);
        let (mismatch_status, mismatch_attempt) = tracked_status("mismatch");
        let mismatch: SnapshotResult<TypeConfigSqlite> = Err(RaftError::APIError(
            InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
                expect: SnapshotSegmentId::from(("mismatch", 0)),
                got: SnapshotSegmentId::from(("mismatch", 64)),
            }),
        ));
        record_inbound_snapshot_result::<TypeConfigSqlite>(
            &mismatch_status,
            Some(&mismatch_attempt),
            64,
            true,
            None,
            request_vote,
            &mismatch,
        );
        let mismatch_observation = &mismatch_status.snapshot().observations[0];
        assert_eq!(
            mismatch_observation.phase,
            crate::SnapshotTransportPhase::Retrying
        );
        assert_eq!(
            mismatch_observation.last_error_category.as_deref(),
            Some("snapshot_mismatch")
        );

        let (higher_status, higher_attempt) = tracked_status("higher-vote");
        let higher_vote: SnapshotResult<TypeConfigSqlite> = Ok(InstallSnapshotResponse {
            vote: Vote::new_committed(2, 2),
        });
        record_inbound_snapshot_result::<TypeConfigSqlite>(
            &higher_status,
            Some(&higher_attempt),
            64,
            true,
            None,
            request_vote,
            &higher_vote,
        );
        let higher_observation = &higher_status.snapshot().observations[0];
        assert_eq!(
            higher_observation.phase,
            crate::SnapshotTransportPhase::Failed
        );
        assert_eq!(
            higher_observation.last_error_category.as_deref(),
            Some("higher_vote")
        );

        let (fatal_status, fatal_attempt) = tracked_status("fatal");
        let fatal: SnapshotResult<TypeConfigSqlite> = Err(RaftError::Fatal(Fatal::Stopped));
        record_inbound_snapshot_result::<TypeConfigSqlite>(
            &fatal_status,
            Some(&fatal_attempt),
            64,
            true,
            None,
            request_vote,
            &fatal,
        );
        let fatal_observation = &fatal_status.snapshot().observations[0];
        assert_eq!(
            fatal_observation.phase,
            crate::SnapshotTransportPhase::Failed
        );
        assert_eq!(
            fatal_observation.last_error_category.as_deref(),
            Some("snapshot_install_fatal")
        );
    }

    #[derive(Clone)]
    struct FileWrite {
        offset: usize,
        data: Vec<u8>,
    }

    #[tokio::test]
    async fn executor_runs_one_queues_one_and_drops_abandoned_queued_work() {
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_millis(20), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            let completed = Arc::clone(&completed);
            move |request: usize| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                let completed = Arc::clone(&completed);
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    gate.acquire().await.expect("test gate").forget();
                    completed.fetch_add(1, Ordering::SeqCst);
                    request
                }
            }
        }));

        let (_close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(1, &mut first_closed).await }
        });
        while started.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }

        let (close_second, mut second_closed) = watch::channel(false);
        let second = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(2, &mut second_closed).await }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        let (_close_third, mut third_closed) = watch::channel(false);
        assert_eq!(
            executor.submit(3, &mut third_closed).await,
            Err(SubmitError::AdmissionTimeout)
        );

        close_second.send_replace(true);
        assert_eq!(
            second.await.expect("second caller task"),
            Err(SubmitError::ConnectionClosed)
        );
        gate.add_permits(1);
        assert_eq!(first.await.expect("first caller task"), Ok(1));

        tokio::task::yield_now().await;
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn admission_waiter_budget_bounds_cancelled_ticket_retention_and_reports_busy() {
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_secs(1), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            move |request: usize| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    gate.acquire().await.expect("test gate").forget();
                    request
                }
            }
        }));

        let (_close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(1, &mut first_closed).await }
        });
        while started.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }

        let (close_second, mut second_closed) = watch::channel(false);
        let second = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(2, &mut second_closed).await }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        let (_close_head, mut head_closed) = watch::channel(false);
        let head = executor.submit(3, &mut head_closed);
        let (_close_cancelled, mut cancelled_closed) = watch::channel(false);
        let cancelled = executor.submit(4, &mut cancelled_closed);
        drop(cancelled);
        assert_eq!(
            executor.admission_order.waiter_slots.available_permits(),
            0,
            "a cancelled ticket behind a descheduled head retains its bounded slot"
        );
        assert_eq!(
            executor
                .admission_order
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .cancelled
                .len(),
            1
        );

        let next_before_busy = executor.admission_order.next.load(Ordering::Acquire);
        let (_close_busy, mut busy_closed) = watch::channel(false);
        assert_eq!(
            executor.submit(5, &mut busy_closed).await,
            Err(SubmitError::AdmissionBusy)
        );
        assert_eq!(
            executor.admission_order.next.load(Ordering::Acquire),
            next_before_busy,
            "overflow must not allocate an unreachable ticket"
        );

        drop(head);
        assert_eq!(
            executor.admission_order.waiter_slots.available_permits(),
            MAX_ADMISSION_WAITERS
        );
        assert!(
            executor
                .admission_order
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .cancelled
                .is_empty()
        );

        close_second.send_replace(true);
        assert_eq!(
            second.await.expect("second caller task"),
            Err(SubmitError::ConnectionClosed)
        );
        gate.add_permits(1);
        assert_eq!(first.await.expect("first caller task"), Ok(1));
        tokio::task::yield_now().await;
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn shutdown_reports_running_work_without_cancelling_it() {
        let gate = Arc::new(Semaphore::new(0));
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_secs(1), {
            let gate = Arc::clone(&gate);
            move |request: usize| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.acquire().await.expect("test gate").forget();
                    request
                }
            }
        }));
        let (_close, mut connection_closed) = watch::channel(false);
        let caller = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(1, &mut connection_closed).await }
        });
        tokio::task::yield_now().await;

        assert!(!executor.wait_for_shutdown(Duration::from_millis(20)).await);
        gate.add_permits(1);
        assert_eq!(
            caller.await.expect("caller task"),
            Err(SubmitError::ExecutorClosed)
        );
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn shutdown_before_submission_is_latched_and_admits_no_work() {
        let started = Arc::new(AtomicUsize::new(0));
        let executor = NodeOwnedExecutor::start(Duration::from_secs(1), {
            let started = Arc::clone(&started);
            move |request: usize| {
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    request
                }
            }
        });
        executor.request_shutdown();
        let (_close, mut connection_closed) = watch::channel(false);

        assert_eq!(
            executor.submit(1, &mut connection_closed).await,
            Err(SubmitError::ExecutorClosed)
        );
        assert!(executor.tx.is_empty());
        assert_eq!(started.load(Ordering::SeqCst), 0);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn cancelled_shutdown_wait_retains_the_executor_handle() {
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Notify::new());
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_secs(1), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            move |request: usize| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                async move {
                    started.notify_one();
                    gate.acquire().await.expect("test gate").forget();
                    request
                }
            }
        }));
        let (_close, mut connection_closed) = watch::channel(false);
        let caller = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(1, &mut connection_closed).await }
        });
        started.notified().await;

        let waiter = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.wait_for_shutdown(Duration::from_secs(60)).await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(
            waiter
                .await
                .expect_err("cancel first shutdown wait")
                .is_cancelled()
        );

        gate.add_permits(1);
        assert_eq!(
            caller.await.expect("caller task"),
            Err(SubmitError::ExecutorClosed)
        );
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
        assert!(executor.task.lock().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn admission_deadline_never_executes_the_timed_out_job() {
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_millis(10), {
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            move |request: usize| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                async move {
                    started.lock().await.push(request);
                    gate.acquire().await.expect("test gate").forget();
                    request
                }
            }
        }));

        let (_close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(1, &mut first_closed).await }
        });
        while started.lock().await.as_slice() != [1] {
            tokio::task::yield_now().await;
        }
        let (close_second, mut second_closed) = watch::channel(false);
        let second = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(2, &mut second_closed).await }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        let (_close_third, mut third_closed) = watch::channel(false);
        let third = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.submit(3, &mut third_closed).await }
        });
        tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                gate.add_permits(1);
            }
        });
        assert_eq!(
            third.await.expect("third caller task"),
            Err(SubmitError::AdmissionTimeout)
        );

        close_second.send_replace(true);
        assert_eq!(
            second.await.expect("second caller task"),
            Err(SubmitError::ConnectionClosed)
        );
        assert_eq!(first.await.expect("first caller task"), Ok(1));
        gate.add_permits(1);
        tokio::task::yield_now().await;
        assert_eq!(started.lock().await.as_slice(), [1, 2]);
        assert!(!started.lock().await.contains(&3));
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn accepted_partial_write_finishes_before_same_offset_retry() {
        let image = (0..1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let root =
            std::env::temp_dir().join(format!("hiqlite-partial-snapshot-{}", uuid::Uuid::now_v7()));
        tokio::fs::create_dir_all(&root)
            .await
            .expect("create snapshot test directory");
        let file = root.join("received.snapshot");
        let partial_written = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let executor = Arc::new(NodeOwnedExecutor::start(Duration::from_secs(1), {
            let file = file.clone();
            let partial_written = Arc::clone(&partial_written);
            let release = Arc::clone(&release);
            move |write: FileWrite| {
                let file = file.clone();
                let partial_written = Arc::clone(&partial_written);
                let release = Arc::clone(&release);
                async move {
                    let split = write.data.len() / 2;
                    let mut received = tokio::fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .read(true)
                        .write(true)
                        .open(&file)
                        .await
                        .expect("open real snapshot file");
                    received
                        .set_len(write.offset as u64)
                        .await
                        .expect("truncate to accepted offset");
                    received
                        .seek(std::io::SeekFrom::Start(write.offset as u64))
                        .await
                        .expect("seek accepted offset");
                    received
                        .write_all(&write.data[..split])
                        .await
                        .expect("write controlled first half");
                    received.flush().await.expect("flush controlled first half");
                    partial_written.notify_one();
                    release.acquire().await.expect("write release").forget();
                    received
                        .write_all(&write.data[split..])
                        .await
                        .expect("finish accepted snapshot write");
                    received.flush().await.expect("flush complete snapshot");
                    write.data.len()
                }
            }
        }));

        let (close_first, mut first_closed) = watch::channel(false);
        let first = tokio::spawn({
            let executor = Arc::clone(&executor);
            let image = image.clone();
            async move {
                executor
                    .submit(
                        FileWrite {
                            offset: 0,
                            data: image,
                        },
                        &mut first_closed,
                    )
                    .await
            }
        });
        partial_written.notified().await;
        close_first.send_replace(true);
        assert_eq!(
            first.await.expect("first caller task"),
            Err(SubmitError::ConnectionClosed)
        );

        let (_close_retry, mut retry_closed) = watch::channel(false);
        let retry = tokio::spawn({
            let executor = Arc::clone(&executor);
            let image = image.clone();
            async move {
                executor
                    .submit(
                        FileWrite {
                            offset: 0,
                            data: image,
                        },
                        &mut retry_closed,
                    )
                    .await
            }
        });
        while !executor.tx.is_full() {
            tokio::task::yield_now().await;
        }

        release.add_permits(2);
        assert_eq!(retry.await.expect("retry caller task"), Ok(image.len()));
        let actual = tokio::fs::read(&file)
            .await
            .expect("read final snapshot image");
        assert_eq!(Sha256::digest(&actual), Sha256::digest(&image));
        assert_eq!(actual, image);
        assert!(executor.wait_for_shutdown(Duration::from_secs(1)).await);
        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove snapshot test directory");
    }
}
