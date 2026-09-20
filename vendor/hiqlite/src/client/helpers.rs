use crate::app_state::AppState;
use crate::client::LeaderRecovery;
use crate::client::stream::{ClientLeaderChange, ClientStreamControl};
use crate::{Client, Error, LEADER_DISCOVERY_TIMEOUT, LEADER_STREAM_HANDOFF_TIMEOUT, Node, NodeId};
use openraft::RaftMetrics;
use std::clone::Clone;
#[cfg(feature = "sqlite")]
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, warn};

/// One election can expose an old leader and then a replacement that has not
/// learned the winner yet. Both replies are definitive `ForwardToLeader`
/// refusals, so retry the same unaccepted request after each bounded recovery.
/// Three attempts cover that two-transition window without making a wedged
/// cluster unbounded.
#[cfg(feature = "sqlite")]
const LEADER_REQUEST_MAX_ATTEMPTS: usize = 3;

async fn first_some<T: Send + 'static>(mut probes: JoinSet<Option<T>>) -> Option<T> {
    while let Some(result) = probes.join_next().await {
        if let Ok(Some(value)) = result {
            return Some(value);
        }
    }
    None
}

async fn change_leader_and_wait(
    tx: &flume::Sender<ClientStreamControl>,
    leader_id: NodeId,
    node: Node,
) -> bool {
    let (ready, reopened) = tokio::sync::oneshot::channel();
    time::timeout(LEADER_STREAM_HANDOFF_TIMEOUT, async {
        if tx
            .send_async(ClientStreamControl::Leader(ClientLeaderChange {
                leader_id,
                node,
                ready: Some(ready),
            }))
            .await
            .is_err()
        {
            return false;
        }
        reopened.await.is_ok()
    })
    .await
    .unwrap_or(false)
}

#[cfg(feature = "sqlite")]
async fn retry_request_after_leader_change<T, F, Fut, R, Recovery>(
    mut request: F,
    mut recover: R,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
    R: FnMut(Error) -> Recovery,
    Recovery: Future<Output = (Error, bool)>,
{
    for attempt in 1..=LEADER_REQUEST_MAX_ATTEMPTS {
        match request().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < LEADER_REQUEST_MAX_ATTEMPTS => {
                let (error, recovered) = recover(error).await;
                if !recovered {
                    return Err(error);
                }
                warn!(
                    attempt,
                    max_attempts = LEADER_REQUEST_MAX_ATTEMPTS,
                    "leader changed before accepting request; retrying exact request"
                );
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded leader retry loop always returns")
}

impl Client {
    #[cfg(feature = "sqlite")]
    pub(crate) async fn retry_db_after_leader_change<T, F, Fut>(
        &self,
        request: F,
    ) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        retry_request_after_leader_change(request, |error| async move {
            if matches!(&error, Error::RequestNotDispatched(_)) {
                return (error, true);
            }
            let recovered = self
                .was_leader_update_error(&error, &self.inner.leader_db)
                .await;
            (error, recovered)
        })
        .await
    }

    #[inline(always)]
    pub(crate) async fn build_addr(
        &self,
        path: &str,
        leader: &Arc<RwLock<(NodeId, String)>>,
    ) -> String {
        let scheme = if self.inner.tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let url = {
            let lock = leader.read().await;
            format!("{}://{}{}", scheme, lock.1, path)
        };
        debug!("request url: {}", url);
        url
    }

    pub(crate) async fn find_set_active_leader(&self) {
        if self.inner.proxy_mode {
            return;
        }
        if let Some(state) = &self.inner.state {
            // we never need to do any remote lookups for metrics -> get can never fail
            #[cfg(feature = "sqlite")]
            {
                let metrics = state.raft_db.raft.metrics().borrow().clone();
                let mut find_leader = Self::find_set_leader(metrics, &self.inner.leader_db).await;

                while let Err(err) = find_leader {
                    warn!("Find DB leader error: {}", err);
                    time::sleep(Duration::from_millis(500)).await;
                    let metrics = state.raft_db.raft.metrics().borrow().clone();
                    find_leader = Self::find_set_leader(metrics, &self.inner.leader_db).await;
                }
            }

            #[cfg(feature = "cache")]
            {
                let metrics = state.raft_cache.raft.metrics().borrow().clone();
                let mut find_leader =
                    Self::find_set_leader(metrics, &self.inner.leader_cache).await;

                while let Err(err) = find_leader {
                    warn!("Find cache leader error: {}", err);
                    time::sleep(Duration::from_millis(500)).await;
                    let metrics = state.raft_cache.raft.metrics().borrow().clone();
                    find_leader = Self::find_set_leader(metrics, &self.inner.leader_cache).await;
                }
            }
        } else {
            // in this case, we have a remote client
            #[cfg(feature = "sqlite")]
            {
                let mut metrics = self.remote_metrics_loop_db().await;
                loop {
                    match Self::find_set_leader(metrics, &self.inner.leader_db).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {
                            metrics = self.remote_metrics_loop_db().await;
                        }
                    }
                }
            }

            #[cfg(feature = "cache")]
            {
                let mut metrics = self.remote_metrics_loop_cache().await;
                loop {
                    match Self::find_set_leader(metrics, &self.inner.leader_cache).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {
                            metrics = self.remote_metrics_loop_cache().await;
                        }
                    }
                }
            }
        }
    }

    #[cfg(feature = "cache")]
    async fn remote_metrics_loop_cache(&self) -> RaftMetrics<NodeId, Node> {
        loop {
            for addr in &self.inner.nodes {
                {
                    let mut lock = self.inner.leader_cache.write().await;
                    *lock = (lock.0, addr.clone());
                }

                match self.metrics_cache().await {
                    Ok(metrics) => {
                        return metrics;
                    }
                    Err(err) => {
                        error!("Error looking up Cache metrics: {}", err);
                    }
                }
            }
            time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[cfg(feature = "sqlite")]
    async fn remote_metrics_loop_db(&self) -> RaftMetrics<NodeId, Node> {
        loop {
            for addr in &self.inner.nodes {
                {
                    let mut lock = self.inner.leader_db.write().await;
                    *lock = (lock.0, addr.clone());
                }

                match self.metrics_db().await {
                    Ok(metrics) => {
                        return metrics;
                    }
                    Err(err) => {
                        error!("Error looking up DB metrics: {}", err);
                    }
                }
            }
            time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn find_set_leader(
        metrics: RaftMetrics<NodeId, Node>,
        leader: &Arc<RwLock<(NodeId, String)>>,
    ) -> Result<(), Error> {
        let (leader_id, node) = Self::leader_from_metrics(metrics)?;

        let mut lock = leader.write().await;
        *lock = (leader_id, node.addr_api);

        Ok(())
    }

    fn leader_from_metrics(metrics: RaftMetrics<NodeId, Node>) -> Result<(NodeId, Node), Error> {
        let leader_id = metrics
            .current_leader
            .ok_or_else(|| Error::Connect("Leader vote is in progress".to_string()))?;
        let mut leaders = metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| **id == leader_id);
        let (_, node) = leaders.next().ok_or_else(|| {
            Error::Config("Raft leader is absent from the authenticated membership".into())
        })?;
        if leaders.next().is_some() {
            return Err(Error::Config(
                "Raft leader is duplicated in the authenticated membership".into(),
            ));
        }
        Ok((leader_id, node.clone()))
    }

    async fn discover_active_leader(
        &self,
        leader: &Arc<RwLock<(NodeId, String)>>,
    ) -> Result<(NodeId, Node), Error> {
        loop {
            if let Some(state) = &self.inner.state {
                #[cfg(feature = "sqlite")]
                if Arc::ptr_eq(leader, &self.inner.leader_db) {
                    let metrics = state.raft_db.raft.metrics().borrow().clone();
                    match Self::leader_from_metrics(metrics) {
                        Ok(found) => return Ok(found),
                        Err(err) => warn!("Find DB leader error: {}", err),
                    }
                }

                #[cfg(feature = "cache")]
                if Arc::ptr_eq(leader, &self.inner.leader_cache) {
                    let metrics = state.raft_cache.raft.metrics().borrow().clone();
                    match Self::leader_from_metrics(metrics) {
                        Ok(found) => return Ok(found),
                        Err(err) => warn!("Find cache leader error: {}", err),
                    }
                }
            }

            #[cfg(feature = "sqlite")]
            let path = if Arc::ptr_eq(leader, &self.inner.leader_db) {
                "/cluster/metrics/sqlite"
            } else {
                #[cfg(feature = "cache")]
                if Arc::ptr_eq(leader, &self.inner.leader_cache) {
                    "/cluster/metrics/cache"
                } else {
                    return Err(Error::Config("unknown Raft leader lock".into()));
                }

                #[cfg(not(feature = "cache"))]
                return Err(Error::Config("unknown Raft leader lock".into()));
            };
            #[cfg(all(not(feature = "sqlite"), feature = "cache"))]
            let path = if Arc::ptr_eq(leader, &self.inner.leader_cache) {
                "/cluster/metrics/cache"
            } else {
                return Err(Error::Config("unknown Raft leader lock".into()));
            };
            let scheme = if self.inner.tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let mut probes = JoinSet::new();
            for addr in &self.inner.nodes {
                let client = self.clone();
                let url = format!("{scheme}://{addr}{path}");
                probes.spawn(async move {
                    match client
                        .get_metrics_remote(url)
                        .await
                        .and_then(Self::leader_from_metrics)
                    {
                        Ok(found) => Some(found),
                        Err(error) => {
                            warn!("Find configured leader error: {}", error);
                            None
                        }
                    }
                });
            }
            if let Some(found) = first_some(probes).await {
                return Ok(found);
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn leader_recovery(
        &self,
        leader: &Arc<RwLock<(NodeId, String)>>,
    ) -> Option<Arc<LeaderRecovery>> {
        #[cfg(feature = "sqlite")]
        if Arc::ptr_eq(leader, &self.inner.leader_db) {
            return Some(Arc::clone(&self.inner.leader_recovery_db));
        }
        #[cfg(feature = "cache")]
        if Arc::ptr_eq(leader, &self.inner.leader_cache) {
            return Some(Arc::clone(&self.inner.leader_recovery_cache));
        }
        None
    }

    fn leader_change_sender(
        &self,
        leader: &Arc<RwLock<(NodeId, String)>>,
    ) -> Option<flume::Sender<ClientStreamControl>> {
        #[cfg(feature = "sqlite")]
        if Arc::ptr_eq(leader, &self.inner.leader_db) {
            return Some(self.inner.tx_leader_db.clone());
        }
        #[cfg(feature = "cache")]
        if Arc::ptr_eq(leader, &self.inner.leader_cache) {
            return Some(self.inner.tx_leader_cache.clone());
        }
        None
    }

    /// Check if this instance is the current Raft cluster leader for the database.
    #[cfg(feature = "sqlite")]
    pub async fn is_leader_db(&self) -> bool {
        if let Some(state) = &self.inner.state
            && state.id == self.inner.leader_db.read().await.0
        {
            return true;
        }
        false
    }

    /// Check if this instance is the current Raft cluster leader for the cache.
    #[cfg(feature = "cache")]
    pub async fn is_leader_cache(&self) -> bool {
        if let Some(state) = &self.inner.state
            && state.id == self.inner.leader_cache.read().await.0
        {
            return true;
        }
        false
    }

    #[cfg(feature = "sqlite")]
    #[inline(always)]
    pub(crate) async fn is_leader_db_with_state(&self) -> Option<&Arc<AppState>> {
        if let Some(state) = &self.inner.state
            && state.id == self.inner.leader_db.read().await.0
        {
            return Some(state);
        }
        None
    }

    #[cfg(feature = "cache")]
    #[inline(always)]
    pub(crate) async fn is_leader_cache_with_state(&self) -> Option<&Arc<AppState>> {
        if let Some(state) = &self.inner.state
            && state.id == self.inner.leader_cache.read().await.0
        {
            return Some(state);
        }
        None
    }

    #[cfg(not(feature = "dashboard"))]
    #[inline(always)]
    pub(crate) fn new_request_id(&self) -> usize {
        self.inner.request_id.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "dashboard")]
    #[inline(always)]
    pub(crate) fn new_request_id(&self) -> usize {
        if let Some(st) = &self.inner.state {
            st.new_request_id()
        } else {
            self.inner.request_id.fetch_add(1, Ordering::Relaxed)
        }
    }

    #[inline]
    pub(crate) async fn was_leader_update_error(
        &self,
        err: &Error,
        lock: &Arc<RwLock<(NodeId, String)>>,
    ) -> bool {
        let Some((leader_id, node)) = err.is_forward_to_leader() else {
            return false;
        };

        let Some(leader_tx) = self.leader_change_sender(lock) else {
            return false;
        };

        if self.inner.proxy_mode {
            // The stream manager observes the exact ForwardToLeader response
            // before delivering it and owns the one proxy rotation for that
            // socket generation. Retrying here cannot enqueue a second,
            // stale rotation after EOF or a concurrent refusal.
            return true;
        }

        if let (Some(leader_id), Some(node)) = (leader_id, node.clone()) {
            if !change_leader_and_wait(&leader_tx, leader_id, node).await {
                return false;
            }
        } else {
            // A resumed follower can reject a write before it has learned the
            // new leader, yielding ForwardToLeader(None, None). That response
            // is definitive evidence the write was not accepted. Recover in a
            // detached, explicitly bounded task so raw Client callers cannot
            // hang forever and cancellation by a higher-level timeout cannot
            // interrupt recovery. Discovery is side-effect-free; only the
            // stream manager atomically applies the authenticated result.
            let Some(recovery) = self.leader_recovery(lock) else {
                return false;
            };
            let (generation, receiver, starter) = recovery.join_or_begin().await;
            if let Some(sender) = starter {
                let client = self.clone();
                let recovery = Arc::clone(&recovery);
                let leader = Arc::clone(lock);
                let tx = leader_tx.clone();
                tokio::spawn(async move {
                    let succeeded = match time::timeout(
                        LEADER_DISCOVERY_TIMEOUT,
                        client.discover_active_leader(&leader),
                    )
                    .await
                    {
                        Ok(Ok((leader_id, node))) => {
                            change_leader_and_wait(&tx, leader_id, node).await
                        }
                        Ok(Err(_)) | Err(_) => false,
                    };
                    recovery.complete(generation, sender, succeeded).await;
                });
            }
            if !LeaderRecovery::wait(receiver).await {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "sqlite")]
    use std::collections::VecDeque;
    use std::future;

    #[tokio::test]
    async fn configured_leader_probes_do_not_wait_for_the_first_peer() {
        let mut probes = JoinSet::new();
        probes.spawn(async { future::pending::<Option<u64>>().await });
        probes.spawn(async { Some(42) });

        let result = time::timeout(Duration::from_millis(100), first_some(probes))
            .await
            .expect("a later healthy peer must not wait for the first peer");
        assert_eq!(result, Some(42));
    }

    #[test]
    fn direct_leader_recovery_and_proxy_rotation_keep_distinct_bounds() {
        #[cfg(feature = "sqlite")]
        assert_eq!(LEADER_REQUEST_MAX_ATTEMPTS, 3);
        assert_eq!(LEADER_DISCOVERY_TIMEOUT, Duration::from_secs(8));
        assert_eq!(crate::LEADER_STREAM_CONNECT_TIMEOUT, Duration::from_secs(5));
        assert_eq!(LEADER_STREAM_HANDOFF_TIMEOUT, Duration::from_secs(6));
        assert_eq!(
            crate::LEADER_RETRY_RECOVERY_TIMEOUT,
            Duration::from_secs(14)
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn two_consecutive_unaccepted_requests_recover_before_the_third_attempt() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recoveries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut results = VecDeque::from([
            Err(Error::Connect("first leader changed".to_owned())),
            Err(Error::Connect("replacement still electing".to_owned())),
            Ok(42_u8),
        ]);

        let value = retry_request_after_leader_change(
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                future::ready(results.pop_front().expect("bounded request attempt"))
            },
            |error| {
                recoveries.fetch_add(1, Ordering::Relaxed);
                future::ready((error, true))
            },
        )
        .await
        .expect("the third exact request reaches the stable leader");

        assert_eq!(value, 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(recoveries.load(Ordering::Relaxed), 2);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn retained_undispatched_request_retries_without_leader_discovery() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recoveries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut results = VecDeque::from([
            Err(Error::RequestNotDispatched(
                "writer stopped before dispatch".into(),
            )),
            Ok(42_u8),
        ]);

        let value = retry_request_after_leader_change(
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                future::ready(results.pop_front().expect("bounded request attempt"))
            },
            |error| {
                recoveries.fetch_add(1, Ordering::Relaxed);
                let retryable = matches!(&error, Error::RequestNotDispatched(_));
                future::ready((error, retryable))
            },
        )
        .await
        .expect("the retained request is safe to retry on the replacement stream");

        assert_eq!(value, 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(recoveries.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn leader_retry_stays_bounded_and_does_not_recover_terminal_errors() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recoveries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error = retry_request_after_leader_change(
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                future::ready(Err::<(), _>(Error::Connect("still electing".to_owned())))
            },
            |error| {
                recoveries.fetch_add(1, Ordering::Relaxed);
                future::ready((error, true))
            },
        )
        .await
        .expect_err("three unaccepted requests exhaust the bound");
        assert!(error.to_string().contains("still electing"));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(recoveries.load(Ordering::Relaxed), 2);

        let terminal_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let terminal_recoveries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        retry_request_after_leader_change(
            || {
                terminal_attempts.fetch_add(1, Ordering::Relaxed);
                future::ready(Err::<(), _>(Error::BadRequest("terminal".into())))
            },
            |error| {
                terminal_recoveries.fetch_add(1, Ordering::Relaxed);
                future::ready((error, false))
            },
        )
        .await
        .expect_err("a terminal error is returned immediately");
        assert_eq!(terminal_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(terminal_recoveries.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn every_sqlite_leader_operation_uses_the_successive_redirect_retry() {
        let operations = [
            ("transaction", include_str!("transaction.rs")),
            ("execute", include_str!("execute.rs")),
            ("batch", include_str!("batch.rs")),
            ("query", include_str!("query.rs")),
            ("watermark", include_str!("mgmt.rs")),
            ("migration", include_str!("migrate.rs")),
            ("backup", include_str!("backup.rs")),
        ];

        for (operation, source) in operations {
            assert!(
                source.contains("retry_db_after_leader_change"),
                "{operation} must use the bounded successive-redirect retry"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_attempt_and_later_retry_share_one_detached_recovery() {
        let recovery = Arc::new(LeaderRecovery::default());
        let (generation, first_receiver, starter) = recovery.join_or_begin().await;
        let sender = starter.expect("the first caller starts recovery");

        let first_waiter = tokio::spawn(async move {
            time::timeout(Duration::from_secs(3), LeaderRecovery::wait(first_receiver)).await
        });
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(3)).await;
        assert!(first_waiter.await.expect("first waiter").is_err());

        let (retry_generation, retry_receiver, retry_starter) = recovery.join_or_begin().await;
        assert_eq!(retry_generation, generation);
        assert!(
            retry_starter.is_none(),
            "a later Store attempt must not fan out another leader probe"
        );

        recovery.complete(generation, sender, true).await;
        assert!(LeaderRecovery::wait(retry_receiver).await);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_completes_only_after_the_replacement_stream_is_ready() {
        let (tx, rx) = flume::bounded(1);
        let node = Node {
            id: 9,
            addr_raft: "node-nine:21001".to_owned(),
            addr_api: "node-nine:21000".to_owned(),
        };
        let recovery = tokio::spawn(async move { change_leader_and_wait(&tx, 9, node).await });
        let request = match rx.recv_async().await.expect("leader change request") {
            ClientStreamControl::Leader(request) => request,
            #[cfg(feature = "dashboard")]
            ClientStreamControl::DashboardLeader(..) => panic!("expected leader control"),
        };
        let Some(ready) = request.ready else {
            panic!("expected an acknowledged leader change");
        };
        assert_eq!(request.leader_id, 9);

        time::advance(Duration::from_secs(4)).await;
        assert!(
            !recovery.is_finished(),
            "discovering a target is not recovery until its stream is open"
        );
        ready.send(()).expect("replacement stream acknowledgement");
        assert!(recovery.await.expect("recovery task"));
    }

    #[tokio::test(start_paused = true)]
    async fn full_control_channel_is_inside_the_handoff_deadline() {
        let (tx, _rx) = flume::bounded(1);
        tx.send_async(ClientStreamControl::Leader(ClientLeaderChange {
            leader_id: 1,
            node: Node {
                id: 1,
                addr_raft: "queued:21001".to_owned(),
                addr_api: "queued:21000".to_owned(),
            },
            ready: None,
        }))
        .await
        .expect("fill control channel");
        let node = Node {
            id: 9,
            addr_raft: "node-nine:21001".to_owned(),
            addr_api: "node-nine:21000".to_owned(),
        };
        let recovery = tokio::spawn(async move { change_leader_and_wait(&tx, 9, node).await });
        tokio::task::yield_now().await;
        time::advance(LEADER_STREAM_HANDOFF_TIMEOUT).await;
        assert!(!recovery.await.expect("bounded handoff"));
    }

    #[tokio::test(start_paused = true)]
    async fn queued_handoff_allows_a_near_boundary_successful_connect() {
        let (tx, rx) = flume::bounded(1);
        tx.send_async(ClientStreamControl::Leader(ClientLeaderChange {
            leader_id: 1,
            node: Node {
                id: 1,
                addr_raft: "queued:21001".to_owned(),
                addr_api: "queued:21000".to_owned(),
            },
            ready: None,
        }))
        .await
        .expect("fill control channel");
        let node = Node {
            id: 9,
            addr_raft: "node-nine:21001".to_owned(),
            addr_api: "node-nine:21000".to_owned(),
        };
        let recovery = tokio::spawn(async move { change_leader_and_wait(&tx, 9, node).await });
        tokio::task::yield_now().await;
        time::advance(Duration::from_millis(500)).await;
        let _queued = rx.recv_async().await.expect("drain queued control message");
        let request = match rx.recv_async().await.expect("receive leader handoff") {
            ClientStreamControl::Leader(request) => request,
            #[cfg(feature = "dashboard")]
            ClientStreamControl::DashboardLeader(..) => panic!("expected leader control"),
        };
        let Some(ready) = request.ready else {
            panic!("expected handoff acknowledgement");
        };
        time::advance(Duration::from_millis(4_900)).await;
        ready
            .send(())
            .expect("near-boundary stream acknowledgement");
        assert!(recovery.await.expect("successful handoff"));
    }
}
