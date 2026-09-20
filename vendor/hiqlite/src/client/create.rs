use crate::app_state::{AppState, RaftType};
use crate::client::{DbClient, LeaderRecovery};
use crate::config::RateLimitConfig;
use crate::http_client::build_http_client;
use crate::{Client, Error, tls};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use tokio::sync::{RwLock, watch};

#[cfg(feature = "listen_notify")]
use crate::client::listen_notify::remote::RemoteListener;

#[cfg(feature = "sqlite")]
use crate::client::stream::{ClientStreamControl, ClientStreamReq};

const RATE_LIMIT_AWAIT_SIZE: usize = 64;

impl Client {
    /// Create a local client that skips network connections if not necessary
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new_local(
        state: Arc<AppState>,
        nodes: Vec<String>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        tls_no_verify: bool,
        #[cfg(feature = "sqlite")] tx_client_db: flume::Sender<ClientStreamReq>,
        #[cfg(feature = "sqlite")] rx_client_db: flume::Receiver<ClientStreamReq>,
        #[cfg(feature = "sqlite")] tx_leader_db: flume::Sender<ClientStreamControl>,
        #[cfg(feature = "sqlite")] rx_leader_db: flume::Receiver<ClientStreamControl>,
        tx_shutdown: watch::Sender<bool>,
        #[cfg(feature = "cache")] rate_limit_cache: Option<RateLimitConfig>,
        #[cfg(feature = "sqlite")] rate_limit_db: Option<RateLimitConfig>,
    ) -> Self {
        let leader_id = state.id;
        let leader_addr = state.addr_api.clone();

        let secret = state.secret_api.as_bytes().to_vec();
        let api_secret = state.secret_api.clone();

        #[cfg(feature = "cache")]
        let leader_cache = Arc::new(RwLock::new((leader_id, leader_addr.clone())));
        #[cfg(feature = "cache")]
        let leader_recovery_cache = Arc::new(LeaderRecovery::default());
        #[cfg(feature = "sqlite")]
        let leader_db = Arc::new(RwLock::new((leader_id, leader_addr)));
        #[cfg(feature = "sqlite")]
        let leader_recovery_db = Arc::new(LeaderRecovery::default());

        #[cfg(feature = "cache")]
        let (tx_client_cache, rx_client_cache) = flume::bounded(1);
        #[cfg(feature = "cache")]
        let (tx_leader_cache, rx_leader_cache) = flume::bounded(1);
        #[allow(unused_variables)]
        let (rate_limit_cache_await, rx_cache_await) =
            crossbeam::channel::bounded(RATE_LIMIT_AWAIT_SIZE);
        #[allow(unused_variables)]
        let (rate_limit_db_await, rx_db_await) = crossbeam::channel::bounded(RATE_LIMIT_AWAIT_SIZE);

        let db_client = DbClient {
            state: Some(state),
            #[cfg(feature = "cache")]
            leader_cache,
            #[cfg(feature = "cache")]
            leader_recovery_cache,
            #[cfg(feature = "sqlite")]
            leader_db,
            #[cfg(feature = "sqlite")]
            leader_recovery_db,
            // Retain the authenticated roster for bounded recovery from a
            // resumed follower whose local metrics do not know the leader yet.
            nodes,
            proxy_mode: false,
            client: Some(build_http_client(tls_no_verify)),
            #[cfg(feature = "cache")]
            tx_client_cache,
            #[cfg(feature = "cache")]
            tx_leader_cache,
            #[cfg(feature = "sqlite")]
            tx_client_db,
            #[cfg(feature = "sqlite")]
            tx_leader_db,
            tls_config,
            #[cfg(feature = "cache")]
            tls_no_verify,
            api_secret: Some(api_secret),
            request_id: AtomicUsize::new(0),
            tx_shutdown: Some(tx_shutdown),
            stream_shutdown: watch::channel(false).0,
            background_handles: std::sync::Mutex::new(Vec::new()),
            #[cfg(feature = "listen_notify_local")]
            app_start: chrono::Utc::now().timestamp_micros(),
            #[cfg(feature = "listen_notify_local")]
            rx_notify: None,
            #[cfg(feature = "cache")]
            rate_limit_cache: rate_limit_cache.as_ref().map(|c| AtomicU32::new(c.rps)),
            #[cfg(feature = "cache")]
            rate_limit_cache_await,
            #[cfg(feature = "sqlite")]
            rate_limit_db: rate_limit_db.as_ref().map(|c| AtomicU32::new(c.rps)),
            #[cfg(feature = "sqlite")]
            rate_limit_db_await,
        };

        let slf = Self {
            inner: Arc::new(db_client),
        };

        slf.find_set_active_leader().await;

        #[cfg(feature = "cache")]
        slf.open_stream(
            secret.clone(),
            slf.inner.leader_cache.clone(),
            rx_client_cache,
            rx_leader_cache,
            RaftType::Cache,
        );
        #[cfg(feature = "sqlite")]
        slf.open_stream(
            secret,
            slf.inner.leader_db.clone(),
            rx_client_db,
            rx_leader_db,
            RaftType::Sqlite,
        );

        #[cfg(all(feature = "cache", feature = "sqlite"))]
        slf.spawn_rate_limit_ticker(rate_limit_cache, rate_limit_db, rx_cache_await, rx_db_await);
        #[cfg(all(feature = "cache", not(feature = "sqlite")))]
        slf.spawn_rate_limit_ticker(rate_limit_cache, None, rx_cache_await, rx_db_await);
        #[cfg(all(not(feature = "cache"), feature = "sqlite"))]
        slf.spawn_rate_limit_ticker(None, rate_limit_db, rx_cache_await, rx_db_await);

        slf
    }

    /// Manually create a remote client.
    ///
    /// Provide any nodes as address. As long as all nodes can be reached,
    /// leader changes will happen automatically.
    ///
    /// You never need to create the client yourself if your application embeds Hiqlite, as it will
    /// return a `Client` from `hiqlite::start_node()` or `hiqlite::start_node_with_cache()`.
    ///
    /// **Note:**
    /// If your client will be unable to reach all nodes, you can run the Hiqlite Server in proxy
    /// mode like mentioned in the [README](https://github.com/sebadob/hiqlite/blob/main/README.md).
    /// In this case, only provide proxy addresses in `nodes` and set `with_proxy` to `true`.
    /// Those endpoints remain authoritative across reconnects; advertised voter addresses are
    /// never used as a fallback.
    #[allow(clippy::too_many_arguments)]
    pub async fn remote(
        nodes: Vec<String>,
        tls: bool,
        tls_no_verify: bool,
        api_secret: String,
        with_proxy: bool,
        #[cfg(feature = "cache")] rate_limit_cache: Option<RateLimitConfig>,
        #[cfg(feature = "sqlite")] rate_limit_db: Option<RateLimitConfig>,
    ) -> Result<Self, Error> {
        if nodes.is_empty() {
            return Err(Error::Config(
                "You must provide at least 1 node to connect to".into(),
            ));
        }

        let tls_config = if tls {
            Some(tls::build_tls_config(tls_no_verify))
        } else {
            None
        };

        // we just use this as a placeholder to be able to initialize the remote note
        let node_id = 0;
        let node_addr = nodes[0].clone();

        #[cfg(feature = "cache")]
        let leader_cache = Arc::new(RwLock::new((node_id, node_addr.clone())));
        #[cfg(feature = "cache")]
        let leader_recovery_cache = Arc::new(LeaderRecovery::default());
        #[cfg(feature = "sqlite")]
        let leader_db = Arc::new(RwLock::new((node_id, node_addr)));
        #[cfg(feature = "sqlite")]
        let leader_recovery_db = Arc::new(LeaderRecovery::default());

        #[cfg(feature = "sqlite")]
        let (tx_client_db, rx_client_db) = flume::bounded(1);
        #[cfg(feature = "sqlite")]
        let (tx_leader_db, rx_leader_db) = flume::bounded(1);
        #[cfg(feature = "cache")]
        let (tx_client_cache, rx_client_cache) = flume::bounded(1);
        #[cfg(feature = "cache")]
        let (tx_leader_cache, rx_leader_cache) = flume::bounded(1);

        let stream_shutdown = watch::channel(false).0;
        #[allow(unused_mut)]
        let mut background_handles = Vec::new();

        #[cfg(feature = "listen_notify")]
        let rx_notify = {
            let (receiver, handle) = RemoteListener::spawn(
                leader_cache.clone(),
                tls,
                api_secret.clone(),
                stream_shutdown.subscribe(),
            );
            background_handles.push(handle);
            Some(receiver)
        };

        #[cfg(all(feature = "listen_notify_local", not(feature = "listen_notify")))]
        let rx_notify = None;

        #[allow(unused_variables)]
        let (rate_limit_cache_await, rx_cache_await) =
            crossbeam::channel::bounded(RATE_LIMIT_AWAIT_SIZE);
        #[allow(unused_variables)]
        let (rate_limit_db_await, rx_db_await) = crossbeam::channel::bounded(RATE_LIMIT_AWAIT_SIZE);

        let api_secret_bytes = api_secret.as_bytes().to_vec();

        let db_client = DbClient {
            state: None,
            #[cfg(feature = "sqlite")]
            leader_db,
            #[cfg(feature = "sqlite")]
            leader_recovery_db,
            #[cfg(feature = "cache")]
            leader_cache,
            #[cfg(feature = "cache")]
            leader_recovery_cache,
            nodes,
            proxy_mode: with_proxy,
            client: Some(build_http_client(tls_no_verify)),
            #[cfg(feature = "cache")]
            tx_client_cache,
            #[cfg(feature = "cache")]
            tx_leader_cache,
            #[cfg(feature = "sqlite")]
            tx_client_db,
            #[cfg(feature = "sqlite")]
            tx_leader_db,
            tls_config,
            #[cfg(feature = "cache")]
            tls_no_verify,
            api_secret: Some(api_secret),
            request_id: AtomicUsize::new(0),
            tx_shutdown: None,
            stream_shutdown,
            background_handles: std::sync::Mutex::new(background_handles),
            #[cfg(feature = "listen_notify_local")]
            app_start: chrono::Utc::now().timestamp_micros(),
            #[cfg(feature = "listen_notify_local")]
            rx_notify,
            #[cfg(feature = "cache")]
            rate_limit_cache: rate_limit_cache.as_ref().map(|c| AtomicU32::new(c.rps)),
            #[cfg(feature = "cache")]
            rate_limit_cache_await,
            #[cfg(feature = "sqlite")]
            rate_limit_db: rate_limit_db.as_ref().map(|c| AtomicU32::new(c.rps)),
            #[cfg(feature = "sqlite")]
            rate_limit_db_await,
        };

        let slf = Self {
            inner: Arc::new(db_client),
        };

        // Proxy endpoints remain authoritative for this client's whole life;
        // reconnect and ForwardToLeader handling preserve the same boundary.
        if !with_proxy {
            slf.find_set_active_leader().await;
        }

        #[cfg(feature = "cache")]
        slf.open_stream(
            api_secret_bytes.clone(),
            slf.inner.leader_cache.clone(),
            rx_client_cache,
            rx_leader_cache,
            RaftType::Cache,
        );
        #[cfg(feature = "sqlite")]
        slf.open_stream(
            api_secret_bytes,
            slf.inner.leader_db.clone(),
            rx_client_db,
            rx_leader_db,
            RaftType::Sqlite,
        );

        #[cfg(all(feature = "cache", feature = "sqlite"))]
        slf.spawn_rate_limit_ticker(rate_limit_cache, rate_limit_db, rx_cache_await, rx_db_await);
        #[cfg(all(feature = "cache", not(feature = "sqlite")))]
        slf.spawn_rate_limit_ticker(rate_limit_cache, None, rx_cache_await, rx_db_await);
        #[cfg(all(not(feature = "cache"), feature = "sqlite"))]
        slf.spawn_rate_limit_ticker(None, rate_limit_db, rx_cache_await, rx_db_await);

        Ok(slf)
    }
}
