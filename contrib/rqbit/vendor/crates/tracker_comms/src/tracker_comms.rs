use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::future::Either;
use futures::future::Shared;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use tracing::debug;
use tracing::error_span;
use tracing::trace;
use tracing::Instrument;
use url::Url;

use crate::tracker_comms_http;
use crate::tracker_comms_http::TrackerRequestEvent;
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

const HTTP_TRACKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HTTP_TRACKER_RESPONSE_BYTES: usize = 1024 * 1024;
const MIN_TRACKER_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);
/// First retry after a failed announce; doubles per consecutive failure.
const TRACKER_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(60);
const MAX_TRACKER_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Statistics poll while a download may still finish in this session, so the
/// tracker hears `completed` promptly.
const TRACKER_STATS_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Statistics poll once complete: keeps the totals reported by the final
/// `stopped` announce reasonably current at a low cost.
const TRACKER_IDLE_STATS_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Upper bound on each best-effort `stopped` announce (libtorrent's
/// `stop_tracker_timeout` is 1 s). Shutdown and restarts wait on it.
const TRACKER_STOPPED_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(2);
/// `left` reported while the torrent's size is not yet known. Reporting 0
/// would tell the tracker this client is a seeder.
const UNKNOWN_LEFT_BYTES: u64 = 16 * 1024;

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    tcp_listen_port: Option<u16>,
    reqwest_client: reqwest::Client,
    udp_client: UdpTrackerClient,
    /// BEP 3 / BEP 15 `key`: random per tracker session and stable across its
    /// announces, so a tracker can recognize this client after an IP change.
    key: u32,
    /// Whether this stream is a torrent's tracker session (`started`,
    /// `completed`, `stopped`). Magnet metadata lookups only ask for peers.
    lifecycle: Option<TrackerLifecycleRegistry>,
    observed: parking_lot::Mutex<ObservedTransfer>,
    completion: tokio::sync::Notify,
    /// Trackers that were sent `started` and must be told `stopped`.
    started_trackers: parking_lot::Mutex<Vec<StoppedTarget>>,
    _activity: Option<ActivityGuard>,
}

/// Session-wide bookkeeping for tracker sessions, so a session can wait for
/// its final `stopped` announces and a restarted torrent does not announce
/// `started` before its previous `stopped` has gone out.
#[derive(Clone, Default)]
pub struct TrackerLifecycleRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Default)]
struct RegistryInner {
    /// Live tracker sessions plus `stopped` announces still in flight.
    active: AtomicUsize,
    idle: tokio::sync::Notify,
    pending_stops: parking_lot::Mutex<HashMap<Id20, Shared<BoxFuture<'static, ()>>>>,
}

struct ActivityGuard {
    inner: Arc<RegistryInner>,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        if self.inner.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.idle.notify_waiters();
        }
    }
}

impl TrackerLifecycleRegistry {
    fn activity(&self) -> ActivityGuard {
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        ActivityGuard {
            inner: self.inner.clone(),
        }
    }

    fn pending_stop(&self, info_hash: Id20) -> Option<Shared<BoxFuture<'static, ()>>> {
        let mut pending = self.inner.pending_stops.lock();
        pending.retain(|_, stop| stop.peek().is_none());
        pending.get(&info_hash).cloned()
    }

    fn set_pending_stop(&self, info_hash: Id20, stop: Shared<BoxFuture<'static, ()>>) {
        let mut pending = self.inner.pending_stops.lock();
        pending.retain(|_, stop| stop.peek().is_none());
        pending.insert(info_hash, stop);
    }

    /// Wait, at most `timeout`, until every tracker session has ended and its
    /// `stopped` announces have finished. Call after pausing every torrent
    /// and before cancelling the session's network tasks.
    pub async fn wait_for_stopped_announces(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, async {
            loop {
                let idle = self.inner.idle.notified();
                tokio::pin!(idle);
                idle.as_mut().enable();
                if self.inner.active.load(Ordering::Acquire) == 0 {
                    return;
                }
                idle.await;
            }
        })
        .await;
    }
}

/// Transfer totals reported in an announce.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AnnounceTransfer {
    uploaded: u64,
    downloaded: u64,
    left: u64,
}

/// What the tracker session has observed of the torrent so far.
#[derive(Clone, Copy, Debug, Default)]
struct ObservedTransfer {
    /// Latest totals while live; the final `stopped` reports these.
    live: Option<AnnounceTransfer>,
    /// The download was incomplete at some point in this session.
    seen_incomplete: bool,
    /// The torrent is live and has every selected byte.
    complete: bool,
}

#[derive(Clone, Debug)]
enum StoppedTarget {
    Http {
        url: Url,
        tracker_id: Option<Vec<u8>>,
    },
    Udp {
        url: Url,
        host_port: (String, u16),
    },
}

impl StoppedTarget {
    fn url(&self) -> &Url {
        match self {
            StoppedTarget::Http { url, .. } | StoppedTarget::Udp { url, .. } => url,
        }
    }
}

/// One tracker's view of the announce lifecycle: `started` until one is
/// accepted, then `completed` exactly once if a download that was incomplete
/// in this session finishes, and no event otherwise. `stopped` is sent when
/// the tracker session ends.
#[derive(Debug, Default)]
struct AnnounceLifecycle {
    started: bool,
    completed: bool,
}

impl AnnounceLifecycle {
    fn next_event(&self, observed: &ObservedTransfer) -> Option<TrackerRequestEvent> {
        if !self.started {
            Some(TrackerRequestEvent::Started)
        } else if !self.completed && observed.seen_incomplete && observed.complete {
            Some(TrackerRequestEvent::Completed)
        } else {
            None
        }
    }

    fn completion_pending(&self, observed: &ObservedTransfer) -> bool {
        self.started && self.next_event(observed) == Some(TrackerRequestEvent::Completed)
    }

    fn accepted(&mut self, event: Option<TrackerRequestEvent>) {
        match event {
            Some(TrackerRequestEvent::Started) => self.started = true,
            Some(TrackerRequestEvent::Completed) => self.completed = true,
            Some(TrackerRequestEvent::Stopped) | None => {}
        }
    }
}

fn udp_event(event: Option<TrackerRequestEvent>) -> u32 {
    match event {
        None => tracker_comms_udp::EVENT_NONE,
        Some(TrackerRequestEvent::Started) => tracker_comms_udp::EVENT_STARTED,
        Some(TrackerRequestEvent::Completed) => tracker_comms_udp::EVENT_COMPLETED,
        Some(TrackerRequestEvent::Stopped) => tracker_comms_udp::EVENT_STOPPED,
    }
}

fn http_key(key: u32) -> String {
    format!("{key:08X}")
}

/// Append announce parameters to the tracker URL without discarding the
/// query it already carries (private trackers commonly put the passkey there).
fn announce_url(tracker_url: &Url, announce_query: &str) -> Url {
    let mut url = tracker_url.clone();
    match tracker_url.query() {
        Some(existing) if !existing.is_empty() => {
            url.set_query(Some(&format!("{existing}&{announce_query}")))
        }
        _ => url.set_query(Some(announce_query)),
    }
    url
}

/// Scheme, host and port only: announce URLs carry passkeys in their path or
/// query, and announce queries carry the session key.
fn tracker_endpoint(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", url.scheme()),
        (Some(host), None) => format!("{}://{host}", url.scheme()),
        _ => format!("{}://<unknown>", url.scheme()),
    }
}

fn error_retry_interval(consecutive_failures: u32) -> Duration {
    let doublings = consecutive_failures.saturating_sub(1).min(16);
    TRACKER_ERROR_RETRY_INTERVAL
        .saturating_mul(1 << doublings)
        .min(MAX_TRACKER_ERROR_RETRY_INTERVAL)
}

#[derive(Default)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

#[derive(Default)]
pub struct TrackerCommsStats {
    /// Payload bytes uploaded since this tracker session started.
    pub uploaded_bytes: u64,
    /// Payload bytes downloaded since this tracker session started. This is
    /// not verified progress: data that was already on disk is not counted.
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    /// Selected bytes still needed before the download is complete.
    pub left_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        self.left_bytes
    }

    pub fn is_completed(&self) -> bool {
        self.left_bytes == 0
    }

    fn is_live(&self) -> bool {
        matches!(self.torrent_state, TrackerCommsStatsState::Live)
    }

    fn transfer(&self) -> AnnounceTransfer {
        let size_unknown =
            matches!(self.torrent_state, TrackerCommsStatsState::None) || self.total_bytes == 0;
        AnnounceTransfer {
            uploaded: self.uploaded_bytes,
            downloaded: self.downloaded_bytes,
            left: if size_unknown {
                self.left_bytes.max(UNKNOWN_LEFT_BYTES)
            } else {
                self.left_bytes
            },
        }
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

fn normalized_tracker_interval(interval: Duration) -> Duration {
    interval.max(MIN_TRACKER_ANNOUNCE_INTERVAL)
}

async fn fetch_http_tracker_response(
    client: &reqwest::Client,
    url: Url,
    request_timeout: Duration,
    max_response_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    tokio::time::timeout(request_timeout, async {
        // reqwest errors embed the request URL, which carries the passkey
        // and the session key.
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        match response.content_length() {
            Some(length) if length > max_response_bytes as u64 => anyhow::bail!(
                "tracker response is too large ({length} bytes; maximum {max_response_bytes})"
            ),
            _ => {}
        }

        let mut bytes = Vec::with_capacity(
            response
                .content_length()
                .map(|length| length as usize)
                .unwrap_or_default()
                .min(max_response_bytes),
        );
        while let Some(chunk) = response.chunk().await.map_err(reqwest::Error::without_url)? {
            let new_length = bytes
                .len()
                .checked_add(chunk.len())
                .context("tracker response length overflow")?;
            if new_length > max_response_bytes {
                anyhow::bail!(
                    "tracker response is too large ({new_length} bytes; maximum {max_response_bytes})"
                );
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    })
    .await
    .context("tracker request timed out")?
}

enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedTracker::Udp(u) | SupportedTracker::Http(u) => {
                f.write_str(&tracker_endpoint(u))
            }
        }
    }
}

impl TrackerComms {
    /// Start announcing to `trackers` and stream the peers they return.
    ///
    /// With `lifecycle` set this is the torrent's tracker session: announces
    /// carry `started`, `completed` and, when the stream is dropped,
    /// `stopped`. Without it (magnet metadata lookups) announces only ask
    /// for peers.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        tcp_listen_port: Option<u16>,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        lifecycle: Option<TrackerLifecycleRegistry>,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        Self::start_with_stats_poll_interval(
            info_hash,
            peer_id,
            trackers,
            stats,
            force_interval,
            tcp_listen_port,
            reqwest_client,
            udp_client,
            lifecycle,
            (
                TRACKER_STATS_POLL_INTERVAL,
                TRACKER_IDLE_STATS_POLL_INTERVAL,
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_stats_poll_interval(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        tcp_listen_port: Option<u16>,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        lifecycle: Option<TrackerLifecycleRegistry>,
        (active_poll, idle_poll): (Duration, Duration),
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!("unsuppoted tracker URL: {}", tracker_endpoint(&t));
                    None
                }
            })
            .collect::<Vec<_>>();
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let s = async_stream::stream! {
            use futures::StreamExt;
            // A restarted torrent must not reach the tracker with `started`
            // before the previous session's `stopped`.
            if let Some(previous_stop) = lifecycle
                .as_ref()
                .and_then(|registry| registry.pending_stop(info_hash))
            {
                let _ = tokio::time::timeout(
                    TRACKER_STOPPED_ANNOUNCE_TIMEOUT + Duration::from_secs(1),
                    previous_stop,
                )
                .await;
            }
            let comms = Arc::new(Self {
                info_hash,
                peer_id,
                stats,
                force_tracker_interval: force_interval,
                tx,
                tcp_listen_port,
                reqwest_client,
                udp_client,
                key: rand::random(),
                _activity: lifecycle.as_ref().map(TrackerLifecycleRegistry::activity),
                lifecycle,
                observed: Default::default(),
                completion: tokio::sync::Notify::new(),
                started_trackers: Default::default(),
            });
            comms.observe();
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                futures.push(comms.add_tracker(tracker))
            }
            let poll = tokio::time::sleep(active_poll);
            tokio::pin!(poll);
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        if let Some(Err(e)) = e {
                            debug!("error: {e}");
                        }
                    }
                    _ = &mut poll => {
                        comms.observe();
                        let next = if comms.observed.lock().complete { idle_poll } else { active_poll };
                        poll.as_mut().reset(tokio::time::Instant::now() + next);
                    }
                }
            }
        };

        Some(s.boxed())
    }

    fn add_tracker(
        &self,
        url: SupportedTracker,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        match url {
            SupportedTracker::Udp(url) => {
                let span = error_span!(parent: None, "udp_tracker", tracker = %tracker_endpoint(&url), info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, self.udp_client.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = error_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %tracker_endpoint(&url),
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(url)
                    .instrument(span)
                    .left_future()
            }
        }
    }

    /// Read the torrent's statistics, record what the tracker session has
    /// seen, and wake announce loops when the download has just finished.
    fn observe(&self) -> AnnounceTransfer {
        let stats = self.stats.get();
        let became_complete = {
            let mut observed = self.observed.lock();
            let was_complete = observed.complete;
            if stats.is_live() {
                observed.live = Some(stats.transfer());
                // Payload fetched in this session also proves the download
                // was incomplete, even if a poll never caught it mid-way.
                if !stats.is_completed() || stats.downloaded_bytes > 0 {
                    observed.seen_incomplete = true;
                }
            }
            observed.complete = stats.is_live() && stats.is_completed();
            observed.complete && !was_complete
        };
        if became_complete {
            self.completion.notify_waiters();
        }
        stats.transfer()
    }

    fn next_event(&self, lifecycle: &AnnounceLifecycle) -> Option<TrackerRequestEvent> {
        self.lifecycle.as_ref()?;
        lifecycle.next_event(&self.observed.lock())
    }

    /// Sleep until the next regular announce. After a successful announce,
    /// wake early when the download finishes so `completed` goes out
    /// promptly; after a failure, wait out the retry interval regardless.
    async fn sleep_until_next_announce(
        &self,
        interval: Duration,
        lifecycle: &AnnounceLifecycle,
        wake_on_completion: bool,
    ) {
        let deadline = tokio::time::Instant::now() + interval;
        loop {
            let completion = self.completion.notified();
            tokio::pin!(completion);
            completion.as_mut().enable();
            if wake_on_completion
                && self.lifecycle.is_some()
                && lifecycle.completion_pending(&self.observed.lock())
            {
                trace!("download completed; announcing early");
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return,
                _ = completion => {}
            }
        }
    }

    fn remember_started(&self, target: StoppedTarget) {
        if self.lifecycle.is_none() {
            return;
        }
        let mut started = self.started_trackers.lock();
        match started.iter_mut().find(|t| t.url() == target.url()) {
            Some(existing) => *existing = target,
            None => started.push(target),
        }
    }

    fn http_request(
        &self,
        event: Option<TrackerRequestEvent>,
        transfer: AnnounceTransfer,
        tracker_id: Option<Vec<u8>>,
    ) -> tracker_comms_http::TrackerRequest {
        tracker_comms_http::TrackerRequest {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            port: self.tcp_listen_port.unwrap_or(0),
            uploaded: transfer.uploaded,
            downloaded: transfer.downloaded,
            left: transfer.left,
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: None,
            key: Some(http_key(self.key)),
            trackerid: tracker_id,
        }
    }

    fn udp_request(
        &self,
        event: Option<TrackerRequestEvent>,
        transfer: AnnounceTransfer,
    ) -> tracker_comms_udp::AnnounceFields {
        tracker_comms_udp::AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            downloaded: transfer.downloaded,
            left: transfer.left,
            uploaded: transfer.uploaded,
            event: udp_event(event),
            key: self.key,
            port: self.tcp_listen_port.unwrap_or(0),
        }
    }

    fn retry_interval(&self, consecutive_failures: u32) -> Duration {
        self.force_tracker_interval
            .unwrap_or_else(|| error_retry_interval(consecutive_failures))
    }

    async fn task_single_tracker_monitor_http(&self, tracker_url: Url) -> anyhow::Result<()> {
        trace!("starting monitor");
        let mut lifecycle = AnnounceLifecycle::default();
        let mut tracker_id: Option<Vec<u8>> = None;
        let mut consecutive_failures = 0u32;
        loop {
            let transfer = self.observe();
            let event = self.next_event(&lifecycle);
            let target = |tracker_id: &Option<Vec<u8>>| StoppedTarget::Http {
                url: tracker_url.clone(),
                tracker_id: tracker_id.clone(),
            };
            // The tracker may register a `started` whose response never
            // arrives; it still gets a `stopped`.
            if event == Some(TrackerRequestEvent::Started) {
                self.remember_started(target(&tracker_id));
            }
            let request = self.http_request(event, transfer, tracker_id.clone());
            let url = announce_url(&tracker_url, &request.as_querystring());

            let (interval, succeeded) = match self.tracker_one_request_http(url).await {
                Ok(response) => {
                    consecutive_failures = 0;
                    lifecycle.accepted(event);
                    let response_interval = response.interval;
                    if response.tracker_id.is_some() {
                        tracker_id = response.tracker_id;
                    }
                    if lifecycle.started {
                        self.remember_started(target(&tracker_id));
                    }
                    let interval = self.force_tracker_interval.unwrap_or_else(|| {
                        normalized_tracker_interval(Duration::from_secs(response_interval))
                    });
                    debug!(?interval, "tracker announce accepted");
                    (interval, true)
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    debug!("error calling the tracker: {e:#}");
                    (self.retry_interval(consecutive_failures), false)
                }
            };
            self.sleep_until_next_announce(interval, &lifecycle, succeeded)
                .await;
        }
    }

    async fn tracker_one_request_http(
        &self,
        tracker_url: Url,
    ) -> anyhow::Result<HttpAnnounceOutcome> {
        debug!("calling tracker over http");
        let bytes = fetch_http_tracker_response(
            &self.reqwest_client,
            tracker_url,
            HTTP_TRACKER_REQUEST_TIMEOUT,
            MAX_HTTP_TRACKER_RESPONSE_BYTES,
        )
        .await?;
        let response = parse_http_announce_response(&bytes)?;

        for peer in response.peers {
            self.tx.send(peer).await?;
        }
        Ok(response.outcome)
    }

    async fn task_single_tracker_monitor_udp(
        &self,
        url: Url,
        client: UdpTrackerClient,
    ) -> anyhow::Result<()> {
        if url.scheme() != "udp" {
            bail!("expected UDP scheme in {}", tracker_endpoint(&url));
        }
        let hp: (String, u16) = (
            url.host_str().context("missing host")?.to_owned(),
            url.port().context("missing port")?,
        );

        let mut lifecycle = AnnounceLifecycle::default();
        let mut consecutive_failures = 0u32;
        loop {
            let transfer = self.observe();
            let event = self.next_event(&lifecycle);
            let target = || StoppedTarget::Udp {
                url: url.clone(),
                host_port: hp.clone(),
            };
            if event == Some(TrackerRequestEvent::Started) {
                self.remember_started(target());
            }
            let request = self.udp_request(event, transfer);

            let (interval, succeeded) = match client.announce(&hp, request).await {
                Ok(response) => {
                    trace!(len = response.addrs.len(), "received announce response");
                    consecutive_failures = 0;
                    lifecycle.accepted(event);
                    let response_interval = response.interval;
                    for addr in response.addrs {
                        self.tx
                            .send(SocketAddr::V4(addr))
                            .await
                            .context("rx closed")?;
                    }
                    let interval = self.force_tracker_interval.unwrap_or_else(|| {
                        normalized_tracker_interval(Duration::from_secs(response_interval as u64))
                    });
                    (interval, true)
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    debug!("error reading announce response: {e:#}");
                    (self.retry_interval(consecutive_failures), false)
                }
            };
            trace!(?interval, "sleeping");
            self.sleep_until_next_announce(interval, &lifecycle, succeeded)
                .await;
        }
    }
}

struct HttpAnnounceOutcome {
    interval: u64,
    tracker_id: Option<Vec<u8>>,
}

struct HttpAnnounceResponse {
    outcome: HttpAnnounceOutcome,
    peers: Vec<SocketAddr>,
}

fn parse_http_announce_response(bytes: &[u8]) -> anyhow::Result<HttpAnnounceResponse> {
    if let Ok(error) = bencode::from_bytes::<tracker_comms_http::TrackerError>(bytes) {
        anyhow::bail!(
            "tracker returned failure. Failure reason: {}",
            error.failure_reason
        )
    };
    let response = bencode::from_bytes::<tracker_comms_http::TrackerResponse>(bytes)?;
    Ok(HttpAnnounceResponse {
        outcome: HttpAnnounceOutcome {
            interval: response.interval,
            tracker_id: response.tracker_id.map(|id| id.as_ref().to_vec()),
        },
        peers: response.peers.iter_sockaddrs().collect(),
    })
}

impl Drop for TrackerComms {
    /// Tell every tracker that was sent `started` that this client stopped,
    /// so it stops handing out a peer that is gone and closes its accounting
    /// for the session. Best effort and bounded; skipped without a runtime.
    fn drop(&mut self) {
        let Some(registry) = self.lifecycle.clone() else {
            return;
        };
        let targets = std::mem::take(&mut *self.started_trackers.lock());
        if targets.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let transfer = self.observed.lock().live.unwrap_or_default();
        let http_client = self.reqwest_client.clone();
        let udp_client = self.udp_client.clone();
        let requests = targets
            .into_iter()
            .map(|target| match target {
                StoppedTarget::Http { url, tracker_id } => {
                    let request =
                        self.http_request(Some(TrackerRequestEvent::Stopped), transfer, tracker_id);
                    Either::Left(announce_url(&url, &request.as_querystring()))
                }
                StoppedTarget::Udp { host_port, .. } => Either::Right((
                    host_port,
                    self.udp_request(Some(TrackerRequestEvent::Stopped), transfer),
                )),
            })
            .collect::<Vec<_>>();
        let info_hash = self.info_hash;
        let activity = registry.activity();
        let stop = runtime.spawn(
            async move {
                let _activity = activity;
                let announces = requests.into_iter().map(|request| {
                    let http_client = http_client.clone();
                    let udp_client = udp_client.clone();
                    async move {
                        let result =
                            tokio::time::timeout(TRACKER_STOPPED_ANNOUNCE_TIMEOUT, async {
                                match request {
                                    Either::Left(url) => fetch_http_tracker_response(
                                        &http_client,
                                        url,
                                        TRACKER_STOPPED_ANNOUNCE_TIMEOUT,
                                        MAX_HTTP_TRACKER_RESPONSE_BYTES,
                                    )
                                    .await
                                    .map(|_| ()),
                                    Either::Right((host_port, fields)) => {
                                        udp_client.announce(&host_port, fields).await.map(|_| ())
                                    }
                                }
                            })
                            .await;
                        match result {
                            Ok(Ok(())) => trace!("stopped announce delivered"),
                            Ok(Err(e)) => debug!("stopped announce failed: {e:#}"),
                            Err(_) => debug!("stopped announce timed out"),
                        }
                    }
                });
                futures::future::join_all(announces).await;
            }
            .instrument(error_span!(parent: None, "tracker_stopped", info_hash = ?info_hash)),
        );
        registry.set_pending_stop(
            info_hash,
            async move {
                let _ = stop.await;
            }
            .boxed()
            .shared(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn one_response_server(response: Vec<u8>, delay: Duration) -> Url {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request);
            std::thread::sleep(delay);
            let _ = stream.write_all(&response);
        });
        Url::parse(&format!("http://{address}/announce")).unwrap()
    }

    #[tokio::test]
    async fn http_tracker_response_is_bounded_and_timed() {
        let client = reqwest::Client::new();
        let body = b"tracker-response";
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let exact = fetch_http_tracker_response(
            &client,
            one_response_server(response, Duration::ZERO),
            Duration::from_secs(1),
            body.len(),
        )
        .await
        .unwrap();
        assert_eq!(exact, body);

        let oversized = b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\n";
        let error = fetch_http_tracker_response(
            &client,
            one_response_server(oversized.to_vec(), Duration::ZERO),
            Duration::from_secs(1),
            16,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("too large"));

        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\n0123456789abcdef\r\n1\r\ng\r\n0\r\n\r\n";
        let error = fetch_http_tracker_response(
            &client,
            one_response_server(chunked.to_vec(), Duration::ZERO),
            Duration::from_secs(1),
            16,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("too large"));

        let error = fetch_http_tracker_response(
            &client,
            one_response_server(Vec::new(), Duration::from_millis(100)),
            Duration::from_millis(10),
            16,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
    }

    #[derive(Clone, Default)]
    struct FakeStats(Arc<parking_lot::Mutex<(u64, u64, u64)>>);

    impl FakeStats {
        fn set(&self, uploaded: u64, downloaded: u64, left: u64) {
            *self.0.lock() = (uploaded, downloaded, left);
        }
    }

    impl TorrentStatsProvider for FakeStats {
        fn get(&self) -> TrackerCommsStats {
            let (uploaded_bytes, downloaded_bytes, left_bytes) = *self.0.lock();
            TrackerCommsStats {
                uploaded_bytes,
                downloaded_bytes,
                total_bytes: 1000,
                left_bytes,
                torrent_state: TrackerCommsStatsState::Live,
            }
        }
    }

    #[test]
    fn announce_url_keeps_the_trackers_own_query() {
        let passkey = Url::parse("https://t.example/announce.php?passkey=abc123").unwrap();
        assert_eq!(
            announce_url(&passkey, "info_hash=x&port=1").as_str(),
            "https://t.example/announce.php?passkey=abc123&info_hash=x&port=1"
        );
        let plain = Url::parse("https://t.example/abc123/announce").unwrap();
        assert_eq!(
            announce_url(&plain, "info_hash=x").as_str(),
            "https://t.example/abc123/announce?info_hash=x"
        );
        let empty = Url::parse("https://t.example/announce?").unwrap();
        assert_eq!(
            announce_url(&empty, "info_hash=x").as_str(),
            "https://t.example/announce?info_hash=x"
        );
    }

    fn observed(seen_incomplete: bool, complete: bool) -> ObservedTransfer {
        ObservedTransfer {
            live: None,
            seen_incomplete,
            complete,
        }
    }

    #[test]
    fn lifecycle_sends_started_until_accepted_then_completed_once() {
        use TrackerRequestEvent as E;
        let mut lifecycle = AnnounceLifecycle::default();
        let downloading = observed(true, false);
        assert_eq!(lifecycle.next_event(&downloading), Some(E::Started));
        // A failed started announce is retried.
        lifecycle.accepted(None);
        assert_eq!(lifecycle.next_event(&downloading), Some(E::Started));
        lifecycle.accepted(Some(E::Started));
        assert_eq!(lifecycle.next_event(&downloading), None);
        assert!(!lifecycle.completion_pending(&downloading));

        let done = observed(true, true);
        assert!(lifecycle.completion_pending(&done));
        assert_eq!(lifecycle.next_event(&done), Some(E::Completed));
        lifecycle.accepted(Some(E::Completed));
        assert_eq!(lifecycle.next_event(&done), None);
        assert!(!lifecycle.completion_pending(&done));
    }

    #[test]
    fn lifecycle_never_reports_completed_for_a_session_that_started_complete() {
        use TrackerRequestEvent as E;
        let mut lifecycle = AnnounceLifecycle::default();
        let seeding = observed(false, true);
        assert_eq!(lifecycle.next_event(&seeding), Some(E::Started));
        lifecycle.accepted(Some(E::Started));
        assert_eq!(lifecycle.next_event(&seeding), None);
        assert!(!lifecycle.completion_pending(&seeding));
    }

    async fn test_comms(stats: FakeStats) -> TrackerComms {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        TrackerComms {
            info_hash: Id20::new([1; 20]),
            peer_id: Id20::new([2; 20]),
            stats: Box::new(stats),
            force_tracker_interval: None,
            tx,
            tcp_listen_port: Some(6881),
            reqwest_client: reqwest::Client::new(),
            udp_client: UdpTrackerClient::new(Default::default()).await.unwrap(),
            key: 7,
            lifecycle: Some(TrackerLifecycleRegistry::default()),
            observed: Default::default(),
            completion: tokio::sync::Notify::new(),
            started_trackers: Default::default(),
            _activity: None,
        }
    }

    #[tokio::test]
    async fn observation_needs_a_live_incomplete_session_before_completion() {
        // Started complete with nothing fetched: a seeder, never `completed`.
        let stats = FakeStats::default();
        stats.set(0, 0, 0);
        let seeder = test_comms(stats).await;
        seeder.observe();
        let observed = *seeder.observed.lock();
        assert!(observed.complete && !observed.seen_incomplete);

        // Finished before any poll saw it incomplete: payload fetched in the
        // session proves the download happened here.
        let stats = FakeStats::default();
        stats.set(0, 500, 0);
        let finished = test_comms(stats).await;
        finished.observe();
        let observed = *finished.observed.lock();
        assert!(observed.complete && observed.seen_incomplete);
        assert_eq!(observed.live.unwrap().downloaded, 500);

        // An unknown size is never reported as a seeder.
        assert_eq!(
            TrackerCommsStats::default().transfer().left,
            UNKNOWN_LEFT_BYTES
        );
    }

    #[test]
    fn failed_announces_back_off() {
        assert_eq!(error_retry_interval(1), Duration::from_secs(60));
        assert_eq!(error_retry_interval(2), Duration::from_secs(120));
        assert_eq!(error_retry_interval(5), Duration::from_secs(960));
        assert_eq!(error_retry_interval(6), MAX_TRACKER_ERROR_RETRY_INTERVAL);
        assert_eq!(
            error_retry_interval(u32::MAX),
            MAX_TRACKER_ERROR_RETRY_INTERVAL
        );
    }

    #[test]
    fn logged_tracker_endpoints_omit_passkeys() {
        let url = Url::parse("https://u:p@t.example:8443/secret/announce?passkey=abc").unwrap();
        assert_eq!(tracker_endpoint(&url), "https://t.example:8443");
        assert_eq!(
            format!("{:?}", SupportedTracker::Http(url)),
            "https://t.example:8443"
        );
    }

    #[test]
    fn udp_events_use_bep15_codes() {
        use TrackerRequestEvent as E;
        assert_eq!(udp_event(None), 0);
        assert_eq!(udp_event(Some(E::Completed)), 1);
        assert_eq!(udp_event(Some(E::Started)), 2);
        assert_eq!(udp_event(Some(E::Stopped)), 3);
        assert_eq!(http_key(0xAB), "000000AB");
    }

    #[test]
    fn http_response_tracker_id_is_parsed() {
        let body = b"d8:completei1e10:incompletei2e8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe110:tracker id3:t-1e";
        let parsed = parse_http_announce_response(body).unwrap();
        assert_eq!(parsed.outcome.interval, 1800);
        assert_eq!(parsed.outcome.tracker_id.as_deref(), Some(&b"t-1"[..]));
        assert_eq!(
            parsed.peers,
            vec!["127.0.0.1:6881".parse::<SocketAddr>().unwrap()]
        );
    }

    /// Serves every announce with a fixed response and reports each request
    /// head (request line and headers) in arrival order.
    fn recording_tracker() -> (Url, std::sync::mpsc::Receiver<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte) {
                        Ok(1) => head.push(byte[0]),
                        _ => break,
                    }
                }
                let body =
                    b"d8:completei0e10:incompletei0e8:intervali1800e5:peers0:10:tracker id4:tid1e";
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                response.extend_from_slice(body);
                let _ = stream.write_all(&response);
                if tx
                    .send(String::from_utf8_lossy(&head).into_owned())
                    .is_err()
                {
                    return;
                }
            }
        });
        (
            Url::parse(&format!("http://{address}/announce.php?passkey=secret")).unwrap(),
            rx,
        )
    }

    fn query_param(head: &str, name: &str) -> Option<String> {
        let target = head.lines().next()?.split(' ').nth(1)?;
        let query = target.split_once('?')?.1;
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
    }

    fn header(head: &str, name: &str) -> Option<String> {
        head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_owned())
        })
    }

    #[tokio::test]
    async fn http_announces_follow_the_tracker_lifecycle() {
        let (url, requests) = recording_tracker();
        let stats = FakeStats::default();
        stats.set(0, 0, 900);
        let peer_id = Id20::new(*b"-RN0200-abcdefghijkl");
        let stream = TrackerComms::start_with_stats_poll_interval(
            Id20::new([7; 20]),
            peer_id,
            std::iter::once(url).collect(),
            Box::new(stats.clone()),
            Some(Duration::from_secs(3600)),
            Some(6881),
            reqwest::Client::builder()
                .user_agent("Runner/0.2.0")
                .build()
                .unwrap(),
            UdpTrackerClient::new(Default::default()).await.unwrap(),
            Some(TrackerLifecycleRegistry::default()),
            (Duration::from_millis(50), Duration::from_millis(50)),
        )
        .unwrap();
        // The stream drives the announce loop; keep polling it in the
        // background until the test drops it.
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let driver = tokio::spawn(async move {
            let mut stream = stream;
            tokio::select! {
                _ = async { while stream.next().await.is_some() {} } => {}
                _ = stop_rx => {}
            }
        });
        let (requests_tx, requests_rx) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            while let Ok(head) = requests.recv() {
                if requests_tx.send(head).is_err() {
                    return;
                }
            }
        });
        let mut requests_rx = requests_rx;
        async fn next(requests: &mut tokio::sync::mpsc::UnboundedReceiver<String>) -> String {
            tokio::time::timeout(Duration::from_secs(20), requests.recv())
                .await
                .expect("tracker request timed out")
                .expect("tracker closed")
        }

        let first = next(&mut requests_rx).await;
        assert_eq!(query_param(&first, "passkey").as_deref(), Some("secret"));
        assert_eq!(query_param(&first, "event").as_deref(), Some("started"));
        assert_eq!(query_param(&first, "left").as_deref(), Some("900"));
        assert_eq!(
            header(&first, "user-agent").as_deref(),
            Some("Runner/0.2.0")
        );
        let key = query_param(&first, "key").expect("announce key");
        assert_eq!(key.len(), 8);
        assert!(query_param(&first, "peer_id")
            .unwrap()
            .starts_with("-RN0200-"));

        // Finishing the download wakes the loop long before the forced
        // one-hour interval, and completed is sent exactly once.
        stats.set(40, 900, 0);
        let second = next(&mut requests_rx).await;
        assert_eq!(query_param(&second, "event").as_deref(), Some("completed"));
        assert_eq!(query_param(&second, "left").as_deref(), Some("0"));
        assert_eq!(query_param(&second, "downloaded").as_deref(), Some("900"));
        assert_eq!(query_param(&second, "trackerid").as_deref(), Some("tid1"));
        assert_eq!(query_param(&second, "key"), Some(key.clone()));
        assert_eq!(query_param(&second, "passkey").as_deref(), Some("secret"));

        // Dropping the stream ends the session: one best-effort stopped.
        stats.set(75, 900, 0);
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop_tx.send(()).unwrap();
        driver.await.unwrap();
        let third = next(&mut requests_rx).await;
        assert_eq!(query_param(&third, "event").as_deref(), Some("stopped"));
        assert_eq!(query_param(&third, "uploaded").as_deref(), Some("75"));
        assert_eq!(query_param(&third, "trackerid").as_deref(), Some("tid1"));
        assert_eq!(query_param(&third, "key"), Some(key));
        assert_eq!(query_param(&third, "passkey").as_deref(), Some("secret"));
        assert_eq!(
            header(&third, "user-agent").as_deref(),
            Some("Runner/0.2.0")
        );
    }

    #[derive(Debug)]
    struct HttpRecord {
        head: String,
        arrived: std::time::Instant,
        responded: std::time::Instant,
    }

    /// An HTTP tracker that answers each announce from `respond` (after an
    /// optional delay) on its own thread and reports every request.
    fn scripted_http_tracker(
        respond: impl Fn(&str) -> (Duration, Vec<u8>) + Send + Sync + 'static,
    ) -> (Url, tokio::sync::mpsc::UnboundedReceiver<HttpRecord>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let respond = Arc::new(respond);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let tx = tx.clone();
                let respond = respond.clone();
                std::thread::spawn(move || {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match stream.read(&mut byte) {
                            Ok(1) => head.push(byte[0]),
                            _ => break,
                        }
                    }
                    let arrived = std::time::Instant::now();
                    let head = String::from_utf8_lossy(&head).into_owned();
                    let (delay, body) = respond(&head);
                    std::thread::sleep(delay);
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(&body);
                    let _ = stream.write_all(&response);
                    let _ = tx.send(HttpRecord {
                        head,
                        arrived,
                        responded: std::time::Instant::now(),
                    });
                });
            }
        });
        (
            Url::parse(&format!("http://{address}/announce.php?passkey=secret")).unwrap(),
            rx,
        )
    }

    const ACCEPT: &[u8] =
        b"d8:completei0e10:incompletei0e8:intervali1800e5:peers0:10:tracker id4:tid1e";

    /// Drive a tracker stream on a task until the returned sender fires.
    fn drive(
        stream: BoxStream<'static, SocketAddr>,
    ) -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let driver = tokio::spawn(async move {
            let mut stream = stream;
            tokio::select! {
                _ = async { while stream.next().await.is_some() {} } => {}
                _ = stop_rx => {}
            }
        });
        (stop_tx, driver)
    }

    async fn start_http(
        url: &Url,
        stats: &FakeStats,
        lifecycle: Option<TrackerLifecycleRegistry>,
    ) -> BoxStream<'static, SocketAddr> {
        TrackerComms::start_with_stats_poll_interval(
            Id20::new([9; 20]),
            Id20::new(*b"-RN0200-abcdefghijkl"),
            std::iter::once(url.clone()).collect(),
            Box::new(stats.clone()),
            None,
            Some(6881),
            reqwest::Client::new(),
            UdpTrackerClient::new(Default::default()).await.unwrap(),
            lifecycle,
            (Duration::from_millis(50), Duration::from_millis(50)),
        )
        .unwrap()
    }

    async fn next_http(rx: &mut tokio::sync::mpsc::UnboundedReceiver<HttpRecord>) -> HttpRecord {
        tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("tracker request timed out")
            .expect("tracker closed")
    }

    async fn assert_quiet(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<HttpRecord>,
        window: Duration,
    ) {
        if let Ok(Some(record)) = tokio::time::timeout(window, rx.recv()).await {
            panic!("unexpected tracker request: {}", record.head);
        }
    }

    #[tokio::test]
    async fn a_rejected_completed_waits_for_the_retry_interval() {
        let (url, mut requests) = scripted_http_tracker(|head| {
            if head.contains("event=started") {
                (Duration::ZERO, ACCEPT.to_vec())
            } else {
                (Duration::ZERO, b"d14:failure reason4:nopee".to_vec())
            }
        });
        let stats = FakeStats::default();
        stats.set(0, 0, 900);
        let (stop, driver) = drive(start_http(&url, &stats, Some(Default::default())).await);
        let started = next_http(&mut requests).await;
        assert_eq!(
            query_param(&started.head, "event").as_deref(),
            Some("started")
        );

        stats.set(0, 900, 0);
        let completed = next_http(&mut requests).await;
        assert_eq!(
            query_param(&completed.head, "event").as_deref(),
            Some("completed")
        );
        // Rejected: the next attempt waits for the 60 s retry, even though
        // the 50 ms statistics poll still sees a pending completion.
        assert_quiet(&mut requests, Duration::from_secs(1)).await;
        stop.send(()).unwrap();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn peer_lookups_send_no_lifecycle_events() {
        let (url, mut requests) = scripted_http_tracker(|_| (Duration::ZERO, ACCEPT.to_vec()));
        // The torrent is not in the session yet: its size is unknown.
        let stream = TrackerComms::start_with_stats_poll_interval(
            Id20::new([9; 20]),
            Id20::new(*b"-RN0200-abcdefghijkl"),
            std::iter::once(url).collect(),
            Box::new(()),
            None,
            None,
            reqwest::Client::new(),
            UdpTrackerClient::new(Default::default()).await.unwrap(),
            None,
            (Duration::from_millis(50), Duration::from_millis(50)),
        )
        .unwrap();
        let (stop, driver) = drive(stream);
        let lookup = next_http(&mut requests).await;
        assert_eq!(query_param(&lookup.head, "event"), None);
        assert_eq!(
            query_param(&lookup.head, "left"),
            Some(UNKNOWN_LEFT_BYTES.to_string())
        );
        stop.send(()).unwrap();
        driver.await.unwrap();
        assert_quiet(&mut requests, Duration::from_millis(500)).await;
    }

    #[tokio::test]
    async fn a_restarted_session_announces_started_after_the_previous_stopped() {
        let (url, mut requests) = scripted_http_tracker(|head| {
            let delay = if head.contains("event=stopped") {
                Duration::from_millis(400)
            } else {
                Duration::ZERO
            };
            (delay, ACCEPT.to_vec())
        });
        let registry = TrackerLifecycleRegistry::default();
        let stats = FakeStats::default();
        stats.set(0, 0, 900);

        let (stop, driver) = drive(start_http(&url, &stats, Some(registry.clone())).await);
        let first = next_http(&mut requests).await;
        assert_eq!(
            query_param(&first.head, "event").as_deref(),
            Some("started")
        );
        stop.send(()).unwrap();
        driver.await.unwrap();
        let (stop, driver) = drive(start_http(&url, &stats, Some(registry.clone())).await);

        let mut records = vec![
            next_http(&mut requests).await,
            next_http(&mut requests).await,
        ];
        records.sort_by_key(|record| record.arrived);
        assert_eq!(
            query_param(&records[0].head, "event").as_deref(),
            Some("stopped")
        );
        assert_eq!(
            query_param(&records[1].head, "event").as_deref(),
            Some("started")
        );
        assert!(records[1].arrived >= records[0].responded);

        // Session shutdown can wait for the final stopped announces.
        stop.send(()).unwrap();
        driver.await.unwrap();
        let waited = std::time::Instant::now();
        registry
            .wait_for_stopped_announces(Duration::from_secs(10))
            .await;
        assert!(waited.elapsed() >= Duration::from_millis(300));
        let last = requests
            .try_recv()
            .expect("stopped delivered before the wait returned");
        assert_eq!(query_param(&last.head, "event").as_deref(), Some("stopped"));
        let idle = std::time::Instant::now();
        registry
            .wait_for_stopped_announces(Duration::from_secs(10))
            .await;
        assert!(idle.elapsed() < Duration::from_millis(100));
    }

    #[derive(Debug)]
    struct UdpAnnounce {
        event: u32,
        key: u32,
        left: u64,
        downloaded: u64,
    }

    /// A minimal BEP 15 tracker that reports every announce.
    fn udp_tracker() -> (Url, tokio::sync::mpsc::UnboundedReceiver<UdpAnnounce>) {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut buf = [0u8; 2048];
            loop {
                let Ok((len, from)) = socket.recv_from(&mut buf) else {
                    return;
                };
                let request = &buf[..len];
                let u32_at = |at: usize| {
                    u32::from_be_bytes(
                        std::convert::TryInto::try_into(&request[at..at + 4]).unwrap(),
                    )
                };
                let u64_at = |at: usize| {
                    u64::from_be_bytes(
                        std::convert::TryInto::try_into(&request[at..at + 8]).unwrap(),
                    )
                };
                let transaction = &request[12..16];
                let mut reply = Vec::new();
                match u32_at(8) {
                    0 if len >= 16 => {
                        reply.extend_from_slice(&0u32.to_be_bytes());
                        reply.extend_from_slice(transaction);
                        reply.extend_from_slice(&0x1234u64.to_be_bytes());
                    }
                    1 if len >= 98 => {
                        if tx
                            .send(UdpAnnounce {
                                downloaded: u64_at(56),
                                left: u64_at(64),
                                event: u32_at(80),
                                key: u32_at(88),
                            })
                            .is_err()
                        {
                            return;
                        }
                        reply.extend_from_slice(&1u32.to_be_bytes());
                        reply.extend_from_slice(transaction);
                        reply.extend_from_slice(&1800u32.to_be_bytes());
                        reply.extend_from_slice(&0u32.to_be_bytes());
                        reply.extend_from_slice(&0u32.to_be_bytes());
                    }
                    _ => continue,
                }
                let _ = socket.send_to(&reply, from);
            }
        });
        (
            Url::parse(&format!("udp://{address}/announce")).unwrap(),
            rx,
        )
    }

    #[tokio::test]
    async fn udp_announces_follow_the_same_lifecycle() {
        let (url, mut announces) = udp_tracker();
        let stats = FakeStats::default();
        stats.set(0, 0, 900);
        let registry = TrackerLifecycleRegistry::default();
        let (stop, driver) = drive(start_http(&url, &stats, Some(registry.clone())).await);
        async fn next(
            announces: &mut tokio::sync::mpsc::UnboundedReceiver<UdpAnnounce>,
        ) -> UdpAnnounce {
            tokio::time::timeout(Duration::from_secs(20), announces.recv())
                .await
                .expect("udp announce timed out")
                .expect("udp tracker closed")
        }
        let started = next(&mut announces).await;
        assert_eq!(started.event, tracker_comms_udp::EVENT_STARTED);
        assert_eq!(started.left, 900);
        assert_ne!(started.key, 0);

        stats.set(0, 900, 0);
        let completed = next(&mut announces).await;
        assert_eq!(completed.event, tracker_comms_udp::EVENT_COMPLETED);
        assert_eq!(completed.downloaded, 900);
        assert_eq!(completed.key, started.key);

        stop.send(()).unwrap();
        driver.await.unwrap();
        let stopped = next(&mut announces).await;
        assert_eq!(stopped.event, tracker_comms_udp::EVENT_STOPPED);
        assert_eq!(stopped.key, started.key);
        registry
            .wait_for_stopped_announces(Duration::from_secs(10))
            .await;
    }

    #[test]
    fn hostile_tracker_intervals_are_clamped() {
        assert_eq!(
            normalized_tracker_interval(Duration::from_secs(0)),
            MIN_TRACKER_ANNOUNCE_INTERVAL
        );
        assert_eq!(
            normalized_tracker_interval(Duration::from_secs(59)),
            MIN_TRACKER_ANNOUNCE_INTERVAL
        );
        assert_eq!(
            normalized_tracker_interval(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
        assert_eq!(
            normalized_tracker_interval(Duration::from_secs(61)),
            Duration::from_secs(61)
        );
    }
}
