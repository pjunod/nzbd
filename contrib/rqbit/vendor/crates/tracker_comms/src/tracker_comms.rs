use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use anyhow::Context;
use futures::future::Either;
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
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

const HTTP_TRACKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HTTP_TRACKER_RESPONSE_BYTES: usize = 1024 * 1024;
const MIN_TRACKER_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);
const TRACKER_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(60);
/// How often a sleeping announce loop re-reads torrent statistics, so the
/// `completed` event goes out promptly and the final `stopped` announce
/// carries current transfer totals.
const TRACKER_STATS_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Upper bound on the best-effort `stopped` announce sent when a torrent's
/// tracker stream is dropped.
const TRACKER_STOPPED_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    tcp_listen_port: Option<u16>,
    reqwest_client: reqwest::Client,
    udp_client: UdpTrackerClient,
    /// BEP 3 / BEP 15 `key`: random per tracker session, stable across its
    /// announces, so a tracker can recognize this client after an IP change.
    key: u32,
    stats_poll_interval: Duration,
    last_transfer: parking_lot::Mutex<Option<AnnounceTransfer>>,
    /// Trackers that accepted a `started` announce and must be told `stopped`.
    started_trackers: parking_lot::Mutex<Vec<StoppedTarget>>,
}

/// Transfer totals reported in an announce.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AnnounceTransfer {
    uploaded: u64,
    downloaded: u64,
    left: u64,
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

/// The announce lifecycle a tracker expects from one client session:
/// `started` until one is accepted, `completed` exactly once when a download
/// that was incomplete in this session finishes, and nothing otherwise.
/// `stopped` is sent separately when the session ends.
#[derive(Debug, Default)]
struct AnnounceLifecycle {
    started: bool,
    seen_incomplete: bool,
    completed: bool,
}

impl AnnounceLifecycle {
    fn observe(&mut self, stats: &TrackerCommsStats) {
        if matches!(stats.torrent_state, TrackerCommsStatsState::Live) && !stats.is_completed() {
            self.seen_incomplete = true;
        }
    }

    fn next_event(
        &self,
        stats: &TrackerCommsStats,
    ) -> Option<tracker_comms_http::TrackerRequestEvent> {
        use tracker_comms_http::TrackerRequestEvent as E;
        if !self.started {
            Some(E::Started)
        } else if !self.completed && self.seen_incomplete && stats.is_completed() {
            Some(E::Completed)
        } else {
            None
        }
    }

    fn completion_pending(&self, stats: &TrackerCommsStats) -> bool {
        self.started
            && self.next_event(stats) == Some(tracker_comms_http::TrackerRequestEvent::Completed)
    }

    fn accepted(&mut self, event: Option<tracker_comms_http::TrackerRequestEvent>) {
        use tracker_comms_http::TrackerRequestEvent as E;
        match event {
            Some(E::Started) => self.started = true,
            Some(E::Completed) => self.completed = true,
            Some(E::Stopped) | None => {}
        }
    }
}

fn udp_event(event: Option<tracker_comms_http::TrackerRequestEvent>) -> u32 {
    use tracker_comms_http::TrackerRequestEvent as E;
    match event {
        None => tracker_comms_udp::EVENT_NONE,
        Some(E::Started) => tracker_comms_udp::EVENT_STARTED,
        Some(E::Completed) => tracker_comms_udp::EVENT_COMPLETED,
        Some(E::Stopped) => tracker_comms_udp::EVENT_STOPPED,
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

    fn transfer(&self) -> AnnounceTransfer {
        AnnounceTransfer {
            uploaded: self.uploaded_bytes,
            downloaded: self.downloaded_bytes,
            left: self.left_bytes,
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
        let mut response = client.get(url).send().await?;
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
        while let Some(chunk) = response.chunk().await? {
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
            SupportedTracker::Udp(u) => std::fmt::Display::fmt(u, f),
            SupportedTracker::Http(u) => std::fmt::Display::fmt(u, f),
        }
    }
}

impl TrackerComms {
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
            TRACKER_STATS_POLL_INTERVAL,
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
        stats_poll_interval: Duration,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!("unsuppoted tracker URL: {}", t);
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
                stats_poll_interval,
                last_transfer: Default::default(),
                started_trackers: Default::default(),
            });
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                futures.push(comms.add_tracker(tracker))
            }
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
                let span = error_span!(parent: None, "udp_tracker", tracker = %url, info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, self.udp_client.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = error_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %url,
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(url)
                    .instrument(span)
                    .left_future()
            }
        }
    }

    /// Read the torrent's current statistics, advance the lifecycle, and keep
    /// the latest live transfer totals for the final `stopped` announce.
    fn observe_stats(&self, lifecycle: &mut AnnounceLifecycle) -> TrackerCommsStats {
        let stats = self.stats.get();
        lifecycle.observe(&stats);
        if matches!(stats.torrent_state, TrackerCommsStatsState::Live) {
            *self.last_transfer.lock() = Some(stats.transfer());
        }
        stats
    }

    /// Sleep until the next regular announce, waking early when a download
    /// finishes so the tracker hears `completed` promptly.
    async fn sleep_until_next_announce(
        &self,
        interval: Duration,
        lifecycle: &mut AnnounceLifecycle,
    ) {
        let deadline = tokio::time::Instant::now() + interval;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return;
            }
            tokio::time::sleep((deadline - now).min(self.stats_poll_interval)).await;
            let stats = self.observe_stats(lifecycle);
            if lifecycle.completion_pending(&stats) {
                trace!("download completed; announcing early");
                return;
            }
        }
    }

    fn remember_started(&self, target: StoppedTarget) {
        let mut started = self.started_trackers.lock();
        match started.iter_mut().find(|t| t.url() == target.url()) {
            Some(existing) => *existing = target,
            None => started.push(target),
        }
    }

    fn http_request(
        &self,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
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
        event: Option<tracker_comms_http::TrackerRequestEvent>,
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

    async fn task_single_tracker_monitor_http(&self, tracker_url: Url) -> anyhow::Result<()> {
        trace!(url=%tracker_url, "starting monitor");
        let mut lifecycle = AnnounceLifecycle::default();
        let mut tracker_id: Option<Vec<u8>> = None;
        loop {
            let stats = self.observe_stats(&mut lifecycle);
            let event = lifecycle.next_event(&stats);
            let request = self.http_request(event, stats.transfer(), tracker_id.clone());
            let url = announce_url(&tracker_url, &request.as_querystring());

            let interval = match self.tracker_one_request_http(url).await {
                Ok(response) => {
                    lifecycle.accepted(event);
                    let response_interval = response.interval;
                    if response.tracker_id.is_some() {
                        tracker_id = response.tracker_id;
                    }
                    if lifecycle.started {
                        self.remember_started(StoppedTarget::Http {
                            url: tracker_url.clone(),
                            tracker_id: tracker_id.clone(),
                        });
                    }
                    let interval = self.force_tracker_interval.unwrap_or_else(|| {
                        normalized_tracker_interval(Duration::from_secs(response_interval))
                    });
                    debug!(
                        "sleeping for {:?} after calling tracker {}",
                        interval,
                        tracker_url.host().unwrap()
                    );
                    interval
                }
                Err(e) => {
                    debug!("error calling the tracker {}: {:#}", tracker_url, e);
                    TRACKER_ERROR_RETRY_INTERVAL
                }
            };
            self.sleep_until_next_announce(interval, &mut lifecycle)
                .await;
        }
    }

    async fn tracker_one_request_http(
        &self,
        tracker_url: Url,
    ) -> anyhow::Result<HttpAnnounceOutcome> {
        debug!(url = %tracker_url, "calling tracker over http");
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
            bail!("expected UDP scheme in {}", url);
        }
        let hp: (String, u16) = (
            url.host_str().context("missing host")?.to_owned(),
            url.port().context("missing port")?,
        );

        let mut lifecycle = AnnounceLifecycle::default();
        loop {
            let stats = self.observe_stats(&mut lifecycle);
            let event = lifecycle.next_event(&stats);
            let request = self.udp_request(event, stats.transfer());

            let interval = match client.announce(&hp, request).await {
                Ok(response) => {
                    trace!(len = response.addrs.len(), "received announce response");
                    lifecycle.accepted(event);
                    if lifecycle.started {
                        self.remember_started(StoppedTarget::Udp {
                            url: url.clone(),
                            host_port: hp.clone(),
                        });
                    }
                    let response_interval = response.interval;
                    for addr in response.addrs {
                        self.tx
                            .send(SocketAddr::V4(addr))
                            .await
                            .context("rx closed")?;
                    }
                    self.force_tracker_interval.unwrap_or_else(|| {
                        normalized_tracker_interval(Duration::from_secs(response_interval as u64))
                    })
                }
                Err(e) => {
                    debug!(url = %url, "error reading announce response: {e:#}");
                    self.force_tracker_interval
                        .unwrap_or(TRACKER_ERROR_RETRY_INTERVAL)
                }
            };
            trace!(?interval, "sleeping");
            self.sleep_until_next_announce(interval, &mut lifecycle)
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
    /// Tell every tracker that accepted `started` that this client stopped,
    /// so it stops handing out a peer that is gone and closes its accounting
    /// for the session. Best effort: bounded, and skipped without a runtime.
    fn drop(&mut self) {
        let targets = std::mem::take(&mut *self.started_trackers.lock());
        if targets.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let transfer = self.last_transfer.lock().unwrap_or_default();
        let http_client = self.reqwest_client.clone();
        let udp_client = self.udp_client.clone();
        let requests = targets
            .into_iter()
            .map(|target| match target {
                StoppedTarget::Http { url, tracker_id } => {
                    let request = self.http_request(
                        Some(tracker_comms_http::TrackerRequestEvent::Stopped),
                        transfer,
                        tracker_id,
                    );
                    Either::Left(announce_url(&url, &request.as_querystring()))
                }
                StoppedTarget::Udp { host_port, .. } => Either::Right((
                    host_port,
                    self.udp_request(
                        Some(tracker_comms_http::TrackerRequestEvent::Stopped),
                        transfer,
                    ),
                )),
            })
            .collect::<Vec<_>>();
        let info_hash = self.info_hash;
        runtime.spawn(
            async move {
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

    fn live(left_bytes: u64) -> TrackerCommsStats {
        TrackerCommsStats {
            left_bytes,
            total_bytes: 1000,
            torrent_state: TrackerCommsStatsState::Live,
            ..Default::default()
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

    #[test]
    fn lifecycle_sends_started_until_accepted_then_completed_once() {
        use tracker_comms_http::TrackerRequestEvent as E;
        let mut lifecycle = AnnounceLifecycle::default();
        let downloading = live(10);
        lifecycle.observe(&downloading);
        assert_eq!(lifecycle.next_event(&downloading), Some(E::Started));
        // A failed started announce is retried.
        assert_eq!(lifecycle.next_event(&downloading), Some(E::Started));
        lifecycle.accepted(Some(E::Started));
        assert_eq!(lifecycle.next_event(&downloading), None);
        assert!(!lifecycle.completion_pending(&downloading));

        let done = live(0);
        lifecycle.observe(&done);
        assert!(lifecycle.completion_pending(&done));
        assert_eq!(lifecycle.next_event(&done), Some(E::Completed));
        lifecycle.accepted(Some(E::Completed));
        assert_eq!(lifecycle.next_event(&done), None);
        assert!(!lifecycle.completion_pending(&done));
    }

    #[test]
    fn lifecycle_never_reports_completed_for_a_session_that_started_complete() {
        use tracker_comms_http::TrackerRequestEvent as E;
        let mut lifecycle = AnnounceLifecycle::default();
        let seeding = live(0);
        lifecycle.observe(&seeding);
        assert_eq!(lifecycle.next_event(&seeding), Some(E::Started));
        lifecycle.accepted(Some(E::Started));
        lifecycle.observe(&seeding);
        assert_eq!(lifecycle.next_event(&seeding), None);

        // Initializing is not evidence of an incomplete download.
        let mut checking = live(500);
        checking.torrent_state = TrackerCommsStatsState::Initializing;
        let mut lifecycle = AnnounceLifecycle::default();
        lifecycle.observe(&checking);
        lifecycle.accepted(Some(E::Started));
        lifecycle.observe(&seeding);
        assert_eq!(lifecycle.next_event(&seeding), None);
    }

    #[test]
    fn udp_events_use_bep15_codes() {
        use tracker_comms_http::TrackerRequestEvent as E;
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
            Duration::from_millis(50),
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
