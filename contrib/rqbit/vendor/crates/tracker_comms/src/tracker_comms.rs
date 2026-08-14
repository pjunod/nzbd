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

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    tcp_listen_port: Option<u16>,
    reqwest_client: reqwest::Client,
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
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        let total = self.total_bytes;
        let down = self.downloaded_bytes;
        if total >= down {
            return total - down;
        }
        0
    }

    pub fn is_completed(&self) -> bool {
        self.downloaded_bytes >= self.total_bytes
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
                reqwest_client
            });
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                futures.push(comms.add_tracker(tracker, &udp_client))
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
        client: &UdpTrackerClient,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        match url {
            SupportedTracker::Udp(url) => {
                let span = error_span!(parent: None, "udp_tracker", tracker = %url, info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, client.clone())
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

    async fn task_single_tracker_monitor_http(&self, mut tracker_url: Url) -> anyhow::Result<()> {
        let mut event = Some(tracker_comms_http::TrackerRequestEvent::Started);
        trace!(url=%tracker_url, "starting monitor");
        loop {
            let stats = self.stats.get();
            let request = tracker_comms_http::TrackerRequest {
                info_hash: self.info_hash,
                peer_id: self.peer_id,
                port: self.tcp_listen_port.unwrap_or(0),
                uploaded: stats.uploaded_bytes,
                downloaded: stats.downloaded_bytes,
                left: stats.get_left_to_download_bytes(),
                compact: true,
                no_peer_id: false,
                event,
                ip: None,
                numwant: None,
                key: None,
                trackerid: None,
            };

            let request_query = request.as_querystring();
            tracker_url.set_query(Some(&request_query));

            match self.tracker_one_request_http(tracker_url.clone()).await {
                Ok(interval) => {
                    event = None;
                    let interval = self.force_tracker_interval.unwrap_or_else(|| {
                        normalized_tracker_interval(Duration::from_secs(interval))
                    });
                    debug!(
                        "sleeping for {:?} after calling tracker {}",
                        interval,
                        tracker_url.host().unwrap()
                    );
                    tokio::time::sleep(interval).await;
                }
                Err(e) => {
                    debug!("error calling the tracker {}: {:#}", tracker_url, e);
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            };
        }
    }

    async fn tracker_one_request_http(&self, tracker_url: Url) -> anyhow::Result<u64> {
        debug!(url = %tracker_url, "calling tracker over http");
        let bytes = fetch_http_tracker_response(
            &self.reqwest_client,
            tracker_url,
            HTTP_TRACKER_REQUEST_TIMEOUT,
            MAX_HTTP_TRACKER_RESPONSE_BYTES,
        )
        .await?;
        if let Ok(error) = bencode::from_bytes::<tracker_comms_http::TrackerError>(&bytes) {
            anyhow::bail!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            )
        };
        let response = bencode::from_bytes::<tracker_comms_http::TrackerResponse>(&bytes)?;

        for peer in response.peers.iter_sockaddrs() {
            self.tx.send(peer).await?;
        }
        Ok(response.interval)
    }

    async fn task_single_tracker_monitor_udp(
        &self,
        url: Url,
        client: UdpTrackerClient,
    ) -> anyhow::Result<()> {
        use tracker_comms_udp::*;

        if url.scheme() != "udp" {
            bail!("expected UDP scheme in {}", url);
        }
        let hp: (String, u16) = (
            url.host_str().context("missing host")?.to_owned(),
            url.port().context("missing port")?,
        );

        let mut sleep_interval: Option<Duration> = None;
        loop {
            if let Some(i) = sleep_interval {
                trace!(interval=?sleep_interval, "sleeping");
                tokio::time::sleep(i).await;
            }

            let stats = self.stats.get();
            let request = AnnounceFields {
                info_hash: self.info_hash,
                peer_id: self.peer_id,
                downloaded: stats.downloaded_bytes,
                left: stats.get_left_to_download_bytes(),
                uploaded: stats.uploaded_bytes,
                event: match stats.torrent_state {
                    TrackerCommsStatsState::None => EVENT_NONE,
                    TrackerCommsStatsState::Initializing => EVENT_STARTED,
                    TrackerCommsStatsState::Paused => EVENT_STOPPED,
                    TrackerCommsStatsState::Live => {
                        if stats.is_completed() {
                            EVENT_COMPLETED
                        } else {
                            EVENT_STARTED
                        }
                    }
                },
                key: 0, // whatever that is?
                port: self.tcp_listen_port.unwrap_or(0),
            };

            match client.announce(&hp, request).await {
                Ok(response) => {
                    trace!(len = response.addrs.len(), "received announce response");
                    for addr in response.addrs {
                        self.tx
                            .send(SocketAddr::V4(addr))
                            .await
                            .context("rx closed")?;
                    }
                    let new_interval =
                        normalized_tracker_interval(Duration::from_secs(response.interval as u64));
                    sleep_interval = Some(self.force_tracker_interval.unwrap_or(new_interval));
                }
                Err(e) => {
                    debug!(url = %url, "error reading announce response: {e:#}");
                    if sleep_interval.is_none() {
                        sleep_interval = Some(
                            self.force_tracker_interval
                                .unwrap_or(Duration::from_secs(60)),
                        );
                    }
                }
            }
        }
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
