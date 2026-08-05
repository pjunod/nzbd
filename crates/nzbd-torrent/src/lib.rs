//! BitTorrent engine boundary for nzbd.
//!
//! M0 deliberately keeps this crate disconnected from the daemon. It proves
//! the pinned engine and the process-wide TLS provider before queue admission,
//! persistence, API, or configuration can start peer-to-peer traffic.

#![forbid(unsafe_code)]

use librqbit::api::TorrentIdOrHash;
use librqbit::{
    AddTorrent, AddTorrentOptions, ManagedTorrent, Session, SessionOptions, TorrentStatsState,
};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

/// The exact engine release the M0 contract and interop tests describe.
pub const ENGINE_VERSION: &str = "8.1.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoProviderInstall {
    InstalledAwsLc,
    AlreadyInstalled,
}

#[derive(Debug, thiserror::Error)]
pub enum TorrentError {
    #[error("could not install the process rustls crypto provider")]
    CryptoProvider,
    #[error("torrent engine: {0}")]
    Engine(String),
    #[error("torrent engine returned no managed handle")]
    MissingHandle,
    #[error("BitTorrent v2-only magnets are not supported by librqbit 8.1.1")]
    UnsupportedV2Magnet,
    #[error("hybrid BitTorrent v1/v2 magnets are not supported in the first release")]
    UnsupportedHybridMagnet,
    #[error("invalid SOCKS proxy configuration: {0}")]
    InvalidProxy(&'static str),
    #[error(
        "private torrents must declare exactly one unique tracker in the first release (found {0})"
    )]
    PrivateTrackerCount(usize),
}

fn engine_error(error: impl std::fmt::Display) -> TorrentError {
    TorrentError::Engine(error.to_string())
}

/// Install nzbd's aws-lc provider before librqbit constructs its reqwest
/// rustls client.
///
/// `librqbit`'s Rust-TLS feature brings ring-backed hashing/TLS features into
/// a workspace that already enables aws-lc. rustls 0.23 cannot infer a process
/// default when both providers are compiled, so daemon startup must call this
/// before starting either network stack.
pub fn install_process_crypto_provider() -> Result<CryptoProviderInstall, TorrentError> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(CryptoProviderInstall::AlreadyInstalled);
    }

    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => Ok(CryptoProviderInstall::InstalledAwsLc),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => {
            Ok(CryptoProviderInstall::AlreadyInstalled)
        }
        Err(_) => Err(TorrentError::CryptoProvider),
    }
}

#[derive(Debug, Clone, Default)]
pub struct TorrentSessionConfig {
    pub dht: bool,
    pub listen_port_range: Option<Range<u16>>,
    pub upnp_port_forwarding: bool,
    pub proxy: Option<TorrentProxyConfig>,
}

#[derive(Clone, Default)]
pub struct TorrentProxyConfig {
    /// A credential-free `socks5://host:port` URL.
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for TorrentProxyConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TorrentProxyConfig")
            .field("url", &self.url)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl TorrentProxyConfig {
    fn engine_url(&self) -> Result<String, TorrentError> {
        let mut url = url::Url::parse(&self.url)
            .map_err(|_| TorrentError::InvalidProxy("URL is not valid"))?;
        if url.scheme() != "socks5" {
            return Err(TorrentError::InvalidProxy("URL scheme must be socks5"));
        }
        if url.host_str().is_none() || url.port().is_none() {
            return Err(TorrentError::InvalidProxy(
                "URL must contain a host and port",
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(TorrentError::InvalidProxy(
                "put credentials in the separate username and password fields",
            ));
        }
        match (&self.username, &self.password) {
            (None, None) => {}
            (Some(username), Some(password)) if !username.is_empty() => {
                if !username.bytes().all(is_url_unreserved)
                    || !password.bytes().all(is_url_unreserved)
                {
                    return Err(TorrentError::InvalidProxy(
                        "credentials may contain only URL-unreserved ASCII characters",
                    ));
                }
                url.set_username(username)
                    .map_err(|_| TorrentError::InvalidProxy("username is not URL-safe"))?;
                url.set_password(Some(password))
                    .map_err(|_| TorrentError::InvalidProxy("password is not URL-safe"))?;
            }
            _ => {
                return Err(TorrentError::InvalidProxy(
                    "username and password must be set together",
                ));
            }
        }
        Ok(url.into())
    }
}

fn is_url_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

#[derive(Debug, Clone, Default)]
pub struct TorrentAddConfig {
    pub paused: bool,
    pub overwrite: bool,
    pub initial_peers: Vec<SocketAddr>,
}

#[derive(Clone)]
pub struct TorrentSession {
    inner: Arc<Session>,
}

impl TorrentSession {
    pub async fn start(
        output_root: PathBuf,
        config: TorrentSessionConfig,
    ) -> Result<Self, TorrentError> {
        install_process_crypto_provider()?;
        let socks_proxy_url = config
            .proxy
            .as_ref()
            .map(TorrentProxyConfig::engine_url)
            .transpose()?;
        let options = SessionOptions {
            disable_dht: !config.dht,
            disable_dht_persistence: !config.dht,
            listen_port_range: config.listen_port_range,
            enable_upnp_port_forwarding: config.upnp_port_forwarding,
            socks_proxy_url,
            ..Default::default()
        };
        let inner = Session::new_with_opts(output_root, options)
            .await
            .map_err(engine_error)?;
        Ok(Self { inner })
    }

    pub fn tcp_listen_port(&self) -> Option<u16> {
        self.inner.tcp_listen_port()
    }

    pub fn set_download_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.inner.ratelimits.set_download_bps(bytes_per_second);
    }

    pub fn set_upload_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.inner.ratelimits.set_upload_bps(bytes_per_second);
    }

    pub async fn add_metainfo(
        &self,
        bytes: Vec<u8>,
        config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        validate_private_tracker_contract(&bytes)?;
        self.add(AddTorrent::from_bytes(bytes), config).await
    }

    pub async fn add_magnet(
        &self,
        magnet: String,
        config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        let lower = magnet.to_ascii_lowercase();
        let has_v1 = lower.contains("urn:btih:");
        let has_v2 = lower.contains("urn:btmh:");
        match (has_v1, has_v2) {
            (true, true) => return Err(TorrentError::UnsupportedHybridMagnet),
            (false, true) => return Err(TorrentError::UnsupportedV2Magnet),
            _ => {}
        }
        self.add(AddTorrent::from_url(magnet), config).await
    }

    async fn add(
        &self,
        source: AddTorrent<'_>,
        config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        let options = AddTorrentOptions {
            paused: config.paused,
            overwrite: config.overwrite,
            initial_peers: Some(config.initial_peers),
            ..Default::default()
        };
        let handle = self
            .inner
            .add_torrent(source, Some(options))
            .await
            .map_err(engine_error)?
            .into_handle()
            .ok_or(TorrentError::MissingHandle)?;
        Ok(TorrentHandle { inner: handle })
    }

    pub async fn pause(&self, torrent: &TorrentHandle) -> Result<(), TorrentError> {
        self.inner.pause(&torrent.inner).await.map_err(engine_error)
    }

    pub async fn resume(&self, torrent: &TorrentHandle) -> Result<(), TorrentError> {
        self.inner
            .unpause(&torrent.inner)
            .await
            .map_err(engine_error)
    }

    /// Forget a torrent idempotently, optionally deleting the files the
    /// engine's parsed metainfo owns. Higher layers still prove the persisted
    /// canonical root before they may request `delete_files`.
    pub async fn delete(
        &self,
        torrent: &TorrentHandle,
        delete_files: bool,
    ) -> Result<(), TorrentError> {
        let id = TorrentIdOrHash::Id(torrent.id());
        if self.inner.get(id).is_none() {
            return Ok(());
        }
        self.inner
            .delete(id, delete_files)
            .await
            .map_err(engine_error)
    }

    pub async fn stop(&self) {
        self.inner.stop().await;
    }
}

fn validate_private_tracker_contract(bytes: &[u8]) -> Result<(), TorrentError> {
    let metainfo =
        librqbit::torrent_from_bytes::<librqbit::ByteBuf<'_>>(bytes).map_err(engine_error)?;
    if !metainfo.info.private {
        return Ok(());
    }
    let trackers = metainfo
        .iter_announce()
        .map(AsRef::<[u8]>::as_ref)
        .collect::<HashSet<_>>();
    if trackers.len() != 1 {
        return Err(TorrentError::PrivateTrackerCount(trackers.len()));
    }
    Ok(())
}

#[derive(Clone)]
pub struct TorrentHandle {
    inner: Arc<ManagedTorrent>,
}

impl TorrentHandle {
    pub fn id(&self) -> usize {
        self.inner.id()
    }

    pub fn name(&self) -> Option<String> {
        self.inner.name()
    }

    pub fn info_hash(&self) -> String {
        self.inner.info_hash().as_string()
    }

    pub fn is_paused(&self) -> bool {
        self.inner.is_paused()
    }

    pub fn stats(&self) -> TorrentStats {
        let stats = self.inner.stats();
        let phase = match stats.state {
            TorrentStatsState::Initializing => TorrentPhase::Initializing,
            TorrentStatsState::Live => TorrentPhase::Live,
            TorrentStatsState::Paused => TorrentPhase::Paused,
            TorrentStatsState::Error => TorrentPhase::Error,
        };
        let (download_bps, upload_bps, peers) = stats
            .live
            .as_ref()
            .map(|live| {
                let peers = &live.snapshot.peer_stats;
                (
                    mib_per_second_to_bps(live.download_speed.mbps),
                    mib_per_second_to_bps(live.upload_speed.mbps),
                    TorrentPeerStats {
                        queued: peers.queued,
                        connecting: peers.connecting,
                        live: peers.live,
                        seen: peers.seen,
                        dead: peers.dead,
                    },
                )
            })
            .unwrap_or_default();
        let eta_seconds = (download_bps > 0 && stats.progress_bytes < stats.total_bytes)
            .then(|| (stats.total_bytes - stats.progress_bytes).div_ceil(download_bps));
        TorrentStats {
            phase,
            progress_bytes: stats.progress_bytes,
            uploaded_bytes: stats.uploaded_bytes,
            total_bytes: stats.total_bytes,
            file_progress_bytes: stats.file_progress,
            download_bps,
            upload_bps,
            eta_seconds,
            peers,
            finished: stats.finished,
            error: stats.error,
        }
    }

    pub async fn wait_until_initialized(&self) -> Result<(), TorrentError> {
        self.inner
            .wait_until_initialized()
            .await
            .map_err(engine_error)
    }

    pub async fn wait_until_completed(&self) -> Result<(), TorrentError> {
        self.inner
            .wait_until_completed()
            .await
            .map_err(engine_error)
    }
}

fn mib_per_second_to_bps(mib_per_second: f64) -> u64 {
    if !mib_per_second.is_finite() || mib_per_second <= 0.0 {
        return 0;
    }
    (mib_per_second * 1024.0 * 1024.0)
        .round()
        .clamp(0.0, u64::MAX as f64) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentPhase {
    Initializing,
    Live,
    Paused,
    Error,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentPeerStats {
    pub queued: usize,
    pub connecting: usize,
    pub live: usize,
    pub seen: usize,
    pub dead: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentStats {
    pub phase: TorrentPhase,
    pub progress_bytes: u64,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    pub file_progress_bytes: Vec<u64>,
    pub download_bps: u64,
    pub upload_bps: u64,
    pub eta_seconds: Option<u64>,
    pub peers: TorrentPeerStats,
    pub finished: bool,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_credentials_are_separate_and_redacted() {
        let proxy = TorrentProxyConfig {
            url: "socks5://127.0.0.1:1080".into(),
            username: Some("alice".into()),
            password: Some("do-not-log-me".into()),
        };
        assert_eq!(
            proxy.engine_url().unwrap(),
            "socks5://alice:do-not-log-me@127.0.0.1:1080"
        );
        let shown = format!("{proxy:?}");
        assert!(shown.contains("***"));
        assert!(!shown.contains("do-not-log-me"));
    }

    #[test]
    fn proxy_refuses_embedded_or_ambiguous_credentials() {
        let embedded = TorrentProxyConfig {
            url: "socks5://alice:secret@127.0.0.1:1080".into(),
            ..Default::default()
        };
        assert!(matches!(
            embedded.engine_url(),
            Err(TorrentError::InvalidProxy(_))
        ));

        let partial = TorrentProxyConfig {
            url: "socks5://127.0.0.1:1080".into(),
            username: Some("alice".into()),
            password: None,
        };
        assert!(matches!(
            partial.engine_url(),
            Err(TorrentError::InvalidProxy(_))
        ));
    }
}
