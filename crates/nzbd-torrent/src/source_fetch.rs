use percent_encoding::percent_decode;
use reqwest::header::LOCATION;
use reqwest::{Client, StatusCode};
use std::time::Duration;
use url::Url;

use crate::{
    install_process_crypto_provider, validate_metainfo_contract_with_limit, TorrentError,
    DEFAULT_MAX_METAINFO_BYTES, DEFAULT_TORRENT_SOURCE_CONNECT_TIMEOUT,
    DEFAULT_TORRENT_SOURCE_TOTAL_TIMEOUT, MAX_CONFIGURED_MAX_METAINFO_BYTES,
    MAX_TORRENT_SOURCE_REDIRECTS, MIN_CONFIGURED_MAX_METAINFO_BYTES,
};

/// Resource limits for one authenticated HTTP(S) `.torrent` fetch.
///
/// The helper is deliberately not wired to daemon configuration or API input.
/// These values make the dormant boundary reviewable before that integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentSourceFetchLimits {
    pub max_metainfo_bytes: usize,
    pub max_redirects: usize,
    pub connect_timeout: Duration,
    pub total_timeout: Duration,
}

impl Default for TorrentSourceFetchLimits {
    fn default() -> Self {
        Self {
            max_metainfo_bytes: DEFAULT_MAX_METAINFO_BYTES,
            max_redirects: MAX_TORRENT_SOURCE_REDIRECTS,
            connect_timeout: DEFAULT_TORRENT_SOURCE_CONNECT_TIMEOUT,
            total_timeout: DEFAULT_TORRENT_SOURCE_TOTAL_TIMEOUT,
        }
    }
}

impl TorrentSourceFetchLimits {
    fn validate(self) -> Result<Self, TorrentError> {
        if !(MIN_CONFIGURED_MAX_METAINFO_BYTES..=MAX_CONFIGURED_MAX_METAINFO_BYTES)
            .contains(&self.max_metainfo_bytes)
        {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "metainfo limit must be between 1 MiB and 100 MiB",
            ));
        }
        if self.max_redirects > MAX_TORRENT_SOURCE_REDIRECTS {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "redirect limit cannot exceed 5",
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "connect timeout must be non-zero",
            ));
        }
        if self.total_timeout.is_zero() {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "total timeout must be non-zero",
            ));
        }
        if self.connect_timeout > self.total_timeout {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "connect timeout cannot exceed total timeout",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
struct SourceCredentials {
    username: String,
    password: Option<String>,
}

/// Fetch and preflight one authenticated HTTP(S) `.torrent` source.
///
/// Redirects are followed manually so every target is revalidated. Basic URL
/// credentials survive only same-origin redirects, no response cookies are
/// retained, the body is bounded while it streams, and the total timeout spans
/// the complete redirect chain and body. Returned bytes have passed the same
/// metainfo, path, geometry, privacy, and tracker checks used before engine
/// admission. This helper starts no torrent session and performs no payload I/O.
pub async fn fetch_torrent_source(
    source: &str,
    limits: TorrentSourceFetchLimits,
    engine_proxy_enabled: bool,
) -> Result<Vec<u8>, TorrentError> {
    let limits = limits.validate()?;
    let mut source_url = Url::parse(source)
        .map_err(|_| TorrentError::InvalidTorrentSource("URL syntax is not valid"))?;
    validate_source_url(&source_url)?;
    let credentials = take_source_credentials(&mut source_url)?;
    let timeout_origin = safe_origin(&source_url);

    install_process_crypto_provider()?;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(limits.connect_timeout)
        .build()
        .map_err(|_| TorrentError::TorrentSourceRequestFailed {
            origin: timeout_origin.clone(),
        })?;

    match tokio::time::timeout(
        limits.total_timeout,
        fetch_redirect_chain(
            &client,
            source_url,
            credentials,
            limits,
            engine_proxy_enabled,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(TorrentError::TorrentSourceTimeout {
            origin: timeout_origin,
        }),
    }
}

async fn fetch_redirect_chain(
    client: &Client,
    mut source_url: Url,
    mut credentials: Option<SourceCredentials>,
    limits: TorrentSourceFetchLimits,
    engine_proxy_enabled: bool,
) -> Result<Vec<u8>, TorrentError> {
    let mut redirects = 0usize;
    loop {
        let origin = safe_origin(&source_url);
        let mut request = client.get(source_url.clone());
        if let Some(credentials) = &credentials {
            request = request.basic_auth(&credentials.username, credentials.password.as_deref());
        }
        let mut response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                TorrentError::TorrentSourceTimeout {
                    origin: origin.clone(),
                }
            } else {
                TorrentError::TorrentSourceRequestFailed {
                    origin: origin.clone(),
                }
            }
        })?;

        if is_followed_redirect(response.status()) {
            if redirects == limits.max_redirects {
                return Err(TorrentError::TooManyTorrentSourceRedirects {
                    limit: limits.max_redirects,
                });
            }
            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "Location header is missing",
                })?
                .to_str()
                .map_err(|_| TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "Location header is not valid text",
                })?;
            let mut next_url = source_url.join(location).map_err(|_| {
                TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "target URL syntax is not valid",
                }
            })?;
            validate_source_url(&next_url).map_err(|_| {
                TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "target must be an HTTP(S) URL with a host",
                }
            })?;

            let same_origin = source_url.origin() == next_url.origin();
            clear_source_credentials(&mut next_url);
            if !same_origin {
                credentials = None;
            }
            source_url = next_url;
            redirects += 1;
            continue;
        }

        if !response.status().is_success() {
            return Err(TorrentError::TorrentSourceHttpStatus {
                status: response.status().as_u16(),
                origin,
            });
        }

        if let Some(size) = response.content_length() {
            if size > limits.max_metainfo_bytes as u64 {
                return Err(TorrentError::MetainfoTooLarge {
                    size: usize::try_from(size).unwrap_or(usize::MAX),
                    limit: limits.max_metainfo_bytes,
                });
            }
        }

        let capacity = response
            .content_length()
            .and_then(|size| usize::try_from(size).ok())
            .unwrap_or(0);
        let mut bytes = Vec::with_capacity(capacity);
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                TorrentError::TorrentSourceTimeout {
                    origin: origin.clone(),
                }
            } else {
                TorrentError::TorrentSourceRequestFailed {
                    origin: origin.clone(),
                }
            }
        })? {
            let next_size = bytes.len().saturating_add(chunk.len());
            if next_size > limits.max_metainfo_bytes {
                return Err(TorrentError::MetainfoTooLarge {
                    size: next_size,
                    limit: limits.max_metainfo_bytes,
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        validate_metainfo_contract_with_limit(
            &bytes,
            engine_proxy_enabled,
            limits.max_metainfo_bytes,
        )?;
        return Ok(bytes);
    }
}

fn validate_source_url(url: &Url) -> Result<(), TorrentError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TorrentError::InvalidTorrentSource(
            "scheme must be http or https",
        ));
    }
    if url.host_str().is_none() {
        return Err(TorrentError::InvalidTorrentSource("host is required"));
    }
    Ok(())
}

fn take_source_credentials(url: &mut Url) -> Result<Option<SourceCredentials>, TorrentError> {
    let credentials = if url.username().is_empty() && url.password().is_none() {
        None
    } else {
        let username = percent_decode(url.username().as_bytes())
            .decode_utf8()
            .map_err(|_| TorrentError::InvalidTorrentSource("username must contain valid UTF-8"))?
            .into_owned();
        let password = url
            .password()
            .map(|password| {
                percent_decode(password.as_bytes())
                    .decode_utf8()
                    .map(|password| password.into_owned())
                    .map_err(|_| {
                        TorrentError::InvalidTorrentSource("password must contain valid UTF-8")
                    })
            })
            .transpose()?;
        Some(SourceCredentials { username, password })
    };
    clear_source_credentials(url);
    Ok(credentials)
}

fn clear_source_credentials(url: &mut Url) {
    url.set_username("")
        .expect("HTTP(S) URLs support clearing usernames");
    url.set_password(None)
        .expect("HTTP(S) URLs support clearing passwords");
}

fn safe_origin(url: &Url) -> String {
    url.origin().ascii_serialization()
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_metainfo_limits_accept_the_exact_range() {
        for max_metainfo_bytes in [
            MIN_CONFIGURED_MAX_METAINFO_BYTES,
            MAX_CONFIGURED_MAX_METAINFO_BYTES,
        ] {
            assert!(TorrentSourceFetchLimits {
                max_metainfo_bytes,
                ..TorrentSourceFetchLimits::default()
            }
            .validate()
            .is_ok());
        }

        for max_metainfo_bytes in [
            MIN_CONFIGURED_MAX_METAINFO_BYTES - 1,
            MAX_CONFIGURED_MAX_METAINFO_BYTES + 1,
        ] {
            assert!(matches!(
                TorrentSourceFetchLimits {
                    max_metainfo_bytes,
                    ..TorrentSourceFetchLimits::default()
                }
                .validate(),
                Err(TorrentError::InvalidTorrentSourceLimits(_))
            ));
        }
    }

    #[test]
    fn redirect_and_timeout_limits_fail_closed() {
        assert!(TorrentSourceFetchLimits {
            max_redirects: MAX_TORRENT_SOURCE_REDIRECTS,
            ..TorrentSourceFetchLimits::default()
        }
        .validate()
        .is_ok());
        assert!(matches!(
            TorrentSourceFetchLimits {
                max_redirects: MAX_TORRENT_SOURCE_REDIRECTS + 1,
                ..TorrentSourceFetchLimits::default()
            }
            .validate(),
            Err(TorrentError::InvalidTorrentSourceLimits(_))
        ));

        for limits in [
            TorrentSourceFetchLimits {
                connect_timeout: Duration::ZERO,
                ..TorrentSourceFetchLimits::default()
            },
            TorrentSourceFetchLimits {
                total_timeout: Duration::ZERO,
                ..TorrentSourceFetchLimits::default()
            },
            TorrentSourceFetchLimits {
                connect_timeout: Duration::from_secs(2),
                total_timeout: Duration::from_secs(1),
                ..TorrentSourceFetchLimits::default()
            },
        ] {
            assert!(matches!(
                limits.validate(),
                Err(TorrentError::InvalidTorrentSourceLimits(_))
            ));
        }
    }
}
