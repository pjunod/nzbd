use nzbd_torrent::{
    fetch_torrent_source, TorrentError, TorrentSourceFetchLimits, MIN_CONFIGURED_MAX_METAINFO_BYTES,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const VALID_METAINFO: &[u8] =
    b"d4:infod6:lengthi1e4:name1:a12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
const BASIC_ALICE_SECRET: &str = "authorization: Basic YWxpY2U6c2VjcmV0";

struct ResponseSpec {
    bytes: Vec<u8>,
    delay: Duration,
}

impl ResponseSpec {
    fn immediate(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            delay: Duration::ZERO,
        }
    }

    fn delayed(bytes: Vec<u8>, delay: Duration) -> Self {
        Self { bytes, delay }
    }
}

async fn spawn_server(responses: Vec<ResponseSpec>) -> (SocketAddr, JoinHandle<Vec<String>>) {
    spawn_server_with(|_| responses).await
}

async fn spawn_server_with(
    responses: impl FnOnce(SocketAddr) -> Vec<ResponseSpec>,
) -> (SocketAddr, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind loopback server");
    let address = listener.local_addr().expect("loopback address");
    let responses = responses(address);
    let task = tokio::spawn(async move {
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            requests.push(String::from_utf8_lossy(&request).into_owned());
            if !response.delay.is_zero() {
                tokio::time::sleep(response.delay).await;
            }
            let _ = stream.write_all(&response.bytes).await;
        }
        requests
    });
    (address, task)
}

fn response(status: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 {status}\r\n").into_bytes();
    for (name, value) in headers {
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    {
        bytes.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    }
    bytes.extend_from_slice(b"Connection: close\r\n\r\n");
    bytes.extend_from_slice(body);
    bytes
}

fn chunked_response(body: &[u8]) -> Vec<u8> {
    let mut bytes =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for chunk in body.chunks(64 * 1024) {
        bytes.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        bytes.extend_from_slice(chunk);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(b"0\r\n\r\n");
    bytes
}

fn source_url(address: SocketAddr, path: &str) -> String {
    format!("http://{address}{path}")
}

fn short_limits() -> TorrentSourceFetchLimits {
    TorrentSourceFetchLimits {
        connect_timeout: Duration::from_millis(200),
        total_timeout: Duration::from_secs(2),
        ..TorrentSourceFetchLimits::default()
    }
}

fn request_has_header(request: &str, expected: &str) -> bool {
    request
        .lines()
        .any(|line| line.eq_ignore_ascii_case(expected))
}

#[tokio::test]
async fn authenticated_fetch_returns_only_preflighted_metainfo() {
    let (address, server) = spawn_server(vec![ResponseSpec::immediate(response(
        "200 OK",
        &[],
        VALID_METAINFO,
    ))])
    .await;
    let url = format!("http://alice:sec%72et@{address}/download/private?passkey=source-token");

    let bytes = fetch_torrent_source(&url, short_limits(), false)
        .await
        .expect("authenticated source fetch");
    assert_eq!(bytes, VALID_METAINFO);

    let requests = server.await.expect("server task");
    assert_eq!(requests.len(), 1);
    assert!(request_has_header(&requests[0], BASIC_ALICE_SECRET));
}

#[tokio::test]
async fn errors_expose_only_the_source_origin() {
    let (address, server) = spawn_server(vec![ResponseSpec::immediate(response(
        "500 Internal Server Error",
        &[],
        b"upstream included source-token",
    ))])
    .await;
    let url = format!("http://alice:secret@{address}/secret/path?passkey=source-token");

    let error = fetch_torrent_source(&url, short_limits(), false)
        .await
        .expect_err("status must fail");
    let shown = error.to_string();
    assert_eq!(
        shown,
        format!("torrent source returned HTTP 500 from http://{address}")
    );
    for secret in ["alice", "secret", "path", "passkey", "source-token"] {
        assert!(!shown.contains(secret), "leaked {secret:?}: {shown}");
    }
    server.await.expect("server task");
}

#[tokio::test]
async fn authentication_survives_only_same_origin_redirects() {
    let (same_address, same_server) = spawn_server_with(|address| {
        let target = format!("http://mallory:evil@{address}/final");
        vec![
            ResponseSpec::immediate(response("302 Found", &[("Location", &target)], b"")),
            ResponseSpec::immediate(response("200 OK", &[], VALID_METAINFO)),
        ]
    })
    .await;
    let same_url = format!("http://alice:secret@{same_address}/start");
    fetch_torrent_source(&same_url, short_limits(), false)
        .await
        .expect("same-origin redirect");
    let same_requests = same_server.await.expect("same-origin server task");
    assert_eq!(same_requests.len(), 2);
    assert!(same_requests
        .iter()
        .all(|request| request_has_header(request, BASIC_ALICE_SECRET)));
    assert!(same_requests
        .iter()
        .all(|request| !request.contains("bWFsbG9yeTpldmls")));

    let (target_address, target_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "200 OK",
        &[],
        VALID_METAINFO,
    ))])
    .await;
    let target = source_url(target_address, "/final");
    let (source_address, source_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "302 Found",
        &[("Location", &target)],
        b"",
    ))])
    .await;
    let cross_url = format!("http://alice:secret@{source_address}/start");
    fetch_torrent_source(&cross_url, short_limits(), false)
        .await
        .expect("cross-origin redirect");

    let source_requests = source_server.await.expect("source server task");
    let target_requests = target_server.await.expect("target server task");
    assert!(request_has_header(&source_requests[0], BASIC_ALICE_SECRET));
    assert!(!target_requests[0]
        .to_ascii_lowercase()
        .contains("authorization:"));
}

#[tokio::test]
async fn redirects_revalidate_scheme_and_enforce_the_exact_ceiling() {
    let (invalid_address, invalid_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "302 Found",
        &[("Location", "file:///tmp/hidden.torrent")],
        b"",
    ))])
    .await;
    let error = fetch_torrent_source(
        &source_url(invalid_address, "/start"),
        short_limits(),
        false,
    )
    .await
    .expect_err("non-HTTP redirect must fail");
    assert!(matches!(
        error,
        TorrentError::InvalidTorrentSourceRedirect { .. }
    ));
    invalid_server.await.expect("invalid redirect server task");

    let mut accepted_responses = Vec::new();
    for hop in 1..=5 {
        accepted_responses.push(ResponseSpec::immediate(response(
            "302 Found",
            &[("Location", &format!("/hop-{hop}"))],
            b"",
        )));
    }
    accepted_responses.push(ResponseSpec::immediate(response(
        "200 OK",
        &[],
        VALID_METAINFO,
    )));
    let (accepted_address, accepted_server) = spawn_server(accepted_responses).await;
    fetch_torrent_source(
        &source_url(accepted_address, "/start"),
        short_limits(),
        false,
    )
    .await
    .expect("exactly five redirects are accepted");
    assert_eq!(
        accepted_server.await.expect("accepted server task").len(),
        6
    );

    let rejected_responses = (1..=6)
        .map(|hop| {
            ResponseSpec::immediate(response(
                "302 Found",
                &[("Location", &format!("/hop-{hop}"))],
                b"",
            ))
        })
        .collect();
    let (rejected_address, rejected_server) = spawn_server(rejected_responses).await;
    let error = fetch_torrent_source(
        &source_url(rejected_address, "/start"),
        short_limits(),
        false,
    )
    .await
    .expect_err("sixth redirect must fail");
    assert!(matches!(
        error,
        TorrentError::TooManyTorrentSourceRedirects { limit: 5 }
    ));
    assert_eq!(
        rejected_server.await.expect("rejected server task").len(),
        6
    );
}

#[tokio::test]
async fn response_cookies_are_not_replayed_across_redirects() {
    let (address, server) = spawn_server(vec![
        ResponseSpec::immediate(response(
            "302 Found",
            &[
                ("Location", "/final"),
                ("Set-Cookie", "session=source-secret; HttpOnly"),
            ],
            b"",
        )),
        ResponseSpec::immediate(response("200 OK", &[], VALID_METAINFO)),
    ])
    .await;

    fetch_torrent_source(&source_url(address, "/start"), short_limits(), false)
        .await
        .expect("cookie response redirect");
    let requests = server.await.expect("cookie server task");
    assert_eq!(requests.len(), 2);
    assert!(!requests[1].to_ascii_lowercase().contains("cookie:"));
}

#[tokio::test]
async fn content_length_and_streaming_enforce_the_first_excess_byte() {
    let limits = TorrentSourceFetchLimits {
        max_metainfo_bytes: MIN_CONFIGURED_MAX_METAINFO_BYTES,
        ..short_limits()
    };
    let excessive = MIN_CONFIGURED_MAX_METAINFO_BYTES + 1;
    let header = excessive.to_string();
    let (header_address, header_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "200 OK",
        &[("Content-Length", &header)],
        b"",
    ))])
    .await;
    let error = fetch_torrent_source(&source_url(header_address, "/large"), limits, false)
        .await
        .expect_err("oversized Content-Length must fail");
    assert!(matches!(
        error,
        TorrentError::MetainfoTooLarge { size, limit }
            if size == excessive && limit == MIN_CONFIGURED_MAX_METAINFO_BYTES
    ));
    header_server.await.expect("header server task");

    // Keep this body materially larger than the limit so the assertion below
    // distinguishes streaming enforcement from the post-buffering preflight.
    // A mutant that removes the streaming guard reports the full 4 MiB body.
    let streamed_total = MIN_CONFIGURED_MAX_METAINFO_BYTES * 4;
    let body = vec![b'x'; streamed_total];
    let (stream_address, stream_server) =
        spawn_server(vec![ResponseSpec::immediate(chunked_response(&body))]).await;
    let error = fetch_torrent_source(&source_url(stream_address, "/stream"), limits, false)
        .await
        .expect_err("oversized stream must fail");
    assert!(matches!(
        error,
        TorrentError::MetainfoTooLarge { size, limit }
            if size > MIN_CONFIGURED_MAX_METAINFO_BYTES
                && size <= MIN_CONFIGURED_MAX_METAINFO_BYTES + 64 * 1024
                && limit == MIN_CONFIGURED_MAX_METAINFO_BYTES
    ));
    stream_server.await.expect("stream server task");
}

#[tokio::test]
async fn total_timeout_covers_response_wait_and_redacts_the_url() {
    let (address, server) = spawn_server(vec![ResponseSpec::delayed(
        response("200 OK", &[], VALID_METAINFO),
        Duration::from_millis(150),
    )])
    .await;
    let limits = TorrentSourceFetchLimits {
        connect_timeout: Duration::from_millis(20),
        total_timeout: Duration::from_millis(50),
        ..TorrentSourceFetchLimits::default()
    };
    let url = format!("http://alice:secret@{address}/slow?passkey=source-token");

    let error = fetch_torrent_source(&url, limits, false)
        .await
        .expect_err("total timeout must fail");
    assert!(matches!(&error, TorrentError::TorrentSourceTimeout { .. }));
    let shown = error.to_string();
    assert_eq!(
        shown,
        format!("torrent source request timed out for http://{address}")
    );
    server.await.expect("timeout server task");
}

#[tokio::test]
async fn fetched_bytes_still_obey_parser_and_proxy_tracker_policy() {
    let (invalid_address, invalid_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "200 OK",
        &[],
        b"not-bencoded-metainfo",
    ))])
    .await;
    let error = fetch_torrent_source(
        &source_url(invalid_address, "/invalid"),
        short_limits(),
        false,
    )
    .await
    .expect_err("invalid metainfo must fail preflight");
    assert!(matches!(error, TorrentError::Engine(_)));
    invalid_server.await.expect("invalid body server task");

    let tracker = "udp://127.0.0.1:80";
    let proxy_unsafe = format!(
        "d8:announce{}:{}4:infod6:lengthi1e4:name1:a12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        tracker.len(),
        tracker
    );
    let (proxy_address, proxy_server) = spawn_server(vec![ResponseSpec::immediate(response(
        "200 OK",
        &[],
        proxy_unsafe.as_bytes(),
    ))])
    .await;
    let error = fetch_torrent_source(
        &source_url(proxy_address, "/proxy-unsafe"),
        short_limits(),
        true,
    )
    .await
    .expect_err("UDP tracker must fail when engine proxy is enabled");
    assert!(matches!(error, TorrentError::ProxyWithUdpTracker));
    proxy_server.await.expect("proxy server task");
}

#[tokio::test]
async fn unsupported_schemes_and_out_of_range_limits_fail_before_contact() {
    let error = fetch_torrent_source("file:///tmp/a.torrent", short_limits(), false)
        .await
        .expect_err("file sources must fail");
    assert!(matches!(error, TorrentError::InvalidTorrentSource(_)));

    let limits = TorrentSourceFetchLimits {
        max_metainfo_bytes: MIN_CONFIGURED_MAX_METAINFO_BYTES - 1,
        ..short_limits()
    };
    let error = fetch_torrent_source("http://127.0.0.1/", limits, false)
        .await
        .expect_err("undersized configured limit must fail");
    assert!(matches!(error, TorrentError::InvalidTorrentSourceLimits(_)));
}

#[tokio::test]
async fn malformed_sources_and_credentials_fail_before_any_socket() {
    // Every one of these targets a host that must never be contacted, so a
    // hang or a DNS error here means the check moved after the request.
    for (source, reason) in [
        ("not-a-url", "URL syntax is not valid"),
        (
            "http://%FF@source.invalid/a.torrent",
            "username must contain valid UTF-8",
        ),
        (
            "http://alice:%FF@source.invalid/a.torrent",
            "password must contain valid UTF-8",
        ),
    ] {
        let error = fetch_torrent_source(source, short_limits(), false)
            .await
            .expect_err("malformed source must fail");
        assert!(
            matches!(&error, TorrentError::InvalidTorrentSource(named) if *named == reason),
            "{source} was not rejected as {reason:?}: {error}"
        );
    }
}

#[tokio::test]
async fn an_unreachable_origin_reports_a_redacted_request_failure() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind loopback server");
    let address = listener.local_addr().expect("loopback address");
    drop(listener);

    let url = format!("http://alice:secret@{address}/secret/path?passkey=source-token");
    let error = fetch_torrent_source(&url, short_limits(), false)
        .await
        .expect_err("closed port must fail");
    assert!(matches!(
        &error,
        TorrentError::TorrentSourceRequestFailed { .. }
    ));
    let shown = error.to_string();
    assert_eq!(
        shown,
        format!("torrent source request failed for http://{address}")
    );
    for secret in ["alice", "secret", "path", "passkey", "source-token"] {
        assert!(!shown.contains(secret), "leaked {secret:?}: {shown}");
    }
}

#[tokio::test]
async fn redirects_require_a_location_header_that_resolves_to_a_url() {
    // A redirect status with no usable target must fail instead of falling
    // through to the redirect body as if it were metainfo.
    let missing = response("302 Found", &[], b"");
    let unreadable = b"HTTP/1.1 302 Found\r\nLocation: \xff\xfe\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
    let unjoinable = response("302 Found", &[("Location", "http://[not-an-address")], b"");

    for (raw, reason) in [
        (missing, "Location header is missing"),
        (unreadable, "Location header is not valid text"),
        (unjoinable, "target URL syntax is not valid"),
    ] {
        let (address, server) = spawn_server(vec![ResponseSpec::immediate(raw)]).await;
        let error = fetch_torrent_source(&source_url(address, "/start"), short_limits(), false)
            .await
            .expect_err("unusable redirect must fail");
        assert!(
            matches!(
                &error,
                TorrentError::InvalidTorrentSourceRedirect { origin, reason: named }
                    if *named == reason && *origin == format!("http://{address}")
            ),
            "redirect was not rejected as {reason:?}: {error}"
        );
        assert_eq!(server.await.expect("redirect server task").len(), 1);
    }
}

#[tokio::test]
async fn a_truncated_response_body_fails_instead_of_admitting_partial_metainfo() {
    // The server announces a chunk and then closes, so the body ends without
    // its terminating chunk. Partial bytes must never reach the preflight.
    let mut truncated =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    truncated.extend_from_slice(format!("{:x}\r\n", VALID_METAINFO.len()).as_bytes());
    truncated.extend_from_slice(&VALID_METAINFO[..VALID_METAINFO.len() / 2]);

    let (address, server) = spawn_server(vec![ResponseSpec::immediate(truncated)]).await;
    let url = format!("http://alice:secret@{address}/download?passkey=source-token");
    let error = fetch_torrent_source(&url, short_limits(), false)
        .await
        .expect_err("truncated body must fail");
    assert!(matches!(
        &error,
        TorrentError::TorrentSourceRequestFailed { .. }
    ));
    let shown = error.to_string();
    for secret in ["alice", "secret", "passkey", "source-token"] {
        assert!(!shown.contains(secret), "leaked {secret:?}: {shown}");
    }
    server.await.expect("truncated server task");
}
