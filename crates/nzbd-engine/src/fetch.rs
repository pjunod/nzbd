//! Minimal HTTPS/HTTP fetcher for URL jobs (`AddUrl`): hyper HTTP/1.1 over
//! the same rustls stack the NNTP transport uses. Follows up to 5
//! redirects, caps bodies at 64 MiB (an NZB, not a payload), 60 s timeout
//! per hop.

use http_body_util::BodyExt;
use hyper::Request;
use hyper_util::rt::TokioIo;
use nzbd_types::CertLevel;
use std::time::Duration;
use tokio::net::TcpStream;

const MAX_REDIRECTS: usize = 5;
const MAX_BODY: usize = 64 * 1024 * 1024;
const HOP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("bad url: {0}")]
    BadUrl(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(String),
    #[error("redirect loop / too many redirects")]
    TooManyRedirects,
    #[error("timed out")]
    Timeout,
}

struct Url {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<Url, FetchError> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(FetchError::BadUrl(url.into()));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(FetchError::BadUrl(url.into()));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !h.is_empty() => (
            h.to_string(),
            p.parse().map_err(|_| FetchError::BadUrl(url.into()))?,
        ),
        _ => (authority.to_string(), if https { 443 } else { 80 }),
    };
    Ok(Url {
        https,
        host,
        port,
        path: path.to_string(),
    })
}

/// GET a URL and return the body bytes.
pub async fn http_get(url: &str) -> Result<Vec<u8>, FetchError> {
    let mut current = url.to_string();
    for _ in 0..=MAX_REDIRECTS {
        match tokio::time::timeout(HOP_TIMEOUT, get_once(&current)).await {
            Err(_) => return Err(FetchError::Timeout),
            Ok(Ok(Hop::Body(bytes))) => return Ok(bytes),
            Ok(Ok(Hop::Redirect(next))) => {
                current = if next.starts_with("http://") || next.starts_with("https://") {
                    next
                } else {
                    // Relative redirect: resolve against the current origin.
                    let u = parse_url(&current)?;
                    let scheme = if u.https { "https" } else { "http" };
                    if next.starts_with('/') {
                        format!("{scheme}://{}:{}{next}", u.host, u.port)
                    } else {
                        format!("{scheme}://{}:{}/{next}", u.host, u.port)
                    }
                };
            }
            Ok(Err(e)) => return Err(e),
        }
    }
    Err(FetchError::TooManyRedirects)
}

enum Hop {
    Body(Vec<u8>),
    Redirect(String),
}

async fn get_once(url: &str) -> Result<Hop, FetchError> {
    let u = parse_url(url)?;
    let tcp = TcpStream::connect((u.host.as_str(), u.port)).await?;
    if u.https {
        // Same rustls stack (and platform verifier) as the NNTP transport.
        let config = nzbd_nntp::transport::tls_client_config(CertLevel::Strict)
            .map_err(|e| FetchError::Http(e.to_string()))?;
        let connector = tokio_rustls::TlsConnector::from(config);
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(u.host.clone())
            .map_err(|_| FetchError::BadUrl(url.into()))?;
        let tls = connector.connect(name, tcp).await?;
        request(TokioIo::new(tls), &u).await
    } else {
        request(TokioIo::new(tcp), &u).await
    }
}

async fn request<S>(io: S, u: &Url) -> Result<Hop, FetchError>
where
    S: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::get(&u.path)
        .header(hyper::header::HOST, u.host.as_str())
        .header(hyper::header::USER_AGENT, "nzbd")
        .header(hyper::header::ACCEPT, "*/*")
        .body(String::new())
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let status = resp.status();
    if status.is_redirection() {
        let loc = resp
            .headers()
            .get(hyper::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| FetchError::Http("redirect without Location".into()))?;
        return Ok(Hop::Redirect(loc.to_string()));
    }
    if !status.is_success() {
        return Err(FetchError::Http(format!("status {status}")));
    }
    let mut body = Vec::new();
    let mut incoming = resp.into_body();
    while let Some(frame) = incoming.frame().await {
        let frame = frame.map_err(|e| FetchError::Http(e.to_string()))?;
        if let Some(chunk) = frame.data_ref() {
            if body.len() + chunk.len() > MAX_BODY {
                return Err(FetchError::Http("body too large for an NZB".into()));
            }
            body.extend_from_slice(chunk);
        }
    }
    Ok(Hop::Body(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};

    #[test]
    fn url_parsing() {
        let u = parse_url("https://indexer.example/api?t=get&id=1").unwrap();
        assert!(u.https);
        assert_eq!(u.host, "indexer.example");
        assert_eq!(u.port, 443);
        assert_eq!(u.path, "/api?t=get&id=1");

        let u = parse_url("http://10.0.0.5:8080/x.nzb").unwrap();
        assert!(!u.https);
        assert_eq!(u.port, 8080);

        assert!(parse_url("ftp://nope").is_err());
        assert!(parse_url("https://").is_err());
    }

    /// Plain-HTTP round trip against an in-process listener, including a
    /// redirect hop and a chunked response body.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_get_with_redirect_and_chunked() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            // Hop 1: redirect.
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{port}/real\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
            drop(s);
            // Hop 2: chunked body.
            let (mut s, _) = listener.accept().unwrap();
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                  5\r\n<nzb \r\n2\r\n/>\r\n0\r\n\r\n",
            );
        });
        let body = http_get(&format!("http://127.0.0.1:{port}/start"))
            .await
            .unwrap();
        assert_eq!(body, b"<nzb />");
    }
}
