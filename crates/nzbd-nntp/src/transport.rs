//! Async NNTP transport: TCP/TLS connect, greeting, AUTHINFO, response
//! reading and body streaming (phase 1; ARCHITECTURE.md §8.3).
//!
//! One [`NntpConnection`] is owned by exactly one connection task. Commands
//! may be pipelined (send several, then read the responses in order); body
//! bytes are handed to the caller in `fill_buf`-style chunks so the yEnc
//! decoder consumes them in place and reports exactly how many bytes belong
//! to the current article ([`nzbd-yenc`]'s terminator-aware `push`).
//!
//! TLS is rustls; certificate checking implements NZBGet's three
//! `CertVerification` levels: `Strict` (platform verifier), `Minimal`
//! (chain validity but no hostname check — some providers need this) and
//! `None` (unverified; loudly discouraged).

use crate::{codes, Command, NntpError, Response};
use nzbd_types::{CertLevel, ServerDef, TlsMode};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf,
};
use tokio::net::TcpStream;

pub type TlsClientConfig = Arc<ClientConfig>;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("protocol: {0}")]
    Protocol(#[from] NntpError),
    #[error("timed out during {0}")]
    Timeout(&'static str),
    #[error("unexpected response: {0} {1}")]
    Unexpected(u16, String),
    #[error("authentication rejected: {0} {1}")]
    AuthRejected(u16, String),
    #[error("connection closed by peer")]
    Closed,
}

impl TransportError {
    /// Connection-level failure (walks the ladder as `ConnectionFailed`,
    /// blocking the server briefly) vs article/protocol-level failure.
    pub fn is_connection_level(&self) -> bool {
        matches!(
            self,
            TransportError::Io(_)
                | TransportError::Tls(_)
                | TransportError::Timeout(_)
                | TransportError::Closed
        )
    }
}

// ---------------------------------------------------------------------------
// Plain / TLS stream unification
// ---------------------------------------------------------------------------

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Read buffer size. Article lines are ≤ ~1 KB but bodies stream through
/// this buffer, so make it big enough to keep syscall counts low.
const BUF_SIZE: usize = 64 * 1024;

pub struct NntpConnection {
    io: BufReader<Stream>,
    read_timeout: Duration,
    line: String,
}

impl NntpConnection {
    /// TCP (+ optional TLS) connect and greeting. Does not authenticate.
    pub async fn connect(
        server: &ServerDef,
        tls: Option<TlsClientConfig>,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<(Self, Response), TransportError> {
        let addr = (server.host.as_str(), server.port);
        let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::Timeout("connect"))??;
        tcp.set_nodelay(true).ok();

        let stream = match server.tls {
            TlsMode::None => Stream::Plain(tcp),
            TlsMode::Tls => {
                let config = tls.ok_or_else(|| {
                    TransportError::Tls("TLS requested but no client config supplied".into())
                })?;
                let name = ServerName::try_from(server.host.clone())
                    .map_err(|e| TransportError::Tls(format!("invalid server name: {e}")))?;
                let connector = tokio_rustls::TlsConnector::from(config);
                let tls_stream = tokio::time::timeout(connect_timeout, connector.connect(name, tcp))
                    .await
                    .map_err(|_| TransportError::Timeout("tls handshake"))?
                    .map_err(|e| TransportError::Tls(e.to_string()))?;
                Stream::Tls(Box::new(tls_stream))
            }
        };

        let mut conn = NntpConnection {
            io: BufReader::with_capacity(BUF_SIZE, stream),
            read_timeout,
            line: String::with_capacity(256),
        };

        let greeting = conn.read_response().await?;
        if !matches!(
            greeting.code,
            codes::GREETING_POSTING_OK | codes::GREETING_NO_POSTING
        ) {
            return Err(TransportError::Unexpected(greeting.code, greeting.text));
        }
        Ok((conn, greeting))
    }

    /// AUTHINFO USER/PASS. Handles servers that accept USER alone (281).
    pub async fn authenticate(&mut self, user: &str, pass: &str) -> Result<(), TransportError> {
        self.send(&Command::AuthInfoUser(user)).await?;
        let r = self.read_response().await?;
        match r.code {
            codes::AUTH_ACCEPTED => return Ok(()),
            codes::PASSWORD_REQUIRED => {}
            _ => return Err(TransportError::AuthRejected(r.code, r.text)),
        }
        self.send(&Command::AuthInfoPass(pass)).await?;
        let r = self.read_response().await?;
        if r.code == codes::AUTH_ACCEPTED {
            Ok(())
        } else {
            Err(TransportError::AuthRejected(r.code, r.text))
        }
    }

    /// Serialize and send one command (flushes).
    pub async fn send(&mut self, cmd: &Command<'_>) -> Result<(), TransportError> {
        let wire = cmd.encode()?;
        self.io.get_mut().write_all(wire.as_bytes()).await?;
        self.io.get_mut().flush().await?;
        Ok(())
    }

    /// Send several commands in one write (pipelining).
    pub async fn send_pipelined(&mut self, cmds: &[Command<'_>]) -> Result<(), TransportError> {
        let mut wire = String::new();
        for c in cmds {
            wire.push_str(&c.encode()?);
        }
        self.io.get_mut().write_all(wire.as_bytes()).await?;
        self.io.get_mut().flush().await?;
        Ok(())
    }

    /// Read a single response line.
    pub async fn read_response(&mut self) -> Result<Response, TransportError> {
        self.line.clear();
        let n = tokio::time::timeout(self.read_timeout, self.io.read_line(&mut self.line))
            .await
            .map_err(|_| TransportError::Timeout("response"))??;
        if n == 0 {
            return Err(TransportError::Closed);
        }
        Ok(Response::parse(self.line.as_bytes())?)
    }

    /// Body streaming: expose the next buffered chunk (`fill_buf`). Returns
    /// an empty slice on EOF. Follow with [`NntpConnection::consume`] for
    /// exactly the bytes the decoder used.
    pub async fn body_chunk(&mut self) -> Result<&[u8], TransportError> {
        let chunk = tokio::time::timeout(self.read_timeout, self.io.fill_buf())
            .await
            .map_err(|_| TransportError::Timeout("body"))??;
        Ok(chunk)
    }

    pub fn consume(&mut self, n: usize) {
        self.io.consume(n);
    }

    /// Swallow the remainder of a multiline data block up to and including
    /// the `CRLF.CRLF` terminator — used to stay in protocol sync after a
    /// decode error mid-body.
    pub async fn drain_body(&mut self) -> Result<u64, TransportError> {
        let mut scanner = TerminatorScanner::new();
        let mut drained = 0u64;
        loop {
            let chunk = self.body_chunk().await?;
            if chunk.is_empty() {
                return Err(TransportError::Closed);
            }
            let (consumed, done) = scanner.feed(chunk);
            self.consume(consumed);
            drained += consumed as u64;
            if done {
                return Ok(drained);
            }
        }
    }

    /// Best-effort QUIT before closing (does not wait for the goodbye).
    pub async fn quit(mut self) {
        let _ = self.send(&Command::Quit).await;
        let _ = self.io.get_mut().shutdown().await;
    }
}

/// Incremental scanner for the multiline terminator (line-start `.` CRLF).
/// Tolerates bare-LF line endings like the rest of the codebase.
#[derive(Debug)]
pub struct TerminatorScanner {
    at_line_start: bool,
    dot: u8, // 0 none, 1 line-start '.', 2 ".\r"
}

impl Default for TerminatorScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminatorScanner {
    pub fn new() -> Self {
        TerminatorScanner {
            at_line_start: true,
            dot: 0,
        }
    }

    /// Returns `(consumed, done)`. Stops consuming right after the
    /// terminator's final LF.
    pub fn feed(&mut self, buf: &[u8]) -> (usize, bool) {
        let mut i = 0;
        while i < buf.len() {
            let b = buf[i];
            i += 1;
            match self.dot {
                1 => match b {
                    b'\r' => self.dot = 2,
                    b'\n' => {
                        self.dot = 0;
                        return (i, true); // lenient ".\n"
                    }
                    _ => {
                        self.dot = 0;
                        self.at_line_start = b == b'\n';
                    }
                },
                2 => {
                    self.dot = 0;
                    if b == b'\n' {
                        return (i, true);
                    }
                    self.at_line_start = false;
                }
                _ => {
                    if self.at_line_start && b == b'.' {
                        self.dot = 1;
                        self.at_line_start = false;
                    } else {
                        self.at_line_start = b == b'\n';
                    }
                }
            }
        }
        (i, false)
    }
}

// ---------------------------------------------------------------------------
// TLS client configs for the three CertVerification levels
// ---------------------------------------------------------------------------

/// Build the rustls client config for a cert-verification level. Build once
/// per server at startup and share via `Arc`.
pub fn tls_client_config(level: CertLevel) -> Result<TlsClientConfig, TransportError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

    let config = match level {
        CertLevel::Strict => {
            let verifier = platform_verifier(provider.clone())?;
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| TransportError::Tls(e.to_string()))?
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth()
        }
        CertLevel::Minimal => {
            let inner = platform_verifier(provider.clone())?;
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| TransportError::Tls(e.to_string()))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(MinimalVerifier { inner }))
                .with_no_client_auth()
        }
        CertLevel::None => ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier { provider }))
            .with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

fn platform_verifier(
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, TransportError> {
    let v = rustls_platform_verifier::Verifier::new(provider)
        .map_err(|e| TransportError::Tls(format!("platform verifier: {e}")))?;
    Ok(Arc::new(v))
}

/// NZBGet's `CertVerification=minimal`: certificate chain must validate, but
/// the hostname is not checked (needed for some providers' shared certs).
#[derive(Debug)]
struct MinimalVerifier {
    inner: Arc<dyn ServerCertVerifier>,
}

impl ServerCertVerifier for MinimalVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use rustls::CertificateError::*;
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            Err(rustls::Error::InvalidCertificate(NotValidForNameContext { .. })) => {
                Ok(ServerCertVerified::assertion())
            }
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// `CertVerification=none`: accept anything. Kept for parity; warned about
/// at config load.
#[derive(Debug)]
struct NoVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::ServerId;
    use tokio::io::AsyncReadExt;

    fn test_server(port: u16) -> ServerDef {
        ServerDef {
            id: ServerId(1),
            name: "test".into(),
            host: "127.0.0.1".into(),
            port,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 1,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }
    }

    #[tokio::test]
    async fn connect_greet_auth_and_read() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_task = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(b"200 test server ready\r\n").await.unwrap();
            let mut buf = vec![0u8; 1024];
            // AUTHINFO USER
            let n = sock.read(&mut buf).await.unwrap();
            assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("AUTHINFO USER alice"));
            sock.write_all(b"381 password required\r\n").await.unwrap();
            // AUTHINFO PASS
            let n = sock.read(&mut buf).await.unwrap();
            assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("AUTHINFO PASS s3cret"));
            sock.write_all(b"281 welcome\r\n").await.unwrap();
            // BODY -> 430
            let n = sock.read(&mut buf).await.unwrap();
            assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("BODY <x@y>"));
            sock.write_all(b"430 no such article\r\n").await.unwrap();
        });

        let (mut conn, greeting) = NntpConnection::connect(
            &test_server(port),
            None,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(greeting.code, 200);
        conn.authenticate("alice", "s3cret").await.unwrap();
        conn.send(&Command::Body("x@y")).await.unwrap();
        let r = conn.read_response().await.unwrap();
        assert!(r.is_article_missing());
        server_task.await.unwrap();
    }

    #[test]
    fn terminator_scanner_all_split_points() {
        let wire = b"some body bytes\r\n..stuffed\r\nlast\r\n.\r\nNEXT";
        for split in 1..wire.len() - 4 {
            let mut s = TerminatorScanner::new();
            let (c1, d1) = s.feed(&wire[..split]);
            assert_eq!(c1, split.min(wire.len() - 4));
            let mut total = c1;
            let mut done = d1;
            if !done {
                let (c2, d2) = s.feed(&wire[c1..]);
                total += c2;
                done = d2;
            }
            assert!(done, "split at {split}");
            assert_eq!(total, wire.len() - 4, "split at {split}");
        }
    }

    #[test]
    fn terminator_scanner_ignores_stuffed_dots() {
        // A dot-stuffed line ("..x") must not terminate.
        let mut s = TerminatorScanner::new();
        let (c, done) = s.feed(b"..x\r\n.\r\n");
        assert!(done);
        assert_eq!(c, 8);
    }

    #[test]
    fn tls_config_builds_for_all_levels() {
        for level in [CertLevel::Strict, CertLevel::Minimal, CertLevel::None] {
            tls_client_config(level).unwrap();
        }
    }
}
