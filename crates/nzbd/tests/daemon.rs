//! Whole-daemon smoke test: spawns the real `nzbd` binary against an
//! in-process mock NNTP server, drives it with the real `nzbd add` /
//! `nzbd status` CLI, checks the compat shim answers, and verifies a
//! graceful SIGINT shutdown clears the unclean marker.

#![cfg(unix)]

use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    try_http(addr, method, path, body, None)
        .unwrap_or_else(|| panic!("http {method} {path} against {addr} failed"))
}

/// A daemon mid-restart legitimately RESETs in-flight connections while
/// its listener bounces — this poller must treat any socket error as "not
/// yet", not panic (it flaked exactly that way: connect landed on the
/// dying listener, then the read hit ECONNRESET).
fn wait_healthy(addr: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > deadline {
            panic!("daemon did not become healthy at {addr}");
        }
        if let Some((200, body)) = try_http(addr, "GET", "/healthz", b"", None) {
            if body == "ok" {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `http`, but for the window right after a restart. `wait_healthy` can be
/// answered by the OUTGOING listener a moment before it closes, so the very
/// next request lands on a socket that RESETs — a real, if narrow, race, and
/// one a test must not turn into a red build. Retries any transport error
/// and any non-2xx until the deadline.
fn http_settled(addr: &str, method: &str, path: &str, deadline: Duration) -> (u16, String) {
    let start = Instant::now();
    loop {
        let last = try_http(addr, method, path, b"", None);
        if let Some((code, body)) = last.clone() {
            if (200..300).contains(&code) {
                return (code, body);
            }
        }
        if start.elapsed() > deadline {
            panic!("{method} {path} never settled at {addr} (last: {last:?})");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Terminate the daemon gracefully (SIGTERM) so it flushes journals —
/// and, under instrumented builds, its coverage profile. Falls back to
/// SIGKILL if it doesn't exit within a few seconds.
struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status();
            for _ in 0..50 {
                if matches!(self.0.try_wait(), Ok(Some(_))) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Like `http`, but tolerates a dead/bouncing listener (returns None) and
/// can send Basic auth. Used around the setup-reload window.
fn try_http(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    basic: Option<&str>,
) -> Option<(u16, String)> {
    let mut sock = TcpStream::connect(addr).ok()?;
    sock.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    let auth = basic
        .map(|cred| {
            use base64::Engine as _;
            format!(
                "Authorization: Basic {}\r\n",
                base64::engine::general_purpose::STANDARD.encode(cred)
            )
        })
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n{auth}Connection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).ok()?;
    sock.write_all(body).ok()?;
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).ok()?;
    let text = String::from_utf8_lossy(&resp).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.trim().to_string())
        .unwrap_or_default();
    Some((status, payload))
}

/// `[api] tls = true` with no cert configured: the daemon self-generates a
/// persistent certificate and serves HTTPS, including the PWA assets.
#[test]
fn https_selfsigned_serves_healthz_and_pwa_assets() {
    use std::sync::Arc;

    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let api_addr = format!("127.0.0.1:{port}");
    let config = format!(
        "[paths]\nmain_dir = \"{main}\"\ndest_dir = \"{dest}\"\n\n[api]\nbind = \"{api_addr}\"\ntls = true\n",
        main = tmp.path().join("data").display(),
        dest = tmp.path().join("data/complete").display(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);

    // A TLS client that accepts any certificate (it's self-signed).
    #[derive(Debug)]
    struct NoVerify(Arc<rustls::crypto::CryptoProvider>);
    impl rustls::client::danger::ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _: &rustls::pki_types::CertificateDer<'_>,
            _: &[rustls::pki_types::CertificateDer<'_>],
            _: &rustls::pki_types::ServerName<'_>,
            _: &[u8],
            _: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &rustls::pki_types::CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &rustls::pki_types::CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let client_cfg = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let https_get = |path: &'static str| -> Option<(u16, Vec<u8>)> {
        rt.block_on(async {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let tcp = tokio::net::TcpStream::connect(&api_addr).await.ok()?;
            let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let mut tls = connector.connect(sni, tcp).await.ok()?;
            let req =
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            tls.write_all(req.as_bytes()).await.ok()?;
            let mut resp = Vec::new();
            let _ = tls.read_to_end(&mut resp).await;
            let head = String::from_utf8_lossy(&resp);
            let status: u16 = head.split_whitespace().nth(1)?.parse().ok()?;
            let body_at = resp.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
            Some((status, resp[body_at..].to_vec()))
        })
    };

    // Wait for the TLS listener (plain-HTTP probing can't work here).
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < deadline, "https did not come up");
        if let Some((200, body)) = https_get("/healthz") {
            assert!(body.ends_with(b"ok"), "healthz over https");
            break;
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    let (code, body) = https_get("/manifest.webmanifest").unwrap();
    assert_eq!(code, 200);
    let manifest: serde_json::Value =
        serde_json::from_slice(strip_chunking(&body).as_slice()).unwrap();
    assert_eq!(manifest["display"], "standalone");
    let (code, _) = https_get("/sw.js").unwrap();
    assert_eq!(code, 200);
    let (code, body) = https_get("/icons/icon-192.png").unwrap();
    assert_eq!(code, 200);
    assert!(
        strip_chunking(&body).starts_with(&[0x89, b'P', b'N', b'G']),
        "PNG magic"
    );

    // The generated cert persists under the state dir for reuse.
    assert!(tmp.path().join("data/queue/tls/cert.pem").exists());
    assert!(tmp.path().join("data/queue/tls/key.pem").exists());
}

/// HTTP/1.1 with Connection: close may still arrive chunked; strip the
/// framing when present so body assertions see the payload.
fn strip_chunking(body: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(body);
    let Some(first_line_end) = text.find("\r\n") else {
        return body.to_vec();
    };
    // Chunked iff the first line is a bare hex size.
    if !text[..first_line_end]
        .chars()
        .all(|c| c.is_ascii_hexdigit())
        || text[..first_line_end].is_empty()
    {
        return body.to_vec();
    }
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
        let size =
            usize::from_str_radix(String::from_utf8_lossy(&rest[..pos]).trim(), 16).unwrap_or(0);
        let start = pos + 2;
        if size == 0 || rest.len() < start + size {
            break;
        }
        out.extend_from_slice(&rest[start..start + size]);
        rest = &rest[(start + size + 2).min(rest.len())..];
    }
    out
}

/// Regression: an open SSE stream (`/api/v1/events`) must NOT block a
/// restart. The browser keeps that connection alive, so graceful
/// shutdown has to end it — otherwise the daemon hangs mid-restart and
/// never re-binds ("clicking restart does nothing"). The earlier restart
/// test used `Connection: close` and missed this entirely.
#[test]
fn restart_completes_with_an_open_sse_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let config = format!(
        "[paths]\nmain_dir = \"{main}\"\ndest_dir = \"{dest}\"\n\n[api]\nbind = \"{addr}\"\n",
        main = tmp.path().join("data").display(),
        dest = tmp.path().join("data/complete").display(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&addr, Duration::from_secs(15));

    // Open an SSE stream and HOLD it open (like a browser tab): read the
    // response head, then keep the socket alive across the restart.
    let mut sse = TcpStream::connect(&addr).unwrap();
    sse.write_all(
        format!("GET /api/v1/events HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n\r\n")
            .as_bytes(),
    )
    .unwrap();
    let mut head = [0u8; 64];
    sse.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let n = sse.read(&mut head).unwrap_or(0);
    assert!(
        String::from_utf8_lossy(&head[..n]).contains("200"),
        "SSE stream should open"
    );

    // Trigger the restart on a separate connection while the SSE is open.
    let (code, _) = http(&addr, "POST", "/api/v1/restart", b"");
    assert_eq!(code, 200);

    // The daemon must CLOSE our SSE stream so graceful shutdown can drain
    // and the process can re-serve. If the stream blocked shutdown (the
    // bug), the socket stays open — only keep-alive pings arrive, never
    // EOF — and this times out. `up_since_unix` can't be used here: the
    // restart is sub-second, so it lands in the same whole second.
    sse.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut buf = [0u8; 256];
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut closed = false;
    while Instant::now() < deadline {
        match sse.read(&mut buf) {
            Ok(0) => {
                closed = true; // server closed the stream — restart proceeded
                break;
            }
            Ok(_) => continue, // keep-alive ping; keep waiting for the close
            Err(_) => break,   // read timeout
        }
    }
    assert!(
        closed,
        "SSE stream was not closed on restart — it blocked graceful shutdown (the daemon hung)"
    );
    drop(sse);

    // And the daemon is serving again.
    wait_healthy(&addr, Duration::from_secs(10));
}

/// Regression (field report 2026-07-25, #2 of the day): a restart clicked
/// while post-processing was running never finished — teardown waited
/// unboundedly on the PP tracker while one job's work ran on (here: an
/// extension script; in the field report the same held for minutes), the
/// engine stayed alive behind it, and the listener never came back. PP
/// job tasks now abort on cancel (crash-safe: no `*PP:done` stamp was
/// written, so the next pass's rescan re-runs them) and the daemon bounds
/// the subsystem drain as a belt.
#[test]
fn restart_completes_while_post_processing_runs() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = prng_bytes(6, 60_000);
    let post = build_post("ppjob", &[("p.bin", data.clone())], 20_000);
    let ns = rt.block_on(async { NservBuilder::new().with_post(&post).start().await.unwrap() });

    let tmp = tempfile::tempdir().unwrap();
    let api_port = free_port();
    let api_addr = format!("127.0.0.1:{api_port}");

    // An extension script that sleeps far longer than the whole test: the
    // restart must NOT wait for it.
    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("slow.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nsleep 600\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let config = format!(
        r#"
[paths]
main_dir = "{main}"
dest_dir = "{dest}"

[[server]]
name = "mock"
host = "127.0.0.1"
port = {nntp_port}
tls = false
connections = 4
pipeline_depth = 2

[api]
bind = "{api_addr}"

[post]
scripts_dir = "{scripts}"
"#,
        main = tmp.path().join("main").display(),
        dest = tmp.path().join("dest").display(),
        nntp_port = ns.port(),
        scripts = scripts.display(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let daemon_log_path = tmp.path().join("daemon.log");
    let daemon_log = std::fs::File::create(&daemon_log_path).unwrap();
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(daemon_log.try_clone().unwrap())
        .stderr(daemon_log)
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    let (code, _) = http(
        &api_addr,
        "POST",
        "/api/v1/jobs?name=ppjob",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201);

    // Wait for the download to finish and PP to reach the sleeping script.
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "job never reached the script stage\n--- daemon log ---\n{}",
            std::fs::read_to_string(&daemon_log_path)
                .unwrap_or_else(|error| format!("could not read daemon log: {error}"))
        );
        let (code, body) = http(&api_addr, "GET", "/api/v1/jobs", b"");
        if code == 200 && body.contains("\"stage\":\"script\"") {
            break;
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    // Restart with the script mid-sleep. Before the fix this hung until
    // the script finished (10 minutes here); the page just never loaded.
    let (code, _) = http(&api_addr, "POST", "/api/v1/restart", b"");
    assert_eq!(code, 200);
    wait_healthy(&api_addr, Duration::from_secs(20));

    // Serving real state again, and the interrupted job survived into the
    // restarted queue (unstamped, so PP will pick it back up).
    let (code, body) = http_settled(&api_addr, "GET", "/api/v1/jobs", Duration::from_secs(20));
    assert_eq!(code, 200);
    assert!(body.contains("ppjob"), "queue survived the restart: {body}");
}

/// Settings round-trip with the new contract: a speed-limit change
/// applies LIVE (no restart); other sections mark restart-required;
/// POST /api/v1/restart bounces the daemon; secrets survive throughout.
#[test]
fn settings_live_apply_restart_flow_keeps_secrets() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let config = format!(
        concat!(
            "[paths]\nmain_dir = \"{main}\"\ndest_dir = \"{dest}\"\n\n",
            "[[server]]\nname = \"prime\"\nhost = \"news.example\"\n",
            "username = \"u\"\npassword = \"srv-secret\"\n\n",
            "[api]\nbind = \"{addr}\"\npassword = \"pw1\"\n"
        ),
        main = tmp.path().join("data").display(),
        dest = tmp.path().join("data/complete").display(),
        addr = addr,
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, &config).unwrap();

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&addr, Duration::from_secs(15));

    let auth = Some("nzbd:pw1");
    let json = |s: &str| -> serde_json::Value {
        let (a, b) = (s.find('{').unwrap(), s.rfind('}').unwrap());
        serde_json::from_str(&s[a..=b]).unwrap()
    };

    // Config requires auth; GET returns masked structured config.
    let (code, _) = try_http(&addr, "GET", "/api/v1/config", b"", None).unwrap();
    assert_eq!(code, 401);
    let (code, body) = try_http(&addr, "GET", "/api/v1/config", b"", auth).unwrap();
    assert_eq!(code, 200);
    let c = json(&body);
    assert_eq!(c["config"]["server"][0]["password"], "***unchanged***");
    assert!(!body.contains("srv-secret"));
    assert_eq!(c["pending_restart"].as_array().unwrap().len(), 0);
    let (code, body) = try_http(&addr, "GET", "/api/v1/status", b"", auth).unwrap();
    assert_eq!(code, 200);
    let up_before = json(&body)["up_since_unix"].as_i64().unwrap();

    // 1) Speed limit via JSON PUT: applied live, no restart required.
    let mut cfg_json = c["config"].clone();
    cfg_json["queue"]["speed_limit_kib"] = serde_json::json!(512);
    let (code, body) = try_http(
        &addr,
        "PUT",
        "/api/v1/config",
        cfg_json.to_string().as_bytes(),
        auth,
    )
    .unwrap();
    assert_eq!(code, 200, "{body}");
    let res = json(&body);
    assert_eq!(res["applied_live"][0], "speed limit");
    assert_eq!(res["restart_required"].as_array().unwrap().len(), 0);
    let (_, body) = try_http(&addr, "GET", "/api/v1/status", b"", auth).unwrap();
    let st = json(&body);
    assert_eq!(st["speed_limit_bps"].as_u64(), Some(512 * 1024), "live");
    assert_eq!(
        st["up_since_unix"].as_i64().unwrap(),
        up_before,
        "no bounce"
    );

    // 2) A post-processing change: saved, flagged restart-required.
    cfg_json["post"]["unpack"] = serde_json::json!(false);
    let (code, body) = try_http(
        &addr,
        "PUT",
        "/api/v1/config",
        cfg_json.to_string().as_bytes(),
        auth,
    )
    .unwrap();
    assert_eq!(code, 200, "{body}");
    let res = json(&body);
    assert!(res["restart_required"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "post-processing"));
    // Banner state survives a fresh GET.
    let (_, body) = try_http(&addr, "GET", "/api/v1/config", b"", auth).unwrap();
    assert!(json(&body)["pending_restart"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "post-processing"));

    // Garbage JSON is rejected.
    let (code, _) = try_http(&addr, "PUT", "/api/v1/config", b"{\"nope\": 1}", auth).unwrap();
    assert_eq!(code, 422);

    // 2b) A mask with nothing behind it must be REFUSED, not silently
    // turned into "no password". Field report 2026-07-26: "it imported the
    // config file but lost my password" — the merge wrote None when it
    // could not resolve the placeholder, and the save reported success.
    // Adding a second server whose password is still the mask is exactly
    // that case: no name match, no index match, nothing to restore from.
    let mut renamed = c["config"].clone();
    let mut extra = renamed["server"][0].clone();
    extra["name"] = serde_json::json!("brand-new");
    extra["host"] = serde_json::json!("news2.example.com");
    extra["password"] = serde_json::json!("***unchanged***");
    renamed["server"] = serde_json::json!([renamed["server"][0].clone(), extra]);
    let (code, body) = try_http(
        &addr,
        "PUT",
        "/api/v1/config",
        renamed.to_string().as_bytes(),
        auth,
    )
    .unwrap();
    assert_eq!(code, 422, "an unresolvable mask must not save: {body}");
    assert!(
        body.contains("brand-new"),
        "the error names the secret to retype: {body}"
    );
    // …and the config on disk still has the real password.
    let on_disk = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(
        on_disk.contains("srv-secret"),
        "the refused save left the real secret alone"
    );

    // 3) Restart button: daemon bounces, pending clears, auth persists.
    let (code, _) = try_http(&addr, "POST", "/api/v1/restart", b"", auth).unwrap();
    assert_eq!(code, 200);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "daemon did not restart");
        if let Some((200, body)) = try_http(&addr, "GET", "/api/v1/status", b"", auth) {
            let st = json(&body);
            if st["up_since_unix"].as_i64().unwrap() > up_before
                || st["speed_limit_bps"].as_u64() == Some(512 * 1024)
            {
                // restarted (or at least reloaded state); confirm pending cleared
                if let Some((200, cb)) = try_http(&addr, "GET", "/api/v1/config", b"", auth) {
                    if json(&cb)["pending_restart"].as_array().unwrap().is_empty() {
                        break;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    // Real secrets on disk, never the mask; the unpack edit persisted.
    let on_disk = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(on_disk.contains("srv-secret"));
    assert!(on_disk.contains("unpack = false"));
    assert!(on_disk.contains("speed_limit_kib = 512"));
    assert!(!on_disk.contains("***unchanged***"));
}

/// Container reality check: when the config location can't be written
/// (read-only mount, ConfigMap), setup still functions as a form —
/// GET reports `writable: false` up front, preview returns the rendered
/// TOML without writing, a failed save hands the TOML back copyable,
/// and the daemon stays up in setup mode throughout.
#[test]
fn setup_unwritable_config_offers_copyable_toml() {
    let tmp = tempfile::tempdir().unwrap();
    // A regular FILE where the config's parent dir should be:
    // create_dir_all fails for everyone (root included, ENOTDIR) — a
    // portable stand-in for a read-only mount.
    let blocker = tmp.path().join("blocked");
    std::fs::write(&blocker, b"i am a file").unwrap();
    let cfg_path = blocker.join("nzbd.toml");

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .args(["--bind", &addr])
        // First-run mode boots with Config::default() until setup writes the
        // real file. Keep that default state under this test's tempdir rather
        // than touching the invoking user's ~/downloads tree.
        .env("HOME", tmp.path())
        .env(
            nzbd_config::durable::MAIN_DIR_ENV,
            tmp.path().join("isolated-main"),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&addr, Duration::from_secs(15));

    let json = |s: &str| -> serde_json::Value {
        let (start, end) = (s.find('{').unwrap(), s.rfind('}').unwrap());
        serde_json::from_str(&s[start..=end]).unwrap()
    };

    // The boot-time probe already knows the location is unwritable.
    let (code, body) = http(&addr, "GET", "/api/v1/setup", b"");
    assert_eq!(code, 200);
    let s = json(&body);
    assert_eq!(s["setup_mode"], true);
    assert_eq!(s["writable"], false);

    let form = serde_json::json!({
        "main_dir": "/data", "dest_dir": "/data/complete",
        "server": { "host": "news.example.com" },
    });

    // Preview: full TOML, nothing written, setup not consumed.
    let mut preview = form.clone();
    preview["preview"] = serde_json::Value::Bool(true);
    let (code, body) = http(
        &addr,
        "POST",
        "/api/v1/setup",
        preview.to_string().as_bytes(),
    );
    assert_eq!(code, 200);
    let toml_text = json(&body)["toml"].as_str().unwrap().to_string();
    assert!(toml_text.contains("main_dir"));
    assert!(toml_text.contains("news.example.com"));
    nzbd_config::Config::from_toml(&toml_text).expect("preview TOML must parse strictly");

    // Real save: fails against the mount, but hands the TOML back.
    let (code, body) = http(&addr, "POST", "/api/v1/setup", form.to_string().as_bytes());
    assert_eq!(code, 500);
    let e = json(&body);
    assert!(e["error"].as_str().unwrap().contains("blocked"));
    assert_eq!(e["toml"].as_str().unwrap(), toml_text);
    assert!(e["hint"].as_str().unwrap().contains("copy"));

    // Daemon alive, still serving the form.
    let (code, body) = http(&addr, "GET", "/api/v1/setup", b"");
    assert_eq!(code, 200);
    assert_eq!(json(&body)["setup_mode"], true);
}

/// A missing config file is NOT automatically a first run.
///
/// Field report 2026-07-26: nzbd on a NAS kept coming back to the first-run
/// wizard. The config directory was writable but was not a mount, so every
/// config the wizard wrote lived in the container's own layer and died with
/// the next `docker compose up` — a configured, downloading install silently
/// reverted to unconfigured on each deploy, and the page blamed the config
/// path instead of the missing volume.
///
/// So boot now looks for the copy kept beside the *state* (which is on the
/// data volume, the mount that demonstrably survives) before it decides
/// anything. This proves it: no config file, a mirror on the data volume,
/// and the daemon must come up CONFIGURED — auth on, wizard gone — and put
/// the file back where it belongs.
#[test]
fn a_missing_config_is_recovered_from_the_data_volume_not_replaced_by_the_wizard() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let api_addr = format!("127.0.0.1:{port}");
    // The config path the daemon is pointed at — nothing there, exactly as
    // after a container recreate wiped an unmounted /etc/nzbd.
    let cfg_path = tmp.path().join("etc/nzbd.toml");
    let main_dir = tmp.path().join("data");

    // What the operator configured last time, still sitting on the data
    // volume because that mount is real.
    let saved = format!(
        "[paths]\nmain_dir = \"{}\"\ndest_dir = \"{}\"\n\n\
         [api]\nbind = \"{}\"\npassword = \"recovered-pw\"\n\n\
         [[server]]\nname = \"primary\"\nhost = \"127.0.0.1\"\nport = 1199\n\
         tls = false\nconnections = 2\n",
        main_dir.display(),
        main_dir.join("complete").display(),
        api_addr,
    );
    let state_dir = main_dir.join("queue");
    nzbd_config::durable::save_mirror(&state_dir, &saved).unwrap();
    assert!(!cfg_path.exists(), "the config file really is gone");

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .args(["--bind", &api_addr])
        // How recovery finds the data volume with no config to read it from
        // — set by the image, so a container needs no extra wiring.
        .env(nzbd_config::durable::MAIN_DIR_ENV, &main_dir)
        .env("HOME", tmp.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    // Auth is on, which can only come from the recovered config.
    let (code, _) = try_http(&api_addr, "GET", "/api/v1/status", b"", None).unwrap();
    assert_eq!(code, 401, "the recovered config's api password is in force");

    let (code, body) = try_http(
        &api_addr,
        "GET",
        "/api/v1/setup",
        b"",
        Some("nzbd:recovered-pw"),
    )
    .unwrap();
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["setup_mode"], false,
        "the wizard must NOT be serving: {body}"
    );
    assert_eq!(
        v["recovered_from"].as_str(),
        Some(
            nzbd_config::durable::mirror_path(&state_dir)
                .display()
                .to_string()
                .as_str()
        ),
        "the UI is told where the config came from so it can name the broken mount"
    );

    // The file is back on disk: a config directory that IS durable heals
    // itself, and a hand-edit finds a real config either way.
    let text = std::fs::read_to_string(&cfg_path).expect("config file restored");
    let cfg = nzbd_config::Config::from_toml(&text).unwrap();
    assert_eq!(cfg.api.password.as_deref(), Some("recovered-pw"));
    assert_eq!(cfg.servers.len(), 1);

    // The queue is live — recovery means running, not just not-wizarding.
    let (code, body) = try_http(
        &api_addr,
        "GET",
        "/api/v1/status",
        b"",
        Some("nzbd:recovered-pw"),
    )
    .unwrap();
    assert_eq!(code, 200, "{body}");
}

/// First-run setup: booting with a missing --config serves the wizard;
/// POST writes the file; the daemon reloads with it (auth turns on).
#[test]
fn first_run_setup_wizard_writes_config_and_reloads() {
    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let api_addr = format!("127.0.0.1:{port}");
    // Parent dir doesn't exist either — setup must create it.
    let cfg_path = tmp.path().join("conf/nzbd.toml");

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .args(["--bind", &api_addr])
        // First-run mode boots with Config::default() until setup writes the
        // real file. Keep that default state under this test's tempdir rather
        // than touching the invoking user's ~/downloads tree.
        .env("HOME", tmp.path())
        .env(
            nzbd_config::durable::MAIN_DIR_ENV,
            tmp.path().join("isolated-main"),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    let (code, body) = http(&api_addr, "GET", "/api/v1/setup", b"");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["setup_mode"], true, "{body}");
    // No config yet means no state directory, so the wizard must not name a
    // mirror path: `current` is still Config::default() there, and reporting
    // its state_dir would print a confident path derived from a main_dir the
    // operator has not chosen yet.
    assert!(
        v["mirror_path"].is_null(),
        "setup mode must not name a mirror: {body}"
    );

    let req = serde_json::json!({
        "main_dir": tmp.path().join("data").to_string_lossy(),
        "dest_dir": tmp.path().join("data/complete").to_string_lossy(),
        "server": {
            "host": "127.0.0.1", "port": 1199, "tls": false,
            "username": "u", "password": "p", "connections": 2
        },
        "api_password": "wizard-pw"
    });
    let (code, body) = http(
        &api_addr,
        "POST",
        "/api/v1/setup",
        req.to_string().as_bytes(),
    );
    assert_eq!(code, 200, "{body}");

    // The daemon bounces its listener and comes back with auth enabled.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "daemon did not reload");
        std::thread::sleep(Duration::from_millis(200));
        match try_http(&api_addr, "GET", "/api/v1/status", b"", None) {
            Some((401, _)) => break, // reloaded: new config requires auth
            _ => continue,
        }
    }

    // The written file exists and round-trips the strict parser.
    let text = std::fs::read_to_string(&cfg_path).unwrap();
    let cfg = nzbd_config::Config::from_toml(&text).unwrap();
    assert_eq!(cfg.servers.len(), 1);
    assert_eq!(cfg.servers[0].host, "127.0.0.1");
    assert!(!cfg.servers[0].tls);
    assert_eq!(cfg.api.password.as_deref(), Some("wizard-pw"));
    assert_eq!(cfg.api.bind, api_addr);

    // …and the durable copy landed beside the state, byte for byte. This is
    // what makes the config survive a container that loses its config mount.
    let mirror = nzbd_config::durable::mirror_path(&cfg.state_dir());
    let saved = std::fs::read_to_string(&mirror)
        .unwrap_or_else(|e| panic!("no durable copy at {}: {e}", mirror.display()));
    assert_eq!(
        saved, text,
        "the mirror is the config, not a re-render of it"
    );

    // …and once a config exists the endpoint names it, so the UI banner can
    // tell the operator where their safety net actually lives.
    let (_, body) = try_http(
        &api_addr,
        "GET",
        "/api/v1/setup",
        b"",
        Some("nzbd:wizard-pw"),
    )
    .unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["mirror_path"].as_str(),
        Some(mirror.display().to_string().as_str()),
        "a running daemon names its mirror: {body}"
    );

    // Authenticated requests work; setup mode is over.
    let (code, body) = try_http(
        &api_addr,
        "GET",
        "/api/v1/setup",
        b"",
        Some("nzbd:wizard-pw"),
    )
    .unwrap();
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["setup_mode"], false);
    let (code, _) = try_http(
        &api_addr,
        "GET",
        "/api/v1/status",
        b"",
        Some("nzbd:wizard-pw"),
    )
    .unwrap();
    assert_eq!(code, 200);
}

#[test]
fn daemon_cli_compat_end_to_end() {
    // Mock provider on its own runtime.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = prng_bytes(5, 120_000);
    let post = build_post("cli demo", &[("payload.bin", data.clone())], 25_000);
    let ns = rt.block_on(async { NservBuilder::new().with_post(&post).start().await.unwrap() });

    let tmp = tempfile::tempdir().unwrap();
    let api_port = free_port();
    let api_addr = format!("127.0.0.1:{api_port}");

    let config = format!(
        r#"
[paths]
main_dir = "{main}"
dest_dir = "{dest}"

[[server]]
name = "mock"
host = "127.0.0.1"
port = {nntp_port}
tls = false
connections = 4
pipeline_depth = 2

[api]
bind = "{api_addr}"
"#,
        main = tmp.path().join("main").display(),
        dest = tmp.path().join("dest").display(),
        nntp_port = ns.port(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let nzb_path = tmp.path().join("cli demo.nzb");
    std::fs::write(&nzb_path, &post.nzb).unwrap();

    // Boot the daemon.
    let bin = env!("CARGO_BIN_EXE_nzbd");
    let daemon_log_path = tmp.path().join("daemon.log");
    let daemon_log = std::fs::File::create(&daemon_log_path).unwrap();
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(daemon_log.try_clone().unwrap())
        .stderr(daemon_log)
        .spawn()
        .expect("spawn nzbd");
    let mut child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    // `nzbd add` via the real CLI.
    let out = Command::new(bin)
        .args(["add"])
        .arg(&nzb_path)
        .args(["--url", &api_addr])
        .output()
        .expect("run nzbd add");
    assert!(
        out.status.success(),
        "add failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("add output json");
    assert!(v["id"].as_u64().is_some());

    // Wait for the finished download on disk (post-processing then retires
    // the job from the queue into history, so queue counters are transient).
    // PP's final deobfuscation pass renames the generically-named payload
    // to the job name, so that is the path that must appear.
    let start = Instant::now();
    let payload_path = tmp.path().join("dest/cli demo/cli demo.bin");
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "download did not finish\n--- daemon log ---\n{}",
            std::fs::read_to_string(&daemon_log_path)
                .unwrap_or_else(|error| format!("could not read daemon log: {error}"))
        );
        if std::fs::read(&payload_path)
            .map(|got| got == data)
            .unwrap_or(false)
        {
            break;
        }
        // `nzbd status` keeps answering while we wait (CLI liveness check).
        let out = Command::new(bin)
            .args(["status", "--url", &api_addr])
            .output()
            .unwrap();
        assert!(out.status.success());
        std::thread::sleep(Duration::from_millis(200));
    }

    // The finished job lands in history with a SUCCESS status.
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "job never reached history"
        );
        let (code, body) = http(&api_addr, "GET", "/api/v1/history", b"");
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        if v["entries"]
            .as_array()
            .is_some_and(|e| e.iter().any(|h| h["status"] == "SUCCESS"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Compat shim: NZBGet JSON-RPC 1.1 dialect.
    let (code, body) = http(
        &api_addr,
        "POST",
        "/jsonrpc",
        br#"{"method":"version","id":3}"#,
    );
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["version"], "1.1");
    assert_eq!(v["result"], "26.2");
    assert!(v.get("jsonrpc").is_none());

    let (_, body) = http(
        &api_addr,
        "POST",
        "/jsonrpc",
        br#"{"method":"status","id":4}"#,
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["result"]["RemainingSizeLo"].is_number());
    assert_eq!(v["result"]["RemainingSizeMB"], 0);

    // Graceful shutdown on SIGINT clears the unclean marker.
    let pid = child.0.id();
    unsafe {
        libc_kill(pid as i32, 2 /* SIGINT */);
    }
    let start = Instant::now();
    loop {
        if let Ok(Some(_)) = child.0.try_wait() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "daemon did not exit on SIGINT"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !tmp.path().join("main/queue/unclean.local").exists(),
        "graceful shutdown must clear the unclean marker"
    );

    drop(ns);
}

extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

/// Deleting a queued job parks it: the regenerated NZB is spooled, a
/// `DELETED` history entry appears, and `requeue` puts the job back. This
/// is what makes a one-click delete safe enough to need no confirmation
/// dialog — a misclick on a 60 GiB job costs an Undo, not a re-download.
#[test]
fn delete_parks_the_job_and_requeue_brings_it_back() {
    let tmp = tempfile::tempdir().unwrap();
    let api_port = free_port();
    let api_addr = format!("127.0.0.1:{api_port}");
    let state_dir = tmp.path().join("main/queue");

    // No provider needed: the job is added paused, so nothing dials out.
    let config = format!(
        r#"
[paths]
main_dir = "{main}"
dest_dir = "{dest}"

[[server]]
name = "unused"
host = "127.0.0.1"
port = {dead_port}
tls = false
connections = 1

[api]
bind = "{api_addr}"
"#,
        main = tmp.path().join("main").display(),
        dest = tmp.path().join("dest").display(),
        dead_port = free_port(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let post = build_post("park me", &[("payload.bin", prng_bytes(9, 40_000))], 20_000);
    let bin = env!("CARGO_BIN_EXE_nzbd");
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    let (code, body) = http(
        &api_addr,
        "POST",
        "/api/v1/jobs?name=park%20me&category=tv&paused=true",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201, "{body}");
    let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_u64()
        .expect("job id");

    // Delete: the response says whether an Undo is on offer.
    let (code, body) = http(
        &api_addr,
        "POST",
        &format!("/api/v1/jobs/{id}/actions/delete"),
        b"",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["parked"], true, "a deleted queued job must be undoable");

    // Gone from the queue…
    let (_, body) = http(&api_addr, "GET", "/api/v1/jobs", b"");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v["jobs"].as_array().unwrap().is_empty(),
        "the job is really gone from the queue: {body}"
    );
    // …the NZB is on disk where requeue will look for it…
    let spool = state_dir.join(format!("nzbs/{id}.nzb"));
    assert!(
        spool.is_file(),
        "spooled NZB missing at {}",
        spool.display()
    );

    // …and it is parked in history as DELETED, flagged requeueable.
    let (code, body) = http(&api_addr, "GET", "/api/v1/history", b"");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entry = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["job"] == id)
        .unwrap_or_else(|| panic!("no history entry for the deleted job: {body}"))
        .clone();
    assert_eq!(entry["status"], "DELETED");
    assert_eq!(entry["can_requeue"], true);
    assert_eq!(entry["category"], "tv");
    assert_eq!(entry["name"], "park me");

    // Undo.
    let (code, body) = http(
        &api_addr,
        "POST",
        &format!("/api/v1/history/{id}/actions/requeue"),
        b"",
    );
    assert_eq!(code, 200, "{body}");
    let new_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_u64()
        .expect("requeued job id");

    let (_, body) = http(&api_addr, "GET", "/api/v1/jobs", b"");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let jobs = v["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1, "the job is back in the queue: {body}");
    assert_eq!(jobs[0]["id"], new_id);
    assert_eq!(jobs[0]["name"], "park me");
    assert_eq!(jobs[0]["category"], "tv");

    // The parked record and its spool are gone: the job is queued again, so
    // a DELETED entry for it would be a lie.
    let (_, body) = http(&api_addr, "GET", "/api/v1/history", b"");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        !v["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["job"] == id),
        "the parked entry is consumed by the requeue: {body}"
    );
    assert!(!spool.is_file(), "the spooled NZB is reaped with its entry");

    // Requeueing something that was never parked is a 404, not a 500.
    let (code, _) = http(
        &api_addr,
        "POST",
        "/api/v1/history/99999/actions/requeue",
        b"",
    );
    assert_eq!(code, 404);
}

fn jsonrpc(addr: &str, body: serde_json::Value) -> serde_json::Value {
    let (code, text) = http(addr, "POST", "/jsonrpc", body.to_string().as_bytes());
    assert_eq!(code, 200, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// The exact call sequence a Sonarr/Radarr download client makes against
/// NZBGet: version gate → config (category check) → append(base64) →
/// listgroups poll → history poll → import from FinalDir.
#[test]
fn sonarr_style_flow_over_jsonrpc() {
    use base64::Engine as _;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = prng_bytes(9, 90_000);
    let post = build_post("arr episode", &[("episode.mkv", data.clone())], 25_000);
    let ns = rt.block_on(async { NservBuilder::new().with_post(&post).start().await.unwrap() });

    let tmp = tempfile::tempdir().unwrap();
    let api_port = free_port();
    let api_addr = format!("127.0.0.1:{api_port}");
    let config = format!(
        r#"
[paths]
main_dir = "{main}"
dest_dir = "{dest}"

[[server]]
name = "mock"
host = "127.0.0.1"
port = {nntp_port}
tls = false
connections = 4

[[category]]
name = "tv"

[api]
bind = "{api_addr}"
"#,
        main = tmp.path().join("main").display(),
        dest = tmp.path().join("dest").display(),
        nntp_port = ns.port(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let bin = env!("CARGO_BIN_EXE_nzbd");
    let daemon_log_path = tmp.path().join("daemon.log");
    let daemon_log = std::fs::File::create(&daemon_log_path).unwrap();
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(daemon_log.try_clone().unwrap())
        .stderr(daemon_log)
        .spawn()
        .expect("spawn nzbd");
    let _child = KillOnDrop(child);
    wait_healthy(&api_addr, Duration::from_secs(15));

    // 1. Version gate (Sonarr requires >= 12).
    let v = jsonrpc(&api_addr, serde_json::json!({"method": "version", "id": 1}));
    let major: u32 = v["result"]
        .as_str()
        .unwrap()
        .split('.')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(major >= 12);

    // 2. Category exists in config.
    let v = jsonrpc(&api_addr, serde_json::json!({"method": "config", "id": 2}));
    assert!(v["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|o| o["Name"] == "Category1.Name" && o["Value"] == "tv"));

    // 3. append — Sonarr's exact 9-arg positional form.
    let b64 = base64::engine::general_purpose::STANDARD.encode(&post.nzb);
    let v = jsonrpc(
        &api_addr,
        serde_json::json!({
            "method": "append",
            "params": ["arr episode.nzb", b64, "tv", 0, false, false, "", 0, "SCORE"],
            "id": 3
        }),
    );
    let nzbid = v["result"].as_i64().unwrap();
    assert!(nzbid > 0, "append returned {v}");

    // 4. Poll listgroups until the download leaves the queue…
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(45),
            "job never left the queue\n--- daemon log ---\n{}",
            std::fs::read_to_string(&daemon_log_path)
                .unwrap_or_else(|error| format!("could not read daemon log: {error}"))
        );
        let v = jsonrpc(
            &api_addr,
            serde_json::json!({"method": "listgroups", "id": 4}),
        );
        let groups = v["result"].as_array().unwrap();
        if groups.is_empty() {
            break;
        }
        assert_eq!(groups[0]["NZBID"].as_i64().unwrap(), nzbid);
        std::thread::sleep(Duration::from_millis(200));
    }

    // 5. …then find it in history, successful, with the import path.
    let start = Instant::now();
    let entry = loop {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "job never reached history"
        );
        let v = jsonrpc(&api_addr, serde_json::json!({"method": "history", "id": 5}));
        let hist = v["result"].as_array().unwrap().clone();
        if let Some(e) = hist.iter().find(|e| e["NZBID"].as_i64() == Some(nzbid)) {
            break e.clone();
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(entry["Status"], "SUCCESS/ALL");
    assert_eq!(entry["Category"], "tv");
    let final_dir = entry["FinalDir"].as_str().unwrap();
    assert!(!final_dir.is_empty());

    // 6. Import: the completed file is where history says it is. The
    // deobfuscation pass renamed the generic "episode.mkv" to the job name
    // — exactly what Sonarr wants to see for import.
    let got = std::fs::read(std::path::Path::new(final_dir).join("arr episode.mkv")).unwrap();
    assert_eq!(got, data);
}

/// Regression (reported 2026-07-24): a state directory the daemon could
/// not write killed startup with a bare
/// `state: io: Permission denied (os error 13)` — no path, so an operator
/// had no way to tell which directory to fix.
///
/// Portable half: a regular FILE where the state dir belongs fails with
/// ENOTDIR for everyone, root included, so this always runs.
#[test]
fn unwritable_state_dir_names_the_path_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let main_dir = tmp.path().join("data");
    std::fs::write(&main_dir, b"i am a file").unwrap();
    // The state dir defaults to `<main_dir>/queue`, which cannot be made.
    let queue_dir = main_dir.join("queue");

    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "[paths]\nmain_dir = \"{}\"\ndest_dir = \"{}\"\n",
            main_dir.display(),
            main_dir.join("complete").display()
        ),
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_nzbd"))
        .args(["run", "--config"])
        .arg(&cfg_path)
        .args(["--bind", &format!("127.0.0.1:{}", free_port())])
        .env("RUST_LOG", "info")
        .output()
        .expect("spawn nzbd");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "daemon should refuse to start:\n{stderr}"
    );
    assert!(
        stderr.contains(&queue_dir.display().to_string()),
        "startup error must name the directory it failed on:\n{stderr}"
    );
    // The resolved dirs are logged before anything touches them (the fmt
    // layer writes to stdout; the fatal error goes to stderr).
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("resolved data directories")
            && stdout.contains(&queue_dir.display().to_string()),
        "startup should log the resolved paths:\n{stdout}"
    );
    // `main` prints the error with Debug; it must not escape to one line.
    assert!(
        !stderr.contains("\\n"),
        "error should print readably, not Debug-escaped:\n{stderr}"
    );
}

/// EACCES half: the exact reported case — a state dir owned by someone
/// else. Mode bits mean nothing to root, so this self-skips there.
#[test]
fn permission_denied_state_dir_suggests_the_fix() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let main_dir = tmp.path().join("data");
    let queue_dir = main_dir.join("queue");
    std::fs::create_dir_all(&queue_dir).unwrap();
    std::fs::set_permissions(&queue_dir, std::fs::Permissions::from_mode(0o500)).unwrap();

    if std::fs::write(queue_dir.join(".probe"), b"").is_ok() {
        eprintln!("SKIP permission_denied_state_dir_suggests_the_fix: running privileged");
        return;
    }

    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "[paths]\nmain_dir = \"{}\"\ndest_dir = \"{}\"\n",
            main_dir.display(),
            main_dir.join("complete").display()
        ),
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_nzbd"))
        .args(["run", "--config"])
        .arg(&cfg_path)
        .args(["--bind", &format!("127.0.0.1:{}", free_port())])
        .output()
        .expect("spawn nzbd");

    // Restore before asserting so tempdir cleanup always succeeds.
    std::fs::set_permissions(&queue_dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "daemon should refuse to start");
    assert!(
        stderr.contains(&queue_dir.display().to_string()),
        "must name the unwritable directory:\n{stderr}"
    );
    assert!(
        stderr.contains("Permission denied"),
        "must still report the errno:\n{stderr}"
    );
    assert!(
        stderr.contains("hint:") && stderr.contains("paths.queue_dir"),
        "must tell the operator how to fix it:\n{stderr}"
    );
}
