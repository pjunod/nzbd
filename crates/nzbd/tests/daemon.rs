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
    let mut sock = TcpStream::connect(addr).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).unwrap();
    sock.write_all(body).unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    let text = String::from_utf8_lossy(&resp).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.trim().to_string())
        .unwrap_or_default();
    (status, payload)
}

fn wait_healthy(addr: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > deadline {
            panic!("daemon did not become healthy at {addr}");
        }
        if TcpStream::connect(addr).is_ok() {
            let (code, body) = http(addr, "GET", "/healthz", b"");
            if code == 200 && body == "ok" {
                return;
            }
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

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
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
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
            "download did not finish"
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
    let child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
            "job never left the queue"
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
