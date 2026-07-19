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
