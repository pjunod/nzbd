//! Multi-node cluster tests (CLUSTERING.md §11): several full nodes in one
//! process sharing a tempdir "shared volume" and real loopback HTTP, with
//! nzbd-nserv as the provider. Lease intervals are time-compressed.

use nzbd_cluster::{ClusterConfig, ClusterRuntime};
use nzbd_engine::Tuning;
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";

fn server_def(port: u16, connections: u16) -> ServerDef {
    ServerDef {
        id: ServerId(1),
        name: "shared-account".into(), // same name on every node = one account
        host: "127.0.0.1".into(),
        port,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: connections,
        pipeline_depth: 2,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    }
}

struct NodeOpts {
    coordinator: bool,
    priority: u32,
    download: bool,
    max_download_jobs: u32,
    /// PP executor (C2). Slots default to 1 when enabled.
    post_process: bool,}

struct Node {
    #[allow(dead_code)] // debugging aid
    name: String,
    url: String,
    runtime: ClusterRuntime,
    serve_cancel: CancellationToken,
    serve_task: JoinHandle<()>,
}

async fn start_node(
    shared: &Path,
    name: &str,
    opts: NodeOpts,
    nserv_port: u16,
    connections: u16,
) -> Node {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("127.0.0.1:{port}");

    let cfg = ClusterConfig {
        node_name: name.to_string(),
        shared_dir: shared.to_path_buf(),
        advertise_url: format!("http://{url}"),
        secret: SECRET.to_string(),
        coordinator: opts.coordinator,
        priority: opts.priority,
        download: opts.download,
        max_download_jobs: opts.max_download_jobs,
        post_process: opts.post_process,
        pp_slots: 1,
        lease_interval: Duration::from_millis(150),
        takeover_after: Duration::from_millis(900),
        worker_ttl: Duration::from_millis(1800),
    };
    let tuning = Tuning {
        retry_interval: Duration::from_millis(400),
        connect_timeout: Duration::from_secs(5),
        article_timeout: Duration::from_secs(10),
        idle_hold: Duration::from_secs(1),
        ..Tuning::default()
    };
    let pp = if opts.post_process {
        let local = shared.join(format!("local-{name}"));
        std::fs::create_dir_all(&local).unwrap();
        let jsonl = shared.join(".nzbd-cluster/history");
        std::fs::create_dir_all(&jsonl).unwrap();
        Some(nzbd_cluster::PpSetup {
            post: nzbd_post::manager::PostConfig::default(),
            history: std::sync::Arc::new(
                nzbd_state::history::HistoryDb::open_tagged(
                    &local.join("history.sqlite"),
                    Some(&jsonl),
                    Some(name),
                )
                .unwrap(),
            ),
        })
    } else {
        None
    };
    let runtime = ClusterRuntime::start(
        cfg,
        vec![server_def(nserv_port, connections)],
        tuning,
        shared.join("complete"),
        None,
        pp,
    )
    .await
    .expect("cluster start");

    let app = runtime.router("26.2");
    let serve_cancel = CancellationToken::new();
    let sc = serve_cancel.clone();
    let serve_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { sc.cancelled().await })
            .await
            .ok();
    });

    Node {
        name: name.to_string(),
        url,
        runtime,
        serve_cancel,
        serve_task,
    }
}

impl Node {
    /// Stop the node (serving + cluster tasks + engine flush). From the
    /// rest of the cluster's perspective this is a death: renewals,
    /// heartbeats and its API all stop.
    async fn kill(self) {
        self.serve_cancel.cancel();
        self.runtime.shutdown().await;
        self.serve_task.abort();
    }
}

fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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

fn get_json(addr: &str, path: &str) -> serde_json::Value {
    let (code, body) = http(addr, "GET", path, b"");
    assert_eq!(code, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

async fn wait_for<F: Fn() -> bool>(what: &str, secs: u64, f: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn journaled_segments(shared: &Path) -> Vec<u32> {
    nzbd_state::JobJournals::replay_all(&shared.join(".nzbd-cluster"))
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.segment_number)
        .collect()
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_nodes_elect_exactly_one_leader() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("idle", &[("x.bin", prng_bytes(1, 1000))], 1000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let opts = |p| NodeOpts {
        coordinator: true,
        priority: p,
        download: true,
        max_download_jobs: 1,
            post_process: false,
        };
    let a = start_node(tmp.path(), "a", opts(0), ns.port(), 4).await;
    let b = start_node(tmp.path(), "b", opts(1), ns.port(), 4).await;
    let c = start_node(tmp.path(), "c", opts(2), ns.port(), 4).await;

    wait_for("one agreed leader", 15, || {
        let views: Vec<_> = [&a, &b, &c]
            .iter()
            .map(|n| get_json(&n.url, "/api/v1/cluster"))
            .collect();
        let leaders = views
            .iter()
            .filter(|v| v["is_leader"].as_bool() == Some(true))
            .count();
        let names: Vec<_> = views
            .iter()
            .filter_map(|v| v["leader"]["node"].as_str().map(String::from))
            .collect();
        leaders == 1
            && names.len() == 3
            && names.windows(2).all(|w| w[0] == w[1])
            && views[0]["nodes"].as_array().is_some_and(|n| n.len() == 3)
    })
    .await;

    a.kill().await;
    b.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn distributed_download_via_any_node_with_budgets() {
    let tmp = tempfile::tempdir().unwrap();
    let files = [("one.bin".to_string(), prng_bytes(11, 120_000))];
    let post = build_post(
        "clusterjob",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // Leader cannot download; the worker must get the job.
    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;

    // Add via the WORKER's API: must proxy to the leader.
    let (code, body) = http(
        &b.url,
        "POST",
        "/api/v1/jobs?name=clusterjob",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201, "proxied add failed: {body}");

    // The job gets delegated to b and completes.
    wait_for("delegation to b", 15, || {
        let v = get_json(&a.url, "/api/v1/jobs");
        v["jobs"][0]["assigned_node"].as_str() == Some("b") || v["jobs"][0]["status"] == "completed"
    })
    .await;
    wait_for("completion", 30, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;

    // Bit-identical output on the shared volume; work done by b.
    let got = std::fs::read(tmp.path().join("complete/clusterjob/one.bin")).unwrap();
    assert_eq!(got, files[0].1);
    assert!(ns.total_hits() > 0);

    // Connection budget: account cap 4 split across leader + 1 downloading
    // node = 2 concurrent connections max (±0 — the gauge is exact).
    assert!(
        ns.max_concurrent_connections() <= 2,
        "budget exceeded: {} concurrent connections",
        ns.max_concurrent_connections()
    );

    // The worker's own API view (proxied) agrees.
    let v = get_json(&b.url, "/api/v1/status");
    assert_eq!(v["jobs_finished"], 1);

    a.kill().await;
    b.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn worker_death_reclaims_and_resumes_elsewhere_without_refetch() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(21, 40 * 4096);
    let post = build_post("reclaimable", &[("big.bin", data.clone())], 4096);

    // Parts 1..=6 fast, the rest slow — b will journal a few then die.
    let mut builder = NservBuilder::new().with_post(&post);
    for part in 7..=40 {
        builder = builder.behavior(
            &post.message_id("big.bin", part),
            nzbd_nserv::Behavior::Delay(Duration::from_millis(400)),
        );
    }
    let ns = builder.start().await.unwrap();

    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    let (code, _) = http(
        &a.url,
        "POST",
        "/api/v1/jobs?name=reclaimable",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201);

    // Wait until b journaled some segments, then kill it.
    let shared = tmp.path().to_path_buf();
    wait_for("progress on b", 20, || {
        journaled_segments(&shared).len() >= 3
    })
    .await;
    let done_before = journaled_segments(&shared);
    b.kill().await;

    // Third node joins; the lease expires; the job is reclaimed and
    // re-delegated to c, which must not re-fetch journaled segments.
    let c = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("completion after reclaim", 60, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;

    let got = std::fs::read(tmp.path().join("complete/reclaimable/big.bin")).unwrap();
    assert_eq!(got, data, "resumed file must be bit-identical");
    for seg in &done_before {
        assert_eq!(
            ns.hits(&post.message_id("big.bin", *seg)),
            1,
            "segment {seg} was journaled by b and must not be re-fetched by c"
        );
    }

    a.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leader_death_fails_over_and_adopts_the_running_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(31, 40 * 4096);
    let post = build_post("failover", &[("f.bin", data.clone())], 4096);

    let mut builder = NservBuilder::new().with_post(&post);
    for part in 7..=40 {
        builder = builder.behavior(
            &post.message_id("f.bin", part),
            nzbd_nserv::Behavior::Delay(Duration::from_millis(300)),
        );
    }
    let ns = builder.start().await.unwrap();

    // a: leader, no downloads. b: pure worker. c: standby coordinator.
    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;
    let c = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: true,
            priority: 4,
            download: false,
            max_download_jobs: 0,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("every node sees a as leader", 15, || {
        [&a, &b, &c]
            .iter()
            .all(|n| get_json(&n.url, "/api/v1/cluster")["leader"]["node"].as_str() == Some("a"))
            && get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    let (code, _) = http(
        &c.url,
        "POST",
        "/api/v1/jobs?name=failover",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201, "add via standby proxies to leader");

    // b makes progress, then the leader dies.
    let shared = tmp.path().to_path_buf();
    wait_for("progress on b", 20, || {
        journaled_segments(&shared).len() >= 3
    })
    .await;
    let done_before = journaled_segments(&shared);
    let epoch_before = get_json(&c.url, "/api/v1/cluster")["epoch"]
        .as_u64()
        .unwrap();
    a.kill().await;

    // c takes over with a higher epoch; b keeps executing (its lease is
    // adopted via heartbeat, not restarted).
    wait_for("c takes office", 30, || {
        let v = get_json(&c.url, "/api/v1/cluster");
        v["is_leader"].as_bool() == Some(true) && v["epoch"].as_u64().unwrap_or(0) > epoch_before
    })
    .await;
    wait_for("completion under the new leader", 60, || {
        get_json(&c.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;

    let got = std::fs::read(tmp.path().join("complete/failover/f.bin")).unwrap();
    assert_eq!(got, data);
    for seg in &done_before {
        assert_eq!(
            ns.hits(&post.message_id("f.bin", *seg)),
            1,
            "segment {seg} must not be re-fetched across the failover"
        );
    }
    // b's view agrees the job is done (proxied to c).
    assert_eq!(get_json(&b.url, "/api/v1/status")["jobs_finished"], 1);

    b.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_restart_keeps_the_queue() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(41, 50_000);
    let post = build_post("solo", &[("s.bin", data.clone())], 10_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let opts = || NodeOpts {
        coordinator: true,
        priority: 0,
        download: true,
        max_download_jobs: 2,
            post_process: false,
        };
    let a = start_node(tmp.path(), "solo", opts(), ns.port(), 4).await;
    wait_for("self-election", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;

    let (code, _) = http(
        &a.url,
        "POST",
        "/api/v1/jobs?name=solo",
        post.nzb.as_bytes(),
    );
    assert_eq!(code, 201);
    wait_for("completion", 30, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;
    assert_eq!(
        std::fs::read(tmp.path().join("complete/solo/s.bin")).unwrap(),
        data
    );
    a.kill().await;

    // Restart: the queue authority state survives on the shared volume.
    let a2 = start_node(tmp.path(), "solo", opts(), ns.port(), 4).await;
    wait_for("re-election after restart", 20, || {
        get_json(&a2.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    wait_for("queue recovered", 15, || {
        let v = get_json(&a2.url, "/api/v1/jobs");
        v["jobs"][0]["status"] == "completed" && v["jobs"][0]["name"] == "solo"
    })
    .await;
    a2.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pp_runs_on_idle_node_via_anti_affinity() {
    // C2: the leader downloads a job with a real par2 set; the scheduler
    // must hand post-processing to the idle non-download node, which
    // quick-verifies natively, stamps the job, appends shared-volume
    // history and returns the finished job to the leader.
    let tmp = tempfile::tempdir().unwrap();

    let src = tempfile::tempdir().unwrap();
    let payload = prng_bytes(77, 90_000);
    std::fs::write(src.path().join("payload.bin"), &payload).unwrap();
    let ok = std::process::Command::new("par2")
        .args(["create", "-q", "-q", "-s8192", "-c4", "set.par2", "payload.bin"])
        .current_dir(src.path())
        .status()
        .expect("par2 binary required (apt-get install par2)")
        .success();
    assert!(ok, "par2 create failed");
    let mut files: Vec<(String, Vec<u8>)> = vec![("payload.bin".into(), payload.clone())];
    let mut pars: Vec<_> = std::fs::read_dir(src.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "par2").unwrap_or(false))
        .collect();
    pars.sort();
    for p in pars {
        files.push((
            p.file_name().unwrap().to_string_lossy().into_owned(),
            std::fs::read(&p).unwrap(),
        ));
    }
    let post = build_post(
        "pardl",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // a: leader + the only downloader, NOT a PP node.
    // c: cannot download, PP executor — the anti-affinity target.
    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: true,
            max_download_jobs: 2,
            post_process: false,
        },
        ns.port(),
        4,
    )
    .await;
    let c = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: false,
            max_download_jobs: 0,
            post_process: true,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    wait_for("both nodes registered", 15, || {
        get_json(&a.url, "/api/v1/cluster")["nodes"]
            .as_array()
            .is_some_and(|n| n.len() == 2)
    })
    .await;

    let (code, body) = http(&a.url, "POST", "/api/v1/jobs?name=pardl", post.nzb.as_bytes());
    assert_eq!(code, 201, "add failed: {body}");

    // Download completes on a (c can't download); PP is assigned to c,
    // executes there, and the stamped job comes back completed. Node a has
    // NO PP manager (post_process=false), so the stamp appearing at all
    // proves remote execution; the history file below proves it was c.
    wait_for("pp done (remotely)", 45, || {
        let j = &get_json(&a.url, "/api/v1/jobs")["jobs"][0];
        j["pp_done"].as_bool() == Some(true) && j["status"] == "completed"
    })
    .await;

    // Payload survived PP bit-identically (no repair was needed).
    let got = std::fs::read(tmp.path().join("complete/pardl/payload.bin")).unwrap();
    assert_eq!(got, payload);

    // No staging residue in the job dir.
    let residue: Vec<String> = std::fs::read_dir(tmp.path().join("complete/pardl"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".pp."))
        .collect();
    assert!(residue.is_empty(), "staging residue: {residue:?}");

    // History JSONL appended by node c on the shared volume.
    let hist = std::fs::read_to_string(
        tmp.path().join(".nzbd-cluster/history/history.c.jsonl"),
    )
    .expect("node c must have appended shared history");
    assert!(hist.contains("\"SUCCESS\""), "history: {hist}");
    assert!(hist.contains("\"pardl\""), "history: {hist}");

    a.kill().await;
    c.kill().await;
}
