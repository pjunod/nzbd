//! End-to-end engine tests against the in-tree mock NNTP server
//! (ARCHITECTURE.md §14): bit-identical downloads, tier failover, CRC
//! retry, health gating, pause/resume/delete, and unclean-restart resume.

use nzbd_engine::{Engine, EngineConfig, EngineHandle, Event, Tuning};
use nzbd_nserv::{build_post, prng_bytes, Behavior, GeneratedPost, Nserv, NservBuilder};
use nzbd_types::{CertLevel, JobId, JobStatus, ServerDef, ServerId, TlsMode};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::broadcast;

fn server_def(id: u32, port: u16, tier: u8, connections: u16, pipeline: u8) -> ServerDef {
    ServerDef {
        id: ServerId(id),
        name: format!("nserv-{id}"),
        host: "127.0.0.1".into(),
        port,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier,
        group: 0,
        fill: false,
        max_connections: connections,
        pipeline_depth: pipeline,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    }
}

fn test_tuning() -> Tuning {
    Tuning {
        retry_interval: Duration::from_millis(500),
        connect_timeout: Duration::from_secs(5),
        article_timeout: Duration::from_secs(10),
        idle_hold: Duration::from_secs(1),
        ..Tuning::default()
    }
}

async fn spawn_engine(dir: &Path, servers: Vec<ServerDef>) -> EngineHandle {
    Engine::spawn(EngineConfig::single_node(
        servers,
        dir.join("state"),
        dir.join("dest"),
        test_tuning(),
        None,
    ))
    .await
    .expect("engine spawn")
}

async fn wait_finished(
    rx: &mut broadcast::Receiver<Event>,
    job: JobId,
    secs: u64,
) -> (JobStatus, u16) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            match rx.recv().await {
                Ok(Event::JobFinished {
                    job: j,
                    status,
                    health,
                    ..
                }) if j == job => return (status, health),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(e) => panic!("event stream closed: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for JobFinished")
}

/// Payload with dot-heavy and escape-heavy regions: encoded lines starting
/// with '.', plus every escaped character class, at awkward boundaries.
fn nasty_bytes(len: usize) -> Vec<u8> {
    let mut v = prng_bytes(4242, len);
    for chunk in v.chunks_mut(97) {
        if chunk.len() > 8 {
            chunk[0] = 0x04; // encodes to '.' (dot-stuffing)
            chunk[1] = 0xD6; // encodes to NUL -> escaped
            chunk[2] = 0xE0; // encodes to LF -> escaped
            chunk[3] = 0xE3; // encodes to CR -> escaped
            chunk[4] = 0x13; // encodes to '=' -> escaped
        }
    }
    v
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_bit_identical_with_auth_and_pipelining() {
    let tmp = tempfile::tempdir().unwrap();
    let files = vec![
        ("alpha.bin".to_string(), nasty_bytes(300_000)),
        ("beta.bin".to_string(), prng_bytes(7, 4095)),
        ("gamma.bin".to_string(), prng_bytes(8, 70_001)),
    ];
    let post = build_post(
        "demo post",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        30_000,
    );
    let ns = NservBuilder::new()
        .with_post(&post)
        .credentials("alice", "s3cret")
        .start()
        .await
        .unwrap();

    let mut server = server_def(1, ns.port(), 0, 4, 3);
    server.username = Some("alice".into());
    server.password = Some("s3cret".into());

    let engine = spawn_engine(tmp.path(), vec![server]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("demo post", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);

    for (name, data) in &files {
        let path = tmp.path().join("dest").join("demo post").join(name);
        let got =
            std::fs::read(&path).unwrap_or_else(|e| panic!("missing {}: {e}", path.display()));
        assert_eq!(&got, data, "bit-exact: {name}");
        assert!(!tmp
            .path()
            .join("dest")
            .join("demo post")
            .join(format!("{name}.part"))
            .exists());
    }

    let snap = engine.snapshot();
    assert_eq!(snap.jobs.len(), 1);
    assert_eq!(snap.jobs[0].status, JobStatus::Completed);
    assert_eq!(snap.jobs[0].remaining_bytes, 0);
    assert!(snap.session_downloaded_bytes > 300_000);

    engine.shutdown().await;
    assert!(
        !tmp.path().join("state").join("unclean.local").exists(),
        "graceful shutdown clears the unclean marker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_missing_and_corrupt_articles_escalate_tiers() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post(
        "failover",
        &[
            ("one.bin", prng_bytes(11, 50_000)),
            ("two.bin", prng_bytes(12, 50_000)),
        ],
        20_000,
    );
    let missing = post.message_id("one.bin", 2);
    let corrupt = post.message_id("two.bin", 1);

    // Tier 0: has everything except one missing article and one corrupt one.
    let ns_a = NservBuilder::new()
        .with_post(&post)
        .behavior(&missing, Behavior::NotFound)
        .behavior(&corrupt, Behavior::CorruptCrc)
        .start()
        .await
        .unwrap();
    // Tier 1: clean copies of everything.
    let ns_b = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(
        tmp.path(),
        vec![
            server_def(1, ns_a.port(), 0, 2, 2),
            server_def(2, ns_b.port(), 1, 2, 2),
        ],
    )
    .await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("failover", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);

    for name in ["one.bin", "two.bin"] {
        let got = std::fs::read(tmp.path().join("dest/failover").join(name)).unwrap();
        assert_eq!(got, post.file(name).data, "{name}");
    }

    // The backup tier served exactly the two bad articles.
    assert_eq!(ns_b.hits(&missing), 1, "missing article from tier 1");
    assert_eq!(
        ns_b.hits(&corrupt),
        1,
        "corrupt article re-fetched from tier 1"
    );
    assert_eq!(ns_b.total_hits(), 2, "tier 1 must not serve anything else");
    // Tier 0 was asked for the bad ones (and failed them) exactly once each.
    assert_eq!(ns_a.hits(&missing), 1);
    assert_eq!(ns_a.hits(&corrupt), 1);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_connections_retry_without_losing_the_article() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("droppy", &[("d.bin", prng_bytes(21, 60_000))], 15_000);
    let victim = post.message_id("d.bin", 2);

    // Server A drops mid-body for one article; same-tier server M is clean.
    let ns_a = NservBuilder::new()
        .with_post(&post)
        .behavior(&victim, Behavior::DropMid)
        .start()
        .await
        .unwrap();
    let ns_m = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(
        tmp.path(),
        vec![
            server_def(1, ns_a.port(), 0, 2, 1),
            server_def(2, ns_m.port(), 0, 2, 1),
        ],
    )
    .await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("droppy", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);
    let got = std::fs::read(tmp.path().join("dest/droppy/d.bin")).unwrap();
    assert_eq!(got, post.file("d.bin").data);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrecoverable_articles_gate_health_and_zero_fill_gaps() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(31, 10 * 5000);
    let post = build_post("damaged", &[("dmg.bin", data.clone())], 5000);

    // Segments 3..=6 missing everywhere: 40% failed -> health 600 < 850.
    let mut b = NservBuilder::new().with_post(&post);
    for part in 3..=6 {
        b = b.behavior(&post.message_id("dmg.bin", part), Behavior::NotFound);
    }
    let ns = b.start().await.unwrap();

    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("damaged", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Failed, "below critical health");
    assert_eq!(health, 600);

    // The partial file is still assembled: good parts intact, gaps zeroed
    // (par repair operates on exactly this in phase 2).
    let got = std::fs::read(tmp.path().join("dest/damaged/dmg.bin")).unwrap();
    let mut expected = data.clone();
    expected[2 * 5000..6 * 5000].fill(0);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn critical_health_abort_stops_wasting_bandwidth() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(77, 10 * 5000);
    let post = build_post("doomed", &[("doomed.bin", data)], 5000);

    // Parts 1..=4 gone everywhere (40% of bytes): health 600 < critical
    // 850 (no par2 in the set). Parts 5..=10 are served only after a long
    // delay — without the abort, the job would sit through ~3s+ of
    // downloads it can never repair.
    let mut b = NservBuilder::new().with_post(&post);
    for part in 1..=4 {
        b = b.behavior(&post.message_id("doomed.bin", part), Behavior::NotFound);
    }
    for part in 5..=10 {
        b = b.behavior(
            &post.message_id("doomed.bin", part),
            Behavior::Delay(Duration::from_secs(3)),
        );
    }
    let ns = b.start().await.unwrap();

    let engine = Engine::spawn(EngineConfig::single_node(
        vec![server_def(1, ns.port(), 0, 2, 2)],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning {
            health_abort: true,
            ..test_tuning()
        },
        None,
    ))
    .await
    .expect("engine spawn");

    let started = std::time::Instant::now();
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("doomed", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Failed, "unrepairable -> failed");
    // Pending delayed parts were failed by the abort, not downloaded:
    // health lands below the no-abort value of 600.
    assert!(
        health < 600,
        "abort must fail pending segments, got {health}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "job should abort quickly, took {:?}",
        started.elapsed()
    );

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("pausable", &[("p.bin", prng_bytes(41, 40_000))], 10_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
    engine.pause_all().await.unwrap();

    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("pausable", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(ns.total_hits(), 0, "paused queue must not download");
    assert!(engine.snapshot().download_paused);

    engine.resume_all().await.unwrap();
    let (status, _) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);

    // Delete with files.
    assert!(engine.delete_job(job, true).await.unwrap());
    assert!(engine.snapshot().jobs.is_empty());
    let dir = tmp.path().join("dest/pausable");
    for _ in 0..100 {
        if !dir.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!dir.exists(), "job directory removed");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Crash-resume: run 1 downloads part of a job on a runtime that is killed
// without shutdown; run 2 recovers from snapshot + journal and must not
// re-fetch journaled segments.
// ---------------------------------------------------------------------------

fn journaled_segments(state_dir: &Path) -> Vec<u32> {
    nzbd_state::JobJournals::replay_all(state_dir)
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.segment_number)
        .collect()
}

#[test]
fn resume_after_unclean_restart_refetches_nothing_done() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(99, 40 * 4096);
    let post = build_post("resume", &[("big.bin", data.clone())], 4096);
    let journal = tmp.path().join("state");

    // ---- run 1: first five parts servable, the rest stall ----
    let rt1 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt1.block_on(async {
        let mut b = NservBuilder::new().with_post(&post);
        for part in 6..=40 {
            b = b.behavior(
                &post.message_id("big.bin", part),
                Behavior::Delay(Duration::from_secs(120)),
            );
        }
        let ns = b.start().await.unwrap();
        let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
        engine
            .add_nzb("resume", post.nzb.as_bytes(), None, 0)
            .await
            .unwrap();
        // Wait until at least three segments are journaled.
        for _ in 0..400 {
            if journaled_segments(&journal).len() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            journaled_segments(&journal).len() >= 3,
            "no progress before the crash"
        );
        std::mem::forget(ns); // avoid Drop-side effects during the hard kill
        std::mem::forget(engine);
    });
    rt1.shutdown_background(); // kill -9 equivalent: no flush, no marker clear

    let done_before = journaled_segments(&journal);
    assert!(done_before.len() >= 3);
    assert!(
        tmp.path().join("state").join("unclean.local").exists(),
        "unclean marker must survive the crash"
    );

    // ---- run 2: everything servable; must finish and not re-fetch ----
    let rt2 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt2.block_on(async {
        let ns2 = NservBuilder::new().with_post(&post).start().await.unwrap();
        let engine = spawn_engine(tmp.path(), vec![server_def(1, ns2.port(), 0, 3, 2)]).await;
        let mut rx = engine.subscribe();

        // The job must have been recovered from the snapshot.
        let mut job = None;
        for _ in 0..100 {
            let snap = engine.snapshot();
            if let Some(j) = snap.jobs.first() {
                job = Some(j.id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let job = job.expect("recovered job in snapshot");

        let (status, health) = wait_finished(&mut rx, job, 60).await;
        assert_eq!(status, JobStatus::Completed);
        assert_eq!(health, 1000);
        engine.shutdown().await;

        for seg in &done_before {
            assert_eq!(
                ns2.hits(&post.message_id("big.bin", *seg)),
                0,
                "segment {seg} was journaled before the crash and must not be re-fetched"
            );
        }
        let got = std::fs::read(tmp.path().join("dest/resume/big.bin")).unwrap();
        assert_eq!(got, data, "resumed file must be bit-identical");
    });
    rt2.shutdown_background();
}

// Keep helper types referenced (silences unused warnings when individual
// tests are filtered out).
#[allow(dead_code)]
fn _hold(_: &Nserv, _: &GeneratedPost, _: PathBuf) {}

/// URL job: the NZB is fetched over HTTP (local listener), then the job
/// queues and downloads normally; a dead URL fails the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn url_jobs_fetch_then_download() {
    use std::io::{Read as _, Write as _};
    let data = prng_bytes(21, 60_000);
    let post = build_post("urljob", &[("u.bin", data.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // One-shot HTTP server handing out the NZB.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let nzb = post.nzb.clone();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = s.read(&mut buf);
        let _ = s.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{nzb}",
                nzb.len()
            )
            .as_bytes(),
        );
    });

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 2)]).await;
    let mut rx = engine.subscribe();
    let id = engine
        .add_url(
            "urljob",
            &format!("http://127.0.0.1:{http_port}/get.nzb"),
            nzbd_engine::AddOpts::default(),
        )
        .await
        .unwrap();
    // Registered instantly in Fetching state.
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .any(|j| j.id == id && matches!(j.status, JobStatus::Fetching)));

    let (status, _) = wait_finished(&mut rx, id, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(
        std::fs::read(tmp.path().join("dest/urljob/u.bin")).unwrap(),
        data
    );

    // Dead URL: job fails (history classification is FAILURE/FETCH,
    // asserted at the PP layer).
    let dead = engine
        .add_url(
            "deadjob",
            "http://127.0.0.1:1/nope.nzb",
            nzbd_engine::AddOpts::default(),
        )
        .await
        .unwrap();
    let (status, _) = wait_finished(&mut rx, dead, 30).await;
    assert_eq!(status, JobStatus::Failed);
    engine.shutdown().await;
}
