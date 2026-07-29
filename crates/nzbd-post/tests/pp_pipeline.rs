//! Post-processing pipeline integration tests against the REAL `par2` and
//! `7z` binaries (ARCHITECTURE.md §9): quick-verify fast path, damage →
//! subprocess repair, unpack + cleanup, extension scripts, failure
//! classification, and the event-driven manager.

use nzbd_engine::{Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_post::manager::{process_job, spawn_post_manager, PostConfig, PpFinal, PP_DONE_PARAM};
use nzbd_state::history::HistoryDb;
use nzbd_types::{DupeInfo, FileEntry, FileId, Job, JobId, JobKind, JobStatus, JobTotals};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn crc(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

async fn spawn_engine(dir: &Path) -> EngineHandle {
    Engine::spawn(EngineConfig::single_node(
        vec![], // no news servers: PP tests drive imported jobs only
        dir.join("state"),
        dir.join("dest"),
        Tuning::default(),
        None,
    ))
    .await
    .expect("engine spawn")
}

fn file_entry(id: u32, name: &str, crc32: Option<u32>, is_par2: bool) -> FileEntry {
    FileEntry {
        id: FileId(id),
        subject: name.into(),
        filename: name.into(),
        filename_confirmed: true,
        is_par2,
        paused: false,
        groups: vec![],
        date: None,
        segments: vec![],
        crc32,
        finalized: true,
    }
}

fn completed_job(id: u32, name: &str, files: Vec<FileEntry>) -> Job {
    Job {
        id: JobId(id),
        kind: JobKind::Nzb,
        name: name.into(),
        dir_name: String::new(),
        name_provisional: false,
        category: Some("test".into()),
        priority: 0,
        dupe: DupeInfo::default(),
        params: vec![("mykey".into(), "myval".into())],
        files,
        totals: JobTotals::default(),
        status: JobStatus::Completed,
        stages: Vec::new(),
    }
}

fn history(dir: &Path) -> Arc<HistoryDb> {
    Arc::new(HistoryDb::open(&dir.join("history.sqlite"), Some(dir)).unwrap())
}

/// Probe for an external tool; on a miss the calling test self-skips with a
/// notice. `NZBD_REQUIRE_TOOLS` (set in CI) turns the miss into a loud
/// failure so CI can never silently lose coverage.
fn require_tool(tool: &str) -> bool {
    let found = std::process::Command::new(tool)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok();
    if found {
        return true;
    }
    if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
        panic!("`{tool}` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
    }
    eprintln!(
        "SKIPPED: `{tool}` not installed — `brew install par2 p7zip` / `apt-get install par2 p7zip-full` for full local coverage"
    );
    false
}

/// par2-create a recovery set for `files` inside `dir`.
fn par2_create(dir: &Path, blocks: u32, files: &[&str]) {
    let mut args = vec![
        "create".into(),
        "-q".into(),
        "-q".into(),
        "-s8192".into(),
        format!("-c{blocks}"),
        "set.par2".into(),
    ];
    args.extend(files.iter().map(|f| f.to_string()));
    let ok = std::process::Command::new("par2")
        .args(&args)
        .current_dir(dir)
        .status()
        .expect("par2 binary required (apt-get install par2)")
        .success();
    assert!(ok, "par2 create failed");
}

fn par2_entries(dir: &Path, first_id: u32) -> Vec<FileEntry> {
    let mut out = Vec::new();
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "par2").unwrap_or(false))
        .collect();
    names.sort();
    for (i, p) in names.iter().enumerate() {
        let bytes = std::fs::read(p).unwrap();
        out.push(file_entry(
            first_id + i as u32,
            &p.file_name().unwrap().to_string_lossy(),
            Some(crc(&bytes)),
            true,
        ));
    }
    out
}

// ---------------------------------------------------------------------------

/// Intact download: the native quick check proves the set without touching
/// par2; a post-processing script then runs with the NZBGet env and
/// redirects the final dir via `[NZB] FINALDIR=`.
#[tokio::test]
async fn intact_quick_path_then_script() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/myjob");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 8, &["payload.bin"]);

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("notify.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
         [ \"$NZBPP_PARSTATUS\" = 1 ] || exit 94\n\
         [ \"$NZBPP_TOTALSTATUS\" = SUCCESS ] || exit 94\n\
         [ \"$NZBPR_mykey\" = myval ] || exit 94\n\
         echo \"[NZB] FINALDIR=$NZBPP_DIRECTORY/final\"\n\
         exit 93\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_job(completed_job(1, "myjob", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);

    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert!(job
        .params
        .iter()
        .any(|(k, v)| k == PP_DONE_PARAM && v == "SUCCESS"));

    let entries = hist.list(10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].status, "SUCCESS");
    assert!(entries[0].final_dir.as_deref().unwrap().ends_with("/final"));
    engine.shutdown().await;
}

/// Damaged download: quick check spots the bad CRC, par2 verifies + repairs,
/// and the original bytes come back.
#[tokio::test]
async fn corrupt_payload_gets_repaired() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/damaged");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 7) % 253) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 16, &["payload.bin"]);

    // Corrupt one block's worth of bytes *as downloaded* (the engine's
    // whole-file CRC reflects the corruption).
    let mut bad = data.clone();
    for b in &mut bad[20_000..20_100] {
        *b ^= 0xA5;
    }
    std::fs::write(dir.join("payload.bin"), &bad).unwrap();

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_job(completed_job(2, "damaged", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig::default();
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(2))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(
        std::fs::read(dir.join("payload.bin")).unwrap(),
        data,
        "repair must restore the original bytes"
    );
    assert_eq!(hist.list(10).unwrap()[0].status, "SUCCESS");
    engine.shutdown().await;
}

/// Damage beyond the recovery blocks on hand and nothing left to unpause:
/// PAR_FAILURE, job marked Failed.
#[tokio::test]
async fn unrepairable_is_par_failure() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/hopeless");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 13) % 249) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 1, &["payload.bin"]); // one lonely recovery block

    // Trash well more than one 8 KiB slice.
    let mut bad = data.clone();
    for b in &mut bad[8_192..49_152] {
        *b = 0;
    }
    std::fs::write(dir.join("payload.bin"), &bad).unwrap();

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_job(completed_job(3, "hopeless", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(3),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::ParFailure);

    let job = engine.export_job(JobId(3)).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(hist.list(10).unwrap()[0].status, "PAR_FAILURE");
    engine.shutdown().await;
}

/// Archive job: unpack extracts, cleanup removes the archive husks.
#[tokio::test]
async fn unpack_then_cleanup() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/packed");
    std::fs::create_dir_all(&dir).unwrap();

    let inner = b"the actual release content";
    std::fs::write(dir.join("movie.mkv"), inner).unwrap();
    let ok = std::process::Command::new("7z")
        .args(["a", "-tzip", "-y", "release.zip", "movie.mkv"])
        .current_dir(&dir)
        .status()
        .expect("7z binary required (apt-get install p7zip-full)")
        .success();
    assert!(ok);
    std::fs::remove_file(dir.join("movie.mkv")).unwrap();

    let zip_bytes = std::fs::read(dir.join("release.zip")).unwrap();
    let files = vec![file_entry(1, "release.zip", Some(crc(&zip_bytes)), false)];
    engine
        .import_job(completed_job(4, "packed", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    // deobfuscate off: this test pins the unpack/cleanup contract; the
    // final-name pass has its own e2e coverage below.
    let cfg = PostConfig {
        deobfuscate_final: false,
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(4))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(std::fs::read(dir.join("movie.mkv")).unwrap(), inner);
    assert!(
        !dir.join("release.zip").exists(),
        "cleanup must remove the extracted archive"
    );
    engine.shutdown().await;
}

/// A script that exits 94 flips the outcome to SCRIPT_FAILURE and fails
/// the job.
#[tokio::test]
async fn script_error_is_script_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/scripted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("plain.txt"), b"nothing to verify or unpack").unwrap();

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("fail.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 94\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let files = vec![file_entry(
        1,
        "plain.txt",
        Some(crc(b"nothing to verify or unpack")),
        false,
    )];
    engine
        .import_job(completed_job(5, "scripted", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(5))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::ScriptFailure);
    assert_eq!(
        engine.export_job(JobId(5)).await.unwrap().unwrap().status,
        JobStatus::Failed
    );
    assert_eq!(hist.list(10).unwrap()[0].status, "SCRIPT_FAILURE");
    engine.shutdown().await;
}

/// The manager end-to-end: an imported finished job is picked up from the
/// event stream, processed, stamped, and never re-processed on a second
/// manager start (the crash-restart scan honors the stamp).
#[tokio::test]
async fn manager_event_driven_and_restart_safe() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/watched");
    std::fs::create_dir_all(&dir).unwrap();
    let data = b"watched payload".to_vec();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig::default(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    // Let the manager subscribe before the finish event fires.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    engine
        .import_job(completed_job(6, "watched", files), false, true)
        .await
        .unwrap();

    // The manager processes the job, records history, then retires it out
    // of the queue (NZBGet parity: finished jobs live in history).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let gone = engine.export_job(JobId(6)).await.unwrap().is_none();
        if gone && hist.list(10).unwrap().len() == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager never processed + retired the job"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(hist.list(10).unwrap()[0].status, "SUCCESS");

    cancel.cancel();
    tracker.close();
    tracker.wait().await;

    // Second manager start: nothing left to process (the job was retired);
    // history stays at exactly one entry.
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig::default(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel2.clone(),
        &tracker2,
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        hist.list(10).unwrap().len(),
        1,
        "restart must not re-process a finished job"
    );
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    engine.shutdown().await;
}

/// Fully obfuscated post: the payload arrives with a garbage name; the
/// rename stage recovers it via the par2 16k-MD5 catalog, evidence paths
/// remap, and the native quick check still proves the set.
#[tokio::test]
async fn obfuscated_names_recovered_then_quick_verified() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/obfus");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..45_000u32).map(|i| ((i * 3) % 250) as u8).collect();
    std::fs::write(dir.join("Real.Name.S01E02.mkv"), &data).unwrap();
    par2_create(&dir, 4, &["Real.Name.S01E02.mkv"]);
    // Obfuscate on disk, exactly as an obfuscated post downloads.
    std::fs::rename(dir.join("Real.Name.S01E02.mkv"), dir.join("d41d8cd9")).unwrap();

    let mut files = vec![file_entry(1, "d41d8cd9", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_job(completed_job(7, "obfus", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(7),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(
        std::fs::read(dir.join("Real.Name.S01E02.mkv")).unwrap(),
        data,
        "true name restored, bytes intact"
    );
    assert!(!dir.join("d41d8cd9").exists());
    engine.shutdown().await;
}

/// A job whose only media file kept an obfuscated name through PP gets
/// renamed to the job name (SABnzbd-style final pass, no tools needed).
#[tokio::test]
async fn deobfuscate_final_renames_to_job_name() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/Great.Show.S02.1080p.WEB");
    std::fs::create_dir_all(&dir).unwrap();
    let data = vec![7u8; 60_000];
    std::fs::write(dir.join("a1b2c3d4e5f6a7b8.mkv"), &data).unwrap();
    std::fs::write(dir.join("a1b2c3d4e5f6a7b8.eng.srt"), b"subtitles").unwrap();

    let files = vec![file_entry(
        1,
        "a1b2c3d4e5f6a7b8.mkv",
        Some(crc(&data)),
        false,
    )];
    engine
        .import_job(
            completed_job(11, "Great.Show.S02.1080p.WEB", files),
            false,
            false,
        )
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(11),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert!(dir.join("Great.Show.S02.1080p.WEB.mkv").exists());
    assert!(
        dir.join("Great.Show.S02.1080p.WEB.eng.srt").exists(),
        "companion subtitle follows the rename"
    );

    // Durable record: the renames land as job params and reach history.
    let job = engine.export_job(JobId(11)).await.unwrap().unwrap();
    assert!(job
        .params
        .iter()
        .any(|(k, v)| k == "Deobfuscate:Count" && v == "2"));
    let entry = &hist.list(10).unwrap()[0];
    let files = &entry
        .params
        .iter()
        .find(|(k, _)| k == "Deobfuscate:Files")
        .expect("history keeps the rename list")
        .1;
    assert!(files.contains("a1b2c3d4e5f6a7b8.mkv → Great.Show.S02.1080p.WEB.mkv"));
    engine.shutdown().await;
}

/// A fully obfuscated season pack (several similar-sized videos, all
/// hex-named) gets stable numbered names — the case SABnzbd skips.
#[tokio::test]
async fn deobfuscate_final_numbers_season_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/Show.S03.1080p.WEB");
    std::fs::create_dir_all(&dir).unwrap();
    let mut files = Vec::new();
    for (i, stem) in ["9f8e7d6c5b4a3f2e", "1a2b3c4d5e6f7a8b", "deadbeefcafef00d"]
        .iter()
        .enumerate()
    {
        let data = vec![i as u8; 40_000];
        std::fs::write(dir.join(format!("{stem}.mkv")), &data).unwrap();
        files.push(file_entry(
            i as u32 + 1,
            &format!("{stem}.mkv"),
            Some(crc(&data)),
            false,
        ));
    }
    engine
        .import_job(completed_job(12, "Show.S03.1080p.WEB", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(12),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    for n in 1..=3 {
        assert!(
            dir.join(format!("Show.S03.1080p.WEB - {n:02}.mkv"))
                .exists(),
            "episode {n} numbered"
        );
    }
    engine.shutdown().await;
}

/// Per-job password (`*Unpack:Password` parameter, NZBGet convention)
/// reaches the extractor.
#[tokio::test]
async fn per_job_password_unlocks_archive() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/locked");
    std::fs::create_dir_all(&dir).unwrap();

    let inner = b"secret contents";
    std::fs::write(dir.join("file.bin"), inner).unwrap();
    let ok = std::process::Command::new("7z")
        .args(["a", "-tzip", "-y", "-phunter2", "locked.zip", "file.bin"])
        .current_dir(&dir)
        .status()
        .unwrap()
        .success();
    assert!(ok);
    std::fs::remove_file(dir.join("file.bin")).unwrap();

    let zip = std::fs::read(dir.join("locked.zip")).unwrap();
    let files = vec![file_entry(1, "locked.zip", Some(crc(&zip)), false)];
    let mut job = completed_job(8, "locked", files);
    job.params
        .push(("*Unpack:Password".into(), "hunter2".into()));
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    // deobfuscate off: the password path is under test, not final naming.
    let cfg = PostConfig {
        deobfuscate_final: false,
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(8))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(std::fs::read(dir.join("file.bin")).unwrap(), inner);
    engine.shutdown().await;
}

/// HealthAction::Delete removes the failed download's files from disk.
#[tokio::test]
async fn health_action_delete_removes_files() {
    use nzbd_post::manager::HealthAction;
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/sick");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("partial.bin"), b"broken half-download").unwrap();

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            health_action: HealthAction::Delete,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let files = vec![file_entry(1, "partial.bin", None, false)];
    let mut job = completed_job(9, "sick", files);
    job.status = JobStatus::Failed;
    engine.import_job(job, false, true).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let recorded = hist
            .list(10)
            .unwrap()
            .iter()
            .any(|e| e.status == "FAILURE/HEALTH");
        if recorded && !dir.exists() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "health delete never happened (dir exists: {})",
            dir.exists()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// N6 — category destination honesty (docs/INTEGRATION_PLAN.md)
// ---------------------------------------------------------------------------

/// `[[category]] dest_dir` was parsed and advertised to compat clients as
/// `CategoryN.DestDir` for a long time while post-processing quietly wrote
/// somewhere else. An *arr that path-maps off the advertised value then
/// looks in a folder that will never contain anything — a silent import
/// failure with nothing in any log to explain it. Advertised must equal
/// actual, and "actual" means: the files are there, and every place we
/// report the path agrees.
#[tokio::test]
async fn category_dest_dir_is_where_the_files_actually_land() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/catjob");
    std::fs::create_dir_all(&dir).unwrap();
    let data = b"payload".to_vec();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();

    let library = tmp.path().join("library/tv");
    let mut job = completed_job(1, "catjob", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("TV".into()); // matched case-insensitively
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        // Off so the assertion below can name the file: the final
        // deobfuscation pass would rename it to the job name, which is a
        // different feature's business.
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);

    let landed = library.join("catjob/payload.bin");
    assert!(
        std::fs::read(&landed).unwrap_or_default() == data,
        "files must be under the category destination: {}",
        landed.display()
    );
    assert!(
        !tmp.path().join("dest/catjob").exists(),
        "and must not be left behind in the global destination"
    );
    let entry = &hist.list(10).unwrap()[0];
    assert_eq!(
        entry.final_dir.as_deref(),
        library.join("catjob").to_str(),
        "history must report where the files are, not where they started"
    );
    engine.shutdown().await;
}

/// A category that turns unpacking off must actually leave the archive
/// alone. (The key was advertised as `CategoryN.Unpack` and ignored.)
#[tokio::test]
async fn category_unpack_false_leaves_the_archive_alone() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/nounpack");
    std::fs::create_dir_all(&dir).unwrap();
    let inner = dir.join("inside.txt");
    std::fs::write(&inner, b"secret").unwrap();
    let archive = dir.join("bundle.7z");
    let ok = std::process::Command::new("7z")
        .arg("a")
        .arg(&archive)
        .arg(&inner)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(ok, "could not build the test archive");
    std::fs::remove_file(&inner).unwrap();

    let mut job = completed_job(1, "nounpack", vec![file_entry(1, "bundle.7z", None, false)]);
    job.category = Some("raw".into());
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        // Off, like the sibling category tests: the final deobfuscation
        // pass renames a lone leftover file to the job name, so with it on
        // the surviving archive is `nounpack.7z` and an assertion by name
        // fails for a reason that has nothing to do with unpacking.
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "raw".into(),
            unpack: Some(false),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert!(archive.is_file(), "the archive must survive");
    assert!(
        !inner.exists(),
        "nothing should have been extracted for a category with unpack = false"
    );
    engine.shutdown().await;
}

/// `extensions` selects which post-processing scripts a category runs.
/// The key was parsed and then neither implemented nor removed; leaving it
/// half-done is the same lie as `dest_dir` was.
#[tokio::test]
async fn category_extensions_select_which_scripts_run() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/scripted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"x").unwrap();

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let touched = tmp.path().join("touched");
    for name in ["wanted.sh", "unwanted.sh"] {
        let path = scripts.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
                 echo {name} >> {}\nexit 93\n",
                touched.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    let mut job = completed_job(
        1,
        "scripted",
        vec![file_entry(1, "payload.bin", None, false)],
    );
    job.category = Some("tv".into());
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            extensions: vec!["wanted".into()], // by stem; the file is wanted.sh
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    let ran = std::fs::read_to_string(&touched).unwrap_or_default();
    assert!(ran.contains("wanted.sh"), "the selected script must run");
    assert!(
        !ran.contains("unwanted.sh"),
        "a script outside the category's extensions must not run: {ran:?}"
    );
    engine.shutdown().await;
}

/// A job with no matching category behaves exactly as before — the global
/// destination, the global unpack setting, every discovered script.
#[tokio::test]
async fn a_job_without_a_category_rule_is_untouched_by_any_of_this() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/plain");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"x").unwrap();

    let mut job = completed_job(1, "plain", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("movies".into()); // configured category is "tv"
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(tmp.path().join("library/tv")),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert!(dir.join("payload.bin").is_file(), "stays put");
    assert_eq!(hist.list(10).unwrap()[0].final_dir.as_deref(), dir.to_str());
    engine.shutdown().await;
}

/// Crash between the category move and the `*PP:done` stamp. The files
/// are at the library, the global path is gone, and the next pass has to
/// pick up where the last one left off. Before this was handled, the
/// re-run drove the whole pipeline against a directory that no longer
/// existed: par2 load failed with ENOENT, `process_job` returned an
/// error, and the job was wedged forever — never stamped, never in
/// history, never announced, never retired from the queue. "The stages
/// are idempotent" is the assumption the entire crash model rests on, and
/// Move is the one stage that relocates its own input.
#[tokio::test]
async fn post_processing_resumes_after_a_crash_between_move_and_stamp() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let library = tmp.path().join("library/tv");

    // The state a crash right after the move leaves behind.
    std::fs::create_dir_all(library.join("crashjob")).unwrap();
    std::fs::write(library.join("crashjob/payload.bin"), b"already moved").unwrap();
    assert!(!tmp.path().join("dest/crashjob").exists());

    let mut job = completed_job(
        1,
        "crashjob",
        vec![file_entry(1, "payload.bin", None, false)],
    );
    job.category = Some("tv".into());
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .expect("the re-run must complete, not error on the vanished source");
    assert_eq!(out, PpFinal::Success);

    assert_eq!(
        std::fs::read(library.join("crashjob/payload.bin")).unwrap(),
        b"already moved",
        "the moved files must survive the re-run untouched"
    );
    assert_eq!(
        hist.list(10).unwrap()[0].final_dir.as_deref(),
        library.join("crashjob").to_str(),
        "and history must name where they are"
    );
    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert!(
        job.params.iter().any(|(k, _)| k == PP_DONE_PARAM),
        "the job must end up stamped rather than looping forever"
    );
    engine.shutdown().await;
}

/// An interrupted cross-filesystem move must leave the library either
/// untouched or complete — never a half-copied folder that a consumer
/// would happily import from and that every later `rename` would then
/// fail against with ENOTEMPTY.
#[tokio::test]
async fn an_interrupted_move_leaves_no_half_copied_folder_behind() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/atomic");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"content").unwrap();

    // Simulate the scratch dir a killed copy leaves behind. The next
    // attempt must clear it rather than trip over it.
    let library = tmp.path().join("library/tv");
    std::fs::create_dir_all(library.join("atomic.pp-move.local")).unwrap();
    std::fs::write(library.join("atomic.pp-move.local/partial.bin"), b"half").unwrap();

    let mut job = completed_job(1, "atomic", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("tv".into());
    engine.import_job(job, false, false).await.unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert_eq!(
        std::fs::read(library.join("atomic/payload.bin")).unwrap(),
        b"content"
    );
    assert!(
        !library.join("atomic.pp-move.local").exists(),
        "a dead attempt's scratch dir must not accumulate in the library"
    );
    engine.shutdown().await;
}

/// A multi-volume archive must extract to the WHOLE file, and a set with a
/// hole in it must fail rather than deliver part of one.
///
/// This is the end-to-end guard on the worst defect this project has had: a
/// 48 GiB remux delivered as a 500 MiB file — exactly one volume, minus its
/// header — reported as a completed download. Three things had to line up
/// for that: the signature renamer severed an old-style volume chain
/// (pinned by `rename::tests::a_numbered_volume_set_is_never_renamed`),
/// unrar's result was judged on its exit code alone with its output
/// suppressed, and nothing ever compared what was extracted against what was
/// promised.
///
/// `rar` is not free software and CI will not have it, so this self-skips
/// rather than going through `require_tool` — which would turn a missing
/// non-free package into a CI failure. The unit test above carries the
/// regression in CI; this one carries the proof that the pipeline really
/// extracts a real multi-volume set.
#[tokio::test]
async fn a_multi_volume_archive_extracts_whole_or_fails() {
    if std::process::Command::new("rar")
        .arg("-iver")
        .output()
        .is_err()
    {
        eprintln!("SKIPPED: `rar` not installed — cannot build a multi-volume set");
        return;
    }
    if !require_tool("unrar") {
        return;
    }

    // A payload several volumes long, and incompressible so that -m0 volumes
    // are genuinely the size we asked for.
    let payload: Vec<u8> = (0..900_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();

    for (case, drop_a_volume) in [("whole", false), ("holed", true)] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join(format!("dest/{case}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("movie.mkv"), &payload).unwrap();

        let built = std::process::Command::new("rar")
            .args(["a", "-m0", "-v200k", "-idq", "-ep", "set.rar", "movie.mkv"])
            .current_dir(&dir)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(built, "could not build the multi-volume set");
        std::fs::remove_file(dir.join("movie.mkv")).unwrap();

        let mut volumes: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "rar"))
            .collect();
        volumes.sort();
        assert!(
            volumes.len() >= 3,
            "the point of this test is more than one volume, got {volumes:?}"
        );
        if drop_a_volume {
            // Lose a middle volume: the chain now stops partway, which is
            // what a severed or incomplete set looks like to unrar.
            std::fs::remove_file(&volumes[1]).unwrap();
        }

        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .enumerate()
            .map(|(i, e)| {
                let name = e.file_name().to_string_lossy().into_owned();
                let bytes = std::fs::read(e.path()).unwrap();
                file_entry(i as u32 + 1, &name, Some(crc(&bytes)), false)
            })
            .collect();
        let job_id = if drop_a_volume { 61 } else { 60 };
        engine
            .import_job(completed_job(job_id, case, files), false, false)
            .await
            .unwrap();

        let hist = history(tmp.path());
        let cfg = PostConfig {
            deobfuscate_final: false,
            ..PostConfig::default()
        };
        let out = process_job(
            &engine,
            &cfg,
            &hist,
            &tmp.path().join("dest"),
            JobId(job_id),
        )
        .await
        .unwrap();

        if drop_a_volume {
            assert_ne!(
                out,
                PpFinal::Success,
                "a set with a missing volume produced a SUCCESS — this is the \
                 shape that shipped 500 MiB of a 48 GiB film"
            );
            let extracted = dir.join("movie.mkv");
            assert!(
                !extracted.exists() || std::fs::metadata(&extracted).unwrap().len() == 0,
                "a failed unpack must not leave a truncated film in place"
            );
        } else {
            assert_eq!(out, PpFinal::Success, "a complete set must extract");
            let got = std::fs::read(dir.join("movie.mkv")).unwrap();
            assert_eq!(
                got.len(),
                payload.len(),
                "extracted {} bytes of a {}-byte file — one volume is not the film",
                got.len(),
                payload.len()
            );
            assert_eq!(got, payload, "the extracted bytes are not the original");
        }
        engine.shutdown().await;
    }
}

/// The stage timeline reaches history — every stage the job actually ran,
/// in order, each with a duration.
///
/// The post manager has always measured this. `Stages::enter` stamps an
/// `Instant` on every transition and `close` banks the elapsed time — into
/// the process-wide `PpStageStats` histogram, and nowhere else. So "how
/// long does unpack take across all jobs" was answerable from the same
/// measurement that could not answer "how long did unpack take for THIS
/// job". This test pins the second question.
///
/// The last stage matters most: it is closed by `Stages::finish` before
/// the finalize export rather than by `Drop` after it, so the entry that
/// lands in history does not show its final stage still running.
#[tokio::test]
async fn the_stage_timeline_reaches_history() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/timed");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 8, &["payload.bin"]);

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_job(completed_job(1, "timed", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(1),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);

    let entries = hist.list(10).unwrap();
    assert_eq!(entries.len(), 1);
    let stages = &entries[0].stages;
    assert!(
        !stages.is_empty(),
        "post-processing ran, so history must say where the time went"
    );

    // Every span is closed. An open span in history means the pipeline
    // ended without the timeline being told — the exact failure that
    // leaving this to `Drop` produces.
    for s in stages {
        assert!(
            s.ms.is_some(),
            "stage {:?} is still running in a finished job's history entry",
            s.stage
        );
    }

    let names: Vec<&str> = stages.iter().map(|s| s.stage.as_str()).collect();
    assert!(
        names.contains(&"par_verify"),
        "a par set was present, so verify must appear: {names:?}"
    );
    // Only the stages that actually ran. This job needs no repair, has
    // nothing to clean and is already in its destination, and the
    // timeline reflects that rather than listing the whole pipeline with
    // zeroes — an operator reading it should see what happened, not what
    // could have.
    assert!(
        !names.contains(&"par_repair"),
        "an intact job never entered repair: {names:?}"
    );
    assert!(
        !names.contains(&"script"),
        "no scripts were configured: {names:?}"
    );
    // Spans are appended in execution order, so their starts never go
    // backwards.
    let starts: Vec<i64> = stages.iter().map(|s| s.started_at_unix).collect();
    assert!(
        starts.windows(2).all(|w| w[0] <= w[1]),
        "spans must be in pipeline order, got {starts:?}"
    );
    // None left open — including the last, which is the one
    // `Stages::finish` exists to close in time.
    assert!(stages.last().unwrap().ms.is_some());

    // And the live queue view agrees with what history recorded.
    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert_eq!(job.stages.len(), stages.len());
    engine.shutdown().await;
}
