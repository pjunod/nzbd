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
        category: Some("test".into()),
        priority: 0,
        dupe: DupeInfo::default(),
        params: vec![("mykey".into(), "myval".into())],
        files,
        totals: JobTotals::default(),
        status: JobStatus::Completed,
    }
}

fn history(dir: &Path) -> Arc<HistoryDb> {
    Arc::new(HistoryDb::open(&dir.join("history.sqlite"), Some(dir)).unwrap())
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
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(4),
    )
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
