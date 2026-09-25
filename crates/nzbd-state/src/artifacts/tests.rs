use super::*;

fn fixture() -> (tempfile::TempDir, Inventory, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("processing");
    std::fs::create_dir(&root).unwrap();
    let inventory = Inventory::open(&tmp.path().join("state")).unwrap();
    (tmp, inventory, root)
}
fn parked(db: &Inventory, root: &Path) -> Artifact {
    let path = root.join("job");
    db.allocate(1, root, &path).unwrap();
    std::fs::write(path.join("episode.mkv"), b"original media").unwrap();
    db.finish(1, &path, root, "parked_failed").unwrap()
}

#[test]
fn failed_inventory_lock_prevents_a_second_writer_and_offline_restore() {
    let (tmp, db, _) = fixture();
    assert!(Inventory::open(&tmp.path().join("state")).is_err());
    drop(db);
    assert!(Inventory::open(&tmp.path().join("state")).is_ok());
}
#[test]
fn history_is_not_required_for_ownership_or_removal() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "delete-one", 0)
        .unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(!a.path.exists());
    assert_eq!(db.get(&a.id).unwrap().state, "deleted");
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
}
#[test]
fn unknown_folders_cannot_be_deleted_and_adoption_starts_with_keep() {
    let (_tmp, db, root) = fixture();
    let path = root.join("old");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("media.mkv"), b"untouched").unwrap();
    let a = db.discover(&root, &path, false).unwrap();
    assert!(db.request_delete(&a.id, a.revision, "unknown", 0).is_err());
    assert!(
        db.adopt(&a.id, a.revision).is_err(),
        "inspection must precede adoption"
    );
    let a = db.inspect(&a.id).unwrap();
    let a = db.adopt(&a.id, a.revision).unwrap();
    assert!(a.keep && a.owned);
    assert!(db.request_delete(&a.id, a.revision, "kept", 0).is_err());
}
#[test]
fn keep_cancels_an_undo_window_without_accelerating_it() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db.request_delete(&a.id, a.revision, "ui", 8).unwrap();
    let same = db.request_delete(&a.id, a.revision, "legacy", 0).unwrap();
    assert_eq!(same.id, op.id);
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "queued");
    db.retention(&a.id, a.revision, true, None).unwrap();
    assert_eq!(db.operation(&op.id).unwrap().state, "cancelled");
    assert!(a.path.exists());
}
#[test]
fn idempotency_keys_cannot_be_reused_with_different_intent() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "request", 8).unwrap();
    assert!(db.request_delete(&a.id, a.revision, "request", 0).is_err());
}
#[test]
fn new_files_and_replaced_directories_refuse_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    std::fs::write(a.path.join("unowned.mkv"), b"neighbour").unwrap();
    let op = db.request_delete(&a.id, a.revision, "new-file", 0).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert_eq!(
        std::fs::read(a.path.join("episode.mkv")).unwrap(),
        b"original media"
    );
    assert!(a.path.join("unowned.mkv").exists());
}
#[test]
fn unavailable_root_is_not_successful_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "lost-volume", 0)
        .unwrap();
    std::fs::rename(&root, root.with_extension("offline")).unwrap();
    let result = db.execute_delete(&op.id).unwrap();
    assert_eq!(result.state, "retry");
    assert_ne!(db.get(&a.id).unwrap().state, "deleted");
}
#[test]
fn directory_substitution_cannot_grant_ownership() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "replacement", 0)
        .unwrap();
    std::fs::rename(&a.path, root.join("original")).unwrap();
    std::fs::create_dir(&a.path).unwrap();
    std::fs::write(a.path.join("other.mkv"), b"other owner").unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert!(a.path.join("other.mkv").exists());
}
#[test]
fn symlink_payload_entry_cannot_escape_the_owned_directory() {
    use std::os::unix::fs::symlink;
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let outside = tmp.path().join("library");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("movie.mkv"), b"library").unwrap();
    symlink(&outside, a.path.join("escape")).unwrap();
    let op = db.request_delete(&a.id, a.revision, "link", 0).unwrap();
    assert_ne!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(outside.join("movie.mkv").exists());
}
#[test]
fn wall_clock_alone_never_expires_a_payload_and_keep_resets_elapsed_time() {
    let (_tmp, db, root) = fixture();
    let mut a = parked(&db, &root);
    db.set_settings(&Settings {
        enabled: true,
        ..Settings::default()
    })
    .unwrap();
    a.deadline = Some(1);
    a.retention_seconds = 100;
    a.eligible_seconds = 0;
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    db.tick().unwrap();
    assert!(a.path.exists());
    db.clocks.lock().unwrap().insert(
        a.id.clone(),
        (a.revision, Instant::now() - Duration::from_secs(50)),
    );
    db.tick().unwrap();
    let a = db.get(&a.id).unwrap();
    assert_eq!(a.eligible_seconds, 50);
    let kept = db.retention(&a.id, a.revision, true, None).unwrap();
    let released = db.retention(&a.id, kept.revision, false, None).unwrap();
    db.clocks.lock().unwrap().insert(
        a.id.clone(),
        (a.revision, Instant::now() - Duration::from_secs(500)),
    );
    db.tick().unwrap();
    assert_eq!(
        db.get(&a.id).unwrap().eligible_seconds,
        0,
        "old checkpoint cannot count held time"
    );
    assert!(released.deadline.unwrap() >= now() + 99);
}
#[test]
fn restore_quarantines_pending_deletion_and_retention() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "old-intent", 0)
        .unwrap();
    db.quarantine_restore().unwrap();
    assert_eq!(db.operation("old-intent").unwrap().state, "review");
    let restored = db.get(&a.id).unwrap();
    assert!(restored.keep);
    assert!(restored.hold.is_some());
    assert_eq!(restored.eligible_seconds, 0);
    assert!(a.path.exists());
}
#[test]
fn recovery_copy_is_independent_and_receipt_releases_only_a_complete_selection() {
    use std::os::unix::fs::MetadataExt;
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "copy-one",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    assert_eq!(r.state, "published", "{:?}", r.error);
    let copy = r.published.join("payload/episode.mkv");
    assert_ne!(
        std::fs::metadata(&copy).unwrap().ino(),
        std::fs::metadata(a.path.join("episode.mkv")).unwrap().ino()
    );
    assert!(db.get(&a.id).unwrap().hold.is_some());
    db.claim_recovery(&r.id, "consumer-a", "import-a", &r.manifest_digest)
        .unwrap();
    assert!(db
        .claim_recovery(&r.id, "consumer-b", "import-b", &r.manifest_digest)
        .is_err());
    let receipt = Receipt {
        import_id: "import-a".into(),
        manifest_digest: r.manifest_digest.clone(),
        files: r
            .files
            .iter()
            .map(|f| ReceiptFile {
                id: f.id.clone(),
                bytes: f.bytes,
                sha256: f.sha256.clone(),
                result: "imported".into(),
            })
            .collect(),
    };
    assert!(db
        .recovery_receipt(&r.id, "wrong-consumer", receipt.clone())
        .is_err());
    assert_eq!(
        db.recovery_receipt(&r.id, "consumer-a", receipt.clone())
            .unwrap()
            .state,
        "imported"
    );
    assert_eq!(
        db.recovery_receipt(&r.id, "consumer-a", receipt)
            .unwrap()
            .state,
        "imported"
    );
    assert!(db.get(&a.id).unwrap().hold.is_none());
    let staged = db.get(&format!("recovery-{}", r.id)).unwrap();
    assert_eq!(staged.state, "recovery_imported");
    assert_eq!(staged.retention_seconds, 86400);
}
#[test]
fn claimed_recovery_never_releases_on_cancel_without_worker_acknowledgement() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "cancel-copy",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    db.claim_recovery(&r.id, "curator", "job", &r.manifest_digest)
        .unwrap();
    assert_eq!(
        db.cancel_recovery(&r.id, None, false).unwrap().state,
        "cancel_pending"
    );
    assert!(db.cancel_recovery(&r.id, Some("other"), true).is_err());
    assert!(db.get(&a.id).unwrap().hold.is_some());
    assert_eq!(
        db.cancel_recovery(&r.id, Some("curator"), true)
            .unwrap()
            .state,
        "cancelled"
    );
    assert!(
        db.get(&a.id).unwrap().hold.is_some(),
        "cancelled staging requires review"
    );
}
