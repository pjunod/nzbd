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

#[test]
fn inspection_of_added_bytes_revokes_automatic_ownership() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    std::fs::write(a.path.join("another.mkv"), b"unrelated").unwrap();
    let observed = db.inspect(&a.id).unwrap();
    assert!(!observed.owned && observed.keep);
    assert!(db
        .request_delete(&a.id, observed.revision, "after-inspect", 0)
        .is_err());
}
#[test]
fn a_missing_sidecar_never_becomes_success_on_retry() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "marker-lost", 0)
        .unwrap();
    std::fs::remove_file(
        db.state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation)),
    )
    .unwrap();
    let mut result = db.execute_delete(&op.id).unwrap();
    assert_eq!(result.state, "retry");
    result.next_retry = 0;
    save_operation(&db.db.lock().unwrap(), &result).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "retry");
    assert!(a.path.exists());
}
#[test]
fn retention_preview_is_atomic_and_never_shortens_elapsed_eligibility() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let p = db.preview_retention(1).unwrap();
    assert_eq!(p.entries.len(), 1);
    let kept = db.retention(&a.id, a.revision, true, None).unwrap();
    assert!(db.apply_retention(&p.id).is_err());
    assert_eq!(db.get(&a.id).unwrap().retention_seconds, 7 * 86400);
    db.retention(&a.id, kept.revision, false, None).unwrap();
    let p = db.preview_retention(1).unwrap();
    db.apply_retention(&p.id).unwrap();
    let a = db.get(&a.id).unwrap();
    assert_eq!(a.retention_seconds, 86400);
    assert_eq!(a.eligible_seconds, 0);
    assert!(a.deadline.unwrap() >= now() + 86399);
}
#[test]
fn relocation_refuses_existing_destination_and_commits_identity_after_rename() {
    let (tmp, db, root) = fixture();
    let source = root.join("live");
    db.allocate(9, &root, &source).unwrap();
    std::fs::write(source.join("media.mkv"), b"whole media").unwrap();
    let target_root = tmp.path().join("failed");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("live");
    std::fs::create_dir(&target).unwrap();
    assert!(db.relocate(9, &target).is_err());
    assert!(source.exists());
    std::fs::remove_dir(&target).unwrap();
    db.relocate(9, &target).unwrap();
    assert!(!source.exists());
    assert_eq!(
        std::fs::read(target.join("media.mkv")).unwrap(),
        b"whole media"
    );
    let a = db
        .finish(9, &target, &target_root, "parked_failed")
        .unwrap();
    assert_eq!(a.path, target);
    assert!(a.owned);
}
#[test]
fn published_recovery_reconciles_after_lost_acknowledgement() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let mut r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "restart-stage",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    r.state = "publishing".into();
    recovery::save(&db.db.lock().unwrap(), &r).unwrap();
    db.reconcile_recoveries().unwrap();
    assert_eq!(db.recovery(&r.id).unwrap().state, "published");
    assert!(db.get(&a.id).unwrap().hold.is_some());
}
#[test]
fn protected_role_change_refuses_an_already_queued_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "root-role", 0)
        .unwrap();
    db.protect_roots(&[a.path.clone()]).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert!(a.path.exists());
}
#[test]
fn offline_backup_restores_only_with_explicit_quarantine() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "pre-backup", 0)
        .unwrap();
    let backup = tmp.path().join("backup");
    db.backup(&backup).unwrap();
    assert!(backup.join("backup.json").exists());
    let restored = Inventory::open(&backup).unwrap();
    restored.quarantine_restore().unwrap();
    assert_eq!(restored.operation("pre-backup").unwrap().state, "review");
    assert!(restored.get(&a.id).unwrap().keep);
}

#[test]
fn receipt_cleanup_leaves_unselected_source_bytes_held() {
    let (tmp, db, root) = fixture();
    let path = root.join("job");
    db.allocate(1, &root, &path).unwrap();
    std::fs::write(path.join("selected.mkv"), b"selected").unwrap();
    std::fs::write(path.join("other.mkv"), b"other").unwrap();
    let a = db.finish(1, &path, &root, "parked_failed").unwrap();
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "selection",
            &["selected.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    db.claim_recovery(&r.id, "curator", "import", &r.manifest_digest)
        .unwrap();
    db.recovery_receipt(
        &r.id,
        "curator",
        Receipt {
            import_id: "import".into(),
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
        },
    )
    .unwrap();
    db.prune_receipted_source(&r.id).unwrap();
    assert!(!path.join("selected.mkv").exists());
    assert_eq!(std::fs::read(path.join("other.mkv")).unwrap(), b"other");
    assert!(db.get(&a.id).unwrap().hold.is_some());
    db.prune_receipted_source(&r.id).unwrap();
    assert!(path.join("other.mkv").exists());
}

#[test]
#[ignore = "release performance fixture; run once during final verification"]
fn lifecycle_scale_one_million_tombstones() {
    let (tmp, db, root) = fixture();
    let sample = parked(&db, &root);
    let started = Instant::now();
    {
        let mut connection = db.db.lock().unwrap();
        let tx = connection.transaction().unwrap();
        for i in 0..1_000_000 {
            let mut a = sample.clone();
            a.id = format!("benchmark-{i:08}");
            a.job = None;
            a.path = root.join(&a.id);
            a.state = "source_gone".into();
            a.files.clear();
            a.updated_at = 1;
            save_artifact(&tx, &a).unwrap();
        }
        tx.commit().unwrap();
    }
    let mut times = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        assert_eq!(db.list(0, 100).unwrap().len(), 100);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(f64::total_cmp);
    let bytes = std::fs::metadata(tmp.path().join("state/artifacts.sqlite"))
        .unwrap()
        .len();
    eprintln!("lifecycle rows=1000001 list_p95_ms={:.3} database_bytes={} preparation_seconds={:.1} os={} arch={}",times[94],bytes,started.elapsed().as_secs_f64(),std::env::consts::OS,std::env::consts::ARCH);
    assert!(
        times[94] < 200.0,
        "cached first-page latency exceeded design target"
    );
}
