use crate::{
    create_torrent, AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions,
    SessionPersistenceConfig,
};

const PIECE_LENGTH: usize = 16 * 1024;

fn payload(seed: usize) -> Vec<u8> {
    (0..PIECE_LENGTH * 4)
        .map(|offset| ((offset * 31 + offset / 7 + seed) % 251) as u8)
        .collect()
}

async fn make_torrent_bytes(path: &std::path::Path, payload: &[u8]) -> bytes::Bytes {
    tokio::fs::write(path, payload).await.unwrap();
    create_torrent(
        path,
        CreateTorrentOptions {
            piece_length: Some(PIECE_LENGTH as u32),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap()
}

fn options(persistence: std::path::PathBuf, disable_auto_restore: bool) -> SessionOptions {
    SessionOptions {
        disable_dht: true,
        disable_dht_persistence: true,
        fastresume: true,
        persistence: Some(SessionPersistenceConfig::Json {
            folder: Some(persistence),
        }),
        disable_auto_restore,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("output");
    let persistence = root.path().join("persistence");
    let first_payload = payload(11);
    let second_payload = payload(29);
    let first_bytes = make_torrent_bytes(&root.path().join("first.bin"), &first_payload).await;
    let second_bytes = make_torrent_bytes(&root.path().join("second.bin"), &second_payload).await;

    tokio::fs::create_dir_all(&output).await.unwrap();
    let mut partial_second = second_payload.clone();
    partial_second[PIECE_LENGTH..].fill(0);
    tokio::fs::write(output.join("second.bin"), partial_second)
        .await
        .unwrap();

    let first = Session::new_with_opts(output.clone(), options(persistence.clone(), false))
        .await
        .unwrap();
    let first_id = first
        .add_torrent(
            AddTorrent::TorrentFileBytes(first_bytes.clone()),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap()
        .id();
    let second = first
        .add_torrent(
            AddTorrent::TorrentFileBytes(second_bytes.clone()),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    let second_id = second.id();
    assert_ne!(first_id, second_id);
    second.wait_until_initialized().await.unwrap();
    assert_eq!(second.stats().progress_bytes, PIECE_LENGTH as u64);
    first.stop().await;

    // Diverge on-disk data from persisted piece state. If the restore path fell
    // back to a full re-check, progress would read 64 KiB and finished would be
    // true, so the assertions below can only pass via fast-resume.
    tokio::fs::write(output.join("second.bin"), &second_payload)
        .await
        .unwrap();

    let authoritative = Session::new_with_opts(output.clone(), options(persistence.clone(), true))
        .await
        .unwrap();
    assert_eq!(
        authoritative.with_torrents(|torrents| torrents.count()),
        0,
        "constructing the session must not implicitly admit persisted torrents"
    );

    let restored = authoritative
        .add_torrent(
            AddTorrent::TorrentFileBytes(second_bytes),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                preferred_id: Some(second_id),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let restored = restored.into_handle().unwrap();
    restored.wait_until_initialized().await.unwrap();
    let restored_stats = restored.stats();
    assert_eq!(restored_stats.progress_bytes, PIECE_LENGTH as u64);
    assert!(!restored_stats.finished);
    let restored_ids =
        authoritative.with_torrents(|torrents| torrents.map(|(id, _)| id).collect::<Vec<_>>());
    assert_eq!(restored_ids, vec![second_id]);
    authoritative.stop().await;

    let legacy = Session::new_with_opts(output, options(persistence, false))
        .await
        .unwrap();
    assert_eq!(
        legacy.with_torrents(|torrents| torrents.count()),
        2,
        "the default behavior must continue to restore every persisted record"
    );
    legacy.stop().await;
}
