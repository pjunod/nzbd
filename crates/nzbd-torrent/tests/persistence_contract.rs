use librqbit::{AddTorrent, AddTorrentOptions, Session, SessionOptions, SessionPersistenceConfig};
use nzbd_torrent::install_process_crypto_provider;
use sha1::{Digest, Sha1};

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn one_piece_metainfo() -> Vec<u8> {
    let payload = b"persistence-contract";
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"persistence-contract.bin");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');

    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
}

fn options(folder: std::path::PathBuf) -> SessionOptions {
    SessionOptions {
        disable_dht: true,
        disable_dht_persistence: true,
        fastresume: true,
        persistence: Some(SessionPersistenceConfig::Json {
            folder: Some(folder),
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enabling_fastresume_also_restores_every_library_record_before_nzbd_can_reconcile() {
    install_process_crypto_provider().unwrap();
    let output = tempfile::tempdir().unwrap();
    let persistence = tempfile::tempdir().unwrap();

    let first = Session::new_with_opts(
        output.path().to_path_buf(),
        options(persistence.path().to_path_buf()),
    )
    .await
    .unwrap();
    first
        .add_torrent(
            AddTorrent::from_bytes(one_piece_metainfo()),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(first.with_torrents(|torrents| torrents.count()), 1);
    first.stop().await;
    assert!(persistence.path().join("session.json").is_file());

    // No nzbd job has been re-added. Construction itself restores the record,
    // proving that librqbit 8.1.1 cannot use fastresume while nzbd remains the
    // sole authority over which torrents are admitted after restart.
    let second = Session::new_with_opts(
        output.path().to_path_buf(),
        options(persistence.path().to_path_buf()),
    )
    .await
    .unwrap();
    assert_eq!(second.with_torrents(|torrents| torrents.count()), 1);
    second.stop().await;
}
