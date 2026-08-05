use nzbd_torrent::{TorrentAddConfig, TorrentError, TorrentSession, TorrentSessionConfig};

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn single_file_metainfo(name: &[u8]) -> Vec<u8> {
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name);
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[0; 20]);
    info.push(b'e');

    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:info");
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
}

#[tokio::test]
async fn rejects_v2_and_hybrid_magnets_by_name() {
    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let v2 = format!("magnet:?xt=urn:btmh:1220{}", "00".repeat(32));
    assert!(matches!(
        session.add_magnet(v2, TorrentAddConfig::default()).await,
        Err(TorrentError::UnsupportedV2Magnet)
    ));
    let hybrid = format!(
        "magnet:?xt=urn:btih:{}&xt=urn:btmh:1220{}",
        "00".repeat(20),
        "00".repeat(32)
    );
    assert!(matches!(
        session
            .add_magnet(hybrid, TorrentAddConfig::default())
            .await,
        Err(TorrentError::UnsupportedHybridMagnet)
    ));
    session.stop().await;
}

#[tokio::test]
async fn traversal_name_is_rejected_before_any_escape_file_exists() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("payload");
    std::fs::create_dir(&root).unwrap();
    let session = TorrentSession::start(root.clone(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let result = session
        .add_metainfo(
            single_file_metainfo(b"../escape"),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await;
    let error = match result {
        Ok(_) => panic!("traversal metainfo was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("path traversal") || error.contains("separator"),
        "unexpected error: {error}"
    );
    assert!(!parent.path().join("escape").exists());
    session.stop().await;
}
