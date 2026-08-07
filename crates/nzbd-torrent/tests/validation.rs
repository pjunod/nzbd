use nzbd_torrent::{
    TorrentAddConfig, TorrentError, TorrentProxyConfig, TorrentSession, TorrentSessionConfig,
};

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn single_file_info(name: &[u8]) -> Vec<u8> {
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
    info
}

fn metainfo(info: &[u8], announce: Option<&str>) -> Vec<u8> {
    let mut torrent = vec![b'd'];
    if let Some(announce) = announce {
        bencode_bytes(&mut torrent, b"announce");
        bencode_bytes(&mut torrent, announce.as_bytes());
    }
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(info);
    torrent.push(b'e');
    torrent
}

fn single_file_metainfo(name: &[u8]) -> Vec<u8> {
    metainfo(&single_file_info(name), None)
}

fn v2_metainfo(hybrid: bool) -> Vec<u8> {
    let mut info = b"d9:file treede12:meta versioni2e4:name2:v212:piece lengthi16384e".to_vec();
    if hybrid {
        bencode_bytes(&mut info, b"pieces");
        bencode_bytes(&mut info, &[0; 20]);
    }
    info.push(b'e');
    metainfo(&info, None)
}

fn proxy() -> TorrentProxyConfig {
    TorrentProxyConfig {
        url: "socks5://127.0.0.1:1".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn rejects_v2_and_hybrid_inputs_by_name() {
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
    assert!(matches!(
        session
            .add_metainfo(v2_metainfo(false), TorrentAddConfig::default())
            .await,
        Err(TorrentError::UnsupportedV2Metainfo)
    ));
    assert!(matches!(
        session
            .add_metainfo(v2_metainfo(true), TorrentAddConfig::default())
            .await,
        Err(TorrentError::UnsupportedHybridMetainfo)
    ));
    let framed_marker = session
        .add_metainfo(
            single_file_metainfo(b"meta version"),
            TorrentAddConfig {
                paused: true,
                ..Default::default()
            },
        )
        .await
        .expect("a v2 key inside a length-framed filename is still v1");
    session.delete(&framed_marker, false).await.unwrap();
    session.stop().await;
}

#[tokio::test]
async fn proxy_rejects_dht_and_udp_tracker_leak_paths() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        TorrentSession::start(
            root.path().to_path_buf(),
            TorrentSessionConfig {
                dht: true,
                proxy: Some(proxy()),
                ..Default::default()
            },
        )
        .await,
        Err(TorrentError::ProxyWithDht)
    ));

    let session = TorrentSession::start(
        root.path().to_path_buf(),
        TorrentSessionConfig {
            proxy: Some(proxy()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let udp_metainfo = metainfo(
        &single_file_info(b"udp-tracker"),
        Some("udp://127.0.0.1:1/announce"),
    );
    assert!(matches!(
        session
            .add_metainfo(udp_metainfo, TorrentAddConfig::default())
            .await,
        Err(TorrentError::ProxyWithUdpTracker)
    ));
    let udp_magnet = format!(
        "magnet:?xt=urn:btih:{}&tr=udp%3A%2F%2F127.0.0.1%3A1%2Fannounce",
        "00".repeat(20)
    );
    assert!(matches!(
        session
            .add_magnet(udp_magnet, TorrentAddConfig::default())
            .await,
        Err(TorrentError::ProxyWithUdpTracker)
    ));
    session.stop().await;
}

#[tokio::test]
async fn unsafe_names_are_rejected_before_any_escape_file_exists() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("payload");
    std::fs::create_dir(&root).unwrap();
    let session = TorrentSession::start(root.clone(), TorrentSessionConfig::default())
        .await
        .unwrap();
    for name in [
        &b"../escape"[..],
        &b"sub/../../escape"[..],
        &b"sub\\..\\..\\escape"[..],
        &b"/absolute"[..],
        &b"\\\\server\\share"[..],
    ] {
        let result = session
            .add_metainfo(
                single_file_metainfo(name),
                TorrentAddConfig {
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await;
        let error = match result {
            Ok(_) => panic!("unsafe metainfo name was accepted: {name:?}"),
            Err(error) => error,
        };
        assert!(
            matches!(error, TorrentError::UnsafeMetainfoPath(_)),
            "unexpected error for {name:?}: {error}"
        );
    }
    assert!(!parent.path().join("escape").exists());
    session.stop().await;
}
