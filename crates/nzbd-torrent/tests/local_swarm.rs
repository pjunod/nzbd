use nzbd_torrent::{TorrentAddConfig, TorrentPhase, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::num::NonZeroU32;
use std::ops::Range;
use std::time::Duration;

const FILE_NAME: &str = "m0-payload.bin";
const PIECE_LENGTH: usize = 16 * 1024;

fn generated_payload() -> Vec<u8> {
    (0..256 * 1024)
        .map(|i| ((i * 31 + i / 7) % 251) as u8)
        .collect()
}

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn metainfo(payload: &[u8]) -> (Vec<u8>, String) {
    let mut pieces = Vec::new();
    for piece in payload.chunks(PIECE_LENGTH) {
        pieces.extend_from_slice(&Sha1::digest(piece));
    }

    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, FILE_NAME.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{PIECE_LENGTH}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &pieces);
    info.push(b'e');

    let info_hash = Sha1::digest(&info)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut torrent = Vec::new();
    torrent.extend_from_slice(b"d4:info");
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    (torrent, info_hash)
}

fn free_port_range() -> Range<u16> {
    loop {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        if port < u16::MAX {
            return port..port + 1;
        }
    }
}

async fn downloader(
    root: &std::path::Path,
    source: Source,
    peer: SocketAddr,
) -> (TorrentSession, nzbd_torrent::TorrentHandle) {
    let session = TorrentSession::start(root.to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let prove_live_rate_change = matches!(&source, Source::Metainfo(_));
    if prove_live_rate_change {
        session.set_download_limit_bps(NonZeroU32::new(16 * 1024));
    }
    let config = TorrentAddConfig {
        overwrite: true,
        initial_peers: vec![peer],
        ..Default::default()
    };
    let handle = match source {
        Source::Metainfo(bytes) => session.add_metainfo(bytes, config).await.unwrap(),
        Source::Magnet(magnet) => session.add_magnet(magnet, config).await.unwrap(),
    };
    if prove_live_rate_change {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !handle.stats().finished,
            "16 KiB/s limit should hold a 256 KiB local transfer"
        );
        session.set_download_limit_bps(None);
    }
    tokio::time::timeout(Duration::from_secs(20), handle.wait_until_completed())
        .await
        .expect("local TCP swarm timed out")
        .unwrap();
    assert!(handle.stats().finished);
    session.pause(&handle).await.unwrap();
    assert!(handle.is_paused());
    session.resume(&handle).await.unwrap();
    (session, handle)
}

enum Source {
    Metainfo(Vec<u8>),
    Magnet(String),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_metainfo_and_magnet_download_then_seed_over_local_tcp() {
    let payload = generated_payload();
    let (torrent, info_hash) = metainfo(&payload);
    let seed_root = tempfile::tempdir().unwrap();
    std::fs::write(seed_root.path().join(FILE_NAME), &payload).unwrap();

    let listen = free_port_range();
    let port = listen.start;
    let seeder = TorrentSession::start(
        seed_root.path().to_path_buf(),
        TorrentSessionConfig {
            listen_port_range: Some(listen),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(seeder.tcp_listen_port(), Some(port));
    let seed_handle = seeder
        .add_metainfo(
            torrent.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), seed_handle.wait_until_completed())
        .await
        .expect("seeder hash check timed out")
        .unwrap();

    let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let metainfo_root = tempfile::tempdir().unwrap();
    let (metainfo_downloader, metainfo_handle) = downloader(
        metainfo_root.path(),
        Source::Metainfo(torrent.clone()),
        peer,
    )
    .await;
    assert_eq!(
        std::fs::read(metainfo_root.path().join(FILE_NAME)).unwrap(),
        payload
    );
    assert_eq!(metainfo_handle.info_hash(), info_hash);
    let completed_stats = metainfo_handle.stats();
    assert_eq!(completed_stats.phase, TorrentPhase::Live);
    assert_eq!(
        completed_stats.file_progress_bytes,
        vec![payload.len() as u64]
    );
    assert_eq!(completed_stats.progress_bytes, payload.len() as u64);
    assert_eq!(completed_stats.eta_seconds, None);

    let magnet_root = tempfile::tempdir().unwrap();
    let (magnet_downloader, magnet_handle) = downloader(
        magnet_root.path(),
        Source::Magnet(format!("magnet:?xt=urn:btih:{info_hash}")),
        peer,
    )
    .await;
    assert_eq!(
        std::fs::read(magnet_root.path().join(FILE_NAME)).unwrap(),
        payload
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if seed_handle.stats().uploaded_bytes >= (payload.len() * 2) as u64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("seeder never accounted both local uploads");

    metainfo_downloader
        .delete(&metainfo_handle, false)
        .await
        .unwrap();
    metainfo_downloader
        .delete(&metainfo_handle, false)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(metainfo_root.path().join(FILE_NAME)).unwrap(),
        payload,
        "keep-data deletion must leave the payload"
    );

    std::fs::write(magnet_root.path().join("not-owned.txt"), b"keep me").unwrap();
    magnet_downloader
        .delete(&magnet_handle, true)
        .await
        .unwrap();
    magnet_downloader
        .delete(&magnet_handle, true)
        .await
        .unwrap();
    assert!(!magnet_root.path().join(FILE_NAME).exists());
    assert_eq!(
        std::fs::read(magnet_root.path().join("not-owned.txt")).unwrap(),
        b"keep me",
        "delete-data must not remove an unrelated sibling"
    );

    metainfo_downloader.stop().await;
    magnet_downloader.stop().await;
    seeder.stop().await;
}
