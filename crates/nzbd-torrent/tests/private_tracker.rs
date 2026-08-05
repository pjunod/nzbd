use nzbd_torrent::{TorrentAddConfig, TorrentError, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILE_NAME: &str = "private-payload.bin";

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn private_metainfo(payload: &[u8], trackers: &[String]) -> Vec<u8> {
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, FILE_NAME.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    bencode_bytes(&mut info, b"private");
    info.extend_from_slice(b"i1e");
    info.push(b'e');

    let mut torrent = Vec::new();
    torrent.push(b'd');
    bencode_bytes(&mut torrent, b"announce");
    bencode_bytes(&mut torrent, trackers[0].as_bytes());
    if trackers.len() > 1 {
        bencode_bytes(&mut torrent, b"announce-list");
        torrent.push(b'l');
        for tracker in trackers {
            torrent.push(b'l');
            bencode_bytes(&mut torrent, tracker.as_bytes());
            torrent.push(b'e');
        }
        torrent.push(b'e');
    }
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
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

async fn tracker_server(
    listener: tokio::net::TcpListener,
    seeder: SocketAddrV4,
    requests: Arc<AtomicUsize>,
) {
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let requests = requests.clone();
        tokio::spawn(async move {
            let mut request = Vec::new();
            loop {
                let byte = stream.read_u8().await.unwrap();
                request.push(byte);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() < 16 * 1024);
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with("GET /announce/m0-passkey?"));
            requests.fetch_add(1, Ordering::Relaxed);

            let target = request.split_whitespace().nth(1).unwrap();
            let announce = url::Url::parse(&format!("http://tracker{target}")).unwrap();
            let announcing_port = announce
                .query_pairs()
                .find(|(key, _)| key == "port")
                .and_then(|(_, value)| value.parse::<u16>().ok())
                .unwrap();

            let mut body = b"d8:completei1e10:incompletei0e8:intervali60e5:peers".to_vec();
            if announcing_port == seeder.port() {
                // Real trackers do not hand a seeder its own endpoint back.
                // Returning it here creates a self-connection race that can
                // poison the synthetic swarm before the downloader announces.
                body.extend_from_slice(b"0:");
            } else {
                body.extend_from_slice(b"6:");
                body.extend_from_slice(&seeder.ip().octets());
                body.extend_from_slice(&seeder.port().to_be_bytes());
            }
            body.push(b'e');
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tracker_private_torrent_downloads_and_multiple_trackers_are_rejected() {
    let payload = (0..64 * 1024)
        .map(|index| ((index * 19 + 5) % 251) as u8)
        .collect::<Vec<_>>();
    let seeder_ports = free_port_range();
    let seeder_port = seeder_ports.start;
    let seeder_address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, seeder_port);

    let tracker_listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let tracker_address = tracker_listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let tracker_task = tokio::spawn(tracker_server(
        tracker_listener,
        seeder_address,
        requests.clone(),
    ));
    let tracker_url = format!("http://{tracker_address}/announce/m0-passkey");
    let torrent = private_metainfo(&payload, std::slice::from_ref(&tracker_url));

    let seed_root = tempfile::tempdir().unwrap();
    std::fs::write(seed_root.path().join(FILE_NAME), &payload).unwrap();
    let seeder = TorrentSession::start(
        seed_root.path().to_path_buf(),
        TorrentSessionConfig {
            listen_port_range: Some(seeder_ports),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(seeder.tcp_listen_port(), Some(seeder_port));
    let seed = seeder
        .add_metainfo(
            torrent.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    seed.wait_until_completed().await.unwrap();

    let download_root = tempfile::tempdir().unwrap();
    let downloader = TorrentSession::start(
        download_root.path().to_path_buf(),
        TorrentSessionConfig::default(),
    )
    .await
    .unwrap();
    let download = downloader
        .add_metainfo(
            torrent,
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let completed =
        tokio::time::timeout(Duration::from_secs(20), download.wait_until_completed()).await;
    if completed.is_err() {
        panic!(
            "private tracker transfer timed out: requests={}, tracker_finished={}, seeder={:?}, downloader={:?}",
            requests.load(Ordering::Relaxed),
            tracker_task.is_finished(),
            seed.stats(),
            download.stats(),
        );
    }
    completed.unwrap().unwrap();
    assert_eq!(
        std::fs::read(download_root.path().join(FILE_NAME)).unwrap(),
        payload
    );
    assert!(requests.load(Ordering::Relaxed) > 0);

    let rejected = private_metainfo(
        &payload,
        &[tracker_url, "http://127.0.0.1:1/announce/second".into()],
    );
    assert!(matches!(
        downloader
            .add_metainfo(rejected, TorrentAddConfig::default())
            .await,
        Err(TorrentError::PrivateTrackerCount(2))
    ));

    downloader.stop().await;
    seeder.stop().await;
    tracker_task.abort();
}
