//! What a private tracker and a remote peer see of Runner: one consistent
//! client identity, and announces that follow the tracker protocol closely
//! enough for ratio accounting (BEP 3 events, session-relative totals, a
//! stable `key`, and the tracker's own passkey query kept intact).

use nzbd_torrent::identity::{
    peer_id_prefix, CLIENT_HANDSHAKE_VERSION, CLIENT_USER_AGENT, PEER_ID_CLIENT_CODE,
};
use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILE_NAME: &str = "identity-payload.bin";
const PASSKEY: &str = "runner-passkey";

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn info_dict(payload: &[u8]) -> Vec<u8> {
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
    info
}

fn private_metainfo(info: &[u8], tracker: &str) -> Vec<u8> {
    let mut torrent = Vec::new();
    torrent.push(b'd');
    bencode_bytes(&mut torrent, b"announce");
    bencode_bytes(&mut torrent, tracker.as_bytes());
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(info);
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

#[derive(Debug, Clone)]
struct Announce {
    params: Vec<(String, String)>,
    peer_id: Vec<u8>,
    user_agent: Option<String>,
}

impl Announce {
    fn get(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn port(&self) -> u16 {
        self.get("port").unwrap().parse().unwrap()
    }

    fn event(&self) -> Option<&str> {
        self.get("event")
    }
}

async fn tracker_server(
    listener: tokio::net::TcpListener,
    seeder: SocketAddrV4,
    announces: Arc<Mutex<Vec<Announce>>>,
) {
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let announces = announces.clone();
        tokio::spawn(async move {
            let mut request = Vec::new();
            loop {
                let Ok(byte) = stream.read_u8().await else {
                    return;
                };
                request.push(byte);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() < 16 * 1024);
            }
            let request = String::from_utf8_lossy(&request).into_owned();
            let target = request.split_whitespace().nth(1).unwrap();
            let announce = url::Url::parse(&format!("http://tracker{target}")).unwrap();
            let params = announce
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect::<Vec<_>>();
            let peer_id = announce
                .query()
                .unwrap()
                .split('&')
                .find_map(|pair| pair.strip_prefix("peer_id="))
                .map(|encoded| percent_encoding::percent_decode_str(encoded).collect::<Vec<u8>>())
                .unwrap();
            let user_agent = request.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("user-agent")
                    .then(|| value.trim().to_owned())
            });
            let recorded = Announce {
                params,
                peer_id,
                user_agent,
            };
            let announcing_port = recorded.port();
            announces.lock().unwrap().push(recorded);

            let mut body = b"d8:completei1e10:incompletei0e8:intervali1800e5:peers".to_vec();
            if announcing_port == seeder.port() {
                body.extend_from_slice(b"0:");
            } else {
                body.extend_from_slice(b"6:");
                body.extend_from_slice(&seeder.ip().octets());
                body.extend_from_slice(&seeder.port().to_be_bytes());
            }
            body.extend_from_slice(b"10:tracker id6:runnere");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(headers.as_bytes()).await;
            let _ = stream.write_all(&body).await;
        });
    }
}

async fn wait_for(
    announces: &Mutex<Vec<Announce>>,
    what: &str,
    predicate: impl Fn(&Announce) -> bool,
) -> Announce {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(found) = announces.lock().unwrap().iter().find(|a| predicate(a)) {
            return found.clone();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "tracker never saw {what}; announces: {:#?}",
                announces.lock().unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Complete a BEP 3 handshake advertising BEP 10 and return the remote peer
/// ID and the remote extended handshake's `v`.
async fn peer_identity(peer: SocketAddrV4, info_hash: [u8; 20]) -> ([u8; 20], Vec<u8>) {
    let mut stream = tokio::net::TcpStream::connect(peer).await.unwrap();
    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    handshake.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
    handshake.extend_from_slice(&info_hash);
    handshake.extend_from_slice(b"-XX0000-identityprob");
    stream.write_all(&handshake).await.unwrap();

    let mut reply = [0u8; 68];
    stream.read_exact(&mut reply).await.unwrap();
    let mut remote_peer_id = [0u8; 20];
    remote_peer_id.copy_from_slice(&reply[48..68]);
    assert_ne!(reply[25] & 0x10, 0, "remote must advertise BEP 10");

    let mut ours = b"d1:md11:ut_metadatai1eee".to_vec();
    let mut message = Vec::new();
    message.extend_from_slice(&((ours.len() + 2) as u32).to_be_bytes());
    message.extend_from_slice(&[20, 0]);
    message.append(&mut ours);
    stream.write_all(&message).await.unwrap();

    loop {
        let length = stream.read_u32().await.unwrap() as usize;
        if length == 0 {
            continue;
        }
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).await.unwrap();
        if body[0] == 20 && body[1] == 0 {
            let dict = &body[2..];
            let marker = b"1:v";
            let start = dict
                .windows(marker.len())
                .position(|window| window == marker)
                .expect("extended handshake carries v")
                + marker.len();
            let rest = &dict[start..];
            let colon = rest.iter().position(|&b| b == b':').unwrap();
            let length: usize = std::str::from_utf8(&rest[..colon])
                .unwrap()
                .parse()
                .unwrap();
            return (remote_peer_id, rest[colon + 1..colon + 1 + length].to_vec());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trackers_and_peers_see_one_runner_identity_and_a_compliant_announce_lifecycle() {
    let payload = (0..64 * 1024)
        .map(|index| ((index * 23 + 11) % 251) as u8)
        .collect::<Vec<_>>();
    let info = info_dict(&payload);
    let info_hash: [u8; 20] = Sha1::digest(&info).into();
    let seeder_ports = free_port_range();
    let seeder_address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, seeder_ports.start);

    let tracker_listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let tracker_address = tracker_listener.local_addr().unwrap();
    let announces = Arc::new(Mutex::new(Vec::new()));
    let tracker_task = tokio::spawn(tracker_server(
        tracker_listener,
        seeder_address,
        announces.clone(),
    ));
    // Many private trackers carry the passkey in the announce query.
    let tracker_url = format!("http://{tracker_address}/announce.php?passkey={PASSKEY}");
    let torrent = private_metainfo(&info, &tracker_url);

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

    // The seeder already had every byte on disk: it starts complete, so it
    // reports nothing downloaded and must never claim a completion.
    let seed_started = wait_for(&announces, "the seeder's started", |a| {
        a.port() == seeder_address.port() && a.event() == Some("started")
    })
    .await;
    assert_eq!(seed_started.get("left"), Some("0"));
    assert_eq!(seed_started.get("downloaded"), Some("0"));

    // A remote peer sees the same identity in the wire handshake.
    let (remote_peer_id, remote_version) = peer_identity(seeder_address, info_hash).await;
    assert_eq!(remote_peer_id[..8], peer_id_prefix());
    assert_eq!(remote_version, CLIENT_HANDSHAKE_VERSION.as_bytes());

    let download_root = tempfile::tempdir().unwrap();
    let downloader_ports = free_port_range();
    let downloader_port = downloader_ports.start;
    let downloader = TorrentSession::start(
        download_root.path().to_path_buf(),
        TorrentSessionConfig {
            listen_port_range: Some(downloader_ports),
            ..Default::default()
        },
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
    tokio::time::timeout(Duration::from_secs(30), download.wait_until_completed())
        .await
        .expect("private transfer timed out")
        .unwrap();

    let started = wait_for(&announces, "the downloader's started", |a| {
        a.port() == downloader_port && a.event() == Some("started")
    })
    .await;
    assert_eq!(
        started.get("left"),
        Some(payload.len().to_string().as_str())
    );
    assert_eq!(started.get("downloaded"), Some("0"));

    // Completion is announced promptly, not at the next 30-minute interval,
    // with session-relative totals and the tracker id echoed back.
    let completed = wait_for(&announces, "the downloader's completed", |a| {
        a.port() == downloader_port && a.event() == Some("completed")
    })
    .await;
    assert_eq!(completed.get("left"), Some("0"));
    assert_eq!(
        completed.get("downloaded"),
        Some(payload.len().to_string().as_str())
    );
    assert_eq!(completed.get("trackerid"), Some("runner"));
    assert_eq!(completed.get("key"), started.get("key"));

    // Pausing ends the tracker session with a stopped announce.
    downloader.pause(&download).await.unwrap();
    let stopped = wait_for(&announces, "the downloader's stopped", |a| {
        a.port() == downloader_port && a.event() == Some("stopped")
    })
    .await;
    assert_eq!(stopped.get("key"), started.get("key"));
    assert_eq!(stopped.get("trackerid"), Some("runner"));

    let recorded = announces.lock().unwrap().clone();
    for announce in &recorded {
        assert_eq!(announce.get("passkey"), Some(PASSKEY), "{announce:?}");
        assert_eq!(
            announce.user_agent.as_deref(),
            Some(CLIENT_USER_AGENT),
            "{announce:?}"
        );
        assert_eq!(announce.peer_id[..8], peer_id_prefix(), "{announce:?}");
        assert_eq!(&announce.peer_id[1..3], &PEER_ID_CLIENT_CODE);
        assert_eq!(announce.get("key").map(str::len), Some(8), "{announce:?}");
    }
    let downloader_completions = recorded
        .iter()
        .filter(|a| a.port() == downloader_port && a.event() == Some("completed"))
        .count();
    assert_eq!(downloader_completions, 1);
    assert!(!recorded
        .iter()
        .any(|a| a.port() == seeder_address.port() && a.event() == Some("completed")));

    downloader.stop().await;
    seeder.stop().await;
    tracker_task.abort();
}
