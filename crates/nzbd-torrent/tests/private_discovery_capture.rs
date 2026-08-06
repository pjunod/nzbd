use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

const CAPTURE_ENV: &str = "NZBD_PRIVATE_DISCOVERY_CAPTURE";
const DHT_PROBE_PORT_ENV: &str = "NZBD_DHT_PROBE_PORT";
const DEFAULT_DHT_PROBE_PORT: u16 = 45_123;

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn metainfo(payload: &[u8], name: &str, private: bool) -> (Vec<u8>, [u8; 20]) {
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    if private {
        bencode_bytes(&mut info, b"private");
        info.extend_from_slice(b"i1e");
    }
    info.push(b'e');

    let info_hash = Sha1::digest(&info).into();
    let mut torrent = Vec::new();
    torrent.push(b'd');
    bencode_bytes(&mut torrent, b"announce");
    bencode_bytes(&mut torrent, b"http://127.0.0.1:9/announce");
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    (torrent, info_hash)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}

fn transaction_id(packet: &[u8]) -> Option<[u8; 2]> {
    const PREFIX: &[u8] = b"1:t2:";
    packet
        .windows(PREFIX.len() + 2)
        .rev()
        .find(|window| window.starts_with(PREFIX))
        .map(|window| [window[PREFIX.len()], window[PREFIX.len() + 1]])
}

fn dht_response(transaction_id: [u8; 2]) -> Vec<u8> {
    // Return one deterministic TEST-NET node. The CI harness redirects that
    // address back to this probe, so no DHT packet can reach the public net.
    let mut response = b"d1:rd2:id20:".to_vec();
    response.extend_from_slice(&[0x42; 20]);
    response.extend_from_slice(b"5:nodes26:");
    response.extend_from_slice(&[0x43; 20]);
    response.extend_from_slice(&[198, 51, 100, 77]);
    response.extend_from_slice(&6881_u16.to_be_bytes());
    response.extend_from_slice(b"e1:t2:");
    response.extend_from_slice(&transaction_id);
    response.extend_from_slice(b"1:y1:re");
    response
}

async fn dht_probe(
    socket: UdpSocket,
    public_hash: [u8; 20],
    private_hash: [u8; 20],
    public_seen: Arc<AtomicBool>,
    private_seen: Arc<AtomicBool>,
) {
    let mut packet = [0_u8; 2048];
    loop {
        let (length, source) = socket.recv_from(&mut packet).await.unwrap();
        let packet = &packet[..length];
        if contains(packet, &public_hash) {
            public_seen.store(true, Ordering::SeqCst);
        }
        if contains(packet, &private_hash) {
            private_seen.store(true, Ordering::SeqCst);
        }
        if let Some(transaction_id) = transaction_id(packet) {
            socket
                .send_to(&dht_response(transaction_id), source)
                .await
                .unwrap();
        }
    }
}

async fn wait_until(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the Linux packet-capture and DHT-redirect harness"]
async fn private_torrent_never_queries_dht_when_the_session_dht_is_live() {
    assert_eq!(
        std::env::var(CAPTURE_ENV).as_deref(),
        Ok("1"),
        "run through scripts/check-private-discovery-leaks.sh"
    );
    let probe_port = std::env::var(DHT_PROBE_PORT_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_DHT_PROBE_PORT);

    let (public_metainfo, public_hash) =
        metainfo(b"public DHT capture control", "public-control.bin", false);
    let (private_metainfo, private_hash) =
        metainfo(b"private DHT leak canary", "private-canary.bin", true);
    println!("NZBD_PUBLIC_INFO_HASH={}", hex(&public_hash));
    println!("NZBD_PRIVATE_INFO_HASH={}", hex(&private_hash));

    let public_seen = Arc::new(AtomicBool::new(false));
    let private_seen = Arc::new(AtomicBool::new(false));
    let probe = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, probe_port))
        .await
        .unwrap();
    let probe_task = tokio::spawn(dht_probe(
        probe,
        public_hash,
        private_hash,
        public_seen.clone(),
        private_seen.clone(),
    ));

    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(
        root.path().to_path_buf(),
        TorrentSessionConfig {
            dht: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _public = session
        .add_metainfo(public_metainfo, TorrentAddConfig::default())
        .await
        .unwrap();
    assert!(
        wait_until(
            || public_seen.load(Ordering::SeqCst),
            Duration::from_secs(15)
        )
        .await,
        "the public control never reached the DHT probe; the capture cannot prove suppression"
    );

    let _private = session
        .add_metainfo(private_metainfo, TorrentAddConfig::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(public_seen.load(Ordering::SeqCst));
    assert!(
        !private_seen.load(Ordering::SeqCst),
        "private torrent info hash escaped through DHT"
    );

    session.stop().await;
    probe_task.abort();
}

#[test]
fn dht_response_preserves_binary_transaction_id() {
    let response = dht_response([0, 0xff]);
    assert!(contains(&response, b"1:t2:\0\xff1:y1:r"));
}

#[test]
fn transaction_id_is_read_from_the_top_level_suffix() {
    let packet = b"d1:ad2:id20:1:t2:not-a-real-id!e1:q4:ping1:t2:\0\xff1:y1:qe";
    assert_eq!(transaction_id(packet), Some([0, 0xff]));
}
