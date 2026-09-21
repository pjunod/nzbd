use nzbd_torrent::{TorrentAddConfig, TorrentError, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn unsafe_info() -> (Vec<u8>, [u8; 20]) {
    let payload = b"x";
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"C:escape.bin");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');
    let info_hash = Sha1::digest(&info).into();
    (info, info_hash)
}

fn private_info() -> (Vec<u8>, [u8; 20]) {
    let payload = b"x";
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"private.bin");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"private");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');
    let info_hash = Sha1::digest(&info).into();
    (info, info_hash)
}

fn structurally_invalid_info() -> (Vec<u8>, [u8; 20]) {
    let payload = b"x";
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"files");
    info.extend_from_slice(b"ld6:lengthi1e4:pathl8:file.bineee");
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"invalid");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');
    let info_hash = Sha1::digest(&info).into();
    (info, info_hash)
}

fn compact_ipv4(address: SocketAddr) -> [u8; 6] {
    let SocketAddr::V4(address) = address else {
        panic!("tracker fixture requires IPv4");
    };
    let mut compact = [0_u8; 6];
    compact[..4].copy_from_slice(&address.ip().octets());
    compact[4..].copy_from_slice(&address.port().to_be_bytes());
    compact
}

async fn http_tracker(listener: TcpListener, peer: SocketAddr, requests: Arc<AtomicUsize>) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = [0_u8; 4096];
    let length = stream.read(&mut request).await.unwrap();
    assert!(request[..length]
        .windows(b"GET /announce?".len())
        .any(|window| window == b"GET /announce?"));
    requests.fetch_add(1, Ordering::SeqCst);
    let mut body = b"d8:completei1e10:incompletei0e8:intervali60e5:peers6:".to_vec();
    body.extend_from_slice(&compact_ipv4(peer));
    body.push(b'e');
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream.write_all(&body).await.unwrap();
}

fn handshake(info_hash: [u8; 20]) -> Vec<u8> {
    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0_u8; 8];
    reserved[5] = 0x10;
    handshake.extend_from_slice(&reserved);
    handshake.extend_from_slice(&info_hash);
    handshake.extend_from_slice(b"-NZ0001-METAPREFLT12");
    assert_eq!(handshake.len(), 68);
    handshake
}

fn extended_message(extension_id: u8, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len() + 2).unwrap();
    let mut message = Vec::with_capacity(payload.len() + 6);
    message.extend_from_slice(&length.to_be_bytes());
    message.push(20);
    message.push(extension_id);
    message.extend_from_slice(payload);
    message
}

async fn read_message(stream: &mut TcpStream) -> Vec<u8> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await.unwrap();
    let length = u32::from_be_bytes(length) as usize;
    let mut message = vec![0_u8; length];
    stream.read_exact(&mut message).await.unwrap();
    message
}

fn advertised_metadata_id(payload: &[u8]) -> Option<u8> {
    let marker = b"11:ut_metadatai";
    let start = payload
        .windows(marker.len())
        .position(|window| window == marker)?
        + marker.len();
    let end = payload[start..].iter().position(|byte| *byte == b'e')? + start;
    std::str::from_utf8(&payload[start..end]).ok()?.parse().ok()
}

async fn metadata_peer(listener: TcpListener, info: Vec<u8>, info_hash: [u8; 20]) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut incoming_handshake = [0_u8; 68];
    stream.read_exact(&mut incoming_handshake).await.unwrap();
    assert_eq!(&incoming_handshake[28..48], &info_hash);
    stream.write_all(&handshake(info_hash)).await.unwrap();

    let extended_handshake = format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", info.len());
    stream
        .write_all(&extended_message(0, extended_handshake.as_bytes()))
        .await
        .unwrap();

    let response_extension_id = tokio::time::timeout(Duration::from_secs(5), async {
        let mut response_extension_id = None;
        loop {
            let request = read_message(&mut stream).await;
            if request.starts_with(&[20, 0]) {
                response_extension_id = advertised_metadata_id(&request[2..]);
            }
            if request.starts_with(&[20, 1]) {
                break response_extension_id
                    .expect("client did not advertise a ut_metadata message id");
            }
        }
    })
    .await
    .expect("metadata request was not received");

    let mut response =
        format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
    response.extend_from_slice(&info);
    stream
        // The request uses the id we advertised; the response uses the id the
        // client advertised for its own local extension table.
        .write_all(&extended_message(response_extension_id, &response))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magnet_metadata_is_preflighted_before_payload_storage_exists() {
    let (info, info_hash) = unsafe_info();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = listener.local_addr().unwrap();
    let peer_task = tokio::spawn(metadata_peer(listener, info, info_hash));

    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.add_magnet(
            magnet,
            TorrentAddConfig {
                initial_peers: vec![peer],
                ..Default::default()
            },
        ),
    )
    .await
    .expect("magnet metadata resolution timed out");

    tokio::time::timeout(Duration::from_secs(5), peer_task)
        .await
        .expect("metadata peer did not finish")
        .expect("metadata peer failed");

    let error = match result {
        Ok(_) => panic!("unsafe magnet metadata was admitted"),
        Err(error) => error,
    };
    assert!(
        matches!(error, TorrentError::UnsafeMetainfoPath(_)),
        "unexpected rejection: {error}"
    );
    assert_eq!(
        std::fs::read_dir(root.path()).unwrap().count(),
        0,
        "list-only resolution must reject unsafe metadata before storage is constructed"
    );

    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_enabled_session_rejects_private_metadata_after_resolution() {
    let (info, info_hash) = private_info();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = listener.local_addr().unwrap();
    let peer_task = tokio::spawn(metadata_peer(listener, info, info_hash));

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
    let magnet = format!(
        "magnet:?xt=urn:btih:{}",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let result = session
        .add_magnet(
            magnet,
            TorrentAddConfig {
                initial_peers: vec![peer],
                ..Default::default()
            },
        )
        .await;

    tokio::time::timeout(Duration::from_secs(5), peer_task)
        .await
        .expect("metadata peer did not finish")
        .expect("metadata peer failed");
    match result {
        Err(TorrentError::PrivateMetainfoWithDht) => {}
        Err(other) => panic!(
            "private metadata returned the wrong rejection after the permitted unknown-hash lookup: {other:?}"
        ),
        Ok(_) => panic!("private metadata was admitted after the unknown-hash lookup"),
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);

    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trackerless_magnet_without_dht_or_initial_peers_is_actionable() {
    let (_info, info_hash) = unsafe_info();
    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}&dn=trackerless",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let error = match session
        .add_magnet(magnet, TorrentAddConfig::default())
        .await
    {
        Ok(_) => panic!("trackerless magnet was admitted without a peer source"),
        Err(error) => error,
    };
    assert!(matches!(error, TorrentError::MagnetDiscoveryUnavailable));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);

    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_disabled_private_magnet_can_resolve_through_an_explicit_peer() {
    let (info, info_hash) = private_info();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = listener.local_addr().unwrap();
    let peer_task = tokio::spawn(metadata_peer(listener, info, info_hash));

    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let handle = tokio::time::timeout(
        Duration::from_secs(10),
        session.add_magnet(
            magnet,
            TorrentAddConfig {
                paused: true,
                initial_peers: vec![peer],
                ..Default::default()
            },
        ),
    )
    .await
    .expect("private magnet metadata resolution timed out")
    .expect("DHT-disabled private magnet should resolve through its explicit peer");

    tokio::time::timeout(Duration::from_secs(5), peer_task)
        .await
        .expect("metadata peer did not finish")
        .expect("metadata peer failed");
    session.delete(&handle, false).await.unwrap();
    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_hash_valid_metadata_has_a_deterministic_error() {
    let (info, info_hash) = structurally_invalid_info();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = listener.local_addr().unwrap();
    let peer_task = tokio::spawn(metadata_peer(listener, info, info_hash));
    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let result = session
        .add_magnet(
            magnet,
            TorrentAddConfig {
                initial_peers: vec![peer],
                ..Default::default()
            },
        )
        .await;
    let error = match result {
        Ok(_) => panic!("structurally invalid metadata was admitted"),
        Err(error) => error,
    };
    assert!(matches!(error, TorrentError::InvalidResolvedMagnetMetadata));
    peer_task.await.unwrap();
    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_disabled_tracker_bearing_magnet_resolves_through_http_tracker() {
    let (info, info_hash) = private_info();
    let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = peer_listener.local_addr().unwrap();
    let peer_task = tokio::spawn(metadata_peer(peer_listener, info, info_hash));
    let tracker_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let tracker_address = tracker_listener.local_addr().unwrap();
    let tracker_requests = Arc::new(AtomicUsize::new(0));
    let tracker_task = tokio::spawn(http_tracker(
        tracker_listener,
        peer,
        tracker_requests.clone(),
    ));
    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2F127.0.0.1%3A{}%2Fannounce",
        info_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        tracker_address.port()
    );

    let resolved = tokio::time::timeout(
        Duration::from_secs(10),
        session.resolve_magnet_metadata(magnet),
    )
    .await
    .expect("tracker metadata resolution timed out")
    .expect("DHT-off tracker discovery should resolve metadata");
    assert!(!resolved.is_empty());
    assert_eq!(tracker_requests.load(Ordering::SeqCst), 1);
    tracker_task.await.unwrap();
    peer_task.await.unwrap();
    session.stop().await;
}
