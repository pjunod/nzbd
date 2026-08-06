use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn metainfo(private: bool) -> (Vec<u8>, [u8; 20]) {
    let payload = if private {
        b"private PEX canary".as_slice()
    } else {
        b"public PEX control".as_slice()
    };
    let name = if private {
        b"private-pex.bin".as_slice()
    } else {
        b"public-pex.bin".as_slice()
    };
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name);
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

fn peer_handshake(info_hash: [u8; 20]) -> Vec<u8> {
    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0_u8; 8];
    reserved[5] = 0x10;
    handshake.extend_from_slice(&reserved);
    handshake.extend_from_slice(&info_hash);
    handshake.extend_from_slice(b"-NZ0001-PEX-CANARY12");
    assert_eq!(handshake.len(), 68);
    handshake
}

fn extended_message(extension_id: u8, bencoded_payload: &[u8]) -> Vec<u8> {
    let length = 2_u32 + u32::try_from(bencoded_payload.len()).unwrap();
    let mut message = Vec::with_capacity(length as usize + 4);
    message.extend_from_slice(&length.to_be_bytes());
    message.push(20);
    message.push(extension_id);
    message.extend_from_slice(bencoded_payload);
    message
}

async fn pex_peer(
    listener: TcpListener,
    info_hash: [u8; 20],
    canary: SocketAddr,
    sent: oneshot::Sender<()>,
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut incoming_handshake = [0_u8; 68];
    stream.read_exact(&mut incoming_handshake).await.unwrap();
    assert_eq!(&incoming_handshake[28..48], &info_hash);
    stream.write_all(&peer_handshake(info_hash)).await.unwrap();

    let extended_handshake = extended_message(0, b"d1:md6:ut_pexi1eee");
    stream.write_all(&extended_handshake).await.unwrap();
    let SocketAddr::V4(canary) = canary else {
        panic!("test canary must use IPv4");
    };
    let mut compact_peer = Vec::with_capacity(6);
    compact_peer.extend_from_slice(&canary.ip().octets());
    compact_peer.extend_from_slice(&canary.port().to_be_bytes());
    let mut pex = b"d5:added6:".to_vec();
    pex.extend_from_slice(&compact_peer);
    pex.push(b'e');
    stream.write_all(&extended_message(1, &pex)).await.unwrap();
    sent.send(()).unwrap();

    let mut drain = [0_u8; 1024];
    while stream.read(&mut drain).await.unwrap_or(0) != 0 {}
}

async fn pex_canary_was_contacted(private: bool) -> bool {
    let initial_peer = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let initial_peer_address = initial_peer.local_addr().unwrap();
    let canary = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let canary_address = canary.local_addr().unwrap();
    let (torrent, info_hash) = metainfo(private);
    let (sent_tx, sent_rx) = oneshot::channel();
    let peer_task = tokio::spawn(pex_peer(initial_peer, info_hash, canary_address, sent_tx));

    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let _torrent = session
        .add_metainfo(
            torrent,
            TorrentAddConfig {
                initial_peers: vec![initial_peer_address],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sent_rx)
        .await
        .expect("fake peer was not contacted")
        .expect("fake peer closed before sending PEX");
    let contacted = tokio::time::timeout(Duration::from_secs(2), canary.accept())
        .await
        .is_ok();

    session.stop().await;
    peer_task.abort();
    contacted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_ignores_pex_while_public_control_uses_it() {
    assert!(
        pex_canary_was_contacted(false).await,
        "public control did not use the injected PEX peer"
    );
    assert!(
        !pex_canary_was_contacted(true).await,
        "private torrent contacted a peer learned only through PEX"
    );
}
