use nzbd_torrent::{TorrentAddConfig, TorrentError, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::Ipv4Addr;
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
