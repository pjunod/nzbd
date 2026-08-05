use nzbd_torrent::{TorrentAddConfig, TorrentProxyConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILE_NAME: &str = "socks-payload.bin";

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn metainfo(payload: &[u8]) -> Vec<u8> {
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
    info.push(b'e');

    let mut torrent = b"d4:info".to_vec();
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

async fn authenticated_socks5_proxy(
    listener: tokio::net::TcpListener,
    expected_target: SocketAddr,
    target_socket: tokio::net::TcpSocket,
    authenticated: tokio::sync::oneshot::Sender<()>,
) {
    let (mut client, _) = listener.accept().await.unwrap();

    let mut greeting = [0u8; 2];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting[0], 5);
    let mut methods = vec![0; greeting[1] as usize];
    client.read_exact(&mut methods).await.unwrap();
    assert!(
        methods.contains(&2),
        "client did not offer username/password"
    );
    client.write_all(&[5, 2]).await.unwrap();

    let mut auth_header = [0u8; 2];
    client.read_exact(&mut auth_header).await.unwrap();
    assert_eq!(auth_header[0], 1);
    let mut username = vec![0; auth_header[1] as usize];
    client.read_exact(&mut username).await.unwrap();
    let password_len = client.read_u8().await.unwrap();
    let mut password = vec![0; password_len as usize];
    client.read_exact(&mut password).await.unwrap();
    assert_eq!(username, b"m0-user");
    assert_eq!(password, b"m0-pass");
    client.write_all(&[1, 0]).await.unwrap();

    let mut request = [0u8; 4];
    client.read_exact(&mut request).await.unwrap();
    assert_eq!(request, [5, 1, 0, 1], "expected an IPv4 CONNECT request");
    let mut ip = [0u8; 4];
    client.read_exact(&mut ip).await.unwrap();
    let port = client.read_u16().await.unwrap();
    let requested = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip), port));
    assert_eq!(requested, expected_target);

    let mut target = target_socket.connect(requested).await.unwrap();
    client
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();
    authenticated.send(()).unwrap();
    tokio::io::copy_bidirectional(&mut client, &mut target)
        .await
        .unwrap();
}

async fn recording_relay(
    listener: tokio::net::TcpListener,
    target: SocketAddr,
    sources: Arc<Mutex<Vec<SocketAddr>>>,
) {
    loop {
        let (mut client, source) = listener.accept().await.unwrap();
        sources.lock().unwrap().push(source);
        tokio::spawn(async move {
            let mut target = tokio::net::TcpStream::connect(target).await.unwrap();
            tokio::io::copy_bidirectional(&mut client, &mut target)
                .await
                .unwrap();
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_traffic_uses_authenticated_socks5() {
    let payload = (0..64 * 1024)
        .map(|index| ((index * 17 + 3) % 251) as u8)
        .collect::<Vec<_>>();
    let torrent = metainfo(&payload);

    let seed_root = tempfile::tempdir().unwrap();
    std::fs::write(seed_root.path().join(FILE_NAME), &payload).unwrap();
    let listen = free_port_range();
    let seeder_port = listen.start;
    let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, seeder_port));
    let seeder = TorrentSession::start(
        seed_root.path().to_path_buf(),
        TorrentSessionConfig {
            listen_port_range: Some(listen),
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

    // Put a recording relay in front of the seeder. The proxy connects to
    // the relay from a pre-bound source socket; any direct peer connection
    // has a different source and makes the privacy assertion fail.
    let relay_listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let relay_address = relay_listener.local_addr().unwrap();
    let relay_sources = Arc::new(Mutex::new(Vec::new()));
    let relay = tokio::spawn(recording_relay(relay_listener, peer, relay_sources.clone()));

    let proxy_listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let proxy_target_socket = tokio::net::TcpSocket::new_v4().unwrap();
    proxy_target_socket
        .bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        .unwrap();
    let proxy_source = proxy_target_socket.local_addr().unwrap();
    let (authenticated_tx, authenticated_rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(authenticated_socks5_proxy(
        proxy_listener,
        relay_address,
        proxy_target_socket,
        authenticated_tx,
    ));

    let download_root = tempfile::tempdir().unwrap();
    let downloader = TorrentSession::start(
        download_root.path().to_path_buf(),
        TorrentSessionConfig {
            proxy: Some(TorrentProxyConfig {
                url: format!("socks5://{proxy_address}"),
                username: Some("m0-user".into()),
                password: Some("m0-pass".into()),
            }),
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
                initial_peers: vec![relay_address],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), download.wait_until_completed())
        .await
        .expect("SOCKS-routed local transfer timed out")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), authenticated_rx)
        .await
        .expect("proxy never observed an authenticated connection")
        .unwrap();
    assert_eq!(
        std::fs::read(download_root.path().join(FILE_NAME)).unwrap(),
        payload
    );

    downloader.stop().await;
    tokio::time::timeout(Duration::from_secs(5), proxy)
        .await
        .expect("proxy did not close")
        .unwrap();
    relay.abort();
    let sources = relay_sources.lock().unwrap().clone();
    assert_eq!(
        sources,
        vec![proxy_source],
        "the seeder relay must observe only the proxy's pre-bound connection"
    );
    seeder.stop().await;
}
