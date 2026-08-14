use librqbit::{
    AddTorrent, AddTorrentOptions, ManagedTorrent, Session, SessionOptions,
    SessionPersistenceConfig,
};
use nzbd_torrent::install_process_crypto_provider;
use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

const CHILD_MODE: &str = "NZBD_M0_PERSISTENCE_CHILD";
const PIECE_LENGTH: usize = 16 * 1024;
const CHILD_MAX_LIFETIME: Duration = Duration::from_secs(30);

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn payload(seed: usize) -> Vec<u8> {
    (0..PIECE_LENGTH * 4)
        .map(|offset| ((offset * 31 + offset / 7 + seed) % 251) as u8)
        .collect()
}

fn metainfo(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut pieces = Vec::new();
    for piece in payload.chunks(PIECE_LENGTH) {
        pieces.extend_from_slice(&Sha1::digest(piece));
    }

    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{PIECE_LENGTH}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &pieces);
    info.push(b'e');

    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
}

fn options(folder: PathBuf, disable_auto_restore: bool) -> SessionOptions {
    SessionOptions {
        disable_dht: true,
        disable_dht_persistence: true,
        fastresume: true,
        persistence: Some(SessionPersistenceConfig::Json {
            folder: Some(folder),
        }),
        disable_auto_restore,
        ..Default::default()
    }
}

async fn add_paused(session: &Arc<Session>, torrent: Vec<u8>) -> Arc<ManagedTorrent> {
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap()
}

async fn child_phase(output: &Path, persistence: &Path, ready: &Path) {
    // The parent normally kills this process at the ready boundary. Keep a
    // process-wide deadline so an early parent panic or interrupt cannot leave
    // a detached librqbit session running indefinitely.
    let _lifetime_guard = std::thread::spawn(|| {
        std::thread::sleep(CHILD_MAX_LIFETIME);
        std::process::exit(1);
    });

    install_process_crypto_provider().unwrap();
    let first_payload = payload(11);
    let second_payload = payload(29);
    std::fs::write(output.join("first.bin"), &first_payload).unwrap();
    let mut partial_second = second_payload.clone();
    partial_second[PIECE_LENGTH..].fill(0);
    std::fs::write(output.join("second.bin"), partial_second).unwrap();

    let session = Session::new_with_opts(
        output.to_path_buf(),
        options(persistence.to_path_buf(), false),
    )
    .await
    .unwrap();
    let first = add_paused(&session, metainfo("first.bin", &first_payload)).await;
    let second = add_paused(&session, metainfo("second.bin", &second_payload)).await;
    first.wait_until_initialized().await.unwrap();
    second.wait_until_initialized().await.unwrap();
    assert_eq!(second.stats().progress_bytes, PIECE_LENGTH as u64);
    assert!(persistence.join("session.json").is_file());

    // Writing this file is the parent signal. Do not stop or drop the session:
    // the parent kills this process to exercise the unclean restart boundary.
    std::fs::write(ready, second.id().to_string()).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nzbd_authoritative_restore_survives_process_kill_without_promoting_unverified_data() {
    if std::env::var_os(CHILD_MODE).is_some() {
        let output = PathBuf::from(std::env::var_os("NZBD_M0_OUTPUT").unwrap());
        let persistence = PathBuf::from(std::env::var_os("NZBD_M0_PERSISTENCE").unwrap());
        let ready = PathBuf::from(std::env::var_os("NZBD_M0_READY").unwrap());
        child_phase(&output, &persistence, &ready).await;
        return;
    }

    install_process_crypto_provider().unwrap();
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("output");
    let persistence = root.path().join("persistence");
    let ready = root.path().join("child-ready");
    std::fs::create_dir_all(&output).unwrap();

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "nzbd_authoritative_restore_survives_process_kill_without_promoting_unverified_data",
            "--nocapture",
        ])
        .env(CHILD_MODE, "1")
        .env("NZBD_M0_OUTPUT", &output)
        .env("NZBD_M0_PERSISTENCE", &persistence)
        .env("NZBD_M0_READY", &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let preferred_id = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(id) = std::fs::read_to_string(&ready) {
                break id.parse::<usize>().unwrap();
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("persistence child exited before the kill boundary: {status}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("persistence child did not reach the kill boundary");
    child.kill().unwrap();
    child.wait().unwrap();

    let second_payload = payload(29);
    std::fs::write(output.join("second.bin"), &second_payload).unwrap();
    let authoritative = Session::new_with_opts(output, options(persistence, true))
        .await
        .unwrap();
    assert_eq!(
        authoritative.with_torrents(|torrents| torrents.count()),
        0,
        "session construction must not admit either library record"
    );

    let restored = authoritative
        .add_torrent(
            AddTorrent::from_bytes(metainfo("second.bin", &second_payload)),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                preferred_id: Some(preferred_id),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    restored.wait_until_initialized().await.unwrap();
    let stats = restored.stats();
    assert_eq!(stats.progress_bytes, PIECE_LENGTH as u64);
    assert!(!stats.finished);
    assert_eq!(
        authoritative.with_torrents(|torrents| torrents.count()),
        1,
        "only nzbd's selected authoritative torrent may be restored"
    );
    authoritative.stop().await;
}
