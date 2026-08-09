use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::time::Duration;

const REGISTERED_TORRENTS: usize = 100;
const ACTIVE_TORRENTS: usize = 10;
const ADMISSION_DEADLINE: Duration = Duration::from_secs(30);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const PIECE_LENGTH: usize = 16 * 1024;

fn bencode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

fn one_byte_metainfo(index: usize) -> Vec<u8> {
    let payload = [u8::try_from(index).expect("100-torrent probe index fits in one byte")];
    let filename = format!("pressure-{index:03}.bin");

    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, filename.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{PIECE_LENGTH}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');

    let mut metainfo = b"d4:info".to_vec();
    metainfo.extend_from_slice(&info);
    metainfo.push(b'e');
    metainfo
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_session_holds_100_torrents_and_stops_within_the_deadline() {
    let root = tempfile::tempdir().expect("create pressure-probe root");
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .expect("start DHT-disabled pressure-probe session");

    // These torrents have no trackers or initial peers and the session has DHT
    // disabled. The first ten are live-but-peerless while the remaining ninety
    // stay paused, so this exercises session bookkeeping and shutdown without
    // public discovery or payload transfer.
    let handles = tokio::time::timeout(ADMISSION_DEADLINE, async {
        let mut handles = Vec::with_capacity(REGISTERED_TORRENTS);
        for index in 0..REGISTERED_TORRENTS {
            handles.push(
                session
                    .add_metainfo(
                        one_byte_metainfo(index),
                        TorrentAddConfig {
                            paused: index >= ACTIVE_TORRENTS,
                            ..Default::default()
                        },
                    )
                    .await?,
            );
        }
        Ok::<_, nzbd_torrent::TorrentError>(handles)
    })
    .await
    .expect("100-torrent admission exceeded 30 seconds")
    .expect("admit pressure-probe torrents");

    assert_eq!(handles.len(), REGISTERED_TORRENTS);
    assert_eq!(
        handles
            .iter()
            .map(|handle| handle.id())
            .collect::<HashSet<_>>()
            .len(),
        REGISTERED_TORRENTS,
        "every registered torrent must keep a distinct engine handle"
    );
    assert_eq!(
        handles.iter().filter(|handle| !handle.is_paused()).count(),
        ACTIVE_TORRENTS,
        "the pressure mix must retain ten active and ninety paused torrents"
    );
    assert!(
        handles.iter().all(|handle| handle.stats().total_bytes == 1),
        "every pressure-probe torrent must finish metadata initialization"
    );

    tokio::time::timeout(SHUTDOWN_DEADLINE, session.stop())
        .await
        .expect("100-torrent session shutdown exceeded 10 seconds");
}
