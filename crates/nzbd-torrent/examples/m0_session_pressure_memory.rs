mod support;

use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::error::Error;
use std::io;
use std::time::{Duration, Instant};
use support::{observe_rss, sampled_rss_bytes};

const REGISTERED_TORRENTS: usize = 100;
const ACTIVE_TORRENTS: usize = 10;
const ADMISSION_DEADLINE: Duration = Duration::from_secs(30);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const PIECE_LENGTH: usize = 16 * 1024;
const RSS_GROWTH_CEILING_BYTES: u64 = 192 * 1024 * 1024;

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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let baseline_rss_bytes = sampled_rss_bytes()?;
    let mut max_sampled_rss_bytes = baseline_rss_bytes;
    let root = tempfile::tempdir()?;
    let session =
        TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default()).await?;
    observe_rss(&mut max_sampled_rss_bytes)?;

    let admission_started = Instant::now();
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
            if (index + 1) % 10 == 0 {
                observe_rss(&mut max_sampled_rss_bytes)?;
            }
        }
        Ok::<_, Box<dyn Error>>(handles)
    })
    .await
    .map_err(|_| io::Error::other("100-torrent admission exceeded 30 seconds"))??;
    let admission_elapsed = admission_started.elapsed();

    if handles.len() != REGISTERED_TORRENTS
        || handles
            .iter()
            .map(|handle| handle.id())
            .collect::<HashSet<_>>()
            .len()
            != REGISTERED_TORRENTS
        || handles.iter().filter(|handle| !handle.is_paused()).count() != ACTIVE_TORRENTS
        || !handles.iter().all(|handle| handle.stats().total_bytes == 1)
    {
        return Err(
            io::Error::other("100-torrent pressure state did not match the contract").into(),
        );
    }

    let sampled_rss_growth_bytes = max_sampled_rss_bytes.saturating_sub(baseline_rss_bytes);
    if sampled_rss_growth_bytes > RSS_GROWTH_CEILING_BYTES {
        return Err(io::Error::other(format!(
            "sampled RSS growth {sampled_rss_growth_bytes} exceeded the {RSS_GROWTH_CEILING_BYTES}-byte regression ceiling"
        ))
        .into());
    }

    let shutdown_started = Instant::now();
    tokio::time::timeout(SHUTDOWN_DEADLINE, session.stop())
        .await
        .map_err(|_| io::Error::other("100-torrent session shutdown exceeded 10 seconds"))?;

    println!(
        "bittorrent_session_pressure_memory baseline_rss_bytes={baseline_rss_bytes} max_sampled_rss_bytes={max_sampled_rss_bytes} sampled_rss_growth_bytes={sampled_rss_growth_bytes} ceiling_bytes={RSS_GROWTH_CEILING_BYTES} admission_ms={} shutdown_ms={}",
        admission_elapsed.as_millis(),
        shutdown_started.elapsed().as_millis()
    );
    Ok(())
}
