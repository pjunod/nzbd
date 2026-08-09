mod support;

use nzbd_torrent::{fuzz_metainfo_preflight, MAX_TORRENT_FILES};
use std::error::Error;
use std::io;
use std::time::{Duration, Instant};
use support::{observe_rss, sampled_rss_bytes};

const PREFLIGHT_DEADLINE: Duration = Duration::from_secs(30);
const RSS_GROWTH_CEILING_BYTES: u64 = 256 * 1024 * 1024;

fn bencode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

fn metainfo_with_file_count(file_count: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(file_count.saturating_mul(40));
    output.extend_from_slice(b"d4:infod5:filesl");
    for index in 0..file_count {
        output.extend_from_slice(b"d6:lengthi1e4:pathl");
        bencode_bytes(&mut output, format!("{index:06}.bin").as_bytes());
        output.extend_from_slice(b"ee");
    }
    output.extend_from_slice(b"e4:name4:bulk12:piece lengthi100000e6:pieces20:");
    output.extend_from_slice(&[0; 20]);
    output.extend_from_slice(b"ee");
    output
}

fn main() -> Result<(), Box<dyn Error>> {
    let baseline_rss_bytes = sampled_rss_bytes()?;
    let mut max_sampled_rss_bytes = baseline_rss_bytes;

    let metainfo = metainfo_with_file_count(MAX_TORRENT_FILES);
    let fixture_rss_bytes = observe_rss(&mut max_sampled_rss_bytes)?;
    let preflight_started = Instant::now();
    if fuzz_metainfo_preflight(&metainfo, false)? {
        return Err(io::Error::other("100,000-file public fixture parsed as private").into());
    }
    let preflight_elapsed = preflight_started.elapsed();
    let validated_rss_bytes = observe_rss(&mut max_sampled_rss_bytes)?;

    if preflight_elapsed > PREFLIGHT_DEADLINE {
        return Err(io::Error::other("100,000-file preflight exceeded 30 seconds").into());
    }

    let sampled_rss_growth_bytes = max_sampled_rss_bytes.saturating_sub(baseline_rss_bytes);
    if sampled_rss_growth_bytes > RSS_GROWTH_CEILING_BYTES {
        return Err(io::Error::other(format!(
            "sampled RSS growth {sampled_rss_growth_bytes} exceeded the {RSS_GROWTH_CEILING_BYTES}-byte regression ceiling"
        ))
        .into());
    }

    println!(
        "bittorrent_metainfo_pressure_memory files={MAX_TORRENT_FILES} metainfo_bytes={} baseline_rss_bytes={baseline_rss_bytes} fixture_rss_bytes={fixture_rss_bytes} validated_rss_bytes={validated_rss_bytes} max_sampled_rss_bytes={max_sampled_rss_bytes} sampled_rss_growth_bytes={sampled_rss_growth_bytes} ceiling_bytes={RSS_GROWTH_CEILING_BYTES} preflight_ms={}",
        metainfo.len(),
        preflight_elapsed.as_millis()
    );
    Ok(())
}
