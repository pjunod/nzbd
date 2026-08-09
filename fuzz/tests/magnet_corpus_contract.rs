use nzbd_torrent::{fuzz_magnet_preflight, TorrentError};

const VALID_V1: &str = include_str!("../corpus/magnet_preflight/valid-v1.magnet");
const LOWERCASE_BASE32: &str = include_str!("../corpus/magnet_preflight/lowercase-base32.magnet");
const V2_ONLY: &str = include_str!("../corpus/magnet_preflight/v2-only.magnet");
const HYBRID: &str = include_str!("../corpus/magnet_preflight/hybrid.magnet");
const SELECT_ONLY: &str = include_str!("../corpus/magnet_preflight/select-only.magnet");
const PROXY_UDP: &str = include_str!("../corpus/magnet_preflight/proxy-udp.magnet");

#[test]
fn committed_magnet_seeds_reach_the_named_preflight_classes() {
    assert!(matches!(
        fuzz_magnet_preflight(VALID_V1, false),
        Ok(magnet) if magnet == VALID_V1
    ));

    let normalized = fuzz_magnet_preflight(LOWERCASE_BASE32, false).unwrap();
    assert!(normalized.contains(&"A".repeat(32)));
    assert!(!normalized.contains(&"a".repeat(32)));

    assert!(matches!(
        fuzz_magnet_preflight(V2_ONLY, false),
        Err(TorrentError::UnsupportedV2Magnet)
    ));
    assert!(matches!(
        fuzz_magnet_preflight(HYBRID, false),
        Err(TorrentError::UnsupportedHybridMagnet)
    ));
    assert!(matches!(
        fuzz_magnet_preflight(SELECT_ONLY, false),
        Err(TorrentError::InvalidMagnet(message))
            if message == "select-only parameters are not supported"
    ));
    assert!(fuzz_magnet_preflight(PROXY_UDP, false).is_ok());
    assert!(matches!(
        fuzz_magnet_preflight(PROXY_UDP, true),
        Err(TorrentError::ProxyWithUdpTracker)
    ));
}
