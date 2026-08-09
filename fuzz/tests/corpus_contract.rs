use nzbd_torrent::{fuzz_metainfo_admission, fuzz_metainfo_preflight, TorrentError};

const VALID_V1: &[u8] = include_bytes!("../seeds/metainfo_preflight/valid-v1.torrent");
const PRIVATE_V1: &[u8] = include_bytes!("../seeds/metainfo_preflight/private-v1.torrent");
const V2_ONLY: &[u8] = include_bytes!("../seeds/metainfo_preflight/v2-only.torrent");
const HYBRID: &[u8] = include_bytes!("../seeds/metainfo_preflight/hybrid.torrent");
const UDP_TRACKER_V1: &[u8] = include_bytes!("../seeds/metainfo_preflight/udp-tracker-v1.torrent");
const MULTI_FILE_V1: &[u8] = include_bytes!("../seeds/metainfo_preflight/multi-file-v1.torrent");
const ANNOUNCE_LIST_V1: &[u8] =
    include_bytes!("../seeds/metainfo_preflight/announce-list-v1.torrent");
const PRIVATE_TWO_TRACKERS_V1: &[u8] =
    include_bytes!("../seeds/metainfo_preflight/private-two-trackers-v1.torrent");
const UNSAFE_PATH_V1: &[u8] = include_bytes!("../seeds/metainfo_preflight/unsafe-path-v1.torrent");

#[test]
fn committed_seeds_reach_the_named_preflight_classes() {
    assert!(matches!(
        fuzz_metainfo_preflight(VALID_V1, false),
        Ok(false)
    ));
    assert!(matches!(fuzz_metainfo_preflight(VALID_V1, true), Ok(false)));

    assert!(matches!(
        fuzz_metainfo_preflight(PRIVATE_V1, false),
        Ok(true)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(PRIVATE_V1, true),
        Ok(true)
    ));
    assert!(matches!(
        fuzz_metainfo_admission(PRIVATE_V1, false, false),
        Ok(())
    ));
    assert!(matches!(
        fuzz_metainfo_admission(PRIVATE_V1, false, true),
        Err(TorrentError::PrivateMetainfoWithDht)
    ));

    for proxy_enabled in [false, true] {
        assert!(matches!(
            fuzz_metainfo_preflight(V2_ONLY, proxy_enabled),
            Err(TorrentError::UnsupportedV2Metainfo)
        ));
        assert!(matches!(
            fuzz_metainfo_preflight(HYBRID, proxy_enabled),
            Err(TorrentError::UnsupportedHybridMetainfo)
        ));
    }

    assert!(matches!(
        fuzz_metainfo_preflight(UDP_TRACKER_V1, false),
        Ok(false)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(UDP_TRACKER_V1, true),
        Err(TorrentError::ProxyWithUdpTracker)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(MULTI_FILE_V1, false),
        Ok(false)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(ANNOUNCE_LIST_V1, false),
        Ok(false)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(PRIVATE_TWO_TRACKERS_V1, false),
        Err(TorrentError::PrivateTrackerCount(2))
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(UNSAFE_PATH_V1, false),
        Err(TorrentError::UnsafeMetainfoPath(_))
    ));
}
