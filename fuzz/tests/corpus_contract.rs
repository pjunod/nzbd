use nzbd_torrent::{fuzz_metainfo_preflight, TorrentError};

const VALID_V1: &[u8] = include_bytes!("../corpus/metainfo_preflight/valid-v1.torrent");
const PRIVATE_V1: &[u8] = include_bytes!("../corpus/metainfo_preflight/private-v1.torrent");
const V2_ONLY: &[u8] = include_bytes!("../corpus/metainfo_preflight/v2-only.torrent");
const HYBRID: &[u8] = include_bytes!("../corpus/metainfo_preflight/hybrid.torrent");

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
        fuzz_metainfo_preflight(V2_ONLY, false),
        Err(TorrentError::UnsupportedV2Metainfo)
    ));
    assert!(matches!(
        fuzz_metainfo_preflight(HYBRID, false),
        Err(TorrentError::UnsupportedHybridMetainfo)
    ));
}
