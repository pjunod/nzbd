use nzbd_torrent::{fuzz_magnet_preflight, TorrentError, MAX_MAGNET_URI_BYTES};

const VALID_V1_PREFIX: &str = "magnet:?xt=urn:btih:0000000000000000000000000000000000000000&dn=";

#[test]
fn exact_magnet_uri_limit_is_accepted_and_first_excess_is_named() {
    let mut at_limit = String::from(VALID_V1_PREFIX);
    at_limit.push_str(&"a".repeat(MAX_MAGNET_URI_BYTES - at_limit.len()));
    assert_eq!(at_limit.len(), MAX_MAGNET_URI_BYTES);
    assert!(fuzz_magnet_preflight(&at_limit, false).is_ok());

    let mut over_limit = at_limit;
    over_limit.push('a');
    assert!(matches!(
        fuzz_magnet_preflight(&over_limit, false),
        Err(TorrentError::MagnetTooLong { size, limit })
            if size == MAX_MAGNET_URI_BYTES + 1 && limit == MAX_MAGNET_URI_BYTES
    ));
}
