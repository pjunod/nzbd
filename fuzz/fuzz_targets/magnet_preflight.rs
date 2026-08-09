#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(magnet) = std::str::from_utf8(bytes) {
        // Proxy mode exercises the additional UDP-tracker rejection path.
        // Every accepted normalized value must also remain parseable by the
        // exact engine parser that receives it in production.
        for proxy_enabled in [false, true] {
            if let Ok(normalized) = nzbd_torrent::fuzz_magnet_preflight(magnet, proxy_enabled) {
                assert!(librqbit::Magnet::parse(&normalized).is_ok());
            }
        }
    }
});
