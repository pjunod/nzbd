#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    // Exercise every proxy/DHT policy combination without creating a session
    // or performing any network or filesystem I/O.
    for proxy_enabled in [false, true] {
        for dht_enabled in [false, true] {
            let _ = nzbd_torrent::fuzz_metainfo_admission(bytes, proxy_enabled, dht_enabled);
        }
    }
});
