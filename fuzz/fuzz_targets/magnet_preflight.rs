#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(magnet) = std::str::from_utf8(bytes) {
        // Proxy mode exercises the additional UDP-tracker rejection path.
        // Both calls remain input-only and use the production preflight.
        let _ = nzbd_torrent::fuzz_magnet_preflight(magnet, false);
        let _ = nzbd_torrent::fuzz_magnet_preflight(magnet, true);
    }
});
