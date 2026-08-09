#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    // Proxy mode exercises the additional UDP-tracker rejection path. Both
    // calls remain metadata-only and use the production preflight function.
    let _ = nzbd_torrent::fuzz_metainfo_preflight(bytes, false);
    let _ = nzbd_torrent::fuzz_metainfo_preflight(bytes, true);
});
