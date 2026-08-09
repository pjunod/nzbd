use nzbd_torrent::{fuzz_metainfo_preflight, TorrentError, MAX_TORRENT_FILES};

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

fn push_integer(output: &mut Vec<u8>, value: usize) {
    output.push(b'i');
    output.extend_from_slice(value.to_string().as_bytes());
    output.push(b'e');
}

fn metainfo_with_file_count(file_count: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(file_count.saturating_mul(40));
    output.extend_from_slice(b"d4:infod5:filesl");
    for index in 0..file_count {
        output.extend_from_slice(b"d6:lengthi1e4:pathl");
        push_bytes(&mut output, format!("{index:06}.bin").as_bytes());
        output.extend_from_slice(b"ee");
    }
    output.extend_from_slice(b"e4:name4:bulk12:piece length");
    push_integer(&mut output, file_count);
    output.extend_from_slice(b"6:pieces20:");
    output.extend_from_slice(&[0; 20]);
    output.extend_from_slice(b"ee");
    output
}

#[test]
fn exact_file_inventory_limit_is_accepted_and_first_excess_is_named() {
    let at_limit = metainfo_with_file_count(MAX_TORRENT_FILES);
    assert!(at_limit.len() < nzbd_torrent::DEFAULT_MAX_METAINFO_BYTES);
    assert!(matches!(
        fuzz_metainfo_preflight(&at_limit, false),
        Ok(false)
    ));

    let over_limit = metainfo_with_file_count(MAX_TORRENT_FILES + 1);
    assert!(over_limit.len() < nzbd_torrent::DEFAULT_MAX_METAINFO_BYTES);
    assert!(matches!(
        fuzz_metainfo_preflight(&over_limit, false),
        Err(TorrentError::TooManyFiles { count, limit })
            if count == MAX_TORRENT_FILES + 1 && limit == MAX_TORRENT_FILES
    ));
}
