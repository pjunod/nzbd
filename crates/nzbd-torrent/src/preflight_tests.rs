use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

const REPLACEMENTS: &[u8] = &[0, b':', b'0', b'9', b'd', b'e', b'i', b'l', 0xff];
const INSERTIONS: &[u8] = &[0, b':', b'0', b'd', b'e', b'i', b'l', 0xff];

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn single_file_info(name: &[u8]) -> Vec<u8> {
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name);
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[0; 20]);
    info.push(b'e');
    info
}

fn metainfo(info: &[u8]) -> Vec<u8> {
    let mut torrent = vec![b'd'];
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(info);
    torrent.push(b'e');
    torrent
}

fn v1_metainfo(name: &[u8]) -> Vec<u8> {
    metainfo(&single_file_info(name))
}

fn v2_metainfo(hybrid: bool) -> Vec<u8> {
    let mut info = b"d9:file treede12:meta versioni2e4:name2:v212:piece lengthi16384e".to_vec();
    if hybrid {
        bencode_bytes(&mut info, b"pieces");
        bencode_bytes(&mut info, &[0; 20]);
    }
    info.push(b'e');
    metainfo(&info)
}

fn assert_preflight_does_not_panic(case: &str, bytes: &[u8]) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = validate_metainfo_contract(bytes, false);
    }));
    assert!(result.is_ok(), "preflight panicked for {case}: {bytes:?}");
}

#[test]
fn bounded_mutation_corpus_never_panics_preflight() {
    let seeds = [
        ("v1", v1_metainfo(b"payload.bin")),
        ("v2", v2_metainfo(false)),
        ("hybrid", v2_metainfo(true)),
    ];

    for (seed_name, seed) in seeds {
        for end in 0..=seed.len() {
            assert_preflight_does_not_panic(&format!("{seed_name}/truncate/{end}"), &seed[..end]);
        }

        for position in 0..seed.len() {
            for replacement in REPLACEMENTS {
                let mut mutated = seed.clone();
                mutated[position] = *replacement;
                assert_preflight_does_not_panic(
                    &format!("{seed_name}/replace/{position}/{replacement}"),
                    &mutated,
                );
            }

            let mut deleted = seed.clone();
            deleted.remove(position);
            assert_preflight_does_not_panic(&format!("{seed_name}/delete/{position}"), &deleted);
        }

        for position in 0..=seed.len() {
            for insertion in INSERTIONS {
                let mut mutated = seed.clone();
                mutated.insert(position, *insertion);
                assert_preflight_does_not_panic(
                    &format!("{seed_name}/insert/{position}/{insertion}"),
                    &mutated,
                );
            }
        }
    }
}

#[test]
fn every_truncated_v1_metainfo_is_rejected() {
    let seed = v1_metainfo(b"payload.bin");
    assert!(validate_metainfo_contract(&seed, false).is_ok());
    for end in 0..seed.len() {
        assert!(
            validate_metainfo_contract(&seed[..end], false).is_err(),
            "truncated metainfo was accepted at byte {end}"
        );
    }
}

#[test]
fn scanner_only_treats_direct_info_keys_as_version_markers() {
    let framed = v1_metainfo(b"meta version pieces d4:infoe ../escape");
    assert_eq!(
        MetainfoVersionScanner::new(&framed).scan().unwrap(),
        (true, false)
    );

    assert!(matches!(
        validate_metainfo_version(&v2_metainfo(false)),
        Err(TorrentError::UnsupportedV2Metainfo)
    ));
    assert!(matches!(
        validate_metainfo_version(&v2_metainfo(true)),
        Err(TorrentError::UnsupportedHybridMetainfo)
    ));
}

fn nested_value_metainfo(depth: usize) -> Vec<u8> {
    let mut torrent = b"d1:x".to_vec();
    torrent.extend(std::iter::repeat_n(b'l', depth));
    torrent.extend_from_slice(b"0:");
    torrent.extend(std::iter::repeat_n(b'e', depth));
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&single_file_info(b"payload.bin"));
    torrent.push(b'e');
    torrent
}

#[test]
fn scanner_enforces_structural_bounds_and_exact_root() {
    assert!(MetainfoVersionScanner::new(&nested_value_metainfo(128))
        .scan()
        .is_ok());
    assert!(MetainfoVersionScanner::new(&nested_value_metainfo(129))
        .scan()
        .is_err());

    let overflow = format!("d1:x{}:4:infodee", usize::MAX).into_bytes();
    assert!(MetainfoVersionScanner::new(&overflow).scan().is_err());

    let mut duplicate_info = v1_metainfo(b"payload.bin");
    duplicate_info.pop();
    bencode_bytes(&mut duplicate_info, b"info");
    duplicate_info.extend_from_slice(&single_file_info(b"second.bin"));
    duplicate_info.push(b'e');
    assert!(MetainfoVersionScanner::new(&duplicate_info).scan().is_err());

    let mut trailing = v1_metainfo(b"payload.bin");
    trailing.push(b'e');
    assert!(MetainfoVersionScanner::new(&trailing).scan().is_err());
}
