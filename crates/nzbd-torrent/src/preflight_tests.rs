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

fn multi_file_info(
    root: Option<&[u8]>,
    path: &[&[u8]],
    attr: Option<&[u8]>,
    symlink_path: Option<&[&[u8]]>,
) -> Vec<u8> {
    let mut file = vec![b'd'];
    if let Some(attr) = attr {
        bencode_bytes(&mut file, b"attr");
        bencode_bytes(&mut file, attr);
    }
    bencode_bytes(&mut file, b"length");
    file.extend_from_slice(b"i1e");
    bencode_bytes(&mut file, b"path");
    file.push(b'l');
    for component in path {
        bencode_bytes(&mut file, component);
    }
    file.push(b'e');
    if let Some(symlink_path) = symlink_path {
        bencode_bytes(&mut file, b"symlink path");
        file.push(b'l');
        for component in symlink_path {
            bencode_bytes(&mut file, component);
        }
        file.push(b'e');
    }
    file.push(b'e');

    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"files");
    info.push(b'l');
    info.extend_from_slice(&file);
    info.push(b'e');
    if let Some(root) = root {
        bencode_bytes(&mut info, b"name");
        bencode_bytes(&mut info, root);
    }
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[0; 20]);
    info.push(b'e');
    info
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

#[test]
fn adapter_owns_portable_single_file_path_rejection() {
    for component in [
        &b""[..],
        &b"."[..],
        &b".."[..],
        &b"../escape"[..],
        &b"sub/escape"[..],
        &b"sub\\escape"[..],
        &b"/absolute"[..],
        &b"\\\\server\\share"[..],
        &b"C:escape"[..],
        &b"nul\0name"[..],
        &b"bad-utf8-\xff"[..],
    ] {
        assert!(
            matches!(
                validate_metainfo_contract(&v1_metainfo(component), false),
                Err(TorrentError::UnsafeMetainfoPath(_))
            ),
            "unsafe single-file component was accepted: {component:?}"
        );
    }
    assert!(validate_metainfo_contract(&v1_metainfo(b"payload.bin"), false).is_ok());
}

#[test]
fn adapter_owns_multi_file_root_component_and_symlink_rejection() {
    let safe = metainfo(&multi_file_info(
        Some(b"release"),
        &[b"sub", b"payload.bin"],
        None,
        None,
    ));
    assert!(validate_metainfo_contract(&safe, false).is_ok());

    let cases = [
        multi_file_info(None, &[b"payload.bin"], None, None),
        multi_file_info(Some(b"C:release"), &[b"payload.bin"], None, None),
        multi_file_info(Some(b"release"), &[], None, None),
        multi_file_info(Some(b"release"), &[b"sub", b""], None, None),
        multi_file_info(
            Some(b"release"),
            &[b"payload.bin"],
            Some(b"l"),
            Some(&[b"target"]),
        ),
        multi_file_info(
            Some(b"release"),
            &[b"payload.bin"],
            None,
            Some(&[b"target"]),
        ),
    ];
    for info in cases {
        assert!(matches!(
            validate_metainfo_contract(&metainfo(&info), false),
            Err(TorrentError::UnsafeMetainfoPath(_))
        ));
    }
}

fn parsed_info(bytes: &[u8]) -> librqbit::TorrentMetaV1<librqbit::ByteBuf<'_>> {
    librqbit::torrent_from_bytes(bytes).unwrap()
}

#[test]
fn proposal_admission_constants_and_input_boundaries_are_pinned() {
    assert_eq!(DEFAULT_MAX_METAINFO_BYTES, 10 * 1024 * 1024);
    assert_eq!(MAX_MAGNET_URI_BYTES, 16 * 1024);
    assert_eq!(MAX_TORRENT_FILES, 100_000);
    assert_eq!(MAX_TORRENT_RELATIVE_PATH_BYTES, 4 * 1024);
    assert_eq!(MAX_TORRENT_PATH_BYTES, 16 * 1024 * 1024);

    assert!(validate_metainfo_size(DEFAULT_MAX_METAINFO_BYTES).is_ok());
    assert!(matches!(
        validate_metainfo_size(DEFAULT_MAX_METAINFO_BYTES + 1),
        Err(TorrentError::MetainfoTooLarge { .. })
    ));
    assert!(validate_magnet_size(MAX_MAGNET_URI_BYTES).is_ok());
    assert!(matches!(
        validate_magnet_size(MAX_MAGNET_URI_BYTES + 1),
        Err(TorrentError::MagnetTooLong { .. })
    ));
}

#[test]
fn path_inventory_limits_fail_at_the_first_excess_byte_or_file() {
    let bytes = metainfo(&multi_file_info(
        Some(b"root"),
        &[b"sub", b"payload.bin"],
        None,
        None,
    ));
    let metainfo = parsed_info(&bytes);
    let projected_path_bytes = b"root/sub/payload.bin".len();

    assert!(validate_metainfo_paths_with_limits(
        &metainfo.info,
        MetainfoPathLimits {
            files: 1,
            relative_path_bytes: projected_path_bytes,
            all_path_bytes: projected_path_bytes,
        },
    )
    .is_ok());

    assert!(matches!(
        validate_metainfo_paths_with_limits(
            &metainfo.info,
            MetainfoPathLimits {
                files: 0,
                relative_path_bytes: usize::MAX,
                all_path_bytes: usize::MAX,
            },
        ),
        Err(TorrentError::TooManyFiles { count: 1, limit: 0 })
    ));
    assert!(matches!(
        validate_metainfo_paths_with_limits(
            &metainfo.info,
            MetainfoPathLimits {
                files: 1,
                relative_path_bytes: projected_path_bytes - 1,
                all_path_bytes: usize::MAX,
            },
        ),
        Err(TorrentError::PathTooLong { .. })
    ));
    assert!(matches!(
        validate_metainfo_paths_with_limits(
            &metainfo.info,
            MetainfoPathLimits {
                files: 1,
                relative_path_bytes: usize::MAX,
                all_path_bytes: projected_path_bytes - 1,
            },
        ),
        Err(TorrentError::PathMetadataTooLarge { .. })
    ));
}

#[test]
fn magnet_preflight_reads_exact_topics_instead_of_substrings() {
    let hex = "00".repeat(20);
    let base32 = "A".repeat(32);
    assert!(validate_magnet_contract(&format!("magnet:?xt=urn:btih:{hex}"), false).is_ok());
    assert!(validate_magnet_contract(&format!("MAGNET:?XT=URN:BTIH:{base32}"), false).is_ok());

    let marker_in_display = format!("magnet:?xt=urn:btih:{hex}&dn=urn%3Abtmh%3Anot-a-topic");
    assert!(validate_magnet_contract(&marker_in_display, false).is_ok());
    let marker_in_tracker = format!(
        "magnet:?xt=urn:btih:{hex}&tr=https%3A%2F%2Ftracker.example%2Furn%3Abtmh%3Aignored"
    );
    assert!(validate_magnet_contract(&marker_in_tracker, false).is_ok());
}

#[test]
fn magnet_preflight_names_format_version_and_proxy_failures() {
    let hex = "00".repeat(20);
    let v2 = format!("urn:btmh:1220{}", "00".repeat(32));
    assert!(matches!(
        validate_magnet_contract(&format!("magnet:?xt={v2}"), false),
        Err(TorrentError::UnsupportedV2Magnet)
    ));
    assert!(matches!(
        validate_magnet_contract(&format!("magnet:?xt=urn:btih:{hex}&xt={v2}"), false),
        Err(TorrentError::UnsupportedHybridMagnet)
    ));

    for magnet in [
        "https://example.test/file.torrent".to_owned(),
        "magnet:?dn=missing-topic".to_owned(),
        "magnet:?xt=urn:btih:short".to_owned(),
        format!("magnet:?xt=urn:btih:{}", "Z".repeat(40)),
        format!("magnet:?xt=urn:btih:{hex}&xt=urn:btih:{hex}"),
    ] {
        assert!(
            matches!(
                validate_magnet_contract(&magnet, false),
                Err(TorrentError::InvalidMagnet(_))
            ),
            "invalid magnet was accepted: {magnet}"
        );
    }

    let udp = format!("magnet:?xt=urn:btih:{hex}&tr=udp%3A%2F%2F127.0.0.1%3A1%2Fannounce");
    assert!(matches!(
        validate_magnet_contract(&udp, true),
        Err(TorrentError::ProxyWithUdpTracker)
    ));
}
