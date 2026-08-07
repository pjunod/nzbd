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
    single_file_info_with_geometry(name, 1, 16_384, &[0; 20])
}

fn unnamed_single_file_info() -> Vec<u8> {
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(b"i1e");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[0; 20]);
    info.push(b'e');
    info
}

fn private_single_file_info(name: &[u8]) -> Vec<u8> {
    let mut info = single_file_info(name);
    info.pop();
    bencode_bytes(&mut info, b"private");
    info.extend_from_slice(b"i1ee");
    info
}

fn single_file_info_with_geometry(
    name: &[u8],
    length: u64,
    piece_length: u32,
    pieces: &[u8],
) -> Vec<u8> {
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{length}e").as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, name);
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{piece_length}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, pieces);
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

fn metainfo_with_announce(info: &[u8], announce: &[u8]) -> Vec<u8> {
    let mut torrent = vec![b'd'];
    bencode_bytes(&mut torrent, b"announce");
    bencode_bytes(&mut torrent, announce);
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

fn multi_file_info_many(root: &[u8], paths: &[&[&[u8]]]) -> Vec<u8> {
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"files");
    info.push(b'l');
    for path in paths {
        info.push(b'd');
        bencode_bytes(&mut info, b"length");
        info.extend_from_slice(b"i1e");
        bencode_bytes(&mut info, b"path");
        info.push(b'l');
        for component in *path {
            bencode_bytes(&mut info, component);
        }
        info.extend_from_slice(b"ee");
    }
    info.push(b'e');
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, root);
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[0; 20]);
    info.push(b'e');
    info
}

fn multi_file_info_with_lengths(lengths: &[u64]) -> Vec<u8> {
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"files");
    info.push(b'l');
    for (index, length) in lengths.iter().enumerate() {
        info.push(b'd');
        bencode_bytes(&mut info, b"length");
        info.extend_from_slice(format!("i{length}e").as_bytes());
        bencode_bytes(&mut info, b"path");
        info.push(b'l');
        bencode_bytes(&mut info, format!("file-{index}").as_bytes());
        info.extend_from_slice(b"ee");
    }
    info.push(b'e');
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"release");
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
fn adapter_rejects_windows_device_aliases_and_reserved_characters() {
    for component in [
        &b"CON"[..],
        &b"con.txt"[..],
        &b"NUL .log"[..],
        &b"COM1"[..],
        "com\u{00b9}.txt".as_bytes(),
        &b"LPT9.bin"[..],
        &b"CONIN$"[..],
        &b"payload:stream"[..],
        &b"question?.mkv"[..],
        &b"quote\".mkv"[..],
        &b"control-\x1f.mkv"[..],
        &b"trailing."[..],
        &b"trailing "[..],
    ] {
        assert!(
            matches!(
                validate_metainfo_contract(&v1_metainfo(component), false),
                Err(TorrentError::UnsafeMetainfoPath(_))
            ),
            "Windows-unsafe component was accepted: {component:?}"
        );
    }

    for component in [
        &b"console.txt"[..],
        &b"com0.bin"[..],
        &b"com10.bin"[..],
        &b"lpt0.bin"[..],
        &b"auxiliary.mkv"[..],
        &b"space inside.mkv"[..],
    ] {
        assert!(
            validate_metainfo_contract(&v1_metainfo(component), false).is_ok(),
            "portable component was rejected: {component:?}"
        );
    }
}

#[test]
fn adapter_owns_checked_v1_piece_geometry() {
    let engine_piece_length = u32::MAX;
    let engine_piece_count = 16_384_u64;
    let engine_chunks_per_piece = u64::from(engine_piece_length).div_ceil(RQBIT_CHUNK_BYTES);
    let engine_chunk_overflow = single_file_info_with_geometry(
        b"payload.bin",
        u64::from(engine_piece_length) * engine_piece_count,
        engine_piece_length,
        &vec![0; engine_piece_count as usize * 20],
    );
    for (case, info) in [
        (
            "zero piece length",
            single_file_info_with_geometry(b"payload.bin", 1, 0, &[0; 20]),
        ),
        (
            "zero aggregate length",
            single_file_info_with_geometry(b"payload.bin", 0, 16_384, &[]),
        ),
        (
            "partial SHA-1",
            single_file_info_with_geometry(b"payload.bin", 1, 16_384, &[0; 19]),
        ),
        (
            "too many hashes",
            single_file_info_with_geometry(b"payload.bin", 1, 16_384, &[0; 40]),
        ),
        (
            "too few hashes",
            single_file_info_with_geometry(b"payload.bin", 16_385, 16_384, &[0; 20]),
        ),
        (
            "aggregate length overflow",
            multi_file_info_with_lengths(&[i64::MAX as u64, i64::MAX as u64, 2]),
        ),
        ("rqbit chunk count overflow", engine_chunk_overflow),
    ] {
        let result = validate_metainfo_contract(&metainfo(&info), false);
        assert!(
            matches!(result, Err(TorrentError::InvalidMetainfoGeometry(_))),
            "{case}: {result:?}"
        );
    }

    let two_pieces = single_file_info_with_geometry(b"payload.bin", 16_385, 16_384, &[0; 40]);
    assert!(validate_metainfo_contract(&metainfo(&two_pieces), false).is_ok());
    let zero_length_sidecar = multi_file_info_with_lengths(&[0, 1]);
    assert!(validate_metainfo_contract(&metainfo(&zero_length_sidecar), false).is_ok());

    let maximum_engine_chunks = single_file_info_with_geometry(
        b"payload.bin",
        u64::from(engine_piece_length) * (engine_piece_count - 1)
            + (engine_chunks_per_piece - 1) * RQBIT_CHUNK_BYTES,
        engine_piece_length,
        &vec![0; engine_piece_count as usize * 20],
    );
    assert!(validate_metainfo_contract(&metainfo(&maximum_engine_chunks), false).is_ok());
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
        multi_file_info(Some(b"NUL"), &[b"payload.bin"], None, None),
        multi_file_info(Some(b"release"), &[b"payload:stream"], None, None),
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

#[test]
fn adapter_rejects_rqbits_shared_fallback_for_unnamed_single_file_torrents() {
    let bytes = metainfo(&unnamed_single_file_info());
    let parsed = parsed_info(&bytes);
    let fallback = parsed
        .info
        .iter_file_details()
        .unwrap()
        .next()
        .unwrap()
        .filename
        .to_string()
        .unwrap();
    assert_eq!(fallback, "torrent-content");

    assert!(matches!(
        validate_metainfo_contract(&bytes, false),
        Err(TorrentError::UnsafeMetainfoPath(
            "torrent metainfo must declare an explicit payload name"
        ))
    ));
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
    assert_eq!(MAX_TORRENT_PATH_COMPONENT_BYTES, 255);
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

    let component_at_limit = vec![b'a'; MAX_TORRENT_PATH_COMPONENT_BYTES];
    assert!(validate_metainfo_contract(&v1_metainfo(&component_at_limit), false).is_ok());
    let component_over_limit = vec![b'a'; MAX_TORRENT_PATH_COMPONENT_BYTES + 1];
    assert!(matches!(
        validate_metainfo_contract(&v1_metainfo(&component_over_limit), false),
        Err(TorrentError::PathComponentTooLong {
            size: 256,
            limit: 255
        })
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

#[test]
fn tracker_preflight_rejects_inputs_the_engine_would_silently_drop() {
    for tracker in [
        &b"not-a-url"[..],
        &b"wss://tracker.example/announce"[..],
        &b"udp://tracker.example/announce"[..],
        &b"udp:/announce"[..],
        &b"https://\xff/announce"[..],
    ] {
        let result = validate_metainfo_contract(
            &metainfo_with_announce(&single_file_info(b"payload.bin"), tracker),
            false,
        );
        assert!(
            matches!(result, Err(TorrentError::InvalidTracker(_))),
            "invalid tracker was accepted: {}",
            String::from_utf8_lossy(tracker)
        );
    }

    let hex = "00".repeat(20);
    for tracker in [
        "not-a-url",
        "wss%3A%2F%2Ftracker.example%2Fannounce",
        "udp%3A%2F%2Ftracker.example%2Fannounce",
    ] {
        assert!(matches!(
            validate_magnet_contract(&format!("magnet:?xt=urn:btih:{hex}&tr={tracker}"), false),
            Err(TorrentError::InvalidTracker(_))
        ));
    }
    assert!(validate_magnet_contract(&format!("magnet:?xt=urn:btih:{hex}&tr="), false).is_ok());

    for tracker in [
        &b""[..],
        &b"http://tracker.example/announce"[..],
        &b"https://tracker.example/announce"[..],
        &b"udp://tracker.example:6969/announce"[..],
    ] {
        assert!(validate_metainfo_contract(
            &metainfo_with_announce(&single_file_info(b"payload.bin"), tracker),
            false
        )
        .is_ok());
    }

    assert!(matches!(
        validate_metainfo_contract(
            &metainfo_with_announce(&private_single_file_info(b"payload.bin"), b""),
            false,
        ),
        Err(TorrentError::PrivateTrackerCount(0))
    ));
}

#[test]
fn adapter_rejects_exact_and_portable_case_path_collisions() {
    let exact = metainfo(&multi_file_info_many(
        b"release",
        &[&[b"disc", b"movie.mkv"], &[b"disc", b"movie.mkv"]],
    ));
    assert!(matches!(
        validate_metainfo_contract(&exact, false),
        Err(TorrentError::PathCollision)
    ));

    let case = metainfo(&multi_file_info_many(
        b"release",
        &[&[b"Disc", b"Movie.mkv"], &[b"disc", b"movie.MKV"]],
    ));
    assert!(matches!(
        validate_metainfo_contract(&case, false),
        Err(TorrentError::PathCollision)
    ));

    let distinct = metainfo(&multi_file_info_many(
        b"release",
        &[&[b"disc-1", b"movie.mkv"], &[b"disc-2", b"movie.mkv"]],
    ));
    assert!(validate_metainfo_contract(&distinct, false).is_ok());
}

#[test]
fn adapter_rejects_canonically_equivalent_unicode_path_collisions() {
    let composed = "Caf\u{e9}.mkv".as_bytes();
    let decomposed = "cafe\u{301}.mkv".as_bytes();
    let composed_path: &[&[u8]] = &[b"Disc", composed];
    let decomposed_path: &[&[u8]] = &[b"disc", decomposed];
    let collision = metainfo(&multi_file_info_many(
        b"release",
        &[composed_path, decomposed_path],
    ));
    assert!(matches!(
        validate_metainfo_contract(&collision, false),
        Err(TorrentError::PathCollision)
    ));

    let distinct_accent = "cafe\u{300}.mkv".as_bytes();
    let distinct_path: &[&[u8]] = &[b"disc", distinct_accent];
    let distinct = metainfo(&multi_file_info_many(
        b"release",
        &[composed_path, distinct_path],
    ));
    assert!(validate_metainfo_contract(&distinct, false).is_ok());
}
