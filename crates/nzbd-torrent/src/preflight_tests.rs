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

fn metainfo_with_announce_list(info: &[u8], trackers: &[Vec<u8>]) -> Vec<u8> {
    let mut torrent = vec![b'd'];
    bencode_bytes(&mut torrent, b"announce-list");
    torrent.push(b'l');
    for tracker in trackers {
        torrent.push(b'l');
        bencode_bytes(&mut torrent, tracker);
        torrent.push(b'e');
    }
    torrent.push(b'e');
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

#[derive(Debug, Default, PartialEq, Eq)]
struct MutationOutcomes {
    accepted: usize,
    rejected: usize,
}

impl MutationOutcomes {
    fn record(&mut self, result: Result<bool, TorrentError>) {
        if result.is_ok() {
            self.accepted += 1;
        } else {
            self.rejected += 1;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum FilesystemNameBehavior {
    Aliased,
    Distinct,
}

fn probe_filesystem_name_behavior(
    root: &std::path::Path,
    left: &str,
    right: &str,
) -> FilesystemNameBehavior {
    std::fs::create_dir(root.join(left)).unwrap();
    let behavior = match std::fs::create_dir(root.join(right)) {
        Ok(()) => FilesystemNameBehavior::Distinct,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            FilesystemNameBehavior::Aliased
        }
        Err(error) => panic!("filesystem probe could not create {right:?} after {left:?}: {error}"),
    };
    let expected_entries = match behavior {
        FilesystemNameBehavior::Aliased => 1,
        FilesystemNameBehavior::Distinct => 2,
    };
    assert_eq!(std::fs::read_dir(root).unwrap().count(), expected_entries);
    behavior
}

fn assert_preflight_does_not_panic(case: &str, bytes: &[u8], outcomes: &mut MutationOutcomes) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        validate_metainfo_contract(bytes, false)
    }));
    assert!(result.is_ok(), "preflight panicked for {case}: {bytes:?}");
    outcomes.record(result.unwrap());
}

#[test]
fn bounded_mutation_corpus_never_panics_preflight() {
    let seeds = [
        ("v1", v1_metainfo(b"payload.bin"), (306, 1_489)),
        ("v2", v2_metainfo(false), (0, 1_396)),
        ("hybrid", v2_metainfo(true), (0, 1_985)),
    ];

    for (seed_name, seed, (accepted, rejected)) in seeds {
        let mut outcomes = MutationOutcomes::default();
        for end in 0..=seed.len() {
            assert_preflight_does_not_panic(
                &format!("{seed_name}/truncate/{end}"),
                &seed[..end],
                &mut outcomes,
            );
        }

        for position in 0..seed.len() {
            for replacement in REPLACEMENTS {
                let mut mutated = seed.clone();
                mutated[position] = *replacement;
                assert_preflight_does_not_panic(
                    &format!("{seed_name}/replace/{position}/{replacement}"),
                    &mutated,
                    &mut outcomes,
                );
            }

            let mut deleted = seed.clone();
            deleted.remove(position);
            assert_preflight_does_not_panic(
                &format!("{seed_name}/delete/{position}"),
                &deleted,
                &mut outcomes,
            );
        }

        for position in 0..=seed.len() {
            for insertion in INSERTIONS {
                let mut mutated = seed.clone();
                mutated.insert(position, *insertion);
                assert_preflight_does_not_panic(
                    &format!("{seed_name}/insert/{position}/{insertion}"),
                    &mutated,
                    &mut outcomes,
                );
            }
        }
        assert_eq!(
            outcomes,
            MutationOutcomes { accepted, rejected },
            "{seed_name} mutation disposition changed; review the exact cases before updating this snapshot"
        );
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
fn portable_component_limit_is_encoded_bytes_not_scalar_count() {
    let at_limit = "日".repeat(MAX_TORRENT_PATH_COMPONENT_BYTES / 3);
    assert_eq!(at_limit.chars().count(), 85);
    assert_eq!(at_limit.len(), MAX_TORRENT_PATH_COMPONENT_BYTES);
    assert!(validate_metainfo_contract(&v1_metainfo(at_limit.as_bytes()), false).is_ok());

    let over_limit = format!("{at_limit}日");
    assert_eq!(over_limit.chars().count(), 86);
    assert_eq!(over_limit.len(), MAX_TORRENT_PATH_COMPONENT_BYTES + 3);
    assert!(matches!(
        validate_metainfo_contract(&v1_metainfo(over_limit.as_bytes()), false),
        Err(TorrentError::PathComponentTooLong { size, limit })
            if size == MAX_TORRENT_PATH_COMPONENT_BYTES + 3
                && limit == MAX_TORRENT_PATH_COMPONENT_BYTES
    ));
}

#[test]
fn private_metainfo_discovery_policy_fails_closed_when_dht_is_live() {
    let bytes = metainfo_with_announce(
        &private_single_file_info(b"payload.bin"),
        b"https://tracker.example/announce",
    );
    assert!(matches!(
        validate_metainfo_admission(&bytes, false, true),
        Err(TorrentError::PrivateMetainfoWithDht)
    ));
    assert!(validate_metainfo_admission(&bytes, false, false).is_ok());
    assert!(validate_metainfo_admission(&v1_metainfo(b"public.bin"), false, true).is_ok());
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
    assert_eq!(MAX_INITIAL_PEERS, 80);
    assert_eq!(MAX_TRACKERS_PER_TORRENT, 64);
    assert_eq!(MAX_TRACKER_URL_BYTES, 2 * 1024);

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
    let base32_magnet = format!("MAGNET:?xt=urn:btih:{base32}");
    assert!(validate_magnet_contract(&base32_magnet, false).is_ok());
    assert!(librqbit::Magnet::parse(&base32_magnet).is_ok());

    let lowercase_base32 = "a".repeat(32);
    let lowercase_magnet = format!("magnet:?xt=urn:btih:{lowercase_base32}");
    let normalized = validate_magnet_contract(&lowercase_magnet, false).unwrap();
    assert!(normalized.contains(&"A".repeat(32)));
    assert!(librqbit::Magnet::parse(&normalized).is_ok());

    let marker_in_display = format!("magnet:?xt=urn:btih:{hex}&dn=urn%3Abtmh%3Anot-a-topic");
    assert!(validate_magnet_contract(&marker_in_display, false).is_ok());
    let marker_in_tracker = format!(
        "magnet:?xt=urn:btih:{hex}&tr=https%3A%2F%2Ftracker.example%2Furn%3Abtmh%3Aignored"
    );
    assert!(validate_magnet_contract(&marker_in_tracker, false).is_ok());
}

#[test]
fn indexed_or_case_varied_magnet_keys_match_pinned_rqbit_behavior() {
    let hex = "00".repeat(20);
    for key in ["XT", "Xt", "xt.1"] {
        let magnet = format!("magnet:?{key}=urn:btih:{hex}");
        assert!(matches!(
            validate_magnet_contract(&magnet, false),
            Err(TorrentError::InvalidMagnet(_))
        ));
        assert!(librqbit::Magnet::parse(&magnet).is_err());
    }

    let indexed_selection = format!("magnet:?xt=urn:btih:{hex}&so.1=0-4000000000");
    let normalized = validate_magnet_contract(&indexed_selection, false).unwrap();
    assert!(librqbit::Magnet::parse(&normalized)
        .unwrap()
        .get_select_only()
        .is_none());

    let indexed_tracker =
        format!("magnet:?xt=urn:btih:{hex}&tr.1=https%3A%2F%2Ftracker.example%2Fannounce");
    let normalized = validate_magnet_contract(&indexed_tracker, false).unwrap();
    assert!(librqbit::Magnet::parse(&normalized)
        .unwrap()
        .trackers
        .is_empty());
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
        format!("magnet:?XT=urn:btih:{hex}"),
        format!("magnet:?xt=URN:BTIH:{hex}"),
        // rqbit 8.1.1 panics after decoding this advertised base32 Id32
        // length to 35 bytes and copying it into a 32-byte destination.
        format!("magnet:?xt=urn:btmh:1220{}", "A".repeat(56)),
        format!("magnet:?xt=urn:btmh:1220{}", "g".repeat(64)),
        format!("magnet:?xt=urn:unknown:{hex}&xt=urn:btih:{hex}"),
        format!("magnet:?xt=urn:btih:{hex}&so=0-4000000000"),
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
        &b"http://tracker.example:0/announce"[..],
        &b"udp://tracker.example:0/announce"[..],
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
        "https%3A%2F%2Ftracker.example%3A0%2Fannounce",
        "udp%3A%2F%2Ftracker.example%3A0%2Fannounce",
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
fn tracker_fanout_and_url_length_fail_at_the_first_excess() {
    let trackers = (0..=MAX_TRACKERS_PER_TORRENT)
        .map(|index| format!("https://tracker-{index}.example/announce").into_bytes())
        .collect::<Vec<_>>();
    assert!(validate_metainfo_contract(
        &metainfo_with_announce_list(
            &single_file_info(b"payload.bin"),
            &trackers[..MAX_TRACKERS_PER_TORRENT],
        ),
        false,
    )
    .is_ok());
    assert!(matches!(
        validate_metainfo_contract(
            &metainfo_with_announce_list(&single_file_info(b"payload.bin"), &trackers),
            false,
        ),
        Err(TorrentError::TooManyTrackers {
            count,
            limit: MAX_TRACKERS_PER_TORRENT,
        }) if count == MAX_TRACKERS_PER_TORRENT + 1
    ));

    let tracker_at_limit = format!(
        "https://tracker.example/{}",
        "a".repeat(MAX_TRACKER_URL_BYTES - "https://tracker.example/".len())
    );
    assert_eq!(tracker_at_limit.len(), MAX_TRACKER_URL_BYTES);
    assert!(validate_tracker_url(tracker_at_limit.as_bytes()).is_ok());
    assert!(matches!(
        validate_tracker_url(format!("{tracker_at_limit}a").as_bytes()),
        Err(TorrentError::TrackerUrlTooLong {
            size,
            limit: MAX_TRACKER_URL_BYTES,
        }) if size == MAX_TRACKER_URL_BYTES + 1
    ));

    let hex = "00".repeat(20);
    let magnet = |count: usize| {
        let mut magnet = format!("magnet:?xt=urn:btih:{hex}");
        for index in 0..count {
            magnet.push_str(&format!(
                "&tr=https%3A%2F%2Ftracker-{index}.example%2Fannounce"
            ));
        }
        magnet
    };
    assert!(validate_magnet_contract(&magnet(MAX_TRACKERS_PER_TORRENT), false).is_ok());
    assert!(matches!(
        validate_magnet_contract(&magnet(MAX_TRACKERS_PER_TORRENT + 1), false),
        Err(TorrentError::TooManyTrackers {
            count,
            limit: MAX_TRACKERS_PER_TORRENT,
        }) if count == MAX_TRACKERS_PER_TORRENT + 1
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

    for (left, right) in [
        ("ΣΑΣ.mkv", "σας.mkv"),
        ("ſong.mkv", "song.mkv"),
        ("ı.mkv", "i.mkv"),
    ] {
        let left_path: &[&[u8]] = &[b"disc", left.as_bytes()];
        let right_path: &[&[u8]] = &[b"disc", right.as_bytes()];
        let collision = metainfo(&multi_file_info_many(b"release", &[left_path, right_path]));
        assert!(matches!(
            validate_metainfo_contract(&collision, false),
            Err(TorrentError::PathCollision)
        ));
    }

    for (left, right) in [("ﬁle.mkv", "file.mkv"), ("Ｆ.mkv", "f.mkv")] {
        let left_path: &[&[u8]] = &[b"disc", left.as_bytes()];
        let right_path: &[&[u8]] = &[b"disc", right.as_bytes()];
        let distinct = metainfo(&multi_file_info_many(b"release", &[left_path, right_path]));
        assert!(validate_metainfo_contract(&distinct, false).is_ok());
    }
}

#[test]
fn filesystem_probe_records_aliasing_and_adapter_rejects_both_name_pairs() {
    let case_root = tempfile::tempdir().unwrap();
    let case_behavior = probe_filesystem_name_behavior(case_root.path(), "CaseProbe", "caseprobe");

    let unicode_root = tempfile::tempdir().unwrap();
    let unicode_behavior =
        probe_filesystem_name_behavior(unicode_root.path(), "Caf\u{e9}Probe", "Cafe\u{301}Probe");

    println!(
        "filesystem name probe: os={} case={case_behavior:?} unicode_nfc_nfd={unicode_behavior:?}",
        std::env::consts::OS
    );

    let upper_case_path: &[&[u8]] = &[b"CaseProbe"];
    let lower_case_path: &[&[u8]] = &[b"caseprobe"];
    let case_collision = metainfo(&multi_file_info_many(
        b"release",
        &[upper_case_path, lower_case_path],
    ));
    assert!(matches!(
        validate_metainfo_contract(&case_collision, false),
        Err(TorrentError::PathCollision)
    ));

    let composed_path: &[&[u8]] = &["Caf\u{e9}Probe".as_bytes()];
    let decomposed_path: &[&[u8]] = &["Cafe\u{301}Probe".as_bytes()];
    let unicode_collision = metainfo(&multi_file_info_many(
        b"release",
        &[composed_path, decomposed_path],
    ));
    assert!(matches!(
        validate_metainfo_contract(&unicode_collision, false),
        Err(TorrentError::PathCollision)
    ));
}

#[test]
fn adapter_rejects_file_directory_path_collisions_in_either_order() {
    let file: &[&[u8]] = &[b"Disc"];
    let child: &[&[u8]] = &[b"disc", b"track.bin"];
    for paths in [&[file, child][..], &[child, file][..]] {
        let collision = metainfo(&multi_file_info_many(b"Release", paths));
        assert!(matches!(
            validate_metainfo_contract(&collision, false),
            Err(TorrentError::PathCollision)
        ));
    }

    let sibling_one: &[&[u8]] = &[b"disc-one", b"track.bin"];
    let sibling_two: &[&[u8]] = &[b"disc-two", b"track.bin"];
    let distinct = metainfo(&multi_file_info_many(
        b"release",
        &[sibling_one, sibling_two],
    ));
    assert!(validate_metainfo_contract(&distinct, false).is_ok());
}

#[test]
fn adapter_rejects_existing_payload_path_type_conflicts() {
    let single_root = tempfile::tempdir().unwrap();
    std::fs::create_dir(single_root.path().join("payload.bin")).unwrap();
    assert!(matches!(
        validate_existing_filesystem_paths(&v1_metainfo(b"payload.bin"), single_root.path()),
        Err(TorrentError::ExistingPathType(
            "a payload leaf is not a regular file"
        ))
    ));

    let multi_root = tempfile::tempdir().unwrap();
    std::fs::write(multi_root.path().join("release"), b"not a directory").unwrap();
    let nested = metainfo(&multi_file_info(
        Some(b"release"),
        &[b"sub", b"payload.bin"],
        None,
        None,
    ));
    assert!(matches!(
        validate_existing_filesystem_paths(&nested, multi_root.path()),
        Err(TorrentError::ExistingPathType(
            "a payload prefix is not a directory"
        ))
    ));

    let resumable_root = tempfile::tempdir().unwrap();
    std::fs::write(resumable_root.path().join("payload.bin"), [0]).unwrap();
    assert!(validate_existing_filesystem_paths(
        &v1_metainfo(b"payload.bin"),
        resumable_root.path()
    )
    .is_ok());
}
