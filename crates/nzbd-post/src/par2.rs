//! par2 packet parsing + native quick verification (ARCHITECTURE.md §9).
//!
//! We parse par2 packets ourselves (simple); GF(2^16) repair math stays in
//! the `par2` subprocess. Quick verification never re-reads file data: par2
//! stores per-slice CRC32s with the last slice zero-padded to the block
//! size, so `combine(slice crcs)` must equal
//! `combine(whole-file CRC from download, crc(zero padding))` — and the
//! whole-file CRC is exactly what the engine computed from segment CRCs at
//! finalize time.

use crate::{DownloadEvidence, PostError, VerifyResult};
use nzbd_yenc::crc32_combine;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"PAR2\0PKT";

#[derive(Debug, Clone)]
pub struct Par2File {
    pub id: [u8; 16],
    pub name: String,
    pub length: u64,
    pub md5_16k: [u8; 16],
    pub slice_crcs: Vec<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct Par2Set {
    pub slice_size: u64,
    pub files: Vec<Par2File>,
    /// Distinct recovery blocks present across the parsed .par2 files.
    pub recovery_blocks: u32,
    /// The "main" par2 file (smallest, index packets) for subprocess calls.
    pub main_path: Option<PathBuf>,
}

/// Parse every readable `*.par2` in a directory into one set.
pub fn load_dir(dir: &Path) -> Result<Option<Par2Set>, PostError> {
    let mut par_files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map(|e| e.eq_ignore_ascii_case("par2"))
                .unwrap_or(false)
        })
        .collect();
    if par_files.is_empty() {
        return Ok(None);
    }
    par_files.sort();

    let mut slice_size = 0u64;
    let mut descs: HashMap<[u8; 16], (String, u64, [u8; 16])> = HashMap::new();
    let mut crcs: HashMap<[u8; 16], Vec<u32>> = HashMap::new();
    let mut exponents: BTreeSet<u32> = BTreeSet::new();
    let mut main_path: Option<PathBuf> = None;
    let mut main_size = u64::MAX;

    for path in &par_files {
        let Ok(bytes) = std::fs::read(path) else {
            continue; // unreadable / still paused-not-downloaded
        };
        // The main file is conventionally the smallest one with FileDesc packets.
        let mut has_desc = false;
        let mut pos = 0usize;
        while pos + 64 <= bytes.len() {
            if &bytes[pos..pos + 8] != MAGIC {
                pos += 4;
                continue;
            }
            let len = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().unwrap()) as usize;
            if len < 64 || pos + len > bytes.len() {
                break; // torn / partial file
            }
            let ptype = &bytes[pos + 48..pos + 64];
            let body = &bytes[pos + 64..pos + len];
            match ptype {
                b"PAR 2.0\0Main\0\0\0\0" if body.len() >= 12 => {
                    slice_size = u64::from_le_bytes(body[0..8].try_into().unwrap());
                }
                b"PAR 2.0\0FileDesc" => {
                    has_desc = true;
                    if body.len() >= 56 {
                        let mut id = [0u8; 16];
                        id.copy_from_slice(&body[0..16]);
                        let mut md5_16k = [0u8; 16];
                        md5_16k.copy_from_slice(&body[32..48]);
                        let length = u64::from_le_bytes(body[48..56].try_into().unwrap());
                        let name = String::from_utf8_lossy(&body[56..])
                            .trim_end_matches('\0')
                            .to_string();
                        descs.entry(id).or_insert((name, length, md5_16k));
                    }
                }
                b"PAR 2.0\0IFSC\0\0\0\0" if body.len() >= 16 => {
                    let mut id = [0u8; 16];
                    id.copy_from_slice(&body[0..16]);
                    let entry = crcs.entry(id).or_default();
                    if entry.is_empty() {
                        for chunk in body[16..].chunks_exact(20) {
                            entry.push(u32::from_le_bytes(chunk[16..20].try_into().unwrap()));
                        }
                    }
                }
                b"PAR 2.0\0RecvSlic" if body.len() >= 4 => {
                    exponents.insert(u32::from_le_bytes(body[0..4].try_into().unwrap()));
                }
                _ => {}
            }
            pos += len;
        }
        if has_desc && bytes.len() as u64 <= main_size {
            main_size = bytes.len() as u64;
            main_path = Some(path.clone());
        }
    }

    if descs.is_empty() || slice_size == 0 {
        return Ok(None);
    }
    let files = descs
        .into_iter()
        .map(|(id, (name, length, md5_16k))| Par2File {
            id,
            name,
            length,
            md5_16k,
            slice_crcs: crcs.get(&id).cloned().unwrap_or_default(),
        })
        .collect();
    Ok(Some(Par2Set {
        slice_size,
        files,
        recovery_blocks: exponents.len() as u32,
        main_path,
    }))
}

/// CRC32 of `len` zero bytes (for the last-slice padding).
pub fn zero_crc(len: u64) -> u32 {
    let mut h = crc32fast::Hasher::new();
    let buf = [0u8; 8192];
    let mut left = len;
    while left > 0 {
        let n = left.min(8192) as usize;
        h.update(&buf[..n]);
        left -= n as u64;
    }
    h.finalize()
}

/// One file's quick check: does the padded whole-file CRC derived from
/// download evidence equal the fold of the par2 slice CRCs?
pub fn quick_check_file(f: &Par2File, slice_size: u64, disk_len: u64, whole_crc: u32) -> bool {
    if disk_len != f.length || f.slice_crcs.is_empty() || slice_size == 0 {
        return false;
    }
    let n_slices = f.length.div_ceil(slice_size);
    if f.slice_crcs.len() as u64 != n_slices {
        return false;
    }
    let mut expected: Option<u32> = None;
    for crc in &f.slice_crcs {
        expected = Some(match expected {
            None => *crc,
            Some(prev) => crc32_combine(prev, *crc, slice_size),
        });
    }
    let pad = n_slices * slice_size - f.length;
    let actual = if pad > 0 {
        crc32_combine(whole_crc, zero_crc(pad), pad)
    } else {
        whole_crc
    };
    expected == Some(actual)
}

/// Quick verification of a whole set against download evidence
/// (whole-file CRCs the engine combined from segments — zero re-reads).
pub fn quick_verify(set: &Par2Set, evidence: &[DownloadEvidence]) -> VerifyResult {
    let mut damaged = 0u32;
    for f in &set.files {
        let ev = evidence.iter().find(|e| {
            e.path
                .file_name()
                .map(|n| n.to_string_lossy() == f.name.as_str())
                == Some(true)
        });
        let ok = match ev {
            Some(e) => match e.crc32 {
                Some(crc) => {
                    let disk_len = std::fs::metadata(&e.path).map(|m| m.len()).unwrap_or(0);
                    quick_check_file(f, set.slice_size, disk_len, crc)
                }
                None => false, // holes: whole-file CRC unknown
            },
            None => false, // file missing entirely
        };
        if !ok {
            damaged += 1;
        }
    }
    if damaged == 0 {
        VerifyResult::Intact
    } else {
        VerifyResult::Repairable {
            blocks_available: set.recovery_blocks,
            blocks_needed: 0, // unknown until a full verify counts them
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn crc(data: &[u8]) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(data);
        h.finalize()
    }

    #[test]
    fn zero_crc_matches_direct() {
        for n in [0u64, 1, 100, 8192, 20000] {
            assert_eq!(zero_crc(n), crc(&vec![0u8; n as usize]), "n={n}");
        }
    }

    #[test]
    fn parse_and_quick_verify_real_par2() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..50_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let file = tmp.path().join("payload.bin");
        std::fs::write(&file, &data).unwrap();
        let ok = Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c8",
                "set.par2",
                "payload.bin",
            ])
            .current_dir(tmp.path())
            .status()
            .unwrap()
            .success();
        assert!(ok, "par2 create failed");

        let set = load_dir(tmp.path()).unwrap().expect("set parsed");
        assert_eq!(set.slice_size, 8192);
        assert_eq!(set.files.len(), 1);
        assert_eq!(set.files[0].name, "payload.bin");
        assert_eq!(set.files[0].length, 50_000);
        assert_eq!(set.files[0].slice_crcs.len(), 7); // ceil(50000/8192)
        assert_eq!(set.recovery_blocks, 8);
        assert!(set.main_path.as_ref().unwrap().ends_with("set.par2"));

        // Quick verify from "download evidence" — the whole-file CRC only.
        let ev = vec![DownloadEvidence {
            path: file.clone(),
            crc32: Some(crc(&data)),
            segment_crcs: vec![],
        }];
        assert_eq!(quick_verify(&set, &ev), VerifyResult::Intact);

        // A single flipped byte must fail the quick check.
        let mut bad = data.clone();
        bad[25_000] ^= 0xFF;
        let ev_bad = vec![DownloadEvidence {
            path: file.clone(),
            crc32: Some(crc(&bad)),
            segment_crcs: vec![],
        }];
        match quick_verify(&set, &ev_bad) {
            VerifyResult::Repairable {
                blocks_available, ..
            } => assert_eq!(blocks_available, 8),
            other => panic!("expected damage, got {other:?}"),
        }

        // Unknown whole-file CRC (holes) is treated as damage.
        let ev_none = vec![DownloadEvidence {
            path: file,
            crc32: None,
            segment_crcs: vec![],
        }];
        assert!(matches!(
            quick_verify(&set, &ev_none),
            VerifyResult::Repairable { .. }
        ));
    }
}
