//! par-rename + rar-rename (ARCHITECTURE.md §9): recover real filenames of
//! obfuscated posts before verification/unpack.
//!
//! - **par-rename**: par2 FileDesc packets carry each source file's name
//!   and the MD5 of its first 16 KiB. Any disk file whose 16k-hash matches
//!   a description is renamed to its true name. Obfuscated `.par2` files
//!   themselves are found by content (`PAR2\0PKT` magic), not extension.
//! - **rar-rename**: files whose *content* is a RAR/7z/zip volume but
//!   whose name hides it get an extension back. Multi-volume RAR sets are
//!   numbered in stem order (uploaders obfuscate consistently); RAR5
//!   internal volume numbers are honored when present.

use md5::{Digest, Md5};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

const PAR2_MAGIC: &[u8] = b"PAR2\0PKT";
const RAR_MAGIC: &[u8] = b"Rar!\x1a\x07"; // v4: +\x00, v5: +\x01\x00
const SEVENZIP_MAGIC: &[u8] = b"7z\xbc\xaf\x27\x1c";
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

fn head(path: &Path, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    let Ok(mut f) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let mut read = 0;
    while read < n {
        match f.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(k) => read += k,
            Err(_) => break,
        }
    }
    buf.truncate(read);
    buf
}

fn md5_16k(path: &Path) -> Option<[u8; 16]> {
    let data = head(path, 16384);
    if data.is_empty() {
        return None;
    }
    let mut h = Md5::new();
    h.update(&data);
    Some(h.finalize().into())
}

fn files_of(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    out.sort();
    out
}

fn ext_is(p: &Path, ext: &str) -> bool {
    p.extension()
        .map(|e| e.eq_ignore_ascii_case(ext))
        .unwrap_or(false)
}

/// Rename a file, refusing to clobber. Returns the final path on success.
fn safe_rename(from: &Path, to: PathBuf) -> Option<(PathBuf, PathBuf)> {
    if from == to.as_path() || to.exists() {
        return None;
    }
    match std::fs::rename(from, &to) {
        Ok(()) => {
            tracing::info!(from = %from.display(), to = %to.display(), "renamed");
            Some((from.to_path_buf(), to))
        }
        Err(e) => {
            tracing::warn!(from = %from.display(), error = %e, "rename failed");
            None
        }
    }
}

/// par-rename. Returns `(old, new)` pairs so the caller can remap download
/// evidence (whole-file CRCs are content-addressed; only paths change).
pub fn par_rename(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    let mut renames = Vec::new();

    // 1. Give obfuscated par2 files their extension back (by magic).
    for p in files_of(dir) {
        if !ext_is(&p, "par2") && head(&p, 8) == PAR2_MAGIC {
            let to = dir.join(format!(
                "{}.par2",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        }
    }

    // 2. Match every remaining file's 16k-MD5 against the par2 catalog.
    let Ok(Some(set)) = crate::par2::load_dir(dir) else {
        return renames;
    };
    let wanted: HashMap<[u8; 16], &str> = set
        .files
        .iter()
        .map(|f| (f.md5_16k, f.name.as_str()))
        .collect();
    for p in files_of(dir) {
        if ext_is(&p, "par2") {
            continue;
        }
        let name = p.file_name().unwrap_or_default().to_string_lossy();
        if set.files.iter().any(|f| f.name == name) {
            continue; // already correctly named
        }
        let Some(hash) = md5_16k(&p) else { continue };
        if let Some(true_name) = wanted.get(&hash) {
            if let Some(pair) = safe_rename(&p, dir.join(true_name)) {
                renames.push(pair);
            }
        }
    }
    renames
}

/// RAR5 archives carry their volume number in the main archive header;
/// parse just enough (magic + one vint field walk) to extract it.
fn rar5_volume_number(data: &[u8]) -> Option<u64> {
    // RAR5 signature is 8 bytes: Rar!\x1a\x07\x01\x00
    if data.len() < 8 || &data[..7] != b"Rar!\x1a\x07\x01" {
        return None;
    }
    let mut pos = 8usize;
    let vint = |data: &[u8], pos: &mut usize| -> Option<u64> {
        let mut v = 0u64;
        for i in 0..10 {
            let b = *data.get(*pos)?;
            *pos += 1;
            v |= ((b & 0x7f) as u64) << (7 * i);
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    };
    // Header: crc32(4) + size(vint) + type(vint) + flags(vint) …
    pos += 4;
    let _size = vint(data, &mut pos)?;
    let htype = vint(data, &mut pos)?;
    if htype != 1 {
        return None; // expected the main archive header
    }
    let hflags = vint(data, &mut pos)?;
    if hflags & 0x0001 != 0 {
        let _extra = vint(data, &mut pos)?;
    }
    let arcflags = vint(data, &mut pos)?;
    // 0x0001 = volume, 0x0002 = volume number field present
    if arcflags & 0x0002 != 0 {
        return vint(data, &mut pos); // 0-based volume number
    }
    if arcflags & 0x0001 != 0 {
        return Some(0); // first volume of a set
    }
    None
}

/// rar-rename (plus 7z/zip signatures). Returns `(old, new)` pairs.
pub fn rar_rename(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    let mut renames = Vec::new();
    let known = ["rar", "7z", "zip", "par2", "nzb", "sfv", "nfo", "srr"];
    let mut hidden_rars: Vec<(PathBuf, Option<u64>)> = Vec::new();

    for p in files_of(dir) {
        let ext = p
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        if known.contains(&ext.as_str())
            || ext.chars().all(|c| c.is_ascii_digit()) && !ext.is_empty()
        {
            continue;
        }
        let h = head(&p, 32);
        if h.starts_with(RAR_MAGIC) {
            hidden_rars.push((p, rar5_volume_number(&h)));
        } else if h.starts_with(SEVENZIP_MAGIC) {
            let to = dir.join(format!(
                "{}.7z",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        } else if h.starts_with(ZIP_MAGIC) {
            let to = dir.join(format!(
                "{}.zip",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        }
    }

    match hidden_rars.len() {
        0 => {}
        1 => {
            let (p, _) = &hidden_rars[0];
            let to = dir.join(format!(
                "{}.rar",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(p, to) {
                renames.push(pair);
            }
        }
        _ => {
            // Multi-volume: RAR5 volume numbers when available, else stem
            // order (obfuscation is applied uniformly, preserving order).
            let base = hidden_rars[0]
                .0
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let numbered = hidden_rars.iter().all(|(_, n)| n.is_some());
            let mut ordered: Vec<PathBuf> = if numbered {
                let mut v = hidden_rars.clone();
                v.sort_by_key(|(_, n)| n.unwrap());
                v.into_iter().map(|(p, _)| p).collect()
            } else {
                hidden_rars.iter().map(|(p, _)| p.clone()).collect()
            };
            ordered.sort_by_key(|p| {
                hidden_rars
                    .iter()
                    .position(|(q, _)| q == p)
                    .unwrap_or(usize::MAX)
            });
            if numbered {
                let mut v = hidden_rars.clone();
                v.sort_by_key(|(_, n)| n.unwrap());
                ordered = v.into_iter().map(|(p, _)| p).collect();
            }
            for (i, p) in ordered.iter().enumerate() {
                let to = dir.join(format!("{base}.part{:02}.rar", i + 1));
                if let Some(pair) = safe_rename(p, to) {
                    renames.push(pair);
                }
            }
        }
    }
    renames
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn par_rename_recovers_obfuscated_names() {
        let tmp = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(tmp.path().join("Great.Movie.2026.mkv"), &data).unwrap();
        let ok = Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c4",
                "set.par2",
                "Great.Movie.2026.mkv",
            ])
            .current_dir(tmp.path())
            .status()
            .expect("par2 required")
            .success();
        assert!(ok);

        // Obfuscate: data file AND the par2 index lose their names.
        std::fs::rename(
            tmp.path().join("Great.Movie.2026.mkv"),
            tmp.path().join("a9f3c2e1"),
        )
        .unwrap();
        std::fs::rename(tmp.path().join("set.par2"), tmp.path().join("b7d1")).unwrap();

        let renames = par_rename(tmp.path());
        assert!(tmp.path().join("Great.Movie.2026.mkv").exists());
        assert!(tmp.path().join("b7d1.par2").exists(), "par2 magic detected");
        assert!(renames
            .iter()
            .any(|(o, n)| o.ends_with("a9f3c2e1") && n.ends_with("Great.Movie.2026.mkv")));

        // Idempotent: nothing left to rename.
        assert!(par_rename(tmp.path()).is_empty());
    }

    #[test]
    fn rar_rename_by_signature() {
        let tmp = tempfile::tempdir().unwrap();
        // A real single-volume rar is not required — the signature is.
        let mut rar4 = b"Rar!\x1a\x07\x00".to_vec();
        rar4.extend_from_slice(&[0u8; 64]);
        std::fs::write(tmp.path().join("obfuscated01"), &rar4).unwrap();
        std::fs::write(tmp.path().join("readme.txt"), b"hello").unwrap();
        let mut z = SEVENZIP_MAGIC.to_vec();
        z.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("mystery"), &z).unwrap();

        let renames = rar_rename(tmp.path());
        assert!(tmp.path().join("obfuscated01.rar").exists());
        assert!(tmp.path().join("mystery.7z").exists());
        assert!(
            tmp.path().join("readme.txt").exists(),
            "plain files untouched"
        );
        assert_eq!(renames.len(), 2);
    }

    #[test]
    fn rar5_volume_number_parses() {
        // Synthesized minimal RAR5 main header: sig + crc + size +
        // type=1 + hflags=0 + arcflags=volume|number + number=3.
        let mut d = b"Rar!\x1a\x07\x01\x00".to_vec();
        d.extend_from_slice(&[0, 0, 0, 0]); // header crc (unchecked)
        d.push(5); // header size vint
        d.push(1); // type = main
        d.push(0); // header flags
        d.push(0x03); // archive flags: volume + number present
        d.push(3); // volume number
        assert_eq!(rar5_volume_number(&d), Some(3));

        let mut first = b"Rar!\x1a\x07\x01\x00".to_vec();
        first.extend_from_slice(&[0, 0, 0, 0]);
        first.push(4);
        first.push(1);
        first.push(0);
        first.push(0x01); // volume, no explicit number => first
        assert_eq!(rar5_volume_number(&first), Some(0));

        assert_eq!(rar5_volume_number(b"Rar!\x1a\x07\x00garbage"), None); // RAR4
    }
}
