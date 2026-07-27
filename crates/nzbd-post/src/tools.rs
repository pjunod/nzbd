//! Hardened subprocess runner + the par2/unrar/7z tool wrappers
//! (ARCHITECTURE.md §9): argv-only, scrubbed env, timeout, output caps,
//! kill-on-drop.

use crate::{ArchiveKind, ExtractOutcome, PostError, RepairResult, VerifyResult};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;

pub const OUTPUT_CAP: usize = 256 * 1024;

#[derive(Debug)]
pub struct ToolOutput {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Run a tool with hardening. `PostError::ToolMissing` on spawn failure.
pub async fn run_tool(
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    timeout: Duration,
) -> Result<ToolOutput, PostError> {
    let mut child = tokio::process::Command::new(cmd)
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .env(
            "PATH",
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| PostError::ToolMissing(format!("{cmd}: {e}")))?;

    let out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let read_capped = |mut r: tokio::process::ChildStdout| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match r.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if buf.len() < OUTPUT_CAP {
                        buf.extend_from_slice(&chunk[..n.min(OUTPUT_CAP - buf.len())]);
                    }
                }
            }
        }
        buf
    };
    let read_err = async {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match err.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if buf.len() < OUTPUT_CAP {
                        buf.extend_from_slice(&chunk[..n.min(OUTPUT_CAP - buf.len())]);
                    }
                }
            }
        }
        buf
    };

    let result = tokio::time::timeout(timeout, async {
        let (o, e, status) = tokio::join!(read_capped(out), read_err, child.wait());
        (o, e, status)
    })
    .await;

    match result {
        Ok((o, e, Ok(status))) => Ok(ToolOutput {
            code: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&o).into_owned(),
            stderr: String::from_utf8_lossy(&e).into_owned(),
        }),
        Ok((_, _, Err(e))) => Err(PostError::Subprocess(format!("{cmd}: {e}"))),
        Err(_) => {
            let _ = child.kill().await;
            Err(PostError::Subprocess(format!("{cmd}: timed out")))
        }
    }
}

// ---------------------------------------------------------------------------
// par2 (subprocess side; native quick-verify lives in par2.rs)
// ---------------------------------------------------------------------------

pub struct Par2Tool {
    pub cmd: String,
    pub timeout: Duration,
}

impl Par2Tool {
    /// Full verification via the subprocess (the fallback when quick
    /// verification reports damage). Parses par2cmdline's plain output.
    pub async fn verify_full(&self, main_par2: &Path) -> Result<VerifyResult, PostError> {
        let dir = main_par2.parent().unwrap_or(Path::new("."));
        let name = main_par2.file_name().unwrap().to_string_lossy();
        let out = run_tool(&self.cmd, &["verify", "-q", &name], dir, self.timeout).await?;
        let text = format!("{}\n{}", out.stdout, out.stderr);
        Ok(parse_verify_output(&text, out.code))
    }

    pub async fn repair(&self, main_par2: &Path) -> Result<RepairResult, PostError> {
        let dir = main_par2.parent().unwrap_or(Path::new("."));
        let name = main_par2.file_name().unwrap().to_string_lossy();
        let out = run_tool(&self.cmd, &["repair", "-q", &name], dir, self.timeout).await?;
        let text = format!("{}\n{}", out.stdout, out.stderr);
        if out.code == 0
            && (text.contains("Repair complete")
                || text.contains("repair is not required")
                || text.contains("All files are correct"))
        {
            Ok(RepairResult::Repaired)
        } else {
            tracing::warn!(code = out.code, "par2 repair failed: {}", text.trim());
            Ok(RepairResult::Failed)
        }
    }
}

pub fn parse_verify_output(text: &str, code: i32) -> VerifyResult {
    let grab = |needle: &str| -> Option<u32> {
        for line in text.lines() {
            if let Some(rest) = line.trim().strip_prefix("You ") {
                if rest.contains(needle) {
                    return rest.split_whitespace().find_map(|w| w.parse().ok());
                }
            }
        }
        None
    };
    if code == 0
        || text.contains("All files are correct")
        || text.contains("repair is not required")
    {
        return VerifyResult::Intact;
    }
    let available = grab("recovery blocks available").unwrap_or(0);
    if text.contains("Repair is possible") {
        return VerifyResult::Repairable {
            blocks_available: available,
            blocks_needed: 0,
        };
    }
    if let Some(needed) = grab("more recovery blocks") {
        return VerifyResult::NeedMoreBlocks {
            blocks_needed: needed,
        };
    }
    if text.contains("Repair is not possible") {
        // "not possible" without a block count = data beyond any repair
        return VerifyResult::Unrepairable;
    }
    VerifyResult::Unrepairable
}

// ---------------------------------------------------------------------------
// unrar / 7z extraction
// ---------------------------------------------------------------------------

pub struct Extractors {
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    pub timeout: Duration,
}

/// The volume of a multi-volume set this file is, if it is one.
///
/// `name.part07.rar` → 7, `name.r05` → 6 (old-style counts the bare `.rar` as
/// volume 1 and `.r00` as volume 2), `name.rar` → 1. Anything else → None.
fn volume_number(name: &str) -> Option<u32> {
    if let Some(idx) = name.rfind(".part") {
        if name.ends_with(".rar") {
            let digits: String = name[idx + 5..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            return digits.parse().ok();
        }
    }
    if name.ends_with(".rar") {
        return Some(1);
    }
    let ext = name.rsplit_once('.')?.1;
    if ext.len() == 3 && ext.starts_with('r') && ext[1..].bytes().all(|b| b.is_ascii_digit()) {
        return ext[1..].parse::<u32>().ok().map(|n| n + 2);
    }
    None
}

/// The volumes missing from a set, by number.
///
/// Extraction has no completeness check of its own: unrar is handed a first
/// volume and trusted to walk the chain, and when the chain stops it writes
/// what it had. Checking before we start turns "a truncated film reported as
/// finished" into a named failure — and names the volume, because "volume 43
/// is missing" is actionable where "unpack failed" is not.
pub fn missing_volumes(dir: &Path, first: &Path) -> Vec<u32> {
    let stem = first
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();
    // The set's base name: strip .partNN.rar, .rNN or .rar.
    let base = match stem.rfind(".part") {
        Some(i) if stem.ends_with(".rar") => stem[..i].to_owned(),
        _ => match stem.rsplit_once('.') {
            Some((b, _)) => b.to_owned(),
            None => stem.clone(),
        },
    };
    let mut seen: Vec<u32> = Vec::new();
    for p in files_of(dir) {
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        let matches_set = match name.rfind(".part") {
            Some(i) if name.ends_with(".rar") => name[..i] == base,
            _ => name.rsplit_once('.').is_some_and(|(b, _)| b == base),
        };
        if !matches_set {
            continue;
        }
        if let Some(n) = volume_number(&name) {
            seen.push(n);
        }
    }
    // One volume is not a set, and a set of one cannot have a gap.
    if seen.len() < 2 {
        return Vec::new();
    }
    seen.sort_unstable();
    seen.dedup();
    let highest = *seen.last().unwrap_or(&1);
    (1..=highest).filter(|n| !seen.contains(n)).collect()
}

/// Files (not directories) directly in `dir`.
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

/// Every file belonging to the same volume set as `first`, and their total
/// size on disk.
fn volume_set(dir: &Path, first: &Path) -> (usize, u64) {
    let stem = first
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();
    let base = match stem.rfind(".part") {
        Some(i) if stem.ends_with(".rar") => stem[..i].to_owned(),
        _ => match stem.rsplit_once('.') {
            Some((b, _)) => b.to_owned(),
            None => stem.clone(),
        },
    };
    let mut count = 0usize;
    let mut bytes = 0u64;
    for p in files_of(dir) {
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        let in_set = match name.rfind(".part") {
            Some(i) if name.ends_with(".rar") => name[..i] == base,
            _ => name.rsplit_once('.').is_some_and(|(b, _)| b == base),
        };
        if in_set && volume_number(&name).is_some() {
            count += 1;
            bytes += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
    }
    (count, bytes)
}

/// Total size of everything under `dir`, recursively.
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                tree_bytes(&p)
            } else {
                std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

/// Did the extraction actually consume the whole volume set?
///
/// Returns the shortfall message when it plainly did not.
///
/// Extractors lie. `unrar-free` — the GPL clone Debian installs when you ask
/// for `unrar`, and what this image shipped until it was caught — cannot do
/// multi-volume archives and does not say so: given volume 1 of a five-volume
/// set with every volume present, it extracts volume 1, prints "All OK" and
/// exits 0. Nothing downstream could tell that from a real success, so a
/// 48 GiB film arrived as its first 500 MiB, marked complete.
///
/// The check is deliberately arithmetic rather than a parse of the archive's
/// declared contents: it needs no listing pass, no per-format output format,
/// and it holds for any extractor. Decompressed output is never meaningfully
/// smaller than the compressed input it came from, so output far below the
/// bytes fed in means volumes went unread. Stored media archives — what
/// nearly every release is — land within a hair of 1.0.
///
/// The 90% floor is slack for archive headers and recovery records, not a
/// tolerance for missing data: the failure this catches is off by whole
/// volumes, so anything near the boundary is already something else.
pub fn unpack_shortfall(dir: &Path, first: &Path, dest: &Path) -> Option<String> {
    let (volumes, packed) = volume_set(dir, first);
    if volumes < 2 || packed == 0 {
        return None; // not a set; nothing to be short of
    }
    let extracted = tree_bytes(dest);
    let floor = packed / 10 * 9;
    if extracted >= floor {
        return None;
    }
    Some(format!(
        "extracted {extracted} bytes from a {volumes}-volume set totalling {packed} \
         bytes — the extractor stopped early (an extractor without multi-volume \
         support reports success after the first volume; check `post.unrar_cmd`)"
    ))
}

/// First-volume archives in a directory, extraction-ordered.
pub fn detect_archives(dir: &Path) -> Vec<(PathBuf, ArchiveKind)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut names: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    names.sort();
    for p in names {
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        if name.ends_with(".rar") {
            // Multi-volume: only the first (partN with N==1, or bare .rar).
            if let Some(idx) = name.rfind(".part") {
                let num: String = name[idx + 5..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if num.parse::<u32>().map(|n| n != 1).unwrap_or(false) {
                    continue;
                }
            }
            out.push((p, ArchiveKind::Rar));
        } else if name.ends_with(".7z") {
            out.push((p, ArchiveKind::SevenZip));
        } else if name.ends_with(".zip") {
            out.push((p, ArchiveKind::Zip));
        } else if name.ends_with(".001") {
            out.push((p, ArchiveKind::Split));
        }
    }
    out
}

impl Extractors {
    pub async fn extract(
        &self,
        archive: &Path,
        kind: ArchiveKind,
        dest: &Path,
        password: Option<&str>,
    ) -> Result<ExtractOutcome, PostError> {
        std::fs::create_dir_all(dest)?;
        let dir = archive.parent().unwrap_or(Path::new("."));
        // RAR, then 7-Zip if RAR did not deliver.
        //
        // The fallback is not belt-and-braces, it is the working path on any
        // image that has `unrar-free` instead of the real thing: that clone
        // extracts volume 1 of a multi-volume set, prints "All OK" and exits
        // 0. 7-Zip reads the same archive correctly. Rather than demand a
        // particular binary, try the configured one and check the result — a
        // tool that under-delivers gets overruled by one that does not.
        if matches!(kind, ArchiveKind::Rar) {
            let first = self.extract_once(archive, kind, dest, password).await;
            let usable = match &first {
                Ok(o) => o.success && unpack_shortfall(dir, archive, dest).is_none(),
                Err(_) => false,
            };
            if usable {
                return first;
            }
            // Password failures are about the credential, not the tool, and
            // retrying with another extractor cannot help.
            if let Ok(o) = &first {
                if o.password_error {
                    return first;
                }
            }
            tracing::warn!(
                archive = %archive.display(),
                "unrar did not deliver the whole archive; retrying with 7-Zip"
            );
            let _ = std::fs::remove_dir_all(dest);
            std::fs::create_dir_all(dest)?;
            let second = self
                .extract_once(archive, ArchiveKind::SevenZip, dest, password)
                .await;
            return match second {
                Ok(o) if o.success => Ok(o),
                // Neither worked: report the first attempt's outcome, which
                // is the one whose password/disk flags mean something.
                other => first.or(other),
            };
        }
        self.extract_once(archive, kind, dest, password).await
    }

    async fn extract_once(
        &self,
        archive: &Path,
        kind: ArchiveKind,
        dest: &Path,
        password: Option<&str>,
    ) -> Result<ExtractOutcome, PostError> {
        std::fs::create_dir_all(dest)?;
        let dir = archive.parent().unwrap_or(Path::new("."));
        let arch = archive.to_string_lossy().into_owned();
        let dest_s = dest.to_string_lossy().into_owned();
        match kind {
            ArchiveKind::Rar => {
                let pw = match password {
                    Some(p) => format!("-p{p}"),
                    None => "-p-".into(),
                };
                let dest_slash = format!("{dest_s}/");
                // `-idc` silences the copyright banner but KEEPS the result
                // line. `-idq` suppressed everything, so the diagnostics below
                // could never fire: the exit code was the only evidence, and
                // unrar can stop at a missing volume having written a whole
                // one — a success by that measure and a truncated film by any
                // other.
                let args = [
                    "x",
                    "-y",
                    "-o+",
                    "-idc",
                    pw.as_str(),
                    arch.as_str(),
                    dest_slash.as_str(),
                ];
                let out = run_tool(&self.unrar_cmd, &args, dir, self.timeout).await?;
                let all = format!("{}\n{}", out.stdout, out.stderr);
                // NZBGet's unrar exit-code map: 11 = wrong password,
                // 5 = write/disk error, 0 = success.
                //
                // Exit 0 is necessary and not sufficient. unrar must also say
                // "All OK", exactly as 7z must say "Everything is Ok" in the
                // arm below — a rule this code enforced for one extractor and
                // not the other, which is the whole difference between
                // noticing a broken volume chain and shipping one volume.
                let broke_the_chain = ["Cannot find volume", "You need to start extraction"]
                    .iter()
                    .any(|m| all.contains(m));
                Ok(ExtractOutcome {
                    success: out.code == 0 && all.contains("All OK") && !broke_the_chain,
                    password_error: out.code == 11
                        || out.stderr.contains("password")
                        || out.stdout.contains("password is incorrect"),
                    disk_space_error: out.code == 5,
                })
            }
            ArchiveKind::SevenZip | ArchiveKind::Zip | ArchiveKind::Split => {
                let pw = format!("-p{}", password.unwrap_or(""));
                let dest_flag = format!("-o{dest_s}");
                let args = ["x", "-y", pw.as_str(), dest_flag.as_str(), arch.as_str()];
                let out = run_tool(&self.sevenzip_cmd, &args, dir, self.timeout).await?;
                let all = format!("{}\n{}", out.stdout, out.stderr);
                // 7z requires the literal success line (nzbget does the same).
                Ok(ExtractOutcome {
                    success: out.code == 0 && all.contains("Everything is Ok"),
                    password_error: all.contains("Wrong password"),
                    disk_space_error: all.contains("There is not enough space"),
                })
            }
        }
    }
}

/// Test-only probe for an external tool. Returns `false` — and the calling
/// test self-skips with a notice — when the binary is missing, so a laptop
/// without `par2`/`7z` still passes the suite. Setting `NZBD_REQUIRE_TOOLS`
/// (CI does) turns a missing tool into a loud failure instead, so CI can
/// never silently lose coverage.
#[cfg(test)]
pub(crate) fn require_tool(tool: &str) -> bool {
    let found = std::process::Command::new(tool)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok();
    if found {
        return true;
    }
    if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
        panic!("`{tool}` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
    }
    eprintln!(
        "SKIPPED: `{tool}` not installed — `brew install par2 p7zip` / `apt-get install par2 p7zip-full` for full local coverage"
    );
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_output_parsing() {
        assert_eq!(
            parse_verify_output("All files are correct, repair is not needed.", 0),
            VerifyResult::Intact
        );
        let dmg = "Repair is required.\nYou have 8 recovery blocks available.\nRepair is possible.";
        assert_eq!(
            parse_verify_output(dmg, 1),
            VerifyResult::Repairable {
                blocks_available: 8,
                blocks_needed: 0
            }
        );
        let need = "Repair is required.\nYou have 0 recovery blocks available.\nYou need 3 more recovery blocks to be able to repair these files.";
        assert_eq!(
            parse_verify_output(need, 1),
            VerifyResult::NeedMoreBlocks { blocks_needed: 3 }
        );
    }

    #[test]
    fn archive_detection_first_volumes_only() {
        let tmp = tempfile::tempdir().unwrap();
        for n in [
            "a.part1.rar",
            "a.part2.rar",
            "b.rar",
            "c.zip",
            "d.7z",
            "e.001",
            "e.002",
            "x.txt",
        ] {
            std::fs::write(tmp.path().join(n), b"x").unwrap();
        }
        let found = detect_archives(tmp.path());
        let names: Vec<String> = found
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["a.part1.rar", "b.rar", "c.zip", "d.7z", "e.001"]
        );
    }

    #[tokio::test]
    async fn sevenzip_zip_roundtrip_and_wrong_password() {
        if !require_tool("7z") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("in");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("hello.txt"), b"hello world").unwrap();

        // plain zip (archive root = the file itself, like release archives)
        let st = std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "../plain.zip", "hello.txt"])
            .current_dir(&src)
            .output()
            .expect("7z required for tests (apt-get install p7zip-full)");
        assert!(st.status.success());

        // passworded zip
        let st = std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "-psecret", "../locked.zip", "hello.txt"])
            .current_dir(&src)
            .output()
            .unwrap();
        assert!(st.status.success());

        let ex = Extractors {
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            timeout: Duration::from_secs(30),
        };
        let out_dir = tmp.path().join("out");
        let r = ex
            .extract(
                &tmp.path().join("plain.zip"),
                ArchiveKind::Zip,
                &out_dir,
                None,
            )
            .await
            .unwrap();
        assert!(r.success);
        assert_eq!(
            std::fs::read(out_dir.join("hello.txt")).unwrap(),
            b"hello world"
        );

        let r = ex
            .extract(
                &tmp.path().join("locked.zip"),
                ArchiveKind::Zip,
                &tmp.path().join("out2"),
                Some("wrong"),
            )
            .await
            .unwrap();
        assert!(!r.success);
        assert!(r.password_error, "wrong password must be detected");

        let r = ex
            .extract(
                &tmp.path().join("locked.zip"),
                ArchiveKind::Zip,
                &tmp.path().join("out3"),
                Some("secret"),
            )
            .await
            .unwrap();
        assert!(r.success);
    }

    #[tokio::test]
    async fn missing_tool_is_tool_missing() {
        let err = run_tool(
            "definitely-not-a-tool-xyz",
            &[],
            Path::new("/tmp"),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PostError::ToolMissing(_)));
    }
}
