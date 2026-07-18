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
        let arch = archive.to_string_lossy().into_owned();
        let dest_s = dest.to_string_lossy().into_owned();
        match kind {
            ArchiveKind::Rar => {
                let pw = match password {
                    Some(p) => format!("-p{p}"),
                    None => "-p-".into(),
                };
                let dest_slash = format!("{dest_s}/");
                let args = [
                    "x",
                    "-y",
                    "-o+",
                    "-idq",
                    pw.as_str(),
                    arch.as_str(),
                    dest_slash.as_str(),
                ];
                let out = run_tool(&self.unrar_cmd, &args, dir, self.timeout).await?;
                // NZBGet's unrar exit-code map: 11 = wrong password,
                // 5 = write/disk error, 0 = success.
                Ok(ExtractOutcome {
                    success: out.code == 0,
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
