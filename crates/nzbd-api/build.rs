//! Build identity for the running daemon.
//!
//! This script only *gathers* facts; the `version` module turns them into
//! the string the footer shows, because a pure function is testable and a
//! build script is not.
//!
//! Emits two rustc-envs:
//!
//! * `NZBD_GIT_DESCRIBE` — `git describe --tags --always --dirty`, or the
//!   `NZBD_GIT_DESCRIBE` build environment variable when no checkout is
//!   visible, or empty. **Empty is the interesting case**: the Docker
//!   build context strips `.git` (context size), so an image built without
//!   `--build-arg NZBD_GIT_DESCRIBE=…` has no idea what it is, and the
//!   composed version says `+unknown` out loud rather than quietly
//!   reporting the same bare Cargo version every deploy for a hundred
//!   commits (field report 2026-07-27).
//! * `NZBD_BUILT` — UTC timestamp of when this crate was last compiled.
//!   Layer-cached rebuilds that reuse the compiled crate keep their stamp,
//!   which is correct: same binary, same build.
//!
//! No new dependencies: the date math is the standard civil-from-days
//! algorithm, and git is invoked only if it exists.

use std::process::Command;

/// `git describe` against the checkout being compiled, if there is one.
///
/// `--always` means a repository with no tags still answers (a bare short
/// hash); `--dirty` means a hash never lies about the working tree;
/// `--match=v[0-9]*` keeps a stray non-release tag out of the version.
fn git_describe() -> Option<String> {
    let out = Command::new("git")
        .args([
            "describe",
            "--tags",
            "--always",
            "--dirty",
            "--abbrev=9",
            "--match=v[0-9]*",
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let d = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!d.is_empty()).then_some(d)
}

/// Unix seconds → `YYYY-MM-DD HH:MM UTC` (civil-from-days; Hinnant).
fn utc_stamp(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hh, mm) = (rem / 3600, (rem % 3600) / 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02} UTC")
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Re-stamp when HEAD, the index or the tags move, so a local commit or
    // `git tag` + rebuild shows the new identity without a clean build.
    for p in [
        "../../.git/HEAD",
        "../../.git/index",
        "../../.git/refs/tags",
        "../../.git/packed-refs",
    ] {
        if std::path::Path::new(p).exists() {
            println!("cargo:rerun-if-changed={p}");
        }
    }
    println!("cargo:rerun-if-env-changed=NZBD_GIT_DESCRIBE");

    // A visible checkout is the truth. The build argument is the fallback
    // for container builds, which by design cannot see one.
    let describe = git_describe()
        .or_else(|| std::env::var("NZBD_GIT_DESCRIBE").ok())
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
        .unwrap_or_default();
    println!("cargo:rustc-env=NZBD_GIT_DESCRIBE={describe}");

    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=NZBD_BUILT={}", utc_stamp(secs));
}
