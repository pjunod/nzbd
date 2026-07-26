//! Build identity for the running daemon (field request 2026-07-26: the
//! UI said "v0.1.0" through every deploy — a version that never changes
//! identifies nothing, and "which build is nuc3 actually running?" burned
//! three debugging rounds in one evening).
//!
//! Emits two rustc-envs:
//!
//! * `NZBD_VERSION_FULL` — `<cargo version>` plus `+g<short-hash>` when a
//!   git checkout is visible. The Docker build context deliberately strips
//!   `.git` (context size), so container builds fall back to the bare
//!   version — the build stamp below is what distinguishes them.
//! * `NZBD_BUILT` — UTC timestamp of when this crate was last compiled.
//!   Layer-cached rebuilds that reuse the compiled crate keep their stamp,
//!   which is correct: same binary, same build.
//!
//! No new dependencies: the date math is the standard civil-from-days
//! algorithm, and git is invoked only if it exists.

use std::process::Command;

fn git_short_hash() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short=9", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let hash = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if hash.is_empty() {
        return None;
    }
    // Mark builds with uncommitted changes: a hash that lies about the
    // working tree is worse than no hash.
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    Some(if dirty { format!("{hash}-dirty") } else { hash })
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
    // Re-stamp when HEAD moves, so a local `git commit` + rebuild shows
    // the new hash without a full clean build. Harmless if absent.
    for p in ["../../.git/HEAD", "../../.git/index"] {
        if std::path::Path::new(p).exists() {
            println!("cargo:rerun-if-changed={p}");
        }
    }
    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    let full = match git_short_hash() {
        Some(h) => format!("{version}+g{h}"),
        None => version,
    };
    println!("cargo:rustc-env=NZBD_VERSION_FULL={full}");
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=NZBD_BUILT={}", utc_stamp(secs));
}
