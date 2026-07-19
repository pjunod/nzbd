//! UI boot smoke test: executes the embedded page's inline JavaScript
//! against a minimal DOM shim (via node + `ui_boot_harness.js`) and fails
//! on uncaught errors, unhandled rejections, `$("id")` lookups with no
//! matching element, or a boot that never starts the SSE/refresh
//! plumbing. Syntax checks are not enough — this is the net for
//! "the script parses but dies at load" regressions (a refactor once
//! deleted `connectSse()` while a later block still called it; the page
//! rendered but never live-updated).
//!
//! Needs `node` (present on GitHub runners and most dev machines);
//! self-skips with a notice otherwise. `NZBD_REQUIRE_TOOLS` (set in CI)
//! turns the miss into a loud failure.

use std::path::Path;
use std::process::Command;

#[test]
fn ui_script_boots_without_errors() {
    if Command::new("node").arg("--version").output().is_err() {
        if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
            panic!("`node` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
        }
        eprintln!("SKIP ui_script_boots_without_errors: `node` not found");
        return;
    }

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let harness = manifest.join("tests/ui_boot_harness.js");
    let ui = manifest.join("../nzbd-api/ui/index.html");
    assert!(ui.exists(), "embedded UI missing at {}", ui.display());

    let out = Command::new("node")
        .arg(&harness)
        .arg(&ui)
        .output()
        .expect("run node harness");
    assert!(
        out.status.success(),
        "UI boot harness failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
