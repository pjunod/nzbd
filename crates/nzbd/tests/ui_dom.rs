//! UI rendering-law test: drives the embedded page's renderer against a
//! fake `dom` adapter (via node + `ui_dom_harness.js`) and asserts the five
//! laws from `docs/UI_V2_PLAN.md` §3 — row identity survives ticks, a tick
//! writes only the cells that changed, a reorder moves nodes instead of
//! rebuilding them, the detail panel is a stable subtree, and no markup
//! carries inline `on*=` handlers.
//!
//! This is the regression net for field report 2026-07-25: the old renderer
//! assigned `tbody.innerHTML` once a second, so a tick landing between
//! mousedown and mouseup destroyed the pressed button and the browser never
//! fired `click` — "the first delete click does nothing", a coin flip at
//! 1 Hz. A unit test cannot see that; node identity across simulated ticks
//! can.
//!
//! Needs `node` (present on GitHub runners and most dev machines);
//! self-skips with a notice otherwise. `NZBD_REQUIRE_TOOLS` (set in CI)
//! turns the miss into a loud failure.

use std::path::Path;
use std::process::Command;

#[test]
fn ui_renderer_obeys_the_rendering_laws() {
    if Command::new("node").arg("--version").output().is_err() {
        if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
            panic!("`node` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
        }
        eprintln!("SKIP ui_renderer_obeys_the_rendering_laws: `node` not found");
        return;
    }

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let harness = manifest.join("tests/ui_dom_harness.js");
    let ui = manifest.join("../nzbd-api/ui/index.html");
    assert!(ui.exists(), "embedded UI missing at {}", ui.display());

    let out = Command::new("node")
        .arg(&harness)
        .arg(&ui)
        .output()
        .expect("run node harness");
    assert!(
        out.status.success(),
        "UI DOM harness failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
