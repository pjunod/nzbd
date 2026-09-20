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

    // Drive every rendered setting using the actual server schema, then
    // deserialize the submitted payload. A stale alias in one unrelated
    // field must not silently break every Save changes click.
    let mut cfg = nzbd_config::Config::default();
    cfg.post.failure_action = "park".into();
    let mut model = serde_json::to_value(&cfg).unwrap();
    model["torrent"] = serde_json::to_value(&cfg.torrent).unwrap();
    let submitted = tempfile::NamedTempFile::new().unwrap();
    let out = Command::new("node")
        .arg(&harness)
        .arg(&ui)
        .env("NZBD_UI_CONFIG", model.to_string())
        .env("NZBD_UI_SAVED_CONFIG_PATH", submitted.path())
        .output()
        .expect("run node harness");
    assert!(
        out.status.success(),
        "UI DOM harness failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let saved: nzbd_config::Config =
        serde_json::from_slice(&std::fs::read(submitted.path()).unwrap())
            .expect("the complete settings form must submit a valid Config");
    cfg.torrent.enabled = true;
    assert_eq!(
        saved, cfg,
        "enabling BitTorrent must preserve all other settings"
    );
}
