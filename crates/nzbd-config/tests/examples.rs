//! The example configs shipped in the repo must always parse through the
//! real validator (`deny_unknown_fields` means doc rot fails loudly here
//! instead of silently confusing a user).

use std::path::Path;

#[test]
fn shipped_example_configs_parse() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for rel in [
        "examples/docker-compose/nzbd.toml.example",
        "dev/nzbd.toml.example",
    ] {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        nzbd_config::Config::from_toml(&text)
            .unwrap_or_else(|e| panic!("{rel} does not parse: {e}"));
    }
}

/// The shipped Compose recipes must keep the config mount **writable**.
///
/// Regression (2026-07-25): `examples/docker-compose` delivered the config
/// through a Compose `configs:` entry — which Docker always mounts
/// read-only — so every save in the settings editor failed with
/// `Read-only file system (os error 30)`. The default deployment silently
/// disabled a headline feature. Deliberate read-only shapes (Kubernetes
/// ConfigMaps) are fine; the Docker happy path is not one of them.
#[test]
fn shipped_compose_files_mount_the_config_writable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for rel in [
        "examples/docker-compose/docker-compose.yml",
        "dev/docker-compose.yml",
    ] {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

        // Only look at real YAML, not the explanatory comments.
        let code: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n");

        let mount = code
            .lines()
            .find(|l| l.contains(":/etc/nzbd"))
            .unwrap_or_else(|| panic!("{rel}: no /etc/nzbd mount at all"));
        assert!(
            !mount.trim_end().ends_with(":ro"),
            "{rel}: config mounted read-only, the settings editor cannot save:\n  {mount}"
        );
        assert!(
            !mount.contains("nzbd.toml:/etc/nzbd"),
            "{rel}: mount the config DIRECTORY, not the file — Docker turns a \
             missing source file into a directory and the daemon won't boot:\n  {mount}"
        );
        assert!(
            !code.contains("configs:"),
            "{rel}: Compose `configs:` entries are always read-only; use a \
             read-write bind mount of the config directory instead"
        );
    }
}
