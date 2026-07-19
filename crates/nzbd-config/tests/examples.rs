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
