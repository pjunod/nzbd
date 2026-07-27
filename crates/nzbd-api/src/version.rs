//! What build is this, exactly?
//!
//! Field report 2026-07-27: "the version at the bottom of the main page
//! has been the same for a hundred commits." Two reasons, and the first
//! one hid the second:
//!
//! 1. The Docker build context strips `.git`, so the build script's
//!    `git` call found nothing and silently fell back to the bare Cargo
//!    version — on the one machine whose build anyone ever needs to
//!    identify. Fixed by passing `NZBD_GIT_DESCRIBE` in as a build
//!    argument, and by making a build that still has no identity *say so*
//!    (`+unknown`) instead of impersonating a release.
//! 2. Nothing ever bumped the Cargo version and nothing was ever tagged,
//!    so even a build that knew its hash led with `0.1.0`. The version is
//!    now derived from `git describe`, which moves on every commit.
//!
//! The composition lives here rather than in `build.rs` so it can be
//! tested; the build script only gathers the raw facts.

use std::sync::LazyLock;

/// Raw `git describe --tags --always --dirty` for this build (may be
/// empty — see the module docs).
const DESCRIBE: &str = env!("NZBD_GIT_DESCRIBE");

/// UTC timestamp of the compile, `YYYY-MM-DD HH:MM UTC`.
pub const BUILT: &str = env!("NZBD_BUILT");

static FULL: LazyLock<String> =
    LazyLock::new(|| compose_version(env!("CARGO_PKG_VERSION"), DESCRIBE));

/// The build identity the footer and `/api/v1/status` show.
pub fn full() -> &'static str {
    &FULL
}

/// Fold the Cargo version and `git describe` into one string that always
/// starts with the Cargo version, so a client can still parse the release
/// out of it.
///
/// | describe | result |
/// |---|---|
/// | `v0.2.0` (on the tag) | `0.2.0` |
/// | `v0.2.0-7-gabc123def` | `0.2.0-7-gabc123def` |
/// | `v0.2.0-7-gabc123def-dirty` | `0.2.0-7-gabc123def-dirty` |
/// | `abc123def` (no tags yet) | `0.2.0+gabc123def` |
/// | `v0.1.0-3-gabc123def` (tag behind Cargo) | `0.2.0+v0.1.0-3-gabc123def` |
/// | *empty* (no checkout, no build arg) | `0.2.0+unknown` |
fn compose_version(cargo: &str, describe: &str) -> String {
    let d = describe.trim();
    if d.is_empty() {
        return format!("{cargo}+unknown");
    }
    // A release tag for *this* version: use it as-is, it is already the
    // richer string (`0.2.0-7-gabc123def` beats `0.2.0+gabc123def`).
    let tagless = d.strip_prefix('v').unwrap_or(d);
    if let Some(rest) = tagless.strip_prefix(cargo) {
        if rest.is_empty() || rest.starts_with('-') {
            return tagless.to_string();
        }
    }
    // A bare hash (no tags in the repo) is the common pre-release case;
    // give it the conventional `g` so `+gabc123def` reads as a git hash.
    let core = d.strip_suffix("-dirty").unwrap_or(d);
    if !core.is_empty() && core.chars().all(|c| c.is_ascii_hexdigit()) {
        return format!("{cargo}+g{d}");
    }
    // Anything else — most likely a tag older than the Cargo version.
    // Keep both: the version we claim, and what the checkout actually was.
    format!("{cargo}+{d}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_on_a_release_tag_is_the_version() {
        assert_eq!(compose_version("0.2.0", "v0.2.0"), "0.2.0");
        assert_eq!(compose_version("0.2.0", "0.2.0"), "0.2.0");
    }

    #[test]
    fn commits_past_the_tag_move_the_version_every_commit() {
        assert_eq!(
            compose_version("0.2.0", "v0.2.0-7-gabc123def"),
            "0.2.0-7-gabc123def"
        );
        assert_eq!(
            compose_version("0.2.0", "v0.2.0-7-gabc123def-dirty"),
            "0.2.0-7-gabc123def-dirty"
        );
    }

    #[test]
    fn an_untagged_repo_still_identifies_its_commit() {
        assert_eq!(compose_version("0.2.0", "abc123def"), "0.2.0+gabc123def");
        assert_eq!(
            compose_version("0.2.0", "abc123def-dirty"),
            "0.2.0+gabc123def-dirty"
        );
    }

    /// The tag says 0.1.0 but Cargo says 0.2.0 — the crate version is what
    /// we claim to be, the describe is what we actually are. Keep both
    /// rather than picking the wrong one.
    #[test]
    fn a_tag_behind_the_crate_version_does_not_win() {
        assert_eq!(
            compose_version("0.2.0", "v0.1.0-3-gabc123def"),
            "0.2.0+v0.1.0-3-gabc123def"
        );
    }

    /// The whole point. A container built without the build argument must
    /// announce that it does not know what it is — the old behaviour was
    /// to report a bare, unchanging Cargo version, which is exactly how a
    /// hundred commits shipped under one number.
    #[test]
    fn a_build_with_no_identity_says_so() {
        assert_eq!(compose_version("0.2.0", ""), "0.2.0+unknown");
        assert_eq!(compose_version("0.2.0", "   "), "0.2.0+unknown");
    }

    /// Whatever the shape, a client can always read the release off the
    /// front of it.
    #[test]
    fn every_form_starts_with_the_crate_version() {
        for d in [
            "v0.2.0",
            "v0.2.0-7-gabc123def",
            "abc123def",
            "v0.1.0-3-gabc",
            "",
        ] {
            assert!(
                compose_version("0.2.0", d).starts_with("0.2.0"),
                "describe {d:?}"
            );
        }
    }
}
