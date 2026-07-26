//! Config durability: a copy of the last-saved configuration kept beside
//! the *data*, and the boot-time recovery that reads it back.
//!
//! # Why this exists
//!
//! `nzbd run --config /etc/nzbd/nzbd.toml` boots the first-run wizard when
//! that file is missing. The wizard writes it and the daemon reloads — and
//! in a container, a write to `/etc/nzbd` succeeds whether or not anything
//! is mounted there. When nothing is, the config lands in the container's
//! writable layer: it survives `docker restart`, and it is destroyed by
//! `docker compose up --build`, an image pull, or any other recreate. The
//! next boot finds no config and serves the wizard again, so a working
//! install silently reverts to unconfigured on every deploy, and the
//! symptom ("No configuration found") points at the config directory
//! rather than at the missing mount that actually caused it.
//!
//! The data volume does not have this problem — it is a real mount, it is
//! declared `VOLUME` in our own image, and the queue state proves it
//! persists. So every config write also drops a mirror there, and a boot
//! that finds no config file looks for that mirror before deciding this is
//! a first run. Recovery is loud, in the log and in the UI: the config
//! mount is still broken and the operator still has to fix it, but the
//! daemon comes back up *configured and downloading* in the meantime.

use crate::Config;
use std::path::{Path, PathBuf};

/// Mirror filename inside the state directory. Deliberately not
/// `nzbd.toml`: it must never be mistaken for a config the daemon was
/// pointed at, and `ls` should say what it is.
pub const MIRROR_NAME: &str = "nzbd.toml.saved";

/// Names the data/main directory when no config is readable yet — the
/// only way boot-time recovery can know where the mirror lives before it
/// has a config to read `paths.main_dir` from. Set in our Dockerfile.
pub const MAIN_DIR_ENV: &str = "NZBD_MAIN_DIR";

/// Where the mirror for a given state directory lives.
pub fn mirror_path(state_dir: &Path) -> PathBuf {
    state_dir.join(MIRROR_NAME)
}

/// Write the mirror next to the state. Best-effort by contract: the
/// caller has already written the real config and must not fail the
/// operator's save because the spare copy didn't land.
pub fn save_mirror(state_dir: &Path, toml_text: &str) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(state_dir)?;
    let path = mirror_path(state_dir);
    // Write-then-rename: a torn mirror is worse than no mirror, because
    // recovery would parse it, fail, and fall through to the wizard with
    // the good bytes already overwritten.
    let tmp = state_dir.join(format!("{MIRROR_NAME}.tmp"));
    std::fs::write(&tmp, toml_text.as_bytes())?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

/// A configuration recovered from a mirror.
#[derive(Debug, Clone)]
pub struct Recovered {
    /// The mirror the bytes came from — shown in the log and the UI so the
    /// operator can see *which* volume saved them.
    pub from: PathBuf,
    pub toml: String,
    pub config: Config,
}

/// State directories to search for a mirror, most authoritative first.
///
/// Boot-time recovery runs when there is no config to read, so the search
/// cannot be derived from `paths.main_dir` — these are the places that
/// answer "where does this install keep its data" without one:
///
/// 1. `$NZBD_MAIN_DIR` — set by our image to the declared `VOLUME`, and
///    the documented escape hatch for a non-standard layout.
/// 2. The compiled-in default `paths.main_dir` — covers every plain
///    (non-container) install that never changed it.
/// 3. `/data` — the container convention this project documents in the
///    Dockerfile, every compose example and the deploy guide.
///
/// Both `<dir>/queue` (the default `state_dir()`) and `<dir>` itself are
/// searched, so a config that set `paths.queue_dir` to the data root is
/// still found.
pub fn candidate_state_dirs() -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Some(v) = std::env::var_os(MAIN_DIR_ENV) {
        if !v.is_empty() {
            roots.push(crate::expand_home(Path::new(&v)));
        }
    }
    roots.push(crate::expand_home(&Config::default().paths.main_dir));
    roots.push(PathBuf::from("/data"));

    let mut out = Vec::new();
    for r in roots {
        for c in [r.join("queue"), r] {
            if !out.contains(&c) {
                out.push(c);
            }
        }
    }
    out
}

/// Look for a recoverable configuration. Returns the first mirror that
/// both reads and *parses* — a mirror that no longer satisfies the
/// current validator is not a config, and pretending otherwise would
/// trade the wizard for a boot loop.
pub fn find_mirror() -> Option<Recovered> {
    find_mirror_in(&candidate_state_dirs())
}

/// [`find_mirror`] over an explicit search path (the seam the tests use —
/// process-wide env and absolute paths are not a fixture).
pub fn find_mirror_in(dirs: &[PathBuf]) -> Option<Recovered> {
    for dir in dirs {
        let path = mirror_path(dir);
        let Ok(toml) = std::fs::read_to_string(&path) else {
            continue;
        };
        match Config::from_toml(&toml) {
            Ok(config) => {
                return Some(Recovered {
                    from: path,
                    toml,
                    config,
                })
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "found a saved configuration but it no longer parses; ignoring it"
                );
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Is the config directory actually persistent?
// ---------------------------------------------------------------------------

/// Whether writes to the config directory survive the container being
/// recreated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// A normal filesystem, or a real volume/bind mount: writes persist.
    Persistent,
    /// Inside a container, on the image's writable layer: every write is
    /// destroyed by the next `docker compose up`/pull/recreate.
    Ephemeral,
    /// Not a Linux `/proc` system, or the mount table was unreadable.
    Unknown,
}

/// Classify the directory a config file lives in.
///
/// Only a container can be `Ephemeral`: on a host, the root filesystem is
/// exactly as durable as anything else, and warning about it would be
/// noise.
pub fn durability(config_path: &Path) -> Durability {
    let Some(dir) = config_path.parent() else {
        return Durability::Unknown;
    };
    if !in_container() {
        return Durability::Persistent;
    }
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return Durability::Unknown;
    };
    match longest_mount_prefix(&mountinfo, dir) {
        // Only the container's own root filesystem covers this path — no
        // volume, no bind mount. Whatever we write here dies with the
        // container.
        Some(m) if m == Path::new("/") => Durability::Ephemeral,
        Some(_) => Durability::Persistent,
        None => Durability::Unknown,
    }
}

/// The mount point covering `dir`: the longest mount-point path that is a
/// prefix of it. `/proc/self/mountinfo` field 5 is the mount point, and
/// later lines shadow earlier ones at the same path, so a plain scan
/// keeping the longest match is correct.
pub fn longest_mount_prefix(mountinfo: &str, dir: &Path) -> Option<PathBuf> {
    let mut best: Option<PathBuf> = None;
    for line in mountinfo.lines() {
        let Some(field) = line.split_whitespace().nth(4) else {
            continue;
        };
        let mp = PathBuf::from(unescape_mountinfo(field));
        if dir.starts_with(&mp) {
            let longer = best
                .as_ref()
                .map(|b| mp.as_os_str().len() >= b.as_os_str().len())
                .unwrap_or(true);
            if longer {
                best = Some(mp);
            }
        }
    }
    best
}

/// mountinfo octal-escapes the characters that would otherwise break its
/// space-separated fields.
fn unescape_mountinfo(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 4], 8) {
                out.push(v as char);
                i += 4;
                continue;
            }
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

/// Best-effort "are we in a container?" — advisory, for wording and for
/// [`durability`]. Covers docker (`/.dockerenv`), podman
/// (`/run/.containerenv`) and kubernetes (service env / cgroup paths).
pub fn in_container() -> bool {
    Path::new("/.dockerenv").exists()
        || Path::new("/run/.containerenv").exists()
        || std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
        || std::fs::read_to_string("/proc/1/cgroup")
            .map(|s| s.contains("docker") || s.contains("kubepods") || s.contains("containerd"))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
23 28 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
24 28 0:22 / /sys rw,nosuid,nodev,noexec,relatime - sysfs sysfs rw
28 27 0:35 / / rw,relatime - overlay overlay rw,lowerdir=/x,upperdir=/y
41 28 259:1 /var/lib/docker/volumes/nzbd_data/_data /data rw,relatime - ext4 /dev/nvme0n1p1 rw
42 28 259:1 /srv/my\\040config /etc/nzbd rw,relatime - ext4 /dev/nvme0n1p1 rw
";

    #[test]
    fn mount_prefix_picks_the_longest_match() {
        // A real bind mount at the config dir.
        assert_eq!(
            longest_mount_prefix(SAMPLE, Path::new("/etc/nzbd")),
            Some(PathBuf::from("/etc/nzbd"))
        );
        // The data volume covers its children.
        assert_eq!(
            longest_mount_prefix(SAMPLE, Path::new("/data/queue")),
            Some(PathBuf::from("/data"))
        );
        // Nothing mounted here: only the container root covers it. This
        // is the case the whole module exists for.
        assert_eq!(
            longest_mount_prefix(SAMPLE, Path::new("/etc/other")),
            Some(PathBuf::from("/"))
        );
    }

    #[test]
    fn mountinfo_octal_escapes_are_decoded() {
        // Field 4 (the *source*) holds the escape here; decoding must not
        // shift which field we read as the mount point.
        assert_eq!(unescape_mountinfo("/srv/my\\040config"), "/srv/my config");
        assert_eq!(unescape_mountinfo("/plain"), "/plain");
    }

    #[test]
    fn mirror_round_trips_through_the_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("queue");
        let toml = "[paths]\nmain_dir = \"/data\"\ndest_dir = \"/data/complete\"\n";

        let written = save_mirror(&state, toml).unwrap();
        assert_eq!(written, state.join(MIRROR_NAME));
        // No .tmp left behind — the rename consumed it.
        assert!(!state.join(format!("{MIRROR_NAME}.tmp")).exists());

        let rec = find_mirror_in(std::slice::from_ref(&state)).expect("mirror is found");
        assert_eq!(rec.from, written);
        assert_eq!(rec.config.paths.main_dir, PathBuf::from("/data"));
    }

    #[test]
    fn search_order_is_honoured_and_misses_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let empty = tmp.path().join("empty");
        let first = tmp.path().join("first");
        let second = tmp.path().join("second");
        save_mirror(&first, "[paths]\nmain_dir = \"/first\"\n").unwrap();
        save_mirror(&second, "[paths]\nmain_dir = \"/second\"\n").unwrap();

        let rec = find_mirror_in(&[empty, first, second]).unwrap();
        assert_eq!(rec.config.paths.main_dir, PathBuf::from("/first"));
    }

    #[test]
    fn an_unparseable_mirror_is_ignored_not_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let bad = tmp.path().join("bad");
        let good = tmp.path().join("good");
        save_mirror(&bad, "this is not toml {{{").unwrap();
        save_mirror(&good, "[paths]\nmain_dir = \"/good\"\n").unwrap();

        let rec = find_mirror_in(&[bad, good]).expect("falls through to the good one");
        assert_eq!(rec.config.paths.main_dir, PathBuf::from("/good"));
        assert!(find_mirror_in(&[tmp.path().join("nothing")]).is_none());
    }

    #[test]
    fn candidates_cover_the_data_volume_and_prefer_the_env() {
        // Not using set_var (process-wide, races other tests): assert the
        // shape that does not depend on it.
        let c = candidate_state_dirs();
        assert!(c.contains(&PathBuf::from("/data/queue")));
        assert!(c.contains(&PathBuf::from("/data")));
        let dflt = crate::expand_home(&Config::default().paths.main_dir).join("queue");
        assert!(c.contains(&dflt));
        // Deduplicated, and `<dir>/queue` is tried before `<dir>`.
        let mut seen = c.clone();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), c.len());
        let qi = c.iter().position(|p| p == &PathBuf::from("/data/queue"));
        let di = c.iter().position(|p| p == &PathBuf::from("/data"));
        assert!(qi < di);
    }

    #[test]
    fn a_host_config_dir_is_never_reported_ephemeral() {
        // On a developer machine `in_container()` is false, so this is the
        // Persistent arm; in CI-in-docker it is whatever the mount table
        // says. Either way `Ephemeral` requires a container.
        let d = durability(Path::new("/etc/nzbd/nzbd.toml"));
        if !in_container() {
            assert_eq!(d, Durability::Persistent);
        }
    }
}
