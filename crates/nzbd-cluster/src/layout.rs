//! Shared-volume layout and atomic JSON file helpers (CLUSTERING.md §3).
//!
//! Everything cluster-visible lives under `<shared_dir>/.nzbd-cluster`:
//!
//! ```text
//! .nzbd-cluster/leader.json        election lease {epoch, node, api_url, seq}
//! .nzbd-cluster/nodes/<name>.json  node presence + capabilities + stats
//! .nzbd-cluster/queue.json         queue-authority snapshot (engine-owned)
//! .nzbd-cluster/jobs/<id>/…        per-job fenced journals (engine-owned)
//! ```
//!
//! All writes are unique-tmp + rename — atomic on POSIX filesystems
//! including Gluster FUSE mounts.

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct SharedLayout {
    root: PathBuf,
    /// Uniquifies tmp names across nodes sharing the volume.
    tag: String,
}

impl SharedLayout {
    pub fn new(shared_dir: &Path, node_tag: &str) -> std::io::Result<SharedLayout> {
        let root = shared_dir.join(".nzbd-cluster");
        std::fs::create_dir_all(root.join("nodes"))?;
        Ok(SharedLayout {
            root,
            tag: node_tag.to_string(),
        })
    }

    /// Engine state (queue.json + jobs/) shares the cluster root.
    pub fn state_dir(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn leader_file(&self) -> PathBuf {
        self.root.join("leader.json")
    }

    pub fn node_file(&self, node: &str) -> PathBuf {
        self.root.join("nodes").join(format!("{node}.json"))
    }

    pub fn nodes_dir(&self) -> PathBuf {
        self.root.join("nodes")
    }

    /// Atomic write: unique tmp (per node + counter) + rename.
    pub fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> std::io::Result<()> {
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp-{}-{}-{}", self.tag, std::process::id(), n));
        let bytes = serde_json::to_vec(value)?;
        std::fs::write(&tmp, &bytes)?;
        // fsync the tmp so the rename never exposes a hole after a crash.
        if let Ok(f) = std::fs::File::open(&tmp) {
            let _ = f.sync_data();
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// `None` on missing or corrupt (a torn read during someone's rename
    /// window is indistinguishable from corruption — callers retry on the
    /// next poll).
    pub fn read_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
        let bytes = std::fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_roundtrip_and_corrupt_tolerance() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "t").unwrap();
        let p = layout.leader_file();

        assert_eq!(SharedLayout::read_json::<serde_json::Value>(&p), None);
        layout
            .write_json(&p, &serde_json::json!({"epoch": 3}))
            .unwrap();
        let v: serde_json::Value = SharedLayout::read_json(&p).unwrap();
        assert_eq!(v["epoch"], 3);

        std::fs::write(&p, b"{torn").unwrap();
        assert_eq!(SharedLayout::read_json::<serde_json::Value>(&p), None);
    }
}
