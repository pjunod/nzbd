use crate::StateError;
use nzbd_types::JobId;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Protected sidecars for source strings that must never enter queue.json.
#[derive(Clone, Debug)]
pub struct PendingSourceStore {
    root: PathBuf,
}

impl PendingSourceStore {
    pub fn open(state_dir: &Path) -> Result<Self, StateError> {
        let root = state_dir.join("torrents/pending");
        std::fs::create_dir_all(&root).map_err(|e| io("create directory", &root, e))?;
        sync_dir(&root)?;
        Ok(Self { root })
    }

    pub fn relative_ref(job: JobId) -> PathBuf {
        PathBuf::from(format!("torrents/pending/{}.source", job.0))
    }

    pub fn write(&self, job: JobId, source: &[u8]) -> Result<PathBuf, StateError> {
        let path = self.root.join(format!("{}.source", job.0));
        let tmp = self.root.join(format!(".{}.source.tmp", job.0));
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp).map_err(|e| io("create", &tmp, e))?;
        file.write_all(source).map_err(|e| io("write", &tmp, e))?;
        file.sync_all().map_err(|e| io("fsync", &tmp, e))?;
        drop(file);
        std::fs::rename(&tmp, &path).map_err(|e| io("rename", &path, e))?;
        sync_dir(&self.root)?;
        Ok(Self::relative_ref(job))
    }

    pub fn read(&self, job: JobId) -> Result<Vec<u8>, StateError> {
        let path = self.root.join(format!("{}.source", job.0));
        std::fs::read(&path).map_err(|e| io("read", &path, e))
    }

    pub fn remove(&self, job: JobId) -> Result<(), StateError> {
        let path = self.root.join(format!("{}.source", job.0));
        match std::fs::remove_file(&path) {
            Ok(()) => sync_dir(&self.root),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io("remove", &path, e)),
        }
    }

    pub fn inventory(&self) -> Result<Vec<JobId>, StateError> {
        let entries =
            std::fs::read_dir(&self.root).map_err(|e| io("read directory", &self.root, e))?;
        let mut jobs = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()?
                    .strip_suffix(".source")?
                    .parse()
                    .ok()
                    .map(JobId)
            })
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| job.0);
        Ok(jobs)
    }
}

fn sync_dir(path: &Path) -> Result<(), StateError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| io("fsync directory", path, e))
}

fn io(op: &'static str, path: &Path, source: std::io::Error) -> StateError {
    StateError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_is_private_durable_and_inventory_is_opaque() {
        let dir = tempfile::tempdir().unwrap();
        let store = PendingSourceStore::open(dir.path()).unwrap();
        let secret = b"https://user:pass@example.invalid/a.torrent?passkey=secret";
        let reference = store.write(JobId(7), secret).unwrap();
        assert_eq!(reference, PathBuf::from("torrents/pending/7.source"));
        assert_eq!(store.read(JobId(7)).unwrap(), secret);
        assert_eq!(store.inventory().unwrap(), vec![JobId(7)]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(&reference))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        store.remove(JobId(7)).unwrap();
        assert!(store.inventory().unwrap().is_empty());
    }
}
