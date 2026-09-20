use crate::error::Error;
use crate::utils::{crc, deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use tracing::info;

static MAGIC_NO_META: &[u8] = b"HQLMETA";

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub last_purged_log_id: Option<Vec<u8>>,
    pub vote: Option<Vec<u8>>,
}

impl Metadata {
    pub fn read_or_create(base_path: &str) -> Result<Self, Error> {
        let path = format!("{base_path}/meta.hql");

        if !fs::exists(&path)? {
            info!("WAL Metadata does not exist, creating new file: {}", path);
            let slf = Self {
                last_purged_log_id: None,
                vote: None,
            };
            let bytes = serialize(&slf)?;
            Self::write_unchecked(&bytes, base_path)?;
            return Ok(slf);
        }

        let Ok(bytes) = fs::read(&path) else {
            return Err(Error::InvalidPath("cannot open metadata file"));
        };
        if bytes.len() < 14 {
            return Err(Error::FileCorrupted("invalid metadata file length".into()));
        }

        debug_assert_eq!(MAGIC_NO_META.len(), 7);
        if bytes[..7].iter().as_slice() != MAGIC_NO_META {
            return Err(Error::FileCorrupted(
                "metadata file is corrupt - magic no does not match".into(),
            ));
        }
        let version = &bytes[7..8];
        match version {
            [1u8] => {
                let crc = &bytes[8..12];
                if crc != crc!(&bytes[12..]) {
                    return Err(Error::FileCorrupted(
                        "metadata CRC checksum does not match".into(),
                    ));
                }
                Ok(deserialize::<Self>(&bytes[12..])?)
            }
            _ => Err(Error::FileCorrupted("unknown metadata file version".into())),
        }
    }

    #[inline]
    pub fn write(meta: Arc<RwLock<Self>>, base_path: &str) -> Result<(), Error> {
        let slf_bytes = {
            let lock = meta.read()?;
            serialize(lock.deref())?
        };
        Self::write_unchecked(&slf_bytes, base_path)
    }

    #[inline]
    fn write_unchecked(bytes: &[u8], base_path: &str) -> Result<(), Error> {
        Self::stage_unchecked(bytes, base_path)?;
        Self::publish_staged(base_path)
    }

    #[inline]
    fn stage_unchecked(bytes: &[u8], base_path: &str) -> Result<(), Error> {
        let path = format!("{base_path}/meta.hql");
        let staged_path = format!("{path}.tmp");

        match fs::remove_file(&staged_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged_path)?;

        debug_assert_eq!(MAGIC_NO_META.len(), 7);
        file.write_all(MAGIC_NO_META)?;
        file.write_all(&[1u8])?;
        file.write_all(crc!(bytes).as_slice())?;
        file.write_all(bytes)?;
        file.sync_all()?;

        Ok(())
    }

    #[inline]
    fn publish_staged(base_path: &str) -> Result<(), Error> {
        let path = format!("{base_path}/meta.hql");
        let staged_path = format!("{path}.tmp");
        fs::rename(staged_path, path)?;

        // The file itself is durable before rename. Persist the directory entry as well on the
        // deployment platform so a power loss cannot resurrect the replaced metadata inode.
        #[cfg(target_os = "linux")]
        std::fs::File::open(base_path)?.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATH: &str = "test_data";

    #[test]
    fn metadata_write_read() -> Result<(), Error> {
        let base_path = format!("{}/metadata_write_read", PATH);
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let meta = Arc::new(RwLock::new(Metadata {
            last_purged_log_id: Some(vec![13, 17, 43]),
            vote: None,
        }));
        Metadata::write(meta.clone(), &base_path)?;

        let meta_back = Metadata::read_or_create(&base_path)?;
        let lock = meta.read()?;
        assert_eq!(lock.last_purged_log_id, meta_back.last_purged_log_id);
        assert_eq!(lock.vote, meta_back.vote);

        Ok(())
    }

    #[test]
    fn interrupted_metadata_replacement_keeps_the_previous_record_readable() -> Result<(), Error> {
        let base_path = format!(
            "{}/interrupted_metadata_replacement_keeps_the_previous_record_readable",
            PATH
        );
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let original = Arc::new(RwLock::new(Metadata {
            last_purged_log_id: Some(vec![1, 2, 3]),
            vote: Some(vec![4, 5, 6]),
        }));
        Metadata::write(original, &base_path)?;

        let replacement = Metadata {
            last_purged_log_id: Some(vec![7, 8, 9]),
            vote: Some(vec![10, 11, 12]),
        };
        Metadata::stage_unchecked(&serialize(&replacement)?, &base_path)?;

        let after_interruption = Metadata::read_or_create(&base_path)?;
        assert_eq!(after_interruption.last_purged_log_id, Some(vec![1, 2, 3]));
        assert_eq!(after_interruption.vote, Some(vec![4, 5, 6]));

        Metadata::publish_staged(&base_path)?;
        let after_publish = Metadata::read_or_create(&base_path)?;
        assert_eq!(
            after_publish.last_purged_log_id,
            replacement.last_purged_log_id
        );
        assert_eq!(after_publish.vote, replacement.vote);

        fs::remove_dir_all(base_path)?;
        Ok(())
    }
}
