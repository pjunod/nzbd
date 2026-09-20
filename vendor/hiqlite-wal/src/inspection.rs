//! Read-only WAL inspection for stopped-node diagnostics.
//!
//! This module never starts a writer, creates a WAL, or publishes metadata.
//! It deliberately exposes only Raft identities and physical boundaries; log
//! payloads remain private to the application state machine.

use crate::error::Error;
use crate::lockfile::LockFile;
use crate::log_store_impl::deserialize as deserialize_legacy;
use crate::metadata::Metadata;
use crate::utils::{CHKSUM, deserialize as deserialize_standard};
use crate::wal::WalFile;
use openraft::{LogId, Vote};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const METADATA_MAGIC: &[u8] = b"HQLMETA";
const MAX_INSPECTABLE_WAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct InspectedLogId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

impl From<LogId<u64>> for InspectedLogId {
    fn from(value: LogId<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            index: value.index,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct InspectedVote {
    pub term: u64,
    pub node_id: u64,
    pub committed: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct MetadataInspection {
    pub present: bool,
    pub format_version: Option<u8>,
    pub crc_valid: bool,
    pub last_purged_log_id_present: bool,
    pub last_purged_log_id: Option<InspectedLogId>,
    pub vote: Option<InspectedVote>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WalFileInspection {
    pub file_name: String,
    pub wal_number: u64,
    pub byte_size: u64,
    pub header_first_index: u64,
    pub header_last_index: u64,
    pub first_decodable_log_id: Option<InspectedLogId>,
    pub last_decodable_log_id: Option<InspectedLogId>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WalInspection {
    pub metadata: MetadataInspection,
    pub wal_files: Vec<WalFileInspection>,
    pub invariant_verdicts: Vec<String>,
    pub observations: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WalLockState {
    Missing,
    UnlockedSentinel,
    Locked,
}

/// Probe the actual advisory lock. A sentinel's presence alone is never
/// treated as ownership.
pub fn inspect_lock(base_path: &Path) -> Result<WalLockState, Error> {
    let base = base_path
        .to_str()
        .ok_or(Error::InvalidPath("WAL path is not valid UTF-8"))?;
    if !LockFile::exists(base)? {
        return Ok(WalLockState::Missing);
    }
    if LockFile::is_locked(base)? {
        Ok(WalLockState::Locked)
    } else {
        Ok(WalLockState::UnlockedSentinel)
    }
}

/// Decode only the leading state-machine `last_applied_log_id` field. The
/// remaining SQLite metadata contains membership addresses and is never
/// returned by the diagnostic surface.
pub fn decode_state_machine_last_applied(bytes: &[u8]) -> Result<Option<InspectedLogId>, Error> {
    let value: Option<LogId<u64>> = deserialize_legacy(bytes)?;
    Ok(value.map(Into::into))
}

/// Inspect a stopped Hiqlite `logs/` directory without changing it.
pub fn inspect_logs_dir(base_path: &Path) -> Result<WalInspection, Error> {
    if inspect_lock(base_path)? == WalLockState::Locked {
        return Err(Error::Locked(
            "refusing to inspect a live-locked WAL directory",
        ));
    }

    let metadata = inspect_metadata(base_path)?;
    let mut paths = fs::read_dir(base_path)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| path.extension().is_some_and(|extension| extension == "wal"));
    paths.sort();

    let mut wal_files = Vec::with_capacity(paths.len());
    let mut verdicts = Vec::new();
    let mut observations = Vec::new();
    let mut previous: Option<(u64, u64)> = None;
    for path in paths {
        let file_metadata = fs::symlink_metadata(&path)?;
        if !file_metadata.file_type().is_file() {
            return Err(Error::FileCorrupted(
                "WAL candidate is not a regular file".into(),
            ));
        }
        if file_metadata.len() > MAX_INSPECTABLE_WAL_BYTES {
            return Err(Error::FileCorrupted(
                "WAL candidate exceeds the inspection size ceiling".into(),
            ));
        }
        let wal = WalFile::read_from_file(path_string(&path)?)?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::InvalidPath("WAL filename is not valid UTF-8"))?
            .to_owned();
        if let Some((previous_number, previous_last)) = previous {
            if wal.wal_no != previous_number.saturating_add(1) {
                push_once(&mut verdicts, "wal_file_gap");
            }
            if wal.data_start.is_some() && wal.id_from != previous_last.saturating_add(1) {
                push_once(&mut verdicts, "retained_gap");
            }
        }

        let (first, last) = inspect_file_boundaries(&wal)?;
        if first
            .as_ref()
            .is_some_and(|value| value.index != wal.id_from)
            || last
                .as_ref()
                .is_some_and(|value| value.index != wal.id_until)
        {
            push_once(&mut verdicts, "wal_header_payload_mismatch");
        }
        previous = Some((wal.wal_no, wal.id_until));
        wal_files.push(WalFileInspection {
            file_name,
            wal_number: wal.wal_no,
            byte_size: fs::metadata(&path)?.len(),
            header_first_index: wal.id_from,
            header_last_index: wal.id_until,
            first_decodable_log_id: first,
            last_decodable_log_id: last,
        });
    }

    if wal_files.is_empty() {
        push_once(&mut verdicts, "wal_missing");
    }
    if !metadata.present || !metadata.crc_valid {
        push_once(&mut verdicts, "metadata_corrupt");
    }
    if let (Some(purged), Some(first)) = (
        metadata.last_purged_log_id,
        wal_files
            .iter()
            .find_map(|file| file.first_decodable_log_id),
    ) {
        if first.index > purged.index.saturating_add(1) {
            push_once(&mut verdicts, "metadata_behind_wal");
        } else if first.index < purged.index {
            push_once(&mut verdicts, "metadata_ahead_of_wal");
        }
    }
    if metadata.last_purged_log_id.is_none()
        && wal_files
            .iter()
            .find_map(|file| file.first_decodable_log_id)
            .is_some_and(|first| first.index > 2)
    {
        push_once(&mut verdicts, "missing_purge_boundary");
    }
    if wal_files.len() == 1
        && wal_files[0].wal_number == 1
        && wal_files[0]
            .first_decodable_log_id
            .is_some_and(|first| first.index > 2)
    {
        observations.push("wal_number_one_reused_above_initial_range".to_owned());
    }
    if verdicts.is_empty() {
        verdicts.push("clean".to_owned());
    }

    Ok(WalInspection {
        metadata,
        wal_files,
        invariant_verdicts: verdicts,
        observations,
    })
}

fn inspect_metadata(base_path: &Path) -> Result<MetadataInspection, Error> {
    let path = base_path.join("meta.hql");
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MetadataInspection {
                present: false,
                format_version: None,
                crc_valid: false,
                last_purged_log_id_present: false,
                last_purged_log_id: None,
                vote: None,
            });
        }
        Err(error) => return Err(error.into()),
    };
    if bytes.len() < 12 || &bytes[..7] != METADATA_MAGIC {
        return Ok(MetadataInspection {
            present: true,
            format_version: bytes.get(7).copied(),
            crc_valid: false,
            last_purged_log_id_present: false,
            last_purged_log_id: None,
            vote: None,
        });
    }
    let format_version = bytes[7];
    let crc_valid = bytes[8..12] == CHKSUM.checksum(&bytes[12..]).to_le_bytes();
    if format_version != 1 || !crc_valid {
        return Ok(MetadataInspection {
            present: true,
            format_version: Some(format_version),
            crc_valid,
            last_purged_log_id_present: false,
            last_purged_log_id: None,
            vote: None,
        });
    }
    let metadata: Metadata = deserialize_standard(&bytes[12..])?;
    let last_purged_log_id = metadata
        .last_purged_log_id
        .as_deref()
        .map(deserialize_legacy::<LogId<u64>>)
        .transpose()?
        .map(Into::into);
    let vote = metadata
        .vote
        .as_deref()
        .map(deserialize_legacy::<Vote<u64>>)
        .transpose()?
        .map(|value| InspectedVote {
            term: value.leader_id().term,
            node_id: value.leader_id().node_id,
            committed: value.is_committed(),
        });
    Ok(MetadataInspection {
        present: true,
        format_version: Some(format_version),
        crc_valid,
        last_purged_log_id_present: metadata.last_purged_log_id.is_some(),
        last_purged_log_id,
        vote,
    })
}

fn inspect_file_boundaries(
    wal: &WalFile,
) -> Result<(Option<InspectedLogId>, Option<InspectedLogId>), Error> {
    let (Some(data_start), Some(data_end)) = (wal.data_start, wal.data_end) else {
        if wal.data_start.is_some() != wal.data_end.is_some() {
            return Err(Error::FileCorrupted(
                "WAL has only one populated data boundary".into(),
            ));
        }
        return Ok((None, None));
    };
    let bytes = fs::read(&wal.path)?;
    let start = data_start as usize;
    let end = data_end as usize;
    if start < 32 || start >= end || end > bytes.len() {
        return Err(Error::FileCorrupted("invalid WAL data boundaries".into()));
    }

    let mut offset = start;
    let mut previous_log_id: Option<u64> = None;
    let mut first_payload = None;
    let last_payload = loop {
        let header_end = offset
            .checked_add(16)
            .filter(|header_end| *header_end <= end)
            .ok_or_else(|| Error::FileCorrupted("truncated WAL record header".into()))?;
        let mut id_bytes = [0_u8; 8];
        id_bytes.copy_from_slice(&bytes[offset..offset + 8]);
        let record_log_id = u64::from_be_bytes(id_bytes);
        let mut length_bytes = [0_u8; 4];
        length_bytes.copy_from_slice(&bytes[offset + 12..header_end]);
        let data_length = u32::from_be_bytes(length_bytes) as usize;
        if data_length == 0 {
            return Err(Error::FileCorrupted("zero-length WAL record".into()));
        }
        let record_end = header_end
            .checked_add(data_length)
            .filter(|record_end| *record_end <= end)
            .ok_or_else(|| Error::FileCorrupted("WAL record exceeds its data boundary".into()))?;
        let payload = &bytes[header_end..record_end];
        if bytes[offset + 8..offset + 12] != CHKSUM.checksum(payload).to_le_bytes() {
            return Err(Error::Integrity("Invalid CRC for WAL Record".into()));
        }
        if previous_log_id.is_some_and(|previous| record_log_id != previous.saturating_add(1)) {
            return Err(Error::FileCorrupted(
                "WAL records are not logically contiguous".into(),
            ));
        }
        if first_payload.is_none() {
            first_payload = Some(payload);
        }
        previous_log_id = Some(record_log_id);

        if record_end == end {
            break payload;
        }
        offset = record_end
            .checked_add(1)
            .filter(|offset| *offset < end)
            .ok_or_else(|| Error::FileCorrupted("invalid WAL record separator".into()))?;
    };

    let first = deserialize_legacy::<LogId<u64>>(
        first_payload.ok_or_else(|| Error::FileCorrupted("WAL has no first payload".into()))?,
    )?
    .into();
    let last = deserialize_legacy::<LogId<u64>>(last_payload)?.into();
    Ok((Some(first), Some(last)))
}

fn path_string(path: &Path) -> Result<String, Error> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(Error::InvalidPath("WAL path is not valid UTF-8"))
}

fn push_once(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::serialize as serialize_standard;
    use crate::wal::WalFile;
    use openraft::LeaderId;
    use std::io::Write;

    #[test]
    fn stopped_directory_reports_only_boundaries() -> Result<(), Error> {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-wal-inspection-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;

        let purged = LogId::new(LeaderId::new(7, 3), 9_999);
        let metadata = Metadata {
            last_purged_log_id: Some(crate::log_store_impl::serialize(&purged)?),
            vote: None,
        };
        let payload = serialize_standard(&metadata)?;
        let mut file = fs::File::create(root.join("meta.hql"))?;
        file.write_all(METADATA_MAGIC)?;
        file.write_all(&[1])?;
        file.write_all(&CHKSUM.checksum(&payload).to_le_bytes())?;
        file.write_all(&payload)?;

        let mut buffer = Vec::new();
        let mut wal = WalFile::new(1, root.to_str().unwrap(), 0, 0, 8 * 1024)?;
        wal.create_file(&mut buffer)?;
        wal.mmap_mut()?;
        let retained = LogId::new(LeaderId::new(7, 3), 10_000);
        buffer.clear();
        wal.append_log(
            retained.index,
            &crate::log_store_impl::serialize(&retained)?,
            &mut buffer,
        )?;
        buffer.clear();
        wal.update_header(&mut buffer)?;
        wal.flush()?;
        drop(wal);

        let report = inspect_logs_dir(&root)?;
        assert_eq!(report.invariant_verdicts, ["clean"]);
        assert_eq!(report.metadata.last_purged_log_id, Some(purged.into()));
        assert_eq!(
            report.wal_files[0].first_decodable_log_id,
            Some(retained.into())
        );
        assert_eq!(
            report.observations,
            ["wal_number_one_reused_above_initial_range"]
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn live_locked_directory_is_refused_before_any_file_is_read() -> Result<(), Error> {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-wal-inspection-lock-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        let lock = LockFile::create(root.to_str().unwrap())?;
        lock.lock()?;

        let error = inspect_logs_dir(&root).expect_err("a live WAL must not be inspected");
        assert!(matches!(error, Error::Locked(_)));

        drop(lock);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn mismatched_header_is_a_verdict_instead_of_a_reader_panic() -> Result<(), Error> {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-wal-inspection-mismatch-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;

        let mut buffer = Vec::new();
        let mut wal = WalFile::new(1, root.to_str().unwrap(), 0, 0, 8 * 1024)?;
        wal.create_file(&mut buffer)?;
        wal.mmap_mut()?;
        let retained = LogId::new(LeaderId::new(7, 3), 10_000);
        buffer.clear();
        wal.append_log(
            retained.index,
            &crate::log_store_impl::serialize(&retained)?,
            &mut buffer,
        )?;
        wal.id_from = retained.index + 1;
        wal.id_until = retained.index + 1;
        buffer.clear();
        wal.update_header(&mut buffer)?;
        wal.flush()?;
        drop(wal);

        let report = inspect_logs_dir(&root)?;
        assert!(
            report
                .invariant_verdicts
                .iter()
                .any(|verdict| verdict == "wal_header_payload_mismatch")
        );
        assert_eq!(
            report.wal_files[0].first_decodable_log_id,
            Some(retained.into())
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
