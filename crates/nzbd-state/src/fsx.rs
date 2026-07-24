//! `std::fs` wrappers that attach the path to every error.
//!
//! Nothing in this crate calls `std::fs` directly: an un-annotated
//! `io::Error` escaping to the daemon log is the bug this module exists
//! to prevent.
use super::StateError;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::Path;

pub(super) fn ctx<T>(
    r: std::io::Result<T>,
    op: &'static str,
    path: &Path,
) -> Result<T, StateError> {
    r.map_err(|source| StateError::Io {
        op,
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn create_dir_all(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::create_dir_all(p), "create directory", p)
}

pub(super) fn open(p: &Path) -> Result<File, StateError> {
    ctx(File::open(p), "open", p)
}

pub(super) fn create(p: &Path) -> Result<File, StateError> {
    ctx(File::create(p), "create", p)
}

pub(super) fn open_append(p: &Path) -> Result<File, StateError> {
    ctx(
        OpenOptions::new().create(true).append(true).open(p),
        "open for append",
        p,
    )
}

pub(super) fn read(p: &Path) -> Result<Vec<u8>, StateError> {
    ctx(std::fs::read(p), "read", p)
}

pub(super) fn write(p: &Path, data: &[u8]) -> Result<(), StateError> {
    ctx(std::fs::write(p, data), "write", p)
}

pub(super) fn read_dir(p: &Path) -> Result<std::fs::ReadDir, StateError> {
    ctx(std::fs::read_dir(p), "read directory", p)
}

pub(super) fn rename(from: &Path, to: &Path) -> Result<(), StateError> {
    ctx(std::fs::rename(from, to), "rename onto", to)
}

pub(super) fn remove_file(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::remove_file(p), "remove", p)
}

pub(super) fn remove_dir_all(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::remove_dir_all(p), "remove directory", p)
}

// Handle operations: the caller passes the path it opened, since a
// `File` does not remember where it came from.

pub(super) fn write_all(f: &mut File, buf: &[u8], p: &Path) -> Result<(), StateError> {
    ctx(f.write_all(buf), "append to", p)
}

pub(super) fn sync_data(f: &File, p: &Path) -> Result<(), StateError> {
    ctx(f.sync_data(), "fsync", p)
}

pub(super) fn set_len(f: &File, len: u64, p: &Path) -> Result<(), StateError> {
    ctx(f.set_len(len), "truncate", p)
}
