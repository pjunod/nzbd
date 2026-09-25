//! Descriptor-relative filesystem operations. Payload paths never act as authority.
use super::{Error, FileEntry, Identity, Result};
use std::fs::{self, File};
use std::path::{Component, Path};

pub fn identity(meta: &fs::Metadata) -> Identity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Identity {
            device: meta.dev(),
            inode: meta.ino(),
            bytes: meta.len(),
            modified: format!("{}:{}", meta.mtime(), meta.mtime_nsec()),
            directory: meta.is_dir(),
        }
    }
    #[cfg(not(unix))]
    {
        Identity {
            device: 0,
            inode: 0,
            bytes: meta.len(),
            modified: format!("{:?}", meta.modified()),
            directory: meta.is_dir(),
        }
    }
}

pub fn absolute(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|p| matches!(p, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::Conflict(
            "an absolute path without dot components is required".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    fn name(path: &Path) -> Result<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| Error::Conflict("NUL in path".into()))
    }
    pub fn open_at(parent: &File, path: &Path, directory: bool) -> Result<File> {
        let n = name(path)?;
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | if directory {
                libc::O_DIRECTORY
            } else {
                libc::O_NONBLOCK
            };
        // SAFETY: n is NUL terminated; parent owns its live descriptor. The
        // returned descriptor is transferred exactly once to File.
        let fd = unsafe { libc::openat(parent.as_raw_fd(), n.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    pub fn open_dir(path: &Path) -> Result<File> {
        absolute(path)?;
        let mut dir = File::open("/")?;
        for c in path.components() {
            if let Component::Normal(n) = c {
                dir = open_at(&dir, Path::new(n), true)?;
            }
        }
        Ok(dir)
    }
    pub fn names(dir: &File) -> Result<Vec<std::ffi::OsString>> {
        // fdopendir owns the duplicate, never the caller's descriptor.
        let fd = unsafe { libc::dup(dir.as_raw_fd()) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            unsafe {
                libc::close(fd);
            }
            return Err(std::io::Error::last_os_error().into());
        }
        // rewind because dup shares the underlying directory offset.
        unsafe {
            libc::rewinddir(stream);
        }
        let mut out = Vec::new();
        let result = loop {
            #[cfg(target_os = "linux")]
            unsafe {
                *libc::__errno_location() = 0;
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            unsafe {
                *libc::__error() = 0;
            }
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let err = std::io::Error::last_os_error();
                break if err.raw_os_error() == Some(0) {
                    Ok(out)
                } else {
                    Err(err.into())
                };
            }
            let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if bytes != b"." && bytes != b".." {
                out.push(std::ffi::OsStr::from_bytes(bytes).to_os_string());
            }
        };
        unsafe {
            libc::closedir(stream);
        }
        result
    }
    pub fn unlink(parent: &File, path: &Path, directory: bool) -> Result<()> {
        let n = name(path)?;
        let rc = unsafe {
            libc::unlinkat(
                parent.as_raw_fd(),
                n.as_ptr(),
                if directory { libc::AT_REMOVEDIR } else { 0 },
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        parent.sync_all()?;
        Ok(())
    }
}

#[cfg(unix)]
pub use unix::{names, open_at, open_dir, unlink};

// On platforms without descriptor-relative primitives the view is available;
// mutation reports why ownership could not be proven instead of using recursion.
#[cfg(not(unix))]
pub fn open_dir(_: &Path) -> Result<File> {
    Err(Error::Conflict(
        "descriptor-relative filesystem access unavailable on this platform".into(),
    ))
}
#[cfg(not(unix))]
pub fn open_at(_: &File, _: &Path, _: bool) -> Result<File> {
    open_dir(Path::new("/"))
}
#[cfg(not(unix))]
pub fn names(_: &File) -> Result<Vec<std::ffi::OsString>> {
    Err(Error::Conflict("descriptor enumeration unavailable".into()))
}
#[cfg(not(unix))]
pub fn unlink(_: &File, _: &Path, _: bool) -> Result<()> {
    Err(Error::Conflict("descriptor deletion unavailable".into()))
}

pub fn manifest(dir: &File, limit: usize) -> Result<Vec<FileEntry>> {
    fn walk(
        dir: &File,
        prefix: &Path,
        out: &mut Vec<FileEntry>,
        limit: usize,
        depth: usize,
    ) -> Result<()> {
        if depth > 64 {
            return Err(Error::Conflict(
                "directory depth exceeds 64; inspect manually".into(),
            ));
        }
        for n in names(dir)? {
            if out.len() >= limit {
                return Err(Error::Conflict(
                    "file inventory limit reached; no changes made".into(),
                ));
            }
            let path = prefix.join(&n);
            let display = path
                .to_str()
                .ok_or_else(|| Error::Conflict("non-UTF8 filename requires manual review".into()))?
                .to_owned();
            // Opening a directory as a regular descriptor is allowed. Symlinks
            // are refused by O_NOFOLLOW, special files by the metadata check.
            let child = open_at(dir, Path::new(&n), false)?;
            let meta = child.metadata()?;
            if !meta.is_file() && !meta.is_dir() {
                return Err(Error::Conflict(format!("special file: {display}")));
            }
            out.push(FileEntry {
                path: display,
                identity: identity(&meta),
                digest: None,
            });
            if meta.is_dir() {
                walk(&child, &path, out, limit, depth + 1)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, Path::new(""), &mut out, limit, 0)?;
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub fn open_relative(root: &File, path: &str) -> Result<File> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(Error::Conflict("invalid relative file path".into()));
    }
    let mut dir = root.try_clone()?;
    let mut parts = path.components().peekable();
    while let Some(c) = parts.next() {
        dir = open_at(&dir, Path::new(c.as_os_str()), parts.peek().is_some())?;
    }
    Ok(dir)
}

pub fn remove_entry(root: &File, entry: &FileEntry) -> Result<()> {
    let path = Path::new(&entry.path);
    let parent_path = path.parent().unwrap_or(Path::new(""));
    let parent = if parent_path.as_os_str().is_empty() {
        root.try_clone()?
    } else {
        open_relative(root, parent_path.to_str().unwrap())?
    };
    let n = path
        .file_name()
        .ok_or_else(|| Error::Conflict("empty file path".into()))?;
    let child = open_at(&parent, Path::new(n), entry.identity.directory)?;
    let observed = identity(&child.metadata()?);
    if !entry.identity.same_object(&observed) || (!observed.directory && observed != entry.identity)
    {
        return Err(Error::Conflict(format!("identity changed: {}", entry.path)));
    }
    unlink(&parent, Path::new(n), entry.identity.directory)
}
