//! Windows-native protection for secret files.
//!
//! `PendingSourceStore` sidecars may carry tracker credentials or URL
//! passkeys, so on Unix they are created `0600`. Windows has no mode bits:
//! a file created with the inherited directory DACL is world-readable on a
//! multi-user machine. This module gives the sidecar the Windows equivalent
//! of `0600` — a DACL that permits only the current user (the owner) full
//! control, and grants no one else any right.
//!
//! The ACL is applied directly through the Win32 security APIs. No shell
//! command (`icacls`, `takeown`, …) is invoked: a `cmd.exe` round trip would
//! depend on the process locale for path and SID handling, and would hand a
//! credentials-bearing path to an external process for nothing.
//!
//! The file is *created* with the security descriptor (`CreateFileW` +
//! `SECURITY_ATTRIBUTES`), so it never exists in a world-readable state and
//! we never need a follow-up `SetFileSecurity` call that would require the
//! `WRITE_DAC` right we may not hold. `std::fs::rename` (a same-directory
//! `MoveFileEx`) preserves the descriptor, so the final `<job>.source` keeps
//! the owner-only DACL after `PendingSourceStore::write` moves the temp file.

#![cfg(windows)]

use std::io::{self, ErrorKind, Result as IoResult};
use std::os::raw::c_void;
use std::path::Path;

/// `SECURITY_ATTRIBUTES` passed to `CreateFileW`. `#[repr(C)]` keeps the
/// documented layout: `nLength`, `lpSecurityDescriptor`, `bInheritHandle`.
#[repr(C)]
struct SecurityAttributes {
    n_length: u32,
    lp_security_descriptor: *mut c_void,
    b_inherit_handle: i32,
}

/// `TOKEN_USER`: `DWORD Length; PSID Sid;`. `#[repr(C)]` matches the
/// documented layout on both 32- and 64-bit Windows, so the SID pointer is
/// read by name rather than by a hard-coded byte offset.
#[repr(C)]
struct TokenUser {
    length: u32,
    sid: *const u8,
}

/// Self-relative `SECURITY_DESCRIPTOR` header (the in-file layout), read
/// back for the regression test. The four trailing fields are 32-bit
/// *offsets* from the start of the descriptor, not pointers. The in-file
/// order is `Owner, Group, Sacl, Dacl`, matching `SECURITY_DESCRIPTOR_RELATIVE`.
#[cfg(test)]
#[repr(C)]
struct RelSecurityDescriptor {
    revision: u8,
    sbz1: u8,
    control: u16,
    owner: u32,
    group: u32,
    sacl: u32,
    dacl: u32,
}

/// Self-relative `ACL` header. `first_ace` is a 32-bit offset *from the start
/// of the ACL*, not a pointer and not an offset from the descriptor.
#[cfg(test)]
#[repr(C)]
struct RelAcl {
    revision: u8,
    sbz1: u8,
    ace_count: u16,
    acl_size: u32,
    first_ace: u32,
}

/// Self-relative `ACCESS_ALLOWED_ACE` header plus `Mask` and the 32-bit
/// `SidStart` offset, which is measured *from the start of the descriptor*.
#[cfg(test)]
#[repr(C)]
struct RelAce {
    ace_type: u8,
    ace_flags: u8,
    ace_size: u16,
    mask: u32,
    sid_start: u32,
}

// Sizes of the self-relative headers above. They are the offsets we rely on
// when laying the buffer out and parsing it back, so keep them honest.
const SD_HEADER_SIZE: usize = 20; // 1+1+2 + 4*4 (Owner, Group, Sacl, Dacl)
const ACL_HEADER_SIZE: usize = 12; // 1+1+2 + 4 + 4
const ACE_HEADER_SIZE: usize = 12; // 1+1+2 + 4 + 4

// Access masks.
const FILE_ALL_ACCESS: u32 = 0x001F01FF;

// CreateFileW constants.
const GENERIC_WRITE: u32 = 0x40000000;
const CREATE_ALWAYS: u32 = 4;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x80;

// advapi32 constants.
const TOKEN_QUERY: u32 = 0x0008;
const TOKEN_USER: u32 = 1;

#[link(name = "kernel32")]
extern "system" {
    fn CreateFileW(
        lp_file_name: *const u16,
        dw_desired_access: u32,
        dw_share_mode: u32,
        lp_security_attributes: *const SecurityAttributes,
        dw_creation_disposition: u32,
        dw_flags_and_attributes: u32,
        h_template_file: *mut c_void,
    ) -> *mut c_void;
    fn GetCurrentProcess() -> *mut c_void;
    fn CloseHandle(h: *mut c_void) -> i32;
}

#[link(name = "advapi32")]
extern "system" {
    fn OpenProcessToken(
        h_process: *mut c_void,
        dw_desired_access: u32,
        token: *mut *mut c_void,
    ) -> i32;
    fn GetTokenInformation(
        token_handle: *mut c_void,
        token_information_class: u32,
        token_information: *mut c_void,
        token_information_length: u32,
        return_length: *mut u32,
    ) -> i32;
    fn GetLengthSid(sid: *const c_void) -> u32;
    fn InitializeSecurityDescriptor(p_sd: *mut c_void, dw_revision: u32) -> i32;
    fn InitializeAcl(p_acl: *mut c_void, n_acl_length: u32, n_ace_count: u32) -> i32;
    fn AddAccessAllowedAce(
        p_acl: *mut c_void,
        dw_acl_revision: u32,
        dw_access_mask: u32,
        psid: *const c_void,
    ) -> i32;
    fn SetSecurityDescriptorDacl(
        p_sd: *mut c_void,
        b_inherited: i32,
        b_dacl_present: i32,
        p_dacl: *mut c_void,
    ) -> i32;
}

fn io_err() -> io::Error {
    io::Error::last_os_error()
}

/// Create `path` for write-only access so that its DACL permits only the
/// current user. This is the Windows stand-in for the Unix `0600` that
/// `PendingSourceStore` applies on creation.
pub fn open_secret_for_write(path: &Path) -> IoResult<std::fs::File> {
    use std::os::windows::io::FromRawHandle;

    let sd = owner_only_security_descriptor()?;
    let wide = to_wide(path)?;
    let sa = SecurityAttributes {
        n_length: std::mem::size_of::<SecurityAttributes>() as u32,
        lp_security_descriptor: sd.as_ptr() as *mut c_void,
        b_inherit_handle: 0,
    };
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_WRITE,
            0,
            &sa,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if handle as isize == -1 {
        // `last_os_error()` captures the error from the failed call.
        return Err(io_err());
    }
    // `CreateFileW` returns a `HANDLE` (a pointer); `File` wants the
    // integer `RawHandle`. `File` takes ownership, so we never
    // `CloseHandle` it ourselves.
    let raw = handle as std::os::windows::io::RawHandle;
    Ok(unsafe { std::fs::File::from_raw_handle(raw) })
}

/// Build a self-contained, self-relative security descriptor whose DACL has
/// exactly one `ACCESS_ALLOWED` ACE for the current user with full control.
fn owner_only_security_descriptor() -> IoResult<Vec<u8>> {
    // Own the SID bytes for the whole call so the ACE-builder reads valid
    // memory; never hand `AddAccessAllowedAce` a pointer into a buffer we
    // are about to drop.
    let sid = current_user_sid()?;
    let sid_len = sid.len();

    let acl_total = ACL_HEADER_SIZE + ACE_HEADER_SIZE + sid_len;
    let total = SD_HEADER_SIZE + acl_total;
    let mut buf = vec![0u8; total];
    let sd = buf.as_mut_ptr() as *mut c_void;
    // The DACL region is the slice of the buffer that follows the SD header.
    // Derive its pointer from a safe slice instead of raw arithmetic so the
    // code is correct regardless of toolchain pointer-arithmetic rules.
    let acl = buf[SD_HEADER_SIZE..].as_mut_ptr() as *mut c_void;

    unsafe {
        if InitializeSecurityDescriptor(sd, 1) == 0 {
            return Err(io_err());
        }
        if InitializeAcl(acl, acl_total as u32, 1) == 0 {
            return Err(io_err());
        }
        if AddAccessAllowedAce(acl, 2, FILE_ALL_ACCESS, sid.as_ptr() as *const c_void) == 0 {
            return Err(io_err());
        }
        // DACL present, not inherited.
        if SetSecurityDescriptorDacl(sd, 0, 1, acl) == 0 {
            return Err(io_err());
        }
    }
    Ok(buf)
}

/// The current user's SID, copied into an owned buffer so the caller owns the
/// bytes. We never return a pointer into a buffer this function owns, which
/// would dangle the moment it drops.
fn current_user_sid() -> IoResult<Vec<u8>> {
    unsafe {
        let process = GetCurrentProcess();
        let mut token: *mut c_void = std::ptr::null_mut();
        if OpenProcessToken(process, TOKEN_QUERY, &mut token) == 0 {
            return Err(io_err());
        }
        let result = (|| {
            let mut len = 0u32;
            // Probe the required length with a null buffer.
            let _ = GetTokenInformation(token, TOKEN_USER, std::ptr::null_mut(), 0, &mut len);
            if len == 0 {
                return Err(io::Error::new(ErrorKind::Other, "token user size is zero"));
            }
            let mut buf = vec![0u8; len as usize];
            if GetTokenInformation(
                token,
                TOKEN_USER,
                buf.as_mut_ptr() as *mut c_void,
                len,
                &mut len,
            ) == 0
            {
                return Err(io_err());
            }
            // `buf` is 1-aligned but `TokenUser` holds a pointer (8-aligned);
            // read it unaligned instead of forming a reference through a
            // misaligned pointer.
            let user = std::ptr::read_unaligned(buf.as_ptr() as *const TokenUser);
            let sid = user.sid;
            if sid.is_null() {
                return Err(io::Error::new(ErrorKind::Other, "token user has no SID"));
            }
            let sid_len = GetLengthSid(sid as *const c_void);
            // Copy the SID out of `buf` so it outlives the buffer.
            Ok(std::slice::from_raw_parts(sid, sid_len as usize).to_vec())
        })();
        CloseHandle(token);
        result
    }
}

fn to_wide(path: &Path) -> IoResult<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    Ok(path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent_sources::PendingSourceStore;
    use nzbd_types::JobId;

    // Test-only: the masks we assert are present in the owner ACE, the
    // security-information class for reading the DACL back, and the two
    // advapi32 entry points used only here.
    const FILE_GENERIC_READ: u32 = 0x00120089;
    const FILE_GENERIC_WRITE: u32 = 0x00120116;
    const DACL_SECURITY_INFORMATION: u32 = 0x4;

    #[link(name = "advapi32")]
    extern "system" {
        fn GetFileSecurityW(
            lp_file_name: *const u16,
            security_information: u32,
            p_security_descriptor: *mut c_void,
            n_length: u32,
            lp_n_length: *mut u32,
        ) -> i32;
        fn EqualSid(sid1: *const c_void, sid2: *const c_void) -> i32;
    }

    /// Read back the file's self-relative security descriptor (DACL portion).
    fn read_security_descriptor(path: &Path) -> IoResult<Vec<u8>> {
        let wide = to_wide(path)?;
        unsafe {
            let mut needed = 0u32;
            // First call: probe the required size. `GetFileSecurityW`
            // returns `FALSE` with `ERROR_INSUFFICIENT_BUFFER` on a small
            // buffer.
            let _ = GetFileSecurityW(
                wide.as_ptr(),
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                0,
                &mut needed,
            );
            let mut buf = vec![0u8; needed as usize];
            if GetFileSecurityW(
                wide.as_ptr(),
                DACL_SECURITY_INFORMATION,
                buf.as_mut_ptr() as *mut c_void,
                buf.len() as u32,
                &mut needed,
            ) == 0
            {
                return Err(io_err());
            }
            Ok(buf)
        }
    }

    #[test]
    fn windows_sidecar_acl_is_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = PendingSourceStore::open(dir.path()).unwrap();
        store.write(JobId(7), b"secret-source").unwrap();
        let path = dir.path().join("torrents/pending/7.source");

        let sd = read_security_descriptor(&path).unwrap();
        let rel_sd = unsafe { &*(sd.as_ptr() as *const RelSecurityDescriptor) };
        assert_eq!(
            rel_sd.dacl, SD_HEADER_SIZE as u32,
            "DACL must be present at the expected offset"
        );

        // The DACL region begins at the descriptor's `dacl` offset.
        let acl_region = &sd[rel_sd.dacl as usize..];
        let acl = unsafe { &*(acl_region.as_ptr() as *const RelAcl) };
        assert_eq!(acl.ace_count, 1, "owner-only means exactly one ACE");

        // `first_ace` is an offset from the start of the ACL.
        let ace_region = &acl_region[acl.first_ace as usize..];
        let ace = unsafe { &*(ace_region.as_ptr() as *const RelAce) };
        assert_eq!(ace.ace_type, 0, "the single ACE must be ACCESS_ALLOWED");
        assert!(
            ace.mask & FILE_GENERIC_READ != 0,
            "owner must be able to read"
        );
        assert!(
            ace.mask & FILE_GENERIC_WRITE != 0,
            "owner must be able to write"
        );

        // `sid_start` is an offset from the start of the descriptor.
        let sid_bytes = &sd[ace.sid_start as usize..];
        let mine = current_user_sid().unwrap();
        let same = unsafe {
            EqualSid(
                sid_bytes.as_ptr() as *const c_void,
                mine.as_ptr() as *const c_void,
            )
        };
        assert_eq!(
            same, 1,
            "the only ACE must be for the current user (owner-only)"
        );
    }
}
