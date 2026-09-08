//! Windows-native protection for secret files.
//!
//! `PendingSourceStore` sidecars may carry tracker credentials or URL
//! passkeys, so on Unix they are created `0600`. Windows has no mode bits:
//! this module creates the sidecar with a protected DACL containing one allow
//! ACE for the process user and no inherited entries.
//!
//! The ACL is applied through Win32 APIs, without a locale-dependent shell
//! command. It is supplied to `CreateFileW`, so a new file is private from its
//! first observable instant. It is also applied to the returned handle before
//! any secret is written: `CREATE_ALWAYS` preserves the ACL when a crash left
//! the predictable temporary file behind, and that stale file must be
//! hardened too.

#![cfg(windows)]

use std::ffi::c_void;
use std::fs::File;
use std::io::{self, ErrorKind, Result as IoResult};
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Security::Authorization::{SetSecurityInfo, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    AddAccessAllowedAce, GetLengthSid, GetTokenInformation, InitializeAcl,
    InitializeSecurityDescriptor, SetSecurityDescriptorControl, SetSecurityDescriptorDacl,
    SetSecurityDescriptorOwner, TokenUser, ACCESS_ALLOWED_ACE, ACL, ACL_REVISION,
    DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    PSID, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, CREATE_ALWAYS, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL, WRITE_DAC, WRITE_OWNER,
};
use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// An aligned Win32 output buffer. `Vec<u8>` only promises byte alignment,
/// while token information, ACLs, and security descriptors contain pointers
/// or DWORDs and must be read through their native aligned types.
fn aligned_buffer(byte_len: usize) -> Vec<usize> {
    let words = byte_len.saturating_add(size_of::<usize>() - 1) / size_of::<usize>();
    vec![0; words]
}

fn buffer_bytes(buffer: &[usize]) -> usize {
    buffer.len() * size_of::<usize>()
}

/// Keeps the token-information allocation alive for the SID pointer stored
/// inside it.
struct OwnedSid {
    _buffer: Vec<usize>,
    ptr: PSID,
    len: u32,
}

fn current_user_sid() -> IoResult<OwnedSid> {
    let mut raw_token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw_token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = unsafe { OwnedHandle::from_raw_handle(raw_token) };

    let mut needed = 0u32;
    unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            std::ptr::null_mut(),
            0,
            &mut needed,
        )
    };
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut buffer = aligned_buffer(needed as usize);
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            buffer.as_mut_ptr().cast(),
            buffer_bytes(&buffer) as u32,
            &mut needed,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }

    let ptr = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    if ptr.is_null() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "process token has no user SID",
        ));
    }
    let len = unsafe { GetLengthSid(ptr) };
    let start = buffer.as_ptr() as usize;
    let end = start + buffer_bytes(&buffer);
    let sid_start = ptr as usize;
    let Some(sid_end) = sid_start.checked_add(len as usize) else {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "process token user SID length overflowed",
        ));
    };
    if len == 0 || sid_start < start || sid_end > end {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "process token returned an invalid user SID",
        ));
    }

    Ok(OwnedSid {
        _buffer: buffer,
        ptr,
        len,
    })
}

/// Create `path` for write-only access with a DACL that permits only the
/// process user. This is the Windows equivalent of the Unix `0600` creation
/// used by `PendingSourceStore`.
pub fn open_secret_for_write(path: &Path) -> IoResult<File> {
    let user = current_user_sid()?;
    let acl_len = size_of::<ACL>()
        .checked_add(size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>())
        .and_then(|len| len.checked_add(user.len as usize))
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "user SID is too large"))?;
    let mut acl_storage = aligned_buffer(acl_len as usize);
    let acl = acl_storage.as_mut_ptr().cast::<ACL>();

    if unsafe { InitializeAcl(acl, acl_len, ACL_REVISION) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { AddAccessAllowedAce(acl, ACL_REVISION, FILE_ALL_ACCESS, user.ptr) } == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut descriptor = SECURITY_DESCRIPTOR::default();
    if unsafe {
        InitializeSecurityDescriptor(
            (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            SECURITY_DESCRIPTOR_REVISION,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if unsafe {
        SetSecurityDescriptorDacl(
            (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            1,
            acl,
            0,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // An elevated token can default a new object's owner to the Administrators
    // group even though TokenUser names the account running nzbd. Set the owner
    // explicitly so the file owner and the sole allow ACE always identify the
    // same user.
    if unsafe {
        SetSecurityDescriptorOwner(
            (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            user.ptr,
            0,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if unsafe {
        SetSecurityDescriptorControl(
            (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            SE_DACL_PROTECTED,
            SE_DACL_PROTECTED,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }

    let wide = to_wide(path)?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast::<c_void>(),
        bInheritHandle: 0,
    };
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_WRITE | WRITE_DAC | WRITE_OWNER,
            0,
            &attributes,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let file = unsafe { File::from_raw_handle(handle) };

    // Security attributes affect only a newly created file. CREATE_ALWAYS can
    // reopen a temporary file left by a crash, so restore both its owner and
    // DACL before returning it to the caller for the first secret-bearing
    // write.
    let status = unsafe {
        SetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION
                | DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
            user.ptr,
            std::ptr::null_mut(),
            acl,
            std::ptr::null(),
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }

    Ok(file)
}

fn to_wide(path: &Path) -> IoResult<Vec<u16>> {
    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Windows path contains an interior NUL",
        ));
    }
    wide.push(0);
    Ok(wide)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent_sources::PendingSourceStore;
    use nzbd_types::JobId;
    use windows_sys::Win32::Foundation::FALSE;
    use windows_sys::Win32::Security::{
        AclSizeInformation, EqualSid, GetAce, GetAclInformation, GetFileSecurityW,
        GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSecurityDescriptorOwner,
        ACL_SIZE_INFORMATION, OWNER_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;

    fn read_security_descriptor(path: &Path) -> IoResult<Vec<usize>> {
        let wide = to_wide(path)?;
        let information = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        let mut needed = 0u32;
        unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                information,
                std::ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut buffer = aligned_buffer(needed as usize);
        if unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                information,
                buffer.as_mut_ptr().cast(),
                buffer_bytes(&buffer) as u32,
                &mut needed,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(buffer)
    }

    fn assert_owner_only_acl(path: &Path) {
        let descriptor = read_security_descriptor(path).unwrap();
        let descriptor_ptr = descriptor.as_ptr() as *mut c_void;

        let mut control = 0u16;
        let mut revision = 0u32;
        assert_ne!(
            unsafe { GetSecurityDescriptorControl(descriptor_ptr, &mut control, &mut revision) },
            0,
            "security descriptor control should be readable"
        );
        assert_ne!(
            control & SE_DACL_PROTECTED,
            0,
            "DACL must be protected from inherited entries"
        );

        let mut dacl_present = FALSE;
        let mut dacl_defaulted = FALSE;
        let mut dacl = std::ptr::null_mut();
        assert_ne!(
            unsafe {
                GetSecurityDescriptorDacl(
                    descriptor_ptr,
                    &mut dacl_present,
                    &mut dacl,
                    &mut dacl_defaulted,
                )
            },
            0,
            "security descriptor DACL should be readable"
        );
        assert_ne!(dacl_present, 0, "DACL must be present");
        assert_eq!(dacl_defaulted, 0, "DACL must be explicit");
        assert!(!dacl.is_null(), "a null DACL would grant everyone access");

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        assert_ne!(
            unsafe {
                GetAclInformation(
                    dacl,
                    (&mut acl_info as *mut ACL_SIZE_INFORMATION).cast(),
                    size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            },
            0,
            "DACL metadata should be readable"
        );
        assert_eq!(acl_info.AceCount, 1, "owner-only means exactly one ACE");

        let mut raw_ace = std::ptr::null_mut();
        assert_ne!(
            unsafe { GetAce(dacl, 0, &mut raw_ace) },
            0,
            "the sole DACL ACE should be readable"
        );
        let ace = raw_ace.cast::<ACCESS_ALLOWED_ACE>();
        assert_eq!(
            unsafe { (*ace).Header.AceType },
            ACCESS_ALLOWED_ACE_TYPE as u8,
            "the sole ACE must allow access"
        );
        assert_eq!(
            unsafe { (*ace).Mask },
            FILE_ALL_ACCESS,
            "the owner must have full control"
        );

        let ace_sid = unsafe { std::ptr::addr_of_mut!((*ace).SidStart).cast::<c_void>() };
        let mut owner = std::ptr::null_mut();
        let mut owner_defaulted = FALSE;
        assert_ne!(
            unsafe { GetSecurityDescriptorOwner(descriptor_ptr, &mut owner, &mut owner_defaulted) },
            0,
            "file owner should be readable"
        );
        assert!(!owner.is_null(), "file must have an owner");
        let current_user = current_user_sid().unwrap();
        assert_ne!(
            unsafe { EqualSid(owner, current_user.ptr) },
            0,
            "the file owner must be the process user"
        );
        assert_ne!(
            unsafe { EqualSid(ace_sid, owner) },
            0,
            "the sole ACE must belong to the file owner"
        );
    }

    #[test]
    fn windows_sidecar_acl_is_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = PendingSourceStore::open(dir.path()).unwrap();
        store.write(JobId(7), b"secret-source").unwrap();

        assert_owner_only_acl(&dir.path().join("torrents/pending/7.source"));
    }

    #[test]
    fn windows_sidecar_hardens_a_stale_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = PendingSourceStore::open(dir.path()).unwrap();
        let stale = dir.path().join("torrents/pending/.9.source.tmp");
        std::fs::write(&stale, b"older-longer-secret").unwrap();

        store.write(JobId(9), b"new-secret").unwrap();

        let final_path = dir.path().join("torrents/pending/9.source");
        assert_eq!(std::fs::read(&final_path).unwrap(), b"new-secret");
        assert_owner_only_acl(&final_path);
    }
}
