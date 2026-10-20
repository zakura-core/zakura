//! Narrow Unix operations used by zcashd-compat supervision and preflight.

use std::io;

#[cfg(target_os = "linux")]
use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt, path::Path};

/// Signals a single process, or probes its existence with signal zero.
#[allow(unsafe_code)]
pub(super) fn signal_process(pid: u32, signal: libc::c_int) -> io::Result<()> {
    let pid = libc::pid_t::try_from(pid)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid process id"))?;

    // SAFETY: `kill` takes no pointers. A checked, positive PID targets only
    // the requested process, never a process group.
    if unsafe { libc::kill(pid, signal) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Checks permissions using the real user/group IDs, matching `access(2)`.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
pub(super) fn check_access(path: &Path, mode: libc::c_int) -> io::Result<()> {
    let path = c_path(path)?;
    // SAFETY: `path` is a live, NUL-terminated string. The syscall reads it
    // synchronously and retains no pointer.
    if unsafe { libc::access(path.as_ptr(), mode) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Returns total provisioned filesystem bytes, saturating on overflow.
#[cfg(target_os = "linux")]
#[allow(unsafe_code, clippy::unnecessary_cast)]
pub(super) fn filesystem_size(path: &Path) -> io::Result<u64> {
    let path = c_path(path)?;
    let mut stats = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: The path is NUL-terminated and `stats` points to writable space
    // for a `statvfs`. The syscall retains neither pointer.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: A successful `statvfs` initializes the returned structure.
    let stats = unsafe { stats.assume_init() };

    // These unsigned filesystem counters are at most 64 bits on Linux.
    Ok((stats.f_blocks as u64).saturating_mul(stats.f_frsize as u64))
}

#[cfg(target_os = "linux")]
fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_probe_and_invalid_pids() {
        signal_process(std::process::id(), 0).expect("the test process exists");
        for pid in [0, u32::MAX] {
            assert_eq!(
                signal_process(pid, libc::SIGTERM).unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn filesystem_checks_preserve_non_utf8_paths_and_os_errors() {
        use std::{ffi::OsStr, fs};

        let temp = tempfile::tempdir().expect("the test can create a temporary directory");
        let path = temp.path().join(OsStr::from_bytes(b"directory-\xff"));
        fs::create_dir(&path).expect("Linux supports non-UTF-8 paths");

        check_access(&path, libc::R_OK | libc::W_OK | libc::X_OK)
            .expect("the new directory is accessible to its owner");
        assert!(filesystem_size(&path).unwrap() > 0);

        let missing = path.join("missing");
        assert_eq!(
            check_access(&missing, libc::R_OK)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOENT),
        );
        assert_eq!(
            filesystem_size(&missing).unwrap_err().raw_os_error(),
            Some(libc::ENOENT),
        );

        let mut nul_path = path.as_os_str().as_bytes().to_vec();
        nul_path.extend_from_slice(b"\0ignored");
        let nul_path = Path::new(OsStr::from_bytes(&nul_path));
        assert_eq!(
            check_access(nul_path, libc::R_OK)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EINVAL),
        );
        assert_eq!(
            filesystem_size(nul_path).unwrap_err().raw_os_error(),
            Some(libc::EINVAL),
        );
    }
}
