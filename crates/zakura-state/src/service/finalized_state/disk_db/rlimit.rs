//! Raises the process open-file limit before opening the database.

use std::io;

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
use libc::{getrlimit, rlimit as Rlimit, setrlimit};
#[cfg(any(target_os = "linux", target_os = "android"))]
use libc::{getrlimit64 as getrlimit, rlimit64 as Rlimit, setrlimit64 as setrlimit};

/// Raises the soft open-file limit up to `requested`, preserving the hard limit.
///
/// An already sufficient soft limit is unchanged. Increases are capped by the
/// hard limit and, on Apple and FreeBSD-like systems, `kern.maxfilesperproc`.
/// System-call errors propagate to the database's existing fallback policy.
#[cfg(unix)]
pub(super) fn increase_nofile_limit(requested: u64) -> io::Result<u64> {
    let mut limits = open_file_limits()?;
    // Unix resource-limit types are unsigned integers of at most 64 bits.
    let soft = limits.rlim_cur as u64;
    // Unix resource-limit types are unsigned integers of at most 64 bits.
    let hard = limits.rlim_max as u64;
    if soft >= hard {
        return Ok(hard);
    }
    if soft >= requested {
        return Ok(soft);
    }

    let requested = requested.min(hard);
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "dragonfly"
    ))]
    let requested = requested.min(kernel_open_file_limit()?);

    limits.rlim_cur = requested
        .try_into()
        .expect("the requested limit is bounded by the same type's hard limit");
    set_open_file_limits(&limits)?;
    Ok(requested)
}

/// Windows file and socket limits are separate from Unix resource limits.
#[cfg(not(unix))]
pub(super) fn increase_nofile_limit(requested: u64) -> io::Result<u64> {
    Ok(requested)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn open_file_limits() -> io::Result<Rlimit> {
    let mut limits = Rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limits` is a valid, writable resource-limit struct. The syscall
    // writes it synchronously and retains no pointer.
    if unsafe { getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(limits)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn set_open_file_limits(limits: &Rlimit) -> io::Result<()> {
    // SAFETY: `limits` points to an initialized resource-limit struct. The
    // syscall reads it synchronously and retains no pointer.
    if unsafe { setrlimit(libc::RLIMIT_NOFILE, limits) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly"
))]
#[allow(unsafe_code)]
fn kernel_open_file_limit() -> io::Result<u64> {
    let mut mib = [libc::CTL_KERN, libc::KERN_MAXFILESPERPROC];
    let mut limit: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&limit);
    // SAFETY: The MIB describes a single integer. Its length and the writable
    // output buffer size are exact; null input means this is a read-only query.
    // All pointers remain valid for the synchronous syscall.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len().try_into().expect("the MIB has only two elements"),
            std::ptr::from_mut(&mut limit).cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if length != std::mem::size_of_val(&limit) || limit < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kern.maxfilesperproc did not return a non-negative integer",
        ));
    }
    // The kernel value was checked to be non-negative and fits within u64.
    Ok(limit as u64)
}

#[cfg(test)]
mod tests;
