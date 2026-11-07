//! Best-effort priority reduction for the dedicated miner thread.

use std::io;

/// Lowers only the calling thread's priority, keeping its scheduling policy.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
#[allow(unsafe_code)]
pub(super) fn lower_current_thread_priority() -> io::Result<()> {
    // SAFETY: `pthread_self` has no preconditions and identifies this live thread.
    let thread = unsafe { libc::pthread_self() };
    let mut policy = 0;
    // SAFETY: `sched_param` contains only integers, including Apple's private
    // reserved storage; all-zero bytes are valid before the OS fills it in.
    let mut parameters: libc::sched_param = unsafe { std::mem::zeroed() };
    // SAFETY: the thread is live and both output pointers are writable for the
    // duration of this synchronous call; the API retains neither pointer.
    let error = unsafe { libc::pthread_getschedparam(thread, &mut policy, &mut parameters) };
    if error != 0 {
        // pthread APIs return the error number directly rather than setting errno.
        return Err(io::Error::from_raw_os_error(error));
    }

    // SAFETY: `policy` was returned by the OS for the current thread.
    let minimum = unsafe { libc::sched_get_priority_min(policy) };
    if minimum == -1 {
        return Err(io::Error::last_os_error());
    }
    parameters.sched_priority = minimum;
    // SAFETY: the thread is live, its existing policy is retained, and the
    // initialized parameters contain that policy's minimum supported priority.
    let error = unsafe { libc::pthread_setschedparam(thread, policy, &parameters) };
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    if matches!(
        policy,
        libc::SCHED_OTHER | libc::SCHED_BATCH | libc::SCHED_IDLE
    ) {
        // Normal Linux scheduling uses niceness, with 19 the lowest priority.
        // Retain the upstream setting of 0 for the already-idle policy.
        let niceness = if policy == libc::SCHED_IDLE { 0 } else { 19 };
        // SAFETY: on Linux, PRIO_PROCESS with who=0 targets the calling thread,
        // not the entire process. No pointers are passed. This is intentionally
        // excluded on Apple platforms, where niceness is process-wide.
        if unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, niceness) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    Ok(())
}

/// Lowers only the calling thread's priority within the process priority class.
#[cfg(windows)]
#[allow(unsafe_code)]
pub(super) fn lower_current_thread_priority() -> io::Result<()> {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_LOWEST,
    };

    // SAFETY: the pseudo-handle refers to this live thread, requires no closing,
    // and accepts the documented THREAD_PRIORITY_LOWEST value.
    if unsafe { SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_LOWEST) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Reports unsupported platforms so the caller can keep mining normally.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    windows
)))]
pub(super) fn lower_current_thread_priority() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "lowering miner thread priority is unsupported on this platform",
    ))
}
