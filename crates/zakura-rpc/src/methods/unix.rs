//! Unix signalling for the regtest shutdown RPC.

use std::io;

/// Delivers SIGINT to the calling thread, as the shutdown handler expects.
#[allow(unsafe_code)]
pub(super) fn raise_interrupt() -> io::Result<()> {
    // SAFETY: SIGINT is a valid signal. `raise` takes no pointers and signals
    // only the calling process, whose shutdown handler handles SIGINT.
    if unsafe { libc::raise(libc::SIGINT) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
