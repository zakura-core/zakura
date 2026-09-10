//! Main entry point for zakurad

use zakurad::application::{boot, APPLICATION};

#[cfg(all(target_os = "linux", target_env = "gnu"))]
const GLIBC_MALLOC_ARENA_MAX: i32 = 16;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(
    unsafe_code,
    reason = "calling glibc mallopt is required to bound allocator arena retention"
)]
fn limit_glibc_malloc_arenas() {
    if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        return;
    }

    // SAFETY: this process-global allocator setting is configured before worker threads start.
    let configured = unsafe { libc::mallopt(libc::M_ARENA_MAX, GLIBC_MALLOC_ARENA_MAX) };

    assert_ne!(
        configured, 0,
        "glibc accepts M_ARENA_MAX before worker threads start"
    );
}

/// Process entry point for `zakurad`
fn main() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    limit_glibc_malloc_arenas();

    // Enable backtraces by default for zakurad, but allow users to override it.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
        // Disable library backtraces (i.e. eyre) to avoid performance hit for
        // non-panic errors, but allow users to override it.
        if std::env::var_os("RUST_LIB_BACKTRACE").is_none() {
            std::env::set_var("RUST_LIB_BACKTRACE", "0");
        }
    }
    boot(&APPLICATION);
}
