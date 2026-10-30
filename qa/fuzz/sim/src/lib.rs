//! libFuzzer targets over `zakura-sim` harnesses (explorer.md).
//!
//! Each target is one [`sim_target!`] line over a harness. The target runs
//! the input with `zakura_sim::explore`, which classifies the run, and
//! [`fuzz_one`] acts on the class: it records and panics on an `Active`
//! violation, panics on a harness failure, and saves a trace of each
//! `Known` and `Draft` violation once.
//!
//! The package needs `RUSTFLAGS="--cfg zakura_sim"`. Without it,
//! `zakura-sim` compiles to an empty crate, so the build fails here instead.

#[cfg(not(zakura_sim))]
compile_error!("qa/fuzz/sim needs RUSTFLAGS=\"--cfg zakura_sim\"");

#[cfg(zakura_sim)]
mod feedback;
#[cfg(zakura_sim)]
mod fuzz;

#[cfg(zakura_sim)]
pub use feedback::*;
#[cfg(zakura_sim)]
pub use fuzz::*;

/// AddressSanitizer's defaults for these targets. `ASAN_OPTIONS` still
/// overrides them.
///
/// Fake stacks for use-after-return checks are off. ASan never frees the
/// fake frames of a coroutine whose stack the scheduler reuses, so with them
/// on, a campaign's memory grows until libFuzzer kills it as out of memory:
/// about 3 GiB in 15 seconds of `race_fixtures`, against a flat 450 MiB with
/// them off.
#[cfg(feature = "sanitizer")]
#[no_mangle]
pub extern "C" fn __asan_default_options() -> *const std::ffi::c_char {
    c"detect_stack_use_after_return=0".as_ptr()
}
