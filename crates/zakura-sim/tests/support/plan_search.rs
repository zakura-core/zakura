//! Bounded searches for a Plan whose run shows what a test needs.
//!
//! A test that needs a particular schedule searches a fixed stream of
//! Plans for the first that reaches it, instead of committing a Plan. When
//! a change moves the schedule, the search finds another Plan, and the
//! test fails only when no Plan within the bound reaches it.

use std::time::Instant;

use super::byte_inputs;

/// The first of `count` Plans of up to `max_len` bytes, from
/// [`byte_inputs`], that `check` accepts. `check` returns why a Plan falls
/// short. Panics with each Plan's shortfall if none passes.
///
/// It prints which Plan passed and how long the search took, which
/// `--nocapture` shows.
#[allow(clippy::print_stderr)]
pub fn first_plan(
    what: &str,
    count: usize,
    max_len: usize,
    mut check: impl FnMut(&[u8]) -> Result<(), String>,
) -> Vec<u8> {
    let started = Instant::now();
    let mut shortfalls = Vec::new();
    for (index, plan) in byte_inputs(count, max_len).into_iter().enumerate() {
        match check(&plan) {
            Ok(()) => {
                eprintln!(
                    "{what}: Plan {index} of {count} passed after {:?}",
                    started.elapsed()
                );
                return plan;
            }
            Err(shortfall) => shortfalls.push(format!("Plan {index}: {shortfall}")),
        }
    }
    panic!(
        "{what}: none of {count} Plans passed in {:?}:\n{}",
        started.elapsed(),
        shortfalls.join("\n")
    );
}
