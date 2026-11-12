//! sim-7's acceptance: the judge of every run that counts as evidence, and
//! the manifest and evaluator `cargo xtask sim-acceptance` runs (the sim-7
//! implementation plan, sim-7.4).

pub mod evaluate;
pub mod header;
pub mod judge;

pub use judge::{owned_run_passes, Blocked, CleanupMeaning, Registered, RunExpect, CLEANUP};
