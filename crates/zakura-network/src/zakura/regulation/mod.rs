//! Owned concurrency permits shared by native Zakura message policies.
//!
//! Each service declares when work starts and which owners must finish before
//! its permit is released. The primitive does not select scheduling policy.

mod slots;
pub(crate) use slots::{SlotBudget, SlotPermit};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;

#[cfg(test)]
pub(crate) mod test_support;
