//! Sealed validation of complete observable-header batches.

mod failure;
mod input;
mod pipeline;
mod stage;

pub use failure::HeaderFailure;
pub use input::HeaderBatchInput;
pub use pipeline::{prepare_headers, HeaderRules};
pub use stage::HeaderRule;

#[cfg(test)]
mod tests;
