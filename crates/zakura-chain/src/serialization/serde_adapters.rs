//! Serde adapters for fixed byte arrays and human-readable durations.
//!
//! These adapters preserve the formats used by chain data and configuration.

pub mod bytes;
pub mod duration;
pub mod optional_duration;

#[cfg(test)]
mod tests;
