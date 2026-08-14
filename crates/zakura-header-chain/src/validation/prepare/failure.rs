use thiserror::Error;

use super::HeaderRule;

/// Failure to prepare a batch.
/// A successful batch can represent only a local future-time deferral.
#[derive(Debug, Error)]
pub enum HeaderFailure {
    /// The caller supplied no headers for an insertion event.
    #[error("header batch is empty")]
    Empty,
    /// The batch exceeded the frozen per-transition header bound.
    #[error("header batch has {actual} entries, maximum {maximum}")]
    Oversized {
        /// Supplied header count.
        actual: usize,
        /// Frozen maximum header count.
        maximum: usize,
    },
    /// Durable state supplied an incoherent or stale validation lease.
    #[error("validation lease is incoherent with the authenticated header rules")]
    InvalidLease,
    /// One deterministic observable-header rule failed.
    #[error("header at offset {offset} failed {rule:?}: {reason}")]
    Invalid {
        /// Zero-based header offset.
        offset: usize,
        /// Exact failed stage.
        rule: HeaderRule,
        /// Stable human-readable source description.
        reason: String,
    },
    /// A local time calculation exceeded the representable timestamp range.
    #[error("local future-time boundary is outside the representable timestamp range")]
    ClockRange,
}

pub(super) fn invalid(
    offset: usize,
    rule: HeaderRule,
    error: impl std::fmt::Display,
) -> HeaderFailure {
    HeaderFailure::Invalid {
        offset,
        rule,
        reason: error.to_string(),
    }
}
