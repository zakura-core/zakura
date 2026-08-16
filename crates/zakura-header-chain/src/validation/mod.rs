//! Shared synchronous observable-header validation primitives.
//!
//! # Validation phase order
//!
//! 1. [`context_free`] validates facts available from a header, authenticated
//!    network parameters, explicit height or parent inputs, and an injected
//!    local clock.
//! 2. [`prepare`] applies those primitives to a complete batch and seals the
//!    resulting evidence to its supplied parent, network, and trust anchor.
//! 3. [`contextual`] validates branch-local difficulty and median-time rules
//!    once retained predecessor context is available.
//!
//! # Non-claims
//!
//! Context-free preparation does not claim retained parent linkage,
//! branch-local difficulty or median time, commitment-value authentication,
//! full-block validity, or graph mutation. Contextual arithmetic does not own
//! admission, finality, checkpoints, settled pins, completion, provenance, or
//! persistence; those conformance decisions remain in the transition planner.

mod context_free;
mod contextual;
mod prepare;

pub(crate) use context_free::validate_trusted_anchor_observables;
pub use context_free::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_encoding_version_hash, validate_future_time, validate_hash_filter, validate_link,
    CompactTargetError, HashFilterError, HeaderEncodingError, HeaderHeightError, HeaderLinkError,
    PowPolicy, PowPolicyError,
};
pub use contextual::{
    validate_contextual_difficulty_and_time, AdjustedDifficulty, AdjustedDifficultyError,
    ContextualValidationError, BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN,
    POW_MEDIAN_BLOCK_SPAN, POW_PREDECESSOR_CONTEXT_SPAN,
};
pub use prepare::{prepare_headers, HeaderBatchInput, HeaderFailure, HeaderRule, HeaderRules};
