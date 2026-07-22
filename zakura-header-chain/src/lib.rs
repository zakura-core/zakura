//! Fork-aware header-chain domain types and transition engine.
//!
//! This crate is intentionally synchronous and policy-focused. It owns no
//! network transport, async runtime, consensus service, or database backend.

mod config;
mod error;
mod frontier;
mod graph;
mod ids;
mod node;
mod retention;
mod validation;

pub use config::{
    EngineLimits, MAX_CANDIDATE_TIPS_V1, MAX_NON_FINALIZED_NODES_V1, MAX_STAGED_TARGETS_V1,
};
pub use error::{Attribution, ErrorCategory, ErrorSubject, HeaderChainError, RuleId};
pub use frontier::{
    ChainScore, Frontier, FrontierSet, SuffixWork, WorkCoordinate, WorkCoordinateError,
};
pub use graph::{GraphError, InsertResult, MemHeaderStore};
pub use ids::{
    BranchId, CounterExhausted, EvidenceId, FinalityEpoch, HeaderGeneration, HeaderId,
    OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration, WorkOwner,
};
pub use node::{
    BodyRuleId, BodyUnavailableSummary, BodyValidationState, EligibilityReason, EligibilityState,
    HeaderNode, HeaderValidationState,
};
pub use retention::{enforce_retention, RetentionPlan};
pub use validation::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_future_time,
    validate_hash_filter, validate_link, AdjustedDifficulty, CompactTargetError,
    ContextualValidationError, HashFilterError, HeaderEncodingError, HeaderHeightError,
    HeaderLinkError, PowPolicy, PowPolicyError, BLOCK_MAX_TIME_SINCE_MEDIAN,
    POW_ADJUSTMENT_BLOCK_SPAN, POW_MEDIAN_BLOCK_SPAN,
};

#[cfg(test)]
mod tests {
    #[test]
    fn architecture_dependencies_stay_sync_only_and_layered() {
        let manifest: toml::Value = toml::from_str(include_str!("../Cargo.toml"))
            .expect("the checked-in crate manifest is valid TOML");
        let dependencies = manifest
            .get("dependencies")
            .and_then(toml::Value::as_table)
            .expect("the crate manifest has a dependencies table");
        for forbidden in [
            "tokio",
            "tower",
            "zakura-state",
            "zakura-network",
            "zakura-consensus",
        ] {
            assert!(
                !dependencies.contains_key(forbidden),
                "header-chain architecture forbids a production dependency on {forbidden}"
            );
        }
    }
}
