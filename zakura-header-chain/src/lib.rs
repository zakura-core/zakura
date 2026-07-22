//! Fork-aware header-chain domain types and transition engine.
//!
//! This crate is intentionally synchronous and policy-focused. It owns no
//! network transport, async runtime, consensus service, or database backend.

mod config;
mod error;
mod frontier;
mod ids;

pub use config::{
    EngineLimits, MAX_CANDIDATE_TIPS_V1, MAX_NON_FINALIZED_NODES_V1, MAX_STAGED_TARGETS_V1,
};
pub use error::{Attribution, ErrorCategory, ErrorSubject, HeaderChainError, RuleId};
pub use frontier::{
    ChainScore, Frontier, FrontierSet, SuffixWork, WorkCoordinate, WorkCoordinateError,
};
pub use ids::{
    BranchId, CounterExhausted, EvidenceId, FinalityEpoch, HeaderGeneration, HeaderId,
    OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration, WorkOwner,
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
