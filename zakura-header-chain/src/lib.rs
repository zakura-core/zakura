//! Fork-aware header-chain domain types and transition engine.
//!
//! This crate is intentionally synchronous and policy-focused. It owns no
//! network transport, async runtime, consensus service, or database backend.

mod config;
mod error;
mod frontier;
mod graph;
mod ids;
mod locator;
mod node;
mod ownership;
mod retention;
mod transition;
mod validation;

#[cfg(any(test, feature = "fuzz-impl"))]
mod fuzz;

pub use config::{
    CheckpointSet, EngineConfig, EngineConfigError, EngineLimits, EngineMode,
    SettledUpgradeManifest, SettledUpgradePin, TrustedAnchor, MAX_CANDIDATE_TIPS_V1,
    MAX_NON_FINALIZED_NODES_V1, MAX_STAGED_TARGETS_V1,
};
pub use error::{Attribution, ErrorCategory, ErrorSubject, HeaderChainError, RuleId};
pub use frontier::{
    ChainScore, Frontier, FrontierSet, SuffixWork, WorkCoordinate, WorkCoordinateError,
};
#[cfg(any(test, feature = "fuzz-impl"))]
pub use fuzz::{replay_fork_transition_bytes, ForkReplaySummary};
pub use graph::{GraphError, InsertResult, MemHeaderStore};
pub use ids::{
    BranchId, CounterExhausted, EvidenceId, FinalityEpoch, HeaderGeneration, HeaderId,
    OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration, WorkOwner, WorkScope,
};
pub use locator::{HeaderLocator, VctRepairContext, MAX_HEADER_LOCATOR_HASHES};
pub use node::{
    BodyRuleId, BodyUnavailableSummary, BodyValidationState, DurableNodeError, EligibilityReason,
    EligibilityState, HeaderNode, HeaderValidationState,
};
pub use ownership::{CompletionDecision, CompletionGate, PendingOwners, StaleReason};
pub use retention::RetentionPlan;
pub use transition::*;
pub use validation::{
    infer_height, prepare_headers, validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_future_time,
    validate_hash_filter, validate_link, AdjustedDifficulty, CompactTargetError,
    ContextualValidationError, HashFilterError, HeaderBatchInput, HeaderEncodingError,
    HeaderFailure, HeaderHeightError, HeaderLinkError, HeaderRule, HeaderRules, PowPolicy,
    PowPolicyError, BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN, POW_MEDIAN_BLOCK_SPAN,
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

    #[test]
    fn architecture_excludes_wallet_flyclient_and_block_sync_surfaces() {
        fn production_sources(path: &std::path::Path, sources: &mut Vec<(String, String)>) {
            for entry in std::fs::read_dir(path).expect("the crate source directory is readable") {
                let entry = entry.expect("the source directory entry is readable");
                let path = entry.path();
                if path.is_dir() {
                    production_sources(&path, sources);
                } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                    let source =
                        std::fs::read_to_string(&path).expect("the Rust source is readable");
                    sources.push((
                        path.display().to_string(),
                        source
                            .split("#[cfg(test)]")
                            .next()
                            .expect("production code precedes its tests")
                            .to_ascii_lowercase(),
                    ));
                }
            }
        }

        let manifest: toml::Value = toml::from_str(include_str!("../Cargo.toml"))
            .expect("the checked-in crate manifest is valid TOML");
        let dependencies = manifest
            .get("dependencies")
            .and_then(toml::Value::as_table)
            .expect("the crate manifest has a dependencies table");
        for forbidden in [
            "zcash_client_backend",
            "zcash_client_sqlite",
            "zcash_keys",
            "zcash_note_encryption",
        ] {
            assert!(
                !dependencies.contains_key(forbidden),
                "header-chain architecture forbids wallet dependency {forbidden}"
            );
        }

        let public_surface = include_str!("lib.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the production public surface precedes its tests")
            .to_ascii_lowercase();
        for forbidden in [
            "wallet",
            "flyclient",
            "trial_decryption",
            "note_witness",
            "compact_block",
        ] {
            assert!(
                !public_surface.contains(forbidden),
                "header-chain public API contains excluded surface `{forbidden}`"
            );
        }

        let config = include_str!("config.rs");
        let engine_config = config
            .split_once("pub struct EngineConfig {")
            .and_then(|(_, rest)| rest.split_once("\n}\n\nimpl EngineConfig"))
            .map(|(fields, _)| fields.to_ascii_lowercase())
            .expect("EngineConfig has one inspectable field block");
        for forbidden in [
            "block_sync",
            "token_bucket",
            "connection_eviction",
            "readiness",
            "wallet",
            "flyclient",
        ] {
            assert!(
                !engine_config.contains(forbidden),
                "header-chain selection config contains excluded input `{forbidden}`"
            );
        }

        let mut sources = Vec::new();
        production_sources(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .as_path(),
            &mut sources,
        );
        for forbidden in [
            "zip 307",
            "flyclient",
            "compact_block",
            "trial_decryption",
            "note_witness",
            "wallet_state",
            "token_bucket",
            "connection_eviction",
            "readiness_accounting",
        ] {
            assert!(
                sources
                    .iter()
                    .all(|(_, source)| !source.contains(forbidden)),
                "header-chain production source contains excluded concern `{forbidden}` in [{}]",
                sources
                    .iter()
                    .filter_map(|(path, source)| source
                        .contains(forbidden)
                        .then_some(path.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}
