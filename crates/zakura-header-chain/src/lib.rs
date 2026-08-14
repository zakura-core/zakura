//! Fork-aware header-chain domain types and transition engine.
//!
//! This synchronous crate defines header-chain policy.
//! Higher-level crates own network transport, async runtime, consensus services, and databases.

mod config;
mod discovery;
mod error;
mod graph;
mod identity;
mod transition;
mod validation;
mod work;

#[cfg(any(test, feature = "fuzz-impl"))]
mod fuzz;

pub use config::{
    CheckpointSet, EngineConfig, EngineConfigError, EngineLimits, EngineMode,
    SettledUpgradeManifest, SettledUpgradePin, TrustedAnchor, MAX_AUX_DELIVERIES_PER_HEADER_V1,
    MAX_AUX_DELIVERIES_TOTAL_V1, MAX_CANDIDATE_TIPS_V1, MAX_HEADERS_PER_TRANSITION_V1,
    MAX_NON_FINALIZED_NODES_V1, MAX_STAGED_TARGETS_V1,
};
pub use discovery::{HeaderLocator, VctRepairContext, MAX_HEADER_LOCATOR_HASHES};
pub use error::{Attribution, ErrorCategory, ErrorSubject, HeaderChainError, RuleId};
#[cfg(any(test, feature = "fuzz-impl"))]
pub use fuzz::{replay_fork_transition_bytes, ForkReplaySummary};
pub use graph::{
    BodyRuleId, BodyUnavailableSummary, BodyValidationState, ChainScore,
    ConsensusInvalidBodyTombstone, DurableNodeError, EligibilityReason, EligibilityState, Frontier,
    FrontierSet, GraphError, GraphRevision, HeaderGraphReconstruction, HeaderNode,
    HeaderNodeInvariant, HeaderValidationState, InsertResult, MemHeaderStore, SuffixWork,
    WorkCoordinate, WorkCoordinateError,
};
pub use identity::{
    AuxObservationId, BranchId, CounterExhausted, EvidenceId, FinalityEpoch, HeaderGeneration,
    HeaderId, OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration,
};
pub use transition::*;
pub use validation::{
    infer_height, prepare_headers, validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_future_time,
    validate_hash_filter, validate_link, AdjustedDifficulty, AdjustedDifficultyError,
    CompactTargetError, ContextualValidationError, HashFilterError, HeaderBatchInput,
    HeaderEncodingError, HeaderFailure, HeaderHeightError, HeaderLinkError, HeaderRule,
    HeaderRules, PowPolicy, PowPolicyError, BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN,
    POW_MEDIAN_BLOCK_SPAN, POW_PREDECESSOR_CONTEXT_SPAN,
};
pub use work::{
    BodyWorkAuthority, BodyWorkOwner, CompletionDecision, CompletionOwner, Gate,
    HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner, PendingOwners, StaleReason,
};

#[cfg(test)]
mod tests {
    #[test]
    fn architecture_reduces_public_api_and_seals_auxiliary_authority() {
        let baseline: toml::Value = toml::from_str(include_str!("../public-api-baseline.toml"))
            .expect("the public API baseline is valid TOML");
        let count = |name| {
            baseline[name]
                .as_integer()
                .expect("public API counts are integers")
        };
        assert!(count("current_public_items") < count("baseline_public_items"));
        assert_eq!(count("current_authority_constructors"), 0);

        let auxiliary = include_str!("transition/types/auxiliary.rs");
        assert!(!auxiliary.contains("pub enum AuxAuthentication"));
        assert!(!auxiliary.contains("pub authentication:"));
    }

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

    /// Planning derives its write set incrementally and keeps retention private.
    #[test]
    fn architecture_keeps_planning_encapsulated() {
        let public_surface = include_str!("lib.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the production public surface precedes its tests");
        assert!(
            !public_surface.contains("enforce_retention"),
            "retention stays behind the transition planner"
        );

        fn planner_sources(path: &std::path::Path, sources: &mut Vec<(String, String)>) {
            for entry in std::fs::read_dir(path).expect("the planner source directory is readable")
            {
                let entry = entry.expect("the source directory entry is readable");
                let path = entry.path();
                // Planner test fixtures may build whole graphs; production planning may not.
                if path.file_name().and_then(|name| name.to_str()) == Some("tests") {
                    continue;
                }
                if path.is_dir() {
                    planner_sources(&path, sources);
                } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                    sources.push((
                        path.display().to_string(),
                        std::fs::read_to_string(&path).expect("the Rust source is readable"),
                    ));
                }
            }
        }

        let mut sources = vec![(
            "transition/planner.rs".to_owned(),
            include_str!("transition/planner.rs").to_owned(),
        )];
        planner_sources(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/transition/planner")
                .as_path(),
            &mut sources,
        );
        for (path, source) in &sources {
            for forbidden in [
                "engine.graph().clone()",
                "fn node_map",
                "old_nodes",
                "new_nodes",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{path} derives the write set by whole-graph diff: {forbidden}"
                );
            }
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
