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

pub use config::{
    CheckpointSet, EngineConfig, EngineConfigError, EngineLimits, EngineMode,
    SettledUpgradeManifest, SettledUpgradePin, TrustedAnchor, MAX_CANDIDATE_TIPS_V1,
    MAX_NON_FINALIZED_NODES_V1, MAX_STAGED_TARGETS_V1,
};
pub use error::{Attribution, ErrorCategory, ErrorSubject, HeaderChainError, RuleId};
pub use frontier::{
    ChainScore, Frontier, FrontierSet, SuffixWork, WorkCoordinate, WorkCoordinateError,
};
pub use graph::{GraphError, InsertResult, MemHeaderStore};
pub use ids::{
    BodyWorkAuthority, BodyWorkOwner, BranchId, CounterExhausted, EvidenceId, FinalityEpoch,
    HeaderGeneration, HeaderId, HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner,
    OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration,
};
pub use locator::{HeaderLocator, VctRepairContext, MAX_HEADER_LOCATOR_HASHES};
pub use node::{
    BodyRuleId, BodyUnavailableSummary, BodyValidationState, DurableNodeError, EligibilityReason,
    EligibilityState, HeaderNode, HeaderValidationState,
};
pub use ownership::{
    CompletionDecision, CompletionGate, CompletionOwner, PendingOwners, StaleReason,
};
pub use transition::*;
pub use validation::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_encoding_version_hash, validate_future_time, validate_hash_filter, validate_link,
    CompactTargetError, HashFilterError, HeaderEncodingError, HeaderHeightError, HeaderLinkError,
    PowPolicy, PowPolicyError,
};
