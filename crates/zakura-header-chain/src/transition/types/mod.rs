//! Complete typed input and output surface for serialized header-chain transitions.

mod auxiliary;
mod error;
mod event;
mod outcome;
mod preparation;
mod snapshot;
mod write_set;

#[cfg(test)]
mod tests;

pub use auxiliary::{
    AuxDelivery, BodySizeHint, PreparedAuxDelivery, TreeAuxRecordV1, UntrustedAuxDeliveryRow,
};
pub(crate) use auxiliary::{AuxOutcome, AuxOutcomeStatus};
pub use error::{RowLimit, StoreCollection, StoreError, TransitionTypeError};
pub(crate) use event::AuxVerificationKindV1;
pub use event::{
    AuxEvidence, AuxObservationV1, AuxVerificationFactV1, BodyCommitmentKind, BodyEvidence,
    BodyPayloadMismatch, BodySupplierDiscovered, BodyVerificationClass, BodyVerificationOutcome,
    ConsensusBodyInvalid, EventAdmission, FullStateFinalized, InsertHeaders, MigratedPinRefutation,
    OperatorBodyRetry, OperatorInvalidate, OperatorReconsider, TargetCompletion,
    TransientBodyFailure, TransientBodyFailureKind, TransitionDomain, TransitionEvent,
    TransitionFingerprint, TransitionRequest, VerifiedBlockAccepted, VerifiedBodyEvidence,
    VerifiedChainChanged, VerifiedChangeCause, VerifiedHeaderRef,
};
pub use outcome::{
    ApplyResult, AuxiliaryEffect, BodyWorkEffect, CommittedStallReceipt, FinalityEffect,
    HeaderWorkEffect, NoChangeReceipt, RetiredWork, StaleReceipt, TransitionEffect,
    VctRootRepairState, VctRootRepairStatus,
};
pub(crate) use preparation::hash_network_policy;
pub use preparation::{
    ContextFreePreparationReceipt, HeaderContextFact, PreparedHeader, PreparedHeaderBatch,
    ValidationLease,
};
pub use snapshot::{
    AlarmSet, CommittedHeaderChainView, EngineMetadata, EngineSnapshot, HeaderChainDiskVersion,
};
pub use write_set::{
    checkpoint_finality_evidence, full_state_finality_evidence, full_state_initialization_evidence,
    AuxDelta, ChangeSet, DiskMigrationAuthentication, EligibilityDelta, FinalityAncestryHeader,
    FinalityHistoryCheckpoint, FinalityRecord, FinalitySource, FinalityWitnessProof,
    FullStateFinalityKind, FullStateFinalityProvenance, IndexChanges, ProjectionDelta,
};
