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
    AuxAuthentication, AuxDelivery, BodySizeHint, PreparedAuxDelivery, TreeAuxRecordV1,
};
pub use error::{StoreError, TransitionTypeError};
pub use event::{
    AuxEvidence, BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodySupplierDiscovered,
    BodyVerificationClass, BodyVerificationOutcome, ConsensusBodyInvalid, EventAdmission,
    FullStateFinalized, InsertHeaders, MigratedPinRefutation, OperatorBodyRetry,
    OperatorInvalidate, OperatorReconsider, TargetCompletion, TransientBodyFailure,
    TransientBodyFailureKind, TransitionDomain, TransitionEvent, TransitionFingerprint,
    TransitionRequest, VerifiedBlockAccepted, VerifiedBodyEvidence, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedHeaderRef,
};
pub use outcome::{
    ApplyResult, AuxiliaryEffect, CommittedStallReceipt, FinalityEffect, HeaderWorkEffect,
    NoChangeReceipt, RetiredWork, StaleReceipt, TransitionEffect, VctRootRepairState,
    VctRootRepairStatus,
};
pub use preparation::{
    ContextFreePreparationReceipt, HeaderContextFact, PreparedHeader, PreparedHeaderBatch,
    ValidationLease,
};
pub use snapshot::{AlarmSet, EngineMetadata, EngineSnapshot, HeaderChainDiskVersion};
pub use write_set::{
    AuxDelta, ChangeSet, EligibilityDelta, FinalityRecord, FinalitySource, IndexChanges,
    ProjectionDelta,
};
