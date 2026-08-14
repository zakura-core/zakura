//! Domain-specific violation taxonomy for the transition planner.

use thiserror::Error;

/// Structured invalid-evidence failure replacing stringly typed planner messages.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum InvalidTransitionEvidence {
    /// Request or resource bounds were exceeded.
    #[error(transparent)]
    Limit(#[from] LimitViolation),
    /// Finality evidence or ancestry was invalid.
    #[error(transparent)]
    Finality(#[from] FinalityViolation),
    /// Header path or contextual validation evidence was invalid.
    #[error(transparent)]
    Header(#[from] HeaderViolation),
    /// Body availability evidence was invalid.
    #[error(transparent)]
    Body(#[from] BodyViolation),
    /// Auxiliary provenance or authentication evidence was invalid.
    #[error(transparent)]
    Auxiliary(#[from] AuxiliaryViolation),
    /// Operator invalidation or retry evidence was invalid.
    #[error(transparent)]
    Operator(#[from] OperatorViolation),
    /// Planner projection/coherence failed after event application.
    #[error(transparent)]
    Planner(#[from] PlannerCoherenceViolation),
}

/// Request or preflight limit violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum LimitViolation {
    /// Retention references exceed the per-transition bound.
    #[error("retained-path references exceed the per-transition limit")]
    RetentionReferencesExceeded,
    /// Prepared header batch exceeds the per-transition bound.
    #[error("prepared header batch exceeds the per-transition limit")]
    PreparedHeadersExceeded,
}

/// Finality evidence and ancestry violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum FinalityViolation {
    /// Proposed finality retreated below the current pin.
    #[error("finality retreated")]
    Retreated,
    /// Integrated finality is not on the verified projection.
    #[error("integrated finality is not on the verified projection")]
    OutsideVerifiedProjection,
    /// Finality proof is not the exact verified ancestry.
    #[error("finality proof is not the exact verified ancestry")]
    ProofMismatch,
    /// Full-state refutation does not name an imported pin ancestor.
    #[error("full-state refutation does not name an imported pin ancestor")]
    ImportedPinRefutationMismatch,
}

/// Which header-validation path produced a failure.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderValidationSource {
    /// Full-state verified path insertion/validation.
    #[error("full-state")]
    FullState,
    /// Prepared network header insertion.
    #[error("prepared")]
    Prepared,
}

/// Specific header validation check that failed.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderValidationCheck {
    /// Header policy construction failed.
    #[error("header policy is incoherent")]
    PolicyIncoherent,
    /// Observable/context-free validation failed.
    #[error("header failed observable validation")]
    ObservableValidation,
    /// Validator returned no prepared header.
    #[error("header validation produced no result")]
    NoResult,
    /// Identity or local-time state disagreed.
    #[error("header identity or local-time state is invalid")]
    IdentityOrLocalTime,
    /// Retained difficulty context was incomplete.
    #[error("header has incomplete retained difficulty context")]
    IncompleteDifficultyContext,
    /// Contextual difficulty or time validation failed.
    #[error("header failed contextual difficulty or time validation")]
    ContextualValidation,
}

/// Which path kind failed continuity/emptiness checks.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderPathKind {
    /// Prepared target-completion path.
    #[error("target completion")]
    Completion,
    /// Full-state verified path.
    #[error("verified path")]
    Verified,
    /// Accepted full-state side path.
    #[error("accepted side path")]
    AcceptedSide,
}

/// Path-shape problem on a header path.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderPathProblem {
    /// Path contained no headers.
    #[error("is empty")]
    Empty,
    /// Parent links or heights were discontinuous.
    #[error("is not continuous")]
    Discontinuous,
    /// Completion ancestor disagreed with the retained parent.
    #[error("ancestor does not match the retained parent")]
    AncestorMismatch,
    /// Completion tip disagreed with the pursued hash.
    #[error("does not end at the pursued hash")]
    TipMismatch,
    /// Prepared batch fields were internally inconsistent.
    #[error("batch is inconsistent")]
    BatchInconsistent,
    /// Prepared compact target was invalid.
    #[error("has an invalid prepared target")]
    InvalidPreparedTarget,
}

/// Header path and contextual validation violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderViolation {
    /// A concrete header validation check failed.
    #[error("{source} {check}")]
    Validation {
        /// Whether the failure came from prepared or full-state validation.
        source: HeaderValidationSource,
        /// Exact failed check.
        check: HeaderValidationCheck,
    },
    /// A header path shape invariant failed.
    #[error("{kind} {problem}")]
    Path {
        /// Which path failed.
        kind: HeaderPathKind,
        /// Exact shape problem.
        problem: HeaderPathProblem,
    },
    /// Authenticated owner role does not match the completion kind.
    #[error("ordinary header insertion does not have pure header authority")]
    OrdinaryOwnerRoleMismatch,
    /// Selected auxiliary repair lacks body authority.
    #[error("selected auxiliary repair does not have body authority")]
    RepairOwnerRoleMismatch,
    /// Auxiliary repair is not one exact selected header.
    #[error("auxiliary repair is not one exact selected header")]
    AuxiliaryRepairShape,
}

/// Body availability and episode violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum BodyViolation {
    /// Transient retry episode summary is malformed.
    #[error("body retry evidence has an invalid episode summary")]
    InvalidTransientEpisode,
    /// Retry would regress an already verified body.
    #[error("body retry evidence cannot regress an already verified body")]
    RetryConflictsWithVerified,
    /// Invalidity contradicts an already verified body.
    #[error("body invalid evidence cannot contradict an already verified body")]
    InvalidityConflictsWithVerified,
    /// Supplier discovery requires the selected persistent alarm.
    #[error("body supplier discovery requires the selected persistent alarm")]
    SupplierRequiresPersistentAlarm,
    /// Supplier discovery must preserve the persistent retry episode.
    #[error("body supplier discovery must preserve the persistent retry episode")]
    SupplierEpisodeChanged,
    /// Supplier discovery does not add an eligible supplier.
    #[error("body supplier discovery does not add an eligible supplier")]
    NoNewSupplier,
    /// Operator body retry has an invalid fresh episode.
    #[error("operator body retry has an invalid fresh episode")]
    InvalidOperatorRetryEpisode,
    /// Operator body retry requires the selected persistent alarm.
    #[error("operator body retry requires the selected persistent alarm")]
    OperatorRetryRequiresPersistentAlarm,
}

/// Auxiliary provenance and authentication violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum AuxiliaryViolation {
    /// Delivery count must be one or two.
    #[error("auxiliary evidence must name one or two exact deliveries")]
    DeliveryCount,
    /// The same delivery was named more than once.
    #[error("auxiliary evidence names the same delivery more than once")]
    DuplicateDelivery,
    /// Delivery references an unknown header.
    #[error("auxiliary evidence references an unknown header")]
    UnknownHeader,
    /// Delivery is outside its owned branch.
    #[error("auxiliary evidence is outside its owned branch")]
    OutsideOwnedBranch,
    /// Delivery references an unknown stored delivery.
    #[error("auxiliary evidence references an unknown delivery")]
    UnknownDelivery,
    /// Delivery changes stored provenance.
    #[error("auxiliary evidence changes delivery provenance")]
    ProvenanceMismatch,
    /// Evidence would weaken or replace the existing authentication state.
    #[error("auxiliary authentication evidence would weaken or replace existing evidence")]
    NonRefiningAuthentication,
    /// Authentication boundary header is unknown.
    #[error("auxiliary authentication boundary is unknown")]
    UnknownBoundary,
    /// Authentication boundary height overflowed.
    #[error("auxiliary authentication boundary height overflowed")]
    BoundaryHeightOverflow,
    /// Authentication is not the owned one-header-later boundary.
    #[error("auxiliary authentication is not the owned one-header-later boundary")]
    InvalidBoundary,
    /// Evidence attempted to remove authentication.
    #[error("auxiliary evidence cannot remove authentication")]
    AuthenticationRemoval,
    /// Insertion-time delivery does not match the admitted target.
    #[error("auxiliary delivery does not match the admitted target")]
    AdmittedTargetMismatch,
    /// Insertion-time delivery replay changes provenance or indexing.
    #[error("auxiliary delivery replay changes provenance or indexing")]
    ReplayConflict,
}

/// Operator invalidation violations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum OperatorViolation {
    /// Invalidation identity is not bound to its target.
    #[error("operator invalidation identity is not bound to its target")]
    BindingMismatch,
    /// Invalidation targeted the finalized anchor.
    #[error("operator invalidation cannot target the finalized anchor")]
    FinalizedAnchorTarget,
}

/// Which projection failed a coherence check.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum ProjectionKind {
    /// Selected header projection.
    #[error("selected")]
    Selected,
    /// Verified body projection.
    #[error("verified")]
    Verified,
}

/// Planner coherence failures that are not caller evidence failures.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum PlannerCoherenceViolation {
    /// Selected ancestry required for headers-only finality was incomplete.
    #[error("selected ancestry is incomplete")]
    IncompleteSelectedAncestry,
    /// Checkpoint finality advanced without a finality record.
    #[error("checkpoint finality has no finality record")]
    MissingCheckpointRecord,
    /// Checkpoint finality left the selected projection.
    #[error("checkpoint finality left the selected projection")]
    CheckpointOutsideSelection,
    /// A projection became empty.
    #[error("{0} projection is empty")]
    EmptyProjection(ProjectionKind),
    /// A projection became discontinuous.
    #[error("{0} projection is not continuous")]
    DiscontinuousProjection(ProjectionKind),
}

impl InvalidTransitionEvidence {
    /// Construct a full-state header validation failure.
    pub const fn full_state_header(check: HeaderValidationCheck) -> Self {
        Self::Header(HeaderViolation::Validation {
            source: HeaderValidationSource::FullState,
            check,
        })
    }

    /// Construct a prepared header validation failure.
    pub const fn prepared_header(check: HeaderValidationCheck) -> Self {
        Self::Header(HeaderViolation::Validation {
            source: HeaderValidationSource::Prepared,
            check,
        })
    }

    /// Construct a header path failure.
    pub const fn header_path(kind: HeaderPathKind, problem: HeaderPathProblem) -> Self {
        Self::Header(HeaderViolation::Path { kind, problem })
    }
}
