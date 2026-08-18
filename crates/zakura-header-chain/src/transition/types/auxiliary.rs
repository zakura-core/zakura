//! Hash-keyed auxiliary delivery records and advisory body-size metadata.

use std::num::NonZeroU32;

use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard, sapling,
};

use crate::{AuxObservationId, EvidenceId, HeaderSyncWorkOwner, SourceId};

use super::error::TransitionTypeError;

/// Bounded advisory body-size metadata.
/// Body-size metadata cannot allocate or grant admission credit.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BodySizeHint {
    /// Wire value zero: no size is known.
    Unknown,
    /// Canonical block size in `1..=MAX_BLOCK_BYTES`.
    Known(NonZeroU32),
}

impl BodySizeHint {
    /// Validate an advisory wire value.
    pub fn new(value: u32) -> Result<Self, TransitionTypeError> {
        if value == 0 {
            return Ok(Self::Unknown);
        }
        if u64::from(value) > block::MAX_BLOCK_BYTES {
            return Err(TransitionTypeError::InvalidBodySize(value));
        }
        Ok(Self::Known(
            NonZeroU32::new(value).expect("the zero body-size sentinel returned above"),
        ))
    }
}

/// Read-only status of one sealed auxiliary outcome.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuxOutcomeStatus {
    /// No checked observation has changed the delivery.
    Unauthenticated,
    /// Integrated verification authenticated this exact delivery.
    Authenticated,
    /// Integrated verification rejected this delivery without invalidating its header.
    Rejected,
    /// Verification could not attribute a failure between two deliveries.
    Disputed,
}

/// Engine-derived auxiliary outcome.
///
/// Callers can inspect this value but cannot construct an authoritative outcome.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuxOutcome {
    status: AuxOutcomeStatus,
    observation_ids: [Option<AuxObservationId>; 2],
    boundary_hash: Option<block::Hash>,
}

impl AuxOutcome {
    /// Construct the initial outcome before the engine processes an observation.
    pub(crate) const fn unauthenticated() -> Self {
        Self {
            status: AuxOutcomeStatus::Unauthenticated,
            observation_ids: [None, None],
            boundary_hash: None,
        }
    }

    /// Refine this outcome with one checked observation.
    ///
    /// The caller must first verify [`Self::can_refine_to`]. This method retains
    /// at most two distinct observation identities in refinement order.
    pub(crate) fn refined_by_observation(
        self,
        refined_status: AuxOutcomeStatus,
        observation_id: AuxObservationId,
        boundary_hash: block::Hash,
    ) -> Self {
        let mut observation_ids = self.observation_ids;
        if !observation_ids.contains(&Some(observation_id)) {
            if observation_ids[0].is_none() {
                observation_ids[0] = Some(observation_id);
            } else {
                observation_ids[1] = Some(observation_id);
            }
        }
        Self {
            status: refined_status,
            observation_ids,
            boundary_hash: Some(boundary_hash),
        }
    }

    /// Return the derived status.
    pub(crate) const fn status(self) -> AuxOutcomeStatus {
        self.status
    }

    /// Return the derived boundary, when an observation changed this delivery.
    pub(crate) const fn boundary_hash(self) -> Option<block::Hash> {
        self.boundary_hash
    }

    /// Return the retained observation identities in refinement order.
    pub(crate) const fn observation_ids(self) -> [Option<AuxObservationId>; 2] {
        self.observation_ids
    }

    /// Return whether this outcome already includes an observation identity.
    pub(crate) fn contains_observation(self, observation_id: AuxObservationId) -> bool {
        self.observation_ids.contains(&Some(observation_id))
    }

    /// Return whether a checked observation can refine this outcome to `refined_status`.
    pub(crate) fn can_refine_to(self, refined_status: AuxOutcomeStatus) -> bool {
        matches!(
            (self.status, refined_status),
            (AuxOutcomeStatus::Unauthenticated, next)
                if next != AuxOutcomeStatus::Unauthenticated
        ) || matches!(
            (self.status, refined_status),
            (
                AuxOutcomeStatus::Disputed,
                AuxOutcomeStatus::Authenticated | AuxOutcomeStatus::Rejected
            )
        )
    }

    /// Validate outcome fields after recovery has classified the row as untrusted input.
    pub(crate) fn validate_recovered(
        status_code: u8,
        observation_digests: [Option<[u8; 32]>; 2],
        boundary_hash: Option<block::Hash>,
    ) -> Option<Self> {
        let status = match status_code {
            0 => AuxOutcomeStatus::Unauthenticated,
            1 => AuxOutcomeStatus::Authenticated,
            2 => AuxOutcomeStatus::Rejected,
            3 => AuxOutcomeStatus::Disputed,
            _ => return None,
        };
        let observation_ids =
            observation_digests.map(|digest| digest.map(AuxObservationId::from_digest));
        let valid = match status {
            AuxOutcomeStatus::Unauthenticated => {
                observation_ids == [None, None] && boundary_hash.is_none()
            }
            AuxOutcomeStatus::Authenticated
            | AuxOutcomeStatus::Rejected
            | AuxOutcomeStatus::Disputed => {
                observation_ids[0].is_some()
                    && observation_ids[0] != observation_ids[1]
                    && boundary_hash.is_some()
            }
        };
        valid.then_some(Self {
            status,
            observation_ids,
            boundary_hash,
        })
    }
}

/// Hash-keyed auxiliary delivery with complete provenance.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AuxDelivery {
    /// Stable delivery identity.
    pub delivery_id: EvidenceId,
    /// Exact retained header.
    pub header_hash: block::Hash,
    /// Supplying peer/session identity.
    pub source: SourceId,
    /// Complete work ownership at receipt.
    pub owner: HeaderSyncWorkOwner,
    /// Advisory bounded body size.
    pub body_size: BodySizeHint,
    /// Complete schema-1 record retained for later one-header-later authentication.
    pub tree_aux: Option<TreeAuxRecordV1>,
    outcome: AuxOutcome,
}

impl AuxDelivery {
    /// Construct one untrusted delivery at admission.
    pub const fn new(
        delivery_id: EvidenceId,
        header_hash: block::Hash,
        source: SourceId,
        owner: HeaderSyncWorkOwner,
        body_size: BodySizeHint,
        tree_aux: Option<TreeAuxRecordV1>,
    ) -> Self {
        Self {
            delivery_id,
            header_hash,
            source,
            owner,
            body_size,
            tree_aux,
            outcome: AuxOutcome::unauthenticated(),
        }
    }

    /// Return the engine-derived outcome.
    pub(crate) fn outcome(self) -> AuxOutcome {
        self.outcome
    }

    /// Return whether no checked observation changed this delivery.
    pub fn is_unauthenticated(self) -> bool {
        self.outcome().status() == AuxOutcomeStatus::Unauthenticated
    }

    /// Return whether integrated verification authenticated this delivery.
    pub fn is_authenticated(self) -> bool {
        self.outcome().status() == AuxOutcomeStatus::Authenticated
    }

    /// Return whether integrated verification rejected this delivery.
    pub fn is_rejected(self) -> bool {
        self.outcome().status() == AuxOutcomeStatus::Rejected
    }

    /// Return whether integrated verification disputed this delivery.
    pub fn is_disputed(self) -> bool {
        self.outcome().status() == AuxOutcomeStatus::Disputed
    }

    /// Return the derived boundary, when an observation changed this delivery.
    pub fn outcome_boundary_hash(self) -> Option<block::Hash> {
        self.outcome().boundary_hash()
    }

    /// Return retained observation identities in refinement order.
    pub fn observation_ids(self) -> [Option<AuxObservationId>; 2] {
        self.outcome().observation_ids()
    }

    /// Replace the outcome after engine derivation or recovery validation.
    pub(crate) fn with_outcome(mut self, outcome: AuxOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Validate and install outcome fields decoded from an untrusted durable row.
    pub(crate) fn promote_recovered_outcome(
        self,
        status_code: u8,
        observation_digests: [Option<[u8; 32]>; 2],
        boundary_hash: Option<block::Hash>,
    ) -> Option<Self> {
        AuxOutcome::validate_recovered(status_code, observation_digests, boundary_hash)
            .map(|outcome| self.with_outcome(outcome))
    }

    /// Construct a derived outcome for cross-crate tests only.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_only_with_outcome(
        self,
        status_code: u8,
        observation_digests: [Option<[u8; 32]>; 2],
        boundary_hash: Option<block::Hash>,
    ) -> Option<Self> {
        self.promote_recovered_outcome(status_code, observation_digests, boundary_hash)
    }
}

/// Immutable schema-1 commitment inputs for one inferred block height.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TreeAuxRecordV1 {
    /// Exact inferred height of this record.
    pub height: block::Height,
    /// End-of-block Sapling note-commitment root.
    pub sapling_root: sapling::tree::Root,
    /// End-of-block Orchard root, empty below NU5.
    pub orchard_root: orchard::tree::Root,
    /// End-of-block Ironwood root, empty below NU6.3.
    pub ironwood_root: ironwood::tree::Root,
    /// Per-block Sapling shielded transaction count.
    pub sapling_tx_count: u64,
    /// Per-block Orchard shielded transaction count, zero below NU5.
    pub orchard_tx_count: u64,
    /// Per-block Ironwood shielded transaction count, zero before configured NU7.
    pub ironwood_tx_count: u64,
    /// ZIP-244 authorizing-data root, all zero below NU5.
    pub auth_data_root: AuthDataRoot,
}

/// Prepared auxiliary input admitted alongside a header batch.
pub type PreparedAuxDelivery = AuxDelivery;

/// One auxiliary delivery row decoded from durable state before recovery validation.
///
/// The row keeps raw outcome fields separate from [`AuxDelivery`] so decoding
/// cannot construct an authoritative outcome. Recovery validates the encoding
/// and discards the outcome because the row lacks its full-state observation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct UntrustedAuxDeliveryRow {
    delivery: AuxDelivery,
    outcome_status_code: u8,
    observation_digests: [Option<[u8; 32]>; 2],
    outcome_boundary_hash: Option<block::Hash>,
}

impl UntrustedAuxDeliveryRow {
    /// Construct one decoded row without promoting its outcome.
    pub const fn new(
        delivery: AuxDelivery,
        outcome_status_code: u8,
        observation_digests: [Option<[u8; 32]>; 2],
        outcome_boundary_hash: Option<block::Hash>,
    ) -> Self {
        Self {
            delivery,
            outcome_status_code,
            observation_digests,
            outcome_boundary_hash,
        }
    }

    /// Return the unauthenticated delivery fields.
    pub const fn delivery(self) -> AuxDelivery {
        self.delivery
    }

    /// Return the raw durable outcome status code.
    pub const fn outcome_status_code(self) -> u8 {
        self.outcome_status_code
    }

    /// Return the raw durable observation digests.
    pub const fn observation_digests(self) -> [Option<[u8; 32]>; 2] {
        self.observation_digests
    }

    /// Return the raw durable outcome boundary.
    pub const fn outcome_boundary_hash(self) -> Option<block::Hash> {
        self.outcome_boundary_hash
    }

    pub(crate) const fn into_parts(
        self,
    ) -> (AuxDelivery, u8, [Option<[u8; 32]>; 2], Option<block::Hash>) {
        (
            self.delivery,
            self.outcome_status_code,
            self.observation_digests,
            self.outcome_boundary_hash,
        )
    }
}
