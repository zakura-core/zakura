//! Stable transition domains, replay fingerprints, and canonical payload hashing.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::{
    BodyUnavailableSummary, BodyWorkOwner, EvidenceId, Frontier, HeaderSyncWorkOwner,
    HeaderValidationState,
};

use super::super::auxiliary::{AuxAuthentication, AuxDelivery, BodySizeHint};
use super::super::preparation::hash_network_policy;
use super::body::{BodyCommitmentKind, BodyEvidence, TransientBodyFailureKind};
use super::header::TargetCompletion;
use super::verified::{VerifiedChangeCause, VerifiedHeaderRef};
use super::TransitionEvent;

/// Stable domain of one submitted transition input.
///
/// Replay-protected domains keep their version-one disk discriminants.
/// [`Self::ReevaluateDeferred`] is submitted without a durable fingerprint and
/// therefore uses an appended code that is never persisted in
/// [`TransitionFingerprint`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransitionDomain {
    /// Prepared header admission.
    InsertHeaders,
    /// Full-state selected-path replacement.
    VerifiedChainChanged,
    /// Full-state side-path acceptance.
    VerifiedBlockAccepted,
    /// Supplier-attributed body payload mismatch.
    BodyPayloadMismatch,
    /// Deterministic body invalidity.
    ConsensusBodyInvalid,
    /// Transient body failure.
    TransientBodyFailure,
    /// Verified body acceptance.
    VerifiedBody,
    /// Body supplier-set discovery.
    BodySupplierDiscovered,
    /// Scheduler/operator body retry.
    OperatorBodyRetry,
    /// Operator invalidation.
    OperatorInvalidate,
    /// Operator reconsideration.
    OperatorReconsider,
    /// Full-state finality.
    FullStateFinalized,
    /// Migrated-pin refutation.
    MigratedPinRefutation,
    /// Auxiliary authentication evidence.
    AuxEvidence,
    /// Local reevaluation of due future-time deferrals.
    ReevaluateDeferred,
}

impl TransitionDomain {
    /// Return the stable version-one disk discriminant.
    pub const fn code(self) -> u8 {
        match self {
            Self::InsertHeaders => 0,
            Self::VerifiedChainChanged => 1,
            Self::VerifiedBlockAccepted => 2,
            Self::BodyPayloadMismatch => 3,
            Self::ConsensusBodyInvalid => 4,
            Self::TransientBodyFailure => 5,
            Self::VerifiedBody => 6,
            Self::BodySupplierDiscovered => 7,
            Self::OperatorBodyRetry => 8,
            Self::OperatorInvalidate => 9,
            Self::OperatorReconsider => 10,
            Self::FullStateFinalized => 11,
            Self::MigratedPinRefutation => 12,
            Self::AuxEvidence => 13,
            // Appended after the replay-protected set; never persisted in fingerprints.
            Self::ReevaluateDeferred => 14,
        }
    }

    /// Decode a stable version-one disk discriminant.
    pub const fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::InsertHeaders,
            1 => Self::VerifiedChainChanged,
            2 => Self::VerifiedBlockAccepted,
            3 => Self::BodyPayloadMismatch,
            4 => Self::ConsensusBodyInvalid,
            5 => Self::TransientBodyFailure,
            6 => Self::VerifiedBody,
            7 => Self::BodySupplierDiscovered,
            8 => Self::OperatorBodyRetry,
            9 => Self::OperatorInvalidate,
            10 => Self::OperatorReconsider,
            11 => Self::FullStateFinalized,
            12 => Self::MigratedPinRefutation,
            13 => Self::AuxEvidence,
            14 => Self::ReevaluateDeferred,
            _ => return None,
        })
    }
}

/// Exact replay identity of one committed transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransitionFingerprint {
    domain: TransitionDomain,
    evidence: EvidenceId,
    payload_digest: [u8; 32],
}

impl TransitionFingerprint {
    /// Reconstruct one persisted fingerprint from its canonical fields.
    pub const fn from_parts(
        domain: TransitionDomain,
        evidence: EvidenceId,
        payload_digest: [u8; 32],
    ) -> Self {
        Self {
            domain,
            evidence,
            payload_digest,
        }
    }

    /// Return the stable event domain.
    pub const fn domain(self) -> TransitionDomain {
        self.domain
    }

    /// Return the domain-local idempotency evidence.
    pub const fn evidence(self) -> EvidenceId {
        self.evidence
    }

    /// Return the canonical effect-bearing payload digest.
    pub const fn payload_digest(self) -> [u8; 32] {
        self.payload_digest
    }

    /// True when two events reuse one domain-local key with different effects.
    pub fn conflicts_with(self, other: Self) -> bool {
        self.domain.code() == other.domain.code()
            && self.evidence.digest() == other.evidence.digest()
            && self.payload_digest != other.payload_digest
    }
}

/// Authority/mode gate checked before any transition effect.
///
/// See [`crate::FullStateEvidenceAuthority`] for the capability matrix that
/// [`crate::TransitionContext`] enforces for each variant.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EventAdmission {
    /// Valid in integrated and headers-only modes.
    AnyMode,
    /// Requires authenticated integrated full-state authority.
    IntegratedFullState,
    /// Requires an exact retry action staged by the serialized scheduler boundary.
    RegisteredScheduler,
    /// Requires an exact header completion registered by the serialized authority boundary.
    RegisteredHeaderCompletion,
}

pub(super) fn hash_transition_payload(hasher: &mut Sha256, event: &TransitionEvent) {
    match event {
        TransitionEvent::InsertHeaders(event) => {
            hash_sync_owner(hasher, event.owner);
            hasher.update(event.source.digest());
            hasher.update(event.parent_hash.0);
            hasher.update(event.target_tip_hash.0);
            match event.completion {
                TargetCompletion::TargetComplete { common_ancestor } => {
                    hasher.update([0]);
                    hash_frontier(hasher, common_ancestor);
                }
                TargetCompletion::TargetPrefix { common_ancestor } => {
                    hasher.update([1]);
                    hash_frontier(hasher, common_ancestor);
                }
                TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor,
                    selected_target,
                } => {
                    hasher.update([2]);
                    hash_frontier(hasher, common_ancestor);
                    hash_frontier(hasher, selected_target);
                }
            }
            let receipt = event.batch.receipt();
            hash_frontier(hasher, receipt.parent());
            hasher.update(receipt.trust_anchor_digest());
            hash_network_policy(hasher, receipt.network());
            for header in event.batch.headers() {
                hasher.update(header.height.0.to_le_bytes());
                hasher.update(header.hash.0);
                hasher.update(header.block_work.as_u256().to_big_endian());
                hash_validation_state(hasher, header.validation);
            }
            for delivery in &event.aux {
                hash_aux_delivery(hasher, *delivery);
            }
        }
        TransitionEvent::VerifiedChainChanged(event) => {
            hash_frontier(hasher, event.old_tip);
            hasher.update([match event.cause {
                VerifiedChangeCause::Grow => 0,
                VerifiedChangeCause::Reset => 1,
                VerifiedChangeCause::CheckpointFinalizedGrow => 2,
            }]);
            hash_verified_path(hasher, &event.new_path);
        }
        TransitionEvent::VerifiedBlockAccepted(event) => hash_verified_path(hasher, &event.path),
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => {
            hasher.update(event.requested.0);
            hasher.update(event.delivered.0);
            hasher.update(event.source.digest());
            match event.kind {
                BodyCommitmentKind::HeaderHash => hasher.update([0]),
                BodyCommitmentKind::TransactionMerkleRoot => hasher.update([1]),
                BodyCommitmentKind::AuthDataRoot => hasher.update([2]),
                BodyCommitmentKind::Other(rule) => {
                    hasher.update([3]);
                    hash_bytes(hasher, rule.as_bytes());
                }
            }
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            hasher.update(event.hash.0);
            hash_bytes(hasher, event.rule.as_str().as_bytes());
            hasher.update(event.source.digest());
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            hasher.update(event.hash.0);
            hasher.update([match event.kind {
                TransientBodyFailureKind::MissingContext => 0,
                TransientBodyFailureKind::Canceled => 1,
                TransientBodyFailureKind::Storage => 2,
                TransientBodyFailureKind::VerifierUnavailable => 3,
                TransientBodyFailureKind::Timeout => 4,
                TransientBodyFailureKind::ResourceExhausted => 5,
            }]);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            hasher.update(event.hash.0);
        }
        TransitionEvent::BodySupplierDiscovered(event) => {
            hasher.update(event.hash.0);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::OperatorBodyRetry(event) => {
            hasher.update(event.hash.0);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::OperatorInvalidate(event) => {
            hasher.update(event.target.0);
            hasher.update(event.id.bytes());
            hasher.update(event.operator_reason_digest);
        }
        TransitionEvent::OperatorReconsider(event) => {
            hasher.update(event.target.0);
            hasher.update(event.id.bytes());
            match event.invalidation_evidence {
                Some(evidence) => {
                    hasher.update([1]);
                    hasher.update(evidence.digest());
                }
                None => hasher.update([0]),
            }
        }
        TransitionEvent::FullStateFinalized(event) => {
            hash_frontier(hasher, event.new_finalized);
            for hash in &event.verified_path_proof {
                hasher.update(hash.0);
            }
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            hash_frontier(hasher, event.pin);
            hash_frontier(hasher, event.invalid_header);
            hash_bytes(hasher, event.rule.as_str().as_bytes());
        }
        TransitionEvent::AuxEvidence(event) => {
            hash_body_owner(hasher, event.owner);
            for delivery in &event.deliveries {
                hash_aux_delivery(hasher, *delivery);
            }
            hash_aux_authentication(hasher, event.authentication);
        }
        TransitionEvent::ReevaluateDeferred => {}
    }
}

fn hash_frontier(hasher: &mut Sha256, frontier: Frontier) {
    hasher.update(frontier.height.0.to_le_bytes());
    hasher.update(frontier.hash.0);
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(
        u64::try_from(bytes.len())
            .expect("in-memory payload length fits in u64")
            .to_le_bytes(),
    );
    hasher.update(bytes);
}

fn hash_time(hasher: &mut Sha256, time: DateTime<Utc>) {
    hasher.update(time.timestamp().to_le_bytes());
    hasher.update(time.timestamp_subsec_nanos().to_le_bytes());
}

fn hash_validation_state(hasher: &mut Sha256, validation: HeaderValidationState) {
    match validation {
        HeaderValidationState::Valid => hasher.update([0]),
        HeaderValidationState::DeferredUntil(until) => {
            hasher.update([1]);
            hash_time(hasher, until);
        }
    }
}

fn hash_header_owner(hasher: &mut Sha256, owner: crate::HeaderWorkOwner) {
    hasher.update(owner.authority.header_generation.get().to_le_bytes());
    hasher.update(owner.authority.branch.anchor_hash.0);
    hasher.update(owner.authority.branch.target_tip_hash.0);
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
}

fn hash_body_owner(hasher: &mut Sha256, owner: BodyWorkOwner) {
    hash_header_owner(
        hasher,
        crate::HeaderWorkOwner {
            authority: owner.authority.header,
            session_id: owner.session_id,
            request_id: owner.request_id,
        },
    );
    hasher.update(owner.authority.verified_generation.get().to_le_bytes());
}

fn hash_sync_owner(hasher: &mut Sha256, owner: HeaderSyncWorkOwner) {
    match owner {
        HeaderSyncWorkOwner::Header(owner) => {
            hasher.update([0]);
            hash_header_owner(hasher, owner);
        }
        HeaderSyncWorkOwner::BodyRepair(owner) => {
            hasher.update([1]);
            hash_body_owner(hasher, owner);
        }
    }
}

fn hash_availability(hasher: &mut Sha256, availability: BodyUnavailableSummary) {
    hash_time(hasher, availability.started_at);
    hasher.update(availability.attempts.to_le_bytes());
    hasher.update(availability.suppliers.to_le_bytes());
    hasher.update(availability.supplier_set_digest);
    hasher.update([u8::from(availability.alarmed)]);
    hash_time(hasher, availability.next_probe_at);
}

fn hash_verified_path(hasher: &mut Sha256, path: &[VerifiedHeaderRef]) {
    for header in path {
        hasher.update(header.height.0.to_le_bytes());
        hasher.update(header.hash.0);
        hasher.update(header.header.hash().0);
    }
}

fn hash_aux_delivery(hasher: &mut Sha256, delivery: AuxDelivery) {
    hasher.update(delivery.delivery_id.digest());
    hasher.update(delivery.header_hash.0);
    hasher.update(delivery.source.digest());
    hash_sync_owner(hasher, delivery.owner);
    hasher.update(
        match delivery.body_size {
            BodySizeHint::Unknown => 0_u32,
            BodySizeHint::Known(size) => size.get(),
        }
        .to_le_bytes(),
    );
    match delivery.tree_aux {
        None => hasher.update([0]),
        Some(aux) => {
            hasher.update([1]);
            hasher.update(aux.height.0.to_le_bytes());
            hasher.update(<[u8; 32]>::from(aux.sapling_root));
            hasher.update(<[u8; 32]>::from(aux.orchard_root));
            hasher.update(<[u8; 32]>::from(aux.ironwood_root));
            hasher.update(aux.sapling_tx_count.to_le_bytes());
            hasher.update(aux.orchard_tx_count.to_le_bytes());
            hasher.update(aux.ironwood_tx_count.to_le_bytes());
            hasher.update(<[u8; 32]>::from(aux.auth_data_root));
        }
    }
    hash_aux_authentication(hasher, delivery.authentication);
}

fn hash_aux_authentication(hasher: &mut Sha256, authentication: AuxAuthentication) {
    match authentication {
        AuxAuthentication::Unauthenticated => hasher.update([0]),
        AuxAuthentication::Authenticated {
            evidence,
            boundary_hash,
        } => {
            hasher.update([1]);
            hasher.update(evidence.digest());
            hasher.update(boundary_hash.0);
        }
        AuxAuthentication::Rejected { evidence } => {
            hasher.update([2]);
            hasher.update(evidence.digest());
        }
        AuxAuthentication::Disputed { evidence } => {
            hasher.update([3]);
            hasher.update(evidence.digest());
        }
    }
}
