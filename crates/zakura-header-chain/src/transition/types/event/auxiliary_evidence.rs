//! Hash-scoped auxiliary verification observations.

use sha2::{Digest, Sha256};
use zakura_chain::block::merkle::AuthDataRoot;

use crate::{AuxObservationId, BodySizeHint, BodyWorkOwner, HeaderSyncWorkOwner};

use super::super::auxiliary::PreparedAuxDelivery;

/// Exact VCT verification fact reported by integrated state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AuxVerificationFactV1 {
    kind: AuxVerificationKindV1,
    failure_code: u8,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuxVerificationKindV1 {
    CurrentVerified,
    CurrentFailed,
    SuccessorFailed,
    AmbiguousFailed,
}

impl AuxVerificationFactV1 {
    /// Record successful verification of the current delivery.
    pub const fn current_delivery_verified() -> Self {
        Self {
            kind: AuxVerificationKindV1::CurrentVerified,
            failure_code: 0,
        }
    }

    /// Record a failure attributed to the current delivery.
    pub const fn current_delivery_failed(failure_code: u8) -> Self {
        Self {
            kind: AuxVerificationKindV1::CurrentFailed,
            failure_code,
        }
    }

    /// Record a failure attributed to the successor delivery.
    pub const fn successor_delivery_failed(failure_code: u8) -> Self {
        Self {
            kind: AuxVerificationKindV1::SuccessorFailed,
            failure_code,
        }
    }

    /// Record a failure that cannot be attributed between two deliveries.
    pub const fn ambiguous_deliveries_failed(failure_code: u8) -> Self {
        Self {
            kind: AuxVerificationKindV1::AmbiguousFailed,
            failure_code,
        }
    }

    pub(crate) const fn kind(self) -> AuxVerificationKindV1 {
        self.kind
    }

    pub(crate) const fn failure_code(self) -> u8 {
        self.failure_code
    }
}

/// Sealed schema-1 auxiliary observation.
///
/// The observation records verification facts. The planner derives the boundary
/// and outcome from the retained owned branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxObservationV1 {
    owner: BodyWorkOwner,
    deliveries: Vec<PreparedAuxDelivery>,
    verification: AuxVerificationFactV1,
    boundary_witness: Option<AuthDataRoot>,
    observation_id: AuxObservationId,
}

impl AuxObservationV1 {
    /// Construct one checked VCT observation for the state adapter.
    pub fn from_vct(
        owner: BodyWorkOwner,
        deliveries: Vec<PreparedAuxDelivery>,
        verification: AuxVerificationFactV1,
        boundary_witness: Option<AuthDataRoot>,
    ) -> Option<Self> {
        let expected = match verification.kind() {
            AuxVerificationKindV1::AmbiguousFailed => 2,
            AuxVerificationKindV1::CurrentVerified
            | AuxVerificationKindV1::CurrentFailed
            | AuxVerificationKindV1::SuccessorFailed => 1,
        };
        if deliveries.len() != expected {
            return None;
        }
        if deliveries.len() == 2 && deliveries[0].delivery_id == deliveries[1].delivery_id {
            return None;
        }
        let observation_id =
            derive_observation_id(owner, &deliveries, verification, boundary_witness);
        Some(Self {
            owner,
            deliveries,
            verification,
            boundary_witness,
            observation_id,
        })
    }

    /// Return the exact owner recorded by the observation.
    pub const fn owner(&self) -> BodyWorkOwner {
        self.owner
    }

    /// Return the exact immutable deliveries recorded by the observation.
    pub fn deliveries(&self) -> &[PreparedAuxDelivery] {
        &self.deliveries
    }

    /// Return the verification facts recorded by integrated state.
    pub const fn verification(&self) -> AuxVerificationFactV1 {
        self.verification
    }

    /// Return the exact boundary witness, when integrated state supplied one.
    pub const fn boundary_witness(&self) -> Option<AuthDataRoot> {
        self.boundary_witness
    }

    /// Return the identity derived from the complete observation proof.
    pub const fn observation_id(&self) -> AuxObservationId {
        self.observation_id
    }
}

fn derive_observation_id(
    owner: BodyWorkOwner,
    deliveries: &[PreparedAuxDelivery],
    verification: AuxVerificationFactV1,
    boundary_witness: Option<AuthDataRoot>,
) -> AuxObservationId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura.header-chain.aux-observation.v1");
    hash_owner(&mut hasher, owner);
    hasher.update([1]);
    hasher.update([u8::try_from(deliveries.len()).expect("observations contain at most two rows")]);
    for delivery in deliveries {
        hasher.update(delivery.delivery_id.digest());
        hasher.update(delivery.header_hash.0);
        hasher.update(delivery.source.digest());
        hash_sync_owner(&mut hasher, delivery.owner);
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
    }
    match verification.kind() {
        AuxVerificationKindV1::CurrentVerified => hasher.update([0]),
        AuxVerificationKindV1::CurrentFailed => {
            hasher.update([1, verification.failure_code()]);
        }
        AuxVerificationKindV1::SuccessorFailed => {
            hasher.update([2, verification.failure_code()]);
        }
        AuxVerificationKindV1::AmbiguousFailed => {
            hasher.update([3, verification.failure_code()]);
        }
    }
    match boundary_witness {
        Some(witness) => {
            hasher.update([1]);
            hasher.update(<[u8; 32]>::from(witness));
        }
        None => hasher.update([0]),
    }
    AuxObservationId::from_digest(hasher.finalize().into())
}

fn hash_owner(hasher: &mut Sha256, owner: BodyWorkOwner) {
    hasher.update(owner.authority.header.header_generation.get().to_le_bytes());
    hasher.update(owner.authority.header.branch.anchor_hash.0);
    hasher.update(owner.authority.header.branch.target_tip_hash.0);
    hasher.update(owner.authority.verified_generation.get().to_le_bytes());
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
}

fn hash_sync_owner(hasher: &mut Sha256, owner: HeaderSyncWorkOwner) {
    match owner {
        HeaderSyncWorkOwner::Header(owner) => {
            hasher.update([0]);
            hasher.update(owner.authority.header_generation.get().to_le_bytes());
            hasher.update(owner.authority.branch.anchor_hash.0);
            hasher.update(owner.authority.branch.target_tip_hash.0);
            hasher.update(owner.session_id.to_le_bytes());
            hasher.update(owner.request_id.get().to_le_bytes());
        }
        HeaderSyncWorkOwner::BodyRepair(owner) => {
            hasher.update([1]);
            hash_owner(hasher, owner);
        }
    }
}

/// Auxiliary metadata observation update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxEvidence {
    observation: Option<AuxObservationV1>,
}

impl AuxEvidence {
    /// Wrap a checked observation.
    pub const fn observed(observation: AuxObservationV1) -> Self {
        Self {
            observation: Some(observation),
        }
    }

    /// Represent an absent observation. The planner derives no durable mutation.
    pub const fn missing() -> Self {
        Self { observation: None }
    }

    /// Return the checked observation, when one was supplied.
    pub const fn observation(&self) -> Option<&AuxObservationV1> {
        self.observation.as_ref()
    }
}
