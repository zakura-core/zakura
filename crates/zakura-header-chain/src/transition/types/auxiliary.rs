//! Hash-keyed auxiliary delivery records and advisory body-size metadata.

use std::num::NonZeroU32;

use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard, sapling,
};

use crate::{EvidenceId, HeaderSyncWorkOwner, SourceId};

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

/// Authentication state of one hash-keyed auxiliary delivery.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AuxAuthentication {
    /// Peer metadata has no selection or validity authority.
    Unauthenticated,
    /// Integrated verification authenticated this exact delivery.
    Authenticated {
        /// Stable authentication evidence.
        evidence: EvidenceId,
        /// One-header-later authentication boundary.
        boundary_hash: block::Hash,
    },
    /// Authentication rejected this delivery without invalidating its header.
    Rejected {
        /// Stable rejection evidence.
        evidence: EvidenceId,
    },
    /// Verification failed at a boundary that combines two untrusted deliveries.
    ///
    /// The evidence does not identify which delivery is invalid. Later evidence can refine this
    /// state to [`AuxAuthentication::Authenticated`] or [`AuxAuthentication::Rejected`].
    Disputed {
        /// Stable evidence that binds both deliveries to the failed boundary.
        evidence: EvidenceId,
    },
}

impl AuxAuthentication {
    /// Return whether new evidence can refine this authentication state to `next_state`.
    ///
    /// Evidence cannot restore an unauthenticated state or replace a terminal state.
    pub(crate) fn can_refine_to(self, next_state: Self) -> bool {
        matches!(
            (self, next_state),
            (Self::Unauthenticated, next_state) if next_state != Self::Unauthenticated
        ) || matches!(
            (self, next_state),
            (
                Self::Disputed { .. },
                Self::Authenticated { .. } | Self::Rejected { .. }
            )
        )
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
    /// Current authentication state.
    pub authentication: AuxAuthentication,
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
