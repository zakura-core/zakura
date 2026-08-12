//! Header-insertion command and target-completion contracts.

use zakura_chain::block;

use crate::{Frontier, HeaderSyncWorkOwner, SourceId};

use super::super::auxiliary::PreparedAuxDelivery;
use super::super::error::TransitionTypeError;
use super::super::preparation::PreparedHeaderBatch;

/// Completion contract attached to one atomic header insertion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TargetCompletion {
    /// Header sync completed the peer-advertised target from this exact common ancestor.
    TargetComplete {
        /// Exact locator intersection.
        common_ancestor: Frontier,
    },
    /// Header sync completed a bounded prefix of a larger peer-advertised target.
    ///
    /// Prefix admission bounds requester memory while preserving exact validation and
    /// ownership for the last header actually supplied in this batch.
    TargetPrefix {
        /// Exact locator intersection.
        common_ancestor: Frontier,
    },
    /// A peer redelivered one selected interior header only to replace auxiliary metadata.
    SelectedAuxiliaryRepair {
        /// Exact selected predecessor used as the single-entry locator.
        common_ancestor: Frontier,
        /// Exact selected header whose metadata the peer redelivered.
        selected_target: Frontier,
    },
}

impl TargetCompletion {
    pub(crate) fn rebase_common_ancestor(
        &mut self,
        common_ancestor: Frontier,
    ) -> Result<(), TransitionTypeError> {
        match self {
            Self::TargetComplete {
                common_ancestor: current,
            }
            | Self::TargetPrefix {
                common_ancestor: current,
            } => {
                *current = common_ancestor;
                Ok(())
            }
            Self::SelectedAuxiliaryRepair { .. } => Err(TransitionTypeError::InvalidPreparedRebase),
        }
    }
}

/// Atomically insert one complete prepared header range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertHeaders {
    /// Current asynchronous work owner.
    pub owner: HeaderSyncWorkOwner,
    /// Header supplier.
    pub source: SourceId,
    /// Exact retained parent.
    pub parent_hash: block::Hash,
    /// Exact pursued target.
    pub target_tip_hash: block::Hash,
    /// Target completion proof kind.
    pub completion: TargetCompletion,
    /// Sealed header validation evidence.
    pub batch: PreparedHeaderBatch,
    /// Exact parallel hash-keyed auxiliary deliveries.
    pub aux: Vec<PreparedAuxDelivery>,
}
