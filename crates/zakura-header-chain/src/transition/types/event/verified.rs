//! Full-state verified-path evidence payloads.

use std::sync::Arc;

use zakura_chain::block;

use crate::{EvidenceId, Frontier};

/// One exact header reference accepted by full state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedHeaderRef {
    /// Exact height.
    pub height: block::Height,
    /// Exact locally computed hash.
    pub hash: block::Hash,
    /// Canonical header.
    pub header: Arc<block::Header>,
}

/// Explicit full-state selected-path change kind.
/// Height never determines the change kind.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VerifiedChangeCause {
    /// Direct or forward growth.
    Grow,
    /// Checkpoint-verified growth that atomically advances integrated full-state finality.
    CheckpointFinalizedGrow,
    /// Same-height, lower-height, or forward-height branch reset.
    Reset,
}

/// Authenticated full-state selected-path transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedChainChanged {
    /// Internal full-state transition identity and authority proof.
    pub full_state_transition_id: EvidenceId,
    /// Exact previously selected verified tip.
    pub old_tip: Frontier,
    /// Continuous new verified suffix, possibly empty back to finalized.
    pub new_path: Vec<VerifiedHeaderRef>,
    /// Explicit branch-aware grow/reset cause.
    pub cause: VerifiedChangeCause,
}

/// Full-state acceptance of a block outside the verified winning path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedBlockAccepted {
    /// Internal full-state transition identity and authority proof.
    pub full_state_transition_id: EvidenceId,
    /// Exact finalized-rooted path through the accepted block.
    pub path: Vec<VerifiedHeaderRef>,
}
