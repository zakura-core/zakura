//! Integrated finality and migrated-pin refutation evidence.

use zakura_chain::block;

use crate::{BodyRuleId, EvidenceId, Frontier};

/// Authenticated integrated-mode finality evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullStateFinalized {
    /// Internal full-state transition identity.
    pub full_state_transition_id: EvidenceId,
    /// Exact nonretreating finalized frontier.
    pub new_finalized: Frontier,
    /// Exact verified ancestry proof ending at `new_finalized`.
    pub verified_path_proof: Vec<block::Hash>,
}

/// Deterministic full-state evidence that refutes an imported headers-only trust pin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigratedPinRefutation {
    /// Stable internal full-state transition identity.
    pub full_state_transition_id: EvidenceId,
    /// Exact preserved headers-only pin whose ancestry full state refuted.
    pub pin: Frontier,
    /// Exact body-invalid header on the imported path at or below `pin`.
    pub invalid_header: Frontier,
    /// Exact deterministic full-state rule.
    pub rule: BodyRuleId,
}
