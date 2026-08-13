//! Header-validation capabilities and sealed preparation evidence.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use zakura_chain::{
    block,
    parameters::NetworkKind,
    work::difficulty::{ParameterDifficulty, Work},
};

use crate::{EvidenceId, Frontier, HeaderValidationState};

use super::error::TransitionTypeError;

/// One immutable predecessor fact sealed into a validation lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderContextFact {
    /// Exact predecessor frontier.
    pub frontier: Frontier,
    /// Canonical predecessor header whose hash authenticates all contextual fields.
    pub header: Arc<block::Header>,
}

/// Exact branch-local context used to prepare a header batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationLease {
    /// Exact known parent.
    pub(crate) parent: Frontier,
    /// Up to 28 facts in reverse height order, beginning with `parent`.
    pub(crate) predecessors: Vec<HeaderContextFact>,
    /// Exact network policy used by the issuing engine.
    pub(crate) network: zakura_chain::parameters::Network,
    /// Digest of current trust anchors.
    pub(crate) trust_anchor_digest: [u8; 32],
    /// Digest binding the complete lease contents.
    pub(crate) context_digest: [u8; 32],
}

impl ValidationLease {
    /// Construct a lease digest bound to its exact ordered durable context.
    pub fn new(
        parent: Frontier,
        predecessors: Vec<HeaderContextFact>,
        network: zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-validation-lease-v1");
        hasher.update(parent.height.0.to_le_bytes());
        hasher.update(parent.hash.0);
        hasher.update(trust_anchor_digest);
        hash_network_policy(&mut hasher, &network);
        for fact in &predecessors {
            hasher.update(fact.frontier.height.0.to_le_bytes());
            hasher.update(fact.frontier.hash.0);
            hasher.update(fact.header.hash().0);
        }
        Self {
            parent,
            predecessors,
            network,
            trust_anchor_digest,
            context_digest: hasher.finalize().into(),
        }
    }

    /// Return the exact known parent.
    pub const fn parent(&self) -> Frontier {
        self.parent
    }

    /// Return the reverse-height predecessor context beginning with the parent.
    pub fn predecessors(&self) -> &[HeaderContextFact] {
        &self.predecessors
    }

    /// Return the exact authenticated network policy used to issue this lease.
    pub fn network(&self) -> &zakura_chain::parameters::Network {
        &self.network
    }

    /// Return the digest of the trust anchors used to issue this lease.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }

    /// Return the digest binding all lease contents.
    pub const fn context_digest(&self) -> [u8; 32] {
        self.context_digest
    }

    pub(crate) fn is_coherent(
        &self,
        network: &zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
    ) -> bool {
        let required = usize::try_from(self.parent.height.0)
            .ok()
            .and_then(|height| height.checked_add(1))
            .map(|height| height.min(crate::POW_ADJUSTMENT_BLOCK_SPAN));
        if self.network != *network
            || self.trust_anchor_digest != trust_anchor_digest
            || required != Some(self.predecessors.len())
            || self.predecessors.first().map(|fact| fact.frontier) != Some(self.parent)
        {
            return false;
        }
        for (index, fact) in self.predecessors.iter().enumerate() {
            if fact.header.hash() != fact.frontier.hash {
                return false;
            }
            if let Some(newer) = index
                .checked_sub(1)
                .and_then(|index| self.predecessors.get(index))
            {
                if newer.header.previous_block_hash != fact.frontier.hash
                    || newer.frontier.height.previous().ok() != Some(fact.frontier.height)
                {
                    return false;
                }
            }
        }
        Self::new(
            self.parent,
            self.predecessors.clone(),
            self.network.clone(),
            self.trust_anchor_digest,
        )
        .context_digest
            == self.context_digest
    }
}

/// One fully prepared observable-header result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedHeader {
    /// Canonical header.
    pub header: Arc<block::Header>,
    /// Locally computed hash.
    pub hash: block::Hash,
    /// Locally inferred height.
    pub height: block::Height,
    /// Exact per-block work.
    pub block_work: Work,
    /// Valid or locally future-deferred state.
    pub validation: HeaderValidationState,
}

/// Sealed evidence that preparation completed every graph-independent rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextFreePreparationReceipt {
    parent: Frontier,
    network: zakura_chain::parameters::Network,
    trust_anchor_digest: [u8; 32],
}

impl ContextFreePreparationReceipt {
    /// Return the caller-supplied parent used for height-dependent local rules.
    pub const fn parent(&self) -> Frontier {
        self.parent
    }

    /// Return the exact network policy used for graph-independent validation.
    pub fn network(&self) -> &zakura_chain::parameters::Network {
        &self.network
    }

    /// Return the authenticated immutable rule-set identity.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }
}

/// Sealed nonempty batch carrying explicit graph-independent validation evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedHeaderBatch {
    headers: Vec<PreparedHeader>,
    receipt: ContextFreePreparationReceipt,
    evidence: EvidenceId,
}

impl PreparedHeaderBatch {
    #[allow(dead_code)] // Called by the public preparation pipeline introduced in PR-11.
    pub(crate) fn new(
        headers: Vec<PreparedHeader>,
        parent: Frontier,
        network: zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
        evidence: EvidenceId,
    ) -> Result<Self, TransitionTypeError> {
        if headers.is_empty() {
            return Err(TransitionTypeError::EmptyHeaderBatch);
        }
        if headers.len() > crate::MAX_HEADERS_PER_TRANSITION_V1 {
            // Type-boundary constant check. Planning also enforces
            // `limits.max_headers_per_transition` in admission (authoritative for
            // the active engine). Unifying these gates is deferred.
            return Err(TransitionTypeError::OversizedHeaderBatch);
        }
        Ok(Self {
            headers,
            receipt: ContextFreePreparationReceipt {
                parent,
                network,
                trust_anchor_digest,
            },
            evidence,
        })
    }

    /// Return the prepared headers in exact parent-first order.
    pub fn headers(&self) -> &[PreparedHeader] {
        &self.headers
    }

    /// Return the sealed graph-independent preparation receipt.
    pub const fn receipt(&self) -> &ContextFreePreparationReceipt {
        &self.receipt
    }

    /// Return the batch's stable validation-evidence identity.
    pub const fn evidence(&self) -> EvidenceId {
        self.evidence
    }

    /// Derive the stable context-free batch evidence identity.
    ///
    /// Preparation and finality rebasing must share this exact encoding so a
    /// rebased suffix keeps the same evidence ID that fresh preparation would
    /// produce for the same parent, trust anchor, and header path.
    pub(crate) fn context_free_evidence(
        parent: Frontier,
        trust_anchor_digest: [u8; 32],
        headers: &[PreparedHeader],
    ) -> EvidenceId {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-context-free-batch-v1");
        hasher.update(parent.height.0.to_le_bytes());
        hasher.update(parent.hash.0);
        hasher.update(trust_anchor_digest);
        for header in headers {
            hasher.update(header.height.0.to_le_bytes());
            hasher.update(header.hash.0);
        }
        EvidenceId::from_digest(hasher.finalize().into())
    }

    /// Rebase this sealed batch after an exact prepared header that became finalized.
    ///
    /// The remaining headers retain their validated results and absolute heights.
    /// The method reseals the suffix to the now-durable parent.
    /// The method returns the removed header count.
    pub(crate) fn rebase_after(&mut self, parent: Frontier) -> Result<usize, TransitionTypeError> {
        if self.receipt.parent == parent {
            return Ok(0);
        }
        let Some(index) = self
            .headers
            .iter()
            .position(|header| Frontier::new(header.height, header.hash) == parent)
        else {
            return Err(TransitionTypeError::InvalidPreparedRebase);
        };
        let removed = index.saturating_add(1);
        self.headers.drain(..removed);
        self.receipt.parent = parent;
        self.evidence =
            Self::context_free_evidence(parent, self.receipt.trust_anchor_digest, &self.headers);
        Ok(removed)
    }

    pub(crate) fn clear_already_applied(&mut self) {
        self.headers.clear();
    }
}

/// Hash the authenticated network policy into a replay or lease digest.
pub(super) fn hash_network_policy(
    hasher: &mut Sha256,
    network: &zakura_chain::parameters::Network,
) {
    hasher.update([match network.kind() {
        NetworkKind::Mainnet => 0,
        NetworkKind::Testnet => 1,
        NetworkKind::Regtest => 2,
    }]);
    hasher.update(network.genesis_hash().0);
    let target: zakura_chain::work::difficulty::U256 = network.target_difficulty_limit().into();
    hasher.update(target.to_big_endian());
    hasher.update([u8::from(network.disable_pow())]);
    let max_time_height = match network {
        zakura_chain::parameters::Network::Mainnet => block::Height::MIN,
        zakura_chain::parameters::Network::Testnet(parameters) => {
            parameters.max_block_time_start_height()
        }
    };
    hasher.update(max_time_height.0.to_le_bytes());
    for (height, upgrade) in network.activation_list() {
        hasher.update(height.0.to_le_bytes());
        let (branch_tag, upgrade_code) = match upgrade.branch_id() {
            Some(branch) => (1_u8, u32::from(branch)),
            None => (
                0,
                match upgrade {
                    zakura_chain::parameters::NetworkUpgrade::Genesis => 0,
                    zakura_chain::parameters::NetworkUpgrade::BeforeOverwinter => 1,
                    zakura_chain::parameters::NetworkUpgrade::Nu7 => 2,
                    _ => u32::MAX,
                },
            ),
        };
        hasher.update([branch_tag]);
        hasher.update(upgrade_code.to_le_bytes());
    }
}
