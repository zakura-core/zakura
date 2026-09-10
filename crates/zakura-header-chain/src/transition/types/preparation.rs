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
pub(crate) fn hash_network_policy(
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

#[cfg(test)]
mod tests {
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };

    use super::*;

    const TRUST_ANCHOR_DIGEST: [u8; 32] = [0x5a; 32];

    fn context_chain(max_height: u32) -> Vec<HeaderContextFact> {
        let genesis = regtest_genesis_block();
        let mut facts = vec![HeaderContextFact {
            frontier: Frontier::new(block::Height(0), genesis.hash()),
            header: genesis.header.clone(),
        }];
        for height in 1..=max_height {
            let mut header = *genesis.header;
            header.previous_block_hash = facts
                .last()
                .expect("the context chain starts at genesis")
                .frontier
                .hash;
            let mut nonce = [0; 32];
            nonce[..4].copy_from_slice(&height.to_le_bytes());
            header.nonce = nonce.into();
            let header = Arc::new(header);
            facts.push(HeaderContextFact {
                frontier: Frontier::new(block::Height(height), header.hash()),
                header,
            });
        }
        facts
    }

    fn lease_at(height: u32) -> ValidationLease {
        let facts = context_chain(height);
        let parent = facts
            .last()
            .expect("the context chain includes genesis")
            .frontier;
        let required = usize::try_from(height)
            .expect("the fixture height fits in memory")
            .saturating_add(1)
            .min(crate::POW_ADJUSTMENT_BLOCK_SPAN);
        ValidationLease::new(
            parent,
            facts.into_iter().rev().take(required).collect(),
            Network::new_regtest(RegtestParameters::default()),
            TRUST_ANCHOR_DIGEST,
        )
    }

    #[test]
    fn validation_lease_coherence_enforces_context_boundaries() {
        let network = Network::new_regtest(RegtestParameters::default());
        for (height, expected_len) in [(0, 1), (27, 28), (28, 28), (40, 28)] {
            let lease = lease_at(height);
            assert_eq!(lease.predecessors.len(), expected_len, "height {height}");
            assert!(
                lease.is_coherent(&network, TRUST_ANCHOR_DIGEST),
                "height {height}"
            );
        }

        let baseline = lease_at(2);
        let mut wrong_digest = baseline.clone();
        wrong_digest.context_digest[0] ^= 0xff;

        let mut wrong_first = baseline.predecessors.clone();
        wrong_first.swap(0, 1);
        let wrong_first = ValidationLease::new(
            baseline.parent,
            wrong_first,
            network.clone(),
            TRUST_ANCHOR_DIGEST,
        );

        let short = ValidationLease::new(
            baseline.parent,
            baseline.predecessors[..2].to_vec(),
            network.clone(),
            TRUST_ANCHOR_DIGEST,
        );

        let mut hash_mismatch = baseline.predecessors.clone();
        hash_mismatch[1].frontier.hash.0[0] ^= 0xff;
        let hash_mismatch = ValidationLease::new(
            baseline.parent,
            hash_mismatch,
            network.clone(),
            TRUST_ANCHOR_DIGEST,
        );

        let mut broken_link = baseline.predecessors.clone();
        let mut alternate = *regtest_genesis_block().header;
        alternate.previous_block_hash = broken_link[2].frontier.hash;
        alternate.nonce = [0xee; 32].into();
        let alternate = Arc::new(alternate);
        broken_link[1] = HeaderContextFact {
            frontier: Frontier::new(block::Height(1), alternate.hash()),
            header: alternate,
        };
        let broken_link = ValidationLease::new(
            baseline.parent,
            broken_link,
            network.clone(),
            TRUST_ANCHOR_DIGEST,
        );

        let cases = [
            ("digest", wrong_digest, network.clone(), TRUST_ANCHOR_DIGEST),
            (
                "first predecessor",
                wrong_first,
                network.clone(),
                TRUST_ANCHOR_DIGEST,
            ),
            (
                "context length",
                short,
                network.clone(),
                TRUST_ANCHOR_DIGEST,
            ),
            (
                "header hash",
                hash_mismatch,
                network.clone(),
                TRUST_ANCHOR_DIGEST,
            ),
            (
                "parent link",
                broken_link,
                network.clone(),
                TRUST_ANCHOR_DIGEST,
            ),
            (
                "network",
                baseline.clone(),
                Network::Mainnet,
                TRUST_ANCHOR_DIGEST,
            ),
            ("trust anchor", baseline, network, [0xa5; 32]),
        ];
        for (name, lease, expected_network, expected_trust_anchor) in cases {
            assert!(
                !lease.is_coherent(&expected_network, expected_trust_anchor),
                "{name}"
            );
        }
    }

    fn prepared_headers(count: usize) -> (Frontier, Vec<PreparedHeader>) {
        let genesis = regtest_genesis_block();
        let parent = Frontier::new(block::Height(0), genesis.hash());
        let mut parent_hash = parent.hash;
        let mut headers = Vec::with_capacity(count);
        for offset in 1..=count {
            let height = u32::try_from(offset).expect("the fixture count fits in u32");
            let mut header = *genesis.header;
            header.previous_block_hash = parent_hash;
            let mut nonce = [0; 32];
            nonce[..4].copy_from_slice(&height.to_le_bytes());
            header.nonce = nonce.into();
            let header = Arc::new(header);
            let hash = header.hash();
            headers.push(PreparedHeader {
                header: header.clone(),
                hash,
                height: block::Height(height),
                block_work: header
                    .difficulty_threshold
                    .to_work()
                    .expect("the regtest target has valid work"),
                validation: HeaderValidationState::Valid,
            });
            parent_hash = hash;
        }
        (parent, headers)
    }

    fn prepared_batch(count: usize) -> Result<PreparedHeaderBatch, TransitionTypeError> {
        let (parent, headers) = prepared_headers(count);
        let evidence =
            PreparedHeaderBatch::context_free_evidence(parent, TRUST_ANCHOR_DIGEST, &headers);
        PreparedHeaderBatch::new(
            headers,
            parent,
            Network::new_regtest(RegtestParameters::default()),
            TRUST_ANCHOR_DIGEST,
            evidence,
        )
    }

    #[test]
    fn prepared_batch_constructor_enforces_frozen_size_boundaries() {
        assert_eq!(
            prepared_batch(0),
            Err(TransitionTypeError::EmptyHeaderBatch)
        );
        assert_eq!(
            prepared_batch(crate::MAX_HEADERS_PER_TRANSITION_V1)
                .expect("the exact frozen maximum is accepted")
                .headers
                .len(),
            crate::MAX_HEADERS_PER_TRANSITION_V1
        );
        assert_eq!(
            prepared_batch(crate::MAX_HEADERS_PER_TRANSITION_V1 + 1),
            Err(TransitionTypeError::OversizedHeaderBatch)
        );
    }

    #[test]
    fn prepared_batch_rebase_is_atomic_and_reseals_the_suffix() {
        let original = prepared_batch(3).expect("the fixture batch is nonempty");
        let unchanged_parent = original.receipt.parent;
        let first = Frontier::new(original.headers[0].height, original.headers[0].hash);
        let final_header = Frontier::new(original.headers[2].height, original.headers[2].hash);
        let missing = Frontier::new(block::Height(9), block::Hash([0x99; 32]));

        let mut unchanged = original.clone();
        assert_eq!(unchanged.rebase_after(unchanged_parent), Ok(0));
        assert_eq!(unchanged, original);

        let mut suffix = original.clone();
        assert_eq!(suffix.rebase_after(first), Ok(1));
        assert_eq!(suffix.receipt.parent, first);
        assert_eq!(suffix.headers, original.headers[1..]);
        assert_eq!(
            suffix.evidence,
            PreparedHeaderBatch::context_free_evidence(first, TRUST_ANCHOR_DIGEST, &suffix.headers)
        );

        let mut invalid = original.clone();
        assert_eq!(
            invalid.rebase_after(missing),
            Err(TransitionTypeError::InvalidPreparedRebase)
        );
        assert_eq!(invalid, original);

        let mut consumed = original.clone();
        assert_eq!(consumed.rebase_after(final_header), Ok(3));
        assert!(consumed.headers.is_empty());
        assert_eq!(consumed.receipt.parent, final_header);
        assert_eq!(
            consumed.evidence,
            PreparedHeaderBatch::context_free_evidence(final_header, TRUST_ANCHOR_DIGEST, &[])
        );

        let mut already_applied = original.clone();
        let receipt = already_applied.receipt.clone();
        let evidence = already_applied.evidence;
        already_applied.clear_already_applied();
        assert!(already_applied.headers.is_empty());
        assert_eq!(already_applied.receipt, receipt);
        assert_eq!(already_applied.evidence, evidence);
    }
}
