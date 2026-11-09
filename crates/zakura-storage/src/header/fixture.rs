//! Fixture chains for the header adapter.
//!
//! Every fixture starts at the Regtest genesis block. Regtest checks only the
//! shape of each proof-of-work solution, so a fixture header is the genesis
//! header with a new parent, nonce and time, as in the header-chain fuzz
//! harness, and it still passes the real `prepare_headers`. Fork branches 1
//! and 2 may carry a weight, a harder target that gives each of their
//! headers more work. Regtest's proof-of-work waiver also skips the
//! contextual check that a header's target is the expected one.

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    ironwood, orchard,
    parameters::Network,
    work::difficulty::{CompactDifficulty, U256},
};
use zakura_header_chain::{
    EngineSnapshot, Frontier, HeaderWorkAuthority, SourceId, TargetCompletion, TreeAuxRecordV1,
};
use zakura_node_services::header_chain::{PrepareHeaderTarget, TargetEntry};

/// The Regtest network and its genesis block, the anchor of every fixture,
/// and the weights of fork branches 1 and 2.
#[derive(Clone, Debug)]
pub struct HeaderFixture {
    network: Network,
    genesis: Arc<block::Block>,
    /// Branch 1's and branch 2's weight. See [`Self::with_weight`].
    weights: [u8; 2],
}

/// The heaviest weight a branch can take.
pub const MAX_WEIGHT: u8 = 3;

impl HeaderFixture {
    /// The default Regtest network, anchored at its genesis block.
    pub fn regtest() -> Self {
        HeaderFixture {
            network: Network::new_regtest(Default::default()),
            genesis: regtest_genesis_block(),
            weights: [0; 2],
        }
    }

    /// This fixture with `branch`'s headers at `weight`: their target is
    /// the genesis target shifted right by `weight` bits, so each header
    /// has about `2^weight` times the work of a genesis-target header.
    /// Weight 0 keeps the genesis target exactly. Branch 0 and branches
    /// above 2 always keep it.
    ///
    /// # Panics
    ///
    /// If `branch` isn't 1 or 2, or `weight` is above [`MAX_WEIGHT`].
    pub fn with_weight(mut self, branch: u8, weight: u8) -> Self {
        assert!(
            matches!(branch, 1 | 2),
            "only branches 1 and 2 take a weight, not branch {branch}"
        );
        assert!(
            weight <= MAX_WEIGHT,
            "a weight is 0 to {MAX_WEIGHT}, not {weight}"
        );
        self.weights[usize::from(branch - 1)] = weight;
        self
    }

    /// The compact target of `branch`'s headers. See [`Self::with_weight`].
    pub fn target(&self, branch: u8) -> CompactDifficulty {
        let genesis = self.genesis.header.difficulty_threshold;
        let weight = match branch {
            1 | 2 => self.weights[usize::from(branch - 1)],
            _ => 0,
        };
        if weight == 0 {
            return genesis;
        }
        let expanded = genesis
            .to_expanded()
            .expect("the Regtest genesis target is valid");
        (expanded / (U256::one() << weight)).to_compact()
    }

    /// The fixture's network.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// The anchor's header.
    pub fn genesis_header(&self) -> Arc<block::Header> {
        self.genesis.header.clone()
    }

    /// The anchor's block.
    pub fn genesis_block(&self) -> Arc<block::Block> {
        self.genesis.clone()
    }

    /// The anchor, genesis at height 0.
    pub fn anchor(&self) -> Frontier {
        Frontier::new(block::Height(0), self.genesis.hash())
    }

    /// `count` headers extending `parent`, parent first. `branch` goes in each
    /// nonce, so two branches from the same parent never share a header.
    /// Each header's time is genesis plus its height in seconds, so times
    /// rise along every branch. Those fixed historical times validate the
    /// same whichever clock validation reads, which is what lets a port
    /// commit them before its run starts.
    ///
    /// # Panics
    ///
    /// If a height overflows, which a fixture never reaches.
    pub fn extend(&self, parent: Frontier, count: u32, branch: u8) -> Vec<Arc<block::Header>> {
        let mut headers = Vec::new();
        let mut parent_hash = parent.hash;
        for offset in 1..=count {
            let height = parent
                .height
                .0
                .checked_add(offset)
                .expect("fixture heights stay small");
            let header =
                self.header_at(parent_hash, height, branch, self.genesis.header.merkle_root);
            parent_hash = header.hash();
            headers.push(Arc::new(header));
        }
        headers
    }

    /// The fixture header at `height` on `branch`, extending `parent_hash`
    /// and committing to `merkle_root`, with `branch`'s target.
    /// [`Self::extend`] builds each header this way with the genesis root.
    /// The block fixture passes its body's root.
    pub(crate) fn header_at(
        &self,
        parent_hash: block::Hash,
        height: u32,
        branch: u8,
        merkle_root: block::merkle::Root,
    ) -> block::Header {
        let mut header = *self.genesis.header;
        header.previous_block_hash = parent_hash;
        header.merkle_root = merkle_root;
        header.difficulty_threshold = self.target(branch);
        header.nonce.0[0] = branch;
        header.nonce.0[1..5].copy_from_slice(&height.to_le_bytes());
        header.time = self
            .genesis
            .header
            .time
            .checked_add_signed(chrono::Duration::seconds(i64::from(height)))
            .expect("fixture times stay representable");
        header
    }

    /// The request a requester sends to prepare `headers`, which extend
    /// `parent`, as complete work toward their last header. Its owner is
    /// bound to `snapshot`'s header generation, session 1 and `request_id`.
    ///
    /// # Panics
    ///
    /// If `headers` is empty or longer than the transition cap, or
    /// `request_id` is 0.
    pub fn target_request(
        &self,
        snapshot: &EngineSnapshot,
        parent: Frontier,
        headers: &[Arc<block::Header>],
        request_id: u64,
    ) -> PrepareHeaderTarget {
        let sized: Vec<_> = headers.iter().map(|header| (header.clone(), 0)).collect();
        self.sized_target_request(snapshot, parent, &sized, request_id)
    }

    /// [`Self::target_request`] with an advertised body size for each
    /// header, which the engine keeps as that header's size hint. A size of
    /// 0 advertises none.
    ///
    /// # Panics
    ///
    /// As [`Self::target_request`] does.
    pub fn sized_target_request(
        &self,
        snapshot: &EngineSnapshot,
        parent: Frontier,
        headers: &[(Arc<block::Header>, u32)],
        request_id: u64,
    ) -> PrepareHeaderTarget {
        let (last, _) = headers.last().expect("a target has headers");
        let count = u32::try_from(headers.len()).expect("a fixture batch fits u32");
        let height = parent
            .height
            .0
            .checked_add(count)
            .expect("fixture heights stay small");
        let target = Frontier::new(block::Height(height), last.hash());
        PrepareHeaderTarget {
            source: SourceId::from_digest([7; 32]),
            owner: HeaderWorkAuthority::for_target(snapshot, target.hash)
                .bind(
                    1,
                    NonZeroU64::new(request_id).expect("request IDs start at 1"),
                )
                .into(),
            common_ancestor: parent,
            target,
            entries: headers
                .iter()
                .map(|(header, body_size)| TargetEntry {
                    header: header.clone(),
                    body_size: *body_size,
                    tree_aux: None,
                })
                .collect::<Vec<_>>()
                .try_into()
                .expect("a fixture batch fits the transition cap"),
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
        }
    }

    /// The fixture's tree auxiliary record for the header at `height`. Its
    /// Sapling count is made up, since admission authenticates none of it,
    /// and differs by height. Regtest activates neither NU5 nor NU6.3 at a
    /// fixture height, so its Orchard and Ironwood fields and its auth data
    /// root take the defaults the wire codec requires before them.
    pub fn tree_aux(&self, height: block::Height) -> TreeAuxRecordV1 {
        TreeAuxRecordV1 {
            height,
            sapling_root: Default::default(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx_count: u64::from(height.0),
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: [0; 32].into(),
        }
    }

    /// `request` with each entry carrying the fixture's record for its
    /// height ([`Self::tree_aux`]), as a supplier that has the header's
    /// tree roots sends it.
    pub fn with_tree_aux(&self, mut request: PrepareHeaderTarget) -> PrepareHeaderTarget {
        let parent = request.common_ancestor.height.0;
        let mut entries = request.entries.into_vec();
        for (offset, entry) in (1..).zip(&mut entries) {
            let height = parent
                .checked_add(offset)
                .expect("fixture heights stay small");
            entry.tree_aux = Some(self.tree_aux(block::Height(height)));
        }
        request.entries = entries
            .try_into()
            .expect("the request's own entries fit the transition cap");
        request
    }
}

/// The scenario clock: `zakura_rt::time::utc_now()`, which a simulator run
/// derives from virtual time and other builds read from the host.
#[derive(Copy, Clone, Debug, Default)]
pub struct ScenarioClock;

impl zakura_header_chain::Clock for ScenarioClock {
    fn now(&self) -> DateTime<Utc> {
        zakura_rt::time::utc_now().into()
    }
}
