//! Test helpers shared by the version 9 tests.

use std::{collections::HashMap, sync::Arc};

use zakura_chain::{
    block::{self, genesis::regtest_genesis_block, merkle::AuthDataRoot},
    fmt::HexDebug,
    ironwood, orchard, sapling,
    work::difficulty::U256,
};
use zakura_header_chain::Frontier;

use super::{smallest_page, HeadersOutcomeCode, Status, MAX_HS_SUBSCRIPTIONS};
use crate::zakura::header_sync::wire::{AuxSchema, TreeAuxRecordV1};

/// Every outcome code, in wire order.
pub(crate) const OUTCOME_CODES: [HeadersOutcomeCode; 6] = [
    HeadersOutcomeCode::TargetNotRetained,
    HeadersOutcomeCode::NoLocatorIntersection,
    HeadersOutcomeCode::HistoryPruned,
    HeadersOutcomeCode::TargetNotSelected,
    HeadersOutcomeCode::SubscriptionSuperseded,
    HeadersOutcomeCode::SubscriptionClosed,
];

/// `P` on a network with `HEADER_BYTES`-byte headers: the smallest legal page.
pub(crate) fn p<const HEADER_BYTES: usize>() -> u32 {
    u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::None)).expect("a page fits u32")
}

/// A schema-1 record at `height`, with every root and count at its empty
/// default.
pub(crate) fn record(height: block::Height) -> TreeAuxRecordV1 {
    TreeAuxRecordV1 {
        height,
        sapling_root: sapling::tree::Root::default(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: AuthDataRoot::from([0; 32]),
    }
}

/// A legal `Status` with every height and cap at its minimum: heights 0, one
/// header per page, no auxiliary schema, and a message cap of `P`.
pub(crate) fn smallest_status<const HEADER_BYTES: usize>() -> Status {
    Status {
        work_anchor_height: block::Height(0),
        work_anchor_hash: block::Hash([0x11; 32]),
        selected_tip_height: block::Height(0),
        selected_tip_hash: block::Hash([0x22; 32]),
        suffix_cumulative_work: U256::zero(),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 1,
        max_subscriptions: MAX_HS_SUBSCRIPTIONS,
        max_message_bytes: p::<HEADER_BYTES>(),
        tree_aux_schema_mask: 0,
    }
}

/// A legal `Status` that advertises `tip` and schema 1, with the smallest
/// caps otherwise.
pub(crate) fn status<const HEADER_BYTES: usize>(tip: Frontier) -> Status {
    Status {
        selected_tip_height: tip.height,
        selected_tip_hash: tip.hash,
        max_message_bytes: u32::try_from(smallest_page(HEADER_BYTES, AuxSchema::V1))
            .expect("a page fits u32"),
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
        ..smallest_status::<HEADER_BYTES>()
    }
}

/// A header tree over the Regtest genesis header. Siblings differ by nonce,
/// so every header has its own hash.
#[derive(Debug)]
pub(crate) struct HeaderTree {
    /// Every header the tree holds, by hash, with its height and hash.
    pub(crate) nodes: HashMap<block::Hash, (Frontier, Arc<block::Header>)>,
    /// The Regtest genesis header.
    pub(crate) genesis: Frontier,
    /// Distinguishes sibling headers.
    next_nonce: u32,
}

impl Default for HeaderTree {
    /// A tree holding only the Regtest genesis header.
    fn default() -> Self {
        Self::new()
    }
}

impl HeaderTree {
    /// A tree holding only the Regtest genesis header.
    pub(crate) fn new() -> Self {
        let header = regtest_genesis_block().header.clone();
        let genesis = Frontier::new(block::Height(0), header.hash());
        Self {
            nodes: HashMap::from([(genesis.hash, (genesis, header))]),
            genesis,
            next_nonce: 0,
        }
    }

    /// Add `count` headers above `parent`, and return them.
    pub(crate) fn extend(&mut self, parent: Frontier, count: u32) -> Vec<Frontier> {
        let mut added = Vec::new();
        let mut parent = parent;
        for _ in 0..count {
            let mut header = *self.nodes[&parent.hash].1;
            header.previous_block_hash = parent.hash;
            self.next_nonce += 1;
            let mut nonce = [0; 32];
            nonce[..4].copy_from_slice(&self.next_nonce.to_le_bytes());
            header.nonce = HexDebug(nonce);
            let child = Frontier::new(
                parent.height.next().expect("test heights are small"),
                header.hash(),
            );
            self.nodes.insert(child.hash, (child, Arc::new(header)));
            added.push(child);
            parent = child;
        }
        added
    }

    /// The parent of `frontier`, if the tree holds both.
    pub(crate) fn parent(&self, frontier: Frontier) -> Option<Frontier> {
        let (_, header) = self.nodes.get(&frontier.hash)?;
        self.nodes
            .get(&header.previous_block_hash)
            .map(|(parent, _)| *parent)
    }

    /// The ancestor of `of` at `height`, or `of` itself at its own height.
    /// `None` if a header on the way is missing or `height` is above `of`.
    pub(crate) fn ancestor(&self, of: Frontier, height: block::Height) -> Option<Frontier> {
        let mut at = of;
        while at.height > height {
            at = self.parent(at)?;
        }
        (at.height == height).then_some(at)
    }
}
