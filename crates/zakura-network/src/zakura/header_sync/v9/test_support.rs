//! Test helpers shared by the version 9 tests.

use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard, sapling,
    work::difficulty::U256,
};

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
