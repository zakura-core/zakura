//! Inputs that the peer-work tests and the peer-work model share. Nothing here
//! derives an expected value.

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    AlarmSet, ChainScore, EngineMode, EngineSnapshot, Frontier, FrontierSet, HeaderGeneration,
    StateVersion, SuffixWork, VerifiedGeneration,
};

use crate::zakura::{header_sync::Status, ZakuraPeerId};

pub(super) fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

pub(super) fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
}

/// A snapshot at header `generation`, finalized at height 10 with its tip at 100.
pub(super) fn snapshot(generation: u8) -> EngineSnapshot {
    let finalized = Frontier::new(block::Height(10), hash(10));
    let tip = Frontier::new(block::Height(100), hash(100));
    EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(u64::from(generation)),
        verified_generation: VerifiedGeneration::new(1),
        frontiers: FrontierSet {
            finalized,
            header_best: tip,
            verified_best: finalized,
        },
        header_best_score: ChainScore::new(SuffixWork::new(U256::from(100_u32)), tip.hash),
        oldest_retained_height: finalized.height,
        alarms: AlarmSet::default(),
    }
}

/// A status anchored at [`snapshot`]'s finalized frontier.
pub(super) fn status(tip_height: u32, tip_hash: block::Hash, work: u32) -> Status {
    Status {
        work_anchor_height: block::Height(10),
        work_anchor_hash: hash(10),
        selected_tip_height: block::Height(tip_height),
        selected_tip_hash: tip_hash,
        suffix_cumulative_work: U256::from(work),
        oldest_retained_height: block::Height(10),
        max_headers_per_response: 1_000,
        max_subscriptions: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 1,
    }
}
