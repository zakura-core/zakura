//! Legacy gossip and request codec conformance against their rule tables.

use proptest::prelude::*;
use zakura_chain::{
    block,
    parameters::NetworkUpgrade,
    transaction::{self, AuthDigest, LockTime, Transaction, UnminedTx, UnminedTxId, WtxId},
};

use super::*;
use crate::zakura::wire_codec::conformance::{wire_conformance_tests, WireSample, WireViolation};

fn hash(index: usize) -> block::Hash {
    let mut bytes = [0xab; 32];
    bytes[..8].copy_from_slice(
        &u64::try_from(index)
            .expect("test index fits u64")
            .to_le_bytes(),
    );
    block::Hash(bytes)
}

fn legacy_id(index: usize) -> UnminedTxId {
    UnminedTxId::Legacy(transaction::Hash(hash(index).0))
}

fn witnessed_id(index: usize) -> UnminedTxId {
    UnminedTxId::Witnessed(WtxId {
        id: transaction::Hash(hash(index).0),
        auth_digest: AuthDigest([0xcd; 32]),
    })
}

fn push_transaction() -> UnminedTx {
    UnminedTx::from(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: block::Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    })
}

/// A frame with a CompactSize count prefix followed by `tail` zero bytes.
fn counted_frame(message_type: u16, count: u64, tail: usize) -> Frame {
    let mut payload = zakura_chain::serialization::CompactSize64::from(count)
        .zcash_serialize_to_vec()
        .expect("CompactSize encodes");
    payload.resize(payload.len() + tail, 0);
    Frame {
        message_type,
        flags: 0,
        payload,
    }
}

fn is_count_exceeds_payload(error: &LegacyGossipError) -> bool {
    matches!(
        error,
        LegacyGossipError::Wire(WireError::CountExceedsPayload { .. })
    )
}

fn is_count_out_of_range(error: &LegacyGossipError) -> bool {
    matches!(
        error,
        LegacyGossipError::Wire(WireError::CountOutOfRange { .. })
    )
}

fn small_hashes() -> impl Strategy<Value = Vec<block::Hash>> {
    proptest::collection::vec(any::<[u8; 32]>().prop_map(block::Hash), 0..8)
}

impl WireSample for LegacyGossipFrame {
    const OPEN_ENDED_ROWS: &'static [u16] = &[];

    fn samples() -> Vec<Self> {
        vec![
            Self::AdvertiseBlock(hash(1)),
            Self::AdvertiseTransactionIds(vec![legacy_id(1)]),
            Self::AdvertiseTransactionIds((0..MAX_INVENTORY_ITEMS).map(witnessed_id).collect()),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            any::<[u8; 32]>().prop_map(|bytes| Self::AdvertiseBlock(block::Hash(bytes))),
            (1..8usize).prop_map(|count| Self::AdvertiseTransactionIds(
                (0..count).map(legacy_id).collect()
            )),
        ]
        .boxed()
    }

    fn violations() -> Vec<WireViolation<Self>> {
        let mut block_inventory = InventoryHash::Block(hash(1))
            .zcash_serialize_to_vec()
            .expect("inventory encodes");
        block_inventory.insert(0, 1);
        vec![
            WireViolation {
                name: "empty advertisement",
                frame: counted_frame(MSG_ADVERTISE_TX_IDS, 0, 36),
                rejected_by: is_count_out_of_range,
            },
            WireViolation {
                name: "count claims more ids than the payload holds",
                frame: counted_frame(MSG_ADVERTISE_TX_IDS, 25_000, 36),
                rejected_by: is_count_exceeds_payload,
            },
            WireViolation {
                name: "count above the inventory cap",
                frame: counted_frame(MSG_ADVERTISE_TX_IDS, 25_001, 36),
                rejected_by: is_count_out_of_range,
            },
            WireViolation {
                name: "block inventory in a transaction advertisement",
                frame: Frame {
                    message_type: MSG_ADVERTISE_TX_IDS,
                    flags: 0,
                    payload: block_inventory,
                },
                rejected_by: |error| {
                    matches!(error, LegacyGossipError::Wire(WireError::InvalidItem(_)))
                },
            },
        ]
    }

    fn decode_allocation_bound(message_type: u16, payload_len: usize) -> Option<usize> {
        match message_type {
            MSG_ADVERTISE_BLOCK => Some(0),
            MSG_ADVERTISE_TX_IDS => Some(ADVERTISED_TRANSACTION_IDS.allocation_bound(payload_len)),
            _ => None,
        }
    }
}

impl WireSample for LegacyRequestFrame {
    // The codec accepts transactions that consensus rejects, so the push row's
    // bounds are the codec's lower bound and the block-size limit.
    const OPEN_ENDED_ROWS: &'static [u16] = &[MSG_REQUEST_PUSH_TRANSACTION];

    fn samples() -> Vec<Self> {
        let full_locator: Vec<_> = (0..MAX_LOCATOR_HASHES).map(hash).collect();
        vec![
            Self::BlocksByHash(Vec::new()),
            Self::BlocksByHash((0..MAX_INVENTORY_ITEMS).map(hash).collect()),
            Self::TransactionsById(Vec::new()),
            Self::TransactionsById((0..MAX_INVENTORY_ITEMS).map(witnessed_id).collect()),
            Self::FindBlocks {
                known_blocks: Vec::new(),
                stop: None,
            },
            Self::FindBlocks {
                known_blocks: full_locator.clone(),
                stop: Some(hash(0)),
            },
            Self::FindHeaders {
                known_blocks: Vec::new(),
                stop: None,
            },
            Self::FindHeaders {
                known_blocks: full_locator,
                stop: Some(hash(0)),
            },
            Self::MempoolTransactionIds,
            Self::Ping,
            Self::PushTransaction(push_transaction()),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        let stop = proptest::option::of(any::<[u8; 32]>().prop_map(|bytes| {
            // A zero stop hash encodes "no stop hash", so it cannot round-trip.
            let mut bytes = bytes;
            bytes[0] |= 1;
            block::Hash(bytes)
        }));
        prop_oneof![
            small_hashes().prop_map(Self::BlocksByHash),
            (0..8usize)
                .prop_map(|count| Self::TransactionsById((0..count).map(legacy_id).collect())),
            (small_hashes(), stop.clone())
                .prop_map(|(known_blocks, stop)| Self::FindBlocks { known_blocks, stop }),
            (small_hashes(), stop)
                .prop_map(|(known_blocks, stop)| Self::FindHeaders { known_blocks, stop }),
            Just(Self::MempoolTransactionIds),
            Just(Self::Ping),
        ]
        .boxed()
    }

    fn violations() -> Vec<WireViolation<Self>> {
        vec![
            WireViolation {
                name: "a 3-byte payload claims 25,000 hashes",
                frame: counted_frame(MSG_REQUEST_BLOCKS_BY_HASH, 25_000, 0),
                rejected_by: is_count_exceeds_payload,
            },
            WireViolation {
                name: "a 3-byte payload claims 25,000 transaction ids",
                frame: counted_frame(MSG_REQUEST_TRANSACTIONS_BY_ID, 25_000, 0),
                rejected_by: is_count_exceeds_payload,
            },
            WireViolation {
                name: "locator above its cap",
                frame: counted_frame(MSG_REQUEST_FIND_BLOCKS, 102, 32),
                rejected_by: is_count_out_of_range,
            },
            WireViolation {
                name: "locator without its stop hash",
                frame: counted_frame(MSG_REQUEST_FIND_HEADERS, 1, 32),
                rejected_by: |error| matches!(error, LegacyGossipError::Wire(WireError::Item(_))),
            },
            WireViolation {
                name: "pushed bytes that are not a transaction",
                frame: Frame {
                    message_type: MSG_REQUEST_PUSH_TRANSACTION,
                    flags: 0,
                    payload: vec![0xff; 64],
                },
                rejected_by: |error| matches!(error, LegacyGossipError::Wire(WireError::Item(_))),
            },
        ]
    }

    fn decode_allocation_bound(message_type: u16, payload_len: usize) -> Option<usize> {
        match message_type {
            MSG_REQUEST_BLOCKS_BY_HASH => Some(INVENTORY_HASHES.allocation_bound(payload_len)),
            MSG_REQUEST_TRANSACTIONS_BY_ID => Some(TRANSACTION_IDS.allocation_bound(payload_len)),
            MSG_REQUEST_FIND_BLOCKS | MSG_REQUEST_FIND_HEADERS => {
                Some(BLOCK_LOCATOR.allocation_bound(payload_len))
            }
            MSG_REQUEST_MEMPOOL_TRANSACTION_IDS | MSG_REQUEST_PING => Some(0),
            _ => None,
        }
    }
}

wire_conformance_tests!(legacy_gossip_wire_conformance, LegacyGossipFrame);
wire_conformance_tests!(legacy_request_wire_conformance, LegacyRequestFrame);
