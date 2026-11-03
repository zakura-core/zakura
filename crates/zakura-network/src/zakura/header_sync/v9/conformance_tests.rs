//! Shared transport conformance with structurally valid stream-5 messages.
//! The cluster tests separately check header sync's own rules over QUIC.

use std::sync::{Arc, OnceLock};
use zakura_chain::{block, serialization::ZcashDeserialize};

use super::{
    test_support::smallest_status, CommonMessage, Cursor, Headers, HeadersOutcome,
    HeadersOutcomeCode, Message, Operation, SubscribeHeaders,
};
use crate::zakura::{
    header_sync::{config::COMMON_HEADER_BYTES, wire::AuxSchema, HeaderEntry},
    testkit::stream_conformance::{
        stream_conformance_suite, StreamConformance, SubscriptionUpdate, UpdateOp,
    },
    MessageRule,
};

/// The initial target every adapter subscription names.
const TARGET: block::Hash = block::Hash([0x7a; 32]);

/// The harness's subscription key `key` as a nonzero subscription ID.
fn subscription_id(key: u32) -> u64 {
    u64::from(key) + 1
}

/// The harness key of `subscription_id`.
fn key(subscription_id: u64) -> u32 {
    u32::try_from(subscription_id - 1).expect("the adapter only names harness keys")
}

/// The harness cursor `cursor` as a version 9 cursor: a height and a hash
/// that name it, with no credit spent.
fn cursor(cursor: u32) -> Cursor {
    let mut hash = [0x5c; 32];
    hash[..4].copy_from_slice(&cursor.to_le_bytes());
    Cursor {
        height: block::Height(cursor),
        hash: block::Hash(hash),
        headers: 0,
        bytes: 0,
    }
}

#[derive(Debug)]
struct HeaderSyncConformance;

impl StreamConformance for HeaderSyncConformance {
    type Message = CommonMessage;

    fn message(row: &MessageRule, exchange: u32) -> CommonMessage {
        match row.message_type {
            1 => Message::Status(smallest_status::<COMMON_HEADER_BYTES>()),
            3 => Self::page(row, exchange, 1),
            4 => Message::HeadersOutcome(HeadersOutcome {
                subscription_id: subscription_id(exchange),
                initial_target_tip_hash: TARGET,
                outcome: HeadersOutcomeCode::SubscriptionClosed,
            }),
            other => unreachable!("row {other} is not a request, page, or ending"),
        }
    }

    fn exchange(message: &CommonMessage) -> u32 {
        match message {
            Message::Headers(page) => page.common_ancestor_height.0,
            Message::HeadersOutcome(outcome) => key(outcome.subscription_id),
            Message::SubscribeHeaders(update) => key(update.subscription_id),
            Message::Status(_) => 0,
        }
    }

    fn update(_: &MessageRule, update: SubscriptionUpdate) -> CommonMessage {
        let acknowledged = cursor(update.acknowledged);
        let operation = match update.op {
            UpdateOp::Open => Operation::Open,
            UpdateOp::Grant => Operation::Grant,
            UpdateOp::Close => Operation::Close,
        };
        Message::SubscribeHeaders(SubscribeHeaders {
            operation,
            subscription_id: subscription_id(update.key),
            update_sequence: update.sequence,
            initial_target_tip_hash: TARGET,
            acknowledged,
            locator_hashes: if operation == Operation::Open {
                vec![acknowledged.hash]
            } else {
                Vec::new()
            },
            added_credit: update.added,
            tree_aux_schema: AuxSchema::None,
        })
    }

    fn read_update(message: &CommonMessage) -> Option<SubscriptionUpdate> {
        let Message::SubscribeHeaders(update) = message else {
            return None;
        };
        Some(SubscriptionUpdate {
            op: match update.operation {
                Operation::Open => UpdateOp::Open,
                Operation::Grant => UpdateOp::Grant,
                Operation::Close => UpdateOp::Close,
            },
            key: key(update.subscription_id),
            sequence: update.update_sequence,
            acknowledged: update.acknowledged.height.0,
            added: update.added_credit,
        })
    }

    /// A one-header page whose parent height carries the cursor. Only the
    /// session's credit and the harness read it, so the header is any
    /// Mainnet header.
    fn page(_: &MessageRule, key: u32, cursor: u32) -> CommonMessage {
        static HEADER: OnceLock<Arc<block::Header>> = OnceLock::new();
        let header = HEADER.get_or_init(|| {
            block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..])
                .expect("the Mainnet vector decodes")
                .header
        });
        Message::Headers(Headers {
            subscription_id: subscription_id(key),
            initial_target_tip_hash: TARGET,
            common_ancestor_height: block::Height(cursor),
            common_ancestor_hash: self::cursor(cursor).hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: header.clone(),
                body_size: 0,
                tree_aux: None,
            }],
        })
    }

    fn read_page(message: &CommonMessage) -> Option<(u32, u32)> {
        let Message::Headers(page) = message else {
            return None;
        };
        Some((key(page.subscription_id), page.common_ancestor_height.0))
    }
}

stream_conformance_suite!(stream_five, CommonMessage::LAYOUT, HeaderSyncConformance);
