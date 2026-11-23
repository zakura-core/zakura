//! The stream conformance suite over `tx-relay`'s two streams.
//!
//! Exchange `e` asks for the id whose mined hash starts with `e`, little
//! endian. Its `Tx` answer carries that id and a one-byte body, and its
//! `NotFound` answer names it.

use zakura_chain::transaction::{self, UnminedTxId};

use super::wire::{TxRelayMessage, MAX_TX_BYTES, TX_RELAY_STREAMS};
use crate::zakura::{
    testkit::stream_conformance::{
        stream_conformance_suite, StreamConformance, SubscriptionUpdate,
    },
    MessageRule,
};

#[derive(Debug)]
struct TxRelayConformance;

/// The id exchange `exchange` asks for.
fn id(exchange: u32) -> UnminedTxId {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&exchange.to_le_bytes());
    UnminedTxId::Legacy(transaction::Hash(hash))
}

/// The exchange `id` belongs to.
fn exchange_of(id: &UnminedTxId) -> u32 {
    let hash = id.mined_id().0;
    u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]])
}

impl StreamConformance for TxRelayConformance {
    type Message = TxRelayMessage;

    fn message(row: &MessageRule, exchange: u32) -> TxRelayMessage {
        match row.message_type {
            1 => TxRelayMessage::Hello {
                serves: 16,
                max_tx_bytes: MAX_TX_BYTES,
            },
            2 => TxRelayMessage::Have(vec![(id(exchange), 1)]),
            3 => TxRelayMessage::GetTxs(id(exchange)),
            4 => TxRelayMessage::Tx {
                id: id(exchange),
                body: vec![1],
            },
            5 => TxRelayMessage::NotFound(id(exchange)),
            _ => unreachable!("tx-relay declares five message kinds"),
        }
    }

    fn exchange(message: &TxRelayMessage) -> u32 {
        match message {
            TxRelayMessage::GetTxs(id)
            | TxRelayMessage::NotFound(id)
            | TxRelayMessage::Tx { id, .. } => exchange_of(id),
            TxRelayMessage::Have(entries) => entries.first().map_or(0, |(id, _)| exchange_of(id)),
            TxRelayMessage::Hello { .. } => 0,
        }
    }

    fn update(_: &MessageRule, _: SubscriptionUpdate) -> TxRelayMessage {
        unreachable!("tx-relay has no subscriptions")
    }

    fn read_update(_: &TxRelayMessage) -> Option<SubscriptionUpdate> {
        None
    }

    fn page(_: &MessageRule, _: u32, _: u32) -> TxRelayMessage {
        unreachable!("tx-relay has no subscriptions")
    }

    fn read_page(_: &TxRelayMessage) -> Option<(u32, u32)> {
        None
    }
}

stream_conformance_suite!(tx_relay_streams, TX_RELAY_STREAMS, TxRelayConformance);
