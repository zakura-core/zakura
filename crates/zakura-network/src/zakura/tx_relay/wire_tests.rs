//! The message suite for `tx-relay`'s wire.

use proptest::prelude::*;
use zakura_chain::transaction::{AuthDigest, Hash, UnminedTxId, WtxId};

use super::wire::{TxRelayMessage, MAX_HAVE_ENTRIES, MAX_TX_BYTES, TX_RELAY_STREAMS};
use crate::zakura::{
    wire_codec::{
        message_suite::{check_layout_carries_family, message_suite, MessageSample, Violation},
        WireError, WireMessage,
    },
    Frame,
};

/// A transaction before v5, named by its mined id alone.
fn legacy(n: u8) -> UnminedTxId {
    UnminedTxId::from_legacy_id(Hash([n; 32]))
}

/// A v5 transaction, named by its mined id and authorizing-data digest.
fn witnessed(n: u8) -> UnminedTxId {
    UnminedTxId::from(WtxId {
        id: Hash([n; 32]),
        auth_digest: AuthDigest([n.wrapping_add(1); 32]),
    })
}

fn frame(message_type: u16, payload: Vec<u8>) -> Frame {
    Frame {
        message_type,
        flags: 0,
        payload,
    }
}

fn any_id() -> impl Strategy<Value = UnminedTxId> {
    prop_oneof![
        any::<[u8; 32]>().prop_map(|id| UnminedTxId::from_legacy_id(Hash(id))),
        (any::<[u8; 32]>(), any::<[u8; 32]>())
            .prop_filter("the legacy placeholder", |(_, digest)| *digest
                != [0xFF; 32])
            .prop_map(|(id, digest)| UnminedTxId::from(WtxId {
                id: Hash(id),
                auth_digest: AuthDigest(digest),
            })),
    ]
}

impl MessageSample for TxRelayMessage {
    fn samples() -> Vec<Self> {
        // A `u32` limit always fits a `usize` on the platforms the node runs
        // on.
        let largest = MAX_TX_BYTES as usize;
        vec![
            Self::Hello {
                serves: 1,
                max_tx_bytes: MAX_TX_BYTES,
            },
            Self::Have(vec![(legacy(1), 300)]),
            Self::Have(
                (0..MAX_HAVE_ENTRIES)
                    .map(|n| (witnessed(u8::try_from(n % 250).unwrap()), 1_000))
                    .collect(),
            ),
            Self::GetTxs(legacy(2)),
            Self::GetTxs(witnessed(3)),
            Self::Tx {
                id: witnessed(4),
                body: vec![7],
            },
            Self::Tx {
                id: legacy(5),
                body: vec![9; largest],
            },
            Self::NotFound(witnessed(6)),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            (any::<u16>(), any::<u32>()).prop_map(|(serves, max_tx_bytes)| Self::Hello {
                serves,
                max_tx_bytes
            }),
            prop::collection::vec((any_id(), any::<u32>()), 1..8).prop_map(Self::Have),
            any_id().prop_map(Self::GetTxs),
            (any_id(), prop::collection::vec(any::<u8>(), 1..64))
                .prop_map(|(id, body)| Self::Tx { id, body }),
            any_id().prop_map(Self::NotFound),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        vec![Violation {
            // A zero count, padded to the row's minimum length.
            name: "have with no ids",
            frame: frame(Self::Have(vec![(legacy(1), 1)]).message_type(), {
                let mut payload = vec![0];
                payload.extend_from_slice(&[0; 68]);
                payload
            }),
            rejected_by: |error| matches!(error, WireError::CountOutOfRange { count: 0, .. }),
        }]
    }

    fn opaque_types() -> &'static [u16] {
        // Any body length within the row decodes: the pool, not the codec,
        // checks the body.
        &[4]
    }
}

message_suite!(message_family, TxRelayMessage);

#[test]
fn the_layout_carries_exactly_the_family_rows() {
    check_layout_carries_family::<TxRelayMessage>(&TX_RELAY_STREAMS);
}

#[test]
fn every_message_names_its_stream() {
    for message in TxRelayMessage::samples() {
        let kind = message.stream_kind();
        let stream = TX_RELAY_STREAMS
            .iter()
            .find(|stream| stream.kind == kind)
            .expect("each message has a stream");
        assert!(stream
            .messages
            .unwrap()
            .iter()
            .any(|rule| rule.message_type == message.message_type()));
    }
}

/// A served body is serialized straight into its frame, and that frame is
/// the one `encode_frame` makes of the same `Tx`.
#[test]
fn a_served_frame_is_the_encoded_tx() {
    use zakura_chain::{parameters::Network, serialization::ZcashSerialize};

    for tx in Network::Mainnet.unmined_transactions_in_blocks(..).take(20) {
        let id = tx.transaction.id();
        let body = tx
            .transaction
            .transaction()
            .zcash_serialize_to_vec()
            .expect("vector transactions serialize");
        let encoded = crate::zakura::wire_codec::encode_frame(&TxRelayMessage::Tx { id, body })
            .expect("a vector transaction fits the row");
        let frame = super::wire::tx_frame(id, &tx.transaction).expect("a vector transaction fits");
        // The buffer holds no spare capacity beyond the frame.
        assert_eq!(frame.payload.capacity(), frame.payload.len());
        assert_eq!(Some(frame), Some(encoded));
    }
}
