//! Wire and boundary properties for the stream-6 `GetBlocks` message.

use std::collections::HashMap;

use allocation_counter::measure;
use proptest::{collection::vec, prelude::*};
use tokio::{
    sync::watch,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use super::super::super::{
    config::MAX_BS_RESPONSE_BYTES, spawn_block_sync_reactor, wire::*, BlockSyncAction,
    BlockSyncFrontiers, BlockSyncMisbehavior, BlockSyncService, BlockSyncStartup, BlockSyncStatus,
    BlockSyncWireError, ZakuraBlockSyncConfig,
};
use super::super::{
    connect_peer_with_status, mainnet_blocks_1_to_3, next_action, peer,
    wait_for_outbound_range_unavailable,
};
use crate::zakura::{
    framed_channel, testkit::SyntheticBlockSyncPeers, Frame, Peer, Service, ServicePeerDirection,
};
use zakura_chain::block;

use super::runner::assert_contract_test_manifest;

// These literals are the independent test contract. Keeping them separate from
// production constants makes accidental wire drift observable.
const GET_BLOCKS_TYPE: u8 = 2;
const GET_BLOCKS_PAYLOAD_BYTES: usize = 9;
const GET_BLOCKS_MAX_HEIGHT: u32 = 0x7fff_ffff;
const GET_BLOCKS_MAX_COUNT: u32 = 128;

const GB_WF_TEST_MANIFEST: &[(&str, &[&str])] = &[
    ("GB-WF-01", &["gb_wf_01_payload_and_frame_type_are_two"]),
    (
        "GB-WF-02",
        &["gb_wf_02_payload_uses_canonical_nine_byte_layout"],
    ),
    ("GB-WF-03", &["gb_wf_03_start_height_is_bounded"]),
    ("GB-WF-04", &["gb_wf_04_count_is_between_one_and_128"]),
    ("GB-WF-05", &["gb_wf_05_decoder_rejects_trailing_bytes"]),
    ("GB-WF-06", &["gb_wf_06_frames_require_zero_flags"]),
    (
        "GB-WF-07",
        &["gb_wf_07_accepted_messages_reencode_canonically"],
    ),
    (
        "GB-WF-08",
        &["gb_wf_08_maximum_start_and_count_are_safe_to_serve"],
    ),
    (
        "GB-WF-09",
        &["gb_wf_09_get_blocks_payload_cap_precedes_allocation"],
    ),
    (
        "GB-WF-10",
        &["gb_wf_10_fixed_fields_do_not_size_decode_allocation"],
    ),
    (
        "GB-WF-11",
        &["gb_wf_11_incomplete_get_blocks_frame_expires_at_read_deadline"],
    ),
    (
        "GB-WF-12",
        &["gb_wf_12_arbitrary_get_blocks_payloads_never_panic"],
    ),
];

#[test]
fn gb_wf_contract_manifest_names_every_requirement() {
    const EXPECTED_IDS: &[&str] = &[
        "GB-WF-01", "GB-WF-02", "GB-WF-03", "GB-WF-04", "GB-WF-05", "GB-WF-06", "GB-WF-07",
        "GB-WF-08", "GB-WF-09", "GB-WF-10", "GB-WF-11", "GB-WF-12",
    ];
    assert_contract_test_manifest(EXPECTED_IDS, GB_WF_TEST_MANIFEST);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Contract-level fields used without calling the production encoder.
struct GetBlocksFields {
    start_height: u32,
    count: u32,
}

/// Encode the canonical payload from independent literal contract values.
fn contract_payload(fields: GetBlocksFields) -> [u8; GET_BLOCKS_PAYLOAD_BYTES] {
    let mut payload = [0; GET_BLOCKS_PAYLOAD_BYTES];
    payload[0] = GET_BLOCKS_TYPE;
    payload[1..5].copy_from_slice(&fields.start_height.to_le_bytes());
    payload[5..9].copy_from_slice(&fields.count.to_le_bytes());
    payload
}

/// Build boundary payloads that may deliberately violate the contract.
fn structured_payload(message_type: u8, start_height: u32, count: u32, trailing: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(GET_BLOCKS_PAYLOAD_BYTES + trailing.len());
    payload.push(message_type);
    payload.extend_from_slice(&start_height.to_le_bytes());
    payload.extend_from_slice(&count.to_le_bytes());
    payload.extend_from_slice(trailing);
    payload
}

/// Decode only a production `GetBlocks` result into oracle-comparable fields.
fn decoded_get_blocks(payload: &[u8]) -> Option<GetBlocksFields> {
    match BlockSyncMessage::decode(payload) {
        Ok(BlockSyncMessage::GetBlocks {
            start_height,
            count,
        }) => Some(GetBlocksFields {
            start_height: start_height.0,
            count,
        }),
        Ok(_) | Err(_) => None,
    }
}

/// Generate valid fields with extra weight on meaningful wire boundaries.
fn legal_get_blocks() -> impl Strategy<Value = GetBlocksFields> {
    (
        prop_oneof![
            3 => Just(0),
            2 => Just(1),
            2 => Just(GET_BLOCKS_MAX_HEIGHT),
            8 => 0..=GET_BLOCKS_MAX_HEIGHT,
        ],
        prop_oneof![
            3 => Just(1),
            2 => Just(127),
            3 => Just(GET_BLOCKS_MAX_COUNT),
            8 => 1..=GET_BLOCKS_MAX_COUNT,
        ],
    )
        .prop_map(|(start_height, count)| GetBlocksFields {
            start_height,
            count,
        })
}

#[test]
fn gb_wf_01_payload_and_frame_type_are_two() {
    let fields = GetBlocksFields {
        start_height: 1,
        count: 1,
    };
    let message = BlockSyncMessage::GetBlocks {
        start_height: block::Height(fields.start_height),
        count: fields.count,
    };
    let payload = message.encode().expect("GetBlocks message encodes");
    let frame = message.encode_frame().expect("GetBlocks frame encodes");

    assert_eq!(GET_BLOCKS_TYPE, 2);
    assert_eq!(MSG_BS_GET_BLOCKS, GET_BLOCKS_TYPE);
    assert_eq!(payload[0], GET_BLOCKS_TYPE);
    assert_eq!(frame.message_type, u16::from(GET_BLOCKS_TYPE));

    for mismatched_type in 0..=u16::MAX {
        if mismatched_type == u16::from(GET_BLOCKS_TYPE) {
            continue;
        }
        assert!(
            BlockSyncMessage::decode_frame(Frame {
                message_type: mismatched_type,
                flags: 0,
                payload: payload.clone(),
            })
            .is_err(),
            "GetBlocks payload decoded under mismatched outer type {mismatched_type}",
        );
    }
}

/// Anchor the independent layout to readable examples.
#[test]
fn gb_wf_02_payload_uses_canonical_nine_byte_layout() {
    let vectors = [
        (
            GetBlocksFields {
                start_height: 0,
                count: 1,
            },
            [2, 0, 0, 0, 0, 1, 0, 0, 0],
        ),
        (
            GetBlocksFields {
                start_height: 0x0102_0304,
                count: 128,
            },
            [2, 4, 3, 2, 1, 128, 0, 0, 0],
        ),
        (
            GetBlocksFields {
                start_height: GET_BLOCKS_MAX_HEIGHT,
                count: GET_BLOCKS_MAX_COUNT,
            },
            [2, 255, 255, 255, 127, 128, 0, 0, 0],
        ),
    ];

    for (fields, expected) in vectors {
        assert_eq!(contract_payload(fields), expected);
        assert_eq!(decoded_get_blocks(&expected), Some(fields));

        let message = BlockSyncMessage::GetBlocks {
            start_height: block::Height(fields.start_height),
            count: fields.count,
        };
        assert_eq!(message.encode().expect("golden message encodes"), expected);
        let frame = message.encode_frame().expect("golden frame encodes");
        assert_eq!(frame.message_type, u16::from(GET_BLOCKS_TYPE));
        assert_eq!(frame.flags, 0);
        assert_eq!(frame.payload, expected);
    }
}

#[test]
fn gb_wf_03_start_height_is_bounded() {
    assert_eq!(block::Height::MAX.0, GET_BLOCKS_MAX_HEIGHT);
    assert_eq!(
        decoded_get_blocks(&structured_payload(
            GET_BLOCKS_TYPE,
            GET_BLOCKS_MAX_HEIGHT,
            1,
            &[],
        )),
        Some(GetBlocksFields {
            start_height: GET_BLOCKS_MAX_HEIGHT,
            count: 1,
        })
    );
    assert_eq!(
        decoded_get_blocks(&structured_payload(
            GET_BLOCKS_TYPE,
            GET_BLOCKS_MAX_HEIGHT + 1,
            1,
            &[],
        )),
        None
    );
}

#[test]
fn gb_wf_04_count_is_between_one_and_128() {
    assert_eq!(MAX_BS_BLOCKS_PER_REQUEST, GET_BLOCKS_MAX_COUNT);
    for count in [1, GET_BLOCKS_MAX_COUNT] {
        assert_eq!(
            decoded_get_blocks(&structured_payload(GET_BLOCKS_TYPE, 1, count, &[])),
            Some(GetBlocksFields {
                start_height: 1,
                count,
            })
        );
    }
    for count in [0, GET_BLOCKS_MAX_COUNT + 1] {
        assert_eq!(
            decoded_get_blocks(&structured_payload(GET_BLOCKS_TYPE, 1, count, &[])),
            None
        );
    }
}

#[test]
fn gb_wf_05_decoder_rejects_trailing_bytes() {
    let payload = structured_payload(GET_BLOCKS_TYPE, 1, 1, &[0]);
    assert_eq!(decoded_get_blocks(&payload), None);
}

#[test]
fn gb_wf_06_frames_require_zero_flags() {
    let payload = contract_payload(GetBlocksFields {
        start_height: 1,
        count: 1,
    })
    .to_vec();
    let frame = Frame {
        message_type: u16::from(GET_BLOCKS_TYPE),
        flags: 0,
        payload: payload.clone(),
    };
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Ok(BlockSyncMessage::GetBlocks { .. })
    ));
    for flags in 1..=u16::MAX {
        assert!(matches!(
            BlockSyncMessage::decode_frame(Frame {
                message_type: u16::from(GET_BLOCKS_TYPE),
                flags,
                payload: payload.clone(),
            }),
            Err(BlockSyncWireError::UnsupportedFlags(rejected)) if rejected == flags
        ));
    }
}

proptest! {
    #[test]
    fn gb_wf_07_accepted_messages_reencode_canonically(fields in legal_get_blocks()) {
        let expected = contract_payload(fields);
        let message = BlockSyncMessage::GetBlocks {
            start_height: block::Height(fields.start_height),
            count: fields.count,
        };
        let encoded = message.encode().expect("contract-legal GetBlocks encodes");

        prop_assert_eq!(&encoded, &expected);
        prop_assert_eq!(decoded_get_blocks(&encoded), Some(fields));
        let decoded = BlockSyncMessage::decode(&encoded).expect("canonical GetBlocks decodes");
        prop_assert_eq!(decoded.encode().expect("decoded GetBlocks re-encodes"), encoded);
    }

    #[test]
    fn get_blocks_structured_payloads_match_contract(
        message_type in prop_oneof![
            8 => Just(GET_BLOCKS_TYPE),
            2 => any::<u8>(),
        ],
        start_height in prop_oneof![
            Just(0),
            Just(GET_BLOCKS_MAX_HEIGHT),
            Just(GET_BLOCKS_MAX_HEIGHT + 1),
            Just(u32::MAX),
            any::<u32>(),
        ],
        count in prop_oneof![
            Just(0),
            Just(1),
            Just(GET_BLOCKS_MAX_COUNT),
            Just(GET_BLOCKS_MAX_COUNT + 1),
            Just(u32::MAX),
            any::<u32>(),
        ],
        trailing in prop_oneof![
            4 => Just(Vec::new()),
            1 => vec(any::<u8>(), 1..=8),
        ],
    ) {
        let payload = structured_payload(message_type, start_height, count, &trailing);
        let expected = message_type == GET_BLOCKS_TYPE
            && start_height <= GET_BLOCKS_MAX_HEIGHT
            && (1..=GET_BLOCKS_MAX_COUNT).contains(&count)
            && trailing.is_empty();
        let decoded = decoded_get_blocks(&payload);

        if expected {
            prop_assert_eq!(decoded, Some(GetBlocksFields { start_height, count }));
        } else {
            prop_assert_eq!(decoded, None);
        }
    }

    #[test]
    fn gb_wf_12_arbitrary_get_blocks_payloads_never_panic(
        payload in vec(any::<u8>(), 0..=GET_BLOCKS_PAYLOAD_BYTES)
    ) {
        if let Ok(message @ BlockSyncMessage::GetBlocks { .. }) = BlockSyncMessage::decode(&payload) {
            prop_assert_eq!(message.encode().expect("decoded GetBlocks re-encodes"), payload);
        }
    }

    #[test]
    fn get_blocks_arbitrary_frames_match_envelope_contract(
        message_type in any::<u16>(),
        flags in any::<u16>(),
        payload in vec(any::<u8>(), 0..=256),
    ) {
        let payload_type = payload.first().copied();
        let envelope_matches = flags == 0
            && payload_type.is_some_and(|payload_type| u16::from(payload_type) == message_type);
        let decoded = BlockSyncMessage::decode_frame(Frame {
            message_type,
            flags,
            payload,
        });

        if !envelope_matches {
            prop_assert!(decoded.is_err());
        }
        if let Ok(message) = decoded {
            prop_assert_eq!(flags, 0);
            prop_assert_eq!(message_type, u16::from(message.message_type()));
        }
    }
}

/// Exercise the maximum legal height and count through the real peer routine
/// and reactor, where height arithmetic and local serving limits are applied.
#[tokio::test]
async fn gb_wf_08_maximum_start_and_count_are_safe_to_serve() {
    let tip = (block::Height::MAX, block::Hash([0xff; 32]));
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: GET_BLOCKS_MAX_COUNT,
        ..ZakuraBlockSyncConfig::default()
    };
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let peers = SyntheticBlockSyncPeers::new(config, handle.clone(), 4);
    let peer_id = peer(0xe8);
    let mut synthetic_peer = peers
        .connect_peer(peer_id.clone(), 1, ServicePeerDirection::Outbound)
        .expect("the maximum-boundary peer connects");
    handle
        .barrier_for_test()
        .await
        .expect("peer admission reaches the reactor");
    assert!(matches!(
        synthetic_peer
            .recv_timeout(Duration::from_secs(1))
            .await
            .expect("initial Status decodes"),
        Some(BlockSyncMessage::Status(_))
    ));
    synthetic_peer
        .try_send(BlockSyncMessage::Status(BlockSyncStatus {
            servable_low: block::Height::MIN,
            servable_high: tip.0,
            tip_hash: tip.1,
            max_blocks_per_response: GET_BLOCKS_MAX_COUNT,
            max_inflight_requests: 1,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }))
        .expect("peer Status queues");
    synthetic_peer
        .try_send(BlockSyncMessage::GetBlocks {
            start_height: block::Height::MAX,
            count: GET_BLOCKS_MAX_COUNT,
        })
        .expect("maximum GetBlocks request queues");
    synthetic_peer
        .barrier_for_test()
        .await
        .expect("peer routine handles the maximum request");
    handle
        .barrier_for_test()
        .await
        .expect("maximum request reaches the reactor");

    let query = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::QueryBlocksByHeightRange {
                peer, start, count, ..
            } => break (peer, start, count),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("maximum GetBlocks request produced {action:?}"),
        }
    };
    assert_eq!(query, (peer_id, block::Height::MAX, 1));

    reactor_task.abort();
    let _ = reactor_task.await;
}

proptest! {
    /// Prove the two peer-controlled scalar values never size an allocation
    /// while the fixed payload is decoded.
    #[test]
    fn gb_wf_10_fixed_fields_do_not_size_decode_allocation(
        start_height in any::<u32>(),
        count in any::<u32>(),
    ) {
        prop_assert_eq!(
            preallocation_payload_cap(u16::from(GET_BLOCKS_TYPE)),
            Some(GET_BLOCKS_PAYLOAD_BYTES)
        );
        let fields = GetBlocksFields {
            start_height,
            count,
        };
        let payload = contract_payload(fields);
        let mut decoded = None;
        let allocations = measure(|| {
            decoded = decoded_get_blocks(&payload);
        });

        prop_assert_eq!(
            allocations.count_total,
            0,
            "fixed-field decode allocated for {:?}",
            fields,
        );
        if start_height <= GET_BLOCKS_MAX_HEIGHT && (1..=GET_BLOCKS_MAX_COUNT).contains(&count) {
            prop_assert_eq!(decoded, Some(fields));
        } else {
            prop_assert_eq!(decoded, None);
        }
    }
}

/// Keep malformed wire input distinct from a valid request outside our serving range.
#[tokio::test]
async fn malformed_get_blocks_disconnects_but_unavailable_range_does_not() {
    let blocks = mainnet_blocks_1_to_3();
    let tip = (block::Height(1), blocks[0].hash());
    let config = ZakuraBlockSyncConfig::default();
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let malformed_peer = peer(0xe1);
    let (malformed_inbound, malformed_recv) = framed_channel(4);
    let (malformed_outbound, _malformed_sent) = framed_channel(4);
    let malformed_streams = HashMap::from([(
        ZAKURA_STREAM_BLOCK_SYNC,
        (malformed_recv, malformed_outbound),
    )]);
    let malformed_connection = CancellationToken::new();
    service.add_peer(Peer::new(
        malformed_peer.clone(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        malformed_streams,
        malformed_connection.clone(),
    ));
    malformed_inbound
        .send(Frame {
            message_type: u16::from(GET_BLOCKS_TYPE),
            flags: 0,
            payload: structured_payload(GET_BLOCKS_TYPE, 1, 1, &[0xa5]),
        })
        .await
        .expect("malformed GetBlocks queues");

    timeout(Duration::from_secs(1), malformed_connection.cancelled())
        .await
        .expect("malformed GetBlocks cancels its connection");
    handle
        .barrier_for_test()
        .await
        .expect("the malformed-message report reaches the reactor");

    let reported = loop {
        match next_action(&mut actions).await {
            BlockSyncAction::Misbehavior { peer, reason } => break (peer, reason),
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => panic!("unexpected action before malformed-message report: {action:?}"),
        }
    };
    assert_eq!(
        reported,
        (malformed_peer, BlockSyncMisbehavior::MalformedMessage)
    );

    let (_peer, valid_inbound, mut valid_outbound) = connect_peer_with_status(
        &service,
        &mut actions,
        0xe2,
        tip.0,
        tip.1,
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let unavailable = BlockSyncMessage::GetBlocks {
        start_height: block::Height(2),
        count: 1,
    }
    .encode_frame()
    .expect("valid unavailable request encodes");

    for _ in 0..2 {
        valid_inbound
            .send(unavailable.clone())
            .await
            .expect("valid unavailable request queues");
        assert_eq!(
            wait_for_outbound_range_unavailable(&mut valid_outbound).await,
            (block::Height(2), 1)
        );
    }

    reactor_task.abort();
}
