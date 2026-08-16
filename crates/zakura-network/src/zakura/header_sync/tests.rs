use std::io;

use zakura_chain::{
    block::{genesis::regtest_genesis_block, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::{
        testnet::{ConfiguredCheckpoints, RegtestParameters},
        Network, NetworkUpgrade,
    },
    sapling,
    serialization::ZcashSerialize,
    work::difficulty::U256,
};

use super::{config::*, wire::*, *};
use crate::zakura::{HeaderSyncDecodeContext, HeaderSyncMessage};

#[test]
fn stale_accept_new_blocks_setting_is_rejected() {
    let error = toml::from_str::<ZakuraHeaderSyncConfig>("accept_new_blocks = true")
        .expect_err("the removed block-relay setting must not be silently ignored");

    assert!(error
        .to_string()
        .contains("unknown field `accept_new_blocks`"));
}

#[test]
fn advertised_inflight_limit_matches_the_one_lease_per_peer_contract() {
    let config = ZakuraHeaderSyncConfig::default();
    assert_eq!(config.advertised_max_inflight_requests(), 1);
    let error = toml::from_str::<ZakuraHeaderSyncConfig>("max_inflight_requests = 2")
        .expect_err("the fixed one-request capability is not configurable");
    assert!(error
        .to_string()
        .contains("unknown field `max_inflight_requests`"));
}

#[test]
fn configured_anchor_must_match_an_exact_network_checkpoint() {
    let mainnet = Network::Mainnet;
    let checkpoints = mainnet.checkpoint_list();
    let (&checkpoint_height, &checkpoint_hash) = checkpoints
        .iter()
        .nth(1)
        .expect("mainnet has a checkpoint above genesis");
    let mut config = ZakuraHeaderSyncConfig {
        anchor_height: Some(checkpoint_height),
        anchor_hash: Some(checkpoint_hash),
        ..ZakuraHeaderSyncConfig::default()
    };
    assert_eq!(
        config
            .anchor(&mainnet)
            .expect("exact checkpoint is trusted"),
        (checkpoint_height, checkpoint_hash),
    );

    config.anchor_hash = Some(block::Hash([0; 32]));
    assert!(matches!(
        config.anchor(&mainnet),
        Err(HeaderSyncStartError::InvalidAnchor { .. })
    ));

    let non_checkpoint_height = checkpoints
        .iter()
        .zip(checkpoints.iter().skip(1))
        .find_map(|((&height, _), (&next_height, _))| {
            (next_height.0 > height.0.saturating_add(1)).then_some(block::Height(height.0 + 1))
        })
        .expect("mainnet checkpoint list has a non-checkpoint height");
    config.anchor_height = Some(non_checkpoint_height);
    config.anchor_hash = Some(checkpoint_hash);
    assert!(matches!(
        config.anchor(&mainnet),
        Err(HeaderSyncStartError::InvalidAnchor { .. })
    ));

    let custom = Network::new_regtest(RegtestParameters {
        checkpoints: Some(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (
                block::Height(0),
                Network::new_regtest(Default::default()).genesis_hash(),
            ),
            (block::Height(10), hash(0x11)),
        ])),
        ..Default::default()
    });
    config.anchor_height = Some(block::Height(10));
    config.anchor_hash = Some(hash(0x11));
    assert_eq!(
        config
            .anchor(&custom)
            .expect("custom checkpoint is trusted"),
        (block::Height(10), hash(0x11)),
    );
    config.anchor_hash = Some(hash(0x12));
    assert!(matches!(
        config.anchor(&custom),
        Err(HeaderSyncStartError::InvalidAnchor { .. })
    ));
}

#[test]
fn status_refresh_interval_is_clamped_at_startup() {
    let network = Network::new_regtest(Default::default());
    let anchor = (block::Height(0), network.genesis_hash());
    let config = ZakuraHeaderSyncConfig {
        status_refresh_interval: std::time::Duration::ZERO,
        ..ZakuraHeaderSyncConfig::default()
    };
    let startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        config,
        u32::try_from(MAX_HS_MESSAGE_BYTES).expect("the wire cap fits in u32"),
    );

    assert_eq!(
        startup.status_refresh_interval,
        std::time::Duration::from_secs(1),
    );
}

fn codec() -> HeaderSyncCodec {
    HeaderSyncCodec::new(
        Network::new_regtest(Default::default()),
        u32::try_from(MAX_HS_MESSAGE_BYTES).expect("the 2 MiB hard cap fits in u32"),
        MAX_HS_RANGE,
        KNOWN_TREE_AUX_SCHEMA_MASK,
    )
}

fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

#[test]
fn local_serving_limits_reject_zero_resource_limits() {
    assert!(HeaderServingLimits::new(0, 1, 1, 1).is_none());
    assert!(HeaderServingLimits::new(1, 0, 1, 1).is_none());
    assert!(HeaderServingLimits::new(1, 1, 0, 1).is_none());
    assert!(HeaderServingLimits::new(1, 1, 1, 0).is_some());
}

fn request() -> GetHeaders {
    GetHeaders {
        request_id: 0x0807_0605_0403_0201,
        target_tip_hash: hash(0x22),
        locator_hashes: vec![hash(0x33), hash(0x44)],
        max_header_count: 4000,
        tree_aux_schema: AuxSchema::V1,
    }
}

fn empty_aux(height: block::Height) -> TreeAuxRecordV1 {
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

#[test]
fn status_golden_vector_both_directions_and_exact_work_width() {
    let status = Status {
        work_anchor_height: block::Height(7),
        work_anchor_hash: hash(0x11),
        selected_tip_height: block::Height(9),
        selected_tip_hash: hash(0x22),
        suffix_cumulative_work: U256::from_little_endian(&[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        oldest_retained_height: block::Height(5),
        max_headers_per_response: 4000,
        max_inflight_requests: 16,
        max_message_bytes: 2_097_152,
        tree_aux_schema_mask: 0x8000_0001,
    };
    let mut golden = vec![MSG_HS_STATUS];
    golden.extend_from_slice(&7u32.to_le_bytes());
    golden.extend_from_slice(&[0x11; 32]);
    golden.extend_from_slice(&9u32.to_le_bytes());
    golden.extend_from_slice(&[0x22; 32]);
    golden.extend(0u8..32);
    golden.extend_from_slice(&5u32.to_le_bytes());
    golden.extend_from_slice(&4000u32.to_le_bytes());
    golden.extend_from_slice(&16u16.to_le_bytes());
    golden.extend_from_slice(&2_097_152u32.to_le_bytes());
    golden.extend_from_slice(&0x8000_0001u32.to_le_bytes());

    let message = HeaderSyncMessage::Status(status);
    assert_eq!(
        codec().encode(&message).expect("valid status encodes"),
        golden
    );
    assert_eq!(
        codec()
            .decode(&golden, None)
            .expect("golden status decodes"),
        message
    );
    assert!(matches!(
        codec().decode(&golden[..golden.len() - 1], None),
        Err(HeaderSyncWireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));

    let mut zero_caps = golden;
    zero_caps[109..123].fill(0);
    assert!(matches!(
        codec()
            .decode(&zero_caps, None)
            .expect("zero received serving caps are a valid pure-requester status"),
        HeaderSyncMessage::Status(Status {
            max_headers_per_response: 0,
            max_inflight_requests: 0,
            max_message_bytes: 0,
            tree_aux_schema_mask: 0,
            ..
        })
    ));
}

#[test]
fn get_headers_golden_vector_both_directions() {
    let request = request();
    let mut golden = vec![MSG_HS_GET_HEADERS];
    golden.extend_from_slice(&request.request_id.to_le_bytes());
    golden.extend_from_slice(&[0x22; 32]);
    golden.push(2);
    golden.extend_from_slice(&[0x33; 32]);
    golden.extend_from_slice(&[0x44; 32]);
    golden.extend_from_slice(&4000u32.to_le_bytes());
    golden.push(1);

    let message = HeaderSyncMessage::GetHeaders(request);
    assert_eq!(
        codec().encode(&message).expect("valid request encodes"),
        golden
    );
    assert_eq!(
        codec()
            .decode(&golden, None)
            .expect("golden request decodes"),
        message
    );
}

#[test]
fn headers_golden_vector_uses_parallel_wire_sections() {
    let header = regtest_genesis_block().header.clone();
    let target_tip_hash = block::Hash::from(header.as_ref());
    let response = Headers {
        request_id: 9,
        target_tip_hash,
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: header.previous_block_hash,
        complete: true,
        tree_aux_schema: AuxSchema::V1,
        entries: vec![HeaderEntry {
            header: header.clone(),
            body_size: 2_000_000,
            tree_aux: Some(empty_aux(block::Height(1))),
        }],
    };
    let mut golden = vec![MSG_HS_HEADERS];
    golden.extend_from_slice(&9u64.to_le_bytes());
    target_tip_hash
        .zcash_serialize(&mut golden)
        .expect("hash serialization succeeds");
    golden.extend_from_slice(&0u32.to_le_bytes());
    header
        .previous_block_hash
        .zcash_serialize(&mut golden)
        .expect("hash serialization succeeds");
    golden.extend_from_slice(&1u32.to_le_bytes());
    golden.push(1);
    golden.push(1);
    header
        .zcash_serialize(&mut golden)
        .expect("header serialization succeeds");
    golden.extend_from_slice(&2_000_000u32.to_le_bytes());
    empty_aux(block::Height(1))
        .encode_to(&mut golden)
        .expect("tree aux serialization succeeds");

    assert_eq!(
        codec()
            .encode(&HeaderSyncMessage::Headers(response.clone()))
            .expect("valid response encodes"),
        golden
    );
    assert_eq!(
        codec()
            .decode(
                &golden,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::V1,
                }),
            )
            .expect("golden response decodes"),
        HeaderSyncMessage::Headers(response)
    );

    let header_offset = 1 + 8 + 32 + 4 + 32 + 4 + 1 + 1;
    let hint_offset = header_offset + header_sync_header_bytes_for_network(&codec().network);
    let aux_offset = hint_offset + 4;
    assert_eq!(
        &golden[hint_offset..aux_offset],
        &2_000_000u32.to_le_bytes()
    );
    assert_eq!(golden.len() - aux_offset, TREE_AUX_SCHEMA_V1_BYTES);
}

#[test]
fn headers_outcome_golden_vector_both_directions() {
    let message = HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
        request_id: 7,
        target_tip_hash: hash(0xaa),
        outcome: HeadersOutcomeCode::HistoryPruned,
    });
    let mut golden = vec![MSG_HS_HEADERS_OUTCOME];
    golden.extend_from_slice(&7u64.to_le_bytes());
    golden.extend_from_slice(&[0xaa; 32]);
    golden.push(3);
    assert_eq!(
        codec().encode(&message).expect("valid outcome encodes"),
        golden
    );
    assert_eq!(
        codec()
            .decode(&golden, None)
            .expect("golden outcome decodes"),
        message
    );
}

#[test]
fn discriminant_four_is_decoded_only_as_headers_outcome() {
    let outcome = HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
        request_id: 7,
        target_tip_hash: hash(0xaa),
        outcome: HeadersOutcomeCode::HistoryPruned,
    });
    let outcome_frame = codec()
        .encode_frame(&outcome)
        .expect("valid outcome encodes as a frame");
    assert_eq!(
        codec()
            .decode_frame(outcome_frame, None)
            .expect("a negotiated codec decodes outcome discriminator 4"),
        outcome
    );
    let mut block_relay_payload = vec![MSG_HS_HEADERS_OUTCOME];
    regtest_genesis_block()
        .zcash_serialize(&mut block_relay_payload)
        .expect("genesis block serialization succeeds");
    let block_relay_frame = Frame {
        message_type: u16::from(MSG_HS_HEADERS_OUTCOME),
        flags: 0,
        payload: block_relay_payload,
    };
    assert!(codec().decode_frame(block_relay_frame, None).is_err());
}

#[test]
fn bounded_decode_rejects_discriminants_ids_bools_heights_and_trailing_bytes() {
    assert!(matches!(
        codec().decode(&[5], None),
        Err(HeaderSyncWireError::UnknownMessageType(5))
    ));

    let outcome = codec()
        .encode(&HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id: 1,
            target_tip_hash: hash(0),
            outcome: HeadersOutcomeCode::Busy,
        }))
        .expect("valid outcome encodes");
    let mut zero_id = outcome.clone();
    zero_id[1..9].fill(0);
    assert!(matches!(
        codec().decode(&zero_id, None),
        Err(HeaderSyncWireError::ZeroRequestId("HeadersOutcome"))
    ));
    let mut unknown_outcome = outcome.clone();
    *unknown_outcome.last_mut().expect("outcome has a code") = 5;
    assert!(matches!(
        codec().decode(&unknown_outcome, None),
        Err(HeaderSyncWireError::UnknownOutcome(5))
    ));
    let mut trailing = outcome;
    trailing.push(0);
    assert!(matches!(
        codec().decode(&trailing, None),
        Err(HeaderSyncWireError::TrailingBytes)
    ));

    let empty_headers = Headers {
        request_id: 1,
        target_tip_hash: hash(1),
        common_ancestor_height: block::Height(1),
        common_ancestor_hash: hash(1),
        complete: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![],
    };
    let bytes = codec()
        .encode(&HeaderSyncMessage::Headers(empty_headers))
        .expect("valid empty completion encodes");
    let mut invalid_bool = bytes.clone();
    invalid_bool[81] = 2;
    assert!(matches!(
        codec().decode(
            &invalid_bool,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            })
        ),
        Err(HeaderSyncWireError::InvalidBool { .. })
    ));
    let mut invalid_empty_page = bytes.clone();
    invalid_empty_page[81] = 0;
    assert!(matches!(
        codec().decode(
            &invalid_empty_page,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            })
        ),
        Err(HeaderSyncWireError::InvalidHeadersCompletion)
    ));
    let mut invalid_height = bytes;
    invalid_height[41..45].copy_from_slice(&(block::Height::MAX.0 + 1).to_le_bytes());
    assert!(matches!(
        codec().decode(
            &invalid_height,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            })
        ),
        Err(HeaderSyncWireError::HeightOutOfRange(_))
    ));
}

#[test]
fn request_bounds_and_schema_advertisement_are_enforced() {
    for locator_count in [0, 14] {
        let mut request = request();
        request.locator_hashes = vec![hash(1); locator_count];
        assert!(matches!(
            codec().encode(&HeaderSyncMessage::GetHeaders(request)),
            Err(HeaderSyncWireError::CountOutOfRange {
                field: "locator",
                ..
            })
        ));
    }
    for count in [0, MAX_HS_RANGE + 1] {
        let mut request = request();
        request.max_header_count = count;
        assert!(matches!(
            codec().encode(&HeaderSyncMessage::GetHeaders(request)),
            Err(HeaderSyncWireError::CountOutOfRange {
                field: "max_header",
                ..
            })
        ));
    }
    let no_aux_codec = HeaderSyncCodec::new(Network::Mainnet, 1000, MAX_HS_RANGE, 0);
    assert!(matches!(
        no_aux_codec.encode(&HeaderSyncMessage::GetHeaders(request())),
        Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(1))
    ));

    let mut encoded = codec()
        .encode(&HeaderSyncMessage::GetHeaders(request()))
        .expect("valid request encodes");
    *encoded.last_mut().expect("request has a schema byte") = 2;
    assert!(matches!(
        codec().decode(&encoded, None),
        Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(2))
    ));
}

#[test]
fn payload_and_response_count_caps_apply_before_vector_decode() {
    let small_codec = HeaderSyncCodec::new(Network::Mainnet, 8, MAX_HS_RANGE, 1);
    assert!(matches!(
        small_codec.decode(&[0; 9], None),
        Err(HeaderSyncWireError::OversizedPayload { actual: 9, max: 8 })
    ));

    let mut response = vec![MSG_HS_HEADERS];
    response.extend_from_slice(&1u64.to_le_bytes());
    response.extend_from_slice(&[0; 32]);
    response.extend_from_slice(&0u32.to_le_bytes());
    response.extend_from_slice(&[0; 32]);
    response.extend_from_slice(&2u32.to_le_bytes());
    response.push(0);
    response.push(0);
    assert!(matches!(
        codec().decode(
            &response,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            })
        ),
        Err(HeaderSyncWireError::CountOutOfRange {
            field: "header",
            ..
        })
    ));
}

#[test]
fn body_hints_completion_and_aux_defaults_are_enforced() {
    let header = regtest_genesis_block().header.clone();
    let mut response = Headers {
        request_id: 1,
        target_tip_hash: block::Hash::from(header.as_ref()),
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: header.previous_block_hash,
        complete: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header,
            body_size: 2_000_001,
            tree_aux: None,
        }],
    };
    assert!(matches!(
        codec().encode(&HeaderSyncMessage::Headers(response.clone())),
        Err(HeaderSyncWireError::BodySizeHintOutOfRange(2_000_001))
    ));
    response.entries[0].body_size = 0;
    assert!(codec()
        .encode(&HeaderSyncMessage::Headers(response.clone()))
        .is_ok());
    response.complete = false;
    response.entries.clear();
    assert!(matches!(
        codec().encode(&HeaderSyncMessage::Headers(response)),
        Err(HeaderSyncWireError::InvalidHeadersCompletion)
    ));

    let network = Network::Mainnet;
    let pre_nu5 = NetworkUpgrade::Nu5
        .activation_height(&network)
        .expect("mainnet has NU5")
        .previous()
        .expect("NU5 activates above genesis");
    let mut aux = empty_aux(pre_nu5);
    aux.orchard_root = orchard::tree::Root::default();
    assert!(matches!(
        aux.validate_for(pre_nu5, &network),
        Err(HeaderSyncWireError::InvalidTreeAuxDefault {
            field: "orchard_root",
            ..
        })
    ));
    let mut aux = empty_aux(pre_nu5);
    aux.orchard_tx_count = 1;
    assert!(matches!(
        aux.validate_for(pre_nu5, &network),
        Err(HeaderSyncWireError::InvalidTreeAuxDefault {
            field: "orchard_tx_count",
            ..
        })
    ));
    let before_nu6_3 = NetworkUpgrade::Nu6_3
        .activation_height(&network)
        .expect("mainnet has NU6.3")
        .previous()
        .expect("NU6.3 activates above genesis");
    let mut aux = empty_aux(before_nu6_3);
    aux.ironwood_root = ironwood::tree::Root::default();
    assert!(matches!(
        aux.validate_for(before_nu6_3, &network),
        Err(HeaderSyncWireError::InvalidTreeAuxDefault {
            field: "ironwood_root",
            ..
        })
    ));
    let mut aux = empty_aux(before_nu6_3);
    aux.ironwood_tx_count = 1;
    assert!(matches!(
        aux.validate_for(before_nu6_3, &network),
        Err(HeaderSyncWireError::InvalidTreeAuxDefault {
            field: "ironwood_tx_count",
            ..
        })
    ));
}

#[test]
fn headers_encoding_uses_the_exact_known_payload_capacity() {
    let header = regtest_genesis_block().header.clone();
    let response = Headers {
        request_id: 1,
        target_tip_hash: block::Hash::from(header.as_ref()),
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: header.previous_block_hash,
        complete: true,
        tree_aux_schema: AuxSchema::V1,
        entries: vec![HeaderEntry {
            header,
            body_size: 0,
            tree_aux: Some(empty_aux(block::Height(1))),
        }],
    };
    let expected = headers_response_bytes(
        &codec().network,
        response.tree_aux_schema,
        response.entries.len(),
    )
    .expect("the bounded fixture has a known payload size");
    let encoded = codec()
        .encode(&HeaderSyncMessage::Headers(response))
        .expect("the exact-sized response encodes");

    assert_eq!(encoded.len(), expected);
    assert_eq!(encoded.capacity(), expected);
}

#[test]
fn broken_ancestry_and_completed_target_are_rejected() {
    let header = regtest_genesis_block().header.clone();
    let mut response = Headers {
        request_id: 1,
        target_tip_hash: header.hash(),
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: header.previous_block_hash,
        complete: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header,
            body_size: 0,
            tree_aux: None,
        }],
    };

    response.common_ancestor_hash = hash(0x41);
    assert!(matches!(
        codec().encode(&HeaderSyncMessage::Headers(response.clone())),
        Err(HeaderSyncWireError::NonContiguousHeaders)
    ));

    response.common_ancestor_hash = response.entries[0].header.previous_block_hash;
    response.target_tip_hash = hash(0x42);
    assert!(matches!(
        codec().encode(&HeaderSyncMessage::Headers(response)),
        Err(HeaderSyncWireError::InvalidHeadersCompletion)
    ));
}

#[test]
fn discriminator_four_never_guesses_block_relay_payload() {
    let block = regtest_genesis_block();
    let mut block_relay_payload = vec![MSG_HS_HEADERS_OUTCOME];
    block
        .zcash_serialize(&mut block_relay_payload)
        .expect("genesis block serialization succeeds");
    assert!(codec().decode(&block_relay_payload, None).is_err());
}

#[test]
fn decode_rejects_each_vector_hint_schema_and_byte_boundary() {
    let valid_request = codec()
        .encode(&HeaderSyncMessage::GetHeaders(request()))
        .expect("valid request encodes");
    for locator_count in [0, 14] {
        let mut malformed = valid_request.clone();
        malformed[41] = locator_count;
        assert!(matches!(
            codec().decode(&malformed, None),
            Err(HeaderSyncWireError::CountOutOfRange {
                field: "locator",
                ..
            })
        ));
    }
    let max_count_offset = valid_request.len() - 5;
    for count in [0, MAX_HS_RANGE + 1] {
        let mut malformed = valid_request.clone();
        malformed[max_count_offset..max_count_offset + 4].copy_from_slice(&count.to_le_bytes());
        assert!(matches!(
            codec().decode(&malformed, None),
            Err(HeaderSyncWireError::CountOutOfRange {
                field: "max_header",
                ..
            })
        ));
    }

    let header = regtest_genesis_block().header.clone();
    let response = Headers {
        request_id: 3,
        target_tip_hash: block::Hash::from(header.as_ref()),
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: header.previous_block_hash,
        complete: true,
        tree_aux_schema: AuxSchema::V1,
        entries: vec![HeaderEntry {
            header,
            body_size: 1,
            tree_aux: Some(empty_aux(block::Height(1))),
        }],
    };
    let encoded = codec()
        .encode(&HeaderSyncMessage::Headers(response))
        .expect("valid response encodes");
    let context = HeaderSyncDecodeContext {
        max_header_count: 1,
        requested_tree_aux_schema: AuxSchema::V1,
    };
    let header_offset = 83;
    let hint_offset = header_offset + header_sync_header_bytes_for_network(&codec().network);
    let aux_offset = hint_offset + 4;

    let mut bad_hint = encoded.clone();
    bad_hint[hint_offset..aux_offset].copy_from_slice(&2_000_001u32.to_le_bytes());
    assert!(matches!(
        codec().decode(&bad_hint, Some(context)),
        Err(HeaderSyncWireError::BodySizeHintOutOfRange(2_000_001))
    ));
    let mut wrong_aux_height = encoded.clone();
    wrong_aux_height[aux_offset..aux_offset + 4].copy_from_slice(&2u32.to_le_bytes());
    assert!(matches!(
        codec().decode(&wrong_aux_height, Some(context)),
        Err(HeaderSyncWireError::TreeAuxHeightMismatch { .. })
    ));
    assert!(matches!(
        codec().decode(&encoded[..encoded.len() - 1], Some(context)),
        Err(HeaderSyncWireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));

    let mut mismatched_schema = encoded;
    mismatched_schema[82] = AuxSchema::V1.wire_value();
    assert!(matches!(
        codec().decode(
            &mismatched_schema,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::None,
            })
        ),
        Err(HeaderSyncWireError::ResponseTreeAuxSchemaMismatch {
            requested: 0,
            actual: 1
        })
    ));

    let hard_cap_codec = HeaderSyncCodec::new(Network::Mainnet, u32::MAX, 1, 0);
    let over_hard_cap = vec![0; MAX_HS_MESSAGE_BYTES + 1];
    assert!(matches!(
        hard_cap_codec.decode(&over_hard_cap, None),
        Err(HeaderSyncWireError::OversizedPayload {
            actual,
            max: MAX_HS_MESSAGE_BYTES
        }) if actual == MAX_HS_MESSAGE_BYTES + 1
    ));
}
