use zakura_chain::{
    block::merkle::AuthDataRoot,
    ironwood, orchard,
    parameters::{
        testnet::{ConfiguredCheckpoints, RegtestParameters},
        Network, NetworkUpgrade,
    },
    sapling,
};

use super::{config::*, wire::*, *};

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
fn tree_aux_records_need_their_header_height_and_activation_defaults() {
    let height = block::Height(7);
    assert!(matches!(
        empty_aux(height).validate_for(height.next().expect("a small height"), &Network::Mainnet),
        Err(HeaderSyncWireError::TreeAuxHeightMismatch { .. })
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

#[test]
fn bounded_serving_header_cap_keeps_encoded_responses_below_byte_ceiling() {
    assert!(
        u64::from(super::wire::MAX_HS_RANGE)
            * zakura_header_chain::BOUNDED_HEADER_RESPONSE_BYTES_PER_HEADER
            <= zakura_header_chain::MAX_BOUNDED_HEADER_RESPONSE_BYTES
    );
    for header_bytes in [COMMON_HEADER_BYTES, REGTEST_HEADER_BYTES] {
        for schema in [AuxSchema::None, AuxSchema::V1] {
            let bytes = v9::smallest_page(header_bytes, schema);
            assert!(
                u64::try_from(bytes).unwrap()
                    <= zakura_header_chain::BOUNDED_HEADER_RESPONSE_BYTES_PER_HEADER
            );
        }
    }
}
