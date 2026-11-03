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

fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
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
