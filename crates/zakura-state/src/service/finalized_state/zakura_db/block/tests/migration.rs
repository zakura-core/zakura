//! Startup fixtures for the single-protocol header DAG cutover.

use std::sync::Arc;

use zakura_chain::{
    block::{self, Height},
    parameters::Network,
    work::difficulty::U256,
};
use zakura_header_chain::{
    prepare_headers, CheckpointSet, EngineConfig, EngineMode, Frontier, HeaderBatchInput,
    HeaderRules, RowLimit, StoreAuditRead, StoreAuditSnapshot, SystemClock, TrustedAnchor,
    MAX_NON_FINALIZED_NODES_V1,
};

use super::{
    super::{
        ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    },
    common::{mainnet_block, state_with_genesis_config, write_full_block_header_and_transactions},
};
use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        disk_format::RawBytes,
        header_chain::{
            migration::{initialize_header_chain_reconciled, HeaderChainInitializationError},
            HeaderChainStore,
        },
    },
    Config,
};

fn engine_config(network: Network, genesis: &Arc<block::Block>) -> EngineConfig {
    let frontier = Frontier::new(Height(0), genesis.hash());
    EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier,
            header: genesis.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the fixture has an authenticated genesis anchor")
}

#[test]
fn clean_store_initializes_only_from_finalized_full_state() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let block2 = mainnet_block(2);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    write_full_block_header_and_transactions(&state, block1.clone());
    let config = engine_config(network.clone(), &genesis);

    let (runtime, report) = initialize_header_chain_reconciled(&state, &config, Vec::new())
        .expect("an empty overlay initializes from authenticated full state");
    let anchor = Frontier::new(Height(1), block1.hash());
    assert_eq!(report.anchor, anchor);
    assert_eq!(report.validation_context_rows, 1);
    assert_eq!(report.startup.current.frontiers.header_best, anchor);
    assert_eq!(runtime.publisher().snapshot(), report.startup.current);
    let store = HeaderChainStore::new(state.header_chain_disk_db());
    let audit = store.audit_snapshot().expect("the audit snapshot opens");
    let metadata = audit.metadata().expect("the initialized metadata decodes");
    assert_eq!(metadata.work_origin, anchor);
    let mut nodes = Vec::new();
    audit
        .visit_header_nodes(RowLimit::new(MAX_NON_FINALIZED_NODES_V1 + 1), &mut |node| {
            nodes.push(node);
            Ok(())
        })
        .expect("the initialized node decodes");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].work_coordinate().origin_hash(), anchor.hash);
    assert_eq!(nodes[0].work_coordinate().cumulative_work(), U256::zero());
    assert_eq!(
        store
            .selected_projection()
            .expect("the initialized selection decodes"),
        vec![anchor]
    );

    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the authenticated context read succeeds")
        .expect("the finalized anchor is retained");
    assert_eq!(
        lease.predecessors().len(),
        2,
        "the lease contains the anchor and its one available predecessor"
    );
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the production validation policy is authenticated");
    prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&block2.header)),
        lease.parent(),
        &rules,
        &SystemClock,
    )
    .expect("the first post-anchor header validates from the seeded context");
}

#[test]
fn predecessor_overlay_is_atomically_replaced_from_finalized_state() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let block2 = mainnet_block(2);
    let block3 = mainnet_block(3);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    write_full_block_header_and_transactions(&state, block1.clone());
    let header_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let hash_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let height_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH)
        .expect("the obsolete column remains physically present");
    let body_size_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .expect("the advisory body-size column remains physically present");
    let mut legacy = DiskWriteBatch::new();
    legacy.zs_insert(&header_cf, Height(2), &block2.header);
    legacy.zs_insert(&header_cf, Height(3), &block3.header);
    legacy.zs_insert(&hash_cf, Height(2), block2.hash());
    legacy.zs_insert(&hash_cf, Height(3), block3.hash());
    legacy.zs_insert(&height_cf, block2.hash(), Height(2));
    legacy.zs_insert(&height_cf, block3.hash(), Height(3));
    legacy.zs_insert(
        &body_size_cf,
        Height(2),
        RawBytes::new_raw_bytes(1_u32.to_be_bytes().to_vec()),
    );
    state
        .db
        .write(legacy)
        .expect("the legacy fixture row writes");
    let config = engine_config(network, &genesis);

    let (runtime, report) = initialize_header_chain_reconciled(&state, &config, Vec::new())
        .expect("obsolete overlay rows are replaced from authenticated full state");
    let anchor = Frontier::new(Height(1), block1.hash());
    assert_eq!(report.anchor, anchor);
    assert_eq!(report.startup.current.frontiers.header_best, anchor);
    assert_eq!(runtime.publisher().snapshot(), report.startup.current);
    assert_eq!(state.hash(Height(1)), Some(block1.hash()));
    assert_eq!(
        HeaderChainStore::new(state.header_chain_disk_db())
            .selected_projection()
            .expect("the initialized selection decodes"),
        vec![anchor]
    );
    for family in [
        ZAKURA_HEADER_BY_HEIGHT,
        ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    ] {
        let cf = state
            .db
            .cf_handle(family)
            .expect("the obsolete column remains physically present");
        assert!(
            state
                .db
                .raw_range_cf(&cf, &[], None)
                .expect("the obsolete column remains readable")
                .is_empty(),
            "{family} is empty after migration"
        );
    }
    assert_eq!(
        state
            .db
            .raw_range_cf(&body_size_cf, &[], None)
            .expect("the advisory body-size column remains readable")
            .len(),
        1,
        "migration preserves the advisory body-size column"
    );
    assert!(matches!(
        initialize_header_chain_reconciled(&state, &config, Vec::new()),
        Err(HeaderChainInitializationError::AlreadyInitialized)
    ));
    assert_eq!(
        runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the authenticated context read succeeds")
            .expect("the finalized anchor is retained")
            .predecessors()
            .len(),
        2
    );
}

#[test]
fn predecessor_overlay_is_preserved_when_full_state_authentication_fails() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let state = state_with_genesis_config(&network, genesis, Config::ephemeral());
    let header_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let mut legacy = DiskWriteBatch::new();
    legacy.zs_insert(&header_cf, Height(1), &block1.header);
    state
        .db
        .write(legacy)
        .expect("the legacy fixture row writes");
    let before = state
        .db
        .raw_range_cf(&header_cf, &[], None)
        .expect("the predecessor row can be observed without decoding it");
    let config = EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier: Frontier::new(Height(1), block1.hash()),
            header: block1.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the fixture has an authenticated but unavailable anchor");

    assert!(matches!(
        initialize_header_chain_reconciled(&state, &config, Vec::new()),
        Err(HeaderChainInitializationError::AnchorMismatch)
    ));
    assert!(HeaderChainStore::new(state.header_chain_disk_db())
        .metadata()
        .is_err());
    assert_eq!(
        state
            .db
            .raw_range_cf(&header_cf, &[], None)
            .expect("the rejected startup leaves the predecessor row untouched"),
        before
    );
}

#[test]
fn initialization_rejects_finalized_tip_header_mismatch_before_cleanup() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let block2 = mainnet_block(2);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    write_full_block_header_and_transactions(&state, block1);
    let finalized_header_cf = state
        .db
        .cf_handle("block_header_by_height")
        .expect("the finalized header column exists");
    let legacy_header_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let mut corrupted = DiskWriteBatch::new();
    corrupted.zs_insert(&finalized_header_cf, Height(1), &genesis.header);
    corrupted.zs_insert(&legacy_header_cf, Height(2), &block2.header);
    state
        .db
        .write(corrupted)
        .expect("the mismatched tip and legacy fixture write");
    let legacy_before = state
        .db
        .raw_range_cf(&legacy_header_cf, &[], None)
        .expect("the predecessor row can be observed without decoding it");
    let config = engine_config(network, &genesis);

    assert!(matches!(
        initialize_header_chain_reconciled(&state, &config, Vec::new()),
        Err(HeaderChainInitializationError::AnchorMismatch)
    ));
    assert!(HeaderChainStore::new(state.header_chain_disk_db())
        .metadata()
        .is_err());
    assert_eq!(
        state
            .db
            .raw_range_cf(&legacy_header_cf, &[], None)
            .expect("the rejected startup leaves the predecessor row untouched"),
        legacy_before
    );
}

#[test]
fn initialization_does_not_fill_finalized_gaps_from_legacy_overlay() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    write_full_block_header_and_transactions(&state, block1);
    let finalized_hash_cf = state
        .db
        .cf_handle("hash_by_height")
        .expect("the finalized hash column exists");
    let header_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let hash_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT)
        .expect("the obsolete column remains physically present");
    let height_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH)
        .expect("the obsolete column remains physically present");
    let mut corrupted = DiskWriteBatch::new();
    corrupted.zs_delete(&finalized_hash_cf, Height(0));
    corrupted.zs_insert(&header_cf, Height(0), &genesis.header);
    corrupted.zs_insert(&hash_cf, Height(0), genesis.hash());
    corrupted.zs_insert(&height_cf, genesis.hash(), Height(0));
    state
        .db
        .write(corrupted)
        .expect("the finalized gap and legacy fallback fixture write");
    let legacy_before = [
        ZAKURA_HEADER_BY_HEIGHT,
        ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    ]
    .map(|family| {
        let cf = state
            .db
            .cf_handle(family)
            .expect("the obsolete column remains physically present");
        state
            .db
            .raw_range_cf(&cf, &[], None)
            .expect("the predecessor rows can be observed without decoding them")
    });
    let config = engine_config(network, &genesis);

    assert!(matches!(
        initialize_header_chain_reconciled(&state, &config, Vec::new()),
        Err(HeaderChainInitializationError::AnchorMismatch)
    ));
    assert!(HeaderChainStore::new(state.header_chain_disk_db())
        .metadata()
        .is_err());
    for (family, before) in [
        ZAKURA_HEADER_BY_HEIGHT,
        ZAKURA_HEADER_HASH_BY_HEIGHT,
        ZAKURA_HEADER_HEIGHT_BY_HASH,
    ]
    .into_iter()
    .zip(legacy_before)
    {
        let cf = state
            .db
            .cf_handle(family)
            .expect("the obsolete column remains physically present");
        assert_eq!(
            state
                .db
                .raw_range_cf(&cf, &[], None)
                .expect("the rejected startup leaves predecessor rows untouched"),
            before
        );
    }
}
