//! Startup fixtures for the single-protocol header DAG cutover.

use std::sync::Arc;

use zakura_chain::{
    block::{self, Height},
    parameters::Network,
};
use zakura_header_chain::{
    prepare_headers, CheckpointSet, EngineConfig, EngineMode, Frontier, HeaderBatchInput,
    HeaderRules, StoreAuditRead, StoreRead, SystemClock, TrustedAnchor,
};

use super::{
    super::ZAKURA_HEADER_BY_HEIGHT,
    common::{mainnet_block, state_with_genesis_config, write_full_block_header_and_transactions},
};
use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
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
    assert_eq!(
        StoreAuditRead::selected_projection(&HeaderChainStore::new(state.header_chain_disk_db()))
            .expect("the initialized selection decodes"),
        vec![anchor]
    );

    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the authenticated context read succeeds")
        .expect("the finalized anchor is retained");
    assert_eq!(
        lease.predecessors.len(),
        2,
        "the lease contains the anchor and its one available predecessor"
    );
    let rules = HeaderRules::for_validation_lease(network, &lease)
        .expect("the production validation policy is authenticated");
    prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&block2.header)),
        &lease,
        &rules,
        &SystemClock,
    )
    .expect("the first post-anchor header validates from the seeded context");
}

#[test]
fn predecessor_overlay_fails_closed_without_mutation_or_publication() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
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
    let config = engine_config(network, &genesis);

    assert!(matches!(
        initialize_header_chain_reconciled(&state, &config, Vec::new()),
        Err(HeaderChainInitializationError::IncompatibleLegacyOverlay)
    ));
    assert!(StoreRead::metadata(&HeaderChainStore::new(state.header_chain_disk_db())).is_err());
    assert_eq!(
        state
            .db
            .raw_range_cf(&header_cf, &[], None)
            .expect("the rejected startup leaves the predecessor row untouched"),
        before
    );
}
