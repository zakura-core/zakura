//! Golden fixtures for the one-way legacy header-overlay migration.

use std::sync::Arc;

use zakura_chain::{
    block::{self, Height},
    parameters::{testnet::RegtestParameters, Network, NetworkUpgrade},
    work::difficulty::CompactDifficulty,
};
use zakura_header_chain::{
    validate_hash_filter, AdjustedDifficulty, CheckpointSet, EngineConfig, EngineMode, Frontier,
    StoreAuditRead, StoreRead, TrustedAnchor,
};

use super::{
    super::{ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH},
    common::{
        commit_header_range, mainnet_block, state_with_genesis_config,
        write_full_block_header_and_transactions,
    },
};
use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        header_chain::{
            migration::{migrate_v7_header_store, HeaderChainMigrationError},
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
    .expect("the golden fixture has an authenticated genesis anchor")
}

fn selected(store: &HeaderChainStore) -> Vec<Frontier> {
    StoreAuditRead::selected_projection(store).expect("the migrated selection decodes")
}

#[test]
fn fresh_legacy_store_migrates_only_the_full_state_anchor() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    let config = engine_config(network, &genesis);

    let (runtime, report) =
        migrate_v7_header_store(&state, &config).expect("a fresh legacy store migrates");
    let anchor = Frontier::new(Height(0), genesis.hash());
    assert_eq!(report.anchor, anchor);
    assert_eq!(report.imported_headers, 0);
    assert_eq!(report.startup.current.frontiers.header_best, anchor);
    assert_eq!(runtime.publisher().snapshot(), report.startup.current);
    assert_eq!(
        selected(&HeaderChainStore::new(state.header_chain_disk_db())),
        vec![anchor]
    );
    assert!(matches!(
        migrate_v7_header_store(&state, &config),
        Err(HeaderChainMigrationError::AlreadyInitialized)
    ));
}

#[test]
fn mid_sync_legacy_store_preserves_its_exact_selected_projection() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let block2 = mainnet_block(2);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    write_full_block_header_and_transactions(&state, block1.clone());
    commit_header_range(&state, block1.hash(), std::slice::from_ref(&block2.header));
    let old_rows = state.headers_by_height_range(Height(2), 1);
    let config = engine_config(network, &genesis);

    let (_, report) =
        migrate_v7_header_store(&state, &config).expect("the coherent mid-sync store migrates");
    assert_eq!(report.anchor, Frontier::new(Height(1), block1.hash()));
    assert_eq!(report.imported_headers, 1);
    assert_eq!(report.validation_context_rows, 1);
    assert_eq!(state.headers_by_height_range(Height(2), 1), old_rows);
    assert_eq!(
        selected(&HeaderChainStore::new(state.header_chain_disk_db())),
        vec![
            Frontier::new(Height(1), block1.hash()),
            Frontier::new(Height(2), block2.hash()),
        ]
    );
}

#[test]
fn legacy_reorg_history_migrates_only_the_coherent_winning_path() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(RegtestParameters::default());
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    let original = synthetic_headers(&state, Height(0), genesis.hash(), 2, 1);
    commit_header_range(&state, genesis.hash(), &original);
    let replacement = synthetic_headers(&state, Height(0), genesis.hash(), 3, 19);
    commit_header_range(&state, genesis.hash(), &replacement);
    let expected: Vec<_> = std::iter::once(Frontier::new(Height(0), genesis.hash()))
        .chain(replacement.iter().enumerate().map(|(index, header)| {
            let offset = u32::try_from(index + 1).expect("the tiny fixture offset fits u32");
            Frontier::new(Height(offset), header.hash())
        }))
        .collect();
    let config = engine_config(network, &genesis);

    let (_, report) = migrate_v7_header_store(&state, &config)
        .expect("the coherent post-reorg selected path migrates");
    assert_eq!(report.imported_headers, replacement.len());
    assert_eq!(
        selected(&HeaderChainStore::new(state.header_chain_disk_db())),
        expected
    );
}

#[test]
fn incoherent_legacy_store_writes_no_format_marker() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    commit_header_range(&state, genesis.hash(), std::slice::from_ref(&block1.header));
    let reverse = state
        .db
        .cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH)
        .expect("the legacy reverse-index column is open");
    let mut corrupt = DiskWriteBatch::new();
    corrupt.zs_delete(&reverse, block1.hash());
    state
        .db
        .write(corrupt)
        .expect("the fixture corruption writes");
    let config = engine_config(network, &genesis);

    assert!(migrate_v7_header_store(&state, &config).is_err());
    let store = HeaderChainStore::new(state.header_chain_disk_db());
    assert!(StoreRead::metadata(&store).is_err());
    assert_eq!(
        state.headers_by_height_range(Height(1), 1),
        vec![(Height(1), block1.hash(), block1.header.clone())]
    );
    assert_eq!(
        state
            .db
            .raw_range_cf(
                &state
                    .db
                    .cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT)
                    .expect("the legacy forward index is open"),
                &[],
                None,
            )
            .expect("the legacy forward index remains readable")
            .len(),
        1
    );
}

#[test]
fn linked_bijection_with_the_wrong_anchor_parent_writes_no_format_marker() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = mainnet_block(0);
    let block1 = mainnet_block(1);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    commit_header_range(&state, genesis.hash(), std::slice::from_ref(&block1.header));

    let mut wrong_header = *block1.header;
    wrong_header.previous_block_hash = block::Hash([0x5a; 32]);
    let wrong_header = Arc::new(wrong_header);
    let wrong_hash = wrong_header.hash();
    let header_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BY_HEIGHT)
        .expect("the legacy header column is open");
    let hash_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT)
        .expect("the legacy forward-index column is open");
    let reverse_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH)
        .expect("the legacy reverse-index column is open");
    let mut corrupt = DiskWriteBatch::new();
    corrupt.zs_insert(&header_cf, Height(1), wrong_header);
    corrupt.zs_insert(&hash_cf, Height(1), wrong_hash);
    corrupt.zs_delete(&reverse_cf, block1.hash());
    corrupt.zs_insert(&reverse_cf, wrong_hash, Height(1));
    state
        .db
        .write(corrupt)
        .expect("the linked-bijection fixture corruption writes");
    let config = engine_config(network, &genesis);

    assert!(migrate_v7_header_store(&state, &config).is_err());
    assert!(StoreRead::metadata(&HeaderChainStore::new(state.header_chain_disk_db())).is_err());
}

fn synthetic_headers(
    state: &super::super::ZakuraDb,
    anchor_height: Height,
    anchor_hash: block::Hash,
    count: u32,
    nonce_seed: u8,
) -> Vec<Arc<block::Header>> {
    let network = state.network();
    let template = state
        .header_by_height(anchor_height)
        .expect("the synthetic anchor header exists")
        .1;
    let mut context = state
        .recent_header_context(anchor_height)
        .expect("the synthetic fixture context is coherent");
    let mut previous_hash = anchor_hash;
    let mut previous_height = anchor_height;

    (0..count)
        .map(|index| {
            let height = previous_height
                .next()
                .expect("the tiny fixture remains in the height domain");
            let time = context.first().expect("the anchor context exists").1
                + NetworkUpgrade::target_spacing_for_height(&network, height);
            let difficulty = AdjustedDifficulty::new_from_header_time(
                time,
                previous_height,
                &network,
                context.iter().copied(),
            )
            .expected_difficulty_threshold();
            let header = mine_waived_header(
                &template,
                previous_hash,
                time,
                difficulty,
                nonce_seed
                    .wrapping_add(u8::try_from(index).expect("the tiny fixture index fits u8")),
            );
            previous_hash = header.hash();
            previous_height = height;
            context.insert(0, (header.difficulty_threshold, header.time));
            context.truncate(crate::service::check::difficulty::POW_ADJUSTMENT_BLOCK_SPAN);
            header
        })
        .collect()
}

fn mine_waived_header(
    template: &Arc<block::Header>,
    previous_hash: block::Hash,
    time: chrono::DateTime<chrono::Utc>,
    difficulty: CompactDifficulty,
    nonce_seed: u8,
) -> Arc<block::Header> {
    let target = difficulty
        .to_expanded()
        .expect("the contextual fixture target expands");
    for attempt in 0_u32..100_000 {
        let mut header = **template;
        header.previous_block_hash = previous_hash;
        header.time = time;
        header.difficulty_threshold = difficulty;
        header.nonce.0[0] = nonce_seed;
        header.nonce.0[1..5].copy_from_slice(&attempt.to_be_bytes());
        if validate_hash_filter(header.hash(), target).is_ok() {
            return Arc::new(header);
        }
    }
    panic!("the easy regtest target should find a header hash within the bounded search")
}
