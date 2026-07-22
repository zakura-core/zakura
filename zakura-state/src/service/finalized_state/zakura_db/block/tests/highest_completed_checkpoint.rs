use std::sync::Arc;

use zakura_chain::{
    block::{self, Block, Height},
    parameters::{
        testnet::{self, ConfiguredActivationHeights, ConfiguredCheckpoints},
        Network,
        Network::Mainnet,
        NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    work::difficulty::ParameterDifficulty,
};

use super::common::{
    mainnet_block, no_extra_checkpoint_test_network, persistent_config, persistent_state,
    state_with_genesis_config,
};
use crate::{
    service::{
        check::difficulty::{AdjustedDifficulty, POW_ADJUSTMENT_BLOCK_SPAN},
        finalized_state::{
            DiskWriteBatch, HighestCompletedCheckpoint, HighestCompletedCheckpointTracker,
            WriteDisk, ZakuraDb,
        },
    },
    Config,
};

const HEADER_BY_HEIGHT: &str = "zakura_header_by_height";
const HASH_BY_HEIGHT: &str = "zakura_header_hash_by_height";
const HEIGHT_BY_HASH: &str = "zakura_header_height_by_hash";

#[test]
fn advances_after_disk_commit_and_reconstructs_after_restart() {
    let _init_guard = zakura_test::init();
    let cache_dir = tempfile::tempdir().expect("temporary state directory is created");
    let (genesis, headers, network) = checkpoint_chain(&[2, 4]);
    let config = persistent_config(cache_dir.path());
    let mut state = state_with_genesis_config(&network, genesis.clone(), config.clone());
    let (mut tracker, mut receiver) = HighestCompletedCheckpointTracker::open(&state);
    let genesis_checkpoint = checkpoint(Height(0), genesis.hash());

    assert_eq!(tracker.current(), Some(genesis_checkpoint));
    assert_eq!(*receiver.borrow(), Some(genesis_checkpoint));

    let mut batch = DiskWriteBatch::new();
    batch
        .prepare_header_range_batch(&state, genesis.hash(), &headers[..2], &[0, 0])
        .expect("first checkpoint bracket is valid");
    let proposal = tracker
        .propose_after_headers(&state, genesis.hash(), &headers[..2])
        .expect("first checkpoint proposal is valid");

    assert_eq!(tracker.current(), Some(genesis_checkpoint));
    assert_eq!(*receiver.borrow(), Some(genesis_checkpoint));

    state
        .write_batch(batch)
        .expect("first checkpoint bracket writes");
    tracker.commit_success(proposal);
    let checkpoint_two = checkpoint(Height(2), block::Hash::from(headers[1].as_ref()));
    assert_eq!(tracker.current(), Some(checkpoint_two));
    assert!(receiver.has_changed().expect("tracker sender remains open"));
    assert_eq!(*receiver.borrow_and_update(), Some(checkpoint_two));
    assert!(!receiver.has_changed().expect("tracker sender remains open"));

    let mut batch = DiskWriteBatch::new();
    batch
        .prepare_header_range_batch(&state, checkpoint_two.hash, &headers[2..], &[0, 0])
        .expect("second checkpoint bracket is valid");
    let proposal = tracker
        .propose_after_headers(&state, checkpoint_two.hash, &headers[2..])
        .expect("second checkpoint proposal is valid");
    state
        .write_batch(batch)
        .expect("second checkpoint bracket writes");
    tracker.commit_success(proposal);

    let checkpoint_four = checkpoint(Height(4), block::Hash::from(headers[3].as_ref()));
    assert_eq!(tracker.current(), Some(checkpoint_four));
    assert!(receiver.has_changed().expect("tracker sender remains open"));
    drop(tracker);
    drop(receiver);
    state.shutdown(true);
    drop(state);

    let reopened = persistent_state(&config, &network);
    let (tracker, receiver) = HighestCompletedCheckpointTracker::open(&reopened);
    assert_eq!(tracker.current(), Some(checkpoint_four));
    assert_eq!(*receiver.borrow(), Some(checkpoint_four));
}

#[test]
fn reconstructs_header_completed_frontier_above_body_tip() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2, 5]);
    let state = state_with_genesis_config(&network, genesis, Config::ephemeral());

    for (index, header) in headers.iter().enumerate() {
        let height = Height(u32::try_from(index + 1).expect("small test height fits in u32"));
        store_header(&state, height, header);
    }

    let (tracker, receiver) = HighestCompletedCheckpointTracker::open(&state);
    let checkpoint_five = checkpoint(Height(5), block::Hash::from(headers[4].as_ref()));

    assert_eq!(state.finalized_tip_height(), Some(Height::MIN));
    assert_eq!(tracker.current(), Some(checkpoint_five));
    assert_eq!(*receiver.borrow(), Some(checkpoint_five));
}

#[test]
fn failed_write_proposal_has_no_side_effects() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2]);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    let (tracker, receiver) = HighestCompletedCheckpointTracker::open(&state);
    let initial = tracker.current();

    let _proposal = tracker
        .propose_after_headers(&state, genesis.hash(), &headers)
        .expect("checkpoint proposal is valid");

    // The production write path drops the proposal on a storage error.
    assert_eq!(tracker.current(), initial);
    assert_eq!(*receiver.borrow(), initial);

    let (reconstructed, reconstructed_receiver) = HighestCompletedCheckpointTracker::open(&state);
    assert_eq!(reconstructed.current(), initial);
    assert_eq!(*reconstructed_receiver.borrow(), initial);
}

#[test]
fn reconstruction_error_clears_published_checkpoint() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2]);
    let state = state_with_genesis_config(&network, genesis, Config::ephemeral());
    store_header(&state, Height(1), &headers[0]);
    store_header(&state, Height(2), &headers[1]);
    let (mut tracker, mut receiver) = HighestCompletedCheckpointTracker::open(&state);

    let checkpoint_two = checkpoint(Height(2), block::Hash::from(headers[1].as_ref()));
    assert_eq!(tracker.current(), Some(checkpoint_two));
    assert_eq!(*receiver.borrow_and_update(), Some(checkpoint_two));

    let mut conflicting = *headers[1].as_ref();
    conflicting.nonce.0[0] = conflicting.nonce.0[0].wrapping_add(1);
    store_header(&state, Height(2), &Arc::new(conflicting));

    assert!(tracker.rebind_from_db(&state).is_err());
    assert_eq!(tracker.current(), None);
    assert!(receiver.has_changed().expect("tracker sender remains open"));
    assert_eq!(*receiver.borrow_and_update(), None);
}

#[test]
fn open_clears_checkpoint_on_reconstruction_error() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2]);
    let state = state_with_genesis_config(&network, genesis, Config::ephemeral());
    store_header(&state, Height(1), &headers[0]);

    let mut conflicting = *headers[1].as_ref();
    conflicting.nonce.0[0] = conflicting.nonce.0[0].wrapping_add(1);
    store_header(&state, Height(2), &Arc::new(conflicting));

    let (tracker, receiver) = HighestCompletedCheckpointTracker::open(&state);
    assert_eq!(tracker.current(), None);
    assert_eq!(*receiver.borrow(), None);
    assert!(!receiver.has_changed().expect("tracker sender remains open"));
}

#[test]
fn reconstruction_stops_at_first_header_gap() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2, 4]);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());

    store_header(&state, Height(1), &headers[0]);
    store_header(&state, Height(3), &headers[2]);
    store_header(&state, Height(4), &headers[3]);

    let (tracker, receiver) = HighestCompletedCheckpointTracker::open(&state);
    let genesis_checkpoint = checkpoint(Height(0), genesis.hash());

    assert_eq!(
        state.best_header_tip().map(|(height, _)| height),
        Some(Height(4))
    );
    assert_eq!(tracker.current(), Some(genesis_checkpoint));
    assert_eq!(*receiver.borrow(), Some(genesis_checkpoint));
}

#[test]
fn completed_checkpoint_rejects_conflicting_ancestor() {
    let _init_guard = zakura_test::init();
    let (genesis, headers, network) = checkpoint_chain(&[2]);
    let state = state_with_genesis_config(&network, genesis.clone(), Config::ephemeral());
    store_header(&state, Height(1), &headers[0]);
    store_header(&state, Height(2), &headers[1]);
    let (tracker, _receiver) = HighestCompletedCheckpointTracker::open(&state);

    let mut conflicting = *headers[0].as_ref();
    conflicting.nonce.0[0] = conflicting.nonce.0[0].wrapping_add(1);

    assert_eq!(
        tracker.check_immutable_conflicts(&state, genesis.hash(), &[Arc::new(conflicting)]),
        Err(Height(1))
    );
}

fn checkpoint(height: Height, hash: block::Hash) -> HighestCompletedCheckpoint {
    HighestCompletedCheckpoint { height, hash }
}

fn checkpoint_chain(checkpoint_heights: &[u32]) -> (Arc<Block>, Vec<Arc<block::Header>>, Network) {
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("mainnet genesis block deserializes");
    let base_network = no_extra_checkpoint_test_network(genesis.hash());
    let base_state = state_with_genesis_config(&base_network, genesis.clone(), Config::ephemeral());
    let max_height = checkpoint_heights
        .iter()
        .copied()
        .max()
        .expect("test checkpoint list is non-empty");
    let headers = synthetic_headers(&base_state, max_height);
    let network = checkpoint_network(&genesis, &headers, checkpoint_heights);

    (genesis, headers, network)
}

fn checkpoint_network(
    genesis: &Block,
    headers: &[Arc<block::Header>],
    checkpoint_heights: &[u32],
) -> Network {
    let mut checkpoints = vec![(Height(0), genesis.hash())];
    checkpoints.extend(checkpoint_heights.iter().map(|height| {
        let index = usize::try_from(height - 1).expect("test checkpoint index fits in usize");
        (Height(*height), block::Hash::from(headers[index].as_ref()))
    }));

    testnet::Parameters::build()
        .with_network_name("HighestCompletedCheckpointTest")
        .expect("test network name is valid")
        .with_genesis_hash(genesis.hash())
        .expect("test genesis hash is valid")
        .with_target_difficulty_limit(Mainnet.target_difficulty_limit())
        .expect("mainnet difficulty limit is valid for test network")
        .with_activation_heights(ConfiguredActivationHeights {
            canopy: Some(1),
            ..Default::default()
        })
        .expect("test activation heights are valid")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(checkpoints))
        .expect("test checkpoints are valid")
        .to_network()
        .expect("test network is valid")
}

fn synthetic_headers(state: &ZakuraDb, count: u32) -> Vec<Arc<block::Header>> {
    let network = state.network();
    let template = mainnet_block(1);
    let mut context = state
        .recent_header_context(Height(0))
        .expect("genesis header context is coherent");
    let mut previous_hash = network.genesis_hash();
    let mut previous_height = Height(0);

    (1..=count)
        .map(|nonce_tag| {
            let candidate_height = previous_height
                .next()
                .expect("test header height remains in range");
            let previous_time = context[0].1;
            let target_spacing =
                NetworkUpgrade::target_spacing_for_height(&network, candidate_height);
            let candidate_time = previous_time + target_spacing;
            let expected_difficulty = AdjustedDifficulty::new_from_header_time(
                candidate_time,
                previous_height,
                &network,
                context.iter().copied(),
            )
            .expected_difficulty_threshold();

            let mut header = *template.header;
            header.previous_block_hash = previous_hash;
            header.time = candidate_time;
            header.difficulty_threshold = expected_difficulty;
            header.nonce.0[0] = header.nonce.0[0]
                .wrapping_add(u8::try_from(nonce_tag).expect("small test nonce fits in u8"));

            let header = Arc::new(header);
            previous_hash = block::Hash::from(header.as_ref());
            previous_height = candidate_height;
            context.insert(0, (header.difficulty_threshold, header.time));
            context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
            header
        })
        .collect()
}

fn store_header(state: &ZakuraDb, height: Height, header: &Arc<block::Header>) {
    let header_by_height = state
        .db()
        .cf_handle(HEADER_BY_HEIGHT)
        .expect("header column exists");
    let hash_by_height = state
        .db()
        .cf_handle(HASH_BY_HEIGHT)
        .expect("height-to-hash column exists");
    let height_by_hash = state
        .db()
        .cf_handle(HEIGHT_BY_HASH)
        .expect("hash-to-height column exists");
    let hash = block::Hash::from(header.as_ref());
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_by_height, height, Arc::clone(header));
    batch.zs_insert(&hash_by_height, height, hash);
    batch.zs_insert(&height_by_hash, hash, height);
    state.db().write(batch).expect("test header rows write");
}
