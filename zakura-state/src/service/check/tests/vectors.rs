//! Fixed test vectors for state contextual validation checks.

use chrono::{DateTime, Duration};

use zakura_chain::{
    block::{merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, CommitmentError},
    history_tree::HistoryTree,
    parameters::{Network, NetworkUpgrade},
    sapling,
    serialization::ZcashDeserializeInto,
    work::difficulty::ParameterDifficulty,
};

use super::super::*;
use crate::tests::FakeChainHelper;

#[test]
fn test_orphan_consensus_check() {
    let _init_guard = zakura_test::init();

    let height = zakura_test::vectors::BLOCK_MAINNET_347499_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .unwrap()
        .coinbase_height()
        .unwrap();

    block_is_not_orphaned(block::Height(0), height).expect("tip is lower so it should be fine");
    block_is_not_orphaned(block::Height(347498), height)
        .expect("tip is lower so it should be fine");
    block_is_not_orphaned(block::Height(347499), height)
        .expect_err("tip is equal so it should error");
    block_is_not_orphaned(block::Height(500000), height)
        .expect_err("tip is higher so it should error");
}

#[test]
fn test_sequential_height_check() {
    let _init_guard = zakura_test::init();

    let height = zakura_test::vectors::BLOCK_MAINNET_347499_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .unwrap()
        .coinbase_height()
        .unwrap();

    height_one_more_than_parent_height(block::Height(0), height)
        .expect_err("block is much lower, should panic");
    height_one_more_than_parent_height(block::Height(347497), height)
        .expect_err("parent height is 2 less, should panic");
    height_one_more_than_parent_height(block::Height(347498), height)
        .expect("parent height is 1 less, should be good");
    height_one_more_than_parent_height(block::Height(347499), height)
        .expect_err("parent height is equal, should panic");
    height_one_more_than_parent_height(block::Height(347500), height)
        .expect_err("parent height is way more, should panic");
    height_one_more_than_parent_height(block::Height(500000), height)
        .expect_err("parent height is way more, should panic");
}

/// The commitment check *uses* a supplied precomputed auth data root instead of
/// re-deriving it from the block body (re-deriving would negate the point of
/// precomputing). A matching value passes; a forged value makes an otherwise-valid
/// header fail the ZIP-244 commitment check, proving the supplied value is the one
/// bound into the check.
#[test]
fn block_commitment_uses_the_precomputed_auth_data_root() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let parent_height = 1_687_106;
    let (blocks, sapling_roots) = network.block_sapling_roots_map();

    let parent = Arc::new(
        blocks
            .get(&parent_height)
            .expect("NU5 parent test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("NU5 parent block deserializes"),
    );
    let sapling_root = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&parent_height)
            .expect("NU5 parent Sapling root exists"),
    )
    .expect("Sapling root vector is valid");
    let history_tree = HistoryTree::from_block(
        &network,
        parent.clone(),
        &sapling_root,
        &Default::default(),
        &Default::default(),
    )
    .expect("NU5 parent builds a history tree");

    let child = parent.make_fake_child();
    let auth_data_root = child.auth_data_root();
    let hash_block_commitments = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &history_tree
            .hash()
            .expect("NU5 parent history tree has a root"),
        &auth_data_root,
    );
    let block_commitment: [u8; 32] = hash_block_commitments.into();
    let child = child.set_block_commitment(block_commitment);

    block_commitment_is_valid_for_chain_history(
        child.clone(),
        &network,
        &history_tree,
        Some(auth_data_root),
    )
    .expect("a matching precomputed auth data root is accepted");

    let forged_auth_data_root = AuthDataRoot::from([0x42; 32]);
    assert_ne!(
        forged_auth_data_root, auth_data_root,
        "the forged root must differ from the block body root"
    );
    let error = block_commitment_is_valid_for_chain_history(
        child,
        &network,
        &history_tree,
        Some(forged_auth_data_root),
    )
    .expect_err("a forged precomputed auth data root must fail the commitment check");

    // The supplied root is trusted, not compared against the body, so the forgery
    // surfaces as a header-commitment mismatch: the header committed to the real
    // root, while the check recomputed the commitment from the forged one.
    let forged_hash_block_commitments = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &history_tree
            .hash()
            .expect("NU5 parent history tree has a root"),
        &forged_auth_data_root,
    );
    assert!(matches!(
        error,
        ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { actual, expected }
        ) if actual == block_commitment
            && expected == <[u8; 32]>::from(forged_hash_block_commitments)
    ));
}

#[test]
fn header_daa_accepts_valid_threshold_with_full_context() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let previous_block_height = block::Height(99);
    let candidate_time = DateTime::from_timestamp(15_000, 0).expect("test timestamp is in-range");
    let relevant_headers = daa_context(&network, previous_block_height, candidate_time);
    let expected = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_block_height,
        &network,
        relevant_headers.clone(),
    )
    .expected_difficulty_threshold();
    let mut candidate = *zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 1 deserializes")
        .header
        .as_ref();
    candidate.time = candidate_time;
    candidate.difficulty_threshold = expected;

    header_is_valid_for_recent_chain(
        &candidate,
        previous_block_height,
        &network,
        relevant_headers,
    )
    .expect("expected DAA threshold is accepted");
}

#[test]
fn header_daa_rejects_bad_threshold_with_full_context() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let previous_block_height = block::Height(99);
    let candidate_time = DateTime::from_timestamp(15_000, 0).expect("test timestamp is in-range");
    let relevant_headers = daa_context(&network, previous_block_height, candidate_time);
    let mut candidate = *zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 1 deserializes")
        .header
        .as_ref();
    candidate.time = candidate_time;
    candidate.difficulty_threshold = network.target_difficulty_limit().to_compact();

    header_is_valid_for_recent_chain(
        &candidate,
        previous_block_height,
        &network,
        relevant_headers,
    )
    .expect_err("unexpected DAA threshold is rejected");
}

#[test]
fn height_one_header_skips_max_time_limit_but_later_mainnet_headers_do_not() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("genesis block deserializes");
    let block1 = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 1 deserializes");
    let mut candidate = *block1.header;
    candidate.time = genesis.header.time + Duration::hours(24);
    let context = [(genesis.header.difficulty_threshold, genesis.header.time)];

    header_is_valid_for_recent_chain(&candidate, block::Height(0), &network, context)
        .expect("height 1 is outside the Mainnet max-time consensus rule");

    let block2 = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 2 deserializes");
    let mut candidate = *block2.header;
    candidate.time = block1.header.time + Duration::hours(24);
    let context = [
        (block1.header.difficulty_threshold, block1.header.time),
        (genesis.header.difficulty_threshold, genesis.header.time),
    ];

    assert!(matches!(
        header_is_valid_for_recent_chain(&candidate, block::Height(1), &network, context),
        Err(ValidateContextError::TimeTooLate { .. })
    ));
}

#[test]
fn short_context_early_height_uses_pow_limit_threshold() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("genesis block deserializes");
    let candidate_time =
        genesis.header.time + NetworkUpgrade::target_spacing_for_height(&network, block::Height(1));
    let context = [(genesis.header.difficulty_threshold, genesis.header.time)];

    let expected = difficulty::AdjustedDifficulty::new_from_header_time(
        candidate_time,
        block::Height(0),
        &network,
        context,
    )
    .expected_difficulty_threshold();

    assert_eq!(expected, network.target_difficulty_limit().to_compact());
}

fn daa_context(
    network: &Network,
    previous_block_height: block::Height,
    candidate_time: DateTime<chrono::Utc>,
) -> Vec<(
    zakura_chain::work::difficulty::CompactDifficulty,
    DateTime<chrono::Utc>,
)> {
    let candidate_height = previous_block_height
        .next()
        .expect("test candidate height is valid");
    let target_spacing = NetworkUpgrade::target_spacing_for_height(network, candidate_height);
    let difficulty = network.target_difficulty_limit().to_compact();

    (0..difficulty::POW_ADJUSTMENT_BLOCK_SPAN)
        .map(|offset| {
            let offset = i32::try_from(offset + 1).expect("test offset fits in i32");
            (difficulty, candidate_time - target_spacing * offset)
        })
        .collect()
}
