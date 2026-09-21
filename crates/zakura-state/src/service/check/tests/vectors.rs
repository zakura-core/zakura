//! Fixed test vectors for state contextual validation checks.

use chrono::{DateTime, Duration};

use zakura_chain::{
    block::{merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, CommitmentError},
    history_tree::HistoryTree,
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters},
        Network, NetworkUpgrade, POW_AVERAGING_WINDOW,
    },
    sapling,
    serialization::ZcashDeserializeInto,
    work::difficulty::{CompactDifficulty, ParameterDifficulty},
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
    .expect("the test supplies the complete late-chain difficulty context")
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

/// The difficulty-context reader takes the span for the candidate height, which
/// ZIP 218 widens at NU7.
#[test]
fn difficulty_context_follows_the_candidate_height() {
    let _init_guard = zakura_test::init();

    const NU7: u32 = 1_000;
    let network = Network::new_regtest(
        zakura_chain::parameters::testnet::ConfiguredActivationHeights {
            nu7: Some(NU7),
            ..Default::default()
        }
        .into(),
    );
    let ancestors = 0..difficulty::MAX_POW_ADJUSTMENT_BLOCK_SPAN * 2;

    for (candidate_height, expected) in [
        (NU7 - 1, difficulty::POW_ADJUSTMENT_BLOCK_SPAN),
        (NU7, difficulty::MAX_POW_ADJUSTMENT_BLOCK_SPAN),
        (NU7 + 1, difficulty::MAX_POW_ADJUSTMENT_BLOCK_SPAN),
    ] {
        assert_eq!(
            difficulty_context(&network, block::Height(candidate_height), ancestors.clone()).len(),
            expected,
            "candidate height {candidate_height}",
        );
    }
}

/// Block verification enforces a fixed post-NU7 threshold using the complete
/// difficulty context on a PoW-enabled configured Testnet.
#[test]
fn block_daa_enforces_the_post_nu7_context() {
    let _init_guard = zakura_test::init();

    let network = Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            nu7: Some(347_400),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");
    assert!(!network.disable_pow());

    // The candidate's coinbase height selects the difficulty context.
    let candidate = zakura_test::vectors::BLOCK_MAINNET_347499_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 347499 deserializes");
    let parent_height = block::Height(347_498);
    let candidate_height = parent_height.next().expect("test height is valid");
    assert!(
        difficulty::pow_adjustment_block_span_for_height(&network, candidate_height)
            > difficulty::POW_ADJUSTMENT_BLOCK_SPAN
    );
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in-range");
    let target_bits = [0x1e0ffff0, 0x1e0e0000, 0x1e0c8000, 0x1e0b4000, 0x1e0a2000];
    let time_steps = [19, 31, 23, 29, 17, 37, 21];
    let compact = |bits: u32| {
        CompactDifficulty::from_bytes_in_display_order(&bits.to_be_bytes())
            .expect("the fixed compact target is valid")
    };
    let mut time = candidate_time;
    let context: Vec<_> = (0..difficulty::MAX_POW_ADJUSTMENT_BLOCK_SPAN)
        .map(|index| {
            time -= Duration::seconds(time_steps[index % time_steps.len()]);
            (compact(target_bits[index % target_bits.len()]), time)
        })
        .collect();
    let expected = compact(0x1e0cd7fd);

    let relevant_headers: Vec<block::Header> = context
        .iter()
        .map(|(threshold, time)| {
            let mut header = *candidate.header;
            header.difficulty_threshold = *threshold;
            header.time = *time;
            header
        })
        .collect();
    let mut candidate = (*candidate).clone();
    let mut header = *candidate.header;
    header.time = candidate_time;
    header.difficulty_threshold = expected;
    candidate.header = Arc::new(header);

    block_is_valid_for_recent_chain_data(
        &candidate,
        candidate_height,
        &network,
        Some(block::Height(0)),
        Some(parent_height),
        relevant_headers.clone(),
    )
    .expect("the independently calculated post-NU7 threshold is accepted");

    let mut wrong_window_candidate = candidate.clone();
    let mut header = *wrong_window_candidate.header;
    header.difficulty_threshold = compact(0x1e0af369);
    wrong_window_candidate.header = Arc::new(header);
    assert!(matches!(
        block_is_valid_for_recent_chain_data(
            &wrong_window_candidate,
            candidate_height,
            &network,
            Some(block::Height(0)),
            Some(parent_height),
            relevant_headers,
        ),
        Err(ValidateContextError::InvalidDifficultyThreshold {
            difficulty_threshold,
            expected_difficulty,
        }) if difficulty_threshold == compact(0x1e0af369) && expected_difficulty == expected
    ));
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
    .expect("height one requires exactly one predecessor")
    .expected_difficulty_threshold();

    assert_eq!(expected, network.target_difficulty_limit().to_compact());
}

#[test]
fn full_context_at_averaging_window_height_uses_pow_limit_threshold() {
    let _init_guard = zakura_test::init();

    let network = Network::Mainnet;
    let previous_block_height = block::Height(
        u32::try_from(POW_AVERAGING_WINDOW - 1).expect("averaging window fits in u32"),
    );
    let candidate_height = previous_block_height
        .next()
        .expect("test candidate height is valid");
    let candidate_time = DateTime::from_timestamp(10_000, 0).expect("test timestamp is in-range");
    let target_spacing = NetworkUpgrade::target_spacing_for_height(&network, candidate_height);
    let difficulty = network.target_difficulty_limit().to_compact();
    let context = (0..POW_AVERAGING_WINDOW).map(|offset| {
        let offset = i32::try_from(offset + 1).expect("test offset fits in i32");
        (difficulty, candidate_time - target_spacing * offset)
    });

    let expected = difficulty::AdjustedDifficulty::new_from_header_time(
        candidate_time,
        previous_block_height,
        &network,
        context,
    )
    .expect("the test supplies the complete late-chain difficulty context")
    .expected_difficulty_threshold();

    assert_eq!(expected, difficulty);
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

    // The difficulty context spans the whole chain below the retained span,
    // and the retained span itself above it. Readers take the active span.
    let context_len = usize::try_from(candidate_height.0)
        .expect("test candidate height fits in usize")
        .min(difficulty::MAX_POW_ADJUSTMENT_BLOCK_SPAN);

    (0..context_len)
        .map(|offset| {
            let offset = i32::try_from(offset + 1).expect("test offset fits in i32");
            (difficulty, candidate_time - target_spacing * offset)
        })
        .collect()
}

/// Generate a seed chain long enough that the difficulty adjustment derives a
/// real expectation from it, rather than falling back to the limit.
fn seeded_chain_with_pow_start(
    target_spacing_secs: u32,
) -> zakura_chain::local_genesis::GeneratedLocalTestnet {
    let miner_names: Vec<String> = (0..80).map(|i| format!("miner-{i:03}")).collect();
    let generated = zakura_chain::local_genesis::generate_local_testnet_with_funded_keys(
        miner_names,
        zakura_chain::local_genesis::LocalTestnetGenesisOptions {
            disable_pow: true,
            enforce_pow_after_seeded_tip: true,
            target_spacing_secs,
            maturity_padding_blocks: 8,
            ..Default::default()
        },
    )
    .expect("local testnet should generate");

    assert!(
        generated.blocks.len() > POW_AVERAGING_WINDOW,
        "the seeded chain must be longer than the {POW_AVERAGING_WINDOW}-block averaging \
         window, or the adjustment just returns the limit and proves nothing"
    );

    generated
}

/// The relevant chain for `blocks[index]`: its ancestors, newest first.
fn difficulty_context(
    blocks: &[Block],
    index: usize,
) -> impl Iterator<Item = (CompactDifficulty, DateTime<chrono::Utc>)> + '_ {
    blocks[..index]
        .iter()
        .rev()
        .map(|block| (block.header.difficulty_threshold, block.header.time))
}

/// Seed blocks below `pow_start_height` are exempt from the contextual
/// difficulty-adjustment equality check.
///
/// They have to be. Seed blocks are generated before any network upgrade
/// activates, so the adjustment measures their spacing against the fixed
/// 150-second pre-Blossom target no matter what the network configures. Once the
/// seeded chain is longer than `PoWAveragingWindow` that produces an expectation
/// the seed blocks cannot satisfy, and without the exemption the whole chain is
/// rejected at replay.
#[test]
fn seeded_blocks_below_pow_start_height_skip_the_difficulty_adjustment() {
    let _init_guard = zakura_test::init();

    let generated = seeded_chain_with_pow_start(75);
    let network = &generated.network;
    let pow_start_height = network
        .pow_start_height()
        .expect("enforce_pow_after_seeded_tip sets a start height");

    for (index, candidate) in generated.blocks.iter().enumerate().skip(1) {
        let adjustment = difficulty::AdjustedDifficulty::new_from_block(
            candidate,
            network,
            difficulty_context(&generated.blocks, index),
        )
        .expect("the seeded chain supplies a full difficulty context");

        difficulty_threshold_and_time_are_valid(candidate.header.difficulty_threshold, adjustment)
            .unwrap_or_else(|error| {
                panic!("seeded block at height {index} should be accepted, got {error:?}")
            });
    }

    // The exemption really is bounded: the adjustment does disagree with the
    // seeded blocks, so without it these would be rejected.
    let disagreements = (1..generated.blocks.len())
        .filter(|&index| {
            let expected = difficulty::AdjustedDifficulty::new_from_block(
                &generated.blocks[index],
                network,
                difficulty_context(&generated.blocks, index),
            )
            .expect("the seeded chain supplies a full difficulty context")
            .expected_difficulty_threshold();
            generated.blocks[index].header.difficulty_threshold != expected
        })
        .count();
    assert!(
        disagreements > 0,
        "expected the adjustment to disagree with at least one seeded block, \
         otherwise this test would pass even without the exemption"
    );

    assert_eq!(
        pow_start_height,
        block::Height(
            u32::try_from(generated.blocks.len()).expect("seeded chain is far below Height::MAX")
        ),
        "proof-of-work must start one past the seeded tip"
    );
}

/// The first live-mined block is checked strictly, and the difficulty it has to
/// declare is the network's limit.
///
/// This is the property that removes the difficulty warm-up: the chain starts at
/// whatever `target_difficulty_limit` was calibrated for the fleet instead of
/// climbing to it from a trivial seed difficulty.
#[test]
fn first_block_at_pow_start_height_expects_the_configured_limit() {
    let _init_guard = zakura_test::init();

    let generated = seeded_chain_with_pow_start(75);
    let network = &generated.network;
    let pow_start_height = network
        .pow_start_height()
        .expect("enforce_pow_after_seeded_tip sets a start height");

    assert!(
        !network.should_skip_pow_at_height(pow_start_height),
        "proof-of-work must be enforced at the start height itself"
    );

    let seeded_tip = generated
        .blocks
        .last()
        .expect("the generated chain is not empty");
    let candidate_time = seeded_tip.header.time
        + zakura_chain::parameters::NetworkUpgrade::target_spacing_for_height(network, pow_start_height);

    let expected = difficulty::AdjustedDifficulty::new_from_header_time(
        candidate_time,
        block::Height(pow_start_height.0 - 1),
        network,
        difficulty_context(&generated.blocks, generated.blocks.len()),
    )
    .expect("the seeded chain supplies a full difficulty context")
    .expected_difficulty_threshold();

    let expected_target: U256 = expected
        .to_expanded()
        .expect("an expected difficulty is always representable")
        .into();
    let limit: U256 = network.target_difficulty_limit().into();

    // The expectation is the limit, give or take the truncation in
    // `MeanTarget / AveragingWindowTimespan * DampedTimespan`. What matters is
    // that the first live-mined block already has to do the full configured work
    // rather than climbing to it from a trivially easy seed difficulty, so this
    // pins the target to within a tenth of a percent of the limit.
    assert!(
        expected_target <= limit && expected_target >= limit - limit / U256::from(1_000u64),
        "the first live-mined block should be mined at the configured limit, so the \
         chain starts at its equilibrium difficulty: expected {expected_target:x}, \
         limit {limit:x}"
    );
}
