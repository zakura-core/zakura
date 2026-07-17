//! Randomised property tests for the finalized state.

use std::{collections::HashMap, env, error::Error, fs, sync::Arc};

use tempfile::TempDir;
use tokio::sync::oneshot;

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{
        testnet::{ConfiguredActivationHeights, ParametersBuilder},
        NetworkUpgrade,
    },
    primitives::Groth16Proof,
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto},
    sprout::JoinSplit,
    transaction::{JoinSplitData, LockTime, Transaction, UnminedTx},
    LedgerState,
};
use zakura_test::prelude::*;

use crate::{
    config::Config,
    service::{arbitrary::PreparedChain, check::anchors::tx_anchors_refer_to_final_treestates},
    tests::FakeChainHelper,
    HashOrHeight,
};

use super::super::{
    commitment_aux, serve_block_roots, vct::validate_final_frontiers_bytes,
    CheckpointVerifiedBlock, DiskWriteBatch, FinalizedState, NextVctBlock,
};

const DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES: u32 = 1;

type TestRootMap = HashMap<
    u32,
    (
        zakura_chain::sapling::tree::Root,
        zakura_chain::orchard::tree::Root,
        zakura_chain::ironwood::tree::Root,
    ),
>;
type SaplingTree = Arc<zakura_chain::sapling::tree::NoteCommitmentTree>;
type OrchardTree = Arc<zakura_chain::orchard::tree::NoteCommitmentTree>;
type SproutTree = Arc<zakura_chain::sprout::tree::NoteCommitmentTree>;

fn vct_successor_header(block: Arc<Block>) -> NextVctBlock {
    NextVctBlock::from_header(
        block.header.clone(),
        block
            .coinbase_height()
            .expect("prepared successor blocks have a coinbase height"),
        block.auth_data_root(),
    )
}

fn next_vct_block(block: Arc<Block>) -> Option<NextVctBlock> {
    Some(vct_successor_header(block))
}

#[test]
fn vct_successor_witness_uses_stored_header_without_body() {
    let _init_guard = zakura_test::init();
    let network = zakura_chain::parameters::Network::Mainnet;
    let mut state = FinalizedState::new(
        &Config::ephemeral(),
        &network,
        #[cfg(feature = "elasticsearch")]
        false,
    )
    .expect("opening an ephemeral finalized state succeeds");
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("genesis block deserializes");
    let block1 = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block 1 deserializes");

    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(genesis.clone()).into(),
            None,
            None,
            "header-only VCT successor test genesis",
        )
        .expect("genesis commits");

    let roots = BlockCommitmentRoots {
        height: Height(1),
        sapling_root: zakura_chain::sapling::tree::NoteCommitmentTree::default().root(),
        orchard_root: zakura_chain::orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx: 0,
        orchard_tx: 0,
        ironwood_tx: 0,
        auth_data_root: block1.auth_data_root(),
    };
    let mut batch = DiskWriteBatch::new();
    batch
        .prepare_header_range_batch_with_roots(
            &state.db,
            genesis.hash(),
            std::slice::from_ref(&block1.header),
            &[0],
            &[roots],
        )
        .expect("block 1 header is contextually valid");
    state
        .db
        .write_batch(batch)
        .expect("header range batch writes");

    assert!(
        state.db.block(Height(1).into()).is_none(),
        "the successor body must remain absent"
    );
    let witness = state
        .vct_successor_from_header_store(Height(0), genesis.hash())
        .expect("the stored header and auth-data root form a successor witness");
    assert_eq!(witness.header, block1.header);
    assert_eq!(witness.height, Height(1));
    assert_eq!(witness.hash, block1.hash());
    assert_eq!(witness.auth_data_root, Some(block1.auth_data_root()));
}

/// A handoff frontier over empty trees at `height`, for sources whose test does not
/// exercise the handoff itself. The frontier is mandatory on every source; placing it
/// above every height a test commits keeps all roots fast-path eligible and never
/// engages the handoff behaviors (bounding, treestate write, successor exemption).
fn test_handoff_frontiers(height: Height) -> commitment_aux::FinalFrontiers {
    commitment_aux::FinalFrontiers {
        height,
        sapling: Arc::new(Default::default()),
        orchard: Arc::new(Default::default()),
        sprout: Arc::new(Default::default()),
        ironwood: Arc::new(Default::default()),
    }
}

fn enable_vct_test_fixture_source(state: &mut FinalizedState, roots: TestRootMap) {
    state.enable_vct_fast_source(
        Box::new(commitment_aux::FixtureSource::new(
            roots,
            test_handoff_frontiers(Height::MAX),
        )),
        false,
    );
}

fn enable_vct_test_fixture_source_with_handoff(
    state: &mut FinalizedState,
    roots: TestRootMap,
    handoff_height: Height,
    sapling: SaplingTree,
    orchard: OrchardTree,
    sprout: SproutTree,
    ironwood: Arc<zakura_chain::ironwood::tree::NoteCommitmentTree>,
) {
    state.enable_vct_fast_source(
        Box::new(commitment_aux::FixtureSource::new(
            roots,
            commitment_aux::FinalFrontiers {
                height: handoff_height,
                sapling,
                orchard,
                sprout,
                ironwood,
            },
        )),
        false,
    );
}

/// Builds a structurally valid V4 transaction with two Groth16 JoinSplits from the first
/// historical Sprout JoinSplit fixture. Its later JoinSplit references the first one's
/// interstitial output tree.
///
/// The contextual anchor check does not verify proofs, so the original BCTV14 proof is replaced
/// with a correctly sized placeholder Groth16 proof. Proof verification belongs to semantic
/// verification and is deliberately outside this state-anchor regression.
fn v4_transaction_with_interstitial_anchor(old_anchor_tree: &SproutTree) -> Arc<Transaction> {
    let source = zakura_test::vectors::BLOCK_MAINNET_396_BYTES
        .zcash_deserialize_into::<Block>()
        .expect("the first mainnet Sprout block deserializes");
    let source_joinsplit = source
        .transactions
        .iter()
        .find_map(|transaction| match &**transaction {
            Transaction::V2 {
                joinsplit_data: Some(data),
                ..
            } => data.joinsplits().next(),
            _ => None,
        })
        .expect("the first mainnet Sprout block has a JoinSplit");

    let to_groth16 = |anchor| JoinSplit {
        vpub_old: Amount::zero(),
        vpub_new: Amount::zero(),
        anchor,
        nullifiers: source_joinsplit.nullifiers,
        commitments: source_joinsplit.commitments,
        ephemeral_key: source_joinsplit.ephemeral_key,
        random_seed: source_joinsplit.random_seed.clone(),
        vmacs: source_joinsplit.vmacs.clone(),
        zkproof: Groth16Proof::from([0; 192]),
        enc_ciphertexts: source_joinsplit.enc_ciphertexts,
    };

    let first = to_groth16(old_anchor_tree.root());
    let mut interstitial_tree = (**old_anchor_tree).clone();
    for commitment in first.commitments {
        interstitial_tree
            .append(commitment)
            .expect("two historical JoinSplit commitments fit in the Sprout tree");
    }
    let second = to_groth16(interstitial_tree.root());

    Arc::new(Transaction::V4 {
        inputs: Vec::new(),
        outputs: Vec::new(),
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: Height(0),
        joinsplit_data: Some(JoinSplitData {
            first,
            rest: vec![second],
            pub_key: source
                .transactions
                .iter()
                .find_map(|transaction| match &**transaction {
                    Transaction::V2 {
                        joinsplit_data: Some(data),
                        ..
                    } => Some(data.pub_key),
                    _ => None,
                })
                .expect("the source JoinSplit has a public key"),
            sig: source
                .transactions
                .iter()
                .find_map(|transaction| match &**transaction {
                    Transaction::V2 {
                        joinsplit_data: Some(data),
                        ..
                    } => Some(data.sig),
                    _ => None,
                })
                .expect("the source JoinSplit has a signature"),
        }),
        sapling_shielded_data: None,
    })
}

#[test]
fn vct_generated_final_frontier_bytes_are_node_loader_compatible() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {
            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let height = Height(last as u32);

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct frontier bytes legacy")
                    .unwrap();
            }

            let bytes = commitment_aux::produce_final_frontiers_bytes(&legacy.db, height)
                .expect("legacy DB has final frontiers at the requested height");
            let temp_dir = TempDir::new().expect("temp dir");
            let path = temp_dir.path().join("frontier.bin");
            fs::write(&path, &bytes).expect("frontier bytes write to temp file");

            let bytes_from_file = fs::read(&path).expect("frontier bytes read from temp file");
            validate_final_frontiers_bytes(&bytes_from_file, height)
                .expect("generated frontier bytes pass node loader validation");

            let parsed = commitment_aux::FinalFrontiers::from_bytes(&bytes_from_file)
                .expect("validated bytes parse as final frontiers");
            prop_assert_eq!(parsed.height, height, "frontier height round-trips");
            prop_assert_eq!(
                parsed.sapling.root(),
                legacy.db.sapling_tree_by_height(&height).unwrap().root(),
                "parsed Sapling frontier matches the DB tree at the requested height"
            );
            prop_assert_eq!(
                parsed.orchard.root(),
                legacy.db.orchard_tree_by_height(&height).unwrap().root(),
                "parsed Orchard frontier matches the DB tree at the requested height"
            );
            prop_assert_eq!(
                parsed.sprout.root(),
                legacy.db.sprout_tree_for_tip().unwrap().root(),
                "parsed Sprout frontier matches the DB tip tree"
            );

            let wrong_height = Height(height.0.checked_add(1).expect("test height is in range"));
            prop_assert!(
                validate_final_frontiers_bytes(&bytes_from_file, wrong_height).is_err(),
                "node loader validation rejects a frontier whose height does not match the checkpoint"
            );
    });

    Ok(())
}

#[test]
fn blocks_with_v5_transactions() -> Result<()> {
    let _init_guard = zakura_test::init();
    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, count, network, _history_tree) in PreparedChain::default())| {
            let mut state = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut height = Height(0);
            // use `count` to minimize test failures, so they are easier to diagnose
            for block in chain.iter().take(count) {
                let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
                let (hash, _) = state.commit_finalized_direct(
                    checkpoint_verified.into(),
                    None,
                    None,
                    "blocks_with_v5_transactions test"
                ).unwrap();
                prop_assert_eq!(Some(height), state.finalized_tip_height());
                prop_assert_eq!(hash, block.hash);
                height = Height(height.0 + 1);
            }
    });

    Ok(())
}

/// Test if committing blocks from all upgrades work correctly, to make
/// sure the contextual validation done by the finalized state works.
/// Also test if a block with the wrong commitment is correctly rejected.
#[test]
#[allow(clippy::print_stderr)]
fn all_upgrades_and_wrong_commitments_with_fake_activation_heights() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            // These are dummy values. The particular values don't matter much,
            // as long as the nu5 one is smaller than the chains being generated
            // (MAX_PARTIAL_CHAIN_BLOCKS) to make sure that upgrade is exercised
            // in the test below. (The test will fail if that does not happen.)
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, None, false);

    // Use no_shrink() because we're ignoring _count and there is nothing to actually shrink.
    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy).with_valid_commitments().no_shrink())| {

            let mut state = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut height = Height(0);
            let heartwood_height = NetworkUpgrade::Heartwood.activation_height(&network).unwrap();
            let heartwood_height_plus1 = (heartwood_height + 1).unwrap();
            let nu5_height = NetworkUpgrade::Nu5.activation_height(&network).unwrap();
            let nu5_height_plus1 = (nu5_height + 1).unwrap();

            let mut failure_count = 0;
            let mut bad_auth_root_failure_count = 0;
            for block in chain.iter() {
                let block_hash = block.hash;
                let current_height = block.block.coinbase_height().unwrap();
                // For some specific heights, try to commit a block with
                // corrupted commitment.
                match current_height {
                    h if h == heartwood_height ||
                        h == heartwood_height_plus1 ||
                        h == nu5_height ||
                        h == nu5_height_plus1 => {
                            let block = block.block.clone().set_block_commitment([0x42; 32]);
                            let checkpoint_verified = CheckpointVerifiedBlock::from(block);
                            state.commit_finalized_direct(
                                checkpoint_verified.into(),
                                None,
                                None,
                                "all_upgrades test"
                            ).expect_err("Must fail commitment check");
                            failure_count += 1;
                        },
                    _ => {},
                }
                if current_height == nu5_height_plus1 {
                    let mut checkpoint_verified =
                        CheckpointVerifiedBlock::from(block.block.clone());
                    checkpoint_verified.0.auth_data_root = Some([0x42; 32].into());
                    let err = state.commit_finalized_direct(
                        checkpoint_verified.into(),
                        None,
                        None,
                        "all_upgrades bad auth root test"
                    ).expect_err("Must fail when the supplied auth data root is incorrect");
                    let commit_error = err
                        .source()
                        .and_then(|source| source.downcast_ref::<crate::error::CommitBlockError>())
                        .expect("checkpoint commit error wraps a commit block error");
                    // The committer trusts the precomputed root without re-deriving it
                    // from the body, so a bad value fails the ZIP-244 header commitment
                    // check (the header committed to the real root) rather than a
                    // dedicated auth-data-root comparison.
                    let bad_auth_root_is_rejected = matches!(
                        commit_error,
                        crate::error::CommitBlockError::ValidateContextError(source)
                            if matches!(
                                source.as_ref(),
                                crate::ValidateContextError::InvalidBlockCommitment(
                                    zakura_chain::block::CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. }
                                )
                            )
                    );
                    prop_assert!(bad_auth_root_is_rejected);
                    bad_auth_root_failure_count += 1;
                }
                let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
                let (hash, _) = state.commit_finalized_direct(
                    checkpoint_verified.into(),
                    None,
                    None,
                    "all_upgrades test"
                ).unwrap();
                prop_assert_eq!(Some(height), state.finalized_tip_height());
                prop_assert_eq!(hash, block_hash);
                height = Height(height.0 + 1);
            }
            // Make sure the failure path was triggered
            prop_assert_eq!(failure_count, 4);
            prop_assert_eq!(bad_auth_root_failure_count, 1);
    });

    Ok(())
}

/// Verified-commitment-trees fast path (`commit_finalized_direct` Checkpoint arm):
/// committing with correct fixture roots produces the same consensus state (anchor
/// sets + history root) as the legacy recompute path across all upgrade boundaries,
/// and a wrong fixture root is rejected (verify-before-commit) rather than persisted.
/// Exercises: a below-Heartwood seed, history-tree creation at Heartwood, the NU5
/// V1->V2 transition, verify-ahead against the successor header, trusted fixture tip
/// commits without a successor, and rejection of a corrupted root.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] and the fixture by height
fn vct_fast_path_matches_legacy_and_rejects_wrong_roots() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;

            // Process a bounded prefix [0, last] spanning the Heartwood (history-tree
            // creation) and NU5 (V1->V2) boundaries plus a couple of V2 blocks; `last` is
            // the tip we compare at. Chains are far longer than this
            // (MAX_PARTIAL_CHAIN_BLOCKS), so this is a plain assertion, not a discard.
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");

            // The fast path runs below the checkpoint, seeded from an already-committed
            // tip. Seed just before Heartwood so the fast range creates the history tree
            // (Heartwood) and crosses NU5 (V1->V2).
            let seed = (heartwood - 1) as usize;

            // Legacy pass over [0, last]: record per-block roots for the fast range as
            // the fixture, and the golden consensus state at the tip.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Fast pass over [0, last] with the correct fixture: genesis..=seed recompute
            // (no fixture entry); seed+1..=last verify-ahead against their buffered
            // successor. Every fast-eligible block takes the fast path, and the result
            // equals legacy.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture.clone());
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct fast")
                    .expect("verified fast commit succeeds");
            }
            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors must match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history must match legacy");
            prop_assert_eq!(fast.vct_fast_count(), (last - seed) as u64, "every fast-eligible block took the fast path");
            // The dedup: each header commitment is checked once, not twice. Only the
            // first fast block runs its own commitment check; every later fast block
            // was already validated by its predecessor's look-ahead, so it is skipped.
            prop_assert_eq!(fast.vct_prevalidated_count(), (last - seed - 1) as u64, "every fast block after the first skips its redundant own commitment check");

            // A trusted local fixture may commit its tip root without a successor: it is
            // not adversarial and the root is checked in arrears when a successor arrives.
            let mut no_successor = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut no_successor, fixture.clone());
            for i in 0..last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                no_successor
                    .commit_finalized_direct(cv.into(), None, next, "vct no-successor seed")
                    .expect("verified fast commit succeeds with successor");
            }
            prop_assert!(!no_successor.vct_fast_needs_successor(Height(last as u32)), "a trusted fixture tip can commit without a successor");
            let cv = CheckpointVerifiedBlock::from(blocks[last].block.clone());
            no_successor
                .commit_finalized_direct(cv.into(), None, None, "vct trusted fixture no successor")
                .expect("trusted fixture tip commits without a successor");
            prop_assert_eq!(
                no_successor.db.finalized_tip_height(),
                Some(Height(last as u32)),
                "the trusted fixture tip committed"
            );

            // Negative: corrupt the fixture Sapling root at a V2 (post-NU5) height with a
            // distinct value (the empty root; a V2 block has a non-empty Sapling tree).
            // Fast mode cannot recompute a bad root away (the frontier is frozen), so the
            // wrong root must be *rejected* by the next block's commitment (verify-before-
            // commit) — the commit at that height fails rather than persisting it.
            let bad_height = (nu5 + 1) as usize;
            let mut bad_fixture = fixture.clone();
            let bad_entry = bad_fixture.get_mut(&(bad_height as u32)).unwrap();
            prop_assert_ne!(bad_entry.0, Default::default(), "a V2 block must have a non-empty Sapling root");
            bad_entry.0 = Default::default();

            let mut bad = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut bad, bad_fixture);
            let mut error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                if bad.commit_finalized_direct(cv.into(), None, next, "vct bad").is_err() {
                    error_height = Some(i);
                    break;
                }
            }
            prop_assert_eq!(error_height, Some(bad_height), "a wrong fixture root is rejected at its own commit");

            // Negative (Orchard, below NU5): no header commits to an Orchard root below
            // NU5 (V1 history leaves ignore it; no MMR below Heartwood), so the fast path
            // pins it to the empty-tree root. Corrupt a below-NU5 fixture Orchard root to
            // a non-empty value. Unlike the Sapling MMR path (one-block lag), this is a
            // direct check, so it is rejected at the block's *own* commit — closing the
            // hole where an untrusted source injects a spurious Orchard anchor.
            let bad_orchard_height = (nu5 - 1) as usize;
            prop_assert!(bad_orchard_height > seed, "the corrupted height must be in the fast range");
            let empty_orchard = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
            let wrong_orchard = zakura_chain::orchard::tree::Root::try_from([0u8; 32])
                .expect("zero is a valid pallas base field element");
            prop_assert_ne!(wrong_orchard, empty_orchard, "the wrong root must differ from the empty-tree root");

            let mut bad_orchard_fixture = fixture.clone();
            let bad_orchard_entry = bad_orchard_fixture.get_mut(&(bad_orchard_height as u32)).unwrap();
            prop_assert_eq!(bad_orchard_entry.1, empty_orchard, "a below-NU5 block has the empty Orchard root");
            bad_orchard_entry.1 = wrong_orchard;

            let mut bad_orchard = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut bad_orchard, bad_orchard_fixture);
            let mut orchard_error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                if bad_orchard.commit_finalized_direct(cv.into(), None, next, "vct bad orchard").is_err() {
                    orchard_error_height = Some(i);
                    break;
                }
            }
            prop_assert_eq!(orchard_error_height, Some(bad_orchard_height), "a wrong below-NU5 orchard root is rejected at its own commit");
    });

    Ok(())
}

/// A verified-commitment-trees fast sync must never legacy-recompute a height whose
/// supplied root is missing once the note-commitment frontier is frozen: the running
/// frontier is no longer the real one, so recomputing would fold a wrong root into the
/// history MMR and silently corrupt consensus state (a peer that omits a height — see the
/// driver's gap handling — could trigger this). Instead the committer must refuse with the
/// retryable `VctSuppliedRootUnavailable` error and leave the database untouched, so the
/// block can be committed later from a fetched root. This guards the liveness/no-corruption
/// half of the peer-source fast path (the bad-root rejection half is covered by
/// `vct_fast_path_matches_legacy_and_rejects_wrong_roots`).
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and the fixture by height
fn vct_frozen_frontier_hole_refuses_instead_of_recomputing() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Record the per-block roots for the fast range as the fixture.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct hole legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
            }

            // Punch a hole: drop a post-NU5 height's root from the fixture, simulating a
            // peer that omitted it (or a root evicted after failing verification). Earlier
            // fast blocks freeze the frontier, so this height has no real frontier to
            // recompute against.
            let hole = (nu5 + 1) as usize;
            prop_assert!(hole > seed && hole < last, "the hole must be inside the fast range");
            fixture.remove(&(hole as u32));

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture);

            let mut error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                match fast.commit_finalized_direct(cv.into(), None, next, "vct hole fast") {
                    Ok(_) => {}
                    Err(error) => {
                        // The refusal is the typed, retryable error — not a generic
                        // invalid-block error and not silent corruption.
                        prop_assert!(
                            format!("{error:?}").contains("VctSuppliedRootUnavailable"),
                            "a frozen-frontier hole returns the retryable VctSuppliedRootUnavailable error, got: {error:?}"
                        );
                        error_height = Some(i);
                        break;
                    }
                }
            }

            prop_assert_eq!(error_height, Some(hole), "the commit refuses at the hole height, not before or after");
            // Nothing at or past the hole was persisted: the tip is the last block before
            // the hole, so no corrupt MMR leaf was written.
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((hole - 1) as u32)),
                "the database tip stays just below the hole — the refused block left state untouched"
            );
    });

    Ok(())
}

/// Retryable VCT root misses must stay internal to the finalized write loop: the
/// public checkpoint commit wrapper returns the queued block and error to the caller
/// that can retry, rather than completing the block's response channel with a
/// transient error.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and the fixture by height
fn vct_retryable_root_miss_keeps_checkpoint_response_pending() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct response legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
            }

            let hole = (nu5 + 1) as usize;
            fixture.remove(&(hole as u32));

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture);

            for i in 0..hole {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct response fast")
                    .expect("pre-hole fast commits succeed");
            }

            let cv = CheckpointVerifiedBlock::from(blocks[hole].block.clone());
            let (rsp_tx, mut rsp_rx) = oneshot::channel();
            let next = next_vct_block(blocks[hole + 1].block.clone());
            let result = fast.commit_finalized((cv, rsp_tx), None, next);
            let Err((returned_block, error)) = result else {
                panic!("missing frozen-frontier root should return the queued block for retry");
            };

            prop_assert_eq!(returned_block.0.height, Height(hole as u32));
            prop_assert!(
                error.vct_supplied_root_unavailable_height().is_some(),
                "the returned error is the typed retryable VCT root miss"
            );
            prop_assert!(
                matches!(rsp_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "the checkpoint response stays pending so the write loop can retry internally"
            );
    });

    Ok(())
}

/// An *untrusted* (peer) source must never commit a fast block whose own supplied root has
/// no successor header to confirm it against the header chain. A block's roots are only
/// committed by the next block's header (the one-block lag), so committing at the sync tip
/// would persist a root checked only one block later — irreversibly, once on disk. A wrong
/// tip root would then wedge the sync with no recovery (the failure surfaces at the next
/// block and is mis-attributed to *its* root). So the committer defers: it refuses the tip
/// block with the retryable `VctSuppliedRootAwaitingSuccessor`, leaves the database
/// untouched, and commits the same height once a successor is buffered. A trusted local
/// fixture is exempt (covered by `vct_fast_path_matches_legacy_and_rejects_wrong_roots`,
/// whose tip commits on the in-arrears check); this guards the peer path specifically.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and inserts roots by height
fn vct_peer_source_defers_unverifiable_tip_root_until_successor() -> Result<()> {
    use crate::service::finalized_state::commitment_aux::PeerSource;
    use zakura_chain::parallel::commitment_aux::BlockCommitmentRoots;

    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            // Use a post-Heartwood, pre-NU5 target so its root needs a successor, while a
            // deterministic V4 JoinSplit transaction can exercise the Sprout retry path.
            let tip_target = (heartwood + 1) as usize;
            prop_assert!(blocks.len() > tip_target + 1, "generated chain unexpectedly short");
            prop_assert!((tip_target as u32) < nu5, "the retry target must permit V4 transactions");
            let seed = (heartwood - 1) as usize;

            // The checkpoint commit path intentionally assumes semantic verification already
            // succeeded, so this fixture can append a structurally valid JoinSplit transaction
            // without rebuilding the block's transaction Merkle root.
            let mut target_block = blocks[tip_target].block.clone();
            let empty_sprout_tree = SproutTree::default();
            Arc::make_mut(&mut target_block)
                .transactions
                .push(v4_transaction_with_interstitial_anchor(&empty_sprout_tree));
            let target_sprout_commitment_count: u64 = target_block
                .sprout_note_commitments()
                .count()
                .try_into()
                .expect("the fixture commitment count fits in u64");
            prop_assert!(
                target_sprout_commitment_count > 0,
                "the deferred block must exercise the Sprout update path"
            );

            // Legacy golden pass to source the correct per-block roots for the fast range.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut peer_roots = Vec::new();
            for i in 0..=tip_target {
                let block = if i == tip_target {
                    target_block.clone()
                } else {
                    blocks[i].block.clone()
                };
                let cv = CheckpointVerifiedBlock::from(block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct defer legacy")
                    .unwrap();
                if i > seed {
                    peer_roots.push(BlockCommitmentRoots {
                        height: Height(i as u32),
                        sapling_root: trees.sapling.root(),
                        orchard_root: trees.orchard.root(),
                        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        sapling_tx: 0,
                        orchard_tx: 0,
                        ironwood_tx: 0,
                        auth_data_root: block.auth_data_root(),
                    });
                }
            }
            let legacy_sprout_tree = legacy.db.sprout_tree_for_tip().unwrap();

            // The modified target changes its history-tree leaf. Before NU5 the successor
            // header commits directly to that resulting history root, so update the witness
            // fixture while preserving its link to the target's unchanged header hash.
            let mut target_successor = blocks[tip_target + 1].block.clone();
            let target_history_root = legacy
                .db
                .history_tree()
                .hash()
                .expect("the post-Heartwood history tree has a root");
            Arc::make_mut(&mut Arc::make_mut(&mut target_successor).header).commitment_bytes =
                target_history_root.bytes_in_serialized_order().into();

            // An untrusted peer source pre-filled with the *correct* roots: the deferral is
            // about the missing successor, not a bad root. The roots are persisted into the
            // fast state's own database through the same header-sync write path production
            // uses, and the peer source reads them back from that database.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            fast.db
                .insert_zakura_header_commitment_roots(peer_roots)
                .expect("writing header-sync roots to an ephemeral database succeeds");
            let source = PeerSource::new(fast.db.clone(), test_handoff_frontiers(Height::MAX));
            fast.enable_vct_fast_source(Box::new(source), true);

            // Commit up to (but not including) the tip target, each with its successor.
            for i in 0..tip_target {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct defer pre-tip")
                    .expect("pre-tip fast commits succeed");
            }
            prop_assert_eq!(fast.db.finalized_tip_height(), Some(Height((tip_target - 1) as u32)));
            let sprout_tree_before_retries = fast.db.sprout_tree_for_tip().unwrap();
            let sprout_root_before_retries = sprout_tree_before_retries.root();
            let sprout_count_before_retries = sprout_tree_before_retries.count();

            // The tip target with no successor header must defer, not commit: its own
            // (correct) root is not yet confirmed, and the peer source is untrusted.
            prop_assert!(
                fast.vct_fast_needs_successor(Height(tip_target as u32)),
                "an untrusted peer tip root needs successor verification"
            );
            let pre_deferral_prevalidated = fast.vct_prevalidated_count();
            let cv = CheckpointVerifiedBlock::from(target_block.clone());
            let error = fast
                .commit_finalized_direct(cv.into(), None, None, "vct defer tip no successor")
                .expect_err("an untrusted tip root with no successor must defer, not commit");
            prop_assert!(
                error.vct_supplied_root_unavailable_height().is_none(),
                "deferral is not a missing-root case (the root is present): {error:?}"
            );
            prop_assert!(
                format!("{error:?}").contains("VctSuppliedRootAwaitingSuccessor"),
                "the tip defers with the await-successor error, got: {error:?}"
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((tip_target - 1) as u32)),
                "the deferred block left the database untouched"
            );
            prop_assert_eq!(
                fast.db.sprout_tree_for_tip().unwrap().root(),
                sprout_root_before_retries,
                "the deferred JoinSplit block leaves the persisted Sprout root unchanged"
            );
            prop_assert_eq!(
                fast.db.sprout_tree_for_tip().unwrap().count(),
                sprout_count_before_retries,
                "the deferred JoinSplit block leaves the persisted Sprout count unchanged"
            );
            let after_deferral_prevalidated = fast.vct_prevalidated_count();
            prop_assert_eq!(
                after_deferral_prevalidated,
                pre_deferral_prevalidated + 1,
                "the deferred attempt uses the predecessor look-ahead"
            );

            // Defense in depth: a witness that does not link to the block being committed
            // (here, the block itself — its parent is the previous height) must be ignored
            // and deferred exactly like a missing successor. It must *not* be treated as a
            // verification failure: that would evict the correct root and, because the write
            // loop's parked retry is taken before the look-ahead, wedge the retry loop.
            let cv = CheckpointVerifiedBlock::from(target_block.clone());
            let forged_witness = next_vct_block(target_block.clone());
            let error = fast
                .commit_finalized_direct(cv.into(), None, forged_witness, "vct defer tip forged witness")
                .expect_err("a non-linking witness must defer, not commit or evict");
            prop_assert!(
                format!("{error:?}").contains("VctSuppliedRootAwaitingSuccessor"),
                "a non-linking witness defers with the await-successor error, got: {error:?}"
            );
            prop_assert!(
                error.vct_supplied_root_unavailable_height().is_none(),
                "a non-linking witness is not a root failure — the correct root stays cached: {error:?}"
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((tip_target - 1) as u32)),
                "the forged-witness attempt left the database untouched"
            );
            prop_assert_eq!(
                fast.db.sprout_tree_for_tip().unwrap().root(),
                sprout_root_before_retries,
                "the forged-witness retry leaves the persisted Sprout root unchanged"
            );
            prop_assert_eq!(
                fast.db.sprout_tree_for_tip().unwrap().count(),
                sprout_count_before_retries,
                "the forged-witness retry leaves the persisted Sprout count unchanged"
            );
            let after_forged_prevalidated = fast.vct_prevalidated_count();
            prop_assert_eq!(
                after_forged_prevalidated,
                after_deferral_prevalidated + 1,
                "the forged-witness attempt still uses the predecessor look-ahead"
            );

            // Once a successor is buffered, the very same height commits and the tip advances:
            // the deferral was a wait, not a permanent stall — and the root survived the
            // forged-witness attempt (it was never evicted).
            let cv = CheckpointVerifiedBlock::from(target_block);
            let next = next_vct_block(target_successor);
            fast.commit_finalized_direct(cv.into(), None, next, "vct defer tip with successor")
                .expect("the deferred height commits once its successor is buffered");
            prop_assert_eq!(
                fast.vct_prevalidated_count(),
                after_forged_prevalidated + 1,
                "the retry reuses the preserved predecessor look-ahead"
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height(tip_target as u32)),
                "the tip advances once the successor confirms the root"
            );
            let fast_sprout_tree = fast.db.sprout_tree_for_tip().unwrap();
            prop_assert_eq!(
                fast_sprout_tree.count(),
                sprout_count_before_retries + target_sprout_commitment_count,
                "the successful retry appends each target Sprout commitment exactly once"
            );
            prop_assert_eq!(
                fast_sprout_tree.root(),
                legacy_sprout_tree.root(),
                "the retried fast commit produces the same Sprout root as legacy commit"
            );
    });

    Ok(())
}

/// A wrong peer-supplied root must be recoverable at the same height: the committer rejects and
/// evicts the bad cached value, leaves the database parked below the height, then commits the
/// same block once the `tree_aux` driver refills that height with a verifiable root.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and inserts roots by height
fn vct_peer_source_bad_root_refill_commits_same_height() -> Result<()> {
    use crate::service::finalized_state::commitment_aux::PeerSource;
    use zakura_chain::parallel::commitment_aux::BlockCommitmentRoots;

    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let target = (nu5 + 1) as usize;
            prop_assert!(blocks.len() > target + 1, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Source the true roots from a legacy pass, then poison the target height exactly
            // as a malicious peer would. Earlier roots are correct so the frontier freezes
            // before the bad root is encountered.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut peer_roots = Vec::new();
            let mut correct_target_root = None;
            for i in 0..=target {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct refill legacy")
                    .unwrap();
                if i > seed {
                    let root = BlockCommitmentRoots {
                        height: Height(i as u32),
                        sapling_root: trees.sapling.root(),
                        orchard_root: trees.orchard.root(),
                        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        sapling_tx: 0,
                        orchard_tx: 0,
                        ironwood_tx: 0,
                        auth_data_root: blocks[i].block.auth_data_root(),
                    };
                    if i == target {
                        correct_target_root = Some(root.clone());
                        let mut poisoned = root;
                        prop_assert_ne!(
                            poisoned.sapling_root,
                            Default::default(),
                            "a V2 target block must have a non-empty Sapling root"
                        );
                        poisoned.sapling_root = Default::default();
                        peer_roots.push(poisoned);
                    } else {
                        peer_roots.push(root);
                    }
                }
            }
            let correct_target_root = correct_target_root.expect("target root was produced");

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            fast.db
                .insert_zakura_header_commitment_roots(peer_roots)
                .expect("writing header-sync roots to an ephemeral database succeeds");
            let source = PeerSource::new(fast.db.clone(), test_handoff_frontiers(Height::MAX));
            fast.enable_vct_fast_source(Box::new(source), true);

            for i in 0..target {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct refill pre-target")
                    .expect("pre-target fast commits succeed");
            }
            prop_assert_eq!(fast.db.finalized_tip_height(), Some(Height((target - 1) as u32)));

            let cv = CheckpointVerifiedBlock::from(blocks[target].block.clone());
            let next = next_vct_block(blocks[target + 1].block.clone());
            let error = fast
                .commit_finalized_direct(cv.into(), None, next.clone(), "vct poisoned target")
                .expect_err("the poisoned peer root must be rejected before commit");
            prop_assert_eq!(
                error.vct_supplied_root_unavailable_height(),
                Some(Height(target as u32)),
                "the bad root is exposed as a retryable missing root for its own height"
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((target - 1) as u32)),
                "the rejected root left the database parked below the target"
            );

            // Simulate the `tree_aux` driver refilling the evicted height from another peer:
            // header sync persists the replacement through the same database write path.
            fast.db
                .insert_zakura_header_commitment_roots([correct_target_root])
                .expect("refilling the evicted height succeeds");

            let cv = CheckpointVerifiedBlock::from(blocks[target].block.clone());
            fast.commit_finalized_direct(cv.into(), None, next, "vct refilled target")
                .expect("the same height commits once the peer cache is refilled");
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height(target as u32)),
                "the refilled root unblocks the parked height"
            );
    });

    Ok(())
}

/// The frozen-frontier guard must survive a restart. A fast sync interrupted before the
/// checkpoint handoff leaves the stale frozen frontier persisted (fast commits never write
/// per-height trees) with the tip still below the handoff, but the in-memory `frozen` flag
/// is rebuilt from scratch on open. If it came back `false`, the first post-restart height
/// with no supplied root would legacy-recompute against the stale on-disk frontier and
/// corrupt the history MMR — the exact hazard the in-session guard prevents
/// (`vct_frozen_frontier_hole_refuses_instead_of_recomputing`). So `FinalizedState::new`
/// re-derives the flag from the durable fast-sync marker. This reopens the database between
/// freezing and the hole, and asserts that the very first commit of the new session (no
/// prior fast block to re-arm the flag in-session) still refuses with the retryable
/// `VctSuppliedRootUnavailable`, leaves state untouched, and commits once the root arrives.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and the fixture by height
fn vct_frozen_frontier_survives_reopen() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let handoff_height = nu5 + 3;
            let last = handoff_height as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Stop the fast sync two blocks below the handoff, so the tip is inside the
            // frozen region and there is room for the hole at `stop + 1` (still below the
            // handoff, where the real frontier would have been written).
            let stop = (handoff_height - 2) as usize;
            let hole = stop + 1;
            prop_assert!(seed < stop && hole < last, "the hole must sit inside the frozen fast range");

            // Legacy golden pass over [0, last]: the per-block fixture for the fast range
            // and the real final frontiers at the handoff (needed to configure fast mode).
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            let mut handoff_trees = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct reopen legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
                if i == last {
                    handoff_trees = Some(trees);
                }
            }
            let handoff_trees = handoff_trees.expect("committed the handoff block");

            // A persistent database so the syncing handle can be dropped and reopened by
            // path, modelling a node restart. Archive storage mode (the default): fast sync
            // is the default under checkpoint sync, and a fast-synced database reopens fine
            // in archive mode, exactly as in production.
            let dir = TempDir::new().expect("temp dir");
            let config = Config {
                cache_dir: dir.path().to_path_buf(),
                ephemeral: false,
                ..Config::default()
            };

            // Session 1: a genesis-start fast sync interrupted at `stop`, two blocks below
            // the handoff. The fast commits write the fast-sync marker but no per-height
            // trees, so the on-disk frontier is frozen and the tip is below the handoff.
            {
                let mut fast = FinalizedState::new(&config, &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
                enable_vct_test_fixture_source_with_handoff(
                    &mut fast,
                    fixture.clone(),
                    Height(handoff_height),
                    handoff_trees.sapling.clone(),
                    handoff_trees.orchard.clone(),
                    handoff_trees.sprout.clone(),
                    handoff_trees.ironwood.clone(),
                );
                for i in 0..=stop {
                    let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                    let next = next_vct_block(blocks[i + 1].block.clone());
                    fast.commit_finalized_direct(cv.into(), None, next, "vct reopen fast")
                        .expect("verified fast commit succeeds");
                }
                prop_assert_eq!(fast.vct_fast_synced_below(), Some(Height(handoff_height)), "the interrupted sync left the fast-sync marker");
                prop_assert_eq!(fast.db.finalized_tip_height(), Some(Height(stop as u32)), "the tip is parked below the handoff");
                // Drop releases the database lock for the reopen below.
            }

            // Session 2 (restart): reopen the same database, then punch a hole at the next
            // height (a peer that omitted it, or a root evicted after failing verification).
            // Skip the constructor-time interrupted-fast-sync resume guard: this configured
            // network has no embedded frontiers, so `from_config` yields no source, but the
            // test attaches a fixture source below the way a real (Mainnet) node's configured
            // source is already present at open time.
            let mut reopened = FinalizedState::new_with_debug_and_storage_validation(
                &config,
                &network,
                false,
                #[cfg(feature = "elasticsearch")]
                false,
                false,
                true,
                false,
            ).expect("opening the finalized state should succeed");
            prop_assert_eq!(reopened.vct_fast_synced_below(), Some(Height(handoff_height)), "the marker is still durable after reopen");

            let mut holed = fixture.clone();
            holed.remove(&(hole as u32));
            enable_vct_test_fixture_source_with_handoff(
                &mut reopened,
                holed,
                Height(handoff_height),
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );

            // The very first commit of the new session is the hole. No fast block has run
            // since the reopen, so the only thing that can arm the guard is the flag seeded
            // from the durable marker. Before the fix it came back `false` and this would
            // legacy-recompute against the stale frontier; now it refuses.
            let cv = CheckpointVerifiedBlock::from(blocks[hole].block.clone());
            let next = next_vct_block(blocks[hole + 1].block.clone());
            let error = reopened
                .commit_finalized_direct(cv.into(), None, next, "vct reopen hole")
                .expect_err("a frozen-frontier hole must refuse after reopen, not recompute");
            prop_assert!(
                format!("{error:?}").contains("VctSuppliedRootUnavailable"),
                "the reopened committer returns the retryable VctSuppliedRootUnavailable, got: {error:?}"
            );
            prop_assert_eq!(reopened.db.finalized_tip_height(), Some(Height(stop as u32)), "the refused block left the reopened state untouched");

            // Retryable: once a verifiable root for the hole is supplied, the same height
            // commits and the tip advances — the refusal was a stall, not a permanent wedge.
            enable_vct_test_fixture_source_with_handoff(
                &mut reopened,
                fixture.clone(),
                Height(handoff_height),
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );
            let cv = CheckpointVerifiedBlock::from(blocks[hole].block.clone());
            let next = next_vct_block(blocks[hole + 1].block.clone());
            reopened
                .commit_finalized_direct(cv.into(), None, next, "vct reopen refill")
                .expect("the height commits once its root is fetched");
            prop_assert_eq!(reopened.db.finalized_tip_height(), Some(Height(hole as u32)), "the tip advances past the former hole once the root arrives");
    });

    Ok(())
}

/// Verified-commitment-trees checkpoint handoff (merged increments 4+5): a
/// genesis-start fast sync writes the verified final frontier at the handoff
/// height, marks the database fast-synced, guards historical per-height tree reads
/// below the handoff, and leaves the tip treestate (which post-checkpoint semantic
/// verification resumes from) byte-identical to the legacy recompute.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] and the fixture by height
fn vct_fast_sync_handoff_marks_database_and_resumes() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
            let handoff = Height(last as u32);

            // The fast range is seeded just below Heartwood, so it is authenticated by
            // the ZIP-221 MMR (the synthetic chain's pre-Heartwood `FinalSaplingRoot`
            // headers are not consistent with the computed trees, so the Sapling-era
            // direct-header path can't be exercised here — that rides with the real
            // synced node). The handoff is at the tip.
            let seed = (heartwood - 1) as usize;

            // Legacy pass over [0, last]: the per-block fixture for the fast range, the
            // golden consensus state, and the real final frontiers at the handoff.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            let mut handoff_trees = None;
            let mut previous_sprout_root =
                zakura_chain::sprout::tree::NoteCommitmentTree::default().root();
            let mut historical_sprout_frontiers = Vec::new();
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
                if i > seed && i < last && trees.sprout.root() != previous_sprout_root {
                    historical_sprout_frontiers.push((trees.sprout.root(), trees.sprout.clone()));
                }
                previous_sprout_root = trees.sprout.root();
                if i == last {
                    handoff_trees = Some(trees);
                }
            }
            prop_assert!(
                !historical_sprout_frontiers.is_empty(),
                "the VCT fixture must include a pre-handoff Sprout commitment"
            );
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();
            let golden_tip = legacy.db.note_commitment_trees_for_tip().unwrap();
            let handoff_trees = handoff_trees.expect("committed the handoff block");

            // Fast genesis-start pass over [0, last], supplying the verified frontiers
            // for the handoff at `last`.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut fast,
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );
            prop_assert!(!fast.vct_fast_needs_successor(handoff), "the trusted handoff frontier authenticates the handoff root without a successor");
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                fast.commit_finalized_direct(cv.into(), None, next, "vct fast handoff")
                    .expect("verified fast commit succeeds");
            }

            // The database is marked fast-synced at the handoff height, and the upgrade height is
            // genesis: a node that fast-syncs from genesis records `U = 0`, so its whole `[0, H)`
            // range is the absent band and every request is served from the index.
            prop_assert_eq!(fast.vct_fast_synced_below(), Some(handoff), "fast-sync marker is set to the handoff height");
            prop_assert_eq!(fast.db.vct_upgrade_height(), Some(Height(0)), "genesis fast sync records the upgrade height at genesis");

            // Consensus state (anchor sets + history root) matches the legacy recompute.
            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors must match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history must match legacy");

            // The handoff wrote the real frontier at the checkpoint, so the tip
            // treestate that semantic verification resumes from matches legacy.
            let fast_tip = fast.db.note_commitment_trees_for_tip().unwrap();
            prop_assert_eq!(fast_tip.sapling.root(), golden_tip.sapling.root(), "tip sapling frontier must match legacy");
            prop_assert_eq!(fast_tip.orchard.root(), golden_tip.orchard.root(), "tip orchard frontier must match legacy");
            prop_assert_eq!(fast_tip.sprout.root(), golden_tip.sprout.root(), "tip sprout frontier must match legacy");
            for (root, expected_frontier) in &historical_sprout_frontiers {
                let actual_frontier = fast
                    .db
                    .sprout_tree_by_anchor(root)
                    .expect("each changed fast-sync Sprout root is persisted");
                prop_assert_eq!(
                    actual_frontier.root(),
                    expected_frontier.root(),
                    "historical Sprout root resolves to its complete frontier after fast sync"
                );
            }

            // State contextual validation must still resolve an old pre-handoff Sprout
            // anchor after a fresh VCT sync, then derive the interstitial tree for a
            // later JoinSplit in the same post-handoff V4 transaction.
            //
            // The fixture keeps historical JoinSplit fields and V4/Groth16 structure,
            // but uses a placeholder proof because this routine intentionally performs
            // contextual anchor validation only (proof verification runs earlier).
            let (_old_anchor, old_anchor_tree) = historical_sprout_frontiers
                .first()
                .expect("the VCT fixture has a changed pre-handoff Sprout frontier");
            let post_handoff_v4 = v4_transaction_with_interstitial_anchor(old_anchor_tree);
            prop_assert_eq!(
                post_handoff_v4.sprout_groth16_joinsplits().count(),
                2,
                "the regression transaction has multiple Groth16 JoinSplits"
            );
            tx_anchors_refer_to_final_treestates(
                &fast.db,
                None,
                &UnminedTx::from(post_handoff_v4),
            )
            .expect(
                "fresh VCT sync preserves the old final Sprout tree needed to validate \
                 the later JoinSplit's interstitial anchor",
            );

            // A corrupted embedded Sprout handoff frontier is a local artifact failure,
            // not a retryable peer-root stall. It must reject the handoff atomically and
            // leave the previous finalized tip and locally reconstructed Sprout tree intact.
            let mut corrupt_sprout = zakura_chain::sprout::tree::NoteCommitmentTree::default();
            corrupt_sprout
                .append(zakura_chain::sprout::NoteCommitment::from([99; 32]))
                .expect("one corrupt fixture commitment fits");
            prop_assert_ne!(corrupt_sprout.root(), handoff_trees.sprout.root());
            let mut corrupt_handoff = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut corrupt_handoff,
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                Arc::new(corrupt_sprout),
                handoff_trees.ironwood.clone(),
            );
            for i in 0..last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = Some(vct_successor_header(blocks[i + 1].block.clone()));
                corrupt_handoff
                    .commit_finalized_direct(cv.into(), None, next, "vct corrupt Sprout handoff prefix")
                    .expect("the prefix before the corrupt handoff commits");
            }
            let prior_sprout_root = corrupt_handoff.db.sprout_tree_for_tip().unwrap().root();
            let error = corrupt_handoff
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(blocks[last].block.clone()).into(),
                    None,
                    None,
                    "vct corrupt Sprout handoff",
                )
                .expect_err("a corrupt embedded Sprout handoff must fail");
            prop_assert_eq!(error.vct_retryable_height(), None, "embedded Sprout corruption is non-retryable");
            prop_assert!(error.to_string().contains("checkpoint-verified block"));
            prop_assert_eq!(corrupt_handoff.finalized_tip_height(), Some(Height(last as u32 - 1)), "failed handoff leaves the previous tip");
            prop_assert_eq!(corrupt_handoff.db.sprout_tree_for_tip().unwrap().root(), prior_sprout_root, "failed handoff leaves Sprout state unchanged");

            // Historical per-height tree reads below the handoff are unavailable
            // (guarded, no panic), while the handoff height itself is present.
            prop_assert!(fast.db.sapling_tree_by_height(&Height(last as u32 - 1)).is_none(), "below-handoff sapling tree read is guarded");
            prop_assert!(fast.db.orchard_tree_by_height(&Height(last as u32 - 1)).is_none(), "below-handoff orchard tree read is guarded");
            prop_assert!(fast.db.sapling_tree_by_height(&handoff).is_some(), "handoff sapling tree is present");
            prop_assert!(fast.db.orchard_tree_by_height(&handoff).is_some(), "handoff orchard tree is present");

            // Root-serving index (design §4): the fast-synced node holds no per-height trees
            // below the handoff (asserted just above), yet it must still serve `tree_aux`
            // roots for that range so the root-serving fleet does not collapse as nodes
            // fast-sync. Those roots come from the compact `commitment_roots_by_height` index
            // the fast path persists per block, and they match exactly the roots the
            // legacy/archive node derives from its per-height trees.
            let below_handoff = Height((seed + 1) as u32)..=Height(last as u32 - 1);
            let served = fast.db.commitment_roots_by_height_range(below_handoff.clone());
            let expected = commitment_aux::produce_block_roots(&legacy.db, below_handoff.clone());
            prop_assert!(!served.is_empty(), "a fast-synced node serves below-handoff roots from the index");
            prop_assert_eq!(served, expected.clone(), "index-served roots match the legacy per-height-tree roots");

            // The same range goes through `serve_block_roots`: with `U = 0` the request starts at
            // or above the upgrade height, so it is served entirely from the index — no per-height
            // trees (which the fast-synced node lacks below the handoff) are consulted.
            prop_assert_eq!(serve_block_roots(&fast.db, below_handoff), expected, "serve_block_roots serves the fast-synced range from the index");

            // The `z_gettreestate` RPC gate predicate matches the read guard: a
            // below-handoff height is unavailable (typed archive-mode error), while the
            // handoff height itself is available.
            prop_assert!(fast.db.vct_historical_tree_unavailable(HashOrHeight::Height(Height(last as u32 - 1))), "RPC gate: below-handoff treestate is unavailable");
            prop_assert!(!fast.db.vct_historical_tree_unavailable(HashOrHeight::Height(handoff)), "RPC gate: handoff treestate is available");

            // Negative: a peer can supply a wrong root exactly at the handoff height,
            // where there is no buffered checkpoint successor to authenticate it. The
            // final embedded frontier still binds the expected root, so the committer
            // must reject and retry instead of panicking or writing a bad handoff.
            let mut bad_handoff_fixture = fixture.clone();
            let bad_handoff_entry = bad_handoff_fixture
                .get_mut(&(last as u32))
                .expect("fixture contains the handoff root");
            prop_assert_ne!(bad_handoff_entry.0, Default::default(), "a post-NU5 handoff block must have a non-empty Sapling root");
            bad_handoff_entry.0 = Default::default();

            let mut bad_handoff = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut bad_handoff,
                bad_handoff_fixture,
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );

            let mut error_height = None;
            let mut handoff_error = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                match bad_handoff.commit_finalized_direct(cv.into(), None, next, "vct bad handoff") {
                    Ok(_) => {}
                    Err(error) => {
                        error_height = Some(i);
                        handoff_error = Some(error);
                        break;
                    }
                }
            }
            prop_assert_eq!(error_height, Some(last), "the bad handoff root is rejected at the handoff height");
            let handoff_error = handoff_error.expect("the bad handoff root failed");
            prop_assert!(
                format!("{handoff_error:?}").contains("VctSuppliedRootUnavailable"),
                "a bad handoff root returns the retryable VctSuppliedRootUnavailable error, got: {handoff_error:?}"
            );
            prop_assert_eq!(
                bad_handoff.db.finalized_tip_height(),
                Some(Height(last as u32 - 1)),
                "the refused handoff block left state untouched"
            );

            // Negative: the handoff's *Ironwood* frontier is authenticated too, not just
            // Sapling/Orchard. Below Nu6_3 (true for every height in this test's range),
            // the supplied Ironwood root is pinned to empty and the fixture's roots are
            // all empty already, so this exercises the frontier comparison itself
            // (`vct_verify_last_checkpoint_frontier_roots`) rather than the below-Nu6_3
            // pin: a non-empty Ironwood *frontier* mismatches the (correctly empty)
            // supplied root, and the handoff must be rejected instead of silently
            // accepted (which it would have been before the frontier gained an Ironwood
            // slot: the frontier had no Ironwood field to check against at all).
            let mut wrong_ironwood_frontier = zakura_chain::ironwood::tree::NoteCommitmentTree::default();
            wrong_ironwood_frontier
                .append(halo2::pasta::pallas::Base::from(1u64))
                .expect("single-note Ironwood tree is not full");
            prop_assert_ne!(
                wrong_ironwood_frontier.root(),
                handoff_trees.ironwood.root(),
                "test needs an Ironwood frontier distinct from the real (empty) one"
            );

            let mut bad_ironwood_handoff = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut bad_ironwood_handoff,
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                Arc::new(wrong_ironwood_frontier),
            );

            let mut ironwood_error_height = None;
            let mut ironwood_handoff_error = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                match bad_ironwood_handoff.commit_finalized_direct(cv.into(), None, next, "vct bad ironwood handoff") {
                    Ok(_) => {}
                    Err(error) => {
                        ironwood_error_height = Some(i);
                        ironwood_handoff_error = Some(error);
                        break;
                    }
                }
            }
            prop_assert_eq!(ironwood_error_height, Some(last), "the bad Ironwood handoff frontier is rejected at the handoff height");
            let ironwood_handoff_error = ironwood_handoff_error.expect("the bad Ironwood handoff frontier failed");
            prop_assert!(
                format!("{ironwood_handoff_error:?}").contains("VctSuppliedRootUnavailable"),
                "a bad Ironwood handoff frontier returns the retryable VctSuppliedRootUnavailable error, got: {ironwood_handoff_error:?}"
            );
            prop_assert_eq!(
                bad_ironwood_handoff.db.finalized_tip_height(),
                Some(Height(last as u32 - 1)),
                "the refused Ironwood handoff block left state untouched"
            );
    });

    Ok(())
}

/// Switching between the rollout fast path and the manual recompute path is safe at the
/// committed-state boundaries: after the handoff writes the real frontier, legacy recompute can
/// resume from that frontier; before any fast commit has frozen the frontier, a later fast sync
/// can consume verified roots for future heights.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] and the fixture by height
fn vct_mode_switches_continue_from_safe_boundaries() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {
            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let handoff_index = (nu5 + 3) as usize;
            let post_handoff_tip = handoff_index + 2;
            prop_assert!(blocks.len() > post_handoff_tip, "generated chain unexpectedly short");
            let handoff = Height(handoff_index as u32);
            let seed = (heartwood - 1) as usize;

            // Legacy golden pass over the full range: source fast roots and final frontiers, then
            // compare both switching scenarios against this byte-identical manual recompute.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            let mut handoff_trees = None;
            let mut post_handoff_roots = None;
            for i in 0..=post_handoff_tip {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct switch legacy")
                    .unwrap();
                if i > seed && i <= handoff_index {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
                if i == handoff_index {
                    handoff_trees = Some(trees);
                } else if i == handoff_index + 1 {
                    post_handoff_roots = Some((
                        trees.sapling.root(),
                        trees.orchard.root(),
                        zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                    ));
                }
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();
            let golden_tip = legacy.db.note_commitment_trees_for_tip().unwrap();
            let handoff_trees = handoff_trees.expect("committed the handoff block");
            let post_handoff_roots = post_handoff_roots.expect("committed a post-handoff block");

            // Fast -> manual: complete the fast handoff, reopen with the force-disable knob, and
            // keep checkpoint sync enabled while post-handoff blocks recompute from the real
            // frontier written at the handoff.
            let fast_to_manual_dir = TempDir::new().expect("temp dir");
            let fast_config = Config {
                cache_dir: fast_to_manual_dir.path().to_path_buf(),
                ephemeral: false,
                ..Config::default()
            };
            {
                let mut fast = FinalizedState::new(&fast_config, &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
                enable_vct_test_fixture_source_with_handoff(
                    &mut fast,
                    fixture.clone(),
                    handoff,
                    handoff_trees.sapling.clone(),
                    handoff_trees.orchard.clone(),
                    handoff_trees.sprout.clone(),
                    handoff_trees.ironwood.clone(),
                );
                for i in 0..=handoff_index {
                    let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                    let next = (i < handoff_index)
                        .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                    fast.commit_finalized_direct(cv.into(), None, next, "vct switch fast prefix")
                        .expect("verified fast prefix commits");
                }
                prop_assert_eq!(fast.vct_fast_synced_below(), Some(handoff), "fast sync reached the handoff before the switch");
            }

            let manual_config = Config {
                vct_fast_sync: false,
                ..fast_config
            };
            let mut manual = FinalizedState::new(&manual_config, &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            for i in (handoff_index + 1)..=post_handoff_tip {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                manual
                    .commit_finalized_direct(cv.into(), None, None, "vct switch manual suffix")
                    .expect("manual suffix commits after fast handoff");
            }
            let manual_tip = manual.db.note_commitment_trees_for_tip().unwrap();
            prop_assert_eq!(manual.db.vct_anchor_digest(), golden_anchors, "fast-to-manual anchors match legacy");
            prop_assert_eq!(manual.db.history_tree().hash(), golden_history, "fast-to-manual history matches legacy");
            prop_assert_eq!(manual_tip.sapling.root(), golden_tip.sapling.root(), "fast-to-manual sapling tip matches legacy");
            prop_assert_eq!(manual_tip.orchard.root(), golden_tip.orchard.root(), "fast-to-manual orchard tip matches legacy");
            prop_assert_eq!(manual_tip.sprout.root(), golden_tip.sprout.root(), "fast-to-manual sprout tip matches legacy");

            // Manual -> fast: commit a prefix with the force-disable knob before any fast block
            // can freeze the frontier, then reopen and consume verified roots through the handoff.
            let manual_to_fast_dir = TempDir::new().expect("temp dir");
            let manual_prefix_config = Config {
                cache_dir: manual_to_fast_dir.path().to_path_buf(),
                ephemeral: false,
                vct_fast_sync: false,
                ..Config::default()
            };
            {
                let mut manual_prefix = FinalizedState::new(&manual_prefix_config, &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
                for i in 0..=seed {
                    let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                    manual_prefix
                        .commit_finalized_direct(cv.into(), None, None, "vct switch manual prefix")
                        .expect("manual prefix commits");
                }
            }

            let fast_suffix_config = Config {
                vct_fast_sync: true,
                ..manual_prefix_config
            };
            let mut fast_suffix = FinalizedState::new(&fast_suffix_config, &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut guarded_fixture = fixture;
            // A stale or over-eager peer cache entry above the handoff must be ignored so
            // the committer resumes legacy recompute from the real handoff frontier.
            prop_assert_ne!(
                post_handoff_roots.0,
                Default::default(),
                "a post-NU5 post-handoff block must have a non-empty Sapling root",
            );
            guarded_fixture.insert(
                (handoff_index + 1) as u32,
                (
                    Default::default(),
                    post_handoff_roots.1,
                    post_handoff_roots.2,
                ),
            );
            enable_vct_test_fixture_source_with_handoff(
                &mut fast_suffix,
                guarded_fixture,
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );
            for i in (seed + 1)..=post_handoff_tip {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < post_handoff_tip)
                    .then(|| vct_successor_header(blocks[i + 1].block.clone()));
                fast_suffix
                    .commit_finalized_direct(cv.into(), None, next, "vct switch fast suffix")
                    .expect("fast suffix commits after manual prefix");
            }
            prop_assert_eq!(
                fast_suffix.vct_fast_count(),
                (handoff_index - seed) as u64,
                "an above-handoff cached root must not keep the committer on the fast path",
            );
            let fast_suffix_tip = fast_suffix.db.note_commitment_trees_for_tip().unwrap();
            prop_assert_eq!(fast_suffix.db.vct_anchor_digest(), golden_anchors, "manual-to-fast anchors match legacy");
            prop_assert_eq!(fast_suffix.db.history_tree().hash(), golden_history, "manual-to-fast history matches legacy");
            prop_assert_eq!(fast_suffix_tip.sapling.root(), golden_tip.sapling.root(), "manual-to-fast sapling tip matches legacy");
            prop_assert_eq!(fast_suffix_tip.orchard.root(), golden_tip.orchard.root(), "manual-to-fast orchard tip matches legacy");
            prop_assert_eq!(fast_suffix_tip.sprout.root(), golden_tip.sprout.root(), "manual-to-fast sprout tip matches legacy");
    });

    Ok(())
}

/// Standalone test isolating the verify-before-commit **dedup**: each header
/// commitment is checked once, not twice.
///
/// - **Skip:** the first fast block runs its own commitment check; the next one
///   is skipped, because the first block's look-ahead already validated it.
/// - **Stale-cache guard:** a cache entry with the right height but the *wrong*
///   hash must not trigger a skip — the guard forces the own check to run, so a
///   stale or mismatched entry can never let an unverified block through.
/// - **Wrapper-hash guard:** a public `CheckpointVerifiedBlock::with_hash` caller
///   cannot replay a stale cached successor hash onto a different block.
#[test]
fn vct_dedup_skips_redundant_check_and_guards_stale_cache() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0 as usize;

            // Seed just before NU5, then operate on five consecutive fast blocks so
            // the auth-data and forged-wrapper regressions exercise
            // `hashBlockCommitments`.
            let seed = nu5 - 2;
            let last = seed + 5;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");

            // Legacy pass to record the correct per-block roots as the fixture.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            for (i, prepared) in blocks.iter().take(last + 1).enumerate() {
                let cv = CheckpointVerifiedBlock::from(prepared.block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct dedup legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
            }

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture);

            // Commit block `i` with its real successor as the one-block look-ahead.
            let commit = |fast: &mut FinalizedState, i: usize| {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct dedup fast")
                    .expect("verified fast commit succeeds");
            };

            // genesis..=seed take the recompute path (no fixture entries), so the dedup
            // never engages here.
            for i in 0..=seed {
                commit(&mut fast, i);
            }
            prop_assert_eq!(fast.vct_prevalidated_count(), 0, "no fast blocks committed yet");

            // First fast block: no cached predecessor, so it runs its own check.
            commit(&mut fast, seed + 1);
            prop_assert_eq!(fast.vct_prevalidated_count(), 0, "the first fast block runs its own commitment check");

            // ZIP-244 transaction IDs do not commit to authorizing data. Mutate
            // a transparent unlocking script as an untrusted peer can, producing
            // a body with the expected header hash and transaction ID but a
            // different auth-data root. (Coinbase scripts are a special case:
            // they are bound by the mined transaction ID.)
            let honest_block = blocks[seed + 3].block.clone();
            let mut hostile_block = (*honest_block).clone();
            let (transaction_index, input_index) = honest_block
                .transactions
                .iter()
                .enumerate()
                .find_map(|(transaction_index, transaction)| {
                    transaction
                        .inputs()
                        .iter()
                        .position(|input| {
                            matches!(input, zakura_chain::transparent::Input::PrevOut { .. })
                        })
                        .map(|input_index| (transaction_index, input_index))
                })
                .expect("the generated NU5 block must contain a transparent spend");
            let hostile_transaction =
                Arc::make_mut(&mut hostile_block.transactions[transaction_index]);
            let zakura_chain::transparent::Input::PrevOut { unlock_script, .. } =
                &mut hostile_transaction.inputs_mut()[input_index]
            else {
                unreachable!("the selected input is a transparent spend");
            };
            *unlock_script = zakura_chain::transparent::Script::new(&[0x42]);
            let hostile_block = Arc::new(hostile_block);

            prop_assert_eq!(
                hostile_block.hash(),
                honest_block.hash(),
                "authorizing-data malleation must preserve the block hash",
            );
            prop_assert_eq!(
                hostile_block.transactions[transaction_index].hash(),
                honest_block.transactions[transaction_index].hash(),
                "ZIP-244 transaction IDs must not bind transparent unlocking scripts",
            );
            prop_assert_ne!(
                hostile_block.auth_data_root(),
                honest_block.auth_data_root(),
                "the hostile body must have a different auth-data root",
            );

            // Store the canonical successor header and its precomputed auth-data root,
            // as header sync does before body sync. The separately constructed malformed
            // same-hash body must not supply this witness. Using only the stored header
            // preserves the valid root at `seed + 2` and the prevalidation dedup.
            let header_heights =
                Height((seed + 2) as u32)..=Height((seed + 3) as u32);
            let header_roots =
                commitment_aux::produce_block_roots(&legacy.db, header_heights);
            for prepared in &blocks[(seed + 2)..=(seed + 3)] {
                fast.db
                    .seed_zakura_header_from_committed_block(
                        prepared
                            .block
                            .coinbase_height()
                            .expect("prepared successor blocks have a coinbase height"),
                        &prepared.block,
                    )
                    .expect("the canonical successor header is stored");
            }
            fast.db
                .insert_zakura_header_commitment_roots(header_roots)
                .expect("the canonical successor roots are stored");

            let cv = CheckpointVerifiedBlock::from(blocks[seed + 2].block.clone());
            let stored_successor = fast
                .vct_successor_from_header_store(
                    Height((seed + 2) as u32),
                    blocks[seed + 2].hash,
                )
                .expect("header sync stored the canonical successor witness");
            fast.commit_finalized_direct(
                cv.into(),
                None,
                Some(stored_successor),
                "vct header-only successor with malformed body available",
            )
            .expect("the stored successor witness preserves the valid current root");
            prop_assert_eq!(fast.vct_prevalidated_count(), 1, "the second fast block skips its redundant own commitment check");

            let mismatched = CheckpointVerifiedBlock::from(hostile_block.clone());

            let error = fast
                .commit_finalized_direct(
                    mismatched.into(),
                    None,
                    None,
                    "vct mismatched auth-data root",
                )
                .expect_err("a mismatched body must not reuse header-only prevalidation");
            prop_assert!(
                format!("{error:?}").contains("VctBlockAuthDataRootMismatch"),
                "the mismatched body must be classified as invalid, got: {error:?}",
            );
            prop_assert_eq!(
                error.vct_retryable_height(),
                None,
                "the write loop must not park and retry an irreparably invalid body",
            );
            prop_assert_eq!(
                fast.vct_prevalidated_count(),
                1,
                "a mismatched auth-data root must not increment the prevalidated count",
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((seed + 2) as u32)),
                "the rejected body must leave finalized state untouched",
            );

            // A write-loop reset clears the prevalidation cache. The same invalid
            // body must still be a hard error: replacing the supplied roots cannot
            // repair a body whose auth data does not match its header commitment.
            fast.clear_vct_prevalidated_next();
            let mismatched_without_cache = CheckpointVerifiedBlock::from(hostile_block);
            let error = fast
                .commit_finalized_direct(
                    mismatched_without_cache.into(),
                    None,
                    None,
                    "vct mismatched auth-data root without prevalidation",
                )
                .expect_err("an invalid body must not become retryable when the cache is empty");
            prop_assert!(
                format!("{error:?}").contains("InvalidBlockCommitment"),
                "the cache-empty mismatch must remain a block error, got: {error:?}",
            );
            prop_assert_eq!(
                error.vct_retryable_height(),
                None,
                "the write loop must reset rather than park the invalid body",
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((seed + 2) as u32)),
                "the cache-empty rejected body must leave finalized state untouched",
            );

            // Rejecting either form of the invalid body must not evict the
            // authenticated VCT roots. A subsequently downloaded honest body
            // with the same hash can therefore commit and let checkpoint sync
            // continue.
            commit(&mut fast, seed + 3);
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((seed + 3) as u32)),
                "the honest same-hash body must commit after the hostile body is rejected",
            );

            // Stale-cache guard: overwrite the cache with the correct height but the
            // hash of a *different* block. The next commit must NOT skip.
            let stale_hash = blocks[seed + 1].hash;
            prop_assert_ne!(stale_hash, blocks[seed + 4].hash, "stale hash must differ from the real block");
            fast.vct
                .set_prevalidated_next(Some((
                    Height((seed + 4) as u32),
                    stale_hash,
                    Some(blocks[seed + 4].block.auth_data_root()),
                )));
            commit(&mut fast, seed + 4);
            prop_assert_eq!(fast.vct_prevalidated_count(), 1, "a stale cache entry (wrong hash) must not cause a false skip");

            // Public wrapper-hash guard: the stale cache records a real look-ahead
            // hash, but a caller-controlled checkpoint wrapper tries to replay that
            // hash onto a different block whose own NU5 header commitment is invalid.
            // The skip must compare the cache against the wrapped block's real hash,
            // not the wrapper hash, so the bad commitment is checked and rejected.
            let forged_wrapper_hash = blocks[seed + 2].hash;
            let bad_block = blocks[seed + 5].block.clone().set_block_commitment([0x42; 32]);
            let bad_block_hash = bad_block.hash();
            prop_assert_ne!(
                forged_wrapper_hash,
                bad_block_hash,
                "the forged wrapper hash must differ from the bad block's real hash",
            );
            fast.vct
                .set_prevalidated_next(Some((
                    Height((seed + 5) as u32),
                    forged_wrapper_hash,
                    Some(blocks[seed + 5].block.auth_data_root()),
                )));
            let forged = CheckpointVerifiedBlock::with_hash(bad_block, forged_wrapper_hash);
            let error = fast
                .commit_finalized_direct(forged.into(), None, None, "vct forged wrapper hash")
                .expect_err("a forged wrapper hash must not skip the bad block's own commitment check");
            prop_assert!(
                format!("{error:?}").contains("InvalidBlockCommitment"),
                "the forged wrapper hash path must reject the bad commitment, got: {error:?}",
            );
            prop_assert_eq!(
                error.vct_retryable_height(),
                None,
                "a forged block commitment must not be retried as a supplied-root failure",
            );
            prop_assert_eq!(
                fast.vct_prevalidated_count(),
                1,
                "the forged wrapper hash must not increment the prevalidated count",
            );
            prop_assert_eq!(
                fast.db.finalized_tip_height(),
                Some(Height((seed + 4) as u32)),
                "the rejected forged block must leave finalized state untouched",
            );
    });

    Ok(())
}

/// Clearing a cached VCT successor prevalidation must disarm exactly one possible
/// skip without disabling the normal dedup optimization for future contiguous fast
/// blocks. This covers the write-loop reset/drop behavior indirectly: those paths
/// call `clear_vct_prevalidated_next()` when buffered checkpoint state is discarded.
#[test]
fn vct_clear_prevalidation_cache_disarms_skip_then_dedup_resumes() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0 as usize;
            let seed = nu5 - 2;
            let last = seed + 5;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let mut fixture = std::collections::HashMap::new();
            for (i, prepared) in blocks.iter().take(last + 1).enumerate() {
                let cv = CheckpointVerifiedBlock::from(prepared.block.clone());
                let (_h, trees) = legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct clear legacy")
                    .unwrap();
                if i > seed {
                    fixture.insert(
                        i as u32,
                        (
                            trees.sapling.root(),
                            trees.orchard.root(),
                            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                        ),
                    );
                }
            }

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture);

            let commit = |fast: &mut FinalizedState, i: usize| {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct clear fast")
                    .expect("verified fast commit succeeds");
            };

            for i in 0..=seed {
                commit(&mut fast, i);
            }
            commit(&mut fast, seed + 1);
            prop_assert_eq!(fast.vct_prevalidated_count(), 0, "first fast block runs its own check");

            commit(&mut fast, seed + 2);
            prop_assert_eq!(fast.vct_prevalidated_count(), 1, "second fast block uses predecessor look-ahead");

            fast.clear_vct_prevalidated_next();
            commit(&mut fast, seed + 3);
            prop_assert_eq!(
                fast.vct_prevalidated_count(),
                1,
                "clearing the cache forces the next fast block to run its own check",
            );

            commit(&mut fast, seed + 4);
            prop_assert_eq!(
                fast.vct_prevalidated_count(),
                2,
                "normal successor dedup resumes after the cleared block commits",
            );
    });

    Ok(())
}

/// Increment-3 contract proof: a roots/frontier payload **produced from a database**
/// (the serving read path) can replace the fixture and drives the fast path to
/// byte-identical consensus state.
///
/// Builds an archive/legacy state over a generated valid-commitment chain (crossing
/// Heartwood and NU5), produces the per-block roots and final frontier from that DB
/// via [`commitment_aux::produce_block_roots`] / [`commitment_aux::produce_final_frontiers`],
/// then drives a fresh fast-sync state that consumes the produced payload through the
/// test-only [`commitment_aux::FixtureSource`]. Asserts the fast anchors + history-tree hash are
/// byte-identical to the legacy build, and that the produced final frontier agrees with
/// the legacy tip frontier and the produced root at the handoff height.
///
/// This is coverage the existing equivalence test lacks: there the roots are captured
/// from the committer's inline-returned trees, here they come from the **DB read path**
/// a serving node runs. No networking and no DB-format change.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] (the look-ahead) and by height
fn vct_db_produced_payload_round_trips_to_byte_identical_state() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");
            // Seed below Heartwood so the fast range creates the history tree and
            // crosses the NU5 V1->V2 boundary, matching the equivalence test.
            let seed = (heartwood - 1) as usize;

            // Legacy/archive pass: a real DB with per-height trees, plus the golden state.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct round-trip legacy")
                    .unwrap();
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Produce the payload from the legacy DB's per-height trees (the serving read path).
            let last_height = Height(last as u32);
            let produced_roots = commitment_aux::produce_block_roots(
                &legacy.db,
                Height((seed + 1) as u32)..=last_height,
            );
            let produced_frontiers = commitment_aux::produce_final_frontiers(&legacy.db, last_height)
                .expect("legacy DB has the tip frontier");

            // The produced final frontier agrees with the legacy tip frontier and with the
            // produced root at the handoff height (the two producer outputs are consistent).
            let handoff = produced_roots.last().expect("produced a non-empty range");
            prop_assert_eq!(produced_frontiers.sapling.root(), handoff.sapling_root, "produced sapling frontier matches the produced root at handoff");
            prop_assert_eq!(produced_frontiers.orchard.root(), handoff.orchard_root, "produced orchard frontier matches the produced root at handoff");
            prop_assert_eq!(produced_frontiers.sapling.root(), legacy.db.sapling_tree_by_height(&last_height).unwrap().root(), "produced sapling frontier matches legacy tip");
            prop_assert_eq!(produced_frontiers.sprout.root(), legacy.db.sprout_tree_for_tip().unwrap().root(), "produced sprout frontier matches legacy tip");

            // Consume the DB-produced roots in a fresh fast-sync state.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            let produced_roots = produced_roots
                .into_iter()
                .map(|root| {
                    (
                        root.height.0,
                        (root.sapling_root, root.orchard_root, root.ironwood_root),
                    )
                })
                .collect();
            fast.enable_vct_fast_source(
                Box::new(commitment_aux::FixtureSource::new(
                    produced_roots,
                    test_handoff_frontiers(Height::MAX),
                )),
                false,
            );
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct round-trip fast")
                    .expect("verified fast commit from DB-produced roots succeeds");
            }

            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors from DB-produced roots match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history from DB-produced roots match legacy");

            // Serving stitch across the upgrade height `U`. Simulate a node that upgraded
            // mid-chain: it keeps the full per-height trees (written before the upgrade) but only
            // has the serving index from `U` upward. `serve_block_roots` must still return the
            // whole requested range as one contiguous run — trees fill `[start, U)`, the index
            // fills `[U, end]` — matching the all-trees reference, with no short batch at the
            // boundary that would stall the client's minimum-progress check.
            let serve_range = Height((seed + 1) as u32)..=last_height;
            let all_trees_reference =
                commitment_aux::produce_block_roots(&legacy.db, serve_range.clone());
            let upgrade = Height(((seed + 1 + last) / 2) as u32);
            prop_assert!(
                serve_range.start() < &upgrade && upgrade <= last_height,
                "the chosen upgrade height splits the served range"
            );
            let mut batch = DiskWriteBatch::new();
            batch.delete_range_commitment_roots_by_height(&legacy.db, &Height(0), &upgrade);
            batch.update_vct_upgrade_marker(&legacy.db, upgrade);
            legacy
                .db
                .write_batch(batch)
                .expect("simulating a mid-chain upgrade succeeds");
            prop_assert!(
                legacy
                    .db
                    .commitment_roots_by_height_range(Height(0)..=Height(upgrade.0 - 1))
                    .is_empty(),
                "the serving index is dropped below the upgrade height"
            );
            let stitched = serve_block_roots(&legacy.db, serve_range);
            prop_assert_eq!(
                stitched,
                all_trees_reference,
                "serve_block_roots stitches the trees below U with the index at/above U into one gap-free run"
            );
    });

    Ok(())
}

/// Verified-commitment-trees consumer half of the peer source: a
/// [`commitment_aux::PeerSource`] whose database is **filled incrementally** (as header
/// sync persists provisional root ranges when they arrive from peers) drives the fast
/// path to byte-identical consensus state. Same harness as the DB-produced round-trip,
/// but the produced roots are written into `commitment_roots_by_height` in two chunks
/// via [`ZakuraDb::insert_zakura_header_commitment_roots`] — proving the DB-backed,
/// header-sync-fed source is a drop-in for the fixture.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] (the look-ahead) and by height
fn vct_peer_source_filled_incrementally_drives_byte_identical_state() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(10),
            sapling: Some(15),
            blossom: Some(20),
            heartwood: Some(25),
            canopy: Some(30),
            nu5: Some(35),
            nu6: Some(40),
            nu6_1: Some(45),
            nu6_2: Some(47),
            nu6_3: Some(48),
            nu7: Some(50),
        })
        .expect("failed to set activation heights")
        .extend_funding_streams()
        .to_network()
        .expect("failed to build configured network");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy.clone()).with_valid_commitments().no_shrink())| {

            let blocks: Vec<_> = chain.iter().collect();
            let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
            let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
            // The untrusted peer source defers any fast block whose own root has no buffered
            // successor, so every committed fast block needs `blocks[i + 1]`. Keep `last` one
            // below the chain tip so the deepest commit still has a successor witness.
            let last = ((nu5 + 3) as usize).min(blocks.len().saturating_sub(2));
            prop_assert!(last > (nu5 as usize), "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Legacy/archive pass: a real DB with per-height trees, plus the golden state.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct peer-source legacy")
                    .unwrap();
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Produce the payload from the legacy DB (the serving read path).
            let produced_roots = commitment_aux::produce_block_roots(
                &legacy.db,
                Height((seed + 1) as u32)..=Height(last as u32),
            );

            // Consume the peer-source-supplied roots in a fresh fast-sync state. Each fast
            // block is committed with its successor buffered, as the write loop does — the
            // untrusted source defers a tip commit with no successor (covered by
            // `vct_peer_source_defers_unverifiable_tip_root_until_successor`).
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network, #[cfg(feature = "elasticsearch")] false).expect("opening an ephemeral database should succeed");

            // Fill the fast state's database incrementally, in two chunks, as header sync
            // would when successive root ranges arrive from a peer; the peer source reads
            // them back from that database.
            let split = produced_roots.len() / 2;
            fast.db
                .insert_zakura_header_commitment_roots(produced_roots[..split].iter().cloned())
                .expect("writing the first header-sync root chunk succeeds");
            fast.db
                .insert_zakura_header_commitment_roots(produced_roots[split..].iter().cloned())
                .expect("writing the second header-sync root chunk succeeds");
            let peer_source =
                commitment_aux::PeerSource::new(fast.db.clone(), test_handoff_frontiers(Height::MAX));
            fast.enable_vct_fast_source(Box::new(peer_source), true);
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct peer-source fast")
                    .expect("verified fast commit from peer-source roots succeeds");
            }

            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors from peer-source roots match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history from peer-source roots match legacy");
    });

    Ok(())
}
