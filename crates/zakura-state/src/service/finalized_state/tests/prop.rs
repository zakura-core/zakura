//! Randomised property tests for the finalized state.

use std::{
    collections::HashMap,
    env,
    error::Error,
    fs,
    sync::{Arc, Mutex, OnceLock},
};

use tempfile::TempDir;
use tokio::sync::oneshot;

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::{
        testnet::{ConfiguredActivationHeights, ParametersBuilder},
        NetworkUpgrade,
    },
    primitives::Groth16Proof,
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto},
    sprout::JoinSplit,
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{JoinSplitData, LockTime, Transaction, UnminedTx},
    LedgerState,
};
use zakura_test::prelude::*;

use crate::{
    config::{Config, PruningConfig, StorageMode},
    error::HistoricalTreeUnavailable,
    service::{
        arbitrary::PreparedChain,
        check::anchors::tx_anchors_refer_to_final_treestates,
        non_finalized_state::Chain,
        read::{
            derive_historical_frontiers, historical_tree::stored_frontier_before_absent_band,
            historical_tree::HistoricalTreeDerivationError, sapling_subtrees, sapling_tree,
            HistoricalTreeCache,
        },
    },
    tests::FakeChainHelper,
    HashOrHeight, ReadRequest, ReadResponse,
};

use super::super::{
    commitment_aux, export_frontier_grid_to, serve_block_roots,
    vct::validate_final_frontiers_bytes, verify_subtrees_against_stored, CheckpointVerifiedBlock,
    DiskWriteBatch, FinalizedState, FrontierArtifact, FrontierEntry, FrontierGridExportError,
    GridSpacing, VctAuxiliaryWindow, VctSuccessorWitness,
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

fn vct_successor_witness(block: Arc<Block>) -> VctSuccessorWitness {
    VctSuccessorWitness::from_header(
        block.header.clone(),
        block
            .coinbase_height()
            .expect("prepared successor blocks have a coinbase height"),
        block.auth_data_root(),
    )
}

fn next_vct_block(block: Arc<Block>) -> Option<VctSuccessorWitness> {
    Some(vct_successor_witness(block))
}

fn exact_vct_auxiliary_window(
    block: &Arc<Block>,
    height: Height,
    roots: (
        zakura_chain::sapling::tree::Root,
        zakura_chain::orchard::tree::Root,
        zakura_chain::ironwood::tree::Root,
    ),
    successor: &Arc<Block>,
) -> VctAuxiliaryWindow {
    use std::num::NonZeroU64;
    use zakura_header_chain::{
        AlarmSet, AuxDelivery, BodySizeHint, ChainScore, EngineMode, EngineSnapshot, EvidenceId,
        Frontier, FrontierSet, HeaderGeneration, SourceId, StateVersion, SuffixWork,
        TreeAuxRecordV1, VerifiedGeneration,
    };

    let hash = block.hash();
    let successor_height = height.next().expect("the VCT fixture has a successor");
    let successor_hash = successor.hash();
    let frontier = Frontier::new(height, hash);
    let snapshot = EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        frontiers: FrontierSet {
            finalized: frontier,
            header_best: Frontier::new(successor_height, successor_hash),
            verified_best: frontier,
        },
        header_best_score: ChainScore::new(SuffixWork::zero(), successor_hash),
        oldest_retained_height: height,
        alarms: AlarmSet::default(),
    };
    let owner: zakura_header_chain::HeaderSyncWorkOwner =
        zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot)
            .bind(1, NonZeroU64::new(1).expect("one is nonzero"))
            .into();
    let mut current_delivery_id = [0; 32];
    current_delivery_id[..4].copy_from_slice(&height.0.to_le_bytes());
    let mut successor_delivery_id = current_delivery_id;
    successor_delivery_id[4] = 1;
    let current = AuxDelivery::new(
        EvidenceId::from_digest(current_delivery_id),
        hash,
        SourceId::from_digest([1; 32]),
        owner,
        BodySizeHint::Unknown,
        Some(TreeAuxRecordV1 {
            height,
            sapling_root: roots.0,
            orchard_root: roots.1,
            ironwood_root: roots.2,
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: block.auth_data_root(),
        }),
    );
    let successor_delivery = AuxDelivery::new(
        EvidenceId::from_digest(successor_delivery_id),
        successor_hash,
        SourceId::from_digest([2; 32]),
        owner,
        BodySizeHint::Unknown,
        Some(TreeAuxRecordV1 {
            height: successor_height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: successor.auth_data_root(),
        }),
    );
    VctAuxiliaryWindow {
        engine_snapshot: snapshot,
        delivery_header: block.header.clone(),
        delivery: current,
        successor_height: Some(successor_height),
        successor: VctSuccessorWitness::from_delivery(
            successor.header.clone(),
            successor_height,
            successor_delivery,
        ),
    }
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

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            let mut state = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

/// This test commits blocks across all network upgrades.
/// The commits exercise finalized-state contextual validation.
/// The test also verifies that finalized state rejects an incorrect commitment.
#[test]
#[allow(clippy::print_stderr)]
fn all_upgrades_and_wrong_commitments_with_fake_activation_heights() -> Result<()> {
    let _init_guard = zakura_test::init();

    let network = ParametersBuilder::default()
        .with_activation_heights(ConfiguredActivationHeights {
            // These fixture values place NU5 within the generated chains.
            // The test fails if a generated chain does not cross NU5.
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

    // The test ignores `_count`, so the strategy disables shrinking.
    proptest!(ProptestConfig::with_cases(env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES)),
        |((chain, _count, network, _history_tree) in PreparedChain::default().with_ledger_strategy(ledger_strategy).with_valid_commitments().no_shrink())| {

            let mut state = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

/// This test compares the VCT fast path with legacy recomputation across upgrade boundaries.
/// Correct fixture roots produce the same anchor sets and history root.
/// Verify-before-commit rejects an incorrect fixture root.
/// The test seeds below Heartwood and creates the history tree at Heartwood.
/// The test crosses the NU5 V1-to-V2 transition.
/// The test also covers successor-header verification and trusted fixture tip commits.
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

            // Process a bounded prefix that crosses Heartwood and NU5.
            // The prefix includes two additional V2 blocks.
            // `last` identifies the comparison tip.
            // Generated chains exceed this prefix, so the test uses an assertion instead of a
            // proptest discard.
            let last = (nu5 + 3) as usize;
            prop_assert!(blocks.len() > last + 1, "generated chain unexpectedly short");

            // The fast path runs below the checkpoint, seeded from an already-committed
            // tip. Seed just before Heartwood so the fast range creates the history tree
            // (Heartwood) and crosses NU5 (V1->V2).
            let seed = (heartwood - 1) as usize;

            // Legacy pass over [0, last]: record per-block roots for the fast range as
            // the fixture, and the golden consensus state at the tip.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            // The fast pass recomputes genesis through `seed` because those heights lack fixtures.
            // The pass verifies later blocks against their buffered successors.
            // Every eligible block takes the fast path.
            // The fast-path result matches legacy recomputation.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            // Deduplication checks each header commitment once.
            // Only the first fast block runs its own commitment check.
            // Each predecessor look-ahead validates the next fast block.
            // The next block therefore skips a redundant check.
            prop_assert_eq!(fast.vct_prevalidated_count(), (last - seed - 1) as u64, "every fast block after the first skips its redundant own commitment check");

            // Production does not have a height-keyed root source.
            // This pass proves that exact hash-scoped auxiliary windows produce the same result.
            let mut exact = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            exact.enable_vct_fast_source(
                Box::new(commitment_aux::FixtureSource::new(
                    HashMap::new(),
                    test_handoff_frontiers(Height::MAX),
                )),
                true,
            );
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                if i <= seed {
                    exact
                        .commit_finalized_direct(cv.into(), None, None, "vct exact seed")
                        .expect("the pre-VCT prefix recomputes normally");
                    continue;
                }
                let height_u32 = u32::try_from(i).expect("the bounded fixture height fits in u32");
                let roots = fixture
                    .get(&height_u32)
                    .copied()
                    .expect("the exact fast range has roots");
                let window = exact_vct_auxiliary_window(
                    &blocks[i].block,
                    Height(height_u32),
                    roots,
                    &blocks[i + 1].block,
                );
                exact
                    .commit_finalized_direct_with_exact_aux_for_test(
                        cv.into(),
                        window,
                        "vct exact auxiliary",
                    )
                    .expect("exact hash-scoped auxiliary roots commit");
            }
            prop_assert_eq!(exact.db.vct_anchor_digest(), golden_anchors, "exact auxiliary anchors must match legacy");
            prop_assert_eq!(exact.db.history_tree().hash(), golden_history, "exact auxiliary history must match legacy");
            prop_assert_eq!(exact.vct_fast_count(), (last - seed) as u64, "every exact auxiliary height took the fast path");

            // A trusted local fixture may commit its tip root without a successor: it is
            // not adversarial and the root is checked in arrears when a successor arrives.
            let mut no_successor = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut no_successor, fixture.clone());
            for i in 0..last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                no_successor
                    .commit_finalized_direct(cv.into(), None, next, "vct no-successor seed")
                    .expect("verified fast commit succeeds with successor");
            }
            prop_assert!(!no_successor.vct_fast_needs_successor(Height(last as u32), true), "a trusted fixture tip can commit without a successor");
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

            let mut bad = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let mut bad_orchard = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

/// This test verifies that VCT fast sync never recomputes a missing supplied root after freezing
/// the note-commitment frontier. The running frontier no longer represents the actual frontier.
/// Recomputation could fold an incorrect root into the history MMR and corrupt consensus state.
/// The committer must return `VctSuppliedRootUnavailable` and leave the database untouched.
/// A later header-range delivery can then provide the missing root.
/// `vct_fast_path_matches_legacy_and_rejects_wrong_roots` covers incorrect-root rejection.
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            // Remove a post-NU5 root from the fixture.
            // The missing root models a peer omission or a verification eviction.
            // Earlier fast blocks freeze the frontier.
            // This height therefore has no actual frontier for recomputation.
            let hole = (nu5 + 1) as usize;
            prop_assert!(hole > seed && hole < last, "the hole must be inside the fast range");
            fixture.remove(&(hole as u32));

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source(&mut fast, fixture);

            let mut error_height = None;
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
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

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

/// This test prevents an untrusted peer source from committing a root without a successor header.
/// The next block's header authenticates the current block's roots.
/// A tip commit would otherwise persist an unauthenticated root irreversibly.
/// An incorrect tip root could then stall sync at the next block.
/// The committer returns `VctSuppliedRootAwaitingSuccessor` and leaves the database untouched.
/// The committer accepts the same height after the test buffers a successor.
/// A trusted local fixture remains exempt from this requirement.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and inserts roots by height
fn vct_untrusted_source_defers_unverifiable_tip_root_until_successor() -> Result<()> {
    use crate::service::finalized_state::commitment_aux::FixtureSource;
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            // This untrusted source contains the correct roots.
            // A missing successor causes the deferral.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            let roots = peer_roots
                .into_iter()
                .map(|roots| {
                    (
                        roots.height.0,
                        (roots.sapling_root, roots.orchard_root, roots.ironwood_root),
                    )
                })
                .collect();
            let source = FixtureSource::new(roots, test_handoff_frontiers(Height::MAX));
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

            // The tip target lacks a successor header and must defer.
            // The untrusted source cannot authenticate the correct root by itself.
            prop_assert!(
                fast.vct_fast_needs_successor(Height(tip_target as u32), true),
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
                "a non-linking witness is not a root failure — the correct fixture remains available: {error:?}"
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

            // Buffering a successor lets the same height commit and advances the tip.
            // The deferral represented a wait instead of a permanent stall.
            // The forged-witness attempt did not evict the root.
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

/// This test verifies same-height recovery from an incorrect untrusted root.
/// The committer rejects the root and leaves the database below the height.
/// The committer then accepts the same block with a replacement root.
#[test]
#[allow(clippy::needless_range_loop)] // the loop indexes blocks[i+1] and inserts roots by height
fn vct_untrusted_source_bad_root_replacement_commits_same_height() -> Result<()> {
    use crate::service::finalized_state::commitment_aux::FixtureSource;
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            let mut roots: std::collections::HashMap<_, _> = peer_roots
                .into_iter()
                .map(|roots| {
                    (
                        roots.height.0,
                        (roots.sapling_root, roots.orchard_root, roots.ironwood_root),
                    )
                })
                .collect();
            let source = FixtureSource::new(roots.clone(), test_handoff_frontiers(Height::MAX));
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

            roots.insert(
                correct_target_root.height.0,
                (
                    correct_target_root.sapling_root,
                    correct_target_root.orchard_root,
                    correct_target_root.ironwood_root,
                ),
            );
            fast.enable_vct_fast_source(
                Box::new(FixtureSource::new(
                    roots,
                    test_handoff_frontiers(Height::MAX),
                )),
                true,
            );

            let cv = CheckpointVerifiedBlock::from(blocks[target].block.clone());
            fast.commit_finalized_direct(cv.into(), None, next, "vct refilled target")
                .expect("the same height commits once the untrusted root is replaced");
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
                let mut fast = FinalizedState::new(&config, &network).expect("opening an ephemeral database should succeed");
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

            // Session 2 reopens the same database.
            // The session removes the next root to model peer omission or verification eviction.
            // Skip the constructor-time interrupted-fast-sync resume guard.
            // This configured network has no embedded frontiers, so `from_config` yields no source.
            // The test attaches a fixture source below.
            // A Mainnet node already has its configured source when it opens the database.
            let mut reopened = FinalizedState::new_with_debug_and_storage_validation(
                &config,
                &network,
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut fast,
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );
            prop_assert!(!fast.vct_fast_needs_successor(handoff, true), "the trusted handoff frontier authenticates the handoff root without a successor");
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last)
                    .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
                fast.commit_finalized_direct(cv.into(), None, next, "vct fast handoff")
                    .expect("verified fast commit succeeds");
            }

            // The database is marked fast-synced at the handoff height, and the upgrade height is
            // genesis: a node that fast-syncs from genesis records `U = 0`, so its whole `[0, H)`
            // range is the absent band and every request is served from the index.
            prop_assert_eq!(fast.vct_fast_synced_below(), Some(handoff), "fast-sync marker is set to the handoff height");
            prop_assert_eq!(fast.db.vct_upgrade_height(), Some(Height(0)), "genesis fast sync records the upgrade height at genesis");
            prop_assert!(
                stored_frontier_before_absent_band(&fast.db, Height(0))
                    .expect("a genesis-start database does not need a stored predecessor")
                    .is_none(),
                "U = 0 starts frontier replay from empty genesis trees"
            );
            let genesis_export = export_frontier_grid_to(
                &fast.db,
                handoff,
                GridSpacing::Uniform { blocks: 1 },
                None,
                |_, _| {},
            )
            .expect("a genesis-start VCT database exports its absent band");
            let last_published = genesis_export
                .frontiers
                .entries
                .last()
                .expect("a genesis-start band of this length publishes at least one on-grid entry");
            prop_assert!(
                last_published.height < handoff,
                "published heights stay inside the absent band [0, H)"
            );
            prop_assert_eq!(
                genesis_export.replayed_blocks,
                u64::from(last_published.height.0) + 1,
                "a genesis-start export replays from empty frontiers through the last on-grid entry"
            );

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
            let mut corrupt_handoff = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
                let next = Some(vct_successor_witness(blocks[i + 1].block.clone()));
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
            prop_assert!(fast.db.vct_tree_absent(Height(last as u32 - 1)), "RPC gate: below-handoff treestate is unavailable");
            prop_assert!(!fast.db.vct_tree_absent(handoff), "RPC gate: handoff treestate is available");

            // The read handlers turn that predicate into the typed archive-mode error the
            // RPC boundary reports, carrying the handoff so the failure is diagnosable.
            // Without it, a below-handoff `z_gettreestate` returns a JSON `null`, which
            // lightwalletd-style clients read as the *empty* tree.
            let below_handoff_height = HashOrHeight::Height(Height(last as u32 - 1));
            prop_assert_eq!(
                sapling_tree(None::<Arc<Chain>>, &fast.db, below_handoff_height),
                Err(HistoricalTreeUnavailable {
                    hash_or_height: below_handoff_height,
                    last_checkpoint: handoff,
                }),
                "a below-handoff tree read is a typed archive-mode error, not an absent tree"
            );
            prop_assert!(
                sapling_tree(None::<Arc<Chain>>, &fast.db, HashOrHeight::Height(handoff))
                    .expect("the handoff tree is available")
                    .is_some(),
                "the handoff height itself is served normally"
            );
            // A legacy node never reports the archive-mode error, whatever the height.
            prop_assert!(
                sapling_tree(None::<Arc<Chain>>, &legacy.db, below_handoff_height)
                    .expect("legacy tree reads do not report an absent band")
                    .is_some(),
                "a legacy-synced node has the tree and must not report an absent band"
            );

            // The subtree gate must not fire just because a node is fast-synced. This chain is
            // far too short to complete a subtree, so `z_getsubtreesbyindex` at index 0 is an
            // ordinary "nothing here yet" empty list, exactly as on a legacy node.
            prop_assert!(
                sapling_subtrees(None::<Arc<Chain>>, &fast.db, NoteCommitmentSubtreeIndex(0)..)
                    .expect("an index past the last completed subtree is available")
                    .is_empty(),
                "an index past the last completed subtree stays an empty list, not an error"
            );

            // On-demand derivation rebuilds the absent band from retained block bodies. `U` is 0
            // here, so every derivation replays from empty genesis frontiers — the cold path.
            // Each result is accepted only after reproducing the authenticated root, so agreeing
            // with the legacy node's own per-height trees is what proves the replay is faithful
            // rather than merely self-consistent.
            let cache = Mutex::new(HistoricalTreeCache::default());
            for height in (seed as u32 + 1)..(last as u32) {
                let height = Height(height);
                let derived = derive_historical_frontiers(&fast.db, &cache, height, u64::MAX)
                    .expect("every absent-band height derives from retained bodies");

                prop_assert_eq!(
                    derived.sapling.root(),
                    legacy.db.sapling_tree_by_height(&height).expect("the legacy node stores every tree").root(),
                    "derived Sapling frontier matches the legacy node at {:?}", height
                );
                prop_assert_eq!(
                    derived.orchard.root(),
                    legacy.db.orchard_tree_by_height(&height).expect("the legacy node stores every tree").root(),
                    "derived Orchard frontier matches the legacy node at {:?}", height
                );
                prop_assert_eq!(
                    derived.ironwood.root(),
                    legacy.db.ironwood_tree_by_height(&height).expect("the legacy node stores every tree").root(),
                    "derived Ironwood frontier matches the legacy node at {:?}", height
                );
            }

            // The replay bound is a serving limit: with the cache primed by the loop above, the
            // last height is already derived, so it costs nothing. A cold height below every cache
            // entry still has to replay, and refuses rather than running unbounded.
            prop_assert!(
                derive_historical_frontiers(&fast.db, &cache, Height(last as u32 - 1), 0).is_ok(),
                "a cached height is served without replaying, whatever the bound"
            );
            let cold_cache = Mutex::new(HistoricalTreeCache::default());
            let cold_height = Height(last as u32 - 1);
            prop_assert_eq!(
                derive_historical_frontiers(&fast.db, &cold_cache, cold_height, 1).err(),
                Some(HistoricalTreeDerivationError::ReplayTooLong {
                    height: cold_height,
                    // From empty genesis frontiers, reaching `cold_height` replays every block up
                    // to and including it.
                    blocks: u64::from(cold_height.0) + 1,
                    limit: 1,
                }),
                "a cold derivation past the replay bound refuses instead of running unbounded"
            );


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

            let mut bad_handoff = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
                    .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
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

            let mut bad_ironwood_handoff = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
                    .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
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

            // The subtree audit must authenticate the replay endpoint even when no subtree
            // completes in the range. Otherwise corruption after the last boundary (or, as in
            // this short fixture, before the first boundary) would leave no subtree row to expose
            // the bad replay.
            prop_assert_eq!(
                verify_subtrees_against_stored(&legacy.db, handoff, handoff),
                Err(HistoricalTreeDerivationError::InvalidReplayRange {
                    from: handoff,
                    to: handoff,
                }),
                "an empty subtree replay range is rejected"
            );
            prop_assert_eq!(
                verify_subtrees_against_stored(&legacy.db, handoff, Height(seed as u32)),
                Err(HistoricalTreeDerivationError::InvalidReplayRange {
                    from: handoff,
                    to: Height(seed as u32),
                }),
                "a reversed subtree replay range is rejected"
            );

            let subtree_outcome =
                verify_subtrees_against_stored(&legacy.db, Height(seed as u32), handoff)
                    .expect("the unmodified replay endpoint matches its authenticated roots");
            prop_assert_eq!(
                subtree_outcome,
                Default::default(),
                "the short fixture completes no subtrees"
            );

            let mut corrupted_endpoint = legacy
                .db
                .commitment_roots_by_height_range(handoff..=handoff)
                .into_iter()
                .next()
                .expect("the handoff has an authenticated root row");
            prop_assert_ne!(
                corrupted_endpoint.sapling_root,
                Default::default(),
                "the fixture needs a non-empty Sapling endpoint"
            );
            corrupted_endpoint.sapling_root = Default::default();
            let mut corrupt_endpoint_batch = DiskWriteBatch::new();
            corrupt_endpoint_batch
                .insert_body_derived_commitment_roots(&legacy.db, &corrupted_endpoint);
            legacy
                .db
                .write_batch(corrupt_endpoint_batch)
                .expect("the test corrupts the endpoint root row");

            prop_assert_eq!(
                verify_subtrees_against_stored(&legacy.db, Height(seed as u32), handoff),
                Err(HistoricalTreeDerivationError::RootMismatch { height: handoff }),
                "the subtree audit rejects a replay whose final frontiers are unauthenticated"
            );

            // Re-label the genesis-start fast database as a mid-chain upgrade only after all its
            // other assertions. It has no per-height trees below the handoff, so the shared
            // fallback must reject the missing U - 1 frontier rather than silently replay genesis.
            let missing_anchor_upgrade = Height(1);
            let mut batch = DiskWriteBatch::new();
            batch.delete_sapling_tree(&fast.db, &Height(0));
            batch.update_vct_upgrade_marker(&fast.db, missing_anchor_upgrade);
            fast.db
                .write_batch(batch)
                .expect("simulating a missing pre-band frontier succeeds");
            prop_assert_eq!(
                stored_frontier_before_absent_band(&fast.db, missing_anchor_upgrade).err(),
                Some(HistoricalTreeDerivationError::MissingAnchor {
                    height: missing_anchor_upgrade,
                    anchor: Height(0),
                }),
                "a missing stored U - 1 frontier is rejected"
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
                let mut fast = FinalizedState::new(&fast_config, &network).expect("opening an ephemeral database should succeed");
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
                        .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
                    fast.commit_finalized_direct(cv.into(), None, next, "vct switch fast prefix")
                        .expect("verified fast prefix commits");
                }
                prop_assert_eq!(fast.vct_fast_synced_below(), Some(handoff), "fast sync reached the handoff before the switch");
            }

            let manual_config = Config {
                vct_fast_sync: false,
                ..fast_config
            };
            let mut manual = FinalizedState::new(&manual_config, &network).expect("opening an ephemeral database should succeed");
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
                let mut manual_prefix = FinalizedState::new(&manual_prefix_config, &network).expect("opening an ephemeral database should succeed");
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
            let mut fast_suffix = FinalizedState::new(&fast_suffix_config, &network).expect("opening an ephemeral database should succeed");
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
                    .then(|| vct_successor_witness(blocks[i + 1].block.clone()));
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let cv = CheckpointVerifiedBlock::from(blocks[seed + 2].block.clone());
            let successor = vct_successor_witness(blocks[seed + 3].block.clone());
            fast.commit_finalized_direct(
                cv.into(),
                None,
                Some(successor),
                "vct canonical successor with malformed body available",
            )
            .expect("the authenticated successor preserves the valid current root");
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

            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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

            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
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
            batch.update_vct_sync_marker(&legacy.db, last_height);
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
            let below_upgrade = Height(upgrade.0 - 2);
            let fallback_anchor = Height(upgrade.0 - 1);
            prop_assert_eq!(
                derive_historical_frontiers(
                    &legacy.db,
                    &Mutex::new(HistoricalTreeCache::default()),
                    below_upgrade,
                    u64::MAX,
                )
                .err(),
                Some(HistoricalTreeDerivationError::MissingAnchor {
                    height: below_upgrade,
                    anchor: fallback_anchor,
                }),
                "a database fallback anchor above the target is refused without underflowing"
            );
            prop_assert_eq!(
                derive_historical_frontiers(
                    &legacy.db,
                    &Mutex::new(HistoricalTreeCache::default()),
                    fallback_anchor,
                    u64::MAX,
                )
                .err(),
                Some(HistoricalTreeDerivationError::MissingAuthenticatedRoot {
                    height: fallback_anchor,
                }),
                "a zero-replay database fallback is not served without an authenticated root"
            );
            let (stored_anchor_height, stored_anchor) =
                stored_frontier_before_absent_band(&legacy.db, upgrade)
                    .expect("the pre-upgrade trees provide an export anchor")
                    .expect("a mid-chain upgrade has a stored predecessor");
            prop_assert_eq!(stored_anchor_height, fallback_anchor);
            prop_assert_eq!(
                stored_anchor.sapling.root(),
                legacy
                    .db
                    .latest_stored_sapling_tree(&fallback_anchor)
                    .expect("the pre-upgrade Sapling tree is stored")
                    .root()
            );
            let anchored_export = export_frontier_grid_to(
                &legacy.db,
                last_height,
                GridSpacing::Uniform { blocks: 1 },
                None,
                |_, _| {},
            )
            .expect("a mid-chain VCT database exports from its stored predecessor");
            let last_published = anchored_export
                .frontiers
                .entries
                .last()
                .expect("a grid over this chain publishes at least one on-grid entry");
            prop_assert!(
                last_published.height < last_height,
                "published heights stay below the target checkpoint"
            );
            prop_assert!(
                anchored_export
                    .frontiers
                    .entries
                    .first()
                    .is_some_and(|first| first.height < upgrade),
                "coverage starts at genesis, not at this database's own upgrade height"
            );
            prop_assert_eq!(
                anchored_export.replayed_blocks,
                u64::from(last_published.height.0 - upgrade.0) + 1,
                "only the absent band [U, last on-grid entry] is replayed; below U is read"
            );
            prop_assert_eq!(
                anchored_export.frontiers.last_checkpoint,
                last_height,
                "the artifact records the checkpoint it was generated for, not this database's handoff"
            );
            for entry in &anchored_export.frontiers.entries {
                // Unchanged trees are deduplicated, so the newest row at or below the height is
                // the state there.
                let stored = legacy
                    .db
                    .latest_stored_sapling_tree(&entry.height)
                    .expect("the legacy pass stored a tree at or below every height");
                prop_assert_eq!(
                    entry.sapling.root(),
                    stored.root(),
                    "every published entry reproduces the tree the legacy pass stored"
                );
            }

            // A shorter export is a prefix of a longer one. That is what lets the release
            // pipeline check an incoming grid against the committed one entry by entry.
            let shorter_export = export_frontier_grid_to(
                &legacy.db,
                Height(last_height.0 - 2),
                GridSpacing::Uniform { blocks: 1 },
                None,
                |_, _| {},
            )
            .expect("a lower target exports the same grid, minus its tail");
            prop_assert!(
                shorter_export.frontiers.entries.len() < anchored_export.frontiers.entries.len(),
                "a lower target publishes fewer entries"
            );
            for (earlier, later) in shorter_export
                .frontiers
                .entries
                .iter()
                .zip(&anchored_export.frontiers.entries)
            {
                prop_assert_eq!(earlier.height, later.height, "grid heights do not move");
                prop_assert_eq!(
                    earlier.sapling.root(),
                    later.sapling.root(),
                    "an entry's contents do not change as the target advances"
                );
            }

            // Resuming reproduces the from-genesis walk exactly. That is the property the whole
            // incremental path rests on: the cost accumulator resets at every emitted entry, so
            // continuing at `last carried entry + 1` places the remainder identically.
            let resumed_export = export_frontier_grid_to(
                &legacy.db,
                last_height,
                GridSpacing::Uniform { blocks: 1 },
                Some(&shorter_export.frontiers),
                |_, _| {},
            )
            .expect("a published grid can be carried forward");
            prop_assert_eq!(
                resumed_export.frontiers.entries.len(),
                anchored_export.frontiers.entries.len(),
                "resuming publishes the same entries as a walk from genesis"
            );
            for (from_genesis, resumed) in anchored_export
                .frontiers
                .entries
                .iter()
                .zip(&resumed_export.frontiers.entries)
            {
                prop_assert_eq!(from_genesis.height, resumed.height);
                prop_assert_eq!(from_genesis.sapling.root(), resumed.sapling.root());
                prop_assert_eq!(from_genesis.orchard.root(), resumed.orchard.root());
            }
            prop_assert_eq!(
                resumed_export.frontiers.encode(&network),
                anchored_export.frontiers.encode(&network),
                "a resumed artifact is byte-identical to the one a full walk produces"
            );
            prop_assert!(
                resumed_export.replayed_blocks < anchored_export.replayed_blocks,
                "resuming replays less than a walk from genesis"
            );

            // A grid that already reaches the requested checkpoint has nothing to extend, and
            // carrying entries at or above it would describe heights the export does not cover.
            prop_assert!(
                matches!(
                    export_frontier_grid_to(
                        &legacy.db,
                        Height(2),
                        GridSpacing::Uniform { blocks: 1 },
                        Some(&anchored_export.frontiers),
                        |_, _| {},
                    ),
                    Err(FrontierGridExportError::ResumeAboveTarget { .. })
                ),
                "a grid reaching past the target is refused rather than truncated"
            );

            // One export can draw on every source at once. Narrowing the handoff leaves this
            // database with stored trees on both sides of its absent band: entries below `U` and
            // at or above the handoff are read, and only the band between them is replayed.
            let narrow_handoff = Height(upgrade.0 + 2);
            let mut batch = DiskWriteBatch::new();
            batch.update_vct_sync_marker(&legacy.db, narrow_handoff);
            legacy
                .db
                .write_batch(batch)
                .expect("narrowing the absent band succeeds");
            let mixed_export = export_frontier_grid_to(
                &legacy.db,
                last_height,
                GridSpacing::Uniform { blocks: 1 },
                None,
                |_, _| {},
            )
            .expect("a database with trees on both sides of its band exports every height");
            prop_assert!(
                mixed_export.replayed_blocks
                    <= u64::from(narrow_handoff.0 - upgrade.0),
                "replay is confined to the absent band"
            );
            prop_assert!(
                mixed_export.replayed_blocks < anchored_export.replayed_blocks,
                "a narrower band means less replay for the same coverage"
            );
            prop_assert!(
                mixed_export
                    .frontiers
                    .entries
                    .last()
                    .is_some_and(|entry| entry.height >= narrow_handoff),
                "coverage continues above the handoff, where the trees are stored again"
            );
            for (band_generated, read_generated) in anchored_export
                .frontiers
                .entries
                .iter()
                .zip(&mixed_export.frontiers.entries)
            {
                prop_assert_eq!(band_generated.height, read_generated.height);
                prop_assert_eq!(
                    band_generated.sapling.root(),
                    read_generated.sapling.root(),
                    "a replayed entry and a read entry at the same height are the same frontier"
                );
            }

            // Restore the fixture's handoff for the assertions that follow.
            let mut batch = DiskWriteBatch::new();
            batch.update_vct_sync_marker(&legacy.db, last_height);
            legacy
                .db
                .write_batch(batch)
                .expect("restoring the fixture handoff succeeds");
            let stitched = serve_block_roots(&legacy.db, serve_range);
            prop_assert_eq!(
                stitched,
                all_trees_reference,
                "serve_block_roots stitches the trees below U with the index at/above U into one gap-free run"
            );

            // Rollback does not move the write-once upgrade marker. Model that stale metadata by
            // moving it above the current tip: a backwards lookup must not relabel the tip tree as
            // the marker's `U - 1` fallback anchor.
            let stale_upgrade = Height(last_height.0 + 2);
            let stale_anchor = Height(stale_upgrade.0 - 1);
            let mut batch = DiskWriteBatch::new();
            batch.update_vct_upgrade_marker(&legacy.db, stale_upgrade);
            legacy
                .db
                .write_batch(batch)
                .expect("simulating a stale post-rollback upgrade marker succeeds");
            prop_assert_eq!(
                stored_frontier_before_absent_band(&legacy.db, stale_anchor).err(),
                Some(HistoricalTreeDerivationError::MissingAnchor {
                    height: stale_anchor,
                    anchor: stale_anchor,
                }),
                "the shared stored-tree anchor rejects a stale upgrade marker"
            );
            prop_assert_eq!(
                derive_historical_frontiers(
                    &legacy.db,
                    &Mutex::new(HistoricalTreeCache::default()),
                    stale_anchor,
                    u64::MAX,
                )
                .err(),
                Some(HistoricalTreeDerivationError::MissingAnchor {
                    height: stale_anchor,
                    anchor: stale_anchor,
                }),
                "a stale upgrade marker cannot relabel a retained tree above the finalized tip"
            );
    });

    Ok(())
}

/// An untrusted VCT root fixture drives the fast path to byte-identical consensus state.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i+1] (the look-ahead) and by height
fn vct_untrusted_fixture_drives_byte_identical_state() -> Result<()> {
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
            // The untrusted source defers any fast block whose own root has no buffered
            // successor, so every committed fast block needs `blocks[i + 1]`. Keep `last` one
            // below the chain tip so the deepest commit still has a successor witness.
            let last = ((nu5 + 3) as usize).min(blocks.len().saturating_sub(2));
            prop_assert!(last > (nu5 as usize), "generated chain unexpectedly short");
            let seed = (heartwood - 1) as usize;

            // Legacy/archive pass: a real DB with per-height trees, plus the golden state.
            let mut legacy = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");
            for block in blocks.iter().take(last + 1) {
                let cv = CheckpointVerifiedBlock::from(block.block.clone());
                legacy
                    .commit_finalized_direct(cv.into(), None, None, "vct fixture legacy")
                    .unwrap();
            }
            let golden_anchors = legacy.db.vct_anchor_digest();
            let golden_history = legacy.db.history_tree().hash();

            // Produce the payload from the legacy DB (the serving read path).
            let produced_roots = commitment_aux::produce_block_roots(
                &legacy.db,
                Height((seed + 1) as u32)..=Height(last as u32),
            );

            // Consume the untrusted roots in a fresh fast-sync state.
            // The test buffers each fast block's successor before commit, as the write loop does.
            // `vct_peer_source_defers_unverifiable_tip_root_until_successor` covers the tip case.
            let mut fast = FinalizedState::new(&Config::ephemeral(), &network).expect("opening an ephemeral database should succeed");

            let roots = produced_roots
                .into_iter()
                .map(|roots| {
                    (
                        roots.height.0,
                        (roots.sapling_root, roots.orchard_root, roots.ironwood_root),
                    )
                })
                .collect();
            let source = commitment_aux::FixtureSource::new(
                roots,
                test_handoff_frontiers(Height::MAX),
            );
            fast.enable_vct_fast_source(Box::new(source), true);
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = next_vct_block(blocks[i + 1].block.clone());
                fast.commit_finalized_direct(cv.into(), None, next, "vct fixture fast")
                    .expect("verified fast commit from untrusted roots succeeds");
            }

            prop_assert_eq!(fast.db.vct_anchor_digest(), golden_anchors, "fast anchors from fixture roots match legacy");
            prop_assert_eq!(fast.db.history_tree().hash(), golden_history, "fast history from fixture roots match legacy");
    });

    Ok(())
}

/// Builds a [`crate::ReadStateService`] over `finalized_state`, so tests can exercise the real
/// read handlers rather than the helpers underneath them.
///
/// The config gate, the artifact load, and the typed-error fallback all live in the handler, so
/// testing only the helpers would leave the seam that actually serves `z_gettreestate` unproven.
fn read_service_over(finalized_state: &FinalizedState) -> crate::ReadStateService {
    use zakura_node_services::sync_lifecycle::{
        HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
    };

    use crate::service::{
        non_finalized_state::NonFinalizedState, watch_receiver::WatchReceiver,
        HeaderChainSubscriptions, ReadStateService, VctRootRepairStatus,
    };

    let (_non_finalized_sender, non_finalized_receiver) =
        tokio::sync::watch::channel(NonFinalizedState::new(&finalized_state.network()));
    let (_repair_sender, repair_receiver) =
        tokio::sync::watch::channel(VctRootRepairStatus::default());
    let (_header_chain_snapshot_sender, header_chain_snapshot_receiver) =
        tokio::sync::watch::channel(None);
    let (_header_chain_view_sender, header_chain_view_receiver) = tokio::sync::watch::channel(None);
    let (_header_runtime_status_sender, header_runtime_status_receiver) =
        tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
        });
    let (_header_chain_reader_sender, header_chain_reader_receiver) =
        tokio::sync::watch::channel(None);

    ReadStateService::new(
        finalized_state,
        None,
        Arc::new(OnceLock::new()),
        WatchReceiver::new(non_finalized_receiver),
        repair_receiver,
        HeaderChainSubscriptions {
            snapshots: header_chain_snapshot_receiver,
            views: header_chain_view_receiver,
            runtime_status: header_runtime_status_receiver,
            reader: header_chain_reader_receiver,
        },
        crate::service::load_historical_frontier_artifact(
            &finalized_state.network(),
            finalized_state.db.config(),
            finalized_state.db.vct_synced_below().is_some(),
        )
        .expect("the test historical frontier artifact loads")
        .discard_if_before_vct_handoff(finalized_state.db.config(), &finalized_state.db),
    )
}

/// Writes a genesis-empty frontier grid, the anchor a deriving node needs before it will serve
/// the absent band at all.
///
/// Height 0 is empty on this synthetic chain, so the entry root-checks and a below-handoff
/// request still has to replay. The file must outlive the [`FinalizedState`] that points at it.
fn write_genesis_frontier_artifact(
    network: &zakura_chain::parameters::Network,
    last_checkpoint: Height,
) -> tempfile::NamedTempFile {
    let artifact = FrontierArtifact {
        spacing: 1,
        last_checkpoint,
        entries: vec![FrontierEntry {
            height: Height(0),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }],
    };
    let file = tempfile::NamedTempFile::new().expect("a temporary artifact file is created");
    fs::write(file.path(), artifact.encode(network)).expect("the genesis frontier artifact writes");
    file
}

/// The read service must serve absent-band treestates when derivation is enabled, and report the
/// typed archive-mode error when it is not, including when the node is pruned.
///
/// This exercises the handler itself — config gating, derivation, the pruned-mode short-circuit,
/// and the fallback to [`HistoricalTreeUnavailable`] — rather than the helpers underneath it,
/// because that handler is what a wallet's `z_gettreestate` actually reaches.
#[test]
#[allow(clippy::needless_range_loop)] // the loops index blocks[i + 1] for the successor witness
fn vct_read_service_serves_or_refuses_absent_band_treestates() -> Result<()> {
    use tower::ServiceExt;

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
    let nu5_height = NetworkUpgrade::Nu5
        .activation_height(&network)
        .expect("NU5 activation height is configured");
    let tested_block_count =
        usize::try_from(nu5_height.0 + 4).expect("test activation height fits in usize");
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), None::<NetworkUpgrade>, None, false);

    proptest!(ProptestConfig::with_cases(1),
        |((blocks, network) in super::valid_commitment_chain(ledger_strategy.clone(), tested_block_count).no_shrink())| {

        let nu5 = NetworkUpgrade::Nu5.activation_height(&network).unwrap().0;
        let heartwood = NetworkUpgrade::Heartwood.activation_height(&network).unwrap().0;
        let last = (nu5 + 3) as usize;
        prop_assert!(blocks.len() > last, "generated chain unexpectedly short");
        let handoff = Height(last as u32);
        let seed = (heartwood - 1) as usize;

        // Legacy pass: the per-block fixture, and the golden trees to compare against.
        let mut legacy = FinalizedState::new(&Config::ephemeral(), &network)
            .expect("opening an ephemeral database should succeed");
        let mut fixture = TestRootMap::new();
        let mut handoff_trees = None;
        for i in 0..=last {
            let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
            let (_h, trees) = legacy
                .commit_finalized_direct(cv.into(), None, None, "vct legacy")
                .unwrap();
            if i > seed {
                fixture.insert(
                    i as u32,
                    (trees.sapling.root(), trees.orchard.root(), trees.ironwood.root()),
                );
            }
            if i == last {
                handoff_trees = Some(trees);
            }
        }
        let handoff_trees = handoff_trees.expect("the handoff produced trees");

        let commit_fast = |config: Config| {
            let mut fast = FinalizedState::new(&config, &network)
                .expect("opening an ephemeral database should succeed");
            enable_vct_test_fixture_source_with_handoff(
                &mut fast,
                fixture.clone(),
                handoff,
                handoff_trees.sapling.clone(),
                handoff_trees.orchard.clone(),
                handoff_trees.sprout.clone(),
                handoff_trees.ironwood.clone(),
            );
            for i in 0..=last {
                let cv = CheckpointVerifiedBlock::from(blocks[i].block.clone());
                let next = (i < last).then(|| vct_successor_witness(blocks[i + 1].block.clone()));
                fast.commit_finalized_direct(cv.into(), None, next, "vct fast")
                    .expect("verified fast commit succeeds");
            }
            fast
        };

        let probe = Height(last as u32 - 1);
        let runtime = tokio::runtime::Runtime::new().expect("a test runtime starts");

        // An archive node on the VCT fast path derives: the handler serves trees matching the
        // legacy node's. A genesis-empty grid is enough to anchor on, and still leaves the probe
        // as a replay rather than a zero-cost hit.
        let artifact_file = write_genesis_frontier_artifact(&network, handoff);
        let deriving = Config {
            historical_frontier_artifact: Some(artifact_file.path().to_path_buf()),
            ..Config::ephemeral()
        };
        let fast = commit_fast(deriving);
        prop_assert!(fast.db.vct_tree_absent(probe), "the probe height is in the absent band");

        let read_state = read_service_over(&fast);
        let sapling = runtime
            .block_on(read_state.clone().oneshot(ReadRequest::SaplingTree(probe.into())))
            .expect("the read service answers");
        let ReadResponse::SaplingTree(Some(tree)) = sapling else {
            panic!("derivation is enabled, so the absent band must be served, got {sapling:?}")
        };
        prop_assert_eq!(
            tree.root(),
            legacy.db.sapling_tree_by_height(&probe).expect("legacy stores every tree").root(),
            "the served Sapling treestate matches the legacy node"
        );

        let orchard = runtime
            .block_on(read_state.oneshot(ReadRequest::OrchardTree(probe.into())))
            .expect("the read service answers");
        let ReadResponse::OrchardTree(Some(tree)) = orchard else {
            panic!("derivation is enabled, so the absent band must be served, got {orchard:?}")
        };
        prop_assert_eq!(
            tree.root(),
            legacy.db.orchard_tree_by_height(&probe).expect("legacy stores every tree").root(),
            "the served Orchard treestate matches the legacy node"
        );

        // Pruned mode, grid and all: bodies in the retention window may still be present on this
        // short chain, but a pruned node cannot serve historical treestates. Fail with the typed
        // archive-mode error rather than walking the replay until a missing body.
        let pruned = Config {
            historical_frontier_artifact: Some(artifact_file.path().to_path_buf()),
            storage_mode: StorageMode::Pruned(PruningConfig::default()),
            ..Config::ephemeral()
        };
        let fast = commit_fast(pruned);
        let read_state = read_service_over(&fast);
        let sapling_error = runtime
            .block_on(read_state.clone().oneshot(ReadRequest::SaplingTree(probe.into())))
            .expect_err("pruned mode must not derive historical treestates");
        prop_assert!(
            sapling_error.to_string().contains("fast-synced"),
            "pruned mode reports the typed archive-mode error, got: {sapling_error}"
        );
        prop_assert!(
            !sapling_error.to_string().contains("not retained"),
            "pruned mode must not surface a per-height missing-body replay failure, got: {sapling_error}"
        );
        let orchard_error = runtime
            .block_on(read_state.oneshot(ReadRequest::OrchardTree(probe.into())))
            .expect_err("pruned mode must not derive historical treestates");
        prop_assert!(
            orchard_error.to_string().contains("fast-synced"),
            "pruned mode reports the typed archive-mode error, got: {orchard_error}"
        );

    });

    Ok(())
}
