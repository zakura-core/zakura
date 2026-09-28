//! Compare hinted construction with ordinary state and exercise durable recovery.

use std::{collections::HashMap, fs, sync::Arc};

use zakura_chain::{
    block::{Block, Height},
    parameters::{
        spentness_hints::{Commitment, Encoder, Mode, ParsedArtifact},
        Network,
    },
    serialization::ZcashDeserializeInto,
};

use super::super::{
    commitment_aux::{self, FinalFrontiers, FixtureSource},
    disk_format::{IntoDisk, OutputLocation, RawBytes},
    zakura_db::spentness::{
        Progress, ReleaseAuthority, SpentnessConfig, SpentnessSetup, METADATA, OMITTED_OUTPUTS,
    },
    CheckpointVerifiedBlock, FinalizedState,
};
use crate::{service::non_finalized_state::CreatedUtxos, Config};

struct Fixture {
    _directory: tempfile::TempDir,
    config: Config,
    spentness: SpentnessSetup,
    /// The commitment for the fixture artifact.
    commitment: Commitment,
    /// Serialized VCT frontiers at the fixture's terminal height.
    frontiers: Vec<u8>,
    ordinary: FinalizedState,
    blocks: Vec<Arc<Block>>,
    network: Network,
}

impl Fixture {
    fn new(omit_survivor: bool) -> Self {
        let blocks: Vec<Arc<Block>> = zakura_test::vectors::MAINNET_BLOCKS
            .iter()
            .filter(|(height, _)| **height <= 10)
            .map(|(_, bytes)| Arc::new(bytes.zcash_deserialize_into::<Block>().unwrap()))
            .collect();
        assert_eq!(blocks.len(), 11);
        Self::from_blocks(
            blocks,
            Network::Mainnet,
            omit_survivor.then(|| OutputLocation::from_usize(Height(1), 0, 0)),
        )
    }

    /// Build a fixture whose artifact flips the membership bit at `flipped`, if set.
    fn from_blocks(
        blocks: Vec<Arc<Block>>,
        network: Network,
        flipped: Option<OutputLocation>,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let terminal = Height((blocks.len() - 1).try_into().unwrap());
        let mut ordinary = FinalizedState::new(
            &Config {
                vct_fast_sync: false,
                ..Config::ephemeral()
            },
            &network,
        )
        .unwrap();
        for block in &blocks {
            ordinary
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "spentness oracle",
                )
                .unwrap();
        }
        let mut encoder = Encoder::new(
            blocks[0].hash().0,
            terminal.0,
            blocks.last().unwrap().hash().0,
        );
        for (height, block) in blocks.iter().enumerate() {
            for (tx_index, transaction) in block.transactions.iter().enumerate() {
                for (output_index, _) in transaction.outputs().iter().enumerate() {
                    let location = OutputLocation::from_usize(
                        Height(height.try_into().unwrap()),
                        tx_index,
                        output_index,
                    );
                    let survives = ordinary.db.utxo_by_location(location).is_some();
                    encoder
                        .push(survives != (flipped == Some(location)))
                        .unwrap();
                }
            }
        }
        let bytes = encoder.finish();
        let commitment = ParsedArtifact::read(bytes.as_slice())
            .unwrap()
            .commitment()
            .clone();
        let path = directory.path().join("artifact.bin");
        fs::write(&path, bytes).unwrap();
        let frontiers =
            commitment_aux::produce_final_frontiers_bytes(&ordinary.db, terminal).unwrap();
        // The fixture stops VCT sync below the Mainnet checkpoint handoff.
        // A writable open resumes that sync only on a native P2P stack.
        let config = Config {
            cache_dir: directory.path().join("state"),
            enable_zakura_header_seed_from_committed_blocks: true,
            ..Config::default()
        };
        let mut fixture = Self {
            _directory: directory,
            config,
            spentness: SpentnessSetup::ordinary(&network),
            commitment,
            frontiers,
            ordinary,
            blocks,
            network,
        };
        fixture.spentness = fixture.setup(vec![fixture.commitment.clone()], Vec::new());
        fixture.spentness.config = SpentnessConfig {
            mode: Mode::Require,
            artifact: Some(path),
        };
        fixture
    }

    /// Settings that trust `commitments`, keep the fixture frontiers, and revoke `revoked`.
    fn setup(&self, commitments: Vec<Commitment>, revoked: Vec<[u8; 32]>) -> SpentnessSetup {
        let authority = ReleaseAuthority::new(
            &self.network,
            commitments,
            revoked,
            vec![(self.commitment.sha256, self.frontiers.clone())],
        );
        SpentnessSetup {
            config: self.spentness.config.clone(),
            authority: Arc::new(authority),
        }
    }

    fn open(&self) -> FinalizedState {
        self.open_with_storage_validation(true)
    }

    /// Open the fixture state. Without validation, tests can use a short pruning window.
    fn open_with_storage_validation(&self, validate_storage_mode: bool) -> FinalizedState {
        let mut state = FinalizedState::new_with_debug_and_storage_validation(
            &self.config,
            &self.network,
            false,
            false,
            validate_storage_mode,
            true,
            self.spentness.clone(),
        )
        .unwrap();
        if !matches!(
            state.db.spentness_progress().unwrap(),
            Some(Progress::Complete { .. })
        ) {
            let roots: HashMap<_, _> = commitment_aux::produce_block_roots(
                &self.ordinary.db,
                Height(1)..=Height(self.commitment.terminal_height),
            )
            .into_iter()
            .map(|root| {
                (
                    root.height.0,
                    (root.sapling_root, root.orchard_root, root.ironwood_root),
                )
            })
            .collect();
            state.enable_vct_fast_source(
                Box::new(FixtureSource::new(
                    roots,
                    FinalFrontiers::from_bytes(&self.frontiers).unwrap(),
                )),
                false,
            );
        }
        state
    }

    /// Commit every fixture block through H.
    fn commit_all(&self, state: &mut FinalizedState) {
        self.commit(state, 0..=self.blocks.len() - 1);
    }

    fn commit(&self, state: &mut FinalizedState, heights: std::ops::RangeInclusive<usize>) {
        for height in heights {
            state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(self.blocks[height].clone()).into(),
                    None,
                    None,
                    "spentness construction",
                )
                .unwrap();
        }
    }
}

fn entries(state: &FinalizedState, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let cf = state.db.cf_handle(name).unwrap();
    state
        .db
        .zs_forward_range_iter::<_, RawBytes, RawBytes, _>(&cf, ..)
        .map(|(key, value)| (key.as_bytes(), value.as_bytes()))
        .collect()
}

/// Every column family must match, except the progress record and the VCT sync marker.
fn assert_matches_ordinary(state: &FinalizedState, ordinary: &FinalizedState) {
    for name in super::super::STATE_COLUMN_FAMILIES_IN_CODE {
        if [METADATA, super::super::VCT_SYNC_METADATA].contains(name) {
            continue;
        }
        assert_eq!(entries(state, name), entries(ordinary, name), "{name}");
    }
}

/// An ordinary pruned state that skips checkpoint bodies, like a fresh pruned sync.
fn pruned_ordinary(
    blocks: &[Arc<Block>],
    network: &Network,
) -> (tempfile::TempDir, FinalizedState) {
    let directory = tempfile::tempdir().unwrap();
    let config = pruned_config(Config {
        cache_dir: directory.path().to_path_buf(),
        vct_fast_sync: false,
        ..Config::default()
    });
    let mut state = FinalizedState::new(&config, network)
        .unwrap()
        .with_checkpoint_raw_tx_retention(PRUNED_CHECKPOINT_TARGET, &config);
    for block in blocks {
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "pruned oracle",
            )
            .unwrap();
    }
    (directory, state)
}

/// A checkpoint far above the fixtures, so pruned sync skips every checkpoint body.
const PRUNED_CHECKPOINT_TARGET: Height = Height(20_000);

fn pruned_config(config: Config) -> Config {
    Config {
        storage_mode: crate::StorageMode::Pruned(crate::PruningConfig::default()),
        ..config
    }
}

/// Hinted construction writes the same state as ordinary sync, in archive and pruned modes.
///
/// A restart empties the in-memory map, and block 102's 70 outputs overflow the test
/// capacity, so later spends resolve from the journal.
#[test]
fn spentness_matches_ordinary_state_across_restarts() {
    let _guard = zakura_test::init();
    for pruned in [false, true] {
        let (blocks, network) = generated_transparent_chain();
        let mut fixture = Fixture::from_blocks(blocks, network, None);
        let pruned_oracle = pruned.then(|| {
            fixture.config = pruned_config(fixture.config.clone());
            pruned_ordinary(&fixture.blocks, &fixture.network)
        });
        let open = |fixture: &Fixture| {
            fixture
                .open()
                .with_checkpoint_raw_tx_retention(PRUNED_CHECKPOINT_TARGET, &fixture.config)
        };
        let mut state = open(&fixture);
        fixture.commit(&mut state, 0..=100);
        assert!(matches!(
            state.db.spentness_progress().unwrap(),
            Some(Progress::Applying { height: 100, .. })
        ));
        drop(state);
        let mut state = open(&fixture);
        fixture.commit(&mut state, 101..=GENERATED_TIP as usize);
        assert!(!state.db.spentness_incomplete());
        assert!(entries(&state, OMITTED_OUTPUTS).is_empty());
        for height in 1..=GENERATED_TIP {
            assert_eq!(
                state.db.contains_body_at_height(Height(height)),
                !pruned,
                "{height}"
            );
        }
        let (_oracle_directory, mut ordinary) = match pruned_oracle {
            Some((directory, oracle)) => (Some(directory), oracle),
            None => (None, fixture.ordinary),
        };
        assert_matches_ordinary(&state, &ordinary);

        // Ordinary commits and pruning continue above H.
        let next = synthetic_block(
            fixture.blocks.last().unwrap(),
            GENERATED_TIP + 1,
            Vec::new(),
        );
        for target in [&mut state, &mut ordinary] {
            target
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(next.clone()).into(),
                    None,
                    None,
                    "above the handoff",
                )
                .unwrap();
        }
        assert_matches_ordinary(&state, &ordinary);
    }
}

#[test]
fn spentness_matches_mainnet_blocks_and_resumes_applying() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=4);
    assert!(matches!(
        state.db.spentness_progress().unwrap(),
        Some(Progress::Applying { height: 4, .. })
    ));
    drop(state);
    let mut state = fixture.open();
    fixture.commit(&mut state, 5..=10);
    assert!(matches!(
        state.db.spentness_progress().unwrap(),
        Some(Progress::Complete {
            rollback_floor: 10,
            ..
        })
    ));
    assert_matches_ordinary(&state, &fixture.ordinary);
    drop(state);
    assert!(!fixture.open().db.spentness_incomplete());
}

/// Commit blocks until one fails, and return the spentness failure.
fn first_failure(fixture: &Fixture, state: &mut FinalizedState) -> String {
    for block in &fixture.blocks {
        if let Err(error) = state.commit_finalized_direct(
            CheckpointVerifiedBlock::from(block.clone()).into(),
            None,
            None,
            "spentness construction",
        ) {
            assert!(state.db.spentness_incomplete());
            return error
                .spentness_failure()
                .expect("construction reports its own failures")
                .to_owned();
        }
    }
    panic!("a false artifact must never complete construction");
}

#[test]
fn spentness_rejects_an_omitted_survivor_at_h() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(true);
    let mut state = fixture.open();
    let error = first_failure(&fixture, &mut state);
    assert!(error.to_string().contains("omits outputs"), "{error}");
    assert_eq!(state.db.finalized_tip_height(), Some(Height(9)));
}

#[test]
fn spentness_rejects_zero_value_non_address_omission() {
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    let fixture = Fixture::from_blocks(
        blocks,
        network,
        Some(OutputLocation::from_usize(Height(1), 0, 1)),
    );
    let mut state = fixture.open();
    let error = first_failure(&fixture, &mut state);
    assert!(error.to_string().contains("omits outputs"), "{error}");
    assert_eq!(
        state.db.finalized_tip_height(),
        Some(Height(GENERATED_TIP - 1))
    );
}

#[test]
fn spentness_rejects_a_retained_spent_output() {
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    // Block 101 spends this coinbase output, but the artifact retains it.
    let fixture = Fixture::from_blocks(
        blocks,
        network,
        Some(OutputLocation::from_usize(Height(1), 0, 0)),
    );
    let mut state = fixture.open();
    let error = first_failure(&fixture, &mut state);
    assert!(error.to_string().contains("artifact retains"), "{error}");
    assert!(error.to_string().contains("delete the state"), "{error}");
    assert_eq!(state.db.finalized_tip_height(), Some(Height(100)));
}

#[test]
fn spentness_rejects_a_shifted_cursor() {
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    let fixture = Fixture::from_blocks(blocks, network, None);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=50);
    let mut progress = state.db.spentness_progress().unwrap().unwrap();
    if let Progress::Applying { next_ordinal, .. } = &mut progress {
        *next_ordinal += 1;
    }
    let mut batch = crate::DiskWriteBatch::new();
    batch
        .prepare_spentness_progress(&state.db, progress)
        .unwrap();
    state.db.write_batch(batch).unwrap();
    drop(state);
    let mut state = fixture.open();
    let mut failed = false;
    for block in &fixture.blocks[51..] {
        if state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "shifted cursor",
            )
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed, "a shifted cursor must never complete construction");
    assert!(state.db.spentness_incomplete());
}

#[test]
fn spentness_reconciles_failed_batch_outcomes() {
    let _guard = zakura_test::init();
    for failure_height in [4, 10] {
        for durable in [false, true] {
            let fixture = Fixture::new(false);
            let mut state = fixture.open();
            fixture.commit(&mut state, 0..=failure_height - 1);
            let result = state.commit_finalized_direct_with(
                CheckpointVerifiedBlock::from(fixture.blocks[failure_height].clone()).into(),
                None,
                None,
                "injected batch failure",
                |db, batch, _proof| {
                    if durable {
                        db.write_batch(batch).unwrap();
                    }
                    Err(
                        crate::SpentnessError::Inconsistent("injected uncertain batch outcome")
                            .into(),
                    )
                },
            );
            assert!(result.is_err());
            assert_eq!(
                state.db.finalized_tip_height(),
                Some(Height(
                    (failure_height - usize::from(!durable)).try_into().unwrap()
                ))
            );
            assert_eq!(state.db.spentness_status(), crate::SpentnessStatus::Failed);
            drop(state);
            let mut state = fixture.open();
            let next = failure_height + usize::from(durable);
            if next <= 10 {
                fixture.commit(&mut state, next..=10);
            }
            assert!(!state.db.spentness_incomplete());
            assert_matches_ordinary(&state, &fixture.ordinary);
        }
    }
}

#[test]
fn spentness_completed_state_needs_no_artifact_and_keeps_rollback_floor() {
    let _guard = zakura_test::init();
    let mut fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit_all(&mut state);
    drop(state);
    fs::remove_file(fixture.spentness.config.artifact.take().unwrap()).unwrap();
    fs::remove_file(crate::artifact_cache_path(
        &fixture.config,
        &fixture.commitment,
    ))
    .unwrap();
    let state = fixture.open();
    assert!(!state.db.spentness_incomplete());
    drop(state);
    for preview in [true, false] {
        let options = crate::RollbackFinalizedStateOptions {
            target_height: Height(9),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        };
        let result = if preview {
            crate::preview_rollback_finalized_state(
                fixture.config.clone(),
                &Network::Mainnet,
                options,
            )
        } else {
            crate::rollback_finalized_state(fixture.config.clone(), &Network::Mainnet, options)
        };
        assert!(
            result.is_err(),
            "rollback must reject crossing the completed handoff"
        );
        assert_eq!(fixture.open().db.finalized_tip_height(), Some(Height(10)));
    }
}

#[test]
fn spentness_completed_state_opens_after_release_drops_commitment() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit_all(&mut state);
    drop(state);
    // The compiled Mainnet release lists no commitments.
    let config = SpentnessConfig {
        mode: Mode::Require,
        artifact: None,
    };
    assert!(
        crate::spentness_artifact_requirement(&fixture.config, &config, &fixture.network)
            .unwrap()
            .is_none()
    );
    let dropped = fixture.setup(Vec::new(), Vec::new());
    let state =
        FinalizedState::new_with_spentness(&fixture.config, &fixture.network, dropped).unwrap();
    assert!(!state.db.spentness_incomplete());
    drop(state);
    let revoked = fixture.setup(Vec::new(), vec![fixture.commitment.sha256]);
    let error = FinalizedState::new_with_spentness(&fixture.config, &fixture.network, revoked)
        .err()
        .unwrap();
    assert!(error.to_string().contains("revoked"), "{error}");
}

#[test]
fn spentness_rollback_reaches_the_completed_handoff_from_above() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit_all(&mut state);
    let transparent = [
        "utxo_by_out_loc",
        "utxo_loc_by_transparent_addr_loc",
        "balance_by_transparent_addr",
        "tx_loc_by_transparent_addr_loc",
        "tip_chain_value_pool",
    ];
    let at_handoff: Vec<_> = transparent
        .iter()
        .map(|name| entries(&state, name))
        .collect();
    let mut previous = fixture.blocks.last().unwrap().clone();
    for height in 11..=12 {
        let next = synthetic_block(&previous, height, Vec::new());
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(next.clone()).into(),
                None,
                None,
                "blocks above the handoff",
            )
            .unwrap();
        previous = next;
    }
    drop(state);
    let options = crate::RollbackFinalizedStateOptions {
        target_height: Height(10),
        keep_rolled_back_blocks: false,
        max_checkpoint_height: None,
    };
    crate::rollback_finalized_state(fixture.config.clone(), &Network::Mainnet, options).unwrap();
    let state = fixture.open();
    assert_eq!(state.db.finalized_tip_height(), Some(Height(10)));
    assert!(!state.db.spentness_incomplete());
    for (name, expected) in transparent.iter().zip(at_handoff) {
        assert_eq!(entries(&state, name), expected, "{name}");
    }
}

#[test]
fn spentness_incomplete_open_guards_preserve_original_commitment() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=4);
    drop(state);
    let off = fixture.config.clone();
    assert!(FinalizedState::new(&off, &Network::Mainnet).is_err());
    assert!(
        FinalizedState::new_with_debug(&fixture.config, &Network::Mainnet, true, true).is_err()
    );
    let unknown = SpentnessSetup::new(
        SpentnessConfig {
            mode: Mode::Require,
            artifact: None,
        },
        &Network::Mainnet,
    );
    assert!(
        FinalizedState::new_with_spentness(&fixture.config, &Network::Mainnet, unknown).is_err()
    );
    let revoked = fixture.setup(
        vec![fixture.commitment.clone()],
        vec![fixture.commitment.sha256],
    );
    assert!(
        FinalizedState::new_with_spentness(&fixture.config, &Network::Mainnet, revoked)
            .err()
            .unwrap()
            .to_string()
            .contains("revoked")
    );
    assert!(crate::init_read_only(fixture.config.clone(), &Network::Mainnet).is_err());
    // Older binaries look for the previous major version, which construction never writes.
    assert!(crate::config::database_format_version_on_disk(
        &fixture.config,
        crate::constants::STATE_DATABASE_KIND,
        crate::constants::state_database_format_version_in_code().major - 1,
        &Network::Mainnet
    )
    .unwrap()
    .is_none());
    let mut missing = fixture.spentness.clone();
    missing.config.artifact = Some(fixture._directory.path().join("missing.bin"));
    let error = FinalizedState::new_with_spentness(&fixture.config, &Network::Mainnet, missing)
        .err()
        .unwrap();
    assert!(error.to_string().contains(&fixture.commitment.digest_hex()));
    let mut newer = fixture.commitment.clone();
    newer.terminal_height += 1;
    newer.sha256[0] ^= 1;
    let newer_release = fixture.setup(vec![fixture.commitment.clone(), newer], Vec::new());
    let state =
        FinalizedState::new_with_spentness(&fixture.config, &Network::Mainnet, newer_release)
            .unwrap();
    assert_eq!(
        state.db.spentness_progress().unwrap().unwrap().commitment(),
        &fixture.commitment
    );
    drop(state);
    for preview in [true, false] {
        let options = crate::PruneFinalizedStateOptions {
            tx_retention: crate::constants::MIN_PRUNING_RETENTION,
        };
        let result = if preview {
            crate::preview_prune_finalized_state(fixture.config.clone(), &Network::Mainnet, options)
        } else {
            crate::prune_finalized_state(fixture.config.clone(), &Network::Mainnet, options)
        };
        assert!(result.is_err());
    }
    assert_eq!(fixture.open().db.finalized_tip_height(), Some(Height(4)));
}

/// Construction keeps balances, address transactions, and pools exact, but the UTXO set
/// holds only survivors until H.
#[tokio::test(flavor = "multi_thread")]
async fn spentness_read_service_gates_only_the_utxo_set() {
    use tower::ServiceExt;
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=4);
    let reader = super::prop::read_service_over(&state);
    let outpoint = zakura_chain::transparent::OutPoint {
        hash: fixture.blocks[1].transactions[0].hash(),
        index: 0,
    };
    for request in [
        crate::ReadRequest::UnspentBestChainUtxo(outpoint),
        crate::ReadRequest::UtxosByAddresses(Default::default()),
        crate::ReadRequest::IsTransparentOutputSpent(outpoint),
    ] {
        let error = reader.clone().oneshot(request).await.unwrap_err();
        assert!(error.to_string().contains("incomplete"), "{error}");
    }
    for request in [
        crate::ReadRequest::BlockHeader(Height(4).into()),
        crate::ReadRequest::AddressBalance(Default::default()),
        crate::ReadRequest::TipPoolValues,
        crate::ReadRequest::BlockInfo(Height(4).into()),
    ] {
        reader.clone().oneshot(request).await.unwrap();
    }
    fixture.commit(&mut state, 5..=10);
    reader
        .oneshot(crate::ReadRequest::UnspentBestChainUtxo(outpoint))
        .await
        .unwrap();
}

fn synthetic_block(
    previous: &Block,
    height: u32,
    transactions: Vec<Arc<zakura_chain::transaction::Transaction>>,
) -> Arc<Block> {
    use zakura_chain::{
        amount::Amount,
        transaction::{LockTime, Transaction},
        transparent::{Input, Output, Script},
    };
    let script = zakura_chain::transparent::Address::from_pub_key_hash(
        zakura_chain::parameters::NetworkKind::Testnet,
        [7; 20],
    )
    .script();
    let coinbase = Arc::new(Transaction::V1 {
        inputs: vec![Input::Coinbase {
            height: Height(height),
            data: vec![0, 0],
            sequence: u32::MAX,
        }],
        outputs: vec![
            Output {
                value: Amount::try_from(100u64).unwrap(),
                lock_script: script,
            },
            Output {
                value: Amount::zero(),
                lock_script: Script::new(&[0x6a]),
            },
            Output {
                value: Amount::try_from(5u64).unwrap(),
                lock_script: Script::new(&[0x51]),
            },
        ],
        lock_time: LockTime::Height(Height(0)),
    });
    let mut block = previous.clone();
    block.transactions = std::iter::once(coinbase).chain(transactions).collect();
    let header = Arc::make_mut(&mut block.header);
    header.previous_block_hash = previous.hash();
    header.merkle_root = block.transactions.iter().collect();
    Arc::new(block)
}

fn spending_transaction(
    outpoint: zakura_chain::transparent::OutPoint,
    output: zakura_chain::transparent::Output,
) -> Arc<zakura_chain::transaction::Transaction> {
    transparent_transaction(vec![outpoint], vec![output])
}

fn transparent_transaction(
    outpoints: Vec<zakura_chain::transparent::OutPoint>,
    outputs: Vec<zakura_chain::transparent::Output>,
) -> Arc<zakura_chain::transaction::Transaction> {
    use zakura_chain::{
        transaction::{LockTime, Transaction},
        transparent::{Input, Script},
    };
    Arc::new(Transaction::V1 {
        inputs: outpoints
            .into_iter()
            .map(|outpoint| Input::PrevOut {
                outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            })
            .collect(),
        outputs,
        lock_time: LockTime::Height(Height(0)),
    })
}

/// A one-zatoshi output to a distinct address for each `index`.
fn indexed_output(index: u8) -> zakura_chain::transparent::Output {
    zakura_chain::transparent::Output {
        value: zakura_chain::amount::Amount::try_from(1u64).unwrap(),
        lock_script: zakura_chain::transparent::Address::from_pub_key_hash(
            zakura_chain::parameters::NetworkKind::Testnet,
            [index; 20],
        )
        .script(),
    }
}

/// Outputs that one transaction spends from creators in distinct transactions.
const FAN_OUT: u8 = 70;

/// The terminal height of the generated chain.
const GENERATED_TIP: u32 = 104;

/// A transparent chain through [`GENERATED_TIP`] with same-block, cross-window, and
/// many-creator spends.
fn generated_transparent_chain() -> (Vec<Arc<Block>>, Network) {
    use zakura_chain::transparent::OutPoint;
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]
            .zcash_deserialize_into()
            .unwrap(),
    );
    let outpoint = |transaction: &Arc<zakura_chain::transaction::Transaction>, index| OutPoint {
        hash: transaction.hash(),
        index,
    };
    let mut blocks = vec![genesis];
    let mut fan_out = Vec::new();
    for height in 1..=GENERATED_TIP {
        let mut transactions = Vec::new();
        if height == 101 {
            let source = &blocks[1].transactions[0];
            let first = spending_transaction(outpoint(source, 0), source.outputs()[0].clone());
            let second = spending_transaction(outpoint(&first, 0), first.outputs()[0].clone());
            transactions.extend([first, second]);
        }
        if height == 102 {
            let source = &blocks[2].transactions[0];
            transactions.push(spending_transaction(
                outpoint(source, 2),
                source.outputs()[2].clone(),
            ));
            transactions.push(transparent_transaction(
                vec![outpoint(source, 0)],
                (0..FAN_OUT).map(indexed_output).collect(),
            ));
        }
        if height == 103 {
            let fan = blocks[102].transactions.last().unwrap().clone();
            fan_out = (0..FAN_OUT)
                .map(|index| {
                    spending_transaction(outpoint(&fan, index.into()), indexed_output(index))
                })
                .collect();
            transactions.extend(fan_out.iter().cloned());
        }
        if height == GENERATED_TIP {
            let mut sweep = indexed_output(0);
            sweep.value = zakura_chain::amount::Amount::try_from(u64::from(FAN_OUT)).unwrap();
            transactions.push(transparent_transaction(
                fan_out.iter().map(|creator| outpoint(creator, 0)).collect(),
                vec![sweep],
            ));
        }
        blocks.push(synthetic_block(
            blocks.last().unwrap(),
            height,
            transactions,
        ));
    }
    let network = generated_network(&blocks[0], None);
    (blocks, network)
}

/// A configured network for the generated chain, with NU7 at `nu7` if set.
fn generated_network(genesis: &Block, nu7: Option<u32>) -> Network {
    use zakura_chain::parameters::testnet::{
        ConfiguredActivationHeights, ConfiguredCheckpoints, ParametersBuilder,
    };
    let mut builder = ParametersBuilder::default()
        .with_genesis_hash(genesis.hash())
        .unwrap()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (Height(0), genesis.hash()),
            (Height(3_000_000), zakura_chain::block::Hash([9; 32])),
        ]))
        .unwrap()
        .with_unshielded_coinbase_spends(true);
    if let Some(nu7) = nu7 {
        builder = builder
            .with_activation_heights(ConfiguredActivationHeights {
                nu7: Some(nu7),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams();
    }
    builder.to_network().unwrap()
}

#[test]
fn spentness_generated_spends_validate_after_handoff() {
    use crate::{service::check::utxo::transparent_spend, SemanticallyVerifiedBlock};
    use zakura_chain::transparent::OutPoint;
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    let fixture = Fixture::from_blocks(blocks, network, None);
    let mut state = fixture.open();
    fixture.commit_all(&mut state);
    assert!(!state.db.spentness_incomplete());
    let source = &fixture.blocks[3].transactions[0];
    let spend = spending_transaction(
        OutPoint {
            hash: source.hash(),
            index: 0,
        },
        source.outputs()[0].clone(),
    );
    let next = synthetic_block(
        fixture.blocks.last().unwrap(),
        GENERATED_TIP + 1,
        vec![spend.clone()],
    );
    let prepared = SemanticallyVerifiedBlock::from(next.clone());
    transparent_spend(
        &prepared,
        &CreatedUtxos::default(),
        &HashMap::new(),
        &state.db,
    )
    .unwrap();
    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(next.clone()).into(),
            None,
            None,
            "post-handoff ordinary commit",
        )
        .unwrap();
    let double_spend = synthetic_block(&next, GENERATED_TIP + 2, vec![spend]);
    assert!(transparent_spend(
        &SemanticallyVerifiedBlock::from(double_spend),
        &CreatedUtxos::default(),
        &HashMap::new(),
        &state.db
    )
    .is_err());
    let source = &fixture.blocks.last().unwrap().transactions[0];
    let immature = spending_transaction(
        OutPoint {
            hash: source.hash(),
            index: 0,
        },
        source.outputs()[0].clone(),
    );
    assert!(transparent_spend(
        &SemanticallyVerifiedBlock::from(synthetic_block(&next, GENERATED_TIP + 2, vec![immature])),
        &CreatedUtxos::default(),
        &HashMap::new(),
        &state.db
    )
    .is_err());
    let source = &fixture.blocks[4].transactions[0];
    let mut excess = source.outputs()[0].clone();
    excess.value = (excess.value + zakura_chain::amount::Amount::try_from(1u64).unwrap()).unwrap();
    let negative = spending_transaction(
        OutPoint {
            hash: source.hash(),
            index: 0,
        },
        excess,
    );
    assert!(transparent_spend(
        &SemanticallyVerifiedBlock::from(synthetic_block(&next, GENERATED_TIP + 2, vec![negative])),
        &CreatedUtxos::default(),
        &HashMap::new(),
        &state.db
    )
    .is_err());
}

/// Construction computes exact NSM accounting, so the NSM seed block may be H.
///
/// Ordinary state seeds the NSM balance at the block before NU7 activation. Synthetic
/// blocks lack NU5 block commitments, so the chain stops before activation.
#[test]
fn spentness_matches_ordinary_nsm_seed() {
    let _guard = zakura_test::init();
    let tip = GENERATED_TIP;
    // NU7 at H + 1 seeds at H, during construction. NU7 at H + 2 seeds after it.
    for nu7 in [tip + 1, tip + 2] {
        let (blocks, _) = generated_transparent_chain();
        let network = generated_network(&blocks[0], Some(nu7));
        let mut fixture = Fixture::from_blocks(blocks, network, None);
        let mut state = fixture.open();
        fixture.commit_all(&mut state);
        assert!(!state.db.spentness_incomplete(), "{nu7}");
        assert_matches_ordinary(&state, &fixture.ordinary);
        if nu7 == tip + 2 {
            let seed = synthetic_block(fixture.blocks.last().unwrap(), tip + 1, Vec::new());
            for target in [&mut state, &mut fixture.ordinary] {
                target
                    .commit_finalized_direct(
                        CheckpointVerifiedBlock::from(seed.clone()).into(),
                        None,
                        None,
                        "NU7 seed block",
                    )
                    .unwrap();
            }
            assert_matches_ordinary(&state, &fixture.ordinary);
        }
        assert_ne!(
            state.db.finalized_value_pool().nsm_value_balance_amount(),
            zakura_chain::amount::Amount::<zakura_chain::amount::NegativeAllowed>::zero(),
            "{nu7}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn spentness_state_service_rejects_pending_utxos_and_semantic_admission() {
    use tower::{Service, ServiceExt};
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut finalized = fixture.open();
    fixture.commit(&mut finalized, 0..=4);
    drop(finalized);
    let (mut state, reader, _tip, _tip_changes) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        crate::service::StateService::new_with_spentness(
            fixture.config.clone(),
            &fixture.network,
            Height(10),
            4,
            fixture.spentness.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let outpoint = zakura_chain::transparent::OutPoint {
        hash: fixture.blocks[1].transactions[0].hash(),
        index: 0,
    };
    for request in [
        crate::Request::AwaitUtxo(outpoint),
        crate::Request::UnspentBestChainUtxo(outpoint),
        crate::Request::CommitSemanticallyVerifiedBlock(crate::SemanticallyVerifiedBlock::from(
            fixture.blocks[5].clone(),
        )),
        crate::Request::CheckBlockProposalValidity(crate::SemanticallyVerifiedBlock::from(
            fixture.blocks[5].clone(),
        )),
    ] {
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            state.ready().await.unwrap().call(request),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("incomplete"), "{error}");
    }
    drop(state);
    drop(reader);
}
