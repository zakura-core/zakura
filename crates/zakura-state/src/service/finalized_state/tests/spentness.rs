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
        audit_progress_with_setup, Progress, ReleaseAuthority, ReplayCache, SpentnessConfig,
        SpentnessSetup, METADATA,
    },
    CheckpointVerifiedBlock, FinalizedState,
};
use crate::Config;

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

    fn from_blocks(
        blocks: Vec<Arc<Block>>,
        network: Network,
        omitted: Option<OutputLocation>,
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
                    encoder.push(survives && omitted != Some(location)).unwrap();
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
        let mut state =
            FinalizedState::new_with_spentness(&self.config, &self.network, self.spentness.clone())
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

#[test]
fn spentness_rebuild_matches_ordinary_state_and_resumes_applying() {
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
    assert!(state.db.spentness_rebuilding());
    let fixed_utxos = entries(&state, "utxo_by_out_loc");
    let mut cache = ReplayCache::default();
    let mut yields = 0;
    while state.db.spentness_rebuilding() {
        state
            .rebuild_spentness_step(&mut cache, &mut || {
                yields += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(entries(&state, "utxo_by_out_loc"), fixed_utxos);
    }
    assert!(yields >= 11);
    assert_eq!(
        state.db.finalized_value_pool(),
        fixture.ordinary.db.finalized_value_pool()
    );
    for name in [
        "utxo_by_out_loc",
        "utxo_loc_by_transparent_addr_loc",
        "balance_by_transparent_addr",
    ] {
        assert_eq!(
            entries(&state, name),
            entries(&fixture.ordinary, name),
            "{name}"
        );
    }
    for height in 0..=10 {
        assert_eq!(
            state.db.block_info(Height(height).into()),
            fixture.ordinary.db.block_info(Height(height).into())
        );
    }
    drop(state);
    assert!(!fixture.open().db.spentness_incomplete());
}

#[test]
fn spentness_rebuild_rejects_false_terminal_membership() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(true);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=10);
    let fixed_utxos = entries(&state, "utxo_by_out_loc");
    let mut cache = ReplayCache::default();
    let error = loop {
        match state.rebuild_spentness_step(&mut cache, &mut || Ok(())) {
            Ok(false) => {}
            Ok(true) => panic!("false membership must never publish completion"),
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains("pools"), "{error}");
    assert!(state.db.spentness_rebuilding());
    assert_eq!(entries(&state, "utxo_by_out_loc"), fixed_utxos);
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
            let mut cache = ReplayCache::default();
            while state.db.spentness_rebuilding() {
                state
                    .rebuild_spentness_step(&mut cache, &mut || Ok(()))
                    .unwrap();
            }
            assert_eq!(
                entries(&state, "utxo_by_out_loc"),
                entries(&fixture.ordinary, "utxo_by_out_loc")
            );
            assert_eq!(
                entries(&state, "balance_by_transparent_addr"),
                entries(&fixture.ordinary, "balance_by_transparent_addr")
            );
        }
    }
}

#[test]
fn spentness_rebuild_resumes_without_artifact_and_keeps_rollback_floor() {
    let _guard = zakura_test::init();
    let mut fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=10);
    let mut cache = ReplayCache::default();
    for _ in 0..6 {
        assert!(!state
            .rebuild_spentness_step(&mut cache, &mut || Ok(()))
            .unwrap());
    }
    assert!(matches!(
        state.db.spentness_progress().unwrap(),
        Some(Progress::Rebuilding {
            indexed_height: Some(5),
            ..
        })
    ));
    drop(state);
    fs::remove_file(fixture.spentness.config.artifact.take().unwrap()).unwrap();
    fs::remove_file(crate::artifact_cache_path(
        &fixture.config,
        &fixture.commitment,
    ))
    .unwrap();
    let state = fixture.open();
    assert!(!state.db.spentness_incomplete());
    assert_eq!(
        entries(&state, "balance_by_transparent_addr"),
        entries(&fixture.ordinary, "balance_by_transparent_addr")
    );
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
    assert!(crate::config::database_format_version_on_disk(
        &fixture.config,
        crate::constants::STATE_DATABASE_KIND,
        28,
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

#[tokio::test(flavor = "multi_thread")]
async fn spentness_read_service_gates_partial_indexes() {
    use tower::ServiceExt;
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=4);
    let reader = super::prop::read_service_over(&state);
    for request in [
        crate::ReadRequest::AddressBalance(Default::default()),
        crate::ReadRequest::UnspentBestChainUtxo(zakura_chain::transparent::OutPoint {
            hash: fixture.blocks[1].transactions[0].hash(),
            index: 0,
        }),
    ] {
        let error = reader.clone().oneshot(request).await.unwrap_err();
        assert!(error.to_string().contains("incomplete"), "{error}");
    }
    assert!(reader
        .clone()
        .oneshot(crate::ReadRequest::BlockHeader(Height(4).into()))
        .await
        .is_ok());
    fixture.commit(&mut state, 5..=10);
    let mut cache = ReplayCache::default();
    while state.db.spentness_rebuilding() {
        state
            .rebuild_spentness_step(&mut cache, &mut || Ok(()))
            .unwrap();
    }
    assert!(reader
        .oneshot(crate::ReadRequest::AddressBalance(Default::default()))
        .await
        .is_ok());
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
    use zakura_chain::{
        transaction::{LockTime, Transaction},
        transparent::{Input, Script},
    };
    Arc::new(Transaction::V1 {
        inputs: vec![Input::PrevOut {
            outpoint,
            unlock_script: Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: vec![output],
        lock_time: LockTime::Height(Height(0)),
    })
}

fn generated_transparent_chain() -> (Vec<Arc<Block>>, Network) {
    use zakura_chain::{
        parameters::testnet::{ConfiguredCheckpoints, ParametersBuilder},
        transparent::OutPoint,
    };
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]
            .zcash_deserialize_into()
            .unwrap(),
    );
    let network = ParametersBuilder::default()
        .with_genesis_hash(genesis.hash())
        .unwrap()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (Height(0), genesis.hash()),
            (Height(3_000_000), zakura_chain::block::Hash([9; 32])),
        ]))
        .unwrap()
        .with_unshielded_coinbase_spends(true)
        .to_network()
        .unwrap();
    let mut blocks = vec![genesis];
    for height in 1..=103 {
        let mut transactions = Vec::new();
        if height == 101 {
            let source = &blocks[1].transactions[0];
            let first = spending_transaction(
                OutPoint {
                    hash: source.hash(),
                    index: 0,
                },
                source.outputs()[0].clone(),
            );
            let second = spending_transaction(
                OutPoint {
                    hash: first.hash(),
                    index: 0,
                },
                first.outputs()[0].clone(),
            );
            transactions.extend([first, second]);
        }
        if height == 102 {
            let source = &blocks[2].transactions[0];
            transactions.push(spending_transaction(
                OutPoint {
                    hash: source.hash(),
                    index: 2,
                },
                source.outputs()[2].clone(),
            ));
        }
        blocks.push(synthetic_block(
            blocks.last().unwrap(),
            height,
            transactions,
        ));
    }
    (blocks, network)
}

#[test]
fn spentness_generated_spends_match_all_indexes_and_validate_after_handoff() {
    use crate::{service::check::utxo::transparent_spend, SemanticallyVerifiedBlock};
    use zakura_chain::transparent::OutPoint;
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    let fixture = Fixture::from_blocks(blocks, network, None);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=103);
    let blocked = synthetic_block(fixture.blocks.last().unwrap(), 104, Vec::new());
    assert!(state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(blocked).into(),
            None,
            None,
            "blocked above handoff"
        )
        .is_err());
    assert_eq!(state.db.finalized_tip_height(), Some(Height(103)));
    let fixed_utxos = entries(&state, "utxo_by_out_loc");
    let mut cache = ReplayCache::default();
    while state.db.spentness_rebuilding() {
        state
            .rebuild_spentness_step(&mut cache, &mut || Ok(()))
            .unwrap();
        assert_eq!(entries(&state, "utxo_by_out_loc"), fixed_utxos);
    }
    for name in super::super::STATE_COLUMN_FAMILIES_IN_CODE {
        if [METADATA, super::super::VCT_SYNC_METADATA].contains(name) {
            continue;
        }
        assert_eq!(
            entries(&state, name),
            entries(&fixture.ordinary, name),
            "{name}"
        );
    }
    let source = &fixture.blocks[3].transactions[0];
    let spend = spending_transaction(
        OutPoint {
            hash: source.hash(),
            index: 0,
        },
        source.outputs()[0].clone(),
    );
    let next = synthetic_block(fixture.blocks.last().unwrap(), 104, vec![spend.clone()]);
    let prepared = SemanticallyVerifiedBlock::from(next.clone());
    transparent_spend(&prepared, &HashMap::new(), &HashMap::new(), &state.db).unwrap();
    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(next.clone()).into(),
            None,
            None,
            "post-handoff ordinary commit",
        )
        .unwrap();
    let double_spend = synthetic_block(&next, 105, vec![spend]);
    assert!(transparent_spend(
        &SemanticallyVerifiedBlock::from(double_spend),
        &HashMap::new(),
        &HashMap::new(),
        &state.db
    )
    .is_err());
    let source = &fixture.blocks[103].transactions[0];
    let immature = spending_transaction(
        OutPoint {
            hash: source.hash(),
            index: 0,
        },
        source.outputs()[0].clone(),
    );
    assert!(transparent_spend(
        &SemanticallyVerifiedBlock::from(synthetic_block(&next, 105, vec![immature])),
        &HashMap::new(),
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
        &SemanticallyVerifiedBlock::from(synthetic_block(&next, 105, vec![negative])),
        &HashMap::new(),
        &HashMap::new(),
        &state.db
    )
    .is_err());
}

#[test]
fn spentness_audit_detects_zero_value_non_address_omission() {
    let _guard = zakura_test::init();
    let (blocks, network) = generated_transparent_chain();
    let fixture = Fixture::from_blocks(
        blocks,
        network,
        Some(OutputLocation::from_usize(Height(1), 0, 1)),
    );
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=103);
    let mut cache = ReplayCache::default();
    let error = loop {
        match state.rebuild_spentness_step(&mut cache, &mut || Ok(())) {
            Ok(false) => {}
            Ok(true) => panic!("zero-valued non-address outputs still require exact membership"),
            Err(error) => break error,
        }
    };
    assert!(
        error.to_string().contains("terminal UTXO entry differs"),
        "{error}"
    );
}

#[test]
fn spentness_offline_audit_reenumerates_instead_of_trusting_cursor() {
    let _guard = zakura_test::init();
    let fixture = Fixture::new(false);
    let mut state = fixture.open();
    fixture.commit(&mut state, 0..=4);
    let mut progress = state.db.spentness_progress().unwrap().unwrap();
    drop(state);
    assert!(
        audit_progress_with_setup(&fixture.config, &fixture.spentness, &fixture.network,)
            .unwrap()
            .cursor_matches
    );
    let state = fixture.open();
    if let Progress::Applying { next_ordinal, .. } = &mut progress {
        *next_ordinal += 1;
    }
    let mut batch = crate::DiskWriteBatch::new();
    batch
        .prepare_spentness_progress(&state.db, progress)
        .unwrap();
    state.db.write_batch(batch).unwrap();
    drop(state);
    let audit =
        audit_progress_with_setup(&fixture.config, &fixture.spentness, &fixture.network).unwrap();
    assert!(!audit.cursor_matches);
    assert_eq!(audit.recorded_outputs, audit.enumerated_outputs + 1);
}

#[test]
fn spentness_pruning_waits_for_rebuild_and_rediscovers_backlog() {
    let _guard = zakura_test::init();
    let mut fixture = Fixture::new(false);
    fixture.config.storage_mode = crate::StorageMode::Pruned(crate::PruningConfig::default());
    // A later checkpoint target would normally let this prefix skip raw bodies.
    let checkpoint_target = Height(20_000);
    let mut state = fixture
        .open()
        .with_checkpoint_raw_tx_retention(checkpoint_target, &fixture.config);
    fixture.commit(&mut state, 0..=4);
    drop(state);
    let mut state = fixture
        .open()
        .with_checkpoint_raw_tx_retention(checkpoint_target, &fixture.config);
    fixture.commit(&mut state, 5..=10);
    for height in 0..=10 {
        assert!(state.db.contains_body_at_height(Height(height)));
    }
    let mut cache = ReplayCache::default();
    while state.db.spentness_rebuilding() {
        state
            .rebuild_spentness_step(&mut cache, &mut || Ok(()))
            .unwrap();
    }
    assert!(state.has_checkpoint_raw_tx_archive_backlog());
    drop(state);
    let mut state = fixture
        .open()
        .with_checkpoint_raw_tx_retention(checkpoint_target, &fixture.config);
    assert!(state.has_checkpoint_raw_tx_archive_backlog());
    let next = synthetic_block(fixture.blocks.last().unwrap(), 11, Vec::new());
    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(next).into(),
            None,
            None,
            "pruning after rebuild",
        )
        .unwrap();
    assert!(!state.has_checkpoint_raw_tx_archive_backlog());
    assert!(state.db.contains_body_at_height(Height(0)));
    assert!(!state.db.contains_body_at_height(Height(1)));
    let source = &fixture.blocks[1].transactions[0];
    let outpoint = zakura_chain::transparent::OutPoint {
        hash: source.hash(),
        index: 0,
    };
    assert!(state.db.output_location(&outpoint).is_some());
    assert!(state.db.utxo(&outpoint).is_some());
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
