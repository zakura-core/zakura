use super::*;

fn commit_verified_change(
    writer: &HeaderChainWriter,
    live: &mut NonFinalizedState,
    staged: NonFinalizedState,
    accepted: Frontier,
) {
    let (evidence, event_path, request) = verified_request(writer, live, &staged, accepted)
        .expect("the generated full-state change has exact header evidence");
    PreparedFullStateTransition::new(
        evidence,
        writer
            .runtime
            .publisher()
            .snapshot()
            .frontiers
            .verified_best,
        event_path,
        staged,
        None,
        request,
    )
    .expect("the generated full-state and header paths agree")
    .commit(&writer.runtime, live, &writer.context())
    .expect("the generated full-state and header transition commits");
}

fn assert_selected_header_matches_full_state(
    writer: &HeaderChainWriter,
    full_state: &NonFinalizedState,
) {
    let best = full_state
        .best_chain()
        .expect("the generated fork graph has a full-state best chain");
    let (_, tip_hash) = best.non_finalized_tip();
    let expected_work = best
        .blocks
        .values()
        .map(|block| {
            block
                .block
                .header
                .difficulty_threshold
                .to_work()
                .expect("generated block targets have exact work")
                .as_u256()
        })
        .fold(zakura_chain::work::difficulty::U256::zero(), |sum, work| {
            sum.checked_add(work)
                .expect("the short generated graph cannot overflow cumulative work")
        });
    let snapshot = writer.runtime.publisher().snapshot();

    assert_eq!(snapshot.frontiers.header_best.hash, tip_hash);
    assert_eq!(snapshot.frontiers.verified_best.hash, tip_hash);
    assert_eq!(snapshot.header_best_score.tip_hash, tip_hash);
    assert_eq!(
        snapshot.header_best_score.suffix_work.as_u256(),
        expected_work
    );
}

#[test]
fn fork_eviction_demotes_only_bodies_missing_from_full_state() {
    assert_fork_eviction_body_evidence(false);
}

#[test]
fn reconsideration_eviction_demotes_only_bodies_missing_from_full_state() {
    assert_fork_eviction_body_evidence(true);
}

fn assert_fork_eviction_body_evidence(reconsider: bool) {
    use zakura_header_chain::{RowLimit, StoreAuditRead, StoreAuditSnapshot};

    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let mut finalized = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    let genesis = regtest_genesis_block();
    let mut parent = genesis.make_fake_child();
    Arc::make_mut(&mut Arc::make_mut(&mut parent).header).time += chrono::Duration::seconds(1);
    for block in [genesis, parent.clone()] {
        finalized
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block).into(),
                None,
                None,
                "fork eviction fixture",
            )
            .unwrap();
    }
    let mut live = NonFinalizedState::new(&network);
    let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized, &live).unwrap();
    let mut template = parent.make_fake_child();
    let height = template.coinbase_height().unwrap();
    let transaction = crate::tests::setup::transaction_v4_from_coinbase(&template.transactions[0]);
    Arc::make_mut(&mut template).transactions[0] = Arc::new(transaction);
    let merkle_root = template.transactions.iter().cloned().collect();
    let header = Arc::make_mut(&mut Arc::make_mut(&mut template).header);
    header.time += chrono::Duration::seconds(1);
    header.merkle_root = merkle_root;
    header.commitment_bytes = <[u8; 32]>::from(finalized.db.history_tree().hash().unwrap()).into();

    let siblings = template.make_fake_siblings(crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS + 1);
    for (order, block) in siblings.iter().enumerate() {
        let refill_after_invalidation =
            reconsider && order == crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS;
        if refill_after_invalidation {
            let mut staged = live.clone();
            staged.invalidate_block(siblings[0].hash()).unwrap();
            commit_operator_change(&writer, &mut live, staged, siblings[0].hash(), true).unwrap();
        }
        let prepared = block.clone().prepare();
        let mut staged = live.clone();
        staged.commit_new_chain(prepared, &finalized.db).unwrap();
        commit_verified_change(
            &writer,
            &mut live,
            staged,
            Frontier::new(height, block.hash()),
        );
        assert_eq!(
            live.best_tip().unwrap().1,
            siblings[usize::from(refill_after_invalidation)].hash()
        );
        assert!(live.any_chain_contains(&siblings[order].hash()));
    }
    if reconsider {
        assert_eq!(
            live.chain_iter().count(),
            crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS
        );
        let mut staged = live.clone();
        staged
            .reconsider_block(siblings[0].hash(), &finalized.db)
            .unwrap();
        commit_operator_change(&writer, &mut live, staged, siblings[0].hash(), false).unwrap();
        assert_eq!(live.best_tip().unwrap().1, siblings[0].hash());
    }
    let evicted_index = siblings.len() - if reconsider { 1 } else { 2 };
    let evicted = siblings[evicted_index].hash();
    assert!(!live.any_chain_contains(&evicted));
    assert_eq!(
        live.chain_iter().count(),
        crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS
    );
    let store = HeaderChainStore::new(finalized.db.db().clone());
    let mut saw_evicted_header = false;
    store
        .audit_snapshot()
        .unwrap()
        .visit_header_nodes(RowLimit::new(20), &mut |node| {
            if node.height > parent.coinbase_height().unwrap() {
                assert_eq!(
                    matches!(
                        node.body_validation_state,
                        BodyValidationState::Verified { .. }
                    ),
                    live.any_chain_contains(&node.hash),
                    "durable body availability must match retained full state",
                );
            }
            if node.hash == evicted {
                saw_evicted_header = true;
                assert!(
                    node.is_eligible(),
                    "eviction must not invalidate the header"
                );
            }
            Ok(())
        })
        .unwrap();
    assert!(
        saw_evicted_header,
        "the eleventh header fits the independent header limit"
    );

    // Replaying an evicted body restores its verified marker and demotes the next eviction.
    let replay = siblings[evicted_index].clone();
    let mut staged = live.clone();
    staged
        .commit_new_chain(replay.prepare(), &finalized.db)
        .unwrap();
    commit_verified_change(&writer, &mut live, staged, Frontier::new(height, evicted));
    assert!(live.any_chain_contains(&evicted));

    // Once every retained body is invalidated, an evicted header must not prevent
    // the atomic operator transition from falling back to the finalized parent.
    let tips: Vec<_> = live
        .chain_iter()
        .map(|chain| chain.non_finalized_tip_hash())
        .collect();
    for hash in tips {
        let mut staged = live.clone();
        staged.invalidate_block(hash).unwrap();
        commit_operator_change(&writer, &mut live, staged, hash, true).unwrap();
    }
    assert!(live.is_chain_set_empty());
    let snapshot = writer.runtime.publisher().snapshot();
    assert_eq!(
        snapshot.frontiers.verified_best,
        snapshot.frontiers.finalized
    );
}

#[test]
// DF-01: real headers at every observable activation boundary exercise the
// shared rules with production network parameters and historical encoding.
fn production_activation_headers_pass_shared_rules() {
    let _init_guard = zakura_test::init();

    for network in Network::iter() {
        let blocks = network.block_map();

        for upgrade in [
            NetworkUpgrade::Overwinter,
            NetworkUpgrade::Sapling,
            NetworkUpgrade::Blossom,
            NetworkUpgrade::Heartwood,
            NetworkUpgrade::Canopy,
            NetworkUpgrade::Nu5,
        ] {
            let height = upgrade
                .activation_height(&network)
                .expect("every production network configures this upgrade");
            let parent_height = height
                .previous()
                .expect("the tested upgrades activate after genesis");
            let vector_height = blocks
                .range(height.0..)
                .next()
                .map(|(height, _)| *height)
                .expect("an activation or post-activation vector exists");
            let candidate = blocks
                .get(&vector_height)
                .expect("the selected activation vector exists")
                .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
                .expect("the activation vector deserializes");
            if vector_height == height.0 {
                let parent = blocks
                    .get(&parent_height.0)
                    .expect("the exact activation vector has its parent vector")
                    .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
                    .expect("the parent activation vector deserializes");
                assert_eq!(candidate.header.previous_block_hash, parent.hash());
            }

            let spacing = NetworkUpgrade::target_spacing_for_height(&network, height);
            let context_times: Vec<_> = (1..=POW_ADJUSTMENT_BLOCK_SPAN)
                .map(|offset| {
                    let offset_i32 =
                        i32::try_from(offset).expect("the DAA context length fits in i32");
                    candidate.header.time - spacing * offset_i32
                })
                .collect();
            let candidate_bits =
                u32::from_le_bytes(candidate.header.difficulty_threshold.to_le_bytes());
            let context_threshold = (-16..=16)
                .filter_map(|delta| {
                    let bits = i64::from(candidate_bits).checked_add(i64::from(delta))?;
                    let bits = u32::try_from(bits).ok()?;
                    let threshold =
                        zakura_chain::work::difficulty::CompactDifficulty::from_le_bytes(
                            bits.to_le_bytes(),
                        );
                    let expected = AdjustedDifficulty::new_from_header_time(
                        candidate.header.time,
                        parent_height,
                        &network,
                        context_times.iter().copied().map(|time| (threshold, time)),
                    )
                    .expect("the fixture retains the complete difficulty context")
                    .expected_difficulty_threshold();
                    (expected == candidate.header.difficulty_threshold).then_some(threshold)
                })
                .next()
                .expect("a nearby compact context exactly reproduces the historical target");
            assert_eq!(
                AdjustedDifficulty::new_from_header_time(
                    candidate.header.time,
                    parent_height,
                    &network,
                    context_times
                        .iter()
                        .copied()
                        .map(|time| (context_threshold, time)),
                )
                .expect("the fixture retains the complete difficulty context")
                .expected_difficulty_threshold(),
                candidate.header.difficulty_threshold,
                "{network:?} {upgrade:?} activation target must match its complete DAA context",
            );
            assert!(
                candidate.header.difficulty_threshold.to_work().is_some(),
                "the historical target must encode exact work",
            );
        }
    }
}

#[test]
// DF-01: one coherent fork graph crosses NU5 through both validation paths;
// matching tips and work are the complete observable fork-choice result.
fn generated_nu5_graph_matches_full_state_before_finalization() {
    let _init_guard = zakura_test::init();
    fn network(checkpoint_blocks: Option<&[Arc<Block>]>) -> Network {
        let builder = ParametersBuilder::default()
            .with_activation_heights(ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(10),
                sapling: Some(15),
                blossom: Some(20),
                heartwood: Some(25),
                canopy: Some(30),
                nu5: Some(35),
                nu6: Some(100),
                nu6_1: Some(110),
                nu6_2: Some(120),
                nu6_3: Some(130),
                nu7: Some(140),
                #[cfg(zcash_unstable = "nutachyon")]
                nu_tachyon: None,
            })
            .expect("the compressed activation schedule is ordered")
            .with_disable_pow(true)
            .extend_funding_streams();
        let builder = if let Some(blocks) = checkpoint_blocks {
            let genesis_hash = blocks
                .first()
                .expect("the generated checkpoint chain contains genesis")
                .hash();
            builder
                .with_genesis_hash(genesis_hash)
                .expect("the generated genesis hash is canonical")
                .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(
                    blocks
                        .iter()
                        .take(31)
                        .map(|block| {
                            (
                                block
                                    .coinbase_height()
                                    .expect("every generated checkpoint has a height"),
                                block.hash(),
                            )
                        })
                        .collect(),
                ))
                .expect("the generated checkpoints are ordered")
        } else {
            builder
        };
        builder
            .to_network()
            .expect("the compressed custom network is valid")
    }

    fn chain(network: &Network) -> Vec<Arc<Block>> {
        let sapling_root = zakura_chain::sapling::tree::NoteCommitmentTree::default().root();
        let orchard_root = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = zakura_chain::ironwood::tree::NoteCommitmentTree::default().root();
        let mut history_tree = HistoryTree::default();
        let mut previous_hash = GENESIS_PREVIOUS_BLOCK_HASH;
        let mut blocks = Vec::new();

        for height in (0..=40).map(block::Height) {
            let upgrade = NetworkUpgrade::current(network, height);
            let input = transparent::Input::Coinbase {
                height,
                data: if height == block::Height(0) {
                    transparent::GENESIS_COINBASE_SCRIPT_SIG.to_vec()
                } else {
                    format!("DF-01 {height:?}").into_bytes()
                },
                sequence: 0,
            };
            let transaction = match upgrade {
                NetworkUpgrade::Genesis | NetworkUpgrade::BeforeOverwinter => Transaction::V1 {
                    inputs: vec![input],
                    outputs: Vec::new(),
                    lock_time: zakura_chain::transaction::LockTime::unlocked(),
                },
                NetworkUpgrade::Overwinter => Transaction::V3 {
                    inputs: vec![input],
                    outputs: Vec::new(),
                    lock_time: zakura_chain::transaction::LockTime::unlocked(),
                    expiry_height: height,
                    joinsplit_data: None,
                },
                NetworkUpgrade::Sapling
                | NetworkUpgrade::Blossom
                | NetworkUpgrade::Heartwood
                | NetworkUpgrade::Canopy => Transaction::V4 {
                    inputs: vec![input],
                    outputs: Vec::new(),
                    lock_time: zakura_chain::transaction::LockTime::unlocked(),
                    expiry_height: height,
                    joinsplit_data: None,
                    sapling_shielded_data: None,
                },
                NetworkUpgrade::Nu5 => Transaction::V5 {
                    network_upgrade: upgrade,
                    lock_time: zakura_chain::transaction::LockTime::unlocked(),
                    expiry_height: height,
                    inputs: vec![input],
                    outputs: Vec::new(),
                    sapling_shielded_data: None,
                    orchard_shielded_data: None,
                },
                _ => unreachable!("the deterministic graph stops during NU5"),
            };
            let transactions = vec![Arc::new(transaction)];
            let merkle_root = transactions.iter().cloned().collect();
            let time =
                chrono::DateTime::from_timestamp(1_700_000_000_i64 + i64::from(height.0) * 150, 0)
                    .expect("the deterministic timestamp is in range");
            let header = zakura_chain::block::Header {
                version: 4,
                previous_block_hash: previous_hash,
                merkle_root,
                commitment_bytes: HexDebug([0; 32]),
                time,
                difficulty_threshold: network.target_difficulty_limit().to_compact(),
                nonce: HexDebug([0; 32]),
                solution: equihash::Solution::for_proposal(),
            };
            let mut block = Arc::new(Block {
                header: Arc::new(header),
                transactions,
            });
            let commitment = match upgrade {
                NetworkUpgrade::Sapling | NetworkUpgrade::Blossom => <[u8; 32]>::from(sapling_root),
                NetworkUpgrade::Heartwood
                    if NetworkUpgrade::Heartwood.activation_height(network) == Some(height) =>
                {
                    [0; 32]
                }
                NetworkUpgrade::Heartwood | NetworkUpgrade::Canopy => history_tree
                    .hash()
                    .expect("the history tree exists after Heartwood activation")
                    .into(),
                NetworkUpgrade::Nu5 => {
                    let history_root = history_tree.hash().expect("the history tree exists at NU5");
                    ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                        &history_root,
                        &block.auth_data_root(),
                    )
                    .into()
                }
                _ => [0; 32],
            };
            Arc::make_mut(&mut Arc::make_mut(&mut block).header).commitment_bytes =
                commitment.into();
            previous_hash = block.hash();
            history_tree
                .push(
                    network,
                    block.clone(),
                    &sapling_root,
                    &orchard_root,
                    &ironwood_root,
                    #[cfg(zcash_unstable = "nutachyon")]
                    &Default::default(),
                )
                .expect("the deterministic history tree advances");
            blocks.push(block);
        }
        blocks
    }

    let preliminary = network(None);
    let preliminary_chain = chain(&preliminary);
    let network = network(Some(&preliminary_chain));
    let chain = chain(&network);
    assert_eq!(network.genesis_hash(), chain[0].hash());
    assert_eq!(
        chain.iter().map(|block| block.hash()).collect::<Vec<_>>(),
        preliminary_chain
            .iter()
            .map(|block| block.hash())
            .collect::<Vec<_>>(),
        "installing generated checkpoints must not change the generated graph"
    );

    let mut finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the differential finalized state opens");
    let mut live = NonFinalizedState::new(&network);
    for block in chain.iter().take(31) {
        finalized_state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "DF-01 generated Canopy anchor",
            )
            .expect("the generated finalized prefix commits");
    }
    let canopy_anchor = Frontier::new(block::Height(30), chain[30].hash());
    let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized_state, &live)
        .expect("the header engine attaches at the exact full-state Canopy anchor");
    assert_eq!(
        writer.runtime.publisher().snapshot().frontiers.finalized,
        canopy_anchor
    );

    for (index, block) in chain.iter().cloned().enumerate().skip(31) {
        let height = block
            .coinbase_height()
            .expect("the generated block has a coinbase height");
        let parent_hash = block.header.previous_block_hash;
        let lease = writer
            .runtime
            .reader()
            .validation_context(parent_hash)
            .expect("the exact generated parent context read succeeds")
            .expect("the exact generated parent is retained");
        let rules = HeaderRules::for_validation_lease(&lease)
            .expect("the custom network authenticates its PoW waiver");
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&block.header)),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .unwrap_or_else(|error| {
            panic!("generated header {height:?} must pass the shared observable rules: {error}")
        });
        assert_eq!(batch.headers()[0].height, height);
        assert_eq!(batch.headers()[0].hash, block.hash());

        let mut staged = live.clone();
        if index == 31 {
            staged
                .commit_new_chain(block.clone().prepare(), &finalized_state.db)
                .expect("the first generated body enters full state");
        } else {
            staged
                .commit_block(block.clone().prepare(), &finalized_state.db)
                .expect("the next generated body enters full state");
        }
        let accepted = Frontier::new(height, block.hash());
        commit_verified_change(&writer, &mut live, staged, accepted);
        assert_selected_header_matches_full_state(&writer, &live);
    }

    assert_eq!(
        NetworkUpgrade::current(
            &network,
            live.best_chain()
                .expect("the generated full-state graph has a best chain")
                .non_finalized_tip()
                .0,
        ),
        NetworkUpgrade::Nu5
    );

    let incumbent = writer
        .runtime
        .publisher()
        .snapshot()
        .frontiers
        .verified_best;
    let mut side = chain[39].clone();
    let side_block = Arc::make_mut(&mut side);
    let Transaction::V5 { expiry_height, .. } = Arc::make_mut(&mut side_block.transactions[0])
    else {
        unreachable!("the side block is after the generated NU5 activation")
    };
    *expiry_height = block::Height(40);
    Arc::make_mut(&mut side_block.header).merkle_root =
        side_block.transactions.iter().cloned().collect();
    let parent_history_root = live
        .best_chain()
        .expect("the generated full-state graph has a best chain")
        .history_tree(crate::HashOrHeight::Height(block::Height(38)))
        .expect("the side parent has a retained history tree")
        .hash()
        .expect("the side parent history tree is nonempty");
    let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &parent_history_root,
        &side.auth_data_root(),
    )
    .into();
    Arc::make_mut(&mut Arc::make_mut(&mut side).header).commitment_bytes = commitment.into();
    let side_frontier = Frontier::new(
        side.coinbase_height().expect("the side block has a height"),
        side.hash(),
    );
    assert!(writer
        .runtime
        .reader()
        .validation_context(side_frontier.hash)
        .expect("the absent side context reads")
        .is_none());
    let mut staged = live.clone();
    staged
        .commit_block(side.prepare(), &finalized_state.db)
        .expect("the lower-work side block enters full state");
    commit_verified_change(&writer, &mut live, staged, side_frontier);
    assert_eq!(
        writer
            .runtime
            .publisher()
            .snapshot()
            .frontiers
            .verified_best,
        incumbent,
        "accepting a valid side block does not replace the full-state winner"
    );
    assert!(writer
        .runtime
        .reader()
        .validation_context(side_frontier.hash)
        .expect("the accepted side context reads")
        .is_some());

    let mut replacement = chain[39].clone().set_work(1_000);
    let replacement_block = Arc::make_mut(&mut replacement);
    let Transaction::V5 { expiry_height, .. } =
        Arc::make_mut(&mut replacement_block.transactions[0])
    else {
        unreachable!("the replacement is after the generated NU5 activation")
    };
    *expiry_height = block::Height(40);
    Arc::make_mut(&mut replacement_block.header).merkle_root =
        replacement_block.transactions.iter().cloned().collect();
    let parent_history_root = live
        .best_chain()
        .expect("the generated full-state graph has a best chain")
        .history_tree(crate::HashOrHeight::Height(block::Height(38)))
        .expect("the replacement parent has a retained history tree")
        .hash()
        .expect("the replacement parent history tree is nonempty");
    let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &parent_history_root,
        &replacement.auth_data_root(),
    )
    .into();
    Arc::make_mut(&mut Arc::make_mut(&mut replacement).header).commitment_bytes = commitment.into();
    let replacement_frontier = Frontier::new(
        replacement
            .coinbase_height()
            .expect("the replacement has a height"),
        replacement.hash(),
    );
    let mut staged = live.clone();
    staged
        .commit_block(replacement.prepare(), &finalized_state.db)
        .expect("the harder supported-format replacement enters full state");
    commit_verified_change(&writer, &mut live, staged, replacement_frontier);
    assert_eq!(
        writer
            .runtime
            .publisher()
            .snapshot()
            .frontiers
            .verified_best,
        replacement_frontier
    );

    let mut staged = live.clone();
    staged
        .invalidate_block(replacement_frontier.hash)
        .expect("the replacement invalidates in staged full state");
    commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, true)
        .expect("invalidation commits both frontiers before swapping full state");
    let invalidated = writer.runtime.publisher().snapshot();
    assert_eq!(invalidated.frontiers.verified_best, incumbent);
    assert_eq!(invalidated.frontiers.header_best, incumbent);

    let mut staged = live.clone();
    staged
        .reconsider_block(replacement_frontier.hash, &finalized_state.db)
        .expect("the replacement replays into staged full state");
    commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, false)
        .expect("reconsider commits both frontiers before swapping full state");
    let reconsidered = writer.runtime.publisher().snapshot();
    assert_eq!(reconsidered.frontiers.verified_best, replacement_frontier);
    assert_eq!(reconsidered.frontiers.header_best, replacement_frontier);

    let first_non_finalized = chain[31].hash();
    let mut staged = live.clone();
    staged
        .invalidate_block(first_non_finalized)
        .expect("invalidating the common root empties every full-state branch");
    commit_operator_change(&writer, &mut live, staged, first_non_finalized, true)
        .expect("empty full state commits its exact finalized fallback");
    assert!(live.is_chain_set_empty());
    let snapshot = writer.runtime.publisher().snapshot();
    assert_eq!(
        snapshot.frontiers.verified_best,
        snapshot.frontiers.finalized
    );
    assert_eq!(snapshot.frontiers.header_best, snapshot.frontiers.finalized);
}

#[test]
fn fork_eviction_after_restart_keeps_the_restored_full_state_winner() {
    for replace_winner in [false, true] {
        assert_fork_eviction_after_restart(replace_winner);
    }
}

fn assert_fork_eviction_after_restart(replace_winner: bool) {
    use zakura_header_chain::{RowLimit, StoreAuditRead, StoreAuditSnapshot};
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let mut finalized = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    let genesis = regtest_genesis_block();
    let mut parent = genesis.make_fake_child();
    Arc::make_mut(&mut Arc::make_mut(&mut parent).header).time += chrono::Duration::seconds(1);
    for block in [genesis, parent.clone()] {
        finalized
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block).into(),
                None,
                None,
                "fork eviction fixture",
            )
            .unwrap();
    }
    let mut live = NonFinalizedState::new(&network);
    let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized, &live).unwrap();
    let mut template = parent.make_fake_child();
    let transaction = crate::tests::setup::transaction_v4_from_coinbase(&template.transactions[0]);
    Arc::make_mut(&mut template).transactions[0] = Arc::new(transaction);
    let merkle_root = template.transactions.iter().cloned().collect();
    let header = Arc::make_mut(&mut Arc::make_mut(&mut template).header);
    header.time += chrono::Duration::seconds(1);
    header.merkle_root = merkle_root;
    header.commitment_bytes = <[u8; 32]>::from(finalized.db.history_tree().hash().unwrap()).into();

    let mut tip = template.clone();
    for depth in 0..3 {
        let accepted = Frontier::new(tip.coinbase_height().unwrap(), tip.hash());
        let mut staged = live.clone();
        if depth == 0 {
            staged
                .commit_new_chain(tip.clone().prepare(), &finalized.db)
                .unwrap();
        } else {
            staged
                .commit_block(tip.clone().prepare(), &finalized.db)
                .unwrap();
        }
        commit_verified_change(&writer, &mut live, staged, accepted);
        if depth < 2 {
            let history_root = live
                .best_chain()
                .unwrap()
                .history_tree(tip.hash().into())
                .unwrap()
                .hash()
                .unwrap();
            tip = tip.make_fake_child();
            let merkle_root = tip.transactions.iter().cloned().collect();
            let header = Arc::make_mut(&mut Arc::make_mut(&mut tip).header);
            header.time += chrono::Duration::seconds(1);
            header.merkle_root = merkle_root;
            header.commitment_bytes = <[u8; 32]>::from(history_root).into();
        }
    }
    let missing_tip = tip.hash();
    drop(writer);

    // A crash can leave durable header evidence ahead of the asynchronous body backup.
    live = NonFinalizedState::new(&network);
    let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized, &live).unwrap();
    let snapshot = writer.runtime.publisher().snapshot();
    assert_eq!(
        snapshot.frontiers.verified_best,
        snapshot.frontiers.finalized
    );
    assert_eq!(snapshot.frontiers.header_best.hash, missing_tip);

    // Cover side acceptance and winning-path replacement when the eleventh fork arrives.
    let mut siblings =
        template.make_fake_siblings(crate::constants::MAX_NON_FINALIZED_CHAIN_FORKS + 1);
    if replace_winner {
        siblings.reverse();
    }
    let mut saw_eviction = false;
    for sibling in siblings {
        let accepted = Frontier::new(sibling.coinbase_height().unwrap(), sibling.hash());
        let mut staged = live.clone();
        staged
            .commit_new_chain(sibling.prepare(), &finalized.db)
            .unwrap();
        let evicted = live.evicted_blocks(&staged);
        saw_eviction |= !evicted.is_empty();
        commit_verified_change(&writer, &mut live, staged, accepted);
        let snapshot = writer.runtime.publisher().snapshot();
        assert_eq!(
            snapshot.frontiers.verified_best.hash,
            live.best_tip().unwrap().1
        );
        assert_eq!(snapshot.frontiers.header_best.hash, missing_tip);
        let store = HeaderChainStore::new(finalized.db.db().clone());
        store
            .audit_snapshot()
            .unwrap()
            .visit_header_nodes(RowLimit::new(32), &mut |node| {
                if evicted.contains(&node.hash) {
                    assert_eq!(node.body_validation_state, BodyValidationState::Unknown);
                }
                Ok(())
            })
            .unwrap();
    }
    assert!(saw_eviction, "the fixture must cross the fork limit");
}
