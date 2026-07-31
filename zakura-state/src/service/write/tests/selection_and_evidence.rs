use super::*;

#[test]
fn same_lower_forward_resets_use_identical_branch_path() {
    let header = regtest_genesis_block().header.clone();
    let reference = |height: u32, hash: u8| VerifiedHeaderRef {
        height: block::Height(height),
        hash: block::Hash([hash; 32]),
        header: header.clone(),
    };
    let old = vec![reference(1, 1), reference(2, 2), reference(3, 3)];
    let direct_growth = vec![
        reference(1, 1),
        reference(2, 2),
        reference(3, 3),
        reference(4, 4),
    ];
    let lower = vec![reference(1, 1), reference(2, 12)];
    let same_height = vec![reference(1, 1), reference(2, 12), reference(3, 13)];
    let forward = vec![
        reference(1, 1),
        reference(2, 12),
        reference(3, 13),
        reference(4, 14),
    ];

    let (cause, changed_path) = classify_verified_change(&old, &direct_growth);
    assert_eq!(cause, VerifiedChangeCause::Grow);
    assert_eq!(changed_path, &direct_growth[old.len()..]);

    for reset in [&lower, &same_height, &forward] {
        let (cause, changed_path) = classify_verified_change(&old, reset);
        assert_eq!(
            cause,
            VerifiedChangeCause::Reset,
            "height relative to the old tip cannot turn a divergent branch into growth"
        );
        assert_eq!(
            changed_path,
            reset.as_slice(),
            "every reset shape replaces the verified path from its exact branch identity"
        );
    }
}

#[test]
fn production_body_unavailability_writer_authenticates_exact_evidence() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the fixture finalized state opens");
    let anchor = regtest_genesis_block();
    let anchor_height = anchor
        .coinbase_height()
        .expect("the anchor has a coinbase height");
    let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
    let result = writer.record_body_unavailable(
        StateVersion::new(1),
        TransientBodyFailure {
            hash: anchor.hash(),
            evidence: EvidenceId::from_digest([0x72; 32]),
            kind: TransientBodyFailureKind::Storage,
            availability: BodyUnavailableSummary {
                attempts: 1,
                suppliers: 1,
                alarmed: false,
                ..Default::default()
            },
        },
    );

    assert!(matches!(
        result,
        Err(HeaderChainStoreError::Transition(
            TransitionFailure::InvalidEvidence(
                "body retry evidence cannot regress an already verified body"
            )
        ))
    ));
}

#[test]
// IN-02: exact authenticated body evidence invalidates only body
// eligibility, and the resulting selected frontier proves reselection.
fn header_valid_body_invalidity_reselects_after_authenticated_evidence() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the fixture finalized state opens");
    let anchor = regtest_genesis_block();
    let anchor_height = anchor
        .coinbase_height()
        .expect("the regtest genesis block has a height");
    let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
    let initial = writer.runtime.publisher().snapshot();
    let anchor_frontier = initial.frontiers.finalized;
    let lease = writer
        .runtime
        .reader()
        .validation_context(anchor.hash())
        .expect("the anchor context read succeeds")
        .expect("the anchor context exists");
    let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
        .expect("the regtest validation policy is coherent");
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash();
    child_header.time += chrono::Duration::seconds(1);
    let child_header = Arc::new(child_header);
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&child_header)),
        &lease,
        &rules,
        &SystemClock,
    )
    .expect("the exact child passes production header validation");
    let child = Frontier::new(
        anchor_height
            .next()
            .expect("the genesis fixture has a next height"),
        child_header.hash(),
    );
    let owner = WorkOwner {
        state_version: initial.state_version,
        header_generation: initial.header_generation,
        verified_generation: None,
        branch: BranchId::new(anchor.hash(), child.hash),
        session_id: 1,
        request_id: std::num::NonZeroU64::new(2).expect("two is nonzero"),
    };
    writer
        .runtime
        .apply(
            TransitionRequest {
                expected_version: initial.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner,
                    source: SourceId::from_digest([3; 32]),
                    parent_hash: anchor.hash(),
                    target_tip_hash: child.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: anchor_frontier,
                    },
                    batch,
                    aux: Vec::new(),
                })),
            },
            &TransitionContext {
                config: &writer.config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the header-only child commits");
    let selected = writer.runtime.publisher().snapshot();
    assert_eq!(selected.frontiers.header_best, child);

    let evidence = EvidenceId::from_digest([4; 32]);
    let rule = BodyRuleId::new("test.commitment_matching_invalid");
    let result = writer
        .record_body_invalid(
            selected.state_version,
            ConsensusBodyInvalid {
                hash: child.hash,
                evidence,
                rule: rule.clone(),
                source: SourceId::from_digest([5; 32]),
            },
        )
        .expect("the exact verifier evidence reaches the production writer");
    assert!(matches!(result, ApplyResult::Committed));
    let rejected = writer.runtime.publisher().snapshot();
    assert_eq!(rejected.frontiers.header_best, anchor_frontier);
    assert_eq!(
        rejected.state_version,
        selected
            .state_version
            .checked_next()
            .expect("the bounded fixture version advances")
    );
    assert_eq!(
        rejected.header_generation,
        selected
            .header_generation
            .checked_next()
            .expect("the bounded fixture generation advances")
    );
}

#[test]
fn contextual_finalization_commits_full_state_header_rows_and_memory_together() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let mut finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the fixture finalized state opens");
    let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
        .expect("genesis deserializes");
    finalized_state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(genesis.clone()).into(),
            None,
            None,
            "shared finalization fixture genesis",
        )
        .expect("genesis commits");
    let block1 = genesis.make_fake_child().set_work(10);
    finalized_state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(block1.clone()).into(),
            None,
            None,
            "shared finalization fixture block one",
        )
        .expect("block one commits");
    let mut live = NonFinalizedState::new(&network);
    let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized_state, &live)
        .expect("the header engine attaches from authenticated finalized state");
    let mut block2 = block1.make_fake_child().set_work(10);
    let block2_height = block2.coinbase_height().expect("block two has a height");
    let mut block2_tx = transaction_to_fake_v5(&block2.transactions[0], &network, block2_height);
    let Transaction::V5 {
        network_upgrade, ..
    } = &mut block2_tx
    else {
        unreachable!("the fake-v5 converter always returns v5 for genesis transactions")
    };
    *network_upgrade = NetworkUpgrade::Nu5;
    Arc::make_mut(&mut block2).transactions[0] = Arc::new(block2_tx);
    let frontier = Frontier::new(block2_height, block2.hash());
    let mut staged = live.clone();
    staged
        .commit_new_chain(block2.prepare(), &finalized_state.db)
        .expect("block two validates into staged full state");
    let (evidence, event_path, request) = verified_request(&writer, &live, &staged, frontier)
        .expect("block two produces exact verified growth");
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
    .expect("block two staging facts agree")
    .commit(&writer.runtime, &mut live, &writer.context())
    .expect("block two commits to both live views");

    commit_contextual_finalization(&writer, &mut finalized_state, &mut live, None)
        .expect("the finalized block and header transition commit together");

    assert!(live.is_chain_set_empty());
    assert_eq!(
        finalized_state.db.tip(),
        Some((frontier.height, frontier.hash))
    );
    let snapshot = writer.runtime.publisher().snapshot();
    assert_eq!(snapshot.frontiers.finalized, frontier);
    assert_eq!(snapshot.frontiers.verified_best, frontier);
    let (reopened, _) = HeaderChainStore::new(finalized_state.db.db().clone())
        .startup(&writer.config)
        .expect("the combined finalized/header transaction reopens coherently");
    assert_eq!(reopened.publisher().snapshot(), snapshot);
}
