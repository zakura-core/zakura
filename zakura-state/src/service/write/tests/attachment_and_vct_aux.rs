use super::*;

#[test]
fn attachment_failure_exits_with_a_typed_error_before_publication() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the empty finalized-state fixture opens");
    let live = NonFinalizedState::new(&network);
    let (chain_tip_sender, _latest_tip, _tip_change) = ChainTipSender::new(None, &network);
    let (non_finalized_state_sender, _non_finalized_state_receiver) = watch::channel(live.clone());
    let (snapshot_sender, snapshot_receiver) = watch::channel(None);
    let (reader_sender, reader_receiver) = watch::channel(None);
    let (
        mut senders,
        _invalid_reset_receiver,
        _rejected_receiver,
        _vct_repair_receiver,
        task_failure,
        task,
    ) = BlockWriteSender::spawn_with_header_chain(
        finalized_state,
        live,
        chain_tip_sender,
        non_finalized_state_sender,
        true,
        None,
        None,
        true,
        HeaderChainObservers::new(snapshot_sender, reader_sender),
    );

    drop(senders.finalized.take());
    let task = Arc::into_inner(task.expect("the writer task was spawned"))
        .expect("the fixture owns the only writer-task handle");
    let result = task.join().expect("attachment failure does not panic");

    assert!(matches!(
        result,
        BlockWriteTaskExit::HeaderChainAttachmentFailed(HeaderChainAttachmentError::MissingGenesis)
    ));
    assert_eq!(
        task_failure
            .get()
            .expect("every service clone can observe the failure")
            .to_string(),
        "header-chain attachment failed: finalized state has no authenticated genesis header at semantic handoff"
    );
    assert!(snapshot_receiver.borrow().is_none());
    assert!(reader_receiver.borrow().is_none());
}

#[test]
fn vct_aux_selection_prefers_authenticated_complete_nonrejected_provenance() {
    let delivery = |byte: u8,
                    authentication: zakura_header_chain::AuxAuthentication,
                    has_aux: bool| zakura_header_chain::AuxDelivery {
        delivery_id: EvidenceId::from_digest([byte; 32]),
        header_hash: block::Hash([1; 32]),
        source: zakura_header_chain::SourceId::from_digest([byte; 32]),
        owner: zakura_header_chain::WorkOwner {
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(2),
            verified_generation: Some(VerifiedGeneration::new(3)),
            branch: zakura_header_chain::BranchId::new(block::Hash([4; 32]), block::Hash([5; 32])),
            session_id: 6,
            request_id: std::num::NonZeroU64::new(7).expect("seven is nonzero"),
        },
        body_size: zakura_header_chain::BodySizeHint::Unknown,
        tree_aux: has_aux.then_some(zakura_header_chain::TreeAuxRecordV1 {
            height: block::Height(1),
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
        }),
        authentication,
    };
    let rejected = delivery(
        1,
        zakura_header_chain::AuxAuthentication::Rejected {
            evidence: EvidenceId::from_digest([8; 32]),
        },
        true,
    );
    let unauthenticated = delivery(
        2,
        zakura_header_chain::AuxAuthentication::Unauthenticated,
        true,
    );
    let authenticated = delivery(
        3,
        zakura_header_chain::AuxAuthentication::Authenticated {
            evidence: EvidenceId::from_digest([9; 32]),
            boundary_hash: block::Hash([10; 32]),
        },
        true,
    );
    let incomplete = delivery(
        0,
        zakura_header_chain::AuxAuthentication::Authenticated {
            evidence: EvidenceId::from_digest([11; 32]),
            boundary_hash: block::Hash([12; 32]),
        },
        false,
    );

    assert_eq!(
        super::select_vct_aux_delivery(vec![rejected, unauthenticated, authenticated, incomplete,]),
        Some(authenticated)
    );
    assert_eq!(
        super::select_vct_aux_delivery(vec![rejected, incomplete]),
        None
    );

    let window = VctAuxWindow {
        snapshot: EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(2),
            verified_generation: VerifiedGeneration::new(3),
            frontiers: FrontierSet {
                finalized: Frontier::new(block::Height(0), block::Hash([0; 32])),
                header_best: Frontier::new(block::Height(1), block::Hash([1; 32])),
                verified_best: Frontier::new(block::Height(0), block::Hash([0; 32])),
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), block::Hash([1; 32])),
            oldest_retained_height: block::Height(0),
            alarms: AlarmSet::default(),
        },
        current: authenticated,
        successor: None,
    };
    let expected_roots = authenticated
        .tree_aux
        .map(|aux| (aux.sapling_root, aux.orchard_root, aux.ironwood_root))
        .expect("the authenticated fixture contains tree auxiliary data");
    assert_eq!(
        window.current_roots(block::Height(1), block::Hash([1; 32])),
        Some(expected_roots)
    );
    assert_eq!(
        window.current_roots(block::Height(2), block::Hash([1; 32])),
        None,
        "height-mismatched provenance fails closed"
    );
    assert_eq!(
        window.current_roots(block::Height(1), block::Hash([2; 32])),
        None,
        "hash-mismatched provenance fails closed"
    );
    assert!(
        HeaderChainWriter::vct_authentication_request(&window).is_none(),
        "already authenticated metadata needs no new transition"
    );

    let successor_block = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
        .expect("the successor fixture deserializes");
    let successor_height = successor_block
        .coinbase_height()
        .expect("the successor fixture has a height");
    let successor_delivery = zakura_header_chain::AuxDelivery {
        header_hash: successor_block.hash(),
        tree_aux: Some(zakura_header_chain::TreeAuxRecordV1 {
            height: successor_height,
            auth_data_root: successor_block.auth_data_root(),
            ..unauthenticated
                .tree_aux
                .expect("the unauthenticated fixture contains tree auxiliary data")
        }),
        ..unauthenticated
    };
    let successor = NextVctBlock::from_delivery(
        successor_block.header.clone(),
        successor_height,
        successor_delivery,
    )
    .expect("the exact successor delivery constructs a witness");
    let auth_window = VctAuxWindow {
        snapshot: window.snapshot,
        current: unauthenticated,
        successor: Some(successor.clone()),
    };
    let (evidence, request) = HeaderChainWriter::vct_authentication_request(&auth_window)
        .expect("an unauthenticated current delivery and successor produce evidence");
    let TransitionEvent::AuxEvidence(event) = request.event else {
        panic!("VCT authentication uses the sole auxiliary evidence transition");
    };
    assert_eq!(event.deliveries, vec![unauthenticated]);
    assert_eq!(
        event.authentication,
        AuxAuthentication::Authenticated {
            evidence,
            boundary_hash: successor.hash,
        }
    );
}

#[test]
fn stale_vct_aux_rejection_has_zero_durable_effects() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("the fixture finalized state opens");
    let anchor = regtest_genesis_block();
    let anchor_height = anchor
        .coinbase_height()
        .expect("the anchor has a coinbase height");
    let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
    let before = writer.runtime.publisher().snapshot();
    let mut stale = before.clone();
    stale.state_version = StateVersion::new(0);
    let current = zakura_header_chain::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0x73; 32]),
        header_hash: anchor.hash(),
        source: zakura_header_chain::SourceId::from_digest([0x74; 32]),
        owner: WorkScope::for_body_work(&stale)
            .bind(1, std::num::NonZeroU64::new(1).expect("one is nonzero")),
        body_size: zakura_header_chain::BodySizeHint::Unknown,
        tree_aux: None,
        authentication: AuxAuthentication::Unauthenticated,
    };
    let result = writer
        .reject_vct_aux(
            &VctAuxWindow {
                snapshot: stale,
                current,
                successor: None,
            },
            VctAuxRejection::Current,
            crate::error::VctCommitFailure::CurrentRoots,
        )
        .expect("stale auxiliary evidence returns a typed receipt");

    assert!(matches!(result, Some(ApplyResult::Stale(_))));
    assert_eq!(
        writer.runtime.publisher().snapshot(),
        before,
        "stale auxiliary evidence publishes and mutates nothing"
    );
}
