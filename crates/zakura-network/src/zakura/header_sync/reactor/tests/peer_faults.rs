use super::*;

#[test]
fn wrong_locator_ancestor_target_and_prepared_header_are_peer_attributable() {
    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let wrong_ancestor = zakura_header_chain::Frontier::new(
        snapshot.frontiers.finalized.height,
        block::Hash([0x41; 32]),
    );
    let mut wrong_ancestor_header = *active.entries[0].header;
    wrong_ancestor_header.previous_block_hash = wrong_ancestor.hash;
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = None;
    active.entries.clear();
    let wrong_ancestor_response = Headers {
        subscription_id: owner.request_id().get(),
        initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
        common_ancestor_height: wrong_ancestor.height,
        common_ancestor_hash: wrong_ancestor.hash,
        reaches_initial_target: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header: Arc::new(wrong_ancestor_header),
            body_size: 0,
            tree_aux: None,
        }],
    };
    assert!(
        encode_frame(&v9::RegtestMessage::Headers(
            wrong_ancestor_response.clone()
        ))
        .is_ok(),
        "the wrong locator member is otherwise wire-valid"
    );
    reactor.deliver_page(peer.clone(), 0, wrong_ancestor_response);
    assert_peer_violation(&mut actions, HeaderSyncMisbehavior::MalformedMessage);

    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let header = active.entries[0].header.clone();
    let mut wrong_target_header = *header;
    wrong_target_header.time += chrono::Duration::seconds(1);
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = None;
    active.entries.clear();
    let _ = active;
    reactor.peer_work_queue.set_capacity_for_test(&peer, 0, 1);
    reactor.deliver_page(
        peer.clone(),
        0,
        Headers {
            subscription_id: owner.request_id().get(),
            initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: snapshot.frontiers.finalized.height,
            common_ancestor_hash: snapshot.frontiers.finalized.hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: Arc::new(wrong_target_header),
                body_size: 0,
                tree_aux: None,
            }],
        },
    );
    assert_peer_violation(&mut actions, HeaderSyncMisbehavior::MalformedMessage);

    let (mut reactor, mut actions, _snapshot, peer, source, owner) = peer_violation_fixture();
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work")
        .phase = HeaderTargetPhase::Preparing;
    reactor.handle_header_target_prepared(
        peer,
        source,
        owner,
        HeaderTargetPreparationResult::Failed(invalid_header_failure(source, owner)),
    );
    assert_peer_violation(&mut actions, HeaderSyncMisbehavior::InvalidHeader);
}

#[test]
fn typed_taxonomy_scores_only_exact_attributed_header_peer_faults() {
    let (mut reactor, mut actions, _snapshot, peer, source, owner) = peer_violation_fixture();
    let subject = zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch);

    for (category, expected) in [
        (
            zakura_header_chain::ErrorCategory::MalformedProtocol,
            HeaderSyncMisbehavior::MalformedMessage,
        ),
        (
            zakura_header_chain::ErrorCategory::InvalidHeader,
            HeaderSyncMisbehavior::InvalidHeader,
        ),
    ] {
        let error = zakura_header_chain::HeaderChainError::new(
            category,
            subject,
            None,
            None,
            zakura_header_chain::Attribution::HeaderPeer(source),
            None,
        );
        reactor.handle_typed_failure(peer.clone(), source, &error);
        assert_peer_violation(&mut actions, expected);
    }

    for category in [
        zakura_header_chain::ErrorCategory::ValidLosingFork,
        zakura_header_chain::ErrorCategory::DeferredHeader,
        zakura_header_chain::ErrorCategory::BodyPayloadMismatch,
        zakura_header_chain::ErrorCategory::ConsensusBodyInvalid,
        zakura_header_chain::ErrorCategory::OperatorIneligible,
        zakura_header_chain::ErrorCategory::StaleTargetOrGeneration,
        zakura_header_chain::ErrorCategory::LocalAnchorOrIncoherence,
        zakura_header_chain::ErrorCategory::LocalResourceOrStorage,
    ] {
        let error = zakura_header_chain::HeaderChainError::new(
            category,
            subject,
            None,
            None,
            zakura_header_chain::Attribution::HeaderPeer(source),
            None,
        );
        reactor.handle_typed_failure(peer.clone(), source, &error);
        assert!(
            actions.try_recv().is_err(),
            "{category:?} cannot cross the header-peer scoring boundary"
        );
    }

    let wrong_source = zakura_header_chain::SourceId::from_digest([0x72; 32]);
    for category in [
        zakura_header_chain::ErrorCategory::MalformedProtocol,
        zakura_header_chain::ErrorCategory::InvalidHeader,
    ] {
        for attribution in [
            zakura_header_chain::Attribution::None,
            zakura_header_chain::Attribution::HeaderPeer(wrong_source),
            zakura_header_chain::Attribution::BodyPeer(source),
            zakura_header_chain::Attribution::AuxPeer(source),
        ] {
            let error = zakura_header_chain::HeaderChainError::new(
                category,
                subject,
                None,
                None,
                attribution,
                None,
            );
            reactor.handle_typed_failure(peer.clone(), source, &error);
            assert!(
                actions.try_recv().is_err(),
                "{category:?} with {attribution:?} cannot score this header peer"
            );
        }
    }
}

/// A page or outcome for another subscription leaves the active request,
/// the peer, and the scheduler untouched.
#[test]
fn pages_and_outcomes_of_another_subscription_have_no_effect() {
    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    active.phase = HeaderTargetPhase::Receiving;
    let entry = active.entries[0].clone();
    let expected = active.clone();
    let other = owner.request_id().get() + 1;
    reactor.deliver_page(
        peer.clone(),
        0,
        Headers {
            subscription_id: other,
            initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: snapshot.frontiers.finalized.height,
            common_ancestor_hash: snapshot.frontiers.finalized.hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![entry],
        },
    );
    reactor.deliver_outcome(
        peer.clone(),
        0,
        HeadersOutcome {
            subscription_id: other,
            initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
            outcome: HeadersOutcomeCode::TargetNotRetained,
        },
    );
    assert_eq!(reactor.peer_work_queue.active(&peer), Some(&expected));
    assert!(
        actions.try_recv().is_err(),
        "a page or outcome of another subscription has no peer or scheduling effect"
    );
}

#[test]
fn response_exceeding_its_owned_reservation_is_malformed_and_releases_capacity() {
    let (mut reactor, mut actions, _snapshot, peer, _source, owner) = peer_violation_fixture();
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let entry = active.entries[0].clone();
    active.phase = HeaderTargetPhase::Receiving;
    active.entries.clear();
    let returned_ancestor = active.common_ancestor.expect("the fixture has an ancestor");
    let _ = active;
    reactor.peer_work_queue.set_capacity_for_test(&peer, 0, 1);
    let response = Headers {
        subscription_id: owner.request_id().get(),
        initial_target_tip_hash: owner.header_authority().branch.target_tip_hash,
        common_ancestor_height: returned_ancestor.height,
        common_ancestor_hash: returned_ancestor.hash,
        reaches_initial_target: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![entry; 2],
    };
    reactor.deliver_page(peer.clone(), 0, response);

    assert!(
        reactor.peer_work_queue.active(&peer).is_none(),
        "over-reservation retires the target and releases its reservation"
    );
    assert_peer_violation(&mut actions, HeaderSyncMisbehavior::MalformedMessage);
    assert_eq!(
        reactor.peer_work_queue.unowned_chunk_capacity(),
        crate::zakura::header_sync::scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1,
    );
}
