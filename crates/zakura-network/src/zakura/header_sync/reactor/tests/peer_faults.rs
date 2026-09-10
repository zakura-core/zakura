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
        request_id: owner.request_id().get(),
        target_tip_hash: owner.header_authority().branch.target_tip_hash,
        common_ancestor_height: wrong_ancestor.height,
        common_ancestor_hash: wrong_ancestor.hash,
        complete: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header: Arc::new(wrong_ancestor_header),
            body_size: 0,
            tree_aux: None,
        }],
    };
    assert!(
        reactor
            .codec
            .encode(&HeaderSyncMessage::Headers(wrong_ancestor_response.clone()))
            .is_ok(),
        "the wrong locator member is otherwise wire-valid"
    );
    reactor.handle_headers(
        peer.clone(),
        0,
        owner.header_authority(),
        wrong_ancestor_response,
    );
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
    reactor.handle_headers(
        peer.clone(),
        0,
        owner.header_authority(),
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: snapshot.frontiers.finalized.height,
            common_ancestor_hash: snapshot.frontiers.finalized.hash,
            complete: true,
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

#[test]
fn response_completion_requires_the_reserved_branch_scope() {
    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let expected = reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has active work")
        .clone();
    let mut wrong_scope = owner.header_authority();
    wrong_scope.header_generation = wrong_scope
        .header_generation
        .checked_next()
        .expect("the fixture generation has a successor");
    reactor.handle_headers(
        peer.clone(),
        0,
        wrong_scope,
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: snapshot.frontiers.finalized.height,
            common_ancestor_hash: snapshot.frontiers.finalized.hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: Vec::new(),
        },
    );
    assert_eq!(reactor.peer_work_queue.active(&peer), Some(&expected));
    assert!(
        actions.try_recv().is_err(),
        "a scope-mismatched page has no peer or scheduling effect"
    );

    let (mut reactor, mut actions, _snapshot, peer, _source, owner) = peer_violation_fixture();
    let expected = reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has active work")
        .clone();
    let mut wrong_scope = owner.header_authority();
    wrong_scope.branch = zakura_header_chain::BranchId::new(
        owner.header_authority().branch.anchor_hash,
        block::Hash([0x73; 32]),
    );
    reactor.handle_headers_outcome(
        peer.clone(),
        0,
        wrong_scope,
        HeadersOutcome {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            outcome: HeadersOutcomeCode::Busy,
        },
    );
    assert_eq!(reactor.peer_work_queue.active(&peer), Some(&expected));
    assert!(
        actions.try_recv().is_err(),
        "a scope-mismatched outcome has no peer or scheduling effect"
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
        request_id: owner.request_id().get(),
        target_tip_hash: owner.header_authority().branch.target_tip_hash,
        common_ancestor_height: returned_ancestor.height,
        common_ancestor_hash: returned_ancestor.hash,
        complete: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![entry; 2],
    };
    reactor.handle_headers(peer.clone(), 0, owner.header_authority(), response);

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
