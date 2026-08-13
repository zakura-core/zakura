use super::*;

#[test]
fn committed_header_generation_reconsiders_the_cached_peer_target() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the cached-target fixture builds");
    let peer = peer();
    let (send, _outbound) = framed_channel(8);
    reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(peer.clone(), 7, send, CancellationToken::new()),
    ));
    let (_source, _owner, _branch) = seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
    let advertised = reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has an active advertised target")
        .target
        .status
        .clone();
    reactor
        .peer_state
        .get_mut(&peer)
        .expect("the peer is connected")
        .last_status = Some(advertised.clone());

    let mut committed = initial;
    committed.state_version = committed
        .state_version
        .checked_next()
        .expect("the fixture state version advances");
    committed.header_generation = committed
        .header_generation
        .checked_next()
        .expect("the fixture header generation advances");
    reactor.observe_latest_committed_snapshot(committed.clone());

    let HeaderPortOperation::QueryHeaderLocator {
        peer: scheduled_peer,
        session_id,
        target_tip_hash,
        scope,
    } = actions
        .try_recv()
        .expect("the new generation immediately reconsiders the cached target")
    else {
        panic!("the cached target must schedule fresh locator work");
    };
    assert_eq!(scheduled_peer, peer);
    assert_eq!(session_id, 7);
    assert_eq!(target_tip_hash, advertised.selected_tip_hash);
    assert_eq!(scope.header_generation, committed.header_generation);
}

#[test]
fn committed_resource_stall_is_terminal_without_retry_or_peer_fault() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the resource-stall fixture builds");
    let peer = peer();
    let (source, owner, branch) = seed_applying_request(&mut reactor, &initial, peer.clone(), 7);

    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetAdmissionResult::ResourceStalled(
            zakura_header_chain::CommittedStallReceipt {
                state_version: initial.state_version,
                alarm_changed: true,
                attempted_branch: Some(branch),
            },
        ),
    });

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(!reactor
        .completed_targets
        .contains(owner.header_authority().header_generation, branch));
    assert!(
        actions.try_recv().is_err(),
        "a committed local resource outcome is not retried as a transport failure"
    );
}

#[test]
// AUD-06/AUD-07: committing a newer snapshot creates the production retirement boundary.
// Held successes and failures must remain inert after that boundary.
fn committed_snapshot_retires_in_flight_state_results() {
    {
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (_handle, _actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the current-result control builds");
        let peer = peer();
        let (source, owner, branch) =
            seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
        reactor.handle_event(Event::HeaderTargetAdmissionReady {
            peer,
            source,
            owner,
            result: HeaderTargetAdmissionResult::Applied,
        });
        assert!(
            reactor
                .completed_targets
                .contains(owner.header_authority().header_generation, branch),
            "the current-result control proves the live handler can mark completion"
        );
    }

    for is_local_failure in [false, true] {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown);
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the live reactor fixture builds");
        let peer = peer();
        let (source, owner, old_branch) =
            seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
        let result = if is_local_failure {
            HeaderTargetAdmissionResult::Failed(local_failure(owner))
        } else {
            HeaderTargetAdmissionResult::Applied
        };

        let replacement =
            zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xb2; 32]));
        let mut committed = initial.clone();
        committed.state_version =
            zakura_header_chain::StateVersion::new(initial.state_version.get().saturating_add(1));
        committed.header_generation = initial
            .header_generation
            .checked_next()
            .expect("the bounded fixture generation advances");
        committed.frontiers.header_best = replacement;
        committed.header_best_score = zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            replacement.hash,
        );
        reactor.observe_latest_committed_snapshot(committed.clone());

        assert!(reactor.peer_work_queue.active(&peer).is_none());
        assert!(!reactor
            .completed_targets
            .contains(owner.header_authority().header_generation, old_branch));
        let published_tip = handle.best_header_tip();
        let published_candidates = handle.candidate_state();
        assert_eq!(published_tip, (replacement.height, replacement.hash));

        reactor.handle_event(Event::HeaderTargetAdmissionReady {
            peer,
            source,
            owner,
            result,
        });

        assert_eq!(reactor.committed_snapshot, Some(committed));
        assert_eq!(handle.best_header_tip(), published_tip);
        assert_eq!(handle.candidate_state(), published_candidates);
        assert!(!reactor
            .completed_targets
            .contains(owner.header_authority().header_generation, old_branch));
        assert!(matches!(
            actions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[test]
fn monotone_finality_records_an_exact_rebased_apply_completion() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the monotone-rebase fixture builds");
    let peer = peer();
    let (source, owner, _) = seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
    let staged_tip = reactor
        .peer_work_queue
        .active(&peer)
        .and_then(ActiveHeaderRequest::staged_tip)
        .expect("the fixture has one staged header");

    let mut committed = initial;
    committed.state_version = committed
        .state_version
        .checked_next()
        .expect("the fixture version advances");
    committed.header_generation = committed
        .header_generation
        .checked_next()
        .expect("the fixture generation advances");
    committed.verified_generation = committed
        .verified_generation
        .checked_next()
        .expect("the fixture verified generation advances");
    committed.frontiers.finalized = staged_tip;
    committed.frontiers.header_best = staged_tip;
    committed.frontiers.verified_best = staged_tip;
    let completion_authority = zakura_header_chain::HeaderWorkAuthority::for_target(
        &committed,
        owner.header_authority().branch.target_tip_hash,
    );
    reactor.observe_latest_committed_snapshot(committed);

    assert_eq!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| active.owner),
        Some(owner),
        "the registered attempt remains available for the uniform completion gate"
    );
    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetAdmissionResult::Applied,
    });
    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(
        actions.try_recv().is_err(),
        "the completed state call emits no duplicate action or peer score"
    );
    assert!(reactor.completed_targets.contains(
        completion_authority.header_generation,
        completion_authority.branch,
    ));
}

#[test]
fn monotone_finality_sends_exact_prepared_header_work_to_state_for_rebase() {
    let mut startup = startup(CancellationToken::new());
    let network = startup.network.clone();
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the monotone preparation fixture builds");
    let peer = peer();
    let (source, owner, _) = seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the exact header attempt is registered");
    active.phase = HeaderTargetPhase::Preparing;
    let common_ancestor = active
        .common_ancestor
        .expect("the prepared target has a fixed common ancestor");
    let target = zakura_header_chain::Frontier::new(
        active.target.status.selected_tip_height,
        active.target.status.selected_tip_hash,
    );
    let headers: Vec<_> = active
        .entries
        .iter()
        .map(|entry| entry.header.clone())
        .collect();
    let lease = zakura_header_chain::ValidationLease::new(
        common_ancestor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: common_ancestor,
            header: regtest_genesis_block().header.clone(),
        }],
        network,
        [9; 32],
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &zakura_header_chain::SystemClock,
    )
    .expect("the exact held header target prepares");
    let adapter_key = zakura_node_services::header_chain::AdapterKey::new();
    let prepared = zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
        &adapter_key,
        Box::new(zakura_header_chain::InsertHeaders {
            owner,
            source,
            parent_hash: common_ancestor.hash,
            target_tip_hash: target.hash,
            completion: zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor },
            batch,
            aux: Vec::new(),
        }),
    );

    let mut committed = initial;
    committed.state_version = committed
        .state_version
        .checked_next()
        .expect("the fixture version advances");
    committed.header_generation = committed
        .header_generation
        .checked_next()
        .expect("the fixture header generation advances");
    committed.verified_generation = committed
        .verified_generation
        .checked_next()
        .expect("the fixture verified generation advances");
    committed.frontiers.finalized = target;
    committed.frontiers.header_best = target;
    committed.frontiers.verified_best = target;
    reactor.observe_latest_committed_snapshot(committed);

    assert_eq!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| active.owner),
        Some(owner),
        "ordinary preparation remains registered for state-side rebase"
    );
    reactor.handle_event(Event::HeaderTargetPrepared {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetPreparationResult::Prepared(prepared),
    });

    let HeaderPortOperation::ApplyHeaderTarget {
        peer: applied_peer,
        source: applied_source,
        owner: applied_owner,
        ..
    } = actions
        .try_recv()
        .expect("the rebase candidate reaches the serialized state planner")
    else {
        panic!("the held preparation must produce one apply operation");
    };
    assert_eq!(applied_peer, peer);
    assert_eq!(applied_source, source);
    assert_eq!(applied_owner, owner);
    assert_eq!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| active.phase),
        Some(HeaderTargetPhase::Applying)
    );
    assert!(
        actions.try_recv().is_err(),
        "one exact preparation produces only one state apply"
    );
}

#[tokio::test(start_paused = true)]
async fn stale_anchor_admission_reanchors_from_durable_snapshot_without_retry_or_score() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the stale-anchor fixture builds");
    let peer = peer();
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(peer.clone(), 7, send, CancellationToken::new()),
    ));
    let initial_status = outbound
        .recv()
        .await
        .expect("the initial committed status is sent");
    assert!(matches!(
        handle
            .codec()
            .decode_frame(initial_status, None)
            .expect("the initial status decodes"),
        HeaderSyncMessage::Status(_)
    ));

    let (source, owner, old_branch) =
        seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
    reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetAdmissionResult::Failed(stale_failure(owner)),
    });

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(!reactor
        .completed_targets
        .contains(owner.header_authority().header_generation, old_branch));
    assert!(
        actions.try_recv().is_err(),
        "a stale local anchor neither retries work nor scores its peer"
    );

    let replacement = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xb3; 32]));
    let mut committed = initial.clone();
    committed.state_version = initial
        .state_version
        .checked_next()
        .expect("the fixture state version advances");
    committed.header_generation = initial
        .header_generation
        .checked_next()
        .expect("the fixture header generation advances");
    committed.verified_generation = initial
        .verified_generation
        .checked_next()
        .expect("the fixture verified generation advances");
    committed.frontiers.finalized = replacement;
    committed.frontiers.header_best = replacement;
    committed.frontiers.verified_best = replacement;
    committed.header_best_score = zakura_header_chain::ChainScore::new(
        zakura_header_chain::SuffixWork::zero(),
        replacement.hash,
    );
    committed.oldest_retained_height = replacement.height;
    snapshots_tx
        .send(Some(committed.clone()))
        .expect("the durable snapshot receiver remains live");
    let durable = reactor
        .startup
        .committed_snapshots
        .as_ref()
        .and_then(|snapshots| snapshots.borrow().clone())
        .expect("the committed watch exposes the winning anchor");
    reactor.observe_latest_committed_snapshot(durable);

    assert_eq!(reactor.committed_snapshot, Some(committed.clone()));
    assert_eq!(
        handle.best_header_tip(),
        (replacement.height, replacement.hash)
    );
    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(
        actions.try_recv().is_err(),
        "re-anchoring does not hot-retry the impossible owner"
    );

    time::advance(std::time::Duration::from_secs(1)).await;
    reactor.refresh_statuses();
    let refreshed = outbound
        .recv()
        .await
        .expect("the bounded status floor eventually publishes the new anchor");
    let HeaderSyncMessage::Status(status) = handle
        .codec()
        .decode_frame(refreshed, None)
        .expect("the refreshed status decodes")
    else {
        panic!("the re-anchor publication must be Status");
    };
    assert_eq!(status.work_anchor_height, replacement.height);
    assert_eq!(status.work_anchor_hash, replacement.hash);
    assert_eq!(status.selected_tip_height, replacement.height);
    assert_eq!(status.selected_tip_hash, replacement.hash);
    assert!(
        !reactor
            .peer_state
            .get(&peer)
            .and_then(|state| state.status_publisher.as_ref())
            .expect("the connected peer retains its publisher")
            .due(Instant::now()),
        "one refreshed publication satisfies the changed status"
    );
    assert!(
        actions.try_recv().is_err(),
        "status refresh emits neither a retry nor peer punishment"
    );
}

#[test]
// AUD-14: preparation and admission straddle the durable boundary.
// Hold both across restart to cover the reactor's local completion paths.
fn restart_drops_old_preparation_and_admission_completions() {
    let shutdown = CancellationToken::new();
    let mut old_startup = startup(shutdown);
    let anchor = zakura_header_chain::Frontier::new(old_startup.anchor.0, old_startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (_old_snapshots_tx, old_snapshots_rx) = watch::channel(Some(initial.clone()));
    old_startup.committed_snapshots = Some(old_snapshots_rx);
    let (_old_handle, _old_actions, mut old_reactor) =
        build_header_sync_reactor(old_startup).expect("the pre-crash reactor builds");
    let peer = peer();
    let (source, owner, old_branch) =
        seed_applying_request(&mut old_reactor, &initial, peer.clone(), 7);
    old_reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the pre-crash work remains active")
        .phase = HeaderTargetPhase::Preparing;
    drop(old_reactor);

    let shutdown = CancellationToken::new();
    let mut same_startup = startup(shutdown);
    let (_same_snapshots_tx, same_snapshots_rx) = watch::channel(Some(initial.clone()));
    same_startup.committed_snapshots = Some(same_snapshots_rx);
    let (same_handle, mut same_actions, mut same_reactor) =
        build_header_sync_reactor(same_startup).expect("the same-snapshot restart builds");
    let same_tip = same_handle.best_header_tip();
    let same_candidates = same_handle.candidate_state();
    same_reactor.handle_event(Event::HeaderTargetPrepared {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetPreparationResult::Failed(local_failure(owner)),
    });
    assert_eq!(same_handle.best_header_tip(), same_tip);
    assert_eq!(same_handle.candidate_state(), same_candidates);
    assert!(same_reactor.peer_work_queue.active(&peer).is_none());
    assert!(!same_reactor
        .completed_targets
        .contains(owner.header_authority().header_generation, old_branch));
    assert!(matches!(
        same_actions.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    let replacement = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xc2; 32]));
    let mut committed = initial;
    committed.state_version =
        zakura_header_chain::StateVersion::new(committed.state_version.get().saturating_add(1));
    committed.header_generation = committed
        .header_generation
        .checked_next()
        .expect("the bounded fixture generation advances");
    committed.frontiers.header_best = replacement;
    committed.header_best_score = zakura_header_chain::ChainScore::new(
        zakura_header_chain::SuffixWork::zero(),
        replacement.hash,
    );

    let shutdown = CancellationToken::new();
    let mut committed_startup = startup(shutdown);
    let (_committed_snapshots_tx, committed_snapshots_rx) = watch::channel(Some(committed.clone()));
    committed_startup.committed_snapshots = Some(committed_snapshots_rx);
    let (committed_handle, mut committed_actions, mut committed_reactor) =
        build_header_sync_reactor(committed_startup)
            .expect("the post-commit reactor restart builds");
    let committed_tip = committed_handle.best_header_tip();
    let committed_candidates = committed_handle.candidate_state();
    assert_eq!(committed_tip, (replacement.height, replacement.hash));
    committed_reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetAdmissionResult::Applied,
    });
    committed_reactor.handle_event(Event::HeaderTargetAdmissionReady {
        peer: peer.clone(),
        source,
        owner,
        result: HeaderTargetAdmissionResult::Failed(local_failure(owner)),
    });
    assert_eq!(committed_reactor.committed_snapshot, Some(committed));
    assert_eq!(committed_handle.best_header_tip(), committed_tip);
    assert_eq!(committed_handle.candidate_state(), committed_candidates);
    assert!(committed_reactor.peer_work_queue.active(&peer).is_none());
    assert!(!committed_reactor
        .completed_targets
        .contains(owner.header_authority().header_generation, old_branch));
    assert!(matches!(
        committed_actions.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
// AUD-14: an old ordered response exercises the network-side completion
// path and must fail the restarted reactor's generation ownership check.
async fn restart_rejects_old_ordered_stream_response() {
    let mut old_startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(old_startup.anchor.0, old_startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_old_snapshots_tx, old_snapshots_rx) = watch::channel(Some(snapshot.clone()));
    old_startup.committed_snapshots = Some(old_snapshots_rx);
    let (_old_handle, _old_actions, mut old_reactor) =
        build_header_sync_reactor(old_startup).expect("the pre-crash reactor builds");
    let peer = peer();
    let (old_send, _old_outbound) = framed_channel(8);
    old_reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(
            peer.clone(),
            7,
            old_send,
            CancellationToken::new(),
        ),
    ));
    let _ = seed_applying_request(&mut old_reactor, &snapshot, peer.clone(), 7);
    let old_active = old_reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the old ordered stream has one request");
    old_active.phase = HeaderTargetPhase::Receiving;
    old_active.common_ancestor = None;
    let response_entry = old_active
        .entries
        .pop()
        .expect("the fixture has one response entry");
    let stale_response = Headers {
        request_id: old_active.request_id.get(),
        target_tip_hash: old_active.target.status.selected_tip_hash,
        common_ancestor_height: anchor.height,
        common_ancestor_hash: anchor.hash,
        complete: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![response_entry],
    };
    let stale_scope = old_active.owner.header_authority();
    drop(old_reactor);

    let mut fresh_startup = startup(CancellationToken::new());
    let (_fresh_snapshots_tx, fresh_snapshots_rx) = watch::channel(Some(snapshot.clone()));
    fresh_startup.committed_snapshots = Some(fresh_snapshots_rx);
    let (fresh_handle, mut fresh_actions, mut fresh_reactor) =
        build_header_sync_reactor(fresh_startup).expect("the replacement reactor builds");
    let (fresh_send, mut fresh_outbound) = framed_channel(8);
    fresh_reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(
            peer.clone(),
            8,
            fresh_send,
            CancellationToken::new(),
        ),
    ));
    let status = fresh_outbound
        .recv()
        .await
        .expect("the replacement stream receives its initial status");
    assert!(matches!(
        fresh_handle
            .codec()
            .decode_frame(status, None)
            .expect("the replacement status decodes"),
        HeaderSyncMessage::Status(_)
    ));
    let _ = seed_applying_request(&mut fresh_reactor, &snapshot, peer.clone(), 8);
    let fresh_active = fresh_reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the replacement stream has one request");
    fresh_active.phase = HeaderTargetPhase::Receiving;
    fresh_active.common_ancestor = None;
    fresh_active.entries.clear();
    let expected_active = fresh_active.clone();
    let published_tip = fresh_handle.best_header_tip();
    let published_candidates = fresh_handle.candidate_state();

    fresh_reactor.handle_event(Event::SessionResponse {
        peer: peer.clone(),
        session_id: 7,
        scope: stale_scope,
        msg: HeaderSyncMessage::Headers(stale_response),
    });

    assert_eq!(
        fresh_reactor
            .peer_state
            .get(&peer)
            .expect("the replacement peer remains connected")
            .session
            .session_id(),
        8
    );
    assert_eq!(
        fresh_reactor.peer_work_queue.active(&peer),
        Some(&expected_active)
    );
    assert_eq!(fresh_handle.best_header_tip(), published_tip);
    assert_eq!(fresh_handle.candidate_state(), published_candidates);
    assert!(matches!(
        fresh_actions.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(
        time::timeout(std::time::Duration::from_millis(10), fresh_outbound.recv())
            .await
            .is_err(),
        "the stale response emits no replacement-stream frame"
    );
}

#[tokio::test]
async fn initial_committed_snapshot_overrides_legacy_startup_frontiers() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown);
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let header_best = zakura_header_chain::Frontier::new(block::Height(7), block::Hash([0x77; 32]));
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best = header_best;
    snapshot.header_best_score = zakura_header_chain::ChainScore::new(
        zakura_header_chain::SuffixWork::zero(),
        header_best.hash,
    );
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);

    let (handle, _actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the snapshot-authoritative reactor starts");
    assert_eq!(
        handle.best_header_tip(),
        (header_best.height, header_best.hash)
    );

    reactor.abort();
}

#[tokio::test]
async fn peer_admission_catches_up_snapshot_before_initial_status() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown);
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (snapshots_tx, snapshots_rx) = watch::channel(None);
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, _actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the pre-handoff reactor starts");

    let header_best = zakura_header_chain::Frontier::new(block::Height(7), block::Hash([0x77; 32]));
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best = header_best;
    snapshot.header_best_score = zakura_header_chain::ChainScore::new(
        zakura_header_chain::SuffixWork::zero(),
        header_best.hash,
    );
    snapshots_tx
        .send(Some(snapshot))
        .expect("the committed snapshot receiver is live");

    let (send, mut outbound) = framed_channel(8);
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("peer admission queues before the watch arm runs");

    let status_frame = time::timeout(time::Duration::from_secs(1), outbound.recv())
        .await
        .expect("the initial status is sent promptly")
        .expect("the peer outbound remains open");
    let HeaderSyncMessage::Status(status) = handle
        .codec()
        .decode_frame(status_frame, None)
        .expect("the initial status decodes")
    else {
        panic!("the first session message must be the committed status");
    };
    assert_eq!(status.selected_tip_height, header_best.height);
    assert_eq!(status.selected_tip_hash, header_best.hash);

    reactor.abort();
}
