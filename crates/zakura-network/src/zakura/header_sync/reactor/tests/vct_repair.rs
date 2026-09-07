use super::*;
use crate::zakura::testkit::{TraceCapture, TraceValue};

#[test]
fn vct_repair_signal_schedules_one_exact_current_height() {
    let anchor = zakura_header_chain::Frontier::new(block::Height(10), block::Hash([1; 32]));
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(20), block::Hash([2; 32]));
    let status = zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: block::Height(11),
        },
        generation: 7,
    };

    let task =
        vct_repair_task(&snapshot, status).expect("an in-range repair need schedules exact work");
    assert_eq!(task.height, block::Height(11));
    assert_eq!(task.repair_generation, 7);
    assert_eq!(
        task.owner.header_authority().header_generation,
        snapshot.header_generation
    );
    assert_eq!(task.owner.verified_generation, snapshot.verified_generation);
    assert_eq!(
        task.owner.header_authority().branch,
        zakura_header_chain::BranchId::new(
            snapshot.frontiers.finalized.hash,
            snapshot.frontiers.header_best.hash,
        )
    );
    assert_eq!(task.owner.session_id(), INTERNAL_VCT_REPAIR_SESSION_ID);
    assert_eq!(task.owner.request_id().get(), 8);

    assert!(vct_repair_task(
        &snapshot,
        zakura_header_chain::VctRootRepairStatus::default()
    )
    .is_none());
    assert!(vct_repair_task(
        &snapshot,
        zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: snapshot.frontiers.finalized.height,
            },
            generation: 0,
        }
    )
    .is_none());
    assert!(vct_repair_task(
        &snapshot,
        zakura_header_chain::VctRootRepairStatus {
            state: status.state,
            generation: u64::MAX,
        }
    )
    .is_none());
}

#[tokio::test]
async fn committer_repair_replaces_and_then_restores_a_sweep_repair() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(20), block::Hash([2; 32]));
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);

    let sweep_height = block::Height(11);
    let committer_height = block::Height(12);
    let (repairs_tx, repairs_rx) = watch::channel(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: sweep_height,
        },
        generation: 7,
    });
    startup.vct_root_repairs = Some(repairs_rx);
    let (handle, mut actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the repair replacement fixture starts");

    let HeaderPortOperation::QueryVctRepairContext {
        owner: sweep_owner,
        height,
    } = next_action(&mut actions).await
    else {
        panic!("the sweep repair requests its context");
    };
    assert_eq!(height, sweep_height);

    repairs_tx
        .send(zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: committer_height,
            },
            generation: 8,
        })
        .expect("the committer repair replaces the sweep repair");
    let HeaderPortOperation::QueryVctRepairContext {
        owner: committer_owner,
        height,
    } = next_action(&mut actions).await
    else {
        panic!("the committer repair requests its context");
    };
    assert_eq!(height, committer_height);
    assert_ne!(committer_owner, sweep_owner);

    handle
        .send(Event::VctRepairContextReady {
            owner: sweep_owner,
            result: VctRepairContextResult::Resolved(
                zakura_header_chain::VctRepairContext::unconstrained(
                    zakura_header_chain::Frontier::new(sweep_height, block::Hash([11; 32])),
                    zakura_header_chain::HeaderLocator::for_continuation(anchor),
                    None,
                ),
            ),
        })
        .await
        .expect("the late sweep completion reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a completion from the retired sweep repair cannot schedule work"
    );

    repairs_tx
        .send(zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: sweep_height,
            },
            generation: 9,
        })
        .expect("clearing the committer repair restores the sweep repair");
    let HeaderPortOperation::QueryVctRepairContext {
        owner: restored_sweep_owner,
        height,
    } = next_action(&mut actions).await
    else {
        panic!("the restored sweep repair requests new context");
    };
    assert_eq!(height, sweep_height);
    assert_ne!(restored_sweep_owner, sweep_owner);
    assert_ne!(restored_sweep_owner, committer_owner);

    shutdown.cancel();
    reactor
        .await
        .expect("the repair replacement reactor stops cleanly");
}

#[tokio::test]
async fn vct_repair_restarts_after_state_rejection_and_refuses_the_same_semantic_input() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut repair_block_header = *regtest_genesis_block().header;
    repair_block_header.previous_block_hash = anchor.hash;
    repair_block_header.time += chrono::Duration::seconds(1);
    let repair_block_header = Arc::new(repair_block_header);
    let repair_header =
        zakura_header_chain::Frontier::new(block::Height(1), repair_block_header.hash());
    let selected_tip = zakura_header_chain::Frontier::new(block::Height(2), block::Hash([3; 32]));
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best = selected_tip;
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let repair_status = zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: repair_header.height,
        },
        generation: 7,
    };
    let (repairs_tx, repairs_rx) = watch::channel(repair_status);
    startup.vct_root_repairs = Some(repairs_rx);
    let (handle, mut actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the repair fixture starts");
    let query = next_action(&mut actions).await;
    let HeaderPortOperation::QueryVctRepairContext { owner, height } = query else {
        panic!("the exact repair context query precedes ordinary maintenance");
    };
    assert_eq!(height, repair_header.height);
    assert_eq!(
        owner.header_authority(),
        zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).header
    );

    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the repair supplier connects");
    let _status = outbound.recv().await.expect("the local status is sent");
    let supplier_status = Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: selected_tip.height,
        selected_tip_hash: selected_tip.hash,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
    };
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(supplier_status.clone()),
        })
        .await
        .expect("the repair supplier status reaches the reactor");
    handle
        .send(Event::VctRepairContextReady {
            owner,
            result: VctRepairContextResult::Resolved(
                zakura_header_chain::VctRepairContext::unconstrained(
                    repair_header,
                    zakura_header_chain::HeaderLocator::for_continuation(anchor),
                    Some(selected_tip.hash),
                ),
            ),
        })
        .await
        .expect("the exact repair context reaches the reactor");

    let request = outbound.recv().await.expect("the repair request is sent");
    let HeaderSyncMessage::GetHeaders(request) = handle
        .codec()
        .decode_frame(request, None)
        .expect("the canonical repair request decodes")
    else {
        panic!("the repair uses the canonical GetHeaders message");
    };
    assert_ne!(request.request_id, 0);
    assert_eq!(request.target_tip_hash, repair_header.hash);
    assert_eq!(request.locator_hashes, vec![anchor.hash]);
    assert_eq!(request.max_header_count, 1);
    assert_eq!(request.tree_aux_schema, AuxSchema::V1);
    let repair_aux = TreeAuxRecordV1 {
        height: repair_header.height,
        sapling_root: zakura_chain::sapling::tree::Root::default(),
        orchard_root: zakura_chain::orchard::tree::Root::default(),
        ironwood_root: zakura_chain::ironwood::tree::Root::default(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
    };
    handle
        .send(Event::SessionResponse {
            peer: peer.clone(),
            session_id: 0,
            scope: owner.header_authority(),
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: request.request_id,
                target_tip_hash: repair_header.hash,
                common_ancestor_height: anchor.height,
                common_ancestor_hash: anchor.hash,
                complete: true,
                tree_aux_schema: AuxSchema::V1,
                entries: vec![HeaderEntry {
                    header: repair_block_header.clone(),
                    body_size: 0,
                    tree_aux: Some(repair_aux),
                }],
            }),
        })
        .await
        .expect("the exact repair response reaches the reactor");
    let HeaderPortOperation::PrepareHeaderTarget {
        purpose,
        source,
        owner: action_owner,
        target,
        completion,
        mut entries,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("the exact repair response is prepared off-reactor");
    };
    assert_eq!(action_owner.session_id(), 0);
    assert_eq!(target, repair_header);
    match purpose {
        HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target, ..
        } if selected_target == repair_header => {}
        _ => panic!("the repair purpose must carry the selected target"),
    };
    let episode = match completion {
        zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
            selected_target,
            episode,
            ..
        } if selected_target == repair_header => episode,
        _ => panic!("the repair completion must carry the selected target and episode"),
    };
    let entry = entries
        .pop()
        .expect("the exact repair preparation contains one header");
    assert!(entries.is_empty());
    let fixture_network = Network::new_regtest(Default::default());
    let engine_config = zakura_header_chain::EngineConfig::new(
        zakura_header_chain::EngineMode::Integrated,
        fixture_network.clone(),
        zakura_header_chain::TrustedAnchor {
            frontier: anchor,
            header: regtest_genesis_block().header.clone(),
        },
        zakura_header_chain::CheckpointSet::default(),
    )
    .expect("the fixture anchor is coherent");
    let lease = zakura_header_chain::ValidationLease::new(
        anchor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: anchor,
            header: regtest_genesis_block().header.clone(),
        }],
        engine_config.network().clone(),
        engine_config.trust_anchor_digest(),
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
        .expect("the fixture validation lease produces rules");
    let repair_headers = vec![entry.header.clone()];
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(&repair_headers),
        lease.parent(),
        &rules,
        &zakura_header_chain::SystemClock,
    )
    .expect("the fixture repair header prepares");
    let delivery = zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([0x44; 32]),
        repair_header.hash,
        source,
        action_owner,
        zakura_header_chain::BodySizeHint::Unknown,
        entry.tree_aux,
    );
    let insert = Box::new(zakura_header_chain::InsertHeaders {
        owner: action_owner,
        source,
        parent_hash: anchor.hash,
        target_tip_hash: repair_header.hash,
        completion: zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
            common_ancestor: anchor,
            selected_target: repair_header,
            episode,
        },
        batch,
        aux: vec![delivery],
    });
    let adapter_key = zakura_node_services::header_chain::AdapterKey::new();
    handle
        .send(Event::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner: action_owner,
            result: HeaderTargetPreparationResult::Prepared(
                zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
                    &adapter_key,
                    insert,
                ),
            ),
        })
        .await
        .expect("the sealed repair reaches the completion gate");
    let HeaderPortOperation::ApplyHeaderTarget {
        purpose,
        owner: dispatched_owner,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("the current sealed repair is dispatched to state");
    };
    assert_eq!(dispatched_owner, action_owner);
    assert!(matches!(
        purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
    ));

    let mut after_delivery = snapshot;
    after_delivery.state_version = after_delivery
        .state_version
        .checked_next()
        .expect("the fixture state version can advance");
    let after_delivery_state_version = after_delivery.state_version;
    snapshots_tx
        .send(Some(after_delivery))
        .expect("the committed metadata-only snapshot is observed");
    time::sleep(std::time::Duration::from_millis(10)).await;
    handle
        .send(Event::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source,
            owner: action_owner,
            result: HeaderTargetAdmissionResult::Applied,
        })
        .await
        .expect("the state acknowledgement follows its published snapshot");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "the same repair generation is not redelivered after its own state-version advance"
    );

    repairs_tx
        .send(zakura_header_chain::VctRootRepairStatus {
            state: repair_status.state,
            generation: repair_status.generation + 1,
        })
        .expect("rejecting the admitted replacement starts a new repair generation");
    let HeaderPortOperation::QueryVctRepairContext {
        owner: replacement_owner,
        height,
    } = next_action(&mut actions).await
    else {
        panic!("the new generation requests fresh repair context");
    };
    assert_eq!(height, repair_header.height);
    assert_ne!(replacement_owner, owner);
    assert_ne!(
        zakura_header_chain::HeaderSyncWorkOwner::from(replacement_owner),
        action_owner
    );

    let replacement_peer = ZakuraPeerId::new(vec![0x72; 32]).expect("the peer ID is bounded");
    let (replacement_send, mut replacement_outbound) = framed_channel(8);
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            replacement_peer.clone(),
            replacement_send,
            CancellationToken::new(),
        )))
        .await
        .expect("the replacement supplier connects");
    let _status = replacement_outbound
        .recv()
        .await
        .expect("the replacement supplier receives local status");
    handle
        .send(Event::WireMessage {
            peer: replacement_peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(supplier_status),
        })
        .await
        .expect("the replacement supplier status reaches the reactor");

    handle
        .send(Event::VctRepairContextReady {
            owner: replacement_owner,
            result: VctRepairContextResult::Resolved(
                zakura_header_chain::VctRepairContext::from_durable_rows(
                    repair_header,
                    zakura_header_chain::HeaderLocator::for_continuation(anchor),
                    after_delivery_state_version,
                    Some(selected_tip.hash),
                    true,
                    &[zakura_header_chain::UntrustedAuxDeliveryRow::new(
                        delivery,
                        2,
                        [Some([0x55; 32]), None],
                        Some(selected_tip.hash),
                    )],
                )
                .expect("the committed rejection row reconstructs a repair episode"),
            ),
        })
        .await
        .expect("the replacement generation resolves fresh context");
    let replacement_request = replacement_outbound
        .recv()
        .await
        .expect("the replacement generation fetches another delivery");
    let HeaderSyncMessage::GetHeaders(replacement_request) = handle
        .codec()
        .decode_frame(replacement_request, None)
        .expect("the replacement request decodes")
    else {
        panic!("the replacement generation uses GetHeaders");
    };
    assert_eq!(replacement_request.target_tip_hash, repair_header.hash);
    assert_eq!(replacement_request.max_header_count, 1);
    assert_eq!(replacement_request.tree_aux_schema, AuxSchema::V1);

    handle
        .send(Event::SessionResponse {
            peer: replacement_peer,
            session_id: 0,
            scope: replacement_owner.header_authority(),
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: replacement_request.request_id,
                target_tip_hash: repair_header.hash,
                common_ancestor_height: anchor.height,
                common_ancestor_hash: anchor.hash,
                complete: true,
                tree_aux_schema: AuxSchema::V1,
                entries: vec![HeaderEntry {
                    header: repair_block_header,
                    body_size: 0,
                    tree_aux: Some(repair_aux),
                }],
            }),
        })
        .await
        .expect("the repeated semantic input reaches the reactor under a new request");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a rejected semantic input cannot reach state under a new request"
    );
    assert!(
        time::timeout(
            std::time::Duration::from_millis(1_100),
            replacement_outbound.recv(),
        )
        .await
        .is_err(),
        "an unchanged episode cannot clear supplier exclusions and repeat the request"
    );

    drop(snapshots_tx);
    shutdown.cancel();
    reactor.await.expect("the repair reactor stops cleanly");
}

#[tokio::test]
async fn retired_vct_request_response_has_no_actions_or_peer_score() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut repair_block_header = *regtest_genesis_block().header;
    repair_block_header.previous_block_hash = anchor.hash;
    repair_block_header.time += chrono::Duration::seconds(1);
    let repair_block_header = Arc::new(repair_block_header);
    let repair_header =
        zakura_header_chain::Frontier::new(block::Height(1), repair_block_header.hash());
    let selected_tip = zakura_header_chain::Frontier::new(block::Height(2), block::Hash([3; 32]));
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best = selected_tip;
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_repairs_tx, repairs_rx) = watch::channel(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: repair_header.height,
        },
        generation: 7,
    });
    startup.vct_root_repairs = Some(repairs_rx);
    let (handle, mut actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the late-response fixture starts");
    let HeaderPortOperation::QueryVctRepairContext { owner, .. } = next_action(&mut actions).await
    else {
        panic!("the repair context is queried");
    };

    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the supplier connects");
    let _status = outbound.recv().await.expect("the local status is sent");
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(Status {
                work_anchor_height: anchor.height,
                work_anchor_hash: anchor.hash,
                selected_tip_height: selected_tip.height,
                selected_tip_hash: selected_tip.hash,
                suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
                oldest_retained_height: anchor.height,
                max_headers_per_response: 1,
                max_inflight_requests: 1,
                max_message_bytes: 2_000_000,
                tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
            }),
        })
        .await
        .expect("the supplier status reaches the reactor");
    handle
        .send(Event::VctRepairContextReady {
            owner,
            result: VctRepairContextResult::Resolved(
                zakura_header_chain::VctRepairContext::unconstrained(
                    repair_header,
                    zakura_header_chain::HeaderLocator::for_continuation(anchor),
                    Some(selected_tip.hash),
                ),
            ),
        })
        .await
        .expect("the exact repair context reaches the reactor");
    let frame = outbound.recv().await.expect("the repair request is sent");
    let HeaderSyncMessage::GetHeaders(request) = handle
        .codec()
        .decode_frame(frame, None)
        .expect("the repair request decodes")
    else {
        panic!("the repair uses GetHeaders");
    };

    snapshot.verified_generation = snapshot
        .verified_generation
        .checked_next()
        .expect("the fixture verified generation can advance");
    snapshots_tx
        .send(Some(snapshot))
        .expect("the replacement snapshot is published");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::QueryVctRepairContext { .. }
    ));
    handle
        .send(Event::SessionResponse {
            peer,
            session_id: 0,
            scope: owner.header_authority(),
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: request.request_id,
                target_tip_hash: repair_header.hash,
                common_ancestor_height: anchor.height,
                common_ancestor_hash: anchor.hash,
                complete: true,
                tree_aux_schema: AuxSchema::V1,
                entries: vec![HeaderEntry {
                    header: repair_block_header,
                    body_size: 0,
                    tree_aux: Some(TreeAuxRecordV1 {
                        height: repair_header.height,
                        sapling_root: zakura_chain::sapling::tree::Root::default(),
                        orchard_root: zakura_chain::orchard::tree::Root::default(),
                        ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                        sapling_tx_count: 0,
                        orchard_tx_count: 0,
                        ironwood_tx_count: 0,
                        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
                    }),
                }],
            }),
        })
        .await
        .expect("the late reserved response reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a retired response cannot prepare work or emit peer misbehavior"
    );

    shutdown.cancel();
    reactor
        .await
        .expect("the late-response reactor stops cleanly");
}

/// A VCT repair that has resolved its exact context, with one peer connected and advertised.
struct RepairAwaitingSupplier {
    handle: HeaderSyncHandle,
    outbound: crate::zakura::FramedRecv,
    reactor: JoinHandle<()>,
    anchor: zakura_header_chain::Frontier,
    repair_header: zakura_header_chain::Frontier,
}

/// Drive a VCT repair to the moment the reactor can choose a supplier.
///
/// The reactor starts with `header_best` two blocks past the anchor and an unavailable root at
/// height 1, which is the stalled shape the fix addresses. One peer connects and advertises the
/// status `supplier_status` builds from the anchor, then the exact repair context arrives. What
/// the reactor does with that peer is the behavior each caller asserts.
///
/// The caller supplies `startup` so it can install a trace capture before the reactor runs.
async fn repair_awaiting_supplier(
    mut startup: HeaderSyncStartup,
    supplier_status: impl FnOnce(zakura_header_chain::Frontier) -> Status,
) -> RepairAwaitingSupplier {
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut repair_block_header = *regtest_genesis_block().header;
    repair_block_header.previous_block_hash = anchor.hash;
    repair_block_header.time += chrono::Duration::seconds(1);
    let repair_block_header = Arc::new(repair_block_header);
    let repair_header =
        zakura_header_chain::Frontier::new(block::Height(1), repair_block_header.hash());
    let mut snapshot = committed_snapshot(anchor);
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(2), block::Hash([3; 32]));
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_repairs_tx, repairs_rx) = watch::channel(zakura_header_chain::VctRootRepairStatus {
        state: zakura_header_chain::VctRootRepairState::Unavailable {
            height: repair_header.height,
        },
        generation: 7,
    });
    startup.vct_root_repairs = Some(repairs_rx);
    let (handle, mut actions, reactor) =
        spawn_header_sync_reactor(startup).expect("the repair fixture starts");
    let HeaderPortOperation::QueryVctRepairContext { owner, .. } = next_action(&mut actions).await
    else {
        panic!("the exact repair context query precedes ordinary maintenance");
    };

    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the repair supplier connects");
    let _status = outbound.recv().await.expect("the local status is sent");
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(supplier_status(anchor)),
        })
        .await
        .expect("the repair supplier status reaches the reactor");
    handle
        .send(Event::VctRepairContextReady {
            owner,
            result: VctRepairContextResult::Resolved(
                zakura_header_chain::VctRepairContext::unconstrained(
                    repair_header,
                    zakura_header_chain::HeaderLocator::for_continuation(anchor),
                    None,
                ),
            ),
        })
        .await
        .expect("the exact repair context reaches the reactor");

    RepairAwaitingSupplier {
        handle,
        outbound,
        reactor,
        anchor,
        repair_header,
    }
}

/// A supplier that has moved past the repair height must still be eligible.
///
/// This is the shape the stalled mainnet nodes saw: every peer advertised a different selected
/// tip and had finalized past the repair predecessor. Requiring the supplier to sit on our own
/// stalled tip, and to retain history below it, left the repair with no supplier at all.
#[tokio::test]
async fn vct_repair_accepts_a_supplier_ahead_of_the_stalled_branch() {
    let shutdown = CancellationToken::new();
    let RepairAwaitingSupplier {
        handle,
        mut outbound,
        reactor,
        anchor,
        repair_header,
    } = repair_awaiting_supplier(startup(shutdown.clone()), |anchor| Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        // A tip we have never selected, well ahead of the stalled branch.
        selected_tip_height: block::Height(9),
        selected_tip_hash: block::Hash([9; 32]),
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(9_u8),
        // Finalized past the repair predecessor, so nothing below is retained.
        oldest_retained_height: block::Height(5),
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
    })
    .await;

    let request = outbound.recv().await.expect("the repair request is sent");
    let HeaderSyncMessage::GetHeaders(request) = handle
        .codec()
        .decode_frame(request, None)
        .expect("the canonical repair request decodes")
    else {
        panic!("the repair uses the canonical GetHeaders message");
    };
    assert_eq!(request.target_tip_hash, repair_header.hash);
    assert_eq!(request.locator_hashes, vec![anchor.hash]);
    assert_eq!(request.max_header_count, 1);
    assert_eq!(request.tree_aux_schema, AuxSchema::V1);

    shutdown.cancel();
    reactor.await.expect("the reactor task joins");
}

/// A peer that cannot reach the repair height never receives a request, and the repair defers.
///
/// The deferral also emits the rejection tally naming the requirement that excluded the peer.
#[tokio::test]
async fn vct_repair_defers_when_no_peer_reaches_the_repair_height() {
    let mut capture =
        TraceCapture::for_test("vct_repair_defers_when_no_peer_reaches_the_repair_height")
            .expect("trace capture starts");
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    startup.trace = crate::zakura::ZakuraTrace::new(capture.tracer(), "vct-repair-test");
    let RepairAwaitingSupplier {
        handle: _handle,
        mut outbound,
        reactor,
        ..
    } = repair_awaiting_supplier(startup, |anchor| Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        // Below the repair target, so this peer cannot hold the header at all.
        selected_tip_height: anchor.height,
        selected_tip_hash: anchor.hash,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
    })
    .await;

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), outbound.recv())
            .await
            .is_err(),
        "an unreachable repair height must not produce a wire request"
    );

    shutdown.cancel();
    reactor.await.expect("the reactor task joins");

    // The tally is the operator's evidence: without it the trace records only that the repair
    // made no progress, not which requirement excluded the network.
    capture.flush().await;
    let reader = capture.reader().expect("the trace reloads");
    reader.table(HEADER_SYNC_TABLE.table()).assert_row(
        hs_trace::HEADER_VCT_REPAIR_STATE,
        &[
            (hs_trace::PHASE, TraceValue::Str("no_supplier")),
            (hs_trace::PEERS_CONSIDERED, TraceValue::U64(1)),
            (hs_trace::REJECTED_HEIGHT, TraceValue::U64(1)),
            (hs_trace::REJECTED_CAPACITY, TraceValue::U64(0)),
            (hs_trace::OUTCOME, TraceValue::Str("no_eligible_supplier")),
        ],
    );
    let _ = capture.finish().await.expect("trace capture finishes");
}
