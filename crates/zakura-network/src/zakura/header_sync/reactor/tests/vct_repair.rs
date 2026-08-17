use super::*;

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
async fn vct_repair_uses_one_exact_canonical_auxiliary_request() {
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
    let (_repairs_tx, repairs_rx) = watch::channel(repair_status);
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
        .expect("the repair supplier status reaches the reactor");
    handle
        .send(Event::VctRepairContextReady {
            owner,
            result: VctRepairContextResult::Resolved(zakura_header_chain::VctRepairContext {
                target: repair_header,
                locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
            }),
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
        .expect("the exact repair response reaches the reactor");
    let HeaderPortOperation::PrepareHeaderTarget {
        purpose,
        source,
        owner: action_owner,
        target,
        mut entries,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("the exact repair response is prepared off-reactor");
    };
    assert_eq!(action_owner.session_id(), 0);
    assert_eq!(target, repair_header);
    assert!(matches!(
        purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            ..
        } if selected_target == repair_header
    ));
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
    snapshots_tx
        .send(Some(after_delivery))
        .expect("the committed metadata-only snapshot is observed");
    time::sleep(std::time::Duration::from_millis(10)).await;
    handle
        .send(Event::HeaderTargetAdmissionReady {
            peer,
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
            result: VctRepairContextResult::Resolved(zakura_header_chain::VctRepairContext {
                target: repair_header,
                locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
            }),
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
