use super::*;
use crate::zakura::block_sync::block_sync_observe::{
    BlockEvent, BlockSyncObserver, ObserverSlot, QueryReplyOutcome, ReactorTransition, RequestStep,
};
use zakura_header_chain::{BodyWorkAuthority, BodyWorkEpoch, CommittedHeaderChainView, Frontier};

#[derive(Debug, Default)]
struct Events(std::sync::Mutex<Vec<BlockEvent>>);

impl BlockSyncObserver for Events {
    fn observe(&self, event: BlockEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn rival_commit_preserves_the_request_before_the_body_arrives() {
    receive_across_rival_commit(false).await;
}

#[tokio::test]
async fn rival_commit_preserves_the_request_waiting_for_decode_capacity() {
    receive_across_rival_commit(true).await;
}

async fn receive_across_rival_commit(waiting_for_decode: bool) {
    let a = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let b = forked_block(&a, 42);
    let parent = Frontier::new(block::Height(0), a.header.previous_block_hash);
    let height = block::Height(1);
    let selected = Frontier::new(height, a.hash());
    let rival = Frontier::new(height, b.hash());
    let mut config = immediate_body_download_config();
    config.request_timeout = Duration::from_secs(300);
    let (views, mut startup) = committed_block_sync_startup(
        committed_snapshot(1, 1, 1, parent, parent, selected),
        config.clone(),
    );
    let events = Arc::new(Events::default());
    startup.observer = ObserverSlot(Some(events.clone()));
    let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup).unwrap();
    let wiring = handle.routine_wiring.as_ref().unwrap();
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let BlockSyncAction::QueryNeededBlocks {
        query_id, scope, ..
    } = next_action(&mut actions).await
    else {
        panic!("startup queries missing blocks")
    };
    let (peer, inbound, mut outbound) = connect_peer_with_status(
        &service,
        &mut actions,
        78,
        height,
        a.hash(),
        1,
        MAX_BS_RESPONSE_BYTES,
    )
    .await;
    let answer = |query_id, scope| BlockSyncEvent::ScopedNeededBlocks {
        query_id,
        scope,
        body_anchor: parent,
        blocks: vec![block_meta(&a)],
    };
    handle.send(answer(query_id, scope)).await.unwrap();
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound).await,
        (height, 1)
    );
    let claim = wiring.registry.outstanding_claims_at(height).pop().unwrap();
    let owner = claim.meta.owner;
    let charge = claim.meta.estimated_bytes;
    assert_eq!(claim.peer, peer);
    assert!(charge > 0);
    let reset_epoch = wiring.view.borrow().reset_epoch;

    let mut permits = Vec::new();
    if waiting_for_decode {
        let generation = events
            .0
            .lock()
            .unwrap()
            .iter()
            .find_map(|event| match event {
                BlockEvent::SessionAdmitted {
                    generation,
                    peer: sender,
                    ..
                } if *sender == peer => Some(*generation),
                _ => None,
            })
            .unwrap();
        let blocked = wiring.registry.handoff_blocked_flag(&peer, generation);
        let input = &wiring.sequencer_input;
        permits
            .extend((0..input.max_capacity()).map(|_| input.clone().try_reserve_owned().unwrap()));
        send_single_block_response(&inbound, height, &a).await;
        await_until(
            "original body waits for decoding",
            Duration::from_secs(2),
            || blocked.load(std::sync::atomic::Ordering::Acquire),
        )
        .await
        .unwrap();
    }

    let view = CommittedHeaderChainView::new(
        parent,
        committed_snapshot(2, 1, 2, parent, rival, selected),
        BodyWorkEpoch::default(),
    );
    let refreshed = BodyWorkAuthority::for_view(&view);
    views.send_replace(Some(view));
    let pending = loop {
        let BlockSyncAction::QueryNeededBlocks {
            query_id, scope, ..
        } = next_action(&mut actions).await
        else {
            panic!("only scheduling queries precede delivery")
        };
        if scope == refreshed {
            break query_id;
        }
    };
    assert_eq!(handle.local_status().tip_hash, rival.hash);
    assert_eq!(handle.local_status().servable_high, height);
    assert_eq!(wiring.view.borrow().download_floor, parent.height);
    assert_eq!(wiring.view.borrow().verified_hash, parent.hash);
    assert_eq!(wiring.view.borrow().reset_epoch, reset_epoch);
    assert_eq!(handle.accounting.reserved(), charge);
    let current = wiring.registry.outstanding_claims_at(height).pop().unwrap();
    assert_eq!(current.peer, peer);
    assert_eq!(current.meta.owner, owner);
    assert_eq!(current.meta.deadline, claim.meta.deadline);

    if waiting_for_decode {
        drop(permits);
    } else {
        send_single_block_response(&inbound, height, &a).await;
    }
    let BlockSyncAction::SubmitBlock {
        block,
        owner: submitted,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("original response reaches the verifier while the state query is pending")
    };
    assert_eq!(block.hash(), a.hash());
    assert_eq!(
        (submitted.session_id, submitted.request_id),
        (owner.session_id, owner.request_id)
    );
    assert_eq!(handle.accounting.reserved(), 0);

    handle.send(answer(pending, refreshed)).await.unwrap();
    await_until("the reactor handled the later state reply", Duration::from_secs(2), || {
        events.0.lock().unwrap().iter().any(|event| matches!(event, BlockEvent::Reactor {
            transition: ReactorTransition::QueryReply { query_id, outcome: QueryReplyOutcome::Accepted, .. }, ..
        } if *query_id == pending))
    }).await.unwrap();
    assert_eq!(wiring.view.borrow().reset_epoch, reset_epoch);
    assert_eq!(wiring.view.borrow().verified_tip, parent.height);
    // A submitted body may advance the speculative download floor.
    // Only a verified commit of A advances the selected-chain verification floor.
    views.send_replace(Some(CommittedHeaderChainView::new(
        selected,
        committed_snapshot(3, 1, 3, parent, selected, selected),
        BodyWorkEpoch::new(1),
    )));
    await_until(
        "the verified selected block advances the sequencer",
        Duration::from_secs(2),
        || wiring.view.borrow().verified_hash == selected.hash,
    )
    .await
    .unwrap();
    let history = events.0.lock().unwrap();
    let settlements: Vec<_> = history
        .iter()
        .filter_map(|event| match event {
            BlockEvent::HandoffEnded {
                peer,
                height,
                owner,
                charge,
            } => Some((peer, *height, *owner, *charge)),
            _ => None,
        })
        .collect();
    assert_eq!(settlements, vec![(&peer, height, owner, charge)]);
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(
                event,
                BlockEvent::Request {
                    step: RequestStep::Published,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(!history.iter().any(|event| matches!(
        event,
        BlockEvent::Request {
            step: RequestStep::Reset,
            ..
        }
    )));
    while let Ok(frame) = outbound.try_recv() {
        assert!(!matches!(
            BlockSyncMessage::decode_frame(frame).unwrap(),
            BlockSyncMessage::GetBlocks { .. }
        ));
    }
    while let Ok(action) = actions.try_recv() {
        assert!(!matches!(action, BlockSyncAction::SubmitBlock { .. }));
    }
    reactor.abort();
}

#[tokio::test]
async fn startup_with_divergent_tips_requests_from_the_shared_parent() {
    let blocks = mainnet_blocks_1_to_3();
    let finalized = Frontier::new(block::Height(0), blocks[0].header.previous_block_hash);
    let parent = Frontier::new(block::Height(1), blocks[0].hash());
    let selected = Frontier::new(block::Height(2), blocks[1].hash());
    let verified = Frontier::new(block::Height(2), forked_block(&blocks[1], 42).hash());
    let initial = committed_snapshot(1, 1, 1, finalized, verified, selected);
    let (views, startup) =
        committed_block_sync_startup(initial.clone(), immediate_body_download_config());
    views.send_replace(Some(CommittedHeaderChainView::new(
        parent,
        initial,
        BodyWorkEpoch::default(),
    )));
    let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup).unwrap();
    let wiring = handle.routine_wiring.as_ref().unwrap();
    assert_eq!(wiring.view.borrow().download_floor, parent.height);
    assert_eq!(wiring.view.borrow().verified_hash, parent.hash);
    assert_eq!(handle.local_status().tip_hash, verified.hash);
    let BlockSyncAction::QueryNeededBlocks { from, .. } = next_action(&mut actions).await else {
        panic!("startup queries the selected branch")
    };
    assert_eq!(from, selected.height);
    reactor.abort();
}
