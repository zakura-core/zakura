//! Deterministic regressions for peer lifecycle ordering boundaries.

use std::collections::HashMap;

use tokio::{
    runtime::Builder,
    sync::mpsc,
    sync::watch,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use super::super::super::{
    spawn_block_sync_reactor, BlockSyncAction, BlockSyncFrontiers, BlockSyncMessage,
    BlockSyncPeerLifecycleEvent, BlockSyncService, BlockSyncStartup, BlockSyncStatus,
    ZakuraBlockSyncConfig, MAX_BS_RESPONSE_BYTES,
};
use super::super::events::RoutineToReactor;
use super::super::peer;
use crate::zakura::{
    framed_channel,
    testkit::{SyntheticBlockSyncPeer, SyntheticBlockSyncPeers, SyntheticBlockSyncReceive},
    Peer, Service, ServicePeerDirection, ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC,
};
use zakura_chain::block;

/// Prove a peer routine cannot consume queued frames before reactor admission.
///
/// The test deliberately holds the lifecycle event while the real routine is
/// running. Reading the queued `Status` before that event is released would
/// recreate the admission race.
#[test]
fn gb_sm_16_peer_frames_wait_for_reactor_admission() {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("the admission regression runtime builds")
        .block_on(async {
            let tip = (block::Height(3), block::Hash([3; 32]));
            let mut reactor_config = ZakuraBlockSyncConfig::default();
            reactor_config.peer_limits.max_outbound_peers = 0;
            let mut service_config = reactor_config.clone();
            service_config.peer_limits.max_outbound_peers = 1;
            let (_tip_tx, tip_rx) = watch::channel(tip);
            let startup = BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: tip.0,
                    verified_block_tip: tip.0,
                    verified_block_hash: tip.1,
                },
                tip,
                tip_rx,
                reactor_config,
            );
            let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
            let registry = handle
                .routine_wiring
                .as_ref()
                .expect("the spawned reactor exposes routine wiring")
                .registry
                .clone();

            let (lifecycle, mut held_lifecycle) = mpsc::unbounded_channel();
            let mut intercepted_handle = handle.clone();
            intercepted_handle.peer_lifecycle = lifecycle;
            // Let the service entry point create the session, while the reactor's
            // lower cap forces the later admission decision to reject it.
            let service =
                BlockSyncService::new_with_handle_for_test(service_config, intercepted_handle);
            let peer_id = peer(0xa1);
            let (inbound, inbound_recv) = framed_channel(4);
            let (outbound, _outbound_recv) = framed_channel(4);
            let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_recv, outbound))]);
            service.add_peer(Peer::new_with_conn_id_and_direction(
                1,
                peer_id.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC,
                ServicePeerDirection::Outbound,
                streams,
                CancellationToken::new(),
            ));
            let connected_session = match timeout(Duration::from_secs(1), held_lifecycle.recv())
                .await
                .expect("peer admission is emitted within one second")
                .expect("the held lifecycle receives peer admission")
            {
                BlockSyncPeerLifecycleEvent::Connected(session) => session,
                event => panic!("expected peer admission, got {event:?}"),
            };

            inbound
                .try_send(
                    BlockSyncMessage::Status(BlockSyncStatus {
                        servable_low: block::Height(1),
                        servable_high: tip.0,
                        tip_hash: tip.1,
                        max_blocks_per_response: 4,
                        max_inflight_requests: 2,
                        max_response_bytes: MAX_BS_RESPONSE_BYTES,
                    })
                    .encode_frame()
                    .expect("Status encodes"),
                )
                .expect("Status queues without yielding");
            inbound
                .try_send(
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(1),
                        count: 1,
                    }
                    .encode_frame()
                    .expect("GetBlocks encodes"),
                )
                .expect("GetBlocks queues without yielding");
            tokio::time::sleep(Duration::from_millis(1)).await;

            let read_before_admission = registry.has_received_status(&peer_id);
            assert!(
                !read_before_admission,
                "the peer routine read Status before the reactor resolved admission"
            );

            let rejected_cancel = connected_session.cancel_token();
            handle
                .peer_lifecycle
                .send(BlockSyncPeerLifecycleEvent::Connected(connected_session))
                .expect("the held admission reaches the real reactor");
            timeout(Duration::from_secs(1), rejected_cancel.cancelled())
                .await
                .expect("the reactor rejects and cancels the session within one second");
            handle
                .barrier_for_test()
                .await
                .expect("the rejection settles through the reactor barrier");

            assert_eq!(handle.peer_snapshot().outbound_peers, 0);
            assert!(
                !registry.has_received_status(&peer_id),
                "a rejected peer's queued Status reached the registry"
            );
            while let Ok(action) = actions.try_recv() {
                assert!(
                    matches!(action, BlockSyncAction::QueryNeededBlocks { .. }),
                    "a rejected peer produced {action:?}"
                );
            }

            reactor_task.abort();
            let _ = reactor_task.await;
        });
}

/// Deliver a replacement admission before its predecessor and prove the older
/// connect and disconnect cannot displace or disable the live reactor session.
#[test]
fn gb_sm_15_delayed_older_connect_cannot_replace_newer_session() {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("the lifecycle regression runtime builds")
        .block_on(async {
            let tip = (block::Height(3), block::Hash([3; 32]));
            let config = ZakuraBlockSyncConfig::default();
            let (_tip_tx, tip_rx) = watch::channel(tip);
            let startup = BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: tip.0,
                    verified_block_tip: tip.0,
                    verified_block_hash: tip.1,
                },
                tip,
                tip_rx,
                config.clone(),
            );
            let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);

            let mut intercepted_handle = handle.clone();
            let (intercepted_lifecycle, mut lifecycle_events) = mpsc::unbounded_channel();
            intercepted_handle.peer_lifecycle = intercepted_lifecycle;
            let peers = SyntheticBlockSyncPeers::new(config, intercepted_handle, 4);
            let peer_id = peer(0xa2);
            let older_peer = peers
                .connect_peer(peer_id.clone(), 1, ServicePeerDirection::Outbound)
                .expect("the older synthetic session connects");
            let mut newer_peer = peers
                .connect_peer(peer_id.clone(), 2, ServicePeerDirection::Outbound)
                .expect("the replacement synthetic session connects");

            let older_session = match timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("the older PeerConnected event is emitted within one second")
                .expect("the older PeerConnected event is intercepted")
            {
                BlockSyncPeerLifecycleEvent::Connected(session) => session,
                event => panic!("expected older PeerConnected, got {event:?}"),
            };
            let newer_session = match timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("the newer PeerConnected event is emitted within one second")
                .expect("the newer PeerConnected event is intercepted")
            {
                BlockSyncPeerLifecycleEvent::Connected(session) => session,
                event => panic!("expected newer PeerConnected, got {event:?}"),
            };
            let older_session_id = older_session.session_id();
            assert!(newer_session.session_id() > older_session_id);

            handle
                .peer_lifecycle
                .send(BlockSyncPeerLifecycleEvent::Connected(newer_session))
                .expect("the newer admission queues first");
            handle
                .peer_lifecycle
                .send(BlockSyncPeerLifecycleEvent::Connected(older_session))
                .expect("the delayed older admission queues second");
            handle
                .barrier_for_test()
                .await
                .expect("both admissions settle through the reactor barrier");
            handle
                .peer_lifecycle
                .send(BlockSyncPeerLifecycleEvent::Disconnected {
                    peer: peer_id.clone(),
                    session_id: older_session_id,
                })
                .expect("the older disconnect queues");
            handle
                .barrier_for_test()
                .await
                .expect("the stale disconnect settles through the reactor barrier");

            assert!(older_peer.cancel_token().is_cancelled());
            assert!(!newer_peer.cancel_token().is_cancelled());
            assert_eq!(
                handle.peer_snapshot().outbound_peers,
                1,
                "a delayed older PeerConnected event replaced the newer reactor session",
            );

            expect_initial_status(&mut newer_peer).await;
            newer_peer
                .try_send(BlockSyncMessage::Status(serving_status(tip)))
                .expect("the newer Status queues without yielding");
            newer_peer
                .try_send(BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 1,
                })
                .expect("the newer GetBlocks queues without yielding");
            match next_serving_action(&mut actions).await {
                BlockSyncAction::QueryBlocksByHeightRange {
                    peer, start, count, ..
                } => {
                    assert_eq!(peer, peer_id);
                    assert_eq!(start, block::Height(1));
                    assert_eq!(count, 1);
                }
                action => panic!("the live replacement produced {action:?}"),
            }

            reactor_task.abort();
            let _ = reactor_task.await;
        });
}

/// Delay a real request from one session until its replacement is live and prove
/// the reactor does not apply it to that replacement.
#[test]
fn gb_sm_17_superseded_routine_request_cannot_reach_replacement_session() {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("the serving ownership regression runtime builds")
        .block_on(async {
            let tip = (block::Height(3), block::Hash([3; 32]));
            let config = ZakuraBlockSyncConfig::default();
            let (_tip_tx, tip_rx) = watch::channel(tip);
            let startup = BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: tip.0,
                    verified_block_tip: tip.0,
                    verified_block_hash: tip.1,
                },
                tip,
                tip_rx,
                config.clone(),
            );
            let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
            let live_routine_sender = handle
                .routine_wiring
                .as_ref()
                .expect("the spawned reactor exposes routine wiring")
                .routine_to_reactor
                .clone();

            // Keep lifecycle events on the real reactor, but intercept routine
            // messages so the old request can be delivered after replacement.
            let mut intercepted_handle = handle.clone();
            let (intercepted_sender, mut intercepted_messages) = mpsc::channel(16);
            intercepted_handle
                .routine_wiring
                .as_mut()
                .expect("the intercepted handle exposes routine wiring")
                .routine_to_reactor = intercepted_sender;
            let peers = SyntheticBlockSyncPeers::new(config, intercepted_handle, 8);
            let peer_id = peer(0xa3);

            let mut older_peer = peers
                .connect_peer(peer_id.clone(), 1, ServicePeerDirection::Outbound)
                .expect("the older synthetic session connects");
            expect_initial_status(&mut older_peer).await;
            older_peer
                .try_send(BlockSyncMessage::Status(serving_status(tip)))
                .expect("the older Status queues without yielding");
            older_peer
                .try_send(BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 1,
                })
                .expect("the older GetBlocks queues without yielding");
            let older_request = next_serving_request(&mut intercepted_messages).await;

            let mut replacement_peer = peers
                .connect_peer(peer_id.clone(), 2, ServicePeerDirection::Outbound)
                .expect("the replacement synthetic session connects");
            expect_initial_status(&mut replacement_peer).await;
            assert!(older_peer.cancel_token().is_cancelled());
            replacement_peer
                .try_send(BlockSyncMessage::Status(serving_status(tip)))
                .expect("the replacement Status queues without yielding");
            replacement_peer
                .try_send(BlockSyncMessage::GetBlocks {
                    start_height: block::Height(2),
                    count: 1,
                })
                .expect("the replacement GetBlocks queues without yielding");
            let replacement_request = next_serving_request(&mut intercepted_messages).await;

            let older_generation = serving_request_generation(&older_request);
            let replacement_generation = serving_request_generation(&replacement_request);
            assert!(replacement_generation > older_generation);

            live_routine_sender
                .send(older_request)
                .await
                .expect("the delayed older request reaches the reactor");
            live_routine_sender
                .send(replacement_request)
                .await
                .expect("the current request reaches the reactor");

            let action = next_serving_action(&mut actions).await;
            match action {
                BlockSyncAction::QueryBlocksByHeightRange {
                    peer, start, count, ..
                } => {
                    assert_eq!(peer, peer_id);
                    assert_eq!(start, block::Height(2));
                    assert_eq!(count, 1);
                }
                action => panic!("the delayed older request produced {action:?}"),
            }
            assert!(
                actions.try_recv().is_err(),
                "the superseded request must not produce another action"
            );
            assert!(
                matches!(
                    replacement_peer
                        .recv_timeout(Duration::from_millis(1))
                        .await
                        .expect("the replacement outbound remains decodable"),
                    SyntheticBlockSyncReceive::TimedOut,
                ),
                "the superseded request must not send a frame to the replacement"
            );
            assert!(
                !replacement_peer.cancel_token().is_cancelled(),
                "the superseded request must not cancel the replacement"
            );

            replacement_peer
                .try_send(BlockSyncMessage::GetBlocks {
                    start_height: block::Height(4),
                    count: 1,
                })
                .expect("the replacement remains able to send GetBlocks");
            let later_request = next_serving_request(&mut intercepted_messages).await;
            live_routine_sender
                .send(later_request)
                .await
                .expect("the replacement's later request reaches the reactor");
            assert!(matches!(
                replacement_peer
                    .recv_timeout(Duration::from_secs(1))
                    .await
                    .expect("the replacement receives a later response"),
                SyntheticBlockSyncReceive::Message(BlockSyncMessage::RangeUnavailable {
                    start_height: block::Height(4),
                    count: 1,
                }),
            ));

            reactor_task.abort();
            let _ = reactor_task.await;
        });
}

/// Wait for the reactor's admission Status on one synthetic session.
async fn expect_initial_status(peer: &mut SyntheticBlockSyncPeer) {
    assert!(matches!(
        peer.recv_timeout(Duration::from_secs(1))
            .await
            .expect("the initial outbound frame decodes"),
        SyntheticBlockSyncReceive::Message(BlockSyncMessage::Status(_)),
    ));
}

/// Return the next action produced by inbound serving, skipping startup work queries.
async fn next_serving_action(actions: &mut mpsc::Receiver<BlockSyncAction>) -> BlockSyncAction {
    timeout(Duration::from_secs(1), async {
        loop {
            match actions.recv().await {
                Some(BlockSyncAction::QueryNeededBlocks { .. }) => {}
                Some(action) => return action,
                None => panic!("the block-sync action channel stays open"),
            }
        }
    })
    .await
    .expect("the serving request produces an action within one second")
}

/// Return the next serving request decoded by a real peer routine.
async fn next_serving_request(messages: &mut mpsc::Receiver<RoutineToReactor>) -> RoutineToReactor {
    timeout(Duration::from_secs(1), async {
        loop {
            let message = messages
                .recv()
                .await
                .expect("the intercepted routine channel stays open");
            if matches!(message, RoutineToReactor::ServeGetBlocks { .. }) {
                return message;
            }
        }
    })
    .await
    .expect("the peer routine decodes GetBlocks within one second")
}

/// Extract the session generation attached by the peer routine.
fn serving_request_generation(message: &RoutineToReactor) -> u64 {
    match message {
        RoutineToReactor::ServeGetBlocks {
            session_generation, ..
        } => *session_generation,
        message => panic!("expected ServeGetBlocks, got {message:?}"),
    }
}

/// Status used by both sides of the replacement regression.
fn serving_status(tip: (block::Height, block::Hash)) -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height::MIN,
        servable_high: tip.0,
        tip_hash: tip.1,
        max_blocks_per_response: 4,
        max_inflight_requests: 2,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}
