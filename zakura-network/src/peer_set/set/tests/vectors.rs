//! Fixed test vectors for the peer set.

use std::{
    cmp::max,
    collections::HashSet,
    iter,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use futures::{stream, FutureExt as _, StreamExt};
use tokio::time::timeout;
use tower::{discover::Change, Service, ServiceExt};

use zakura_chain::{
    block,
    parameters::{Network, NetworkUpgrade},
};

use crate::{
    constants::{CURRENT_NETWORK_PROTOCOL_VERSION, DEFAULT_MAX_CONNS_PER_IP},
    peer::{
        ClientRequest, ClientTestHarness, ConnectedAddr, LoadTrackedClient, MinimumPeerVersion,
    },
    peer_set::{inventory_registry::InventoryStatus, stall_tracker::FIND_RESPONSE_STALL_THRESHOLD},
    protocol::external::{types::Version, InventoryHash},
    BannedIps, BoxError, PeerSocketAddr, Request, Response, SharedPeerError,
};

use super::{PeerSetBuilder, PeerVersions};

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn mock_discovery_for_addresses(
    addresses: Vec<PeerSocketAddr>,
) -> (
    impl futures::Stream<Item = Result<Change<PeerSocketAddr, LoadTrackedClient>, BoxError>>,
    Vec<ClientTestHarness>,
) {
    let version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let mut handles = Vec::with_capacity(addresses.len());
    let changes = addresses
        .into_iter()
        .map(|addr| {
            let (client, handle) = ClientTestHarness::build().with_version(version).finish();
            handles.push(handle);
            Ok::<_, BoxError>(Change::Insert(addr, client.into()))
        })
        .collect::<Vec<_>>();

    (stream::iter(changes).chain(stream::pending()), handles)
}

#[test]
fn peer_set_ready_single_connection() {
    // We are going to use just one peer version in this test
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6_2,
        )],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // We will just use the first peer handle
    let mut client_handle = handles
        .into_iter()
        .next()
        .expect("we always have at least one client");

    // Client did not received anything yet
    assert!(client_handle
        .try_to_receive_outbound_client_request()
        .is_empty());

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Get a ready future
        let peer_ready_future = peer_set.ready();

        // If the readiness future gains a `Drop` impl, we want it to be called here.
        #[allow(unknown_lints)]
        #[allow(clippy::drop_non_drop)]
        std::mem::drop(peer_ready_future);

        // Peer set will remain ready for requests
        let peer_ready1 = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Make sure the client did not received anything yet
        assert!(client_handle
            .try_to_receive_outbound_client_request()
            .is_empty());

        // Make a call to the peer set that returns a future
        let fut = peer_ready1.call(Request::Peers);

        // Client received the request
        assert!(matches!(
            client_handle
                .try_to_receive_outbound_client_request()
                .request(),
            Some(ClientRequest {
                request: Request::Peers,
                ..
            })
        ));

        // Drop the future
        std::mem::drop(fut);

        // Peer set will remain ready for requests
        let peer_ready2 = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Get a new future calling a different request than before
        let _fut = peer_ready2.call(Request::MempoolTransactionIds);

        // Client received the request
        assert!(matches!(
            client_handle
                .try_to_receive_outbound_client_request()
                .request(),
            Some(ClientRequest {
                request: Request::MempoolTransactionIds,
                ..
            })
        ));
    });
}

#[test]
fn peer_set_ready_multiple_connections() {
    // Use three peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 3);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(3, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 3);

        // Stop some peer connections but not all
        handles[0].stop_connection_task().await;
        handles[1].stop_connection_task().await;

        // We can still make the peer set ready
        peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Stop the connection of the last peer
        handles[2].stop_connection_task().await;

        // Peer set hangs when no more connections are present
        let peer_ready = peer_set.ready();
        assert!(timeout(Duration::from_secs(10), peer_ready).await.is_err());
    });
}

#[test]
fn peer_set_rejects_connections_past_per_ip_limit() {
    const NUM_PEER_VERSIONS: usize = crate::constants::DEFAULT_MAX_CONNS_PER_IP + 1;

    // Use three peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: [peer_version; NUM_PEER_VERSIONS].into_iter().collect(),
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), NUM_PEER_VERSIONS);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(
            peer_ready.ready_services.len(),
            crate::constants::DEFAULT_MAX_CONNS_PER_IP
        );
    });
}

/// Check that a peer set with an empty inventory registry routes requests to a random ready peer.
#[test]
fn peer_set_route_inv_empty_registry() {
    let test_hash = block::Hash([0; 32]);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that one of the clients received the request
        let mut received_count = 0;
        for mut handle in handles {
            if let Some(ClientRequest { request, .. }) =
                handle.try_to_receive_outbound_client_request().request()
            {
                assert_eq!(sent_request, request);
                received_count += 1;
            }
        }

        assert_eq!(received_count, 1);
    });
}

#[test]
fn broadcast_all_queued_removes_banned_peers() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6_2,
        )],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        let banned_ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        peer_set.bans = BannedIps::with_banned_ip(banned_ip);

        let banned_addr: PeerSocketAddr = SocketAddr::new(banned_ip, 1).into();
        let mut remaining_peers = HashSet::new();
        remaining_peers.insert(banned_addr);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        peer_set.queued_broadcast_all = Some((Request::Peers, sender, remaining_peers));

        peer_set.broadcast_all_queued();

        if let Some((_req, _sender, remaining_peers)) = peer_set.queued_broadcast_all.take() {
            assert!(remaining_peers.is_empty());
        } else {
            assert!(receiver.try_recv().is_ok());
        }
    });
}

#[test]
fn ban_change_wakes_and_disconnects_every_connection_for_ip() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let banned_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    let banned_addr: PeerSocketAddr = SocketAddr::new(banned_ip, 1).into();
    let mapped_banned_addr: PeerSocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped()), 2).into();
    let unrelated_addr: PeerSocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)), 3).into();
    let (discovered_peers, _handles) =
        mock_discovery_for_addresses(vec![banned_addr, mapped_banned_addr, unrelated_addr]);
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let bans = BannedIps::default();

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();
        peer_set.bans = bans.clone();

        {
            let ready = peer_set.ready().await.expect("peer set is always ready");
            assert_eq!(ready.ready_services.len(), 3);
        }

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut context = Context::from_waker(&waker);
        let _ = Service::poll_ready(&mut peer_set, &mut context);
        let wake_count_before_ban = wake_counter.0.load(Ordering::SeqCst);

        let mut busy_service = peer_set
            .take_ready_service(&mapped_banned_addr)
            .expect("mapped banned peer is ready");
        let in_flight_response = busy_service.call(Request::Peers);
        peer_set.push_unready(mapped_banned_addr, busy_service);

        let (broadcast_sender, _broadcast_receiver) = tokio::sync::mpsc::channel(1);
        peer_set.queued_broadcast_all = Some((
            Request::Peers,
            broadcast_sender,
            HashSet::from([banned_addr, mapped_banned_addr]),
        ));
        peer_set.queued_sidecar_block_gossip = Some((
            Request::AdvertiseBlock(block::Hash([7; 32]), None),
            HashSet::from([banned_addr, mapped_banned_addr]),
        ));

        assert!(bans.insert(banned_ip));
        assert!(
            wake_counter.0.load(Ordering::SeqCst) > wake_count_before_ban,
            "inserting a new ban must wake the registered peer set"
        );

        let _ = Service::poll_ready(&mut peer_set, &mut context);

        assert!(!peer_set.ready_services.contains_key(&banned_addr));
        assert!(!peer_set.ready_services.contains_key(&mapped_banned_addr));
        assert!(!peer_set.cancel_handles.contains_key(&mapped_banned_addr));
        assert!(
            peer_set.unready_services.is_empty(),
            "the canceled in-flight service must be polled and dropped"
        );
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 0);
        assert!(
            peer_set.ready_services.contains_key(&unrelated_addr),
            "a ban must preserve peers with unrelated canonical IPs"
        );
        std::mem::drop(in_flight_response);
        assert!(
            peer_set
                .queued_broadcast_all
                .as_ref()
                .is_none_or(|(_, _, peers)| peers.is_empty()),
            "banned peers must be removed from queued broadcasts"
        );
        assert!(
            peer_set
                .queued_sidecar_block_gossip
                .as_ref()
                .is_none_or(|(_, peers)| peers.is_empty()),
            "banned peers must be removed from queued sidecar gossip"
        );
    });
}

#[test]
fn ban_before_waker_registration_rejects_discovered_peer() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let banned_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    let mapped_banned_addr: PeerSocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped()), 1).into();
    let (discovered_peers, _handles) = mock_discovery_for_addresses(vec![mapped_banned_addr]);
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let bans = BannedIps::with_banned_ip(banned_ip);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();
        peer_set.bans = bans;

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter);
        let mut context = Context::from_waker(&waker);
        let _ = Service::poll_ready(&mut peer_set, &mut context);

        assert!(!peer_set.ready_services.contains_key(&mapped_banned_addr));
        assert!(!peer_set.cancel_handles.contains_key(&mapped_banned_addr));
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 0);
    });
}

/// A mined block advertised via `AdvertiseBlockToAll` while a peer is unready
/// must reach that peer once it is ready again, not be silently dropped.
///
/// Regression test for the mined-block twin of the committed-tip sidecar-gossip
/// stall: `broadcast_all` queued a re-send for peers that were unready at
/// broadcast time, but its drain loop received each queued `send_multiple`
/// future and dropped it unpolled. Because [`crate::peer::Client::call`]
/// enqueues the peer request synchronously yet holds the response receiver
/// inside that future, dropping it makes the connection treat the request as
/// canceled and skip the block `inv`.
///
/// A plain "the peer received the request" assertion does *not* catch this: the
/// mock enqueues the `ClientRequest` synchronously, so it arrives even on the
/// buggy code. The distinguishing signal is whether the delivery future was
/// kept alive — i.e. the received request's response channel is not canceled.
#[test]
fn mined_block_gossip_to_unready_peer_is_delivered_not_canceled() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();
    tokio::time::pause();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    assert_eq!(handles.len(), 2);

    // `mock_peer_discovery` assigns ascending ports from 1, so the two peers
    // share IP `127.0.0.1` and differ only by port. The harnesses are returned
    // in the same order as those ports.
    let mut handles = handles.into_iter();
    let handle_1 = handles.next().expect("first peer harness");
    let handle_2 = handles.next().expect("second peer harness");
    let addr_1: PeerSocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1).into();
    let addr_2: PeerSocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2).into();

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            // Both peers share an IP, so lift the per-IP cap above the default of 1.
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        {
            let ready = peer_set.ready().await.expect("peer set is always ready");
            assert_eq!(ready.ready_services.len(), 2);
        }

        // Force both peers unready, as if a request to each were in flight. With
        // no ready peers, `broadcast_all`'s initial send is empty and its drain
        // loop runs immediately; both peers are then served only by the queued
        // re-send delivered from `broadcast_all_queued`.
        for addr in [addr_1, addr_2] {
            let svc = peer_set.take_ready_service(&addr).expect("peer is ready");
            peer_set.push_unready(addr, svc);
        }

        // Advertise a mined block to all peers while both are unready, and drive
        // the returned future to completion (spawned, as the mined-block gossip
        // caller in `zakurad::components::sync::gossip` does).
        let hash = block::Hash([7; 32]);
        let broadcast_handle =
            tokio::spawn(peer_set.broadcast_all(Request::AdvertiseBlockToAll(hash)));

        // Drive the peer set so both peers re-ready and `broadcast_all_queued`
        // delivers the queued gossip; yield so the spawned drain loop processes
        // it. Once every queued peer has been delivered, the drain loop drains
        // and the broadcast future completes — that completion is the point at
        // which the delivery future has definitively been spawned (fixed) or
        // dropped (buggy), so we can check the response channel deterministically.
        let mut broadcast_finished = false;
        for _ in 0..16 {
            {
                let _ = peer_set.ready().await.expect("peer set is always ready");
            }
            tokio::task::yield_now().await;
            if broadcast_handle.is_finished() {
                broadcast_finished = true;
                break;
            }
        }
        assert!(
            broadcast_finished,
            "the mined-block broadcast future should complete once queued deliveries drain",
        );
        broadcast_handle
            .await
            .expect("broadcast task should not panic")
            .expect("broadcast_all should succeed");

        // Both originally-unready peers must have received the mined-block inv,
        // and — crucially — the delivery future must have been kept alive rather
        // than dropped. On the buggy code the future is dropped, cancelling the
        // response channel, which the connection task treats as a canceled
        // request and skips the block inv.
        for mut handle in [handle_1, handle_2] {
            let client_request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("each once-unready peer should receive the queued mined-block gossip");
            assert!(
                matches!(client_request.request, Request::AdvertiseBlockToAll(h) if h == hash),
                "expected the mined-block advertisement, got {:?}",
                client_request.request,
            );
            assert!(
                !client_request.tx.is_canceled(),
                "the queued send future must be spawned, not dropped: a dropped future \
                 cancels the response channel and the connection skips the block inv",
            );
        }
    });
}

/// A mined block advertised while peers are ready reaches those peers
/// immediately, guarding the common path against a regression from the fix.
#[test]
fn mined_block_gossip_reaches_ready_peers() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();
    tokio::time::pause();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        {
            let ready = peer_set.ready().await.expect("peer set is always ready");
            assert_eq!(ready.ready_services.len(), 2);
        }

        // With every peer ready, `broadcast_all` sends to them synchronously and
        // has nothing to queue. Keep the future alive so its response receivers
        // are not dropped before we inspect the delivered requests.
        let hash = block::Hash([5; 32]);
        let _broadcast_fut = peer_set.broadcast_all(Request::AdvertiseBlockToAll(hash));

        let mut received = 0;
        for mut handle in handles {
            if let Some(client_request) = handle.try_to_receive_outbound_client_request().request()
            {
                assert!(
                    matches!(client_request.request, Request::AdvertiseBlockToAll(h) if h == hash),
                    "expected the mined-block advertisement, got {:?}",
                    client_request.request,
                );
                received += 1;
            }
        }
        assert_eq!(
            received, 2,
            "both ready peers should receive the mined block"
        );
    });
}

#[test]
fn remove_unready_peer_clears_cancel_handle_and_updates_counts() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6_2,
        )],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Prepare a banned IP map (not strictly required for remove(), but keeps
        // the test's setup similar to real-world conditions).
        let banned_ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        peer_set.bans = BannedIps::with_banned_ip(banned_ip);

        // Create a cancel handle as if a request was in-flight to `banned_addr`.
        let banned_addr: PeerSocketAddr = SocketAddr::new(banned_ip, 1).into();
        let (tx, _rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_set.cancel_handles.insert(banned_addr, tx);

        // The peer is counted as 1 peer with that IP.
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 1);

        // Remove the peer (simulates a discovery::Remove or equivalent).
        peer_set.remove(&banned_addr);

        // After removal, the cancel handle should be gone and the count zero.
        assert!(!peer_set.cancel_handles.contains_key(&banned_addr));
        assert_eq!(peer_set.num_peers_with_ip(banned_ip), 0);
    });
}

/// A dual-stack listener reports an inbound IPv4 peer as an IPv4-mapped IPv6 address
/// (`::ffff:A.B.C.D`). The per-IP connection accounting must treat that mapped form and
/// its canonical IPv4 form as the **same host**, so `max_conns_per_ip` can't be evaded by
/// alternating the two forms.
#[test]
fn per_ip_accounting_treats_mapped_and_canonical_as_same_host() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6_2,
        )],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        let canonical_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 7).into();
        let mapped_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 7).to_ipv6_mapped().into();
        assert_ne!(
            canonical_ip, mapped_ip,
            "the two forms are distinct IpAddrs"
        );

        // Insert a single connection keyed by the canonical form...
        let canonical_addr: PeerSocketAddr = SocketAddr::new(canonical_ip, 8233).into();
        let (tx, _rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_set.cancel_handles.insert(canonical_addr, tx);

        // ...both address forms must see that one connection.
        assert_eq!(peer_set.num_peers_with_ip(canonical_ip), 1);
        assert_eq!(
            peer_set.num_peers_with_ip(mapped_ip),
            1,
            "mapped-form query must find the canonically-keyed peer"
        );

        // Reverse the key/query roles: key the connection by the mapped form instead.
        peer_set.cancel_handles.clear();
        let mapped_addr: PeerSocketAddr = SocketAddr::new(mapped_ip, 8233).into();
        let (tx, _rx) =
            crate::peer_set::set::oneshot::channel::<crate::peer_set::set::CancelClientWork>();
        peer_set.cancel_handles.insert(mapped_addr, tx);

        assert_eq!(peer_set.num_peers_with_ip(mapped_ip), 1);
        assert_eq!(
            peer_set.num_peers_with_ip(canonical_ip),
            1,
            "canonical-form query must find the mapped-keyed peer"
        );
    });
}

/// The peer-set ban re-checks key on the **canonical** IP. A banned peer reconnecting as
/// its IPv4-mapped IPv6 form (`::ffff:A.B.C.D`, as seen on a dual-stack listener) must
/// still be treated as banned even though the ban map holds the canonical `A.B.C.D`.
///
/// This drives the ban filter in `broadcast_all_queued`; all three peer-set ban re-checks
/// (`poll_unready`, `poll_ready_peer_errors`, and this filter) share the same `canonical_ip`
/// canonicalization, so this exercises that shared path.
#[test]
fn broadcast_all_queued_bans_mapped_ipv6_against_canonical_ban() {
    let peer_versions = PeerVersions {
        peer_versions: vec![Version::min_specified_for_upgrade(
            &Network::Mainnet,
            NetworkUpgrade::Nu6_2,
        )],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, _handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // The ban map holds the canonical IPv4 address.
        let canonical_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 7).into();
        peer_set.bans = BannedIps::with_banned_ip(canonical_ip);

        // The banned peer reconnects in its IPv4-mapped IPv6 form.
        let mapped_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 7).to_ipv6_mapped().into();
        let mapped_addr: PeerSocketAddr = SocketAddr::new(mapped_ip, 8233).into();
        let mut remaining_peers = HashSet::new();
        remaining_peers.insert(mapped_addr);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        peer_set.queued_broadcast_all = Some((Request::Peers, sender, remaining_peers));

        peer_set.broadcast_all_queued();

        // The mapped-form peer must be dropped by the (canonical) ban filter, so no peers
        // remain queued for the re-send. On un-canonicalized code the mapped peer would
        // survive the filter and this assertion would fail.
        if let Some((_req, _sender, remaining_peers)) = peer_set.queued_broadcast_all.take() {
            assert!(
                remaining_peers.is_empty(),
                "banned mapped-IPv6 peer must be filtered by the canonical ban"
            );
        } else {
            assert!(receiver.try_recv().is_ok());
        }
    });
}

/// Check that a peer set routes inventory requests to a peer that has advertised that inventory.
#[test]
fn peer_set_route_inv_advertised_registry() {
    peer_set_route_inv_advertised_registry_order(true);
    peer_set_route_inv_advertised_registry_order(false);
}

fn peer_set_route_inv_advertised_registry_order(advertised_first: bool) {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = if advertised_first {
        "127.0.0.1:1"
    } else {
        "127.0.0.1:2"
    }
    .parse()
    .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_available(test_inv, test_peer);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Advertise some inventory
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that the client that advertised the inventory received the request
        let advertised_handle = if advertised_first {
            &mut handles[0]
        } else {
            &mut handles[1]
        };

        if let Some(ClientRequest { request, .. }) = advertised_handle
            .try_to_receive_outbound_client_request()
            .request()
        {
            assert_eq!(sent_request, request);
        } else {
            panic!("inv request not routed to advertised peer");
        }

        let other_handle = if advertised_first {
            &mut handles[1]
        } else {
            &mut handles[0]
        };

        assert!(
            other_handle
                .try_to_receive_outbound_client_request()
                .request()
                .is_none(),
            "request routed to non-advertised peer",
        );
    });
}

/// Check that a peer set routes inventory requests to peers that are not missing that inventory.
#[test]
fn peer_set_route_inv_missing_registry() {
    peer_set_route_inv_missing_registry_order(true);
    peer_set_route_inv_missing_registry_order(false);
}

fn peer_set_route_inv_missing_registry_order(missing_first: bool) {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = if missing_first {
        "127.0.0.1:1"
    } else {
        "127.0.0.1:2"
    }
    .parse()
    .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_missing(test_inv, test_peer);

    // Use two peers with the same version
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version, peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get peers and client handles of them
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 2);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .build();

        // Mark some inventory as missing
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 2);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let _fut = peer_ready.call(sent_request.clone());

        // Check that the client missing the inventory did not receive the request
        let missing_handle = if missing_first {
            &mut handles[0]
        } else {
            &mut handles[1]
        };

        assert!(
            missing_handle
                .try_to_receive_outbound_client_request()
                .request()
                .is_none(),
            "request routed to missing peer",
        );

        // Check that the client that was not missing the inventory received the request
        let other_handle = if missing_first {
            &mut handles[1]
        } else {
            &mut handles[0]
        };

        if let Some(ClientRequest { request, .. }) = other_handle
            .try_to_receive_outbound_client_request()
            .request()
        {
            assert_eq!(sent_request, request);
        } else {
            panic!(
                "inv request should have been routed to the only peer not missing the inventory"
            );
        }
    });
}

/// Check that a peer set fails inventory requests if all peers are missing that inventory.
#[test]
fn peer_set_route_inv_all_missing_fail() {
    let test_hash = block::Hash([0; 32]);
    let test_inv = InventoryHash::Block(test_hash);

    // Hard-code the fixed test address created by mock_peer_discovery
    // TODO: add peer test addresses to ClientTestHarness
    let test_peer = "127.0.0.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let test_change = InventoryStatus::new_missing(test_inv, test_peer);

    // Use one peer
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    // Start the runtime
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    // Pause the runtime's timer so that it advances automatically.
    //
    // CORRECTNESS: This test does not depend on external resources that could really timeout, like
    // real network connections.
    tokio::time::pause();

    // Get the peer and its client handle
    let (discovered_peers, mut handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Make sure we have the right number of peers
    assert_eq!(handles.len(), 1);

    runtime.block_on(async move {
        // Build a peerset
        let (mut peer_set, mut peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version.clone())
            .build();

        // Mark the inventory as missing for all peers
        peer_set_guard
            .inventory_sender()
            .as_mut()
            .expect("unexpected missing inv sender")
            .send(test_change)
            .expect("unexpected dropped receiver");

        // Get peerset ready
        let peer_ready = peer_set
            .ready()
            .await
            .expect("peer set service is always ready");

        // Check we have the right amount of ready services
        assert_eq!(peer_ready.ready_services.len(), 1);

        // Send an inventory-based request
        let sent_request = Request::BlocksByHash(iter::once(test_hash).collect());
        let response_fut = peer_ready.call(sent_request.clone());

        // Check that the client missing the inventory did not receive the request
        let missing_handle = &mut handles[0];

        assert!(
            missing_handle
                    .try_to_receive_outbound_client_request()
                    .request().is_none(),
            "request routed to missing peer",
        );

        // Check that the response is a synthetic error
        let response = response_fut.await;
        assert_eq!(
            response
                .expect_err("peer set should return an error (not a Response)")
                .downcast_ref::<SharedPeerError>()
                .expect("peer set should return a boxed SharedPeerError")
                .inner_debug(),
            "NotFoundRegistry([Block(block::Hash(\"0000000000000000000000000000000000000000000000000000000000000000\"))])"
        );
    });
}

/// Check that empty `FindBlocks` responses do not trigger stall tracking when the node is at the
/// chain tip, so peers that correctly return no hashes are not disconnected.
#[test]
fn find_blocks_stall_not_tracked_when_at_tip() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, best_tip) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Simulate being at the chain tip.
    best_tip.send_best_tip_height(Some(block::Height(2_500_000)));
    best_tip.send_estimated_distance_to_network_chain_tip(Some(0));

    let mut handle = handles.into_iter().next().expect("there is one peer");

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();

        // Send more `FindBlocks` requests than the stall threshold, each
        // returning an empty response. If stall events were tracked, the peer
        // would be disconnected after the third response.
        let request_count = FIND_RESPONSE_STALL_THRESHOLD + 1;

        for _ in 0..request_count {
            let peer_ready = peer_set.ready().await.expect("peer set is ready");

            let response_fut = peer_ready.call(Request::FindBlocks {
                known_blocks: vec![],
                stop: None,
            });

            let client_request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("peer received the request");

            // Reply with an empty `BlockHashes` response: protocol-correct at tip.
            let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));

            response_fut.await.expect("response received");
        }

        assert!(
            handle.wants_connection_heartbeats(),
            "peer should not be disconnected when at tip"
        );
    });
}

/// Check that the sync stall detector does not disconnect the configured zcashd-compat sidecar.
#[test]
fn find_blocks_stall_not_tracked_for_zcashd_compat() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let sidecar_ip = Ipv4Addr::LOCALHOST;
    let sidecar_addr: PeerSocketAddr =
        SocketAddr::new(IpAddr::V6(sidecar_ip.to_ipv6_mapped()), 1).into();
    let (sidecar, mut sidecar_handle) = ClientTestHarness::build()
        .with_version(CURRENT_NETWORK_PROTOCOL_VERSION)
        .with_connected_addr(ConnectedAddr::new_inbound_direct(sidecar_addr))
        .finish();
    let discovered_peers = stream::iter([Ok::<_, BoxError>(Change::Insert(
        sidecar_addr,
        sidecar.into(),
    ))])
    .chain(stream::pending());
    let (minimum_peer_version, best_tip) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Simulate Zebra syncing ahead of its zcashd-compat sidecar.
    best_tip.send_best_tip_height(Some(block::Height(2_490_000)));
    best_tip.send_estimated_distance_to_network_chain_tip(Some(10_000));

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_block_gossip_peer_ips(vec![sidecar_ip.into()])
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();

        for _ in 0..FIND_RESPONSE_STALL_THRESHOLD {
            let peer_ready = peer_set.ready().await.expect("peer set is ready");
            let response_fut = peer_ready.call(Request::FindBlocks {
                known_blocks: vec![],
                stop: None,
            });
            let client_request = sidecar_handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("sidecar received the request");
            let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));
            response_fut.await.expect("response received");
        }

        // If sidecar responses were tracked, this poll would process the final
        // stall event and disconnect it.
        let _ = peer_set.ready().now_or_never();

        assert!(
            sidecar_handle.wants_connection_heartbeats(),
            "zcashd-compat sidecar should not be disconnected by the sync stall detector"
        );
    });
}

/// Check that empty `FindBlocks` responses trigger stall tracking when the node is syncing,
/// and that the peer is disconnected after exceeding the stall threshold.
#[test]
fn find_blocks_stall_tracked_when_syncing() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, best_tip) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Simulate being far behind the chain tip, as during initial sync.
    best_tip.send_best_tip_height(Some(block::Height(2_490_000)));
    best_tip.send_estimated_distance_to_network_chain_tip(Some(10_000));

    let mut handle = handles.into_iter().next().expect("there is one peer");

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();

        for _ in 0..FIND_RESPONSE_STALL_THRESHOLD {
            let peer_ready = peer_set.ready().await.expect("peer set is ready");

            let response_fut = peer_ready.call(Request::FindBlocks {
                known_blocks: vec![],
                stop: None,
            });

            let client_request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("peer received the request");

            let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));

            response_fut.await.expect("response received");
        }

        // Drain the final stall event and process the disconnect.
        let _ = peer_set.ready().now_or_never();

        assert!(
            !handle.wants_connection_heartbeats(),
            "peer should be disconnected after stall threshold is reached while syncing"
        );
    });
}

/// Check that stall tracking is active when the chain tip state is unknown, so
/// stalling peers are still disconnected before the first block is synced.
#[test]
fn find_blocks_stall_tracked_when_tip_unknown() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, _best_tip) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    let mut handle = handles.into_iter().next().expect("there is one peer");

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();

        for _ in 0..FIND_RESPONSE_STALL_THRESHOLD {
            let peer_ready = peer_set.ready().await.expect("peer set is ready");

            let response_fut = peer_ready.call(Request::FindBlocks {
                known_blocks: vec![],
                stop: None,
            });

            let client_request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("peer received the request");

            let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));

            response_fut.await.expect("response received");
        }

        let _ = peer_set.ready().now_or_never();

        assert!(
            !handle.wants_connection_heartbeats(),
            "peer should be disconnected when tip is unknown and stall threshold is reached"
        );
    });
}

/// Check that stall counts accumulated while syncing are preserved across a tip
/// transition, so a peer cannot avoid detection by temporarily becoming useful
/// as the node reaches the tip.
#[test]
fn find_blocks_stall_count_preserved_across_tip_transition() {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);
    let peer_versions = PeerVersions {
        peer_versions: vec![peer_version],
    };

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let (discovered_peers, handles) = peer_versions.mock_peer_discovery();
    let (minimum_peer_version, best_tip) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    // Start syncing: one stall away from disconnect.
    best_tip.send_best_tip_height(Some(block::Height(2_490_000)));
    best_tip.send_estimated_distance_to_network_chain_tip(Some(10_000));

    let mut handle = handles.into_iter().next().expect("there is one peer");

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered_peers)
            .with_minimum_peer_version(minimum_peer_version)
            .build();

        for _ in 0..FIND_RESPONSE_STALL_THRESHOLD - 1 {
            let peer_ready = peer_set.ready().await.expect("peer set is ready");

            let response_fut = peer_ready.call(Request::FindBlocks {
                known_blocks: vec![],
                stop: None,
            });

            let client_request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .expect("peer received the request");

            let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));

            response_fut.await.expect("response received");
        }

        // Transition to at-tip. The empty response should not clear or advance
        // the accumulated stall count.
        best_tip.send_best_tip_height(Some(block::Height(2_500_000)));
        best_tip.send_estimated_distance_to_network_chain_tip(Some(0));

        let peer_ready = peer_set.ready().await.expect("peer set is ready");
        let response_fut = peer_ready.call(Request::FindBlocks {
            known_blocks: vec![],
            stop: None,
        });
        let client_request = handle
            .try_to_receive_outbound_client_request()
            .request()
            .expect("peer received the request");
        let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));
        response_fut.await.expect("response received");

        // Transition back to syncing. One more empty response reaches the
        // threshold because the previous at-tip response did not reset the
        // accumulated count.
        best_tip.send_estimated_distance_to_network_chain_tip(Some(10_000));

        let peer_ready = peer_set.ready().await.expect("peer set is ready");
        let response_fut = peer_ready.call(Request::FindBlocks {
            known_blocks: vec![],
            stop: None,
        });
        let client_request = handle
            .try_to_receive_outbound_client_request()
            .request()
            .expect("peer received the request");
        let _ = client_request.tx.send(Ok(Response::BlockHashes(vec![])));
        response_fut.await.expect("response received");

        let _ = peer_set.ready().now_or_never();

        assert!(
            !handle.wants_connection_heartbeats(),
            "peer should be disconnected because its syncing stall count was preserved"
        );
    });
}

/// Returns the block hash of the next `AdvertiseBlock` request the mock peer
/// received, or `None` if it received nothing. Panics on any other request.
fn recv_advertise_block(handle: &mut ClientTestHarness) -> Option<block::Hash> {
    match handle.try_to_receive_outbound_client_request().request() {
        Some(ClientRequest {
            request: Request::AdvertiseBlock(hash, _),
            ..
        }) => Some(hash),
        Some(other) => panic!("unexpected outbound request: {:?}", other.request),
        None => None,
    }
}

/// Builds a discovery stream of two ready inbound-direct peers: a sidecar at
/// `127.0.0.1` (a configured block-gossip carve-out IP) and an ordinary peer at
/// `127.0.0.2`. Returns the stream, the two addresses, and a mock handle for
/// each peer.
fn sidecar_and_ordinary_discovery() -> (
    impl futures::Stream<Item = Result<Change<PeerSocketAddr, LoadTrackedClient>, BoxError>>,
    PeerSocketAddr,
    PeerSocketAddr,
    ClientTestHarness,
    ClientTestHarness,
) {
    let peer_version = Version::min_specified_for_upgrade(&Network::Mainnet, NetworkUpgrade::Nu6_2);

    let sidecar_addr: PeerSocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1).into();
    let ordinary_addr: PeerSocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 2).into();

    let (sidecar_client, sidecar_handle) = ClientTestHarness::build()
        .with_version(peer_version)
        .with_connected_addr(ConnectedAddr::InboundDirect { addr: sidecar_addr })
        .finish();
    let (ordinary_client, ordinary_handle) = ClientTestHarness::build()
        .with_version(peer_version)
        .with_connected_addr(ConnectedAddr::InboundDirect {
            addr: ordinary_addr,
        })
        .finish();

    let discovered = stream::iter([
        Ok::<_, BoxError>(Change::Insert(
            sidecar_addr,
            LoadTrackedClient::from(sidecar_client),
        )),
        Ok::<_, BoxError>(Change::Insert(
            ordinary_addr,
            LoadTrackedClient::from(ordinary_client),
        )),
    ])
    .chain(stream::pending());

    (
        discovered,
        sidecar_addr,
        ordinary_addr,
        sidecar_handle,
        ordinary_handle,
    )
}

/// A block gossip that fires while a configured sidecar peer is unready must be
/// queued for that peer, not silently dropped.
///
/// Regression test: the "always include sidecars" carve-out in
/// [`PeerSet::select_block_broadcast_peers`] could only cover *ready* sidecars.
/// A sidecar that was unready when the committed-tip gossip fired was excluded,
/// and — because it follows a single upstream and learns the tip only from
/// block `inv`s — it then stalled until a later gossip happened to coincide with
/// a ready service.
#[test]
fn unready_sidecar_block_gossip_is_queued_not_dropped() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();
    tokio::time::pause();

    let (discovered, sidecar_addr, _ordinary_addr, mut sidecar_handle, mut ordinary_handle) =
        sidecar_and_ordinary_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_peer_version)
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .with_block_gossip_peer_ips(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .build();

        {
            let ready = peer_set.ready().await.expect("peer set is always ready");
            assert_eq!(ready.ready_services.len(), 2);
        }

        // Force the sidecar unready, as if a request to it were in flight.
        let sidecar_svc = peer_set
            .take_ready_service(&sidecar_addr)
            .expect("sidecar is ready");
        peer_set.push_unready(sidecar_addr, sidecar_svc);
        assert!(peer_set.cancel_handles.contains_key(&sidecar_addr));
        assert!(!peer_set.ready_services.contains_key(&sidecar_addr));

        // Gossip a block while the sidecar is unready.
        let hash = block::Hash([7; 32]);
        let _fut = peer_set.route_block_broadcast(Request::AdvertiseBlock(hash, None));

        // The ordinary ready peer received the gossip immediately.
        assert_eq!(
            recv_advertise_block(&mut ordinary_handle),
            Some(hash),
            "an ordinary ready peer should receive the block gossip immediately",
        );

        // The unready sidecar could not receive it synchronously ...
        assert_eq!(
            recv_advertise_block(&mut sidecar_handle),
            None,
            "an unready sidecar cannot be sent to synchronously",
        );

        // ... but the fix queued it for redelivery rather than dropping it.
        let (queued_req, queued_peers) = peer_set
            .queued_sidecar_block_gossip
            .as_ref()
            .expect("block gossip should be queued for the unready sidecar");
        assert_eq!(*queued_req, Request::AdvertiseBlock(hash, None));
        assert!(
            queued_peers.contains(&sidecar_addr),
            "the unready sidecar should be queued for redelivery"
        );
    });
}

/// A block gossip queued for an unready sidecar is delivered once the sidecar
/// becomes ready again, through the [`PeerSet`] poll cycle. This exercises the
/// `poll_ready` wiring, not just the helper in isolation.
#[test]
fn queued_sidecar_block_gossip_delivered_once_ready() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();
    tokio::time::pause();

    let (discovered, sidecar_addr, _ordinary_addr, mut sidecar_handle, _ordinary_handle) =
        sidecar_and_ordinary_discovery();
    let (minimum_peer_version, _best_tip_height) =
        MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_peer_version)
            .max_conns_per_ip(max(2, DEFAULT_MAX_CONNS_PER_IP))
            .with_block_gossip_peer_ips(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .build();

        {
            let ready = peer_set.ready().await.expect("peer set is always ready");
            assert_eq!(ready.ready_services.len(), 2);
        }

        // Sidecar unready, then a block is gossiped: it gets queued (the queuing
        // itself is asserted by the test above).
        let sidecar_svc = peer_set
            .take_ready_service(&sidecar_addr)
            .expect("sidecar is ready");
        peer_set.push_unready(sidecar_addr, sidecar_svc);

        let hash = block::Hash([9; 32]);
        let _fut = peer_set.route_block_broadcast(Request::AdvertiseBlock(hash, None));
        assert!(peer_set.queued_sidecar_block_gossip.is_some());
        assert_eq!(recv_advertise_block(&mut sidecar_handle), None);

        // Driving the peer set re-readies the sidecar (via `poll_unready`) and
        // then delivers the queued gossip (via `deliver_queued_sidecar_block_gossip`),
        // both inside `poll_ready`.
        let mut delivered = None;
        for _ in 0..8 {
            {
                let _ = peer_set.ready().await.expect("peer set is always ready");
            }
            // Let the spawned send future drain, and allow another poll if the
            // sidecar needed an extra readiness cycle.
            tokio::task::yield_now().await;
            if let Some(received_hash) = recv_advertise_block(&mut sidecar_handle) {
                delivered = Some(received_hash);
                break;
            }
        }

        assert_eq!(
            delivered,
            Some(hash),
            "the sidecar should receive the queued block gossip once it is ready again",
        );
        assert!(
            peer_set.queued_sidecar_block_gossip.is_none(),
            "the queue should be cleared after delivery",
        );
    });
}
