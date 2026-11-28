//! A dial to a node's earlier ID reaches the node under the ID it has now
//! (zakura-quic TLS-7). The dialer registers it once, under that ID, and
//! closes a redundant dial if the peer is already registered at the local check.
//!
//! A connection also says which earlier IDs its peer proved, in both
//! directions. A mining gateway that blocks name by an earlier ID keeps its
//! held place, and a pinned peer entry that names an earlier ID still finds
//! its peer.

use super::*;
use crate::zakura::testkit::{ZakuraTestNode, TEST_NET_TIMEOUT};

/// Waits until `node`'s registered peers are exactly `expected`.
async fn registers(node: &ZakuraTestNode, expected: &[NodeId]) {
    let expected: Vec<_> = expected
        .iter()
        .map(|id| ZakuraPeerId::from_node_id(*id))
        .collect();
    let mut registered = node.supervisor().subscribe();
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        registered.wait_for(|peers| *peers == expected),
    )
    .await
    .expect("the peers register before the timeout")
    .expect("the supervisor outlives the test");
}

/// The connection slots `node` charges to the remote node `id`.
fn slots(node: &ZakuraTestNode, id: &NodeId) -> usize {
    let endpoint = node.endpoint();
    let slots = endpoint.supervisor.ip_slots.counts().node_connections(id);
    slots.total()
}

/// One dial to the ID a node had before it added a key: the node is
/// registered once, under the ID it has now, and the dial's connection slot
/// is charged to that ID.
#[tokio::test]
async fn a_dial_to_an_earlier_id_registers_the_node_under_its_current_id() {
    let listener = ZakuraTestNode::builder(9301)
        .extended_credential()
        .spawn()
        .await
        .expect("listener spawns");
    let dialer = ZakuraTestNode::builder(9302)
        .spawn()
        .await
        .expect("dialer spawns");
    let earlier = listener.earlier_node_addr().await;
    let current = listener.node_addr().await.id;
    assert_ne!(earlier.id, current, "the added key changes the node ID");

    dialer
        .connect_native_to_addr(earlier.clone(), TEST_NET_TIMEOUT)
        .await
        .expect("a dial to the earlier ID connects");

    registers(&dialer, &[current]).await;
    registers(&listener, &[dialer.node_addr().await.id]).await;
    let moved = dialer.supervisor().moved_node_ids();
    assert_eq!(moved.borrow().current(earlier.id), current);
    assert_eq!(slots(&dialer, &current), 1);
    assert_eq!(slots(&dialer, &earlier.id), 0);

    dialer.shutdown().await;
    listener.shutdown().await;
}

/// Only a dial that TLS authenticated says where a node moved. A dial to
/// an ID the listener can't prove fails in the handshake, registers nothing
/// and teaches the dialer no move, so no one can point another node's ID at
/// themselves by answering dials to it.
#[tokio::test]
async fn a_dial_the_listener_cannot_prove_learns_no_move() {
    let listener = ZakuraTestNode::builder(9307)
        .extended_credential()
        .spawn()
        .await
        .expect("listener spawns");
    let dialer = ZakuraTestNode::builder(9308)
        .spawn()
        .await
        .expect("dialer spawns");
    // Another node's ID, at the listener's addresses.
    let other = crate::zakura::testkit::LocalEndpointFactory::secret_key(9309).public();
    let claimed = NodeAddr::with_addrs(other, listener.node_addr().await.direct);

    let endpoint = dialer.endpoint();
    let dial = tokio::time::timeout(
        TEST_NET_TIMEOUT,
        serve_reported_native_dial_connection(
            &endpoint,
            claimed,
            dialer.limits(),
            None,
            NativeDialKind::Discovery {
                ordinary_cap: dialer.discovery().ordinary_connection_cap,
            },
        ),
    )
    .await
    .expect("the dial ends before the timeout");
    assert!(dial.is_err(), "the listener is not the dialed node");

    let moved = dialer.supervisor().moved_node_ids();
    assert_eq!(moved.borrow().current(other), other);
    assert!(dialer.supervisor().subscribe().borrow().is_empty());
    assert_eq!(slots(&dialer, &other), 0);
    assert_eq!(slots(&dialer, &listener.node_addr().await.id), 0);

    dialer.shutdown().await;
    listener.shutdown().await;
}

/// A node is registered under the ID it has now, and a stale record makes
/// the dialer dial its earlier ID as well. That dial is closed before its
/// control handshake and reported as one that registered nothing.
///
/// The registered connection is older than
/// [`ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE`] when the dial arrives, so
/// a second registration at either end would cancel it. Neither end's
/// registered peers change. The test waits that age in real time.
#[tokio::test]
async fn a_dial_to_an_earlier_id_leaves_the_registered_connection() {
    let listener = ZakuraTestNode::builder(9303)
        .extended_credential()
        .spawn()
        .await
        .expect("listener spawns");
    let dialer = ZakuraTestNode::builder(9304)
        .spawn()
        .await
        .expect("dialer spawns");
    let earlier = listener.earlier_node_addr().await;
    let current = listener.node_addr().await.id;

    dialer
        .connect_native(&listener, TEST_NET_TIMEOUT)
        .await
        .expect("a dial to the current ID connects");
    registers(&listener, &[dialer.node_addr().await.id]).await;
    tokio::time::sleep(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_millis(500)).await;
    let mut dialer_peers = dialer.supervisor().subscribe();
    let mut listener_peers = listener.supervisor().subscribe();
    dialer_peers.mark_unchanged();
    listener_peers.mark_unchanged();

    let (authenticated, mut proved) = oneshot::channel();
    let (registration, registered) = oneshot::channel();
    let endpoint = dialer.endpoint();
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        serve_reported_native_dial_connection(
            &endpoint,
            earlier.clone(),
            dialer.limits(),
            Some(DialReport {
                authenticated,
                registration,
            }),
            NativeDialKind::Discovery {
                ordinary_cap: dialer.discovery().ordinary_connection_cap,
            },
        ),
    )
    .await
    .expect("the redundant dial ends before the timeout")
    .expect("a redundant dial is not a failure");
    assert_eq!(
        registered.await.unwrap(),
        DialRegistration::AlreadyConnected
    );
    assert_eq!(
        proved.try_recv().map(|proved| proved.node_id),
        Ok(current),
        "the dial reports the ID its own handshake authenticated"
    );
    let moved = dialer.supervisor().moved_node_ids();
    assert_eq!(moved.borrow().current(earlier.id), current);
    assert_eq!(slots(&dialer, &earlier.id), 0);
    assert_eq!(slots(&dialer, &current), 1);

    // Long enough for the listener to have run a control handshake and
    // registered a second connection, had the dialer started one.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for (end, peers) in [("dialer", &dialer_peers), ("listener", &listener_peers)] {
        assert!(
            !peers
                .has_changed()
                .expect("the supervisor outlives the test"),
            "the {end}'s registered connection was replaced"
        );
    }
    registers(&dialer, &[current]).await;

    dialer.shutdown().await;
    listener.shutdown().await;
}

/// Fills `endpoint`'s ordinary connection slots in `inbound`'s direction,
/// until a claim for an unrelated node is refused. The guards hold the slots.
fn fill_ordinary_slots(endpoint: &ZakuraEndpoint, inbound: bool) -> Vec<ConnectionAdmissionGuard> {
    let mut held = Vec::new();
    for index in 0u32.. {
        let mut id = [if inbound { 0xA4 } else { 0xA5 }; 32];
        id[..4].copy_from_slice(&index.to_be_bytes());
        match endpoint
            .handler
            .claim_connection(NodeId::from_bytes(&id), inbound)
        {
            Ok(slot) => held.push(slot),
            Err(_) => break,
        }
    }
    held
}

/// Dials `addr` from `endpoint` once, as discovery does, and serves the
/// connection until it closes.
async fn dial_once(
    endpoint: ZakuraEndpoint,
    limits: ZakuraLocalLimits,
    addr: NodeAddr,
    ordinary_cap: usize,
) -> Result<(), ZakuraHandlerError> {
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        serve_reported_native_dial_connection(
            &endpoint,
            addr,
            &limits,
            None,
            NativeDialKind::Discovery { ordinary_cap },
        ),
    )
    .await
    .expect("the dial ends before the timeout")
}

/// Whether a dial ended as this node's own connection limit.
fn is_local_limit(result: &Result<(), ZakuraHandlerError>) -> bool {
    matches!(result, Err(ZakuraHandlerError::ResourceLimit(limit)) if *limit == ADMISSION_LIMIT)
}

/// The one ranked gateway: the node blocks name by `id`, known and dialable.
fn ranked(id: NodeId) -> [crate::zakura::RankedGateway; 1] {
    [crate::zakura::RankedGateway::resolved(
        id,
        1,
        crate::zakura::GatewayStanding::Dialable,
    )]
}

/// Blocks still name a gateway by the node ID it had before it added a key,
/// and every ordinary slot is in use. A dial to that ID claims the gateway's
/// held place, reaches the node under the ID it has now, and keeps the place
/// under that ID: the node registers, its slot counts as the preferred one,
/// and the report shows the gateway connected in its place.
///
/// The node then restarts twice. Under its new ID again, the dial is charged
/// to that ID first and finds no ordinary slot. Once the dialer may check the
/// earlier ID again, the dial claims the held place under the dialed ID and
/// the node keeps it. Back under its original ID, it takes the same place,
/// so a gateway that drops the added key is not locked out.
#[tokio::test]
async fn a_dial_to_a_moved_gateway_keeps_its_held_place() {
    let listener = ZakuraTestNode::builder(9310)
        .extended_credential()
        .spawn()
        .await
        .expect("listener spawns");
    let dialer = ZakuraTestNode::builder(9311)
        .spawn()
        .await
        .expect("dialer spawns");
    let earlier = listener.earlier_node_addr().await;
    let current = listener.node_addr().await.id;
    let endpoint = dialer.endpoint();
    let limits = dialer.limits().clone();
    let ordinary_cap = dialer.discovery().ordinary_connection_cap;
    let moved = dialer.supervisor().moved_node_ids();
    endpoint.handler.publish_gateway_ranking(ranked(earlier.id));
    let ordinary = fill_ordinary_slots(&endpoint, false);
    let held_place = |connected_as: NodeId| {
        let report = endpoint.connection_places();
        assert!(report.gateways[0].connected, "{report:?}");
        assert_eq!(
            report.gateways[0].place,
            crate::zakura::GatewayPlace::Preferred
        );
        assert_eq!(report.slots.preferred, 1, "the node fills the held place");
        assert_eq!(report.slots.ordinary, ordinary.len());
        assert!(report
            .peers
            .iter()
            .any(|peer| peer.node_id == *connected_as.as_bytes() && peer.preferred));
        assert_eq!(slots(&dialer, &connected_as), 1);
    };

    let serving = tokio::spawn(dial_once(
        endpoint.clone(),
        limits.clone(),
        earlier.clone(),
        ordinary_cap,
    ));
    registers(&dialer, &[current]).await;
    registers(&listener, &[dialer.node_addr().await.id]).await;
    assert_eq!(moved.borrow().current(earlier.id), current);
    assert_eq!(slots(&dialer, &earlier.id), 0);
    held_place(current);
    // The next ranking still names the earlier ID, and changes nothing.
    endpoint.handler.publish_gateway_ranking(ranked(earlier.id));
    held_place(current);

    // The place is taken, so another dial to the gateway is refused before
    // it sends a packet. Nothing listens here: a dial that started would end
    // as a failed handshake.
    let nowhere = NodeAddr::with_addrs(earlier.id, [SocketAddr::from(([127, 0, 0, 1], 9))]);
    let refused = dial_once(endpoint.clone(), limits.clone(), nowhere, ordinary_cap).await;
    assert!(is_local_limit(&refused), "{refused:?}");

    listener.shutdown().await;
    serving
        .await
        .expect("the dial task finishes")
        .expect("the connection closes");
    registers(&dialer, &[]).await;

    // The node comes back under its new ID. The dialer remembers that ID,
    // which finds no ordinary slot, so the dial claims under the dialed one
    // once that is due again.
    let returned = ZakuraTestNode::builder(9310)
        .extended_credential()
        .spawn()
        .await
        .expect("listener restarts with its added key");
    assert_eq!(moved.borrow().current(earlier.id), current);
    let wait_out_the_recheck = || async {
        tokio::time::pause();
        tokio::time::advance(DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF).await;
        tokio::time::resume();
    };
    wait_out_the_recheck().await;
    endpoint.handler.publish_gateway_ranking(ranked(earlier.id));
    let serving = tokio::spawn(dial_once(
        endpoint.clone(),
        limits.clone(),
        returned.earlier_node_addr().await,
        ordinary_cap,
    ));
    registers(&dialer, &[current]).await;
    held_place(current);
    returned.shutdown().await;
    serving
        .await
        .expect("the dial task finishes")
        .expect("the connection closes");
    registers(&dialer, &[]).await;

    // The node drops the added key and has its original ID again.
    let rolled_back = ZakuraTestNode::builder(9310)
        .spawn()
        .await
        .expect("listener restarts with its original keys");
    let restored = rolled_back.node_addr().await;
    assert_eq!(restored.id, earlier.id);
    wait_out_the_recheck().await;
    endpoint.handler.publish_gateway_ranking(ranked(earlier.id));
    let mut serving = tokio::spawn(dial_once(
        endpoint.clone(),
        limits.clone(),
        restored,
        ordinary_cap,
    ));
    let original_id = [earlier.id];
    tokio::select! {
        () = registers(&dialer, &original_id) => {}
        result = &mut serving => panic!("the rollback dial ended before registration: {result:?}"),
    }
    assert_eq!(moved.borrow().current(earlier.id), earlier.id);
    held_place(earlier.id);
    rolled_back.shutdown().await;
    serving
        .await
        .expect("the rollback dial task finishes")
        .expect("the restored connection closes");

    drop(ordinary);
    dialer.shutdown().await;
}

/// A gateway that added a key calls in. Blocks still name it by its earlier
/// ID, and the node it calls has every ordinary slot in use. The caller's
/// handshake proves the earlier ID, so it takes the gateway's held place
/// under the ID it has now. A node that proves no gateway's ID is refused.
#[tokio::test]
async fn a_moved_gateway_that_calls_in_takes_its_held_place() {
    let gateway = ZakuraTestNode::builder(9320)
        .extended_credential()
        .spawn()
        .await
        .expect("gateway spawns");
    let stranger = ZakuraTestNode::builder(9321)
        .extended_credential()
        .spawn()
        .await
        .expect("stranger spawns");
    let node = ZakuraTestNode::builder(9322)
        .spawn()
        .await
        .expect("node spawns");
    let earlier = gateway.earlier_node_addr().await.id;
    let current = gateway.node_addr().await.id;
    let endpoint = node.endpoint();
    endpoint.handler.publish_gateway_ranking(ranked(earlier));
    let mut held = fill_ordinary_slots(&endpoint, true);
    held.extend(fill_ordinary_slots(&endpoint, false));

    // The stranger proves an earlier ID too, its own, which no block names.
    let refused = stranger.connect_native(&node, Duration::from_secs(3)).await;
    assert!(refused.is_err(), "an ordinary caller finds no slot");
    assert!(node.supervisor().subscribe().borrow().is_empty());

    gateway
        .connect_native(&node, TEST_NET_TIMEOUT)
        .await
        .expect("the gateway takes its held place");
    registers(&node, &[current]).await;
    assert_eq!((slots(&node, &current), slots(&node, &earlier)), (1, 0));
    let report = endpoint.connection_places();
    assert!(report.gateways[0].connected, "{report:?}");
    assert_eq!(
        report.gateways[0].place,
        crate::zakura::GatewayPlace::Preferred
    );
    assert_eq!(report.slots.preferred, 1);
    assert_eq!(report.slots.ordinary, held.len());
    assert!(report
        .peers
        .iter()
        .any(|peer| peer.node_id == *current.as_bytes() && peer.inbound && peer.preferred));

    drop(held);
    stranger.shutdown().await;
    gateway.shutdown().await;
    node.shutdown().await;
}

/// A dial's slot is charged to the ID its node last answered with. Here
/// that ID is a preferred gateway's, so the dial claims a held place, and
/// then another node answers: one that proves the dialed ID but not the
/// gateway's. With the ordinary slots full the dial ends as a local limit
/// before the control handshake. Neither end registers a peer and no slot
/// stays charged, but the move is learned. The next dial is refused before
/// it reaches the network, until an ordinary slot is free.
#[tokio::test]
async fn a_dial_answered_by_another_node_than_its_gateway_waits_for_an_ordinary_slot() {
    let listener = ZakuraTestNode::builder(9312)
        .extended_credential()
        .spawn()
        .await
        .expect("listener spawns");
    let dialer = ZakuraTestNode::builder(9313)
        .spawn()
        .await
        .expect("dialer spawns");
    let earlier = listener.earlier_node_addr().await;
    let current = listener.node_addr().await.id;
    // The ID an earlier dial to this address was answered with: a gateway's.
    let gateway = crate::zakura::testkit::LocalEndpointFactory::secret_key(9314).public();
    let endpoint = dialer.endpoint();
    let limits = dialer.limits().clone();
    let ordinary_cap = dialer.discovery().ordinary_connection_cap;
    endpoint.supervisor.moved_tx.send_modify(|moved| {
        moved.learn(earlier.id, gateway);
    });
    endpoint.handler.publish_gateway_ranking(ranked(gateway));
    assert!(ordinary_cap < dialer.limits().max_connections);
    let mut ordinary = Vec::new();
    for index in 0..ordinary_cap {
        let mut id = [0xA5; 32];
        id[..size_of::<usize>()].copy_from_slice(&index.to_be_bytes());
        ordinary.push(
            endpoint
                .handler
                .claim_connection(NodeId::from_bytes(&id), false)
                .expect("an ordinary slot fits under the discovery cap"),
        );
    }

    let refused = dial_once(
        endpoint.clone(),
        limits.clone(),
        earlier.clone(),
        ordinary_cap,
    )
    .await;
    assert!(
        matches!(
            refused,
            Err(ZakuraHandlerError::ResourceLimit(AUTHENTICATED_SLOT_LIMIT))
        ),
        "{refused:?}"
    );
    let moved = dialer.supervisor().moved_node_ids();
    assert_eq!(moved.borrow().current(earlier.id), current);
    for id in [earlier.id, current, gateway] {
        assert_eq!(slots(&dialer, &id), 0);
    }
    assert!(dialer.supervisor().subscribe().borrow().is_empty());
    assert!(listener.supervisor().subscribe().borrow().is_empty());

    // Nothing listens here. The dial is refused before it sends a packet,
    // or it would end as a failed handshake.
    let nowhere = NodeAddr::with_addrs(earlier.id, [SocketAddr::from(([127, 0, 0, 1], 9))]);
    endpoint.handler.publish_gateway_ranking(ranked(gateway));
    let refused = dial_once(endpoint.clone(), limits.clone(), nowhere, ordinary_cap).await;
    assert!(is_local_limit(&refused), "{refused:?}");

    let bootstrap = ZakuraTestNode::builder(9315)
        .spawn()
        .await
        .expect("bootstrap peer spawns");
    let seed = bootstrap.node_addr().await;
    let [bootstrap_task]: [_; 1] = discovery::spawn_native_bootstrap_dialer(
        endpoint.clone(),
        dialer.discovery(),
        vec![format!("{}@{}", seed.id, seed.direct[0])],
        limits.clone(),
    )
    .try_into()
    .expect("one configured bootstrap peer starts one dial");
    registers(&dialer, &[seed.id]).await;
    assert_eq!(slots(&dialer, &seed.id), 1);
    bootstrap_task.abort();
    let _ = bootstrap_task.await;
    bootstrap.shutdown().await;
    registers(&dialer, &[]).await;

    ordinary.pop();
    endpoint.handler.publish_gateway_ranking(ranked(gateway));
    let serving = tokio::spawn(dial_once(
        endpoint.clone(),
        limits.clone(),
        earlier.clone(),
        ordinary_cap,
    ));
    registers(&dialer, &[current]).await;
    assert_eq!(slots(&dialer, &current), 1);
    let report = endpoint.connection_places();
    assert!(!report.gateways[0].connected, "{report:?}");
    assert_eq!(report.slots.preferred, 0, "the peer uses an ordinary slot");

    listener.shutdown().await;
    serving
        .await
        .expect("the dial task finishes")
        .expect("the connection closes");
    drop(ordinary);
    dialer.shutdown().await;
}

/// A native registration tells the legacy stack's registry which earlier IDs
/// the connection proved, in both directions, so a pinned
/// `network.unconditional_peers` entry that names a peer by the ID it had
/// before it added a key still finds the peer. An ID the peer doesn't prove
/// finds nothing, and the proof ends with the connection.
#[tokio::test]
async fn a_registration_carries_the_earlier_ids_it_proved_to_the_peer_registry() {
    use crate::peer_registry::PeerRegistry;

    for moved_peer_dials in [true, false] {
        let registry = PeerRegistry::default();
        let moved = ZakuraTestNode::builder(9330)
            .extended_credential()
            .spawn()
            .await
            .expect("the moved peer spawns");
        let pinning = ZakuraTestNode::builder(9331)
            .peer_registry(registry.clone())
            .spawn()
            .await
            .expect("the pinning node spawns");
        let earlier = ZakuraPeerId::from_node_id(moved.earlier_node_addr().await.id);
        let current = moved.node_addr().await.id;
        // The ID of the peer's Ed25519 key alone, which its handshake
        // doesn't prove: it signs with its ML-DSA-65 key as well.
        let unproven = ZakuraPeerId::from_node_id(
            zakura_quic::NodeKeyList::single(
                crate::zakura::testkit::LocalEndpointFactory::secret_key(9330).public_key(),
            )
            .node_id(),
        );
        assert_eq!(registry.native_connection_proving(&earlier), None);

        if moved_peer_dials {
            moved.connect_native(&pinning, TEST_NET_TIMEOUT).await
        } else {
            pinning.connect_native(&moved, TEST_NET_TIMEOUT).await
        }
        .expect("the nodes connect");
        registers(&pinning, &[current]).await;

        let proof = registry
            .native_connection_proving(&earlier)
            .expect("the connection proves the earlier ID");
        assert_eq!(proof.moved_to, Some(ZakuraPeerId::from_node_id(current)));
        assert_eq!(proof.remote_ip, Some(IpAddr::from([127, 0, 0, 1])));
        let exact = registry
            .native_connection_proving(&ZakuraPeerId::from_node_id(current))
            .expect("the connection proves the ID it registered under");
        assert_eq!(exact.moved_to, None);
        assert_eq!(registry.native_connection_proving(&unproven), None);

        moved.shutdown().await;
        registers(&pinning, &[]).await;
        assert_eq!(registry.native_connection_proving(&earlier), None);
        pinning.shutdown().await;
    }
}
