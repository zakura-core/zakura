//! A dial to a node's earlier ID reaches the node under the ID it has now
//! (zakura-quic TLS-7). The dialer registers it once, under that ID, and
//! closes a redundant dial if the peer is already registered at the local check.

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

/// Blocks still name a gateway by its earlier ID, so the dial to it claims a
/// held place. The node answers under the ID it has now, which no gateway
/// names, and ordinary discovery is at its soft cap. The dial ends as a local limit
/// before the control handshake: neither end registers a peer and no slot
/// stays charged, but the move is learned. The next dial is charged to the
/// ID the node has now, and is refused before it reaches the network.
#[tokio::test]
async fn moved_gateway_discovery_dials_leave_headroom_for_a_bootstrap_peer() {
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
    let ranking = || {
        [crate::zakura::RankedGateway::resolved(
            earlier.id,
            1,
            crate::zakura::GatewayStanding::Dialable,
        )]
    };
    endpoint.handler.publish_gateway_ranking(ranking());
    let ordinary_cap = dialer.discovery().ordinary_connection_cap;
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
    let limits = dialer.limits().clone();
    let dial = |addr: NodeAddr| {
        let (endpoint, limits) = (endpoint.clone(), limits.clone());
        async move {
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
    };
    let is_pre_dial_limit = |result: &Result<(), ZakuraHandlerError>| matches!(result, Err(ZakuraHandlerError::ResourceLimit(limit)) if *limit == ADMISSION_LIMIT);

    let refused = dial(earlier.clone()).await;
    assert!(
        matches!(
            refused,
            Err(ZakuraHandlerError::ResourceLimit(AUTHENTICATED_SLOT_LIMIT))
        ),
        "{refused:?}"
    );
    let moved = dialer.supervisor().moved_node_ids();
    assert_eq!(moved.borrow().current(earlier.id), current);
    assert_eq!(
        (slots(&dialer, &earlier.id), slots(&dialer, &current)),
        (0, 0)
    );
    assert!(dialer.supervisor().subscribe().borrow().is_empty());
    assert!(listener.supervisor().subscribe().borrow().is_empty());

    // Nothing listens here. The dial is refused before it sends a packet,
    // or it would end as a failed handshake.
    let nowhere = NodeAddr::with_addrs(earlier.id, [SocketAddr::from(([127, 0, 0, 1], 9))]);
    endpoint.handler.publish_gateway_ranking(ranking());
    let refused = dial(nowhere).await;
    assert!(is_pre_dial_limit(&refused), "{refused:?}");

    let bootstrap = ZakuraTestNode::builder(9312)
        .spawn()
        .await
        .expect("bootstrap peer spawns");
    let seed = bootstrap.node_addr().await;
    let [bootstrap_task]: [_; 1] = discovery::spawn_native_bootstrap_dialer(
        endpoint.clone(),
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
    endpoint.handler.publish_gateway_ranking(ranking());
    // One dial to this address. A maintained dial would keep retrying the old
    // listener and compete with our rollback dial after the listener stops.
    let serving = tokio::spawn(dial(earlier.clone()));
    registers(&dialer, &[current]).await;
    assert_eq!(slots(&dialer, &current), 1);

    let report = endpoint.connection_places();
    assert!(
        report.gateways[0].connected,
        "the earlier gateway ID reaches the registered peer"
    );
    assert_eq!(
        report.gateways[0].place,
        crate::zakura::GatewayPlace::Standby
    );
    assert_eq!(report.slots.preferred, 0, "the peer uses an ordinary slot");

    listener.shutdown().await;
    serving
        .await
        .expect("the first dial task finishes")
        .expect("the first connection closes");
    registers(&dialer, &[]).await;
    let rolled_back = ZakuraTestNode::builder(9310)
        .spawn()
        .await
        .expect("listener restarts with its original keys");
    let restored = rolled_back.node_addr().await;
    assert_eq!(restored.id, earlier.id);
    ordinary.push(
        endpoint
            .handler
            .claim_connection(NodeId::from_bytes(&[0xB6; 32]), false)
            .expect("the last ordinary place is free"),
    );
    endpoint.handler.publish_gateway_ranking(ranking());
    assert!(is_pre_dial_limit(&dial(restored.clone()).await));

    tokio::time::pause();
    tokio::time::advance(DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF).await;
    tokio::time::resume();
    endpoint.handler.publish_gateway_ranking(ranking());
    let mut restored_connection = tokio::spawn(dial(restored));
    let original_id = [earlier.id];
    tokio::select! {
        () = registers(&dialer, &original_id) => {}
        result = &mut restored_connection => panic!("the rollback dial ended before registration: {result:?}"),
    }
    assert_eq!(moved.borrow().current(earlier.id), earlier.id);
    rolled_back.shutdown().await;
    restored_connection
        .await
        .expect("the rollback dial task finishes")
        .expect("the restored connection closes");

    drop(ordinary);
    dialer.shutdown().await;
}
