//! A dial to a node's earlier ID reaches the node under the ID it has now
//! (zakura-quic TLS-7). The dialer registers it once, under that ID, and
//! never lets such a dial replace a connection the node already has.

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

    let (admitted_addr, _admitted_addr) = oneshot::channel();
    let (registration, registered) = oneshot::channel();
    let endpoint = dialer.endpoint();
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        serve_reported_native_dial_connection(
            &endpoint,
            earlier.clone(),
            dialer.limits(),
            Some(DialReport {
                admitted_addr,
                registration,
            }),
            NativeDialKind::Discovery,
        ),
    )
    .await
    .expect("the redundant dial ends before the timeout")
    .expect("a redundant dial is not a failure");
    assert!(
        registered.await.is_err(),
        "the redundant dial registered nothing"
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
