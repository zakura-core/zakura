//! A bootstrap dial serves its connection under the token it was given.

use super::*;
use crate::zakura::testkit::{ZakuraTestNode, TEST_NET_TIMEOUT};

/// Cancelling a bootstrap dial's token closes its connection and ends the
/// dial, so the bootstrap dialer can release a seed it connected to without
/// touching any other connection.
#[tokio::test]
async fn a_bootstrap_dial_closes_when_its_token_is_cancelled() {
    let seed = ZakuraTestNode::builder(9420)
        .spawn()
        .await
        .expect("seed spawns");
    let dialer = ZakuraTestNode::builder(9421)
        .spawn()
        .await
        .expect("dialer spawns");
    let seed_addr = seed.node_addr().await;
    let seed_peer = ZakuraPeerId::from_node_id(seed_addr.id);
    let endpoint = dialer.endpoint();
    let connection_token = endpoint.connection_token();
    let mut registered = dialer.supervisor().subscribe();
    let dial = tokio::spawn({
        let limits = dialer.limits().clone();
        let connection_token = connection_token.clone();
        async move {
            serve_reported_native_dial_connection(
                &endpoint,
                seed_addr,
                &limits,
                None,
                NativeDialKind::Bootstrap { connection_token },
            )
            .await
        }
    });

    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        registered.wait_for(|peers| peers.contains(&seed_peer)),
    )
    .await
    .expect("the seed registers before the timeout")
    .expect("the supervisor outlives the test");
    connection_token.cancel();
    tokio::time::timeout(TEST_NET_TIMEOUT, dial)
        .await
        .expect("the dial ends once its token is cancelled")
        .expect("the dial task doesn't panic")
        .expect("a released dial ends cleanly");
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        registered.wait_for(|peers| !peers.contains(&seed_peer)),
    )
    .await
    .expect("the seed deregisters before the timeout")
    .expect("the supervisor outlives the test");

    dialer.shutdown().await;
    seed.shutdown().await;
}
