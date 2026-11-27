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

/// A seed that moved to a new address is still reached when its configured
/// address is stale: after the dial there fails, the bootstrap dialer dials
/// the address the seed's signed record advertises. Only that dialer dials seeds, so without it the
/// node could never reach the seed again.
#[tokio::test]
async fn a_seed_is_dialed_at_its_record_address_too() {
    use crate::zakura::{discovery, testkit::LocalEndpointFactory, ZakuraHandshakeConfig};
    use zakura_chain::parameters::Network;

    // Regtest books keep loopback addresses from records.
    let regtest = Network::new_regtest(Default::default());
    let seed = ZakuraTestNode::builder(9422)
        .handshake_network(regtest.clone())
        .spawn()
        .await
        .expect("seed spawns");
    let dialer = ZakuraTestNode::builder(9423)
        .handshake_network(regtest.clone())
        .spawn()
        .await
        .expect("dialer spawns");
    let seed_addr = seed.node_addr().await;
    let record = discovery::build_discovery_handle(
        LocalEndpointFactory::secret_key(9422),
        None,
        seed_addr.direct.clone(),
        discovery::default_advertised_services(),
        &ZakuraHandshakeConfig::for_network(&regtest),
        dialer.limits().max_connections,
        0,
        watch::channel(Vec::new()).1,
    )
    .expect("the seed's record config is valid")
    .current_self_record();

    // Nothing listens at the configured address.
    let configured = format!("{}@127.0.0.1:9", seed_addr.id);
    let book = dialer.discovery();
    discovery::insert_static_bootstrap_candidates(&book, std::slice::from_ref(&configured)).await;
    book.import_peer_records([(*record).clone()], None)
        .await
        .expect("the seed's own record imports");
    let tasks = discovery::spawn_native_bootstrap_dialer(
        dialer.endpoint(),
        book,
        vec![configured],
        dialer.limits().clone(),
    );

    let seed_peer = ZakuraPeerId::from_node_id(seed_addr.id);
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        dialer
            .supervisor()
            .subscribe()
            .wait_for(|peers| peers.contains(&seed_peer)),
    )
    .await
    .expect("the seed registers at its record address before the timeout")
    .expect("the supervisor outlives the test");

    for task in tasks {
        task.abort();
    }
    dialer.shutdown().await;
    seed.shutdown().await;
}
