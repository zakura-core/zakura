//! Production native admission under incomplete handshakes.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::{net::UdpSocket, task::JoinHandle};

const WAIT: Duration = Duration::from_secs(10);

// All test traffic stays on loopback. Forward the client's datagrams, but discard
// every server reply so a real QUIC Initial cannot finish its handshake.
async fn start_stalled_initial(
    server: &Endpoint,
    seed: u64,
    forward_retry: bool,
) -> Result<(Endpoint, JoinHandle<()>, JoinHandle<()>, Arc<AtomicUsize>), BoxError> {
    let target = *server
        .addr()
        .ip_addrs()
        .find(|addr| addr.is_ipv4())
        .unwrap();
    let proxy = UdpSocket::bind("127.0.0.1:0").await?;
    let address = iroh::EndpointAddr::new(server.id()).with_ip_addr(proxy.local_addr()?);
    let client = LocalEndpointFactory::default().endpoint(seed).await?;
    let dial_client = client.clone();
    let replies = Arc::new(AtomicUsize::new(0));
    let seen = replies.clone();
    let proxy_task = tokio::spawn(async move {
        let mut packet = [0; 65536];
        let mut client_address = None;
        loop {
            let (len, source) = proxy.recv_from(&mut packet).await.unwrap();
            if source != target {
                client_address = Some(source);
                proxy.send_to(&packet[..len], target).await.unwrap();
            } else {
                seen.fetch_add(1, Ordering::SeqCst);
                // QUIC v1 Retry has long-header packet type 0b11.
                if forward_retry && packet[0] & 0xf0 == 0xf0 {
                    proxy
                        .send_to(&packet[..len], client_address.unwrap())
                        .await
                        .unwrap();
                }
            }
        }
    });
    let dial_task = tokio::spawn(async move {
        let _ = dial_client.connect(address, P2P_V2_ALPN).await;
    });
    Ok((client, proxy_task, dial_task, replies))
}

async fn node() -> Result<(tempfile::TempDir, ZakuraEndpoint, ZakuraLocalLimits), BoxError> {
    configured_node(4, 16, 32).await
}

async fn configured_node(
    total: usize,
    per_ip: usize,
    handshakes: usize,
) -> Result<(tempfile::TempDir, ZakuraEndpoint, ZakuraLocalLimits), BoxError> {
    configured_node_with_nat(total, per_ip, handshakes, false).await
}

async fn configured_node_with_nat(
    total: usize,
    per_ip: usize,
    handshakes: usize,
    nat: bool,
) -> Result<(tempfile::TempDir, ZakuraEndpoint, ZakuraLocalLimits), BoxError> {
    let identity = tempfile::tempdir()?;
    let mut config = Config::for_test(P2pStack::Dual);
    config.identity_dir = identity.path().to_owned();
    config.zakura.bootstrap_peers.clear();
    config.zakura.listen_addr = None;
    config.zakura.nat_traversal = nat;
    config.zakura.max_connections = total;
    config.zakura.max_connections_per_ip = per_ip;
    config.zakura.max_pending_handshakes = handshakes;
    let limits = ZakuraLocalLimits::from_config(&config);
    let node = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
        .await?
        .ok_or("native endpoint disabled")?;
    Ok((identity, node, limits))
}

async fn wait_permits(budget: &Semaphore, expected: usize) {
    timeout(WAIT, async {
        while budget.available_permits() != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("expected transport count before deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_initials_leave_capacity_for_native_inbound_and_outbound() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let (_identity, node, limits) = node().await?;
    let mut attempts = Vec::new();
    for seed in 982_000..982_008 {
        attempts.push(start_stalled_initial(node.router.endpoint(), seed, false).await?);
    }
    timeout(WAIT, async {
        while attempts
            .iter()
            .any(|(_, _, _, replies)| replies.load(Ordering::SeqCst) == 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(node.handler.transport_admission.available_permits(), 4);
    assert_eq!(
        node.handler.pending_handshakes.available_permits(),
        limits.max_pending_handshakes
    );
    let honest = ZakuraTestNode::builder(982_010).spawn().await?;
    honest
        .connect_native_to_addr(node.node_addr().await, WAIT)
        .await?;
    let remote = ZakuraTestNode::builder(982_011).spawn().await?;
    let dial = node.spawn_native_dial(remote.node_addr().await);
    let mut peers = node.supervisor().subscribe();
    timeout(WAIT, async {
        while peers.borrow().len() != 2 {
            peers.changed().await.unwrap();
        }
    })
    .await?;
    wait_permits(&node.handler.transport_admission, 2).await;
    dial.abort();
    for (client, proxy, dial, _) in attempts {
        proxy.abort();
        dial.abort();
        timeout(WAIT, client.close()).await?;
    }
    timeout(WAIT, honest.shutdown()).await?;
    timeout(WAIT, remote.shutdown()).await?;
    timeout(WAIT, node.shutdown()).await?;
    Ok(())
}

async fn native_client(
    seed: u64,
    ipv6: bool,
    limits: &ZakuraLocalLimits,
) -> Result<Endpoint, BoxError> {
    let address: SocketAddr = if ipv6 { "[::1]:0" } else { "127.0.0.1:0" }.parse()?;
    Ok(
        direct_endpoint_builder(LocalEndpointFactory::secret_key(seed))
            .bind_addr(address)?
            .transport_config(limits.transport_config())
            .bind()
            .await?,
    )
}

fn address_for(node: &ZakuraEndpoint, ipv6: bool) -> EndpointAddr {
    let address = *node
        .router
        .endpoint()
        .addr()
        .ip_addrs()
        .find(|a| a.is_ipv6() == ipv6)
        .expect("both loopback address families are required for admission qualification");
    EndpointAddr::new(node.router.endpoint().id()).with_ip_addr(address)
}

async fn complete_control(
    client: &Endpoint,
    connection: &Connection,
    node: &ZakuraEndpoint,
    limits: &ZakuraLocalLimits,
) -> Result<(), BoxError> {
    run_native_initiator_handshake(
        connection,
        limits,
        &node.handler.current_handshake_config(),
        &ZakuraPeerId::new(client.id().as_bytes().to_vec())?,
        &ZakuraTrace::noop(),
        &ZakuraConnTrace::placeholder(),
    )
    .await?;
    Ok(())
}

async fn wait_peers(node: &ZakuraEndpoint, count: usize) -> Result<(), BoxError> {
    let mut peers = node.supervisor().subscribe();
    timeout(WAIT, async {
        while peers.borrow().len() != count {
            peers.changed().await.unwrap();
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_inbound_share_preserves_native_outbound() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(4, 4, 32).await?;
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for (seed, ipv6) in [(983_000, false), (983_001, false), (983_002, true)] {
        let client = native_client(seed, ipv6, &limits).await?;
        let connection =
            timeout(WAIT, client.connect(address_for(&node, ipv6), P2P_V2_ALPN)).await??;
        complete_control(&client, &connection, &node, &limits).await?;
        clients.push(client);
        connections.push(connection);
    }
    wait_peers(&node, 3).await?;
    assert_eq!(node.handler.admission.available_permits(), 1);
    assert_eq!(node.handler.incoming_transport.snapshot(), (0, 2, 3, 0));
    let denied = native_client(983_003, true, &limits).await?;
    assert!(
        timeout(WAIT, denied.connect(address_for(&node, true), P2P_V2_ALPN))
            .await?
            .is_err()
    );
    let peer = ZakuraTestNode::builder(983_004).spawn().await?;
    let dial = node.spawn_native_dial(peer.node_addr().await);
    wait_peers(&node, 4).await?;
    assert_eq!(node.handler.transport_admission.available_permits(), 0);
    assert_eq!(node.handler.admission.available_permits(), 0);
    dial.abort();
    drop(connections);
    for client in clients {
        timeout(WAIT, client.close()).await?;
    }
    timeout(WAIT, denied.close()).await?;
    timeout(WAIT, peer.shutdown()).await?;
    timeout(WAIT, node.shutdown()).await?;
    assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
    assert_eq!(node.handler.transport_admission.available_permits(), 4);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_cap_precedes_control_handshake_and_preserves_other_source() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(8, 1, 32).await?;
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for seed in 983_010..983_012 {
        let client = native_client(seed, false, &limits).await?;
        connections
            .push(timeout(WAIT, client.connect(address_for(&node, false), P2P_V2_ALPN)).await??);
        clients.push(client);
    }
    wait_permits(&node.handler.transport_admission, 6).await;
    let denied = native_client(983_012, false, &limits).await?;
    assert!(
        timeout(WAIT, denied.connect(address_for(&node, false), P2P_V2_ALPN))
            .await?
            .is_err()
    );
    assert_eq!(node.handler.incoming_transport.snapshot(), (5, 1, 2, 0));
    let honest = native_client(983_013, true, &limits).await?;
    let connection = timeout(WAIT, honest.connect(address_for(&node, true), P2P_V2_ALPN)).await??;
    complete_control(&honest, &connection, &node, &limits).await?;
    wait_peers(&node, 1).await?;
    let peer = ZakuraTestNode::builder(983_014).spawn().await?;
    let dial = node.spawn_native_dial(peer.node_addr().await);
    wait_peers(&node, 2).await?;
    dial.abort();
    drop((connections, connection));
    for client in clients {
        timeout(WAIT, client.close()).await?;
    }
    timeout(WAIT, denied.close()).await?;
    timeout(WAIT, honest.close()).await?;
    timeout(WAIT, peer.shutdown()).await?;
    timeout(WAIT, node.shutdown()).await?;
    assert_eq!(node.handler.incoming_transport.snapshot(), (7, 0, 0, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_source_guards_survive_unread_stream() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(4, 1, 32).await?;
    let server = native_client(983_020, false, &limits).await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .incoming_filter(ZakuraProtocolHandler::incoming_transport_filter())
        .incoming_admission(node.handler.incoming_transport_admission())
        .accept(
            P2P_V2_ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let client = native_client(983_021, false, &limits).await?;
    let connection = timeout(WAIT, client.connect(router.endpoint().addr(), P2P_V2_ALPN)).await??;
    let remote = timeout(WAIT, connection_rx.recv())
        .await?
        .ok_or("missing connection")?;
    let (mut send, peer_recv) = timeout(WAIT, connection.open_bi()).await??;
    timeout(WAIT, send.write_all(b"unread retained data")).await??;
    send.finish()?;
    assert_eq!(timeout(WAIT, send.stopped()).await??, None);
    let (unused_send, unread) = timeout(WAIT, stream_rx.recv())
        .await?
        .ok_or("missing stream")?;
    remote.close(0u32.into(), b"retain stream");
    timeout(WAIT, remote.closed()).await?;
    drop((unused_send, remote));
    assert_eq!(node.handler.transport_admission.available_permits(), 3);
    assert_eq!(node.handler.incoming_transport.snapshot(), (2, 1, 1, 0));
    drop(unread);
    wait_permits(&node.handler.transport_admission, 4).await;
    assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
    drop((send, peer_recv, connection));
    timeout(WAIT, client.close()).await?;
    timeout(WAIT, router.shutdown()).await??;
    timeout(WAIT, node.shutdown()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validated_tls_stalls_are_source_bounded() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(8, 1, 32).await?;
    let mut attempts = Vec::new();
    for seed in 984_000..984_003 {
        attempts.push(start_stalled_initial(node.router.endpoint(), seed, true).await?);
    }
    wait_permits(&node.handler.transport_admission, 6).await;
    timeout(WAIT, async {
        while attempts
            .iter()
            .any(|(_, _, _, replies)| replies.load(Ordering::SeqCst) < 2)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(node.handler.incoming_transport.snapshot(), (5, 1, 2, 0));
    assert_eq!(node.handler.pending_handshakes.available_permits(), 32);
    let honest = native_client(984_004, true, &limits).await?;
    let connection = timeout(WAIT, honest.connect(address_for(&node, true), P2P_V2_ALPN)).await??;
    complete_control(&honest, &connection, &node, &limits).await?;
    wait_peers(&node, 1).await?;
    for (client, proxy, dial, _) in attempts {
        proxy.abort();
        dial.abort();
        timeout(WAIT, client.close()).await?;
    }
    drop(connection);
    timeout(WAIT, honest.close()).await?;
    timeout(WAIT, node.shutdown()).await?;
    assert_eq!(node.handler.incoming_transport.snapshot(), (7, 0, 0, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_reconnect_cleans_blackholed_incumbent() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(4, 1, 32).await?;
    let target = *address_for(&node, false).ip_addrs().next().unwrap();
    let proxy = UdpSocket::bind("127.0.0.1:0").await?;
    let address = EndpointAddr::new(node.router.endpoint().id()).with_ip_addr(proxy.local_addr()?);
    let blackhole = Arc::new(AtomicBool::new(false));
    let blocked = blackhole.clone();
    let proxy_task = tokio::spawn(async move {
        let mut packet = [0; 65536];
        let mut client_address = None;
        loop {
            let (len, source) = proxy.recv_from(&mut packet).await.unwrap();
            if blocked.load(Ordering::SeqCst) {
                continue;
            }
            let target = if source == target {
                client_address.unwrap()
            } else {
                client_address = Some(source);
                target
            };
            proxy.send_to(&packet[..len], target).await.unwrap();
        }
    });
    let incumbent = native_client(984_010, false, &limits).await?;
    let old = timeout(WAIT, incumbent.connect(address, P2P_V2_ALPN)).await??;
    complete_control(&incumbent, &old, &node, &limits).await?;
    wait_peers(&node, 1).await?;
    blackhole.store(true, Ordering::SeqCst);
    // Include unacknowledged keepalives and retransmission backoff on the old path.
    tokio::time::sleep(Duration::from_secs(31)).await;
    // A new endpoint uses the same identity while the old path cannot cooperate.
    let replacement = native_client(984_010, false, &limits).await?;
    let duplicate = timeout(
        WAIT,
        replacement.connect(address_for(&node, false), P2P_V2_ALPN),
    )
    .await??;
    // The duplicate close may arrive before the control acknowledgment.
    let _ = complete_control(&replacement, &duplicate, &node, &limits).await;
    let closed = timeout(WAIT, duplicate.closed()).await?;
    assert!(
        matches!(closed, iroh::endpoint::ConnectionError::ApplicationClosed(ref close) if close.reason.as_ref() == b"duplicate")
    );
    drop(duplicate);
    wait_peers(&node, 0).await?;
    // This deadline is far below the 150 second QUIC idle timeout.
    wait_permits(&node.handler.transport_admission, 4).await;
    assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
    let new = timeout(
        WAIT,
        replacement.connect(address_for(&node, false), P2P_V2_ALPN),
    )
    .await??;
    complete_control(&replacement, &new, &node, &limits).await?;
    wait_peers(&node, 1).await?;
    drop((new, old));
    proxy_task.abort();
    timeout(WAIT, replacement.close()).await?;
    timeout(WAIT, incumbent.close()).await?;
    timeout(WAIT, node.shutdown()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_control_stalls_preserve_outbound_and_release_both_permits() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = 8;
    limits.max_pending_handshakes = 4;
    // Keep the deliberately stalled handshakes alive across this test's bounded waits.
    limits.control_timeout = Duration::from_secs(60);
    let owner = ZakuraTestNode::builder(985_099)
        .limits(limits.clone())
        .max_connections_per_ip(4)
        .spawn()
        .await?;
    let node = owner.endpoint();
    let client = native_client(985_000, false, &limits).await?;
    let stalled = timeout(WAIT, client.connect(address_for(&node, false), P2P_V2_ALPN)).await??;
    // Independent identities make separate QUIC connections, not reused dials.
    let second = native_client(985_001, false, &limits).await?;
    let third = native_client(985_002, false, &limits).await?;
    let two = timeout(WAIT, second.connect(address_for(&node, false), P2P_V2_ALPN)).await??;
    let three = timeout(WAIT, third.connect(address_for(&node, false), P2P_V2_ALPN)).await??;
    wait_permits(&node.handler.incoming_handshakes, 0).await;
    assert_eq!(node.handler.pending_handshakes.available_permits(), 1);
    let denied = native_client(985_003, false, &limits).await?;
    let excess = timeout(WAIT, denied.connect(address_for(&node, false), P2P_V2_ALPN)).await??;
    timeout(WAIT, excess.closed()).await?;
    assert_eq!(node.handler.pending_handshakes.available_permits(), 1);
    let peer = ZakuraTestNode::builder(985_004).spawn().await?;
    let dial = node.spawn_native_dial(peer.node_addr().await);
    wait_peers(&node, 1).await?;
    assert_eq!(node.handler.pending_handshakes.available_permits(), 1);
    stalled.close(0u32.into(), b"retry after capacity returns");
    drop(stalled);
    wait_permits(&node.handler.incoming_handshakes, 1).await;
    drop(excess);
    let retry = timeout(WAIT, denied.connect(address_for(&node, false), P2P_V2_ALPN)).await??;
    complete_control(&denied, &retry, &node, &limits).await?;
    wait_peers(&node, 2).await?;
    dial.abort();
    drop((two, three, retry));
    for endpoint in [client, second, third, denied] {
        timeout(WAIT, endpoint.close()).await?;
    }
    timeout(WAIT, peer.shutdown()).await?;
    timeout(WAIT, owner.shutdown()).await?;
    assert_eq!(node.handler.pending_handshakes.available_permits(), 4);
    assert_eq!(node.handler.incoming_handshakes.available_permits(), 3);
    assert_eq!(node.handler.incoming_transport.snapshot(), (7, 0, 0, 0));
    Ok(())
}

#[derive(Debug)]
struct HoldControl(mpsc::Sender<Connection>);

impl ProtocolHandler for HoldControl {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.0
            .send(connection.clone())
            .await
            .map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_handshakes_borrow_the_entire_global_budget() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(8, 4, 2).await?;
    let mut routers = Vec::new();
    let mut dials = Vec::new();
    let mut connections = Vec::new();
    for seed in 985_010..985_012 {
        let server = native_client(seed, false, &limits).await?;
        let (tx, mut rx) = mpsc::channel(1);
        let router = Router::builder(server)
            .accept(P2P_V2_ALPN, HoldControl(tx))
            .spawn();
        dials.push(node.spawn_native_dial(router.endpoint().addr()));
        connections.push(
            timeout(WAIT, rx.recv())
                .await?
                .ok_or("missing outbound connection")?,
        );
        routers.push(router);
    }
    wait_permits(&node.handler.pending_handshakes, 0).await;
    assert_eq!(node.handler.incoming_handshakes.available_permits(), 1);
    let client = native_client(985_013, true, &limits).await?;
    let rejected = timeout(WAIT, client.connect(address_for(&node, true), P2P_V2_ALPN)).await??;
    timeout(WAIT, rejected.closed()).await?;
    // The failed global acquisition must return the inbound permit.
    assert_eq!(node.handler.incoming_handshakes.available_permits(), 1);
    for dial in dials {
        dial.abort();
        let _ = dial.await;
    }
    for connection in &connections {
        connection.close(0u32.into(), b"done");
    }
    drop((connections, rejected));
    wait_permits(&node.handler.pending_handshakes, 2).await;
    timeout(WAIT, client.close()).await?;
    for router in routers {
        timeout(WAIT, router.shutdown()).await??;
    }
    timeout(WAIT, node.shutdown()).await?;
    assert_eq!(node.handler.transport_admission.available_permits(), 8);
    assert_eq!(node.handler.incoming_transport.snapshot(), (7, 0, 0, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_dual_stack_nat_toggle_smoke() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    for nat in [false, true] {
        let (_identity, node, limits) = configured_node_with_nat(4, 1, 4, nat).await?;
        for ipv6 in [false, true] {
            let client = native_client(985_020, ipv6, &limits).await?;
            for _ in 0..25 {
                let connection =
                    timeout(WAIT, client.connect(address_for(&node, ipv6), P2P_V2_ALPN)).await??;
                complete_control(&client, &connection, &node, &limits).await?;
                wait_peers(&node, 1).await?;
                connection.close(0u32.into(), b"smoke complete");
                drop(connection);
                wait_peers(&node, 0).await?;
                wait_permits(&node.handler.transport_admission, 4).await;
                assert_eq!(node.handler.pending_handshakes.available_permits(), 4);
                assert_eq!(node.handler.incoming_handshakes.available_permits(), 3);
                assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
            }
            timeout(WAIT, client.close()).await?;
        }
        timeout(WAIT, node.shutdown()).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_dials_with_registered_inbound_share_full() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    // Six outbound slots, with the normal discovery headroom of four.
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = 48;
    let owner = ZakuraTestNode::builder(986_000)
        .limits(limits.clone())
        .max_connections_per_ip(48)
        .spawn()
        .await?;
    let node = owner.endpoint();
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for seed in 986_001..986_043 {
        let client = native_client(seed, false, &limits).await?;
        let connection =
            timeout(WAIT, client.connect(owner.node_addr().await, P2P_V2_ALPN)).await??;
        complete_control(&client, &connection, &node, &limits).await?;
        clients.push(client);
        connections.push(connection);
    }
    wait_peers(&node, 42).await?;
    assert_eq!(node.handler.incoming_transport.snapshot(), (0, 1, 42, 0));
    assert_eq!(node.handler.transport_admission.available_permits(), 6);
    let remote = ZakuraTestNode::builder(986_050).spawn().await?;
    owner.insert_static_discovery_candidate(&remote).await?;
    let mut registrations = node.supervisor.registration_tx.subscribe();
    let discovery = owner.spawn_discovery_dialer();
    let event = timeout(WAIT, registrations.recv()).await??;
    assert_eq!(
        event.peer_id.as_bytes(),
        remote.node_addr().await.id.as_bytes()
    );
    assert!(node.handler.transport_admission.available_permits() >= 4);
    discovery.abort();
    let _ = discovery.await;
    drop(connections);
    timeout(
        WAIT,
        futures::future::join_all(clients.iter().map(Endpoint::close)),
    )
    .await?;
    timeout(WAIT, remote.shutdown()).await?;
    timeout(WAIT, owner.shutdown()).await?;
    assert_eq!(node.handler.transport_admission.available_permits(), 48);
    assert_eq!(node.handler.incoming_transport.snapshot(), (42, 0, 0, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_tls_releases_capacity_at_idle_deadline_before_shutdown() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = 4;
    limits.quic_idle_timeout = Duration::from_secs(2);
    let owner = ZakuraTestNode::builder(987_000)
        .limits(limits)
        .spawn()
        .await?;
    let node = owner.endpoint();
    let (client, proxy, dial, _) =
        start_stalled_initial(node.router.endpoint(), 987_001, true).await?;
    wait_permits(&node.handler.transport_admission, 3).await;
    assert_eq!(node.handler.incoming_transport.snapshot(), (2, 1, 1, 0));
    proxy.abort();
    let _ = proxy.await;
    // No endpoint shutdown or cooperative peer close can release the server owner here.
    wait_permits(&node.handler.transport_admission, 4).await;
    assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
    assert_eq!(node.handler.pending_handshakes.available_permits(), 32);
    dial.abort();
    timeout(WAIT, client.close()).await?;
    timeout(WAIT, owner.shutdown()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_with_both_address_families_available() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (_identity, node, limits) = configured_node(4, 1, 4).await?;
    let client = bind_native_endpoint(
        direct_endpoint_builder(LocalEndpointFactory::secret_key(987_010)),
        None,
    )?
    .transport_config(limits.transport_config())
    .bind()
    .await?;
    let address = node.node_addr().await;
    assert!(address.ip_addrs().any(|a| a.is_ipv4()));
    assert!(address.ip_addrs().any(|a| a.is_ipv6()));
    for _ in 0..50 {
        let connection = timeout(WAIT, client.connect(address.clone(), P2P_V2_ALPN)).await??;
        complete_control(&client, &connection, &node, &limits).await?;
        wait_peers(&node, 1).await?;
        assert_eq!(node.handler.transport_admission.available_permits(), 3);
        assert_eq!(node.handler.incoming_transport.snapshot(), (2, 1, 1, 0));
        connection.close(0u32.into(), b"dual stack attempt complete");
        drop(connection);
        wait_peers(&node, 0).await?;
        wait_permits(&node.handler.transport_admission, 4).await;
        assert_eq!(node.handler.incoming_transport.snapshot(), (3, 0, 0, 0));
        assert_eq!(node.handler.pending_handshakes.available_permits(), 4);
        assert_eq!(node.handler.incoming_handshakes.available_permits(), 3);
    }
    timeout(WAIT, client.close()).await?;
    timeout(WAIT, node.shutdown()).await?;
    Ok(())
}
