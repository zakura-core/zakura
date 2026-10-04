//! Production admission under inbound control-handshake pressure.

use super::*;

const WAIT: Duration = Duration::from_secs(20);

async fn configured_node(
    total: usize,
    handshakes: usize,
) -> Result<(tempfile::TempDir, ZakuraEndpoint, ZakuraLocalLimits), BoxError> {
    let identity = tempfile::tempdir()?;
    let mut config = Config::for_test(P2pStack::Dual);
    config.identity_dir = identity.path().to_owned();
    config.zakura.bootstrap_peers.clear();
    config.zakura.listen_addr = None;
    config.zakura.max_connections = total;
    config.zakura.max_connections_per_ip = 16;
    config.zakura.max_pending_handshakes = handshakes;
    let limits = ZakuraLocalLimits::from_config(&config);
    let node = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
        .await?
        .ok_or("native endpoint disabled")?;
    Ok((identity, node, limits))
}

async fn wait_permits(budget: &Semaphore, expected: usize) {
    while budget.available_permits() != expected {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_peers(node: &ZakuraEndpoint, count: usize) {
    let mut peers = node.supervisor().subscribe();
    while peers.borrow().len() != count {
        peers.changed().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_control_stalls_leave_outbound_capacity() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    timeout(WAIT, async {
        let (_identity, node, limits) = configured_node(8, 4).await?;
        let mut clients = Vec::new();
        let mut stalled = Vec::new();
        for seed in 985_000..985_003 {
            let client = LocalEndpointFactory::with_limits(&limits)
                .endpoint(seed)
                .await?;
            stalled.push(client.connect(node.node_addr().await, P2P_V2_ALPN).await?);
            clients.push(client);
        }
        wait_permits(&node.handler.inbound_handshakes, 0).await;
        assert_eq!(node.handler.pending_handshakes.available_permits(), 1);
        let denied = LocalEndpointFactory::with_limits(&limits)
            .endpoint(985_003)
            .await?;
        assert!(denied
            .connect(node.node_addr().await, P2P_V2_ALPN)
            .await
            .is_err());
        let peer = ZakuraTestNode::builder(985_004).spawn().await?;
        let dial = node.spawn_native_dial(peer.node_addr().await);
        wait_peers(&node, 1).await;
        assert_eq!(node.handler.pending_handshakes.available_permits(), 1);
        for connection in &stalled {
            connection.close(0u32.into(), b"release control capacity");
        }
        drop(stalled);
        wait_permits(&node.handler.pending_handshakes, 4).await;
        assert_eq!(node.handler.inbound_handshakes.available_permits(), 3);
        dial.abort();
        let _ = dial.await;
        for client in clients {
            client.shutdown().await;
        }
        denied.shutdown().await;
        peer.shutdown().await;
        node.shutdown().await;
        Ok::<_, BoxError>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_inbound_share_leaves_outbound_capacity() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    timeout(WAIT, async {
        let (_identity, node, limits) = configured_node(4, 32).await?;
        let mut clients = Vec::new();
        let mut inbound = Vec::new();
        for seed in 985_100..985_103 {
            let client = LocalEndpointFactory::with_limits(&limits)
                .endpoint(seed)
                .await?;
            let connection = client.connect(node.node_addr().await, P2P_V2_ALPN).await?;
            run_native_initiator_handshake(
                &connection,
                &limits,
                &node.handler.current_handshake_config(),
                &ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?,
                &ZakuraTrace::noop(),
                &ZakuraConnTrace::without_peer(seed),
            )
            .await?;
            inbound.push(connection);
            clients.push(client);
        }
        wait_peers(&node, 3).await;
        let denied = LocalEndpointFactory::with_limits(&limits)
            .endpoint(985_103)
            .await?;
        assert!(denied
            .connect(node.node_addr().await, P2P_V2_ALPN)
            .await
            .is_err());
        let peer = ZakuraTestNode::builder(985_104).spawn().await?;
        let dial = node.spawn_native_dial(peer.node_addr().await);
        wait_peers(&node, 4).await;
        assert_eq!(node.handler.admission.available_permits(), 0);
        dial.abort();
        let _ = dial.await;
        drop(inbound);
        for client in clients {
            client.shutdown().await;
        }
        denied.shutdown().await;
        peer.shutdown().await;
        node.shutdown().await;
        Ok::<_, BoxError>(())
    })
    .await?
}
