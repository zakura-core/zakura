//! Reconnects must reach duplicate handling even when their source IP is full.

use super::*;
use std::sync::atomic::AtomicBool;
use tokio::net::UdpSocket;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capped_source_reconnect_evicts_blackholed_incumbent() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    timeout(Duration::from_secs(40), async {
        let identity = tempfile::tempdir()?;
        let mut config = Config::for_test(P2pStack::Dual);
        config.identity_dir = identity.path().to_owned();
        config.zakura.bootstrap_peers.clear();
        config.zakura.listen_addr = None;
        config.zakura.max_connections = 8;
        config.zakura.max_connections_per_ip = 1;
        let limits = ZakuraLocalLimits::from_config(&config);
        let node = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
            .await?
            .ok_or("native endpoint disabled")?;
        let address = node.node_addr().await;
        let target = *address.direct.iter().find(|addr| addr.is_ipv4()).unwrap();
        let proxy = UdpSocket::bind("127.0.0.1:0").await?;
        let proxy_address = NodeAddr::with_addrs(address.id, [proxy.local_addr()?]);
        let blackhole = Arc::new(AtomicBool::new(false));
        let blocked = blackhole.clone();
        let proxy_task = tokio::spawn(async move {
            let mut packet = vec![0; 65_536];
            let mut client_address = None;
            loop {
                let (len, source) = proxy.recv_from(&mut packet).await.unwrap();
                if blocked.load(Ordering::SeqCst) {
                    continue;
                }
                let destination = if source == target {
                    client_address.unwrap()
                } else {
                    client_address = Some(source);
                    target
                };
                proxy.send_to(&packet[..len], destination).await.unwrap();
            }
        });
        let factory = LocalEndpointFactory::with_limits(&limits);
        let incumbent = factory.clone().endpoint(986_100).await?;
        let old = incumbent.connect(proxy_address, P2P_V2_ALPN).await?;
        complete_control(&incumbent, &old, &limits).await?;
        wait_for_peer_count(&node, 1).await;
        blackhole.store(true, Ordering::SeqCst);
        // Cross the production same-IP eviction gate, without waiting for QUIC idle cleanup.
        tokio::time::sleep(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
        assert_eq!(node.supervisor().registered_ids().await.len(), 1);

        let replacement = factory.endpoint(986_100).await?;
        let duplicate = replacement
            .connect(address.clone(), P2P_V2_ALPN)
            .await
            .expect("a capped source must get one attempt to authenticate a reconnect");
        // The duplicate close can arrive before the control acknowledgement.
        let _ = complete_control(&replacement, &duplicate, &limits).await;
        let closed = duplicate.closed().await;
        assert!(
            matches!(closed, zakura_quic::ConnectionError::ApplicationClosed(ref close)
                if close.reason.as_ref() == b"duplicate")
        );
        drop(duplicate);
        wait_for_peer_count(&node, 0).await;
        // The evicted incumbent and the closed duplicate still count against the IP.
        let recovered = connect_after_close_holds(&replacement, &address).await?;
        complete_control(&replacement, &recovered, &limits).await?;
        wait_for_peer_count(&node, 1).await;
        assert_eq!(
            node.supervisor().registered_ids().await,
            [ZakuraPeerId::new(
                replacement.local_id().as_bytes().to_vec()
            )?]
        );

        drop((recovered, old));
        proxy_task.abort();
        let _ = proxy_task.await;
        replacement.shutdown().await;
        incumbent.shutdown().await;
        node.shutdown().await;
        Ok::<_, BoxError>(())
    })
    .await?
}

async fn complete_control(
    client: &QuicEndpoint,
    connection: &Connection,
    limits: &ZakuraLocalLimits,
) -> Result<(), BoxError> {
    run_native_initiator_handshake_without_trace(
        connection,
        limits,
        &ZakuraHandshakeConfig::for_network(&Config::default().network),
        &ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?,
    )
    .await?;
    Ok(())
}

async fn wait_for_peer_count(node: &ZakuraEndpoint, count: usize) {
    let mut peers = node.supervisor().subscribe();
    while peers.borrow().len() != count {
        peers.changed().await.unwrap();
    }
}
