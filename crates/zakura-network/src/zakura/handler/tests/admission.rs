//! Production native admission under incomplete handshakes.

use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::{net::UdpSocket, task::JoinHandle};

const WAIT: Duration = Duration::from_secs(10);

// All test traffic stays on loopback. Forward the client's datagrams, but discard
// every server reply so a real QUIC Initial cannot finish its handshake.
async fn start_stalled_initial(
    server: &Endpoint,
    seed: u64,
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
        loop {
            let (len, source) = proxy.recv_from(&mut packet).await.unwrap();
            if source != target {
                proxy.send_to(&packet[..len], target).await.unwrap();
            } else {
                seen.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    let dial_task = tokio::spawn(async move {
        let _ = dial_client.connect(address, P2P_V2_ALPN).await;
    });
    Ok((client, proxy_task, dial_task, replies))
}

async fn node() -> Result<(tempfile::TempDir, ZakuraEndpoint, ZakuraLocalLimits), BoxError> {
    let identity = tempfile::tempdir()?;
    let mut config = Config::for_test(P2pStack::Dual);
    config.identity_dir = identity.path().to_owned();
    config.zakura.bootstrap_peers.clear();
    config.zakura.max_connections = 4;
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
        attempts.push(start_stalled_initial(node.router.endpoint(), seed).await?);
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
