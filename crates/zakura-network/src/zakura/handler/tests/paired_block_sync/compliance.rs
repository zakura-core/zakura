//! Concurrent useful exchanges and independent service progress on real QUIC.

use super::*;
use crate::zakura::block_sync::BlockSyncStatus;

mod default_traffic;

pub(super) fn cache_status() -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(COUNT),
        tip_hash: block::Hash([0; 32]),
        max_blocks_per_response: 128,
        max_inflight_requests: 32,
        max_response_bytes: 32 * 1024 * 1024,
    }
}

fn handler(
    node: &Node,
    siblings: Arc<paused::PausedService>,
    endpoint: Endpoint,
    limits: &ZakuraLocalLimits,
) -> ZakuraProtocolHandler {
    ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
        Arc::new(ServiceRegistry::new(vec![node.service.clone(), siblings]).unwrap()),
    )
    .with_endpoint(endpoint)
}

async fn need(node: &Node, blocks: &[Arc<Block>]) -> Result<(), BoxError> {
    node._tip
        .send_replace((block::Height(COUNT), blocks.last().unwrap().hash()));
    node.handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|body| BlockSyncBlockMeta {
                    height: body.coinbase_height().unwrap(),
                    hash: body.hash(),
                    size: BlockSizeEstimate::Confirmed(
                        u32::try_from(body.zcash_serialized_size()).unwrap(),
                    ),
                })
                .collect(),
        ))
        .await?;
    Ok(())
}

async fn finished(node: &mut Node) -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        while *node.received.borrow_and_update() != COUNT {
            node.received.changed().await?;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t01_both_directions_complete_after_workers_and_output_are_pressured(
) -> Result<(), BoxError> {
    let bodies = blocks();
    let mut a = Node::with_download_mode(bodies.clone(), false, None, None, None, true);
    let mut b = Node::with_download_mode(bodies.clone(), false, None, None, None, true);
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let factory = || LocalEndpointFactory::with_transport_config(limits.transport_config());
    let a_endpoint = factory().endpoint(89611).await?;
    let b_endpoint = factory().endpoint(89612).await?;
    let (a_sibling, mut a_probes) = paused::PausedService::new(1);
    let (b_sibling, mut b_probes) = paused::PausedService::new(1);
    let router = Router::builder(b_endpoint.clone())
        .accept(ALPN, handler(&b, b_sibling, b_endpoint, &limits))
        .spawn();
    let transport = connect_download_peer(
        &a_endpoint,
        LocalEndpointFactory::node_addr(router.endpoint()).await,
        handler(&a, a_sibling, a_endpoint.clone(), &limits),
        limits,
    )
    .await?;
    await_until("both duplex peers admitted", DEADLINE, || {
        a.service.peer_count() == 1 && b.service.peer_count() == 1
    })
    .await?;
    let a_session = a.service.sessions_for_transport_test().pop().unwrap();
    let b_session = b.service.sessions_for_transport_test().pop().unwrap();
    a_session.1.send_status(cache_status()).await?;
    b_session.1.send_status(cache_status()).await?;
    await_until(
        "initial status writes drain before the controlled output pause",
        DEADLINE,
        || a_session.1.data_capacity_for_test() == 8 && b_session.1.data_capacity_for_test() == 8,
    )
    .await?;
    let a_workers = a.handle.hold_serving_capacity_for_test();
    let b_workers = b.handle.hold_serving_capacity_for_test();
    let a_output = a_session.1.hold_data_capacity_for_test();
    let b_output = b_session.1.hold_data_capacity_for_test();
    need(&a, &bodies).await?;
    need(&b, &bodies).await?;
    await_until(
        "both peers publish useful requests with all workers held",
        DEADLINE,
        || {
            a.handle.outstanding_requests_for_test() > 0
                && b.handle.outstanding_requests_for_test() > 0
        },
    )
    .await?;
    drop((a_workers, b_workers));
    await_until(
        "both serving jobs own output-blocked responses",
        DEADLINE,
        || {
            a.handle.active_serving_requests_for_test() > 0
                && b.handle.active_serving_requests_for_test() > 0
        },
    )
    .await?;
    assert_eq!(a_session.1.data_capacity_for_test(), 0);
    assert_eq!(b_session.1.data_capacity_for_test(), 0);
    assert_eq!(*a.received.borrow(), 0);
    assert_eq!(*b.received.borrow(), 0);
    let mut a_probe = paused::PausedSession::receive(&mut a_probes).await?;
    let mut b_probe = paused::PausedSession::receive(&mut b_probes).await?;
    a_probe.send_probe().await?;
    b_probe.send_probe().await?;
    tokio::try_join!(a_probe.receive_probe(), b_probe.receive_probe())?;
    let started = Instant::now();
    drop((a_output, b_output));
    tokio::try_join!(finished(&mut a), finished(&mut b))?;
    assert!(
        !a_session.1.cancel_token().is_cancelled() && !b_session.1.cancel_token().is_cancelled()
    );
    assert!(
        transport.connection.close_reason().is_none(),
        "T01 completion cannot rely on reconnecting"
    );
    eprintln!(
        "T01 useful={COUNT} each, elapsed={:?}, exchanges={:?}/{:?}, transport={:?}",
        started.elapsed(),
        a.handle.exchange_counts_for_test(),
        b.handle.exchange_counts_for_test(),
        transport.connection.stats()
    );
    for node in [&a, &b] {
        await_until(
            "T01 every published request consumes its exact ending",
            Duration::from_secs(3),
            || {
                let (requests, endings) = node.handle.exchange_counts_for_test();
                requests > 0 && requests == endings
            },
        )
        .await?;
    }
    Ok(())
}

async fn headroom(paused_count: u16) -> Result<(), BoxError> {
    let bodies = blocks();
    let mut a = Node::new(bodies.clone(), false);
    let b = Node::new(bodies.clone(), true);
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let factory = || LocalEndpointFactory::with_transport_config(limits.transport_config());
    let a_endpoint = factory().endpoint(89621).await?;
    let b_endpoint = factory().endpoint(89622).await?;
    let (a_siblings, mut a_streams) = paused::PausedService::new(paused_count + 1);
    let (b_siblings, mut b_streams) = paused::PausedService::new(paused_count + 1);
    let router = Router::builder(b_endpoint.clone())
        .accept(ALPN, handler(&b, b_siblings, b_endpoint, &limits))
        .spawn();
    let transport = connect_download_peer(
        &a_endpoint,
        LocalEndpointFactory::node_addr(router.endpoint()).await,
        handler(&a, a_siblings, a_endpoint.clone(), &limits),
        limits,
    )
    .await?;
    await_until("headroom sessions admitted", DEADLINE, || {
        a.service.peer_count() == 1 && b.service.peer_count() == 1
    })
    .await?;
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    let before = transport.connection.stats().udp_rx.bytes;
    for _ in 0..paused_count {
        let sender = paused::PausedSession::receive(&mut b_streams).await?;
        let receiver = paused::PausedSession::receive(&mut a_streams).await?;
        sender.fill_window().await?;
        senders.push(sender);
        receivers.push(receiver);
    }
    await_until("existing QUIC stream credit is occupied", DEADLINE, || {
        transport
            .connection
            .stats()
            .udp_rx
            .bytes
            .saturating_sub(before)
            >= u64::from(paused_count) * u64::from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW)
    })
    .await?;
    let mut receiver = paused::PausedSession::receive(&mut a_streams).await?;
    let sender = paused::PausedSession::receive(&mut b_streams).await?;
    sender.send_probe().await?;
    need(&a, &bodies).await?;
    // The independent service must make progress while the sibling consumers
    // remain stopped. A reset or resuming those consumers is not the witness.
    let progress = receiver.receive_probe().await;
    eprintln!("T02 paused={paused_count}, stream_window={DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW}, rx_delta={}, independent={progress:?}", transport.connection.stats().udp_rx.bytes.saturating_sub(before));
    let _resumed: Vec<_> = receivers
        .into_iter()
        .map(paused::PausedSession::resume)
        .collect();
    finished(&mut a).await?;
    assert!(transport.connection.close_reason().is_none());
    assert!(
        progress.is_ok(),
        "T02 required independent traffic lost connection headroom: {progress:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_one_paused_sibling_preserves_required_connection_progress() -> Result<(), BoxError> {
    headroom(1).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t02_two_paused_siblings_preserve_required_connection_progress() -> Result<(), BoxError> {
    headroom(2).await
}
