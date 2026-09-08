//! Both endpoints use the production QUIC workers, admission, routines, and
//! reactors. Only storage is replaced with immediate, size-bounded responses.

use super::*;
use crate::zakura::block_sync::{
    spawn_block_sync_reactor, BlockSyncAction, BlockSyncEvent, BlockSyncFrontiers,
    BlockSyncMessage, BlockSyncStatus, ZakuraBlockSyncConfig,
};
use tokio_util::task::AbortOnDropHandle;
use zakura_chain::{serialization::ZcashSerialize, transparent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_serving_progress_with_small_queues() -> Result<(), BoxError> {
    check_bidirectional_serving_progress(128, 32).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_serving_progress_with_default_queues() -> Result<(), BoxError> {
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let queue_depth = per_stream_inbound_queue_depth(local.max_inbound_queue_depth, 2);
    assert_eq!(queue_depth, 2048);
    check_bidirectional_serving_progress(4096, queue_depth).await
}

async fn check_bidirectional_serving_progress(
    requests: u32,
    queue_depth: usize,
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const ALPN: &[u8] = b"/zakura/testkit/bidirectional-serving/0";
    // Thirty-two large responses exceed the default flow-control windows.
    // Reaching this target requires both readers to keep draining the stream.
    const REQUIRED_QUERIES: u32 = 32;
    let mut local = ZakuraLocalLimits::from_config(&Config::default());
    // Allow this test's entire burst through message-rate admission. The large
    // queue case otherwise tests the rate limiter's disconnect, not whether
    // bounded reads and response writes can make progress. Windows stay default.
    local.message_rate_per_second = local.message_rate_per_second.max(requests + 128);
    let endpoint = |seed| {
        crate::zakura::direct_endpoint_builder(LocalEndpointFactory::secret_key(seed))
            .bind_addr_v4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .bind_addr_v6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::LOCALHOST,
                0,
                0,
                0,
            ))
            .transport_config(local.transport_config())
            .bind()
    };
    let server = endpoint(54).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx: conn_tx,
                stream_tx,
            },
        )
        .spawn();
    let client = endpoint(55).await?;
    let address = router.endpoint().node_addr().initialized().await;
    client.add_node_addr(address.clone())?;
    let connection = timeout(Duration::from_secs(10), client.connect(address, ALPN)).await??;
    let (mut client_send, client_recv) = connection.open_bi().await?;
    let config = ZakuraBlockSyncConfig::default();
    assert!(requests <= config.advertised_max_inflight_requests());
    let status = BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(requests),
        ..config.initial_status()
    };
    let status_frame = BlockSyncMessage::Status(status)
        .encode_frame()?
        .encode(MAX_BS_FRAME_BYTES)?;
    client_send.write_all(&status_frame).await?;
    let (mut server_send, server_recv) = timeout(Duration::from_secs(5), stream_rx.recv())
        .await?
        .unwrap();
    server_send.write_all(&status_frame).await?;

    // Put the entire legal burst ahead of every response in both directions.
    // No response is available to help a reader before it crosses its full
    // application channel. These small request frames fit the QUIC windows.
    let mut burst = Vec::new();
    for height in 1..=requests {
        burst.extend(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(height),
                count: 1,
            }
            .encode_frame()?
            .encode(MAX_BS_FRAME_BYTES)?,
        );
    }
    timeout(Duration::from_secs(5), async {
        client_send.write_all(&burst).await?;
        server_send.write_all(&burst).await?;
        Ok::<_, BoxError>(())
    })
    .await??;

    // The fixtures have supported sizes and heights, but repeated transactions
    // are not consensus-valid. Both nodes already have these heights, so their
    // downloaders consume the bodies as stale. This test isolates serving and
    // transport progress; matched-body delivery has its own routine regression.
    let template = large_block_template();
    let block_bytes = template.zcash_serialized_size();
    assert!(
        block_bytes > 1_900_000 && block_bytes <= usize::try_from(block::MAX_BLOCK_BYTES).unwrap()
    );
    let mut tasks = Vec::new();
    let mut progress = Vec::new();
    let mut cancellations = Vec::new();
    let mut services = Vec::new();
    let mut tips = Vec::new();
    for (index, (send, recv)) in [(server_send, server_recv), (client_send, client_recv)]
        .into_iter()
        .enumerate()
    {
        let peer = test_peer(if index == 0 { 55 } else { 54 });
        let cancel = CancellationToken::new();
        let (tip_tx, tip_rx) = watch::channel((block::Height(requests), template.hash()));
        tips.push(tip_tx);
        let mut startup = BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: block::Height(requests),
                verified_block_tip: block::Height(requests),
                verified_block_hash: template.hash(),
            },
            (block::Height(requests), template.hash()),
            tip_rx,
            config.clone(),
        );
        startup.shutdown = cancel.clone();
        let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
        tasks.push(AbortOnDropHandle::new(reactor));
        let service = BlockSyncService::new_with_handle_for_test(config.clone(), handle.clone());
        let (inbound_tx, inbound_rx) = mpsc::channel(queue_depth);
        let (outbound_tx, outbound_rx) = worker_framed_channel(queue_depth);
        let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
        let context = StreamWorkerContext {
            conn: ZakuraConnTrace::without_peer(1),
            peer_id: peer.clone(),
            stream_id: 1,
            _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            limits: local.clamp(&local.initial_limits()),
            inbound_frame_cap: MAX_BS_FRAME_BYTES,
            message_payload_limits: service.message_payload_limits(service.streams()[0]),
            outbound_frame_cap: MAX_BS_FRAME_BYTES,
            message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(
                local.message_rate_per_second,
            ))),
            connection_token: cancel.clone(),
            stream_token: cancel.child_token(),
            close_cause: CloseCause::new(),
            freshness_tx,
        };
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: ZAKURA_STREAM_BLOCK_SYNC,
            stream_version: ZAKURA_BLOCK_SYNC_STREAM_VERSION,
            request_id: None,
            max_frame_bytes: MAX_BS_FRAME_BYTES,
        };
        tasks.push(AbortOnDropHandle::new(tokio::spawn(
            persistent_stream_worker(
                send,
                recv,
                prelude,
                context,
                inbound_tx,
                outbound_rx,
                queue_depth,
            ),
        )));
        service.add_peer(Peer::new_with_direction(
            peer,
            None,
            ZAKURA_CAP_BLOCK_SYNC,
            ServicePeerDirection::Outbound,
            HashMap::from([(
                ZAKURA_STREAM_BLOCK_SYNC,
                (FramedRecv::new(inbound_rx), outbound_tx),
            )]),
            cancel.clone(),
        ));
        let (progress_tx, progress_rx) = watch::channel(0u32);
        let template = template.clone();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            let mut queries = 0;
            while let Some(action) = actions.recv().await {
                match action {
                    BlockSyncAction::QueryBlocksByHeightRange {
                        lease,
                        request_id,
                        peer,
                        start,
                        count,
                        ..
                    } => {
                        assert!(lease.try_start());
                        assert_eq!(count, 1);
                        assert_eq!(
                            start,
                            block::Height(queries + 1),
                            "waiting requests retain arrival order"
                        );
                        let block = block_at_height(&template, start);
                        let bytes = block.zcash_serialized_size();
                        handle
                            .send(BlockSyncEvent::BlockRangeResponseReady {
                                lease,
                                request_id,
                                peer,
                                start_height: start,
                                requested_count: count,
                                blocks: vec![(start, block, bytes)],
                            })
                            .await
                            .unwrap();
                        queries += 1;
                        progress_tx.send_replace(queries);
                        // Stop at the progress target. Otherwise the faster
                        // side can serialize thousands of extra blocks while
                        // this test waits for the other side to catch up.
                        if queries == REQUIRED_QUERIES {
                            future::pending::<()>().await;
                        }
                    }
                    BlockSyncAction::QueryNeededBlocks { .. } => {}
                    action => panic!("unexpected serving action: {action:?}"),
                }
            }
        })));
        cancellations.push(cancel);
        services.push(service);
        progress.push(progress_rx);
    }
    timeout(Duration::from_secs(20), async {
        for observed in &mut progress {
            while *observed.borrow_and_update() < REQUIRED_QUERIES {
                observed
                    .changed()
                    .await
                    .expect("storage driver stays alive");
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "both endpoints must progress: queries={:?}, cancelled={:?}, connection={:?}",
            progress
                .iter()
                .map(|count| *count.borrow())
                .collect::<Vec<_>>(),
            cancellations
                .iter()
                .map(CancellationToken::is_cancelled)
                .collect::<Vec<_>>(),
            connection.stats()
        )
    });
    for cancel in &cancellations {
        assert!(
            !cancel.is_cancelled(),
            "progress must not rely on disconnecting"
        );
        cancel.cancel();
    }
    drop(tasks);
    connection.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

fn large_block_template() -> Arc<Block> {
    let mut block =
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let transaction = block.transactions[0].clone();
    let tx_bytes = transaction.zcash_serialized_size();
    block.transactions = vec![transaction; 1_901_000 / tx_bytes];
    Arc::new(block)
}

fn block_at_height(template: &Arc<Block>, height: block::Height) -> Arc<Block> {
    let mut block = template.as_ref().clone();
    let mut coinbase = block.transactions[0].as_ref().clone();
    let inputs = match &mut coinbase {
        transaction::Transaction::V1 { inputs, .. }
        | transaction::Transaction::V2 { inputs, .. }
        | transaction::Transaction::V3 { inputs, .. }
        | transaction::Transaction::V4 { inputs, .. }
        | transaction::Transaction::V5 { inputs, .. }
        | transaction::Transaction::V6 { inputs, .. } => inputs,
    };
    let transparent::Input::Coinbase {
        height: coinbase_height,
        ..
    } = &mut inputs[0]
    else {
        panic!("fixture has a coinbase input");
    };
    *coinbase_height = height;
    block.transactions[0] = Arc::new(coinbase);
    Arc::new(block)
}
