//! Matched one-way downloads through negotiation, paired workers, both peer
//! routines, sequential serving, and the download sequencer. Storage and final
//! consensus verification are deterministic fixtures; transport is real QUIC.

#![allow(
    clippy::print_stderr,
    reason = "record measured completion times for the transport gate"
)]

use super::*;
use crate::zakura::block_sync::{
    spawn_block_sync_reactor, BlockApplyOutcome, BlockRangeRead, BlockRangeReadResult,
    BlockRangeSource, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent,
    BlockSyncFrontiers, BlockSyncHandle, ZakuraBlockSyncConfig,
};
use futures::future::BoxFuture;
use tokio_util::task::AbortOnDropHandle;
use zakura_chain::serialization::ZcashSerialize;

const ALPN: &[u8] = b"/zakura/test/paired-block-download/1";
const COUNT: u32 = 32;
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct MemorySource(Arc<Vec<Arc<Block>>>);

impl BlockRangeSource for MemorySource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let blocks = self.0.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let (start, count, cap, lease) = request.into_parts();
                assert!(lease.try_start());
                let mut bytes = 0usize;
                let mut result = Vec::new();
                for block in blocks
                    .iter()
                    .skip(usize::try_from(start.0.saturating_sub(1)).unwrap())
                    .take(usize::try_from(count).unwrap())
                {
                    let size = block.zcash_serialized_size();
                    if lease.is_cancelled() || bytes + size > usize::try_from(cap).unwrap() {
                        break;
                    }
                    bytes += size;
                    result.push((block.coinbase_height().unwrap(), block.clone(), size));
                }
                BlockRangeReadResult::new(result, lease)
            })
            .await
            .map_err(Into::into)
        })
    }
}

struct Node {
    handle: BlockSyncHandle,
    service: Arc<BlockSyncService>,
    cancel: CancellationToken,
    _tasks: Vec<AbortOnDropHandle<()>>,
    _tip: watch::Sender<(block::Height, block::Hash)>,
    received: watch::Receiver<u32>,
}

impl Node {
    fn new(blocks: Arc<Vec<Arc<Block>>>, serving: bool, paired: bool) -> Self {
        let genesis = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])
            .unwrap()
            .hash();
        let tip = blocks.last().unwrap();
        let verified = if serving {
            (block::Height(COUNT), tip.hash())
        } else {
            (block::Height(0), genesis)
        };
        let (tip_tx, tip_rx) = watch::channel(verified);
        let mut config = ZakuraBlockSyncConfig::default();
        config.peer_limits.inbound_queue_depth = 8;
        config.peer_limits.outbound_queue_depth = 8;
        let cancel = CancellationToken::new();
        let mut startup = BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: verified.0,
                verified_block_tip: verified.0,
                verified_block_hash: verified.1,
            },
            verified,
            tip_rx,
            config.clone(),
        );
        startup.shutdown = cancel.clone();
        let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
        let mut service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
        if paired {
            service = service.with_paired_source_for_test(Arc::new(MemorySource(blocks.clone())));
        }
        let (progress_tx, received) = watch::channel(0);
        let driver_handle = handle.clone();
        let driver = tokio::spawn(async move {
            let mut completed = 0;
            while let Some(action) = actions.recv().await {
                match action {
                    BlockSyncAction::SubmitBlock {
                        owner,
                        source,
                        token,
                        block,
                    } => {
                        assert!(!serving, "only the downloader has matched body work");
                        completed += 1;
                        let height = block.coinbase_height().unwrap();
                        assert_eq!(height.0, completed);
                        assert_eq!(
                            block.hash(),
                            blocks[usize::try_from(height.0 - 1).unwrap()].hash()
                        );
                        let hash = block.hash();
                        driver_handle
                            .send(BlockSyncEvent::BlockApplyFinished {
                                owner,
                                source,
                                token,
                                height,
                                hash,
                                outcome: BlockApplyOutcome::committed(
                                    zakura_header_chain::VerifiedBodyEvidence {
                                        hash,
                                        evidence: zakura_header_chain::EvidenceId::from_digest(
                                            [0xa5; 32],
                                        ),
                                    },
                                ),
                            })
                            .await
                            .unwrap();
                        progress_tx.send_replace(completed);
                    }
                    BlockSyncAction::QueryBlocksByHeightRange {
                        request_id,
                        peer,
                        start,
                        count,
                        lease,
                        ..
                    } => {
                        assert!(!paired, "paired serving bypasses reactor storage actions");
                        assert!(lease.try_start());
                        let returned = blocks
                            .iter()
                            .skip(usize::try_from(start.0.saturating_sub(1)).unwrap())
                            .take(usize::try_from(count).unwrap())
                            .map(|block| {
                                (
                                    block.coinbase_height().unwrap(),
                                    block.clone(),
                                    block.zcash_serialized_size(),
                                )
                            })
                            .collect();
                        driver_handle
                            .send(BlockSyncEvent::BlockRangeResponseReady {
                                lease,
                                request_id,
                                peer,
                                start_height: start,
                                requested_count: count,
                                blocks: returned,
                            })
                            .await
                            .unwrap();
                    }
                    BlockSyncAction::QueryNeededBlocks { .. } => {}
                    action => panic!("unexpected download action: {action:?}"),
                }
            }
        });
        Self {
            handle,
            service: Arc::new(service),
            cancel,
            _tasks: vec![
                AbortOnDropHandle::new(reactor),
                AbortOnDropHandle::new(driver),
            ],
            _tip: tip_tx,
            received,
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn blocks() -> Arc<Vec<Arc<Block>>> {
    let template = super::serving_progress::large_block_template();
    let mut previous = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])
        .unwrap()
        .hash();
    Arc::new(
        (1..=COUNT)
            .map(|height| {
                let mut block =
                    (*super::serving_progress::block_at_height(&template, block::Height(height)))
                        .clone();
                let mut header = *block.header;
                header.previous_block_hash = previous;
                header.merkle_root = block.transactions.iter().collect();
                block.header = Arc::new(header);
                previous = block.hash();
                Arc::new(block)
            })
            .collect(),
    )
}

async fn download(paired: bool, pressure: bool) -> Result<Duration, BoxError> {
    let blocks = blocks();
    let mut downloader = Node::new(blocks.clone(), false, paired);
    let server_node = Node::new(blocks.clone(), true, paired);
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    // Let the declared request burst test serving pressure, not rate rejection.
    limits.message_rate_per_second = 40_000;
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94101)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94102)
        .await?;
    let handler = |service: Arc<BlockSyncService>, endpoint| {
        ZakuraProtocolHandler::new_with_registry(
            ZakuraSupervisorHandle::new(16),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
            Arc::new(ServiceRegistry::new(vec![service]).unwrap()),
        )
        .with_endpoint(endpoint)
    };
    let server_handler = handler(server_node.service.clone(), server.clone());
    let client_handler = handler(downloader.service.clone(), client.clone());
    let router = Router::builder(server).accept(ALPN, server_handler).spawn();
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let remote_id = address.node_id;
    let local_id = client.node_id();
    let connection = timeout(DEADLINE, client.connect(address, ALPN)).await??;
    let local_peer = ZakuraPeerId::new(local_id.as_bytes().to_vec())?;
    let remote_peer = ZakuraPeerId::new(remote_id.as_bytes().to_vec())?;
    let conn = ZakuraConnTrace::without_peer(1);
    let negotiated = run_native_initiator_handshake(
        &connection,
        &limits,
        &client_handler.current_handshake_config(),
        &local_peer,
        &ZakuraTrace::noop(),
        &conn,
    )
    .await?;
    let serving_connection = connection.clone();
    let transport = AbortOnDropHandle::new(tokio::spawn(async move {
        client_handler
            .register_and_serve(
                serving_connection,
                remote_peer,
                None,
                ConnectionServeContext {
                    limits: limits.clamp(&negotiated.limits),
                    accepted_capabilities: negotiated.accepted_capabilities,
                    role: "initiator",
                    direction: ServicePeerDirection::Outbound,
                    transcript_hash: native_connection_transcript_hash(
                        ServicePeerDirection::Outbound,
                        &local_id,
                        &remote_id,
                    ),
                    i_open_collision_winner: i_open_collision_winner(&local_id, &remote_id),
                    conn,
                },
            )
            .await
    }));
    await_until("both block-sync sessions admitted", DEADLINE, || {
        downloader.service.peer_count() == 1 && server_node.service.peer_count() == 1
    })
    .await?;
    let client_session = downloader
        .service
        .sessions_for_transport_test()
        .pop()
        .unwrap();
    let server_session = server_node
        .service
        .sessions_for_transport_test()
        .pop()
        .unwrap();
    let capacity = pressure.then(|| downloader.handle.hold_serving_capacity_for_test());
    if pressure {
        // All requests precede the download. Their receiver waits for capacity;
        // only A's download below must complete.
        timeout(DEADLINE, async {
            for _ in 0..32_000 {
                server_session
                    .2
                    .send(
                        crate::zakura::block_sync::BlockSyncMessage::GetBlocks {
                            start_height: block::Height(1),
                            count: 1,
                        }
                        .encode_frame()?,
                    )
                    .await?;
            }
            Ok::<_, BoxError>(())
        })
        .await??;
    }
    let start = Instant::now();
    downloader
        ._tip
        .send_replace((block::Height(COUNT), blocks.last().unwrap().hash()));
    downloader
        .handle
        .send(BlockSyncEvent::NeededBlocks(
            blocks
                .iter()
                .map(|block| BlockSyncBlockMeta {
                    height: block.coinbase_height().unwrap(),
                    hash: block.hash(),
                    size: BlockSizeEstimate::Advertised(
                        u32::try_from(block.zcash_serialized_size()).unwrap(),
                    ),
                })
                .collect(),
        ))
        .await?;
    timeout(DEADLINE, async {
        while *downloader.received.borrow_and_update() != COUNT {
            downloader.received.changed().await?;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    await_until(
        "all matched requests consume their ending",
        DEADLINE,
        || downloader.handle.outstanding_requests_for_test() == 0,
    )
    .await?;
    let elapsed = start.elapsed();
    assert!(!client_session.1.cancel_token().is_cancelled());
    assert!(!server_session.1.cancel_token().is_cancelled());
    assert_eq!(
        downloader.service.sessions_for_transport_test()[0].0,
        client_session.0
    );
    assert_eq!(
        server_node.service.sessions_for_transport_test()[0].0,
        server_session.0
    );
    assert!(connection.close_reason().is_none());
    drop(capacity);
    downloader.cancel.cancel();
    server_node.cancel.cancel();
    connection.close(0u32.into(), b"done");
    drop(transport);
    client.close().await;
    router.shutdown().await?;
    Ok(elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_download_matches_every_block_and_ending() -> Result<(), BoxError> {
    eprintln!(
        "paired matched download: {:?}",
        download(true, false).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_download_completes_while_serving_capacity_is_full() -> Result<(), BoxError> {
    eprintln!(
        "paired matched download under 32000-request pressure: {:?}",
        download(true, true).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_download_baseline_matches_every_block_and_ending() -> Result<(), BoxError> {
    eprintln!(
        "legacy matched download: {:?}",
        download(false, false).await?
    );
    Ok(())
}
