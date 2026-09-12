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
const LOSS_DEADLINE: Duration = Duration::from_secs(240);

mod compliance;
mod gate;
mod link;
mod paused;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_policy_keeps_progress_with_all_sibling_streams_unread() -> Result<(), BoxError> {
    gate::transport_windows::check_native_policy_headroom().await
}

struct Workload {
    transport: Option<QuicTransportConfig>,
    peer_limit: Option<usize>,
    pressure: bool,
    impaired: bool,
    rounds: u32,
    paused_siblings: u16,
    resume_after: Option<Duration>,
    recover_on_fresh_peer: bool,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            transport: None,
            peer_limit: None,
            pressure: false,
            impaired: false,
            rounds: 1,
            paused_siblings: 0,
            resume_after: None,
            recover_on_fresh_peer: false,
        }
    }
}

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

struct ConnectedPeer {
    connection: Connection,
    _task: AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
}

impl Drop for ConnectedPeer {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"test finished");
    }
}

async fn connect_download_peer(
    client: &Endpoint,
    address: EndpointAddr,
    handler: ZakuraProtocolHandler,
    limits: ZakuraLocalLimits,
) -> Result<ConnectedPeer, BoxError> {
    let (connection, task) =
        connection::connect_and_serve(client, address, handler, limits, ALPN, DEADLINE).await?;
    Ok(ConnectedPeer {
        connection,
        _task: task,
    })
}

struct Node {
    handle: BlockSyncHandle,
    service: Arc<BlockSyncService>,
    cancel: CancellationToken,
    _tasks: Vec<AbortOnDropHandle<()>>,
    _tip: watch::Sender<(block::Height, block::Hash)>,
    received: watch::Receiver<u32>,
    _capture: Option<crate::zakura::testkit::TraceCapture>,
    _serving_status: Option<watch::Sender<crate::zakura::block_sync::BlockSyncStatus>>,
}

impl Node {
    fn new(blocks: Arc<Vec<Arc<Block>>>, serving: bool) -> Self {
        Self::with_peer_limit(blocks, serving, None)
    }

    fn with_peer_limit(
        blocks: Arc<Vec<Arc<Block>>>,
        serving: bool,
        peer_limit: Option<usize>,
    ) -> Self {
        Self::with_range_source(blocks, serving, peer_limit, None, None)
    }

    fn with_range_source(
        blocks: Arc<Vec<Arc<Block>>>,
        serving: bool,
        peer_limit: Option<usize>,
        source: Option<Arc<dyn BlockRangeSource>>,
        cooldown: Option<Duration>,
    ) -> Self {
        Self::with_download_mode(blocks, serving, peer_limit, source, cooldown, false)
    }

    fn with_download_mode(
        blocks: Arc<Vec<Arc<Block>>>,
        serving: bool,
        peer_limit: Option<usize>,
        source: Option<Arc<dyn BlockRangeSource>>,
        cooldown: Option<Duration>,
        duplex: bool,
    ) -> Self {
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
        if let Some(cooldown) = cooldown {
            config.no_progress_peer_cooldown = cooldown;
        }
        config.peer_limits.inbound_queue_depth = 8;
        config.peer_limits.outbound_queue_depth = 8;
        if duplex {
            config.initial_inflight_requests = COUNT;
            config.initial_block_probe_requests = COUNT;
            config.bbr_min_cwnd = COUNT;
            config.bbr_min_cwnd_bytes = 128 * 1024 * 1024;
        }
        if let Some(limit) = peer_limit {
            config.peer_limits.max_inbound_peers = limit;
            config.peer_limits.max_outbound_peers = limit;
        }
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
        let capture = std::env::var_os("ZAKURA_DOWNLOAD_TRACE").map(|_| {
            let mut capture = crate::zakura::testkit::TraceCapture::for_test(if serving {
                "paired-gate-server"
            } else {
                "paired-gate-downloader"
            })
            .unwrap();
            startup.trace = ZakuraTrace::new(
                capture.tracer(),
                if serving { "server" } else { "downloader" },
            );
            eprintln!("download trace: {}", capture.path().display());
            capture
        });
        let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
        let handle = handle
            .with_range_source(source.unwrap_or_else(|| Arc::new(MemorySource(blocks.clone()))));
        let mut service = BlockSyncService::new_with_handle(config, handle.clone());
        // The transport fixture has an independently populated serving cache.
        // Both verification pipelines still begin at genesis and need bodies.
        let serving_status =
            duplex.then(|| service.with_serving_status_for_test(compliance::cache_status()));
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
                        assert_eq!(height.0, (completed - 1) % COUNT + 1);
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
            _capture: capture,
            _serving_status: serving_status,
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

async fn download(pressure: bool) -> Result<Duration, BoxError> {
    download_over_link(pressure, false).await
}

async fn download_over_link(pressure: bool, impaired: bool) -> Result<Duration, BoxError> {
    download_rounds(pressure, impaired, 1).await
}

async fn download_rounds(
    pressure: bool,
    impaired: bool,
    rounds: u32,
) -> Result<Duration, BoxError> {
    run_download(Workload {
        pressure,
        impaired,
        rounds,
        ..Workload::default()
    })
    .await
}

async fn start_request_pressure(
    send: FramedSend,
) -> Result<AbortOnDropHandle<Result<(), BoxError>>, BoxError> {
    const REQUESTS: usize = 32_000;
    let (progress, mut observed) = watch::channel(0);
    let writer = send.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        for queued in 1..=REQUESTS {
            writer
                .send(
                    crate::zakura::block_sync::BlockSyncMessage::GetBlocks {
                        start_height: block::Height(1),
                        count: 1,
                    }
                    .encode_frame()?,
                )
                .await?;
            progress.send_replace(queued);
        }
        Ok(())
    }));
    timeout(DEADLINE, async {
        loop {
            let queued = *observed.borrow_and_update();
            if queued == REQUESTS {
                break;
            }
            match timeout(Duration::from_millis(100), observed.changed()).await {
                Ok(result) => result?,
                Err(_) if queued > 0 && send.capacity() == 0 => break,
                Err(_) => continue,
            }
        }
        Ok::<_, BoxError>(())
    })
    .await
    .map_err(|_| std::io::Error::other("request pressure setup timed out"))??;
    // Keep offering the same bounded burst during the independent download.
    // A full transport window must not prevent the download from starting.
    eprintln!(
        "pressure burst: {} of {REQUESTS} requests queued",
        *observed.borrow()
    );
    Ok(task)
}

async fn run_download(workload: Workload) -> Result<Duration, BoxError> {
    let Workload {
        transport,
        peer_limit,
        pressure,
        impaired,
        rounds,
        paused_siblings,
        resume_after,
        recover_on_fresh_peer,
    } = workload;
    let completion_deadline = if impaired { LOSS_DEADLINE } else { DEADLINE };
    let blocks = blocks();
    let mut downloader = Node::with_peer_limit(blocks.clone(), false, peer_limit);
    let server_node = Node::with_peer_limit(blocks.clone(), true, peer_limit);
    let initial_client_slots = downloader.service.available_session_slots_for_test();
    let initial_server_slots = server_node.service.available_session_slots_for_test();
    let mut serving_service = server_node.service.clone();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    // Let the declared request burst test serving pressure, not rate rejection.
    if pressure {
        limits.message_rate_per_second = 40_000;
    }
    const SATURATED_STREAM_WINDOW: u32 = 16 * 1024 * 1024;
    let constrained_credit = resume_after.is_some() || recover_on_fresh_peer;
    assert!(!constrained_credit || transport.is_none());
    let transport_config = transport.unwrap_or_else(|| {
        if constrained_credit {
            // Recovery tests deliberately exhaust connection credit. The native
            // policy instead reserves enough credit for independent progress.
            limits
                .transport_config_builder()
                .stream_receive_window(SATURATED_STREAM_WINDOW.into())
                .receive_window((2 * SATURATED_STREAM_WINDOW).into())
                .build()
        } else {
            limits.transport_config()
        }
    });
    let server = LocalEndpointFactory::with_transport_config(transport_config.clone())
        .endpoint(94101)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(transport_config.clone())
        .endpoint(94102)
        .await?;
    let handler =
        |service: Arc<BlockSyncService>, sibling: Arc<paused::PausedService>, endpoint| {
            let mut services: Vec<Arc<dyn Service>> = vec![service];
            if paused_siblings > 0 {
                services.push(sibling);
            }
            ZakuraProtocolHandler::new_with_registry(
                ZakuraSupervisorHandle::new(16),
                Network::Mainnet,
                ZakuraHandshakeConfig::for_network(&Network::Mainnet),
                limits.clone(),
                Arc::new(ServiceRegistry::new(services).unwrap()),
            )
            .with_endpoint(endpoint)
        };
    let (server_sibling, mut server_siblings) = paused::PausedService::new(paused_siblings);
    let (client_sibling, mut client_siblings) = paused::PausedService::new(paused_siblings);
    let server_handler = handler(server_node.service.clone(), server_sibling, server.clone());
    let client_handler = handler(downloader.service.clone(), client_sibling, client.clone());
    let router = Router::builder(server).accept(ALPN, server_handler).spawn();
    let mut address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let link = if impaired {
        let server_address = *address
            .ip_addrs()
            .find(|address| address.is_ipv4())
            .unwrap();
        let link = link::ImpairedLink::new(server_address).await?;
        address = EndpointAddr::new(address.id).with_addrs([iroh::TransportAddr::Ip(link.address)]);
        Some(link)
    } else {
        None
    };
    let transport =
        connect_download_peer(&client, address, client_handler.clone(), limits.clone()).await?;
    let mut connection = transport.connection.clone();
    let mut recovery = None;
    await_until("both block-sync sessions admitted", DEADLINE, || {
        downloader.service.peer_count() == 1 && server_node.service.peer_count() == 1
    })
    .await?;
    let mut paused_receivers = Vec::new();
    let mut paused_senders = Vec::new();
    let sibling_window = if constrained_credit {
        SATURATED_STREAM_WINDOW
    } else {
        DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW
    };
    let before_siblings = connection.stats().udp_rx.bytes;
    for _ in 0..paused_siblings {
        let sender = paused::PausedSession::receive(&mut server_siblings).await?;
        let receiver = paused::PausedSession::receive(&mut client_siblings).await?;
        if constrained_credit {
            sender.fill_window_bytes(SATURATED_STREAM_WINDOW).await?;
        } else {
            sender.fill_window().await?;
        }
        paused_senders.push(sender);
        paused_receivers.push(receiver);
    }
    if paused_siblings > 0 {
        await_until(
            "sibling window traffic reaches the receiver",
            completion_deadline,
            || {
                connection
                    .stats()
                    .udp_rx
                    .bytes
                    .saturating_sub(before_siblings)
                    >= u64::from(paused_siblings) * u64::from(sibling_window)
            },
        )
        .await?;
    }
    let mut resume = None;
    let capacity = pressure.then(|| downloader.handle.hold_serving_capacity_for_test());
    let mut total = Duration::ZERO;
    for round in 0..rounds {
        let mut client_session = downloader
            .service
            .sessions_for_transport_test()
            .pop()
            .unwrap();
        let mut server_session = server_node
            .service
            .sessions_for_transport_test()
            .pop()
            .unwrap();
        let request_pressure = if pressure {
            Some(start_request_pressure(server_session.2.clone()).await?)
        } else {
            None
        };
        let mut start = Instant::now();
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
        if let Some(delay) = resume_after {
            assert_eq!(rounds, 1, "transient saturation resumes once");
            await_until("a request is outstanding before resuming", DEADLINE, || {
                downloader.handle.outstanding_requests_for_test() > 0
            })
            .await?;
            let receivers = std::mem::take(&mut paused_receivers);
            let progress = downloader.received.clone();
            resume = Some(AbortOnDropHandle::new(tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                assert_eq!(
                    *progress.borrow(),
                    0,
                    "saturation must prevent a complete body before resuming"
                );
                let _readers: Vec<_> = receivers
                    .into_iter()
                    .map(paused::PausedSession::resume)
                    .collect();
                std::future::pending::<()>().await;
            })));
        }
        if recover_on_fresh_peer {
            assert!(!impaired && !pressure && rounds == 1 && paused_siblings == 2);
            await_until(
                "the original request arms block-progress liveness",
                DEADLINE,
                || downloader.handle.outstanding_requests_for_test() > 0,
            )
            .await?;
            await_until(
                "an existing write or block-progress deadline retires the saturated pair",
                Duration::from_secs(42),
                || {
                    for sibling in paused_senders.iter().chain(&paused_receivers) {
                        sibling.assert_active();
                    }
                    client_session.1.cancel_token().is_cancelled()
                },
            )
            .await?;
            assert_eq!(*downloader.received.borrow(), 0);
            drop(client_session);
            drop(server_session);
            await_until(
                "retiring workers and serving frames release their slots",
                Duration::from_secs(10),
                || {
                    downloader.service.peer_count() == 0
                        && server_node.service.peer_count() == 0
                        && downloader.handle.outstanding_requests_for_test() == 0
                        && downloader.handle.active_serving_requests_for_test() == 0
                        && server_node.handle.active_serving_requests_for_test() == 0
                        && downloader.service.available_session_slots_for_test()
                            == initial_client_slots
                        && server_node.service.available_session_slots_for_test()
                            == initial_server_slots
                },
            )
            .await?;
            eprintln!(
                "saturated pair cleaned up after {:?}; connection {:?}",
                start.elapsed(),
                connection.close_reason()
            );

            let fresh_node = Node::new(blocks.clone(), true);
            let fresh_endpoint =
                LocalEndpointFactory::with_transport_config(transport_config.clone())
                    .endpoint(94103)
                    .await?;
            let (fresh_sibling, fresh_siblings) = paused::PausedService::new(paused_siblings);
            let fresh_handler = handler(
                fresh_node.service.clone(),
                fresh_sibling,
                fresh_endpoint.clone(),
            );
            let fresh_router = Router::builder(fresh_endpoint)
                .accept(ALPN, fresh_handler)
                .spawn();
            start = Instant::now();
            let fresh_transport = connect_download_peer(
                &client,
                LocalEndpointFactory::node_addr(fresh_router.endpoint()).await,
                client_handler.clone(),
                limits.clone(),
            )
            .await?;
            connection = fresh_transport.connection.clone();
            await_until(
                "a fresh peer admits the returned download work",
                DEADLINE,
                || downloader.service.peer_count() == 1 && fresh_node.service.peer_count() == 1,
            )
            .await?;
            client_session = downloader
                .service
                .sessions_for_transport_test()
                .pop()
                .unwrap();
            server_session = fresh_node
                .service
                .sessions_for_transport_test()
                .pop()
                .unwrap();
            serving_service = fresh_node.service.clone();
            recovery = Some((fresh_node, fresh_router, fresh_transport, fresh_siblings));
        }
        timeout(completion_deadline, async {
            while *downloader.received.borrow_and_update() != (round + 1) * COUNT {
                downloader.received.changed().await?;
                if impaired {
                    eprintln!(
                        "matched {}/{} blocks after {:?}",
                        *downloader.received.borrow(),
                        (round + 1) * COUNT,
                        start.elapsed()
                    );
                }
            }
            Ok::<_, BoxError>(())
        })
        .await
        .map_err(|_| {
            std::io::Error::other(format!(
                "download deadline: {}/{} blocks, {} outstanding; link {:?}; transport {:?}",
                *downloader.received.borrow(),
                COUNT,
                downloader.handle.outstanding_requests_for_test(),
                link.as_ref().map(link::ImpairedLink::counters),
                connection.stats(),
            ))
        })??;
        await_until(
            "all matched requests consume their ending",
            Duration::from_secs(3),
            || {
                let (published, ended) = downloader.handle.exchange_counts_for_test();
                published > 0 && ended == published
            },
        )
        .await?;
        let elapsed = start.elapsed();
        total += elapsed;
        if let Some(task) = request_pressure {
            task.abort();
            match task.await {
                Ok(result) => result?,
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(error.into()),
            }
        }
        if rounds > 1 {
            eprintln!("matched round {}/{}: {:?}", round + 1, rounds, elapsed);
        }
        assert!(!client_session.1.cancel_token().is_cancelled());
        assert!(!server_session.1.cancel_token().is_cancelled());
        assert_eq!(
            downloader.service.sessions_for_transport_test()[0].0,
            client_session.0
        );
        assert_eq!(
            serving_service.sessions_for_transport_test()[0].0,
            server_session.0
        );
        assert!(connection.close_reason().is_none());
        for sibling in paused_senders.iter().chain(&paused_receivers) {
            sibling.assert_active();
        }
        if let Some(link) = &link {
            let useful = blocks
                .iter()
                .map(|block| u64::try_from(block.zcash_serialized_size()).unwrap())
                .sum();
            link.verify_path(&connection, useful);
        }
        if round + 1 < rounds {
            let previous_client = client_session.0;
            let previous_server = server_session.0;
            client_session.1.cancel_token().cancel();
            drop(client_session);
            drop(server_session);
            await_until(
                "replacement pair admitted on the same connection",
                DEADLINE,
                || {
                    downloader
                        .service
                        .sessions_for_transport_test()
                        .first()
                        .is_some_and(|session| session.0 != previous_client)
                        && server_node
                            .service
                            .sessions_for_transport_test()
                            .first()
                            .is_some_and(|session| session.0 != previous_server)
                },
            )
            .await?;
            let genesis = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])
                .unwrap()
                .hash();
            downloader
                .handle
                .send(BlockSyncEvent::ChainTipReset(BlockSyncFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(0),
                    verified_block_hash: genesis,
                }))
                .await?;
        }
    }
    drop(capacity);
    drop(resume);
    drop(paused_receivers);
    drop(paused_senders);
    downloader.cancel.cancel();
    server_node.cancel.cancel();
    connection.close(0u32.into(), b"done");
    drop(transport);
    if let Some((fresh_node, fresh_router, fresh_transport, _siblings)) = recovery {
        fresh_node.cancel.cancel();
        drop(fresh_transport);
        fresh_router.shutdown().await?;
    }
    client.close().await;
    router.shutdown().await?;
    Ok(total)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incoming_pair_uses_the_last_reserved_session_slot() -> Result<(), BoxError> {
    run_download(Workload {
        peer_limit: Some(1),
        ..Workload::default()
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_download_matches_every_block_and_ending() -> Result<(), BoxError> {
    eprintln!("paired matched download: {:?}", download(false).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_download_completes_while_serving_capacity_is_full() -> Result<(), BoxError> {
    eprintln!(
        "paired matched download with an offered 32000-request burst: {:?}",
        download(true).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_download_completes_with_request_pressure_and_a_paused_service(
) -> Result<(), BoxError> {
    eprintln!(
        "paired matched download with pressure and paused service: {:?}",
        run_download(Workload {
            pressure: true,
            paused_siblings: 1,
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_full_connection_credit_resumes_the_original_matched_download(
) -> Result<(), BoxError> {
    eprintln!(
        "paired matched download after transient connection saturation: {:?}",
        run_download(Workload {
            paused_siblings: 2,
            resume_after: Some(Duration::from_secs(1)),
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn reset_during_a_frame_is_stream_local_but_truncated_fin_is_invalid() -> Result<(), BoxError>
{
    let server = LocalEndpointFactory::new().endpoint(94105).await?;
    let (connection_tx, _connections) = mpsc::channel(2);
    let (stream_tx, mut streams) = mpsc::channel(2);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let client = LocalEndpointFactory::new().endpoint(94106).await?;
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let connection = timeout(DEADLINE, client.connect(address, ALPN)).await??;
    for reset in [true, false] {
        let (mut send, _recv) = connection.open_bi().await?;
        send.write_all(&[42]).await?;
        let (_, mut recv) = timeout(DEADLINE, streams.recv()).await?.unwrap();
        let mut first = [0];
        timeout(DEADLINE, recv.read_exact(&mut first)).await??;
        assert_eq!(first, [42], "the peer consumed the beginning of this frame");
        if reset {
            send.reset(0u32.into())?;
        } else {
            send.finish()?;
        }
        let result = read_frame_payload(&mut recv, &mut [0; 16], DEADLINE).await;
        if reset {
            assert!(matches!(result, Err(ZakuraHandlerError::Closed)));
        } else {
            assert!(result.is_err() && !matches!(result, Err(ZakuraHandlerError::Closed)));
        }
        assert!(connection.close_reason().is_none());
    }
    connection.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn paired_roles_reject_wrong_messages_before_reading_payloads() -> Result<(), BoxError> {
    let service = Arc::new(BlockSyncService::new(ZakuraBlockSyncConfig::default()));
    let data = service.streams()[0];
    let requests = service.streams()[1];
    let registry = ServiceRegistry::new(vec![service.clone()])?;
    assert_eq!(registry.message_payload_limits(requests), &[(2, 9)]);
    assert!(registry
        .message_payload_limits(Stream {
            version: requests.version + 1,
            ..requests
        })
        .is_empty());
    let server = LocalEndpointFactory::new().endpoint(94103).await?;
    let (connection_tx, _connections) = mpsc::channel(4);
    let (stream_tx, mut streams) = mpsc::channel(4);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let client = LocalEndpointFactory::new().endpoint(94104).await?;
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    for (role, message) in [(requests, 1u16), (requests, 3), (data, 2), (data, 99)] {
        let connection = timeout(DEADLINE, client.connect(address.clone(), ALPN)).await??;
        let (mut send, _recv) = connection.open_bi().await?;
        let mut header = Vec::new();
        header.extend_from_slice(&message.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&100u32.to_le_bytes());
        send.write_all(&header).await?;
        let (_, mut recv) = timeout(DEADLINE, streams.recv()).await?.unwrap();
        assert!(matches!(timeout(Duration::from_secs(1), read_frame(
            &mut recv, role.frame_cap, service.message_payload_limits(role), service.message_types(role),
            service.allowed_frame_flags(role), Duration::from_secs(5), None,
        )).await?, Err(ZakuraHandlerError::InvalidMessageType(kind)) if kind == message));
        connection.close(0u32.into(), b"checked");
    }
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

#[derive(Debug)]
struct StalledSource(Arc<tokio::sync::Notify>);
impl BlockRangeSource for StalledSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let seen = self.0.clone();
        Box::pin(async move {
            let (_, _, _, lease) = request.into_parts();
            assert!(lease.try_start());
            seen.notify_one();
            std::future::pending::<()>().await;
            drop(lease);
            unreachable!()
        })
    }
}

#[tokio::test]
async fn unfinished_pair_retirement_closes_connection_without_readmission() -> Result<(), BoxError>
{
    for remote_reset in [false, true] {
        check_unfinished_pair_retirement(remote_reset).await?;
    }
    Ok(())
}

async fn check_unfinished_pair_retirement(remote_reset: bool) -> Result<(), BoxError> {
    let blocks = blocks();
    let downloader = Node::with_range_source(
        blocks.clone(),
        false,
        None,
        None,
        Some(Duration::from_secs(2)),
    );
    let seen = Arc::new(tokio::sync::Notify::new());
    let server_node = Node::with_range_source(
        blocks.clone(),
        true,
        None,
        Some(Arc::new(StalledSource(seen.clone()))),
        None,
    );
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94301)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94302)
        .await?;
    let remote_peer = ZakuraPeerId::new(server.id().as_bytes().to_vec())?;
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
    let mut transport = connect_download_peer(
        &client,
        LocalEndpointFactory::node_addr(router.endpoint()).await,
        client_handler,
        limits.clone(),
    )
    .await?;
    await_until("sessions admitted", DEADLINE, || {
        downloader.service.peer_count() == 1 && server_node.service.peer_count() == 1
    })
    .await?;
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
    timeout(DEADLINE, seen.notified())
        .await
        .expect("server sees initial request");
    assert!(downloader.handle.outstanding_requests_for_test() > 0);
    let download_session = downloader
        .service
        .sessions_for_transport_test()
        .pop()
        .unwrap()
        .1;
    if remote_reset {
        server_node
            .service
            .sessions_for_transport_test()
            .pop()
            .unwrap()
            .1
            .cancel_token()
            .cancel();
    } else {
        download_session.cancel_token().cancel();
    }
    // Keeping a QUIC clone in this fixture prevents automatic socket close, so
    // observe the production connection token and handler exit directly.
    timeout(DEADLINE, &mut transport._task)
        .await
        .expect("unfinished authorization requires connection closure")??;
    assert!(download_session.connection_is_closed_for_test());
    assert!(!downloader.service.is_peer_parked_for_test(&remote_peer));
    assert!(
        timeout(Duration::from_millis(100), seen.notified())
            .await
            .is_err(),
        "retirement must not readmit unanswered work on the old connection"
    );
    assert_eq!(downloader.service.peer_count(), 0);
    assert_eq!(*downloader.received.borrow(), 0);
    drop(transport);
    client.close().await;
    router.shutdown().await?;
    Ok(())
}
