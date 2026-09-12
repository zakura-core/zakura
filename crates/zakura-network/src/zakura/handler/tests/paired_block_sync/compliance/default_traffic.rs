//! Legal sequential/released-buffer traffic through default negotiated workers.

use super::*;
use crate::zakura::{BlockSyncMessage, SessionPolicy, StreamWritePolicy};

type ClientSession = (
    FramedRecv,
    FramedSend,
    FramedSend,
    FramedRecv,
    CancellationToken,
);

#[derive(Debug)]
struct Client {
    declarations: BlockSyncService,
    sessions: mpsc::Sender<ClientSession>,
}

async fn connect_raw(
    address: EndpointAddr,
    seed: u64,
) -> Result<(Endpoint, ConnectedPeer, ClientSession), BoxError> {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(seed)
        .await?;
    let (sessions, mut sessions_rx) = mpsc::channel(1);
    let client_service = Arc::new(Client {
        declarations: BlockSyncService::new(ZakuraBlockSyncConfig::default()),
        sessions,
    });
    let client_handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
        Arc::new(ServiceRegistry::new(vec![client_service]).unwrap()),
    )
    .with_endpoint(client.clone());
    let transport = connect_download_peer(&client, address, client_handler, limits).await?;
    let session = timeout(DEADLINE, sessions_rx.recv())
        .await?
        .ok_or("missing conformance session")?;
    session
        .1
        .send(BlockSyncMessage::Status(cache_status()).encode_frame()?)
        .await?;
    Ok((client, transport, session))
}

impl Service for Client {
    fn name(&self) -> &'static str {
        "getblocks-conformance-requester"
    }
    fn streams(&self) -> &[Stream] {
        self.declarations.streams()
    }
    fn stream_queue_depths(&self, stream: Stream) -> Option<(usize, usize)> {
        self.declarations.stream_queue_depths(stream)
    }
    fn message_payload_limits(&self, stream: Stream) -> &'static [(u16, usize)] {
        self.declarations.message_payload_limits(stream)
    }
    fn message_types(&self, stream: Stream) -> Option<&'static [u16]> {
        self.declarations.message_types(stream)
    }
    fn stream_write_policy(&self, stream: Stream) -> StreamWritePolicy {
        self.declarations.stream_write_policy(stream)
    }
    fn session_policy(&self) -> SessionPolicy {
        self.declarations.session_policy()
    }
    fn add_peer(&self, mut peer: Peer) {
        let cancel = peer.service_cancel_token();
        let (data_recv, data_send) = peer.take_stream(6).unwrap();
        let (request_recv, request_send) = peer.take_stream(7).unwrap();
        self.sessions
            .try_send((data_recv, data_send, request_send, request_recv, cancel))
            .unwrap();
    }
    fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
}

async fn next(data: &mut FramedRecv) -> Result<BlockSyncMessage, BoxError> {
    loop {
        let frame = timeout(Duration::from_secs(5), data.recv())
            .await?
            .ok_or("L04 connection closed during conformant traffic")?;
        let message = BlockSyncMessage::decode_frame(frame)?;
        if !matches!(message, BlockSyncMessage::Status(_)) {
            return Ok(message);
        }
    }
}

async fn exchange(
    data: &mut FramedRecv,
    start: u32,
    expected: Option<block::Hash>,
) -> Result<(), BoxError> {
    if let Some(hash) = expected {
        let BlockSyncMessage::Block(body) = next(data).await? else {
            panic!("L04 expected useful body");
        };
        assert_eq!(body.hash(), hash);
        assert_eq!(body.coinbase_height(), Some(block::Height(start)));
        assert_eq!(
            next(data).await?,
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(start),
                returned: 1
            }
        );
    } else {
        assert_eq!(
            next(data).await?,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start),
                count: 1
            }
        );
    }
    Ok(())
}

async fn traffic(buffered: bool) -> Result<(), BoxError> {
    let template = Arc::new(Block::zcash_deserialize(
        &zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..],
    )?);
    let bodies = Arc::new(
        (1..=COUNT)
            .map(|height| {
                crate::zakura::handler::tests::serving_progress::block_at_height(
                    &template,
                    block::Height(height),
                )
            })
            .collect::<Vec<_>>(),
    );
    let server_node = Node::new(bodies.clone(), true);
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    assert_eq!(
        limits.message_rate_per_second,
        DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND
    );
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(89641)
        .await?;
    let (server_siblings, _unused) = paused::PausedService::new(0);
    let router = Router::builder(server.clone())
        .accept(
            ALPN,
            handler(&server_node, server_siblings, server, &limits),
        )
        .spawn();
    let (_client, transport, (mut data, _status, requests, _request_recv, cancel)) = connect_raw(
        LocalEndpointFactory::node_addr(router.endpoint()).await,
        89642,
    )
    .await?;
    let started = Instant::now();
    if buffered {
        let held = server_node.handle.hold_serving_capacity_for_test();
        // Distinct heights remain non-overlapping while earlier endings wait.
        // This burst fits the advertised maximum of concurrent commitments.
        let advertised = ZakuraBlockSyncConfig::default().max_inflight_requests;
        let count = COUNT.min(advertised);
        assert!(count > 1);
        for start in 1..=count {
            requests
                .send(
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(start),
                        count: 1,
                    }
                    .encode_frame()?,
                )
                .await?;
        }
        drop(held);
        for start in 1..=count {
            exchange(
                &mut data,
                start,
                Some(bodies[usize::try_from(start - 1)?].hash()),
            )
            .await?;
        }
    } else {
        for index in 0..zakura_test::resources::load_rounds() * 64 {
            let useful = index % 2 == 0;
            let start = if useful { 1 } else { COUNT + 1 };
            requests
                .send(
                    BlockSyncMessage::GetBlocks {
                        start_height: block::Height(start),
                        count: 1,
                    }
                    .encode_frame()?,
                )
                .await?;
            exchange(&mut data, start, useful.then(|| bodies[0].hash())).await?;
        }
    }
    assert!(!cancel.is_cancelled());
    assert!(transport.connection.close_reason().is_none());
    eprintln!(
        "L04 buffered={buffered}, rounds={}, default_rate={}, elapsed={:?}, transport={:?}",
        zakura_test::resources::load_rounds(),
        DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND,
        started.elapsed(),
        transport.connection.stats()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l04_continuous_legal_exchanges_complete_under_default_transport_settings(
) -> Result<(), BoxError> {
    traffic(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l04_disjoint_requests_buffered_during_local_pause_resume_without_false_faults(
) -> Result<(), BoxError> {
    traffic(true).await
}

#[derive(Debug)]
struct OneBlockedRange {
    body: Arc<Block>,
    started: Arc<tokio::sync::Notify>,
}

impl BlockRangeSource for OneBlockedRange {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let body = self.body.clone();
        let started = self.started.clone();
        Box::pin(async move {
            let (start, _, _, lease) = request.into_parts();
            assert!(lease.try_start());
            if start == block::Height(2) {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            assert_eq!(start, block::Height(1));
            let size = body.zcash_serialized_size();
            Ok(BlockRangeReadResult::new(vec![(start, body, size)], lease))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t04_another_peer_completes_while_one_peer_holds_a_shared_storage_slot(
) -> Result<(), BoxError> {
    let bodies = blocks();
    let started = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(OneBlockedRange {
        body: bodies[0].clone(),
        started: started.clone(),
    });
    let server_node = Node::with_range_source(bodies.clone(), true, None, Some(source), None);
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(89651)
        .await?;
    let (siblings, _unused) = paused::PausedService::new(0);
    let router = Router::builder(server.clone())
        .accept(ALPN, handler(&server_node, siblings, server, &limits))
        .spawn();
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let (_slow_endpoint, slow, slow_session) = connect_raw(address.clone(), 89652).await?;
    slow_session
        .2
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(2),
                count: 1,
            }
            .encode_frame()?,
        )
        .await?;
    timeout(DEADLINE, started.notified()).await?;
    let (_fast_endpoint, fast, mut fast_session) = connect_raw(address, 89653).await?;
    fast_session
        .2
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 1,
            }
            .encode_frame()?,
        )
        .await?;
    // Height 1 is independently useful. The blocked height 2 is not a missing
    // predecessor, so verification sequencing cannot explain lost progress.
    exchange(&mut fast_session.0, 1, Some(bodies[0].hash())).await?;
    assert!(!slow_session.4.is_cancelled());
    assert!(server_node.handle.active_serving_requests_for_test() >= 1);
    assert!(slow.connection.close_reason().is_none() && fast.connection.close_reason().is_none());
    Ok(())
}
