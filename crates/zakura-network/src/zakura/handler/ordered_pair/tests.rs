use super::*;
use crate::zakura::testkit::LocalEndpointFactory;
use tokio_util::task::AbortOnDropHandle;

const DATA: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 2 * 1024 * 1024,
    capability: 1 << 16,
    mode: StreamMode::Ordered,
};
const REQUESTS: Stream = Stream { kind: 65, ..DATA };
const PAIR: OrderedStreamPair = OrderedStreamPair {
    data: DATA,
    requests: REQUESTS,
};
const ALPN: &[u8] = b"/zakura/test/ordered-pair/1";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct PairService {
    sessions: mpsc::Sender<Peer>,
}

impl Service for PairService {
    fn name(&self) -> &'static str {
        "test-pair"
    }
    fn streams(&self) -> &[Stream] {
        &[DATA, REQUESTS]
    }
    fn ordered_stream_pair(&self, stream: Stream) -> Option<OrderedStreamPair> {
        [DATA, REQUESTS].contains(&stream).then_some(PAIR)
    }
    fn ordered_stream_policy(&self, _: u16) -> OrderedStreamPolicy {
        OrderedStreamPolicy {
            opening: OrderedStreamOpening::EitherSide,
            reopen: true,
        }
    }
    fn add_peer(&self, peer: Peer) {
        let cancel = peer.service_cancel_token();
        if self.sessions.try_send(peer).is_err() {
            cancel.cancel();
        }
    }
    fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
}

struct Session {
    id: u64,
    conn_id: u64,
    data_recv: FramedRecv,
    data_send: FramedSend,
    request_recv: FramedRecv,
    request_send: FramedSend,
    cancel: CancellationToken,
    connection_cancel: CancellationToken,
}

impl Session {
    async fn receive(receiver: &mut mpsc::Receiver<Peer>) -> Result<Self, BoxError> {
        let mut peer = timeout(TEST_TIMEOUT, receiver.recv())
            .await?
            .ok_or("pair admission channel closed")?;
        let (data_id, data_version, data_recv, data_send) = peer
            .take_versioned_stream_with_session_id(DATA.kind)
            .expect("complete data role");
        let (request_id, request_version, request_recv, request_send) = peer
            .take_versioned_stream_with_session_id(REQUESTS.kind)
            .expect("complete request role");
        assert_eq!(data_id, request_id);
        assert_ne!(data_id, 0);
        assert_eq!(data_version, DATA.version);
        assert_eq!(request_version, REQUESTS.version);
        Ok(Self {
            id: data_id,
            conn_id: peer.conn_id,
            data_recv,
            data_send,
            request_recv,
            request_send,
            cancel: peer.service_cancel_token(),
            connection_cancel: peer.cancel_token(),
        })
    }
}

struct Fixture {
    router: Router,
    client: Endpoint,
    connection: Connection,
    serving: AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
    server_sessions: mpsc::Receiver<Peer>,
    client_sessions: mpsc::Receiver<Peer>,
}

impl Fixture {
    async fn start() -> Result<Self, BoxError> {
        let local = ZakuraLocalLimits::from_config(&Config::default());
        let server = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(93101)
            .await?;
        let client = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(93102)
            .await?;
        let (server_tx, server_sessions) = mpsc::channel(2);
        let (client_tx, client_sessions) = mpsc::channel(2);
        let handler = |sessions, endpoint: Endpoint| {
            ZakuraProtocolHandler::new_with_registry(
                ZakuraSupervisorHandle::new(16),
                Network::Mainnet,
                ZakuraHandshakeConfig::for_network(&Network::Mainnet),
                local.clone(),
                Arc::new(ServiceRegistry::new(vec![Arc::new(PairService { sessions })]).unwrap()),
            )
            .with_endpoint(endpoint)
        };
        let server_handler = handler(server_tx, server.clone());
        let client_handler = handler(client_tx, client.clone());
        let router = Router::builder(server).accept(ALPN, server_handler).spawn();
        let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
        let remote_id = address.node_id;
        let local_id = client.node_id();
        let connection = timeout(TEST_TIMEOUT, client.connect(address, ALPN)).await??;
        let local_peer = ZakuraPeerId::new(local_id.as_bytes().to_vec())?;
        let remote_peer = ZakuraPeerId::new(remote_id.as_bytes().to_vec())?;
        let conn = ZakuraConnTrace::without_peer(1);
        let negotiated = timeout(
            TEST_TIMEOUT,
            run_native_initiator_handshake(
                &connection,
                &local,
                &client_handler.current_handshake_config(),
                &local_peer,
                &ZakuraTrace::noop(),
                &conn,
            ),
        )
        .await??;
        let serving_connection = connection.clone();
        let serving = AbortOnDropHandle::new(tokio::spawn(async move {
            client_handler
                .register_and_serve(
                    serving_connection,
                    remote_peer,
                    None,
                    ConnectionServeContext {
                        limits: local.clamp(&negotiated.limits),
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
        Ok(Self {
            router,
            client,
            connection,
            serving,
            server_sessions,
            client_sessions,
        })
    }

    async fn sessions(&mut self) -> Result<(Session, Session), BoxError> {
        tokio::try_join!(
            Session::receive(&mut self.client_sessions),
            Session::receive(&mut self.server_sessions)
        )
    }

    async fn close(self) -> Result<(), BoxError> {
        self.connection.close(0u32.into(), b"test complete");
        timeout(TEST_TIMEOUT, self.serving).await???;
        timeout(TEST_TIMEOUT, self.client.close()).await?;
        timeout(TEST_TIMEOUT, self.router.shutdown()).await??;
        Ok(())
    }
}

fn frame(kind: u16, byte: u8, bytes: usize) -> Frame {
    Frame {
        message_type: kind,
        flags: 0,
        payload: vec![byte; bytes],
    }
}

async fn exchange(client: &mut Session, server: &mut Session) -> Result<(), BoxError> {
    let request = frame(1, 17, 9);
    timeout(TEST_TIMEOUT, client.request_send.send(request.clone())).await??;
    assert_eq!(
        timeout(TEST_TIMEOUT, server.request_recv.recv()).await?,
        Some(request)
    );
    let response = frame(2, 43, 1024 * 1024);
    timeout(TEST_TIMEOUT, server.data_send.send(response.clone())).await??;
    assert_eq!(
        timeout(TEST_TIMEOUT, client.data_recv.recv()).await?,
        Some(response)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negotiated_pair_reopens_as_one_session_on_the_same_connection() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut fixture = Fixture::start().await?;
    let (mut client, mut server) = fixture.sessions().await?;
    let conn_id = client.conn_id;
    for _ in 0..3 {
        exchange(&mut client, &mut server).await?;
        let old_id = client.id;
        client.cancel.cancel();
        timeout(TEST_TIMEOUT, server.cancel.cancelled()).await?;
        assert!(!client.connection_cancel.is_cancelled());
        assert!(!server.connection_cancel.is_cancelled());
        (client, server) = fixture.sessions().await?;
        assert_ne!(client.id, old_id);
        assert_eq!(client.conn_id, conn_id);
    }
    exchange(&mut client, &mut server).await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_backpressure_survives_write_timeout_and_pair_cancellation() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let mut fixture = Fixture::start().await?;
    let (mut client, server) = fixture.sessions().await?;
    let sender = client.request_send.clone();
    // Exceed both receive and send windows while the request consumer is paused.
    // This custom service uses larger frames to reach the same transport state
    // without encoding a million tiny GetBlocks requests.
    let mut writes = AbortOnDropHandle::new(tokio::spawn(async move {
        for _ in 0..80 {
            sender.send(frame(1, 17, 1024 * 1024)).await?;
        }
        Ok::<_, mpsc::error::SendError<Frame>>(())
    }));
    let started = Instant::now();
    while started.elapsed() < OUTBOUND_STREAM_WRITE_TIMEOUT + Duration::from_secs(1) {
        let response = frame(2, 43, 64);
        timeout(TEST_TIMEOUT, server.data_send.send(response.clone())).await??;
        assert_eq!(
            timeout(TEST_TIMEOUT, client.data_recv.recv()).await?,
            Some(response)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !writes.is_finished(),
        "request writes are still held by flow control"
    );
    assert!(
        !client.cancel.is_cancelled(),
        "useful responses keep the pair valid"
    );
    client.cancel.cancel();
    assert!(timeout(TEST_TIMEOUT, &mut writes).await??.is_err());
    timeout(TEST_TIMEOUT, server.cancel.cancelled()).await?;
    assert!(!client.connection_cancel.is_cancelled());
    let (mut replacement_client, mut replacement_server) = fixture.sessions().await?;
    exchange(&mut replacement_client, &mut replacement_server).await?;
    fixture.close().await
}

/// Capture only the connection so tests can send exact setup bytes.
#[derive(Debug)]
struct RawConnection(mpsc::Sender<Connection>);

impl ProtocolHandler for RawConnection {
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
async fn incomplete_pairs_expire_and_mismatched_roles_release_stream_permits(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(local.transport_config())
        .endpoint(93201)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(local.transport_config())
        .endpoint(93202)
        .await?;
    let (accepted, mut connections) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(ALPN, RawConnection(accepted))
        .spawn();
    let connection = timeout(
        TEST_TIMEOUT,
        client.connect(
            LocalEndpointFactory::node_addr(router.endpoint()).await,
            ALPN,
        ),
    )
    .await??;
    let remote = timeout(TEST_TIMEOUT, connections.recv())
        .await?
        .ok_or("missing connection")?;
    let permits = Arc::new(Semaphore::new(2));
    let (sessions, _sessions_rx) = mpsc::channel(1);
    let handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        local.clone(),
        Arc::new(ServiceRegistry::new(vec![Arc::new(PairService {
            sessions,
        })])?),
    );
    let mut limits = local.clamp(&local.initial_limits());
    limits.prelude_timeout = Duration::from_millis(100);
    let peer = ZakuraPeerId::new(client.node_id().as_bytes().to_vec())?;
    let (freshness, _freshness_rx) = watch::channel(Instant::now());
    let cancel = CancellationToken::new();
    let mut pending = PendingOrderedPairs::default();
    let mut workers = JoinSet::new();
    let mut buckets = MessageRateBuckets::new();
    let mut open_limiter = TokenBucket::new(100);
    let (exits, _exit_rx) = mpsc::unbounded_channel();
    let mut admission = StreamAdmission {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: &peer,
        stream_sem: &permits,
        open_limiter: &mut open_limiter,
        message_buckets: &mut buckets,
        workers: &mut workers,
        limits,
        accepted_capabilities: DATA.capability,
        connection_token: cancel.clone(),
        close_cause: CloseCause::new(),
        freshness_tx: freshness,
    };
    let mut offers = Vec::new();
    // The request role may arrive first. It must not reach the service alone.
    for (kind, id, should_expire) in [
        (REQUESTS.kind, 1u64, true),
        (DATA.kind, 2, false),
        (REQUESTS.kind, 3, false),
    ] {
        let (mut send, recv) = connection.open_bi().await?;
        let mut bytes = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: kind,
            stream_version: 1,
            request_id: None,
            max_frame_bytes: DATA.frame_cap,
        }
        .encode()?;
        bytes.extend_from_slice(&id.to_le_bytes());
        timeout(TEST_TIMEOUT, send.write_all(&bytes)).await??;
        offers.push((send, recv));
        let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
        assert!(handler
            .admit_bi_stream(send, recv, &mut admission, 2, exits.clone(), &mut pending)
            .await
            .is_none());
        if should_expire {
            assert_eq!(permits.available_permits(), 1);
            let deadline = pending.deadline().expect("incomplete pair owns a deadline");
            tokio::time::sleep_until(deadline).await;
            pending.expire(Instant::now());
            assert_eq!(permits.available_permits(), 2);
            assert!(
                !cancel.is_cancelled(),
                "incomplete setup is retired locally"
            );
        }
    }
    assert!(
        cancel.is_cancelled(),
        "different pair identifiers cannot be combined"
    );
    assert_eq!(permits.available_permits(), 2);
    assert!(pending.deadline().is_none());
    assert!(
        admission.workers.is_empty(),
        "incomplete and mismatched pairs never activate workers"
    );
    drop(offers);
    connection.close(0u32.into(), b"done");
    timeout(TEST_TIMEOUT, client.close()).await?;
    timeout(TEST_TIMEOUT, router.shutdown()).await??;
    Ok(())
}
