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
const SIBLING: Stream = Stream {
    kind: 66,
    capability: 1 << 17,
    ..DATA
};
const PAIR: OrderedStreamPair = OrderedStreamPair {
    data: DATA,
    requests: REQUESTS,
};
const ALPN: &[u8] = b"/zakura/test/ordered-pair/1";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct PairService {
    opening: OrderedStreamOpening,
    fail_first_reservation: std::sync::atomic::AtomicBool,
    capacity: Option<Arc<Semaphore>>,
    sessions: mpsc::Sender<Peer>,
}

#[derive(Debug)]
struct SessionSlot {
    _permit: OwnedSemaphorePermit,
}

impl crate::zakura::OrderedSessionResources for SessionSlot {
    fn admitted(&self) {}
}

#[derive(Debug)]
struct SiblingService {
    sessions: mpsc::Sender<Peer>,
    retired: bool,
}

impl Service for SiblingService {
    fn name(&self) -> &'static str {
        "test-sibling"
    }
    fn streams(&self) -> &[Stream] {
        &[SIBLING]
    }
    fn wants_peer(&self, _: &ZakuraPeerId, _: u64, _: ServicePeerDirection) -> bool {
        !self.retired
    }
    fn add_peer(&self, peer: Peer) {
        let cancel = peer.service_cancel_token();
        if self.sessions.try_send(peer).is_err() {
            cancel.cancel();
        }
    }
    fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
}

impl Service for PairService {
    fn reserve_ordered_session(
        &self,
        _: ServicePeerDirection,
    ) -> Result<
        Option<Arc<dyn crate::zakura::OrderedSessionResources>>,
        crate::zakura::OrderedSessionFull,
    > {
        // Inject the outcome of another connection taking the final slot after
        // this connection's advisory OpenNow check. A retry can succeed.
        if self.fail_first_reservation.swap(false, Ordering::SeqCst) {
            Err(crate::zakura::OrderedSessionFull)
        } else if let Some(capacity) = &self.capacity {
            let permit = capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| crate::zakura::OrderedSessionFull)?;
            Ok(Some(Arc::new(SessionSlot { _permit: permit })))
        } else {
            Ok(None)
        }
    }
    fn name(&self) -> &'static str {
        "test-pair"
    }
    fn streams(&self) -> &[Stream] {
        &[DATA, REQUESTS]
    }
    fn ordered_stream_pair(&self, stream: Stream) -> Option<OrderedStreamPair> {
        [DATA, REQUESTS].contains(&stream).then_some(PAIR)
    }
    fn stream_queue_depths(&self, _: Stream) -> Option<(usize, usize)> {
        Some((1, 1))
    }
    fn ordered_stream_policy(&self, _: u16) -> OrderedStreamPolicy {
        OrderedStreamPolicy {
            opening: self.opening,
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
    server_siblings: mpsc::Receiver<Peer>,
    client_siblings: mpsc::Receiver<Peer>,
}

impl Fixture {
    async fn start() -> Result<Self, BoxError> {
        Self::start_with_reservation_race(false).await
    }

    async fn start_with_reservation_race(fail_first_reservation: bool) -> Result<Self, BoxError> {
        Self::start_with_config(fail_first_reservation, None, false).await
    }

    async fn start_with_config(
        fail_first_reservation: bool,
        max_open_streams: Option<u16>,
        retired_sibling: bool,
    ) -> Result<Self, BoxError> {
        let mut local = ZakuraLocalLimits::from_config(&Config::default());
        if let Some(max_open_streams) = max_open_streams {
            local.max_open_streams = max_open_streams;
        }
        let server = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(93101)
            .await?;
        let client = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(93102)
            .await?;
        let (server_tx, server_sessions) = mpsc::channel(2);
        let (client_tx, client_sessions) = mpsc::channel(2);
        let (server_sibling_tx, server_siblings) = mpsc::channel(1);
        let (client_sibling_tx, client_siblings) = mpsc::channel(1);
        let handler = |sessions, siblings, endpoint: Endpoint, fail_reservation| {
            ZakuraProtocolHandler::new_with_registry(
                ZakuraSupervisorHandle::new(16),
                Network::Mainnet,
                ZakuraHandshakeConfig::for_network(&Network::Mainnet),
                local.clone(),
                Arc::new(
                    ServiceRegistry::new(vec![
                        Arc::new(PairService {
                            capacity: None,
                            opening: OrderedStreamOpening::EitherSide,
                            sessions,
                            fail_first_reservation: std::sync::atomic::AtomicBool::new(
                                fail_reservation,
                            ),
                        }),
                        Arc::new(SiblingService {
                            sessions: siblings,
                            retired: retired_sibling,
                        }),
                    ])
                    .unwrap(),
                ),
            )
            .with_endpoint(endpoint)
        };
        let server_opens = i_open_collision_winner(&server.node_id(), &client.node_id());
        let server_handler = handler(
            server_tx,
            server_sibling_tx,
            server.clone(),
            fail_first_reservation && server_opens,
        );
        let client_handler = handler(
            client_tx,
            client_sibling_tx,
            client.clone(),
            fail_first_reservation && !server_opens,
        );
        let router = Router::builder(server).accept(ALPN, server_handler).spawn();
        let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
        let (connection, serving) = super::super::tests::connection::connect_and_serve(
            &client,
            address,
            client_handler,
            local,
            ALPN,
            TEST_TIMEOUT,
        )
        .await?;
        Ok(Self {
            router,
            client,
            connection,
            serving,
            server_sessions,
            client_sessions,
            server_siblings,
            client_siblings,
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
async fn paired_data_timeout_preserves_sibling_and_reopens_pair() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut fixture = Fixture::start().await?;
    let (client, server) = fixture.sessions().await?;
    let mut client_sibling = timeout(TEST_TIMEOUT, fixture.client_siblings.recv())
        .await
        .expect("the client admits the sibling service")
        .ok_or("missing client sibling")?;
    let mut server_sibling = timeout(TEST_TIMEOUT, fixture.server_siblings.recv())
        .await
        .expect("the server admits the sibling service")
        .ok_or("missing server sibling")?;
    let (mut sibling_recv, _send) = client_sibling.take_stream(SIBLING.kind).unwrap();
    let (_recv, sibling_send) = server_sibling.take_stream(SIBLING.kind).unwrap();

    // Keep the peer's data consumer paused past the actual production deadline.
    // More than both transport windows ensures a data write must wait.
    let started = Instant::now();
    let sender = client.data_send.clone();
    let writes = AbortOnDropHandle::new(tokio::spawn(async move {
        for _ in 0..80 {
            sender.send(frame(2, 43, 1024 * 1024)).await?;
        }
        Ok::<_, BoxError>(())
    }));
    timeout(PAIRED_DATA_WRITE_TIMEOUT + Duration::from_secs(10), async {
        loop {
            let ping = frame(1, 17, 64);
            sibling_send.send(ping.clone()).await?;
            assert_eq!(sibling_recv.recv().await, Some(ping));
            tokio::select! {
                () = client.cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
        Ok::<_, BoxError>(())
    })
    .await
    .expect("the paired data writer retires its session at the write deadline")?;
    assert!(started.elapsed() >= PAIRED_DATA_WRITE_TIMEOUT);
    timeout(TEST_TIMEOUT, server.cancel.cancelled()).await?;
    assert!(!client.connection_cancel.is_cancelled());
    assert!(!server.connection_cancel.is_cancelled());
    assert!(!client_sibling.service_cancel_token().is_cancelled());
    assert!(!server_sibling.service_cancel_token().is_cancelled());
    assert!(fixture.connection.close_reason().is_none());
    assert!(timeout(TEST_TIMEOUT, writes).await??.is_err());

    let (mut replacement_client, mut replacement_server) = fixture.sessions().await?;
    assert_eq!(replacement_client.conn_id, client.conn_id);
    assert_ne!(replacement_client.id, client.id);
    exchange(&mut replacement_client, &mut replacement_server).await?;
    let ping = frame(1, 19, 64);
    timeout(TEST_TIMEOUT, sibling_send.send(ping.clone())).await??;
    assert_eq!(
        timeout(TEST_TIMEOUT, sibling_recv.recv()).await?,
        Some(ping)
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_sibling_does_not_prevent_full_capacity_pair() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    // The pair needs both slots; the third negotiated role has no local demand.
    let mut fixture = Fixture::start_with_config(false, Some(2), true).await?;
    let (mut client, mut server) = fixture.sessions().await?;
    exchange(&mut client, &mut server).await?;
    assert!(matches!(
        fixture.client_siblings.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        fixture.server_siblings.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    client.cancel.cancel();
    timeout(TEST_TIMEOUT, server.cancel.cancelled()).await?;
    let (mut replacement_client, mut replacement_server) = fixture.sessions().await?;
    assert_eq!(replacement_client.conn_id, client.conn_id);
    assert_ne!(replacement_client.id, client.id);
    exchange(&mut replacement_client, &mut replacement_server).await?;
    assert!(fixture.connection.close_reason().is_none());
    fixture.close().await
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

async fn raw_connection() -> Result<(Router, Endpoint, Connection, Connection), BoxError> {
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let transport = || {
        let mut transport = local.transport_config();
        transport.stream_receive_window(64_000u32.into());
        transport.receive_window(128_000u32.into());
        transport.send_window(64_000);
        transport
    };
    let server = LocalEndpointFactory::with_transport_config(transport())
        .endpoint(93201)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(transport())
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
    Ok((router, client, connection, remote))
}

/// Run the real post-handshake connection loop with a raw peer controlling setup bytes.
struct RawFixture {
    router: Router,
    client: Endpoint,
    connection: Connection,
    serving: AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
    shutdown: CancellationToken,
    service: Arc<PairService>,
    sessions: mpsc::Receiver<Peer>,
    siblings: mpsc::Receiver<Peer>,
}

impl RawFixture {
    async fn start(slots: usize, setup_timeout: Duration) -> Result<Self, BoxError> {
        let (router, client, connection, remote) = raw_connection().await?;
        let local = ZakuraLocalLimits::from_config(&Config::default());
        let (sessions_tx, sessions) = mpsc::channel(2);
        let (siblings_tx, siblings) = mpsc::channel(1);
        let service = Arc::new(PairService {
            opening: OrderedStreamOpening::InitiatorOnly,
            fail_first_reservation: std::sync::atomic::AtomicBool::new(false),
            capacity: Some(Arc::new(Semaphore::new(slots))),
            sessions: sessions_tx,
        });
        let handler = ZakuraProtocolHandler::new_with_registry(
            ZakuraSupervisorHandle::new(16),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            local.clone(),
            Arc::new(ServiceRegistry::new(vec![
                service.clone(),
                Arc::new(SiblingService {
                    sessions: siblings_tx,
                    retired: false,
                }),
            ])?),
        );
        let shutdown = handler.shutdown.clone();
        let mut limits = local.clamp(&local.initial_limits());
        limits.prelude_timeout = setup_timeout;
        let peer_id = ZakuraPeerId::new(client.node_id().as_bytes().to_vec())?;
        let transcript_hash = native_connection_transcript_hash(
            ServicePeerDirection::Inbound,
            &router.endpoint().node_id(),
            &client.node_id(),
        );
        let serving = AbortOnDropHandle::new(tokio::spawn(async move {
            handler
                .register_and_serve(
                    remote,
                    peer_id,
                    None,
                    ConnectionServeContext {
                        limits,
                        accepted_capabilities: DATA.capability | SIBLING.capability,
                        role: "responder",
                        direction: ServicePeerDirection::Inbound,
                        transcript_hash,
                        i_open_collision_winner: false,
                        conn: ZakuraConnTrace::without_peer(1),
                    },
                )
                .await
        }));
        Ok(Self {
            router,
            client,
            connection,
            serving,
            shutdown,
            service,
            sessions,
            siblings,
        })
    }

    fn capacity(&self) -> &Arc<Semaphore> {
        self.service.capacity.as_ref().unwrap()
    }

    async fn wait_for_slots(&self, count: usize, deadline: Duration) -> Result<(), BoxError> {
        timeout(deadline, async {
            while self.capacity().available_permits() != count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok(())
    }

    async fn offer(
        &self,
        stream: Stream,
        pair_id: Option<u64>,
    ) -> Result<(SendStream, RecvStream), BoxError> {
        let (mut send, recv) = timeout(TEST_TIMEOUT, self.connection.open_bi()).await??;
        let mut bytes = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        }
        .encode()?;
        if let Some(id) = pair_id {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        timeout(TEST_TIMEOUT, send.write_all(&bytes)).await??;
        Ok((send, recv))
    }

    async fn close(mut self) -> Result<(), BoxError> {
        self.shutdown.cancel();
        timeout(Duration::from_millis(500), &mut self.serving).await???;
        self.connection.close(0u32.into(), b"test complete");
        timeout(TEST_TIMEOUT, self.client.close()).await?;
        timeout(TEST_TIMEOUT, self.router.shutdown()).await??;
        Ok(())
    }
}

#[tokio::test]
async fn withheld_pair_id_does_not_reserve_service_capacity() -> Result<(), BoxError> {
    let fixture = RawFixture::start(1, Duration::from_secs(2)).await?;
    let (mut send, _recv) = fixture.offer(DATA, None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(fixture.capacity().available_permits(), 1);
    let outbound = fixture
        .service
        .reserve_ordered_session(ServicePeerDirection::Outbound)?;
    drop(outbound);
    send.write_all(&1u64.to_le_bytes()).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    fixture.close().await
}

#[tokio::test]
async fn slow_pair_setup_does_not_delay_expiry_or_shutdown() -> Result<(), BoxError> {
    let fixture = RawFixture::start(1, Duration::from_secs(2)).await?;
    let _first = fixture.offer(DATA, Some(1)).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _slow = fixture.offer(REQUESTS, None).await?;
    fixture
        .wait_for_slots(1, Duration::from_millis(1500))
        .await
        .expect("the earlier pair expires while the later pair ID is still missing");
    fixture.close().await
}

#[tokio::test]
async fn expired_pair_cannot_reclaim_capacity_before_an_outgoing_session() -> Result<(), BoxError> {
    let fixture = RawFixture::start(1, Duration::from_secs(1)).await?;
    let _first = fixture.offer(DATA, Some(1)).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
    for id in 2..5 {
        let (_send, mut recv) = fixture.offer(DATA, Some(id)).await?;
        assert!(
            timeout(Duration::from_millis(500), recv.read_exact(&mut [0; 1]))
                .await?
                .is_err()
        );
        assert_eq!(fixture.capacity().available_permits(), 1);
    }
    let outbound = fixture
        .service
        .reserve_ordered_session(ServicePeerDirection::Outbound)?;
    assert_eq!(fixture.capacity().available_permits(), 0);
    drop(outbound);
    fixture.close().await
}

#[tokio::test]
async fn paired_replacement_during_cleanup_preserves_the_connection() -> Result<(), BoxError> {
    let mut fixture = RawFixture::start(2, Duration::from_secs(3)).await?;
    let (mut sibling_send, _sibling_recv) = fixture.offer(SIBLING, None).await?;
    let mut sibling = timeout(TEST_TIMEOUT, fixture.siblings.recv())
        .await?
        .ok_or("missing sibling")?;
    let (mut sibling_recv, _sibling_send) = sibling.take_stream(SIBLING.kind).unwrap();
    let mut old_data = fixture.offer(DATA, Some(1)).await?;
    let mut old_requests = fixture.offer(REQUESTS, Some(1)).await?;
    let old = Session::receive(&mut fixture.sessions).await?;

    let (mut _new_data_send, mut new_data_recv) = fixture.offer(DATA, Some(2)).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    let (mut new_requests_send, mut _new_requests_recv) = fixture.offer(REQUESTS, None).await?;
    // Keep the new setup read pending while the old pair's workers exit.
    tokio::time::sleep(Duration::from_millis(200)).await;
    old_data.0.reset(0u32.into())?;
    old_data.1.stop(0u32.into())?;
    old_requests.0.reset(0u32.into())?;
    old_requests.1.stop(0u32.into())?;
    new_requests_send.write_all(&2u64.to_le_bytes()).await?;
    timeout(TEST_TIMEOUT, old.cancel.cancelled()).await?;
    let old_id = old.id;
    drop(old);
    let mut byte = [0; 1];
    let first_offer = tokio::select! {
        session = Session::receive(&mut fixture.sessions) => Some(session?),
        closed = new_data_recv.read_exact(&mut byte) => {
            assert!(closed.is_err());
            None
        }
    };
    let mut replacement = match first_offer {
        Some(session) => session,
        None => {
            assert!(
                !sibling.cancel_token().is_cancelled(),
                "a raced offer must not close the connection"
            );
            fixture.wait_for_slots(2, TEST_TIMEOUT).await?;
            (_new_data_send, new_data_recv) = fixture.offer(DATA, Some(3)).await?;
            (new_requests_send, _new_requests_recv) = fixture.offer(REQUESTS, Some(3)).await?;
            Session::receive(&mut fixture.sessions).await?
        }
    };
    assert_ne!(replacement.id, old_id);

    let request = frame(1, 7, 8);
    new_requests_send
        .write_all(&request.encode(REQUESTS.frame_cap)?)
        .await?;
    assert_eq!(
        timeout(TEST_TIMEOUT, replacement.request_recv.recv()).await?,
        Some(request)
    );
    let response = frame(2, 8, 64);
    timeout(TEST_TIMEOUT, replacement.data_send.send(response.clone())).await??;
    assert_eq!(
        read_frame(
            &mut new_data_recv,
            DATA.frame_cap,
            &[],
            None,
            TEST_TIMEOUT,
            Some(TEST_TIMEOUT)
        )
        .await?,
        response
    );
    let ping = frame(3, 9, 8);
    sibling_send
        .write_all(&ping.encode(SIBLING.frame_cap)?)
        .await?;
    assert_eq!(
        timeout(TEST_TIMEOUT, sibling_recv.recv()).await?,
        Some(ping)
    );
    assert!(!replacement.connection_cancel.is_cancelled());
    assert!(!sibling.cancel_token().is_cancelled());
    fixture.close().await
}

fn raw_worker_context(client: &Endpoint, slots: Arc<Semaphore>) -> StreamWorkerContext {
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let cancel = CancellationToken::new();
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
    StreamWorkerContext {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: ZakuraPeerId::new(client.node_id().as_bytes().to_vec()).unwrap(),
        stream_id: 1,
        _permit: slots.try_acquire_owned().unwrap(),
        limits: local.clamp(&local.initial_limits()),
        inbound_frame_cap: DATA.frame_cap,
        message_payload_limits: &[],
        message_types: None,
        queue_depths: None,
        session_resources: None,
        outbound_frame_cap: DATA.frame_cap,
        message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(128))),
        connection_token: cancel.clone(),
        stream_token: cancel.child_token(),
        close_cause: CloseCause::new(),
        freshness_tx,
    }
}

#[tokio::test]
async fn paired_request_reader_close_interrupts_a_blocked_write() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (router, client, connection, remote) = raw_connection().await?;
    for reset in [false, true] {
        let (mut peer_send, mut peer_recv) = connection.open_bi().await?;
        peer_send
            .write_all(&frame(1, 0, 0).encode(DATA.frame_cap)?)
            .await?;
        let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
        let slots = Arc::new(Semaphore::new(1));
        let context = raw_worker_context(&client, slots.clone());
        let connection_cancel = context.connection_token.clone();
        let pair_cancel = context.stream_token.clone();
        let remote_close = CancellationToken::new();
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: REQUESTS.kind,
            stream_version: REQUESTS.version,
            request_id: None,
            max_frame_bytes: DATA.frame_cap,
        };
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
        let (outbound_tx, outbound_rx) = worker_framed_channel(1);
        let mut worker =
            AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker_with_policy(
                send,
                recv,
                prelude,
                context,
                inbound_tx,
                outbound_rx,
                1,
                OrderedWritePolicy::PairRequests,
                Some(remote_close.clone()),
            )));
        assert_eq!(
            timeout(TEST_TIMEOUT, inbound_rx.recv()).await?,
            Some(frame(1, 0, 0))
        );
        let resources = Arc::new(Semaphore::new(1));
        outbound_tx.try_reserve_guarded().unwrap().send(
            frame(1, 17, 1024 * 1024),
            crate::zakura::FrameGuard::new(Arc::new(resources.clone().try_acquire_owned()?)),
        );
        // Read only the first byte: the application write cannot finish within
        // the smaller QUIC windows, independently of scheduling or elapsed time.
        timeout(TEST_TIMEOUT, peer_recv.read_exact(&mut [0; 1])).await??;
        assert_eq!(resources.available_permits(), 0);
        if reset {
            peer_send.reset(0u32.into())?;
        } else {
            peer_send.finish()?;
        }
        timeout(Duration::from_secs(2), &mut worker)
            .await
            .expect("closing the reader interrupts a flow-controlled request write")?;
        assert!(remote_close.is_cancelled());
        assert!(pair_cancel.is_cancelled());
        assert!(!connection_cancel.is_cancelled());
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(resources.available_permits(), 1);
    }
    connection.close(0u32.into(), b"done");
    timeout(TEST_TIMEOUT, client.close()).await?;
    timeout(TEST_TIMEOUT, router.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn request_response_allowlists_reject_headers_before_payloads() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (router, client, connection, remote) = raw_connection().await?;
    let stream = Stream {
        kind: LEGACY_REQUEST_STREAM_KIND,
        mode: StreamMode::RequestResponse,
        ..DATA
    };
    let mut header = Vec::new();
    header.extend_from_slice(&u16::MAX.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&1024u32.to_le_bytes());
    let (mut peer_send, _peer_recv) = connection.open_bi().await?;
    peer_send.write_all(&header).await?;
    let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
    let mut context = raw_worker_context(&client, Arc::new(Semaphore::new(1)));
    context.message_types = Some(&[LEGACY_REQUEST_PING, LEGACY_RESPONSE_PONG]);
    let cancel = context.connection_token.clone();
    let limits = context.limits;
    let types = context.message_types;
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: Some(42),
        max_frame_bytes: stream.frame_cap,
    };
    timeout(
        Duration::from_secs(2),
        request_stream_worker(
            send,
            recv,
            prelude,
            context,
            Arc::new(ServiceRegistry::new(vec![])?),
        ),
    )
    .await
    .expect("the request header is rejected without waiting for its absent payload");
    assert!(cancel.is_cancelled());

    let response = write_outbound_request_frame(
        &connection,
        limits,
        stream,
        &[],
        types,
        42,
        LEGACY_REQUEST_PING,
        0,
        Vec::new(),
    );
    let responder = async {
        let (mut send, mut recv) = remote.accept_bi().await?;
        read_stream_prelude(&mut recv, TEST_TIMEOUT).await?;
        let request = read_frame(
            &mut recv,
            stream.frame_cap,
            &[],
            types,
            TEST_TIMEOUT,
            Some(TEST_TIMEOUT),
        )
        .await?;
        assert_eq!(request.message_type, LEGACY_REQUEST_PING);
        send.write_all(&header).await?;
        Ok::<_, BoxError>((send, recv))
    };
    let (result, held_stream) = tokio::join!(timeout(Duration::from_secs(2), response), responder);
    let _held_stream = held_stream?;
    assert!(
        matches!(result.expect("the response header is rejected before its absent payload"),
            Err(OutboundRequestError::Fatal(error))
                if matches!(error.downcast_ref::<ZakuraHandlerError>(), Some(ZakuraHandlerError::InvalidMessageType(u16::MAX)))
        )
    );
    connection.close(0u32.into(), b"done");
    timeout(TEST_TIMEOUT, client.close()).await?;
    timeout(TEST_TIMEOUT, router.shutdown()).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_pairs_expire_and_mismatched_roles_release_stream_permits(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (router, client, connection, remote) = raw_connection().await?;
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let permits = Arc::new(Semaphore::new(2));
    let (sessions, _sessions_rx) = mpsc::channel(1);
    let handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        local.clone(),
        Arc::new(ServiceRegistry::new(vec![Arc::new(PairService {
            capacity: None,
            opening: OrderedStreamOpening::EitherSide,
            fail_first_reservation: std::sync::atomic::AtomicBool::new(false),
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
        is_initiator: false,
        direction: ServicePeerDirection::Inbound,
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
            assert!(pending
                .reserve_or_share(PAIR, &handler.registry, ServicePeerDirection::Inbound)
                .is_err());
            tokio::time::sleep(limits.prelude_timeout).await;
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

#[tokio::test]
async fn ineligible_pair_opener_is_rejected_before_service_reservation() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (router, client, connection, remote) = raw_connection().await?;
    let (sessions, _sessions_rx) = mpsc::channel(1);
    let service = Arc::new(PairService {
        capacity: None,
        opening: OrderedStreamOpening::InitiatorOnly,
        fail_first_reservation: std::sync::atomic::AtomicBool::new(true),
        sessions,
    });
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        local.clone(),
        Arc::new(ServiceRegistry::new(vec![service.clone()])?),
    );
    let slots = Arc::new(Semaphore::new(2));
    let context = raw_worker_context(&client, Arc::new(Semaphore::new(1)));
    let mut workers = JoinSet::new();
    let mut open_limiter = TokenBucket::new(100);
    let mut buckets = MessageRateBuckets::new();
    let mut pending = PendingOrderedPairs::default();
    let (exits, _exit_rx) = mpsc::unbounded_channel();
    let mut admission = StreamAdmission {
        is_initiator: true,
        direction: ServicePeerDirection::Outbound,
        conn: context.conn,
        peer_id: &context.peer_id,
        stream_sem: &slots,
        open_limiter: &mut open_limiter,
        message_buckets: &mut buckets,
        workers: &mut workers,
        limits: context.limits,
        accepted_capabilities: DATA.capability,
        connection_token: context.connection_token.clone(),
        close_cause: context.close_cause,
        freshness_tx: context.freshness_tx,
    };
    let (mut send, _recv) = connection.open_bi().await?;
    // Withhold the pair ID: even the first role cannot reserve service capacity.
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: REQUESTS.kind,
        stream_version: REQUESTS.version,
        request_id: None,
        max_frame_bytes: DATA.frame_cap,
    };
    send.write_all(&prelude.encode()?).await?;
    let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
    assert!(timeout(
        Duration::from_secs(1),
        handler.admit_bi_stream(send, recv, &mut admission, 2, exits, &mut pending,)
    )
    .await?
    .is_none());
    assert!(
        service.fail_first_reservation.load(Ordering::SeqCst),
        "an ineligible opener must not call service reservation"
    );
    assert!(context.connection_token.is_cancelled());
    assert_eq!(slots.available_permits(), 2);
    assert!(pending.deadline().is_none());
    assert!(workers.is_empty());
    connection.close(0u32.into(), b"done");
    timeout(TEST_TIMEOUT, client.close()).await?;
    timeout(TEST_TIMEOUT, router.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn initial_pair_capacity_race_preserves_the_connection() -> Result<(), BoxError> {
    let mut fixture = Fixture::start_with_reservation_race(true).await?;
    let mut client_sibling = timeout(TEST_TIMEOUT, fixture.client_siblings.recv())
        .await?
        .ok_or("missing client sibling")?;
    let mut server_sibling = timeout(TEST_TIMEOUT, fixture.server_siblings.recv())
        .await?
        .ok_or("missing server sibling")?;
    let (mut recv, _) = client_sibling.take_stream(SIBLING.kind).unwrap();
    let (_, send) = server_sibling.take_stream(SIBLING.kind).unwrap();
    let ping = frame(1, 19, 64);
    timeout(TEST_TIMEOUT, send.send(ping.clone())).await??;
    assert_eq!(timeout(TEST_TIMEOUT, recv.recv()).await?, Some(ping));
    let (mut client, mut server) = fixture.sessions().await?;
    exchange(&mut client, &mut server).await?;
    assert!(fixture.connection.close_reason().is_none());
    fixture.close().await
}
