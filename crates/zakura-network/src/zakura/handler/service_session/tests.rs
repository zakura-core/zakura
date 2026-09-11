use super::*;
use crate::zakura::testkit::LocalEndpointFactory;
use tokio_util::task::AbortOnDropHandle;

const DATA: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 2 * 1024 * 1024,
    capability: 1 << 16,
    mode: StreamMode::Persistent,
};
const REQUESTS: Stream = Stream { kind: 65, ..DATA };
const EVENTS: Stream = Stream { kind: 67, ..DATA };
const ONE_SHOT: Stream = Stream {
    kind: 68,
    mode: StreamMode::RequestResponse,
    ..DATA
};
const SIBLING: Stream = Stream {
    kind: 66,
    capability: 1 << 17,
    ..DATA
};
const ALPN: &[u8] = b"/zakura/test/ordered-pair/1";
const TEST_DATA_WRITE_TIMEOUT: Duration = Duration::from_secs(32);
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct SessionService {
    streams: &'static [Stream],
    opening: SessionOpening,
    fail_first_reservation: std::sync::atomic::AtomicBool,
    capacity: Option<Arc<Semaphore>>,
    sessions: mpsc::Sender<Peer>,
}

#[derive(Debug)]
struct SessionSlot {
    _permit: OwnedSemaphorePermit,
}

impl crate::zakura::SessionResources for SessionSlot {
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

impl Service for SessionService {
    fn reserve_session(
        &self,
        _: ServicePeerDirection,
    ) -> Result<Option<Arc<dyn crate::zakura::SessionResources>>, crate::zakura::SessionFull> {
        // Inject the outcome of another connection taking the final slot after
        // this connection's advisory OpenNow check. A retry can succeed.
        if self.fail_first_reservation.swap(false, Ordering::SeqCst) {
            Err(crate::zakura::SessionFull)
        } else if let Some(capacity) = &self.capacity {
            let permit = capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| crate::zakura::SessionFull)?;
            Ok(Some(Arc::new(SessionSlot { _permit: permit })))
        } else {
            Ok(None)
        }
    }
    fn name(&self) -> &'static str {
        "test-pair"
    }
    fn streams(&self) -> &[Stream] {
        self.streams
    }
    fn stream_write_policy(&self, stream: Stream) -> StreamWritePolicy {
        if stream == REQUESTS {
            StreamWritePolicy::UntilCancelled
        } else {
            StreamWritePolicy::Timeout(TEST_DATA_WRITE_TIMEOUT)
        }
    }
    fn stream_queue_depths(&self, stream: Stream) -> Option<(usize, usize)> {
        Some(if stream == EVENTS { (3, 3) } else { (1, 1) })
    }
    fn as_request_response(&self) -> Option<&dyn crate::zakura::RequestResponseService> {
        Some(self)
    }
    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
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

impl crate::zakura::RequestResponseService for SessionService {
    fn request_frame<'a>(
        &'a self,
        _: ZakuraPeerId,
        _: u16,
        _: u64,
        _: u32,
        _: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>> {
        Box::pin(async move { Ok(vec![frame]) })
    }
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
        Self::start_with_streams(
            fail_first_reservation,
            max_open_streams,
            retired_sibling,
            &[DATA, REQUESTS],
        )
        .await
    }

    async fn start_with_streams(
        fail_first_reservation: bool,
        max_open_streams: Option<u16>,
        retired_sibling: bool,
        streams: &'static [Stream],
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
                        Arc::new(SessionService {
                            streams,
                            capacity: None,
                            opening: SessionOpening::EitherSide,
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
    timeout(TEST_DATA_WRITE_TIMEOUT + Duration::from_secs(10), async {
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
    assert!(started.elapsed() >= TEST_DATA_WRITE_TIMEOUT);
    assert_eq!(
        client.data_recv.failure(),
        Some(OrderedStreamFailure::WriteTimeout)
    );
    assert_eq!(
        client.request_recv.failure(),
        Some(OrderedStreamFailure::WriteTimeout)
    );
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
    service: Arc<SessionService>,
    sessions: mpsc::Receiver<Peer>,
    siblings: mpsc::Receiver<Peer>,
}

#[tokio::test]
async fn three_stream_session_reopens_at_the_exact_stream_limit() -> Result<(), BoxError> {
    let mut fixture =
        Fixture::start_with_streams(false, Some(3), true, &[DATA, REQUESTS, EVENTS]).await?;
    let mut previous_id = None;
    for _ in 0..2 {
        let mut client = timeout(TEST_TIMEOUT, fixture.client_sessions.recv())
            .await?
            .ok_or("missing client session")?;
        let mut server = timeout(TEST_TIMEOUT, fixture.server_sessions.recv())
            .await?
            .ok_or("missing server session")?;
        let mut client_streams = Vec::new();
        let mut server_streams = Vec::new();
        for stream in [DATA, REQUESTS, EVENTS] {
            client_streams.push(client.take_stream_with_session_id(stream.kind).unwrap());
            server_streams.push(server.take_stream_with_session_id(stream.kind).unwrap());
        }
        let client_id = client_streams[0].0;
        let server_id = server_streams[0].0;
        assert_ne!(previous_id, Some(client_id));
        previous_id = Some(client_id);
        for ((id, _, send), (remote_id, recv, _)) in
            client_streams.iter().zip(server_streams.iter_mut())
        {
            assert_eq!(*id, client_id);
            assert_eq!(*remote_id, server_id);
            let message = frame(1, 21, 8);
            timeout(TEST_TIMEOUT, send.send(message.clone())).await??;
            assert_eq!(timeout(TEST_TIMEOUT, recv.recv()).await?, Some(message));
        }
        client.service_cancel_token().cancel();
        timeout(TEST_TIMEOUT, server.service_cancel_token().cancelled()).await?;
        assert!(!client.cancel_token().is_cancelled());
    }
    fixture.close().await
}

#[tokio::test]
async fn invalid_third_member_releases_the_entire_pending_session() -> Result<(), BoxError> {
    for (stream, id) in [(REQUESTS, 9), (EVENTS, 10), (EVENTS, 0)] {
        let mut fixture =
            RawFixture::start_with_streams(1, Duration::from_secs(3), &[DATA, REQUESTS, EVENTS])
                .await?;
        let _data = fixture.offer(DATA, Some(9)).await?;
        let _requests = fixture.offer(REQUESTS, Some(9)).await?;
        fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
        let _invalid = fixture.offer(stream, Some(id)).await?;
        fixture.wait_for_slots(1, Duration::from_secs(1)).await?;
        if id == 10 {
            assert!(fixture.connection.close_reason().is_none());
            assert!(!fixture.serving.is_finished());
        } else {
            timeout(Duration::from_secs(1), async {
                while !fixture.serving.is_finished() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
        }
        assert!(fixture.sessions.try_recv().is_err());
        fixture.close().await?;
    }
    Ok(())
}

impl RawFixture {
    async fn start(slots: usize, setup_timeout: Duration) -> Result<Self, BoxError> {
        Self::start_with_streams(slots, setup_timeout, &[DATA, REQUESTS]).await
    }

    async fn start_with_streams(
        slots: usize,
        setup_timeout: Duration,
        streams: &'static [Stream],
    ) -> Result<Self, BoxError> {
        let (router, client, connection, remote) = raw_connection().await?;
        let local = ZakuraLocalLimits::from_config(&Config::default());
        let (sessions_tx, sessions) = mpsc::channel(2);
        let (siblings_tx, siblings) = mpsc::channel(1);
        let service = Arc::new(SessionService {
            streams,
            opening: SessionOpening::InitiatorOnly,
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
async fn three_stream_session_waits_for_every_member_and_leaves_requests_independent(
) -> Result<(), BoxError> {
    let mut fixture = RawFixture::start_with_streams(
        1,
        Duration::from_secs(3),
        &[EVENTS, ONE_SHOT, REQUESTS, DATA],
    )
    .await?;
    let (mut sibling_send, _sibling_recv) = fixture.offer(SIBLING, None).await?;
    let mut sibling = timeout(TEST_TIMEOUT, fixture.siblings.recv())
        .await?
        .ok_or("missing sibling")?;
    let (mut sibling_recv, _sibling_sender) = sibling.take_stream(SIBLING.kind).unwrap();

    let mut events = fixture.offer(EVENTS, Some(42)).await?;
    let mut requests = fixture.offer(REQUESTS, Some(42)).await?;
    let event = frame(3, 11, 8);
    let request = frame(1, 12, 8);
    events.0.write_all(&event.encode(EVENTS.frame_cap)?).await?;
    requests
        .0
        .write_all(&request.encode(REQUESTS.frame_cap)?)
        .await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    assert!(
        timeout(Duration::from_millis(100), fixture.sessions.recv())
            .await
            .is_err(),
        "two of three required streams cannot start the service"
    );

    let _data = fixture.offer(DATA, Some(42)).await?;
    let mut peer = timeout(TEST_TIMEOUT, fixture.sessions.recv())
        .await?
        .ok_or("missing session")?;
    let cancel = peer.service_cancel_token();
    let (data_id, mut data_recv, _data_send) = peer.take_stream_with_session_id(DATA.kind).unwrap();
    let (request_id, mut request_recv, _request_send) =
        peer.take_stream_with_session_id(REQUESTS.kind).unwrap();
    let (event_id, mut event_recv, event_send) =
        peer.take_stream_with_session_id(EVENTS.kind).unwrap();
    assert_eq!((data_id, data_id), (request_id, event_id));
    assert_eq!(
        event_send.max_capacity(),
        3,
        "the service controls each stream's queue"
    );
    assert_eq!(timeout(TEST_TIMEOUT, event_recv.recv()).await?, Some(event));
    assert_eq!(
        timeout(TEST_TIMEOUT, request_recv.recv()).await?,
        Some(request)
    );
    assert!(peer.take_stream(ONE_SHOT.kind).is_none());

    // A per-request stream carries no session identifier and completes independently.
    let (mut send, mut recv) = fixture.connection.open_bi().await?;
    let mut bytes = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: ONE_SHOT.kind,
        stream_version: ONE_SHOT.version,
        request_id: Some(99),
        max_frame_bytes: ONE_SHOT.frame_cap,
    }
    .encode()?;
    let echo = frame(4, 13, 8).encode(ONE_SHOT.frame_cap)?;
    bytes.extend_from_slice(&echo);
    timeout(TEST_TIMEOUT, send.write_all(&bytes)).await??;
    send.finish()?;
    assert_eq!(timeout(TEST_TIMEOUT, recv.read_to_end(1024)).await??, echo);
    assert!(!cancel.is_cancelled());

    events.0.reset(0u32.into())?;
    events.1.stop(0u32.into())?;
    timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
    assert!(timeout(TEST_TIMEOUT, data_recv.recv()).await?.is_none());
    assert!(timeout(TEST_TIMEOUT, request_recv.recv()).await?.is_none());
    assert!(timeout(TEST_TIMEOUT, event_recv.recv()).await?.is_none());
    assert!(!peer.cancel_token().is_cancelled());
    let ping = frame(2, 14, 8);
    sibling_send
        .write_all(&ping.encode(SIBLING.frame_cap)?)
        .await?;
    assert_eq!(
        timeout(TEST_TIMEOUT, sibling_recv.recv()).await?,
        Some(ping)
    );
    fixture.close().await
}

#[tokio::test]
async fn incomplete_three_stream_session_releases_all_arrivals_on_expiry() -> Result<(), BoxError> {
    let mut fixture =
        RawFixture::start_with_streams(1, Duration::from_millis(300), &[DATA, REQUESTS, EVENTS])
            .await?;
    let (_request_send, mut request_recv) = fixture.offer(REQUESTS, Some(1)).await?;
    let (_event_send, mut event_recv) = fixture.offer(EVENTS, Some(1)).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
    assert!(fixture.sessions.try_recv().is_err());
    assert!(timeout(TEST_TIMEOUT, request_recv.read_exact(&mut [0; 1]))
        .await?
        .is_err());
    assert!(timeout(TEST_TIMEOUT, event_recv.read_exact(&mut [0; 1]))
        .await?
        .is_err());
    assert!(fixture.connection.close_reason().is_none());
    fixture.close().await
}

#[tokio::test]
async fn withheld_pair_id_does_not_reserve_service_capacity() -> Result<(), BoxError> {
    let fixture = RawFixture::start(1, Duration::from_secs(2)).await?;
    let (mut send, _recv) = fixture.offer(DATA, None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(fixture.capacity().available_permits(), 1);
    let outbound = fixture
        .service
        .reserve_session(ServicePeerDirection::Outbound)?;
    drop(outbound);
    send.write_all(&1u64.to_le_bytes()).await?;
    fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
    fixture.close().await
}

#[tokio::test]
async fn abandoned_half_pair_retry_preserves_the_connection() -> Result<(), BoxError> {
    // Also cover a reset arriving after the retry, since QUIC streams can reorder.
    for reset_before_retry in [true, false] {
        let setup_timeout = Duration::from_secs(1);
        let mut fixture = RawFixture::start(1, setup_timeout).await?;
        let (mut sibling_send, _sibling_recv) = fixture.offer(SIBLING, None).await?;
        let mut sibling = timeout(TEST_TIMEOUT, fixture.siblings.recv())
            .await?
            .ok_or("missing sibling")?;
        let (mut sibling_recv, _sibling_send) = sibling.take_stream(SIBLING.kind).unwrap();
        let (send, recv) = fixture.offer(DATA, Some(1)).await?;
        let mut abandoned = Some(SetupIo::new(send, recv));
        fixture.wait_for_slots(0, TEST_TIMEOUT).await?;
        if reset_before_retry {
            drop(abandoned.take());
        }

        let (_retry_send, mut retry_recv) = fixture.offer(DATA, Some(2)).await?;
        assert!(timeout(TEST_TIMEOUT, retry_recv.read_exact(&mut [0; 1]))
            .await?
            .is_err());
        fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
        drop(abandoned);
        assert!(
            !sibling.cancel_token().is_cancelled(),
            "retrying an abandoned half pair must preserve sibling services"
        );
        assert!(fixture.sessions.try_recv().is_err());
        let ping = frame(2, 14, 8);
        sibling_send
            .write_all(&ping.encode(SIBLING.frame_cap)?)
            .await?;
        assert_eq!(
            timeout(TEST_TIMEOUT, sibling_recv.recv()).await?,
            Some(ping)
        );

        let (_early_send, mut early_recv) = fixture.offer(DATA, Some(3)).await?;
        assert!(timeout(TEST_TIMEOUT, early_recv.read_exact(&mut [0; 1]))
            .await?
            .is_err());
        assert_eq!(fixture.capacity().available_permits(), 1);
        let outbound = fixture
            .service
            .reserve_session(ServicePeerDirection::Outbound)?;
        drop(outbound);

        tokio::time::sleep(setup_timeout).await;
        let _data = fixture.offer(DATA, Some(4)).await?;
        let (mut requests_send, _requests_recv) = fixture.offer(REQUESTS, Some(4)).await?;
        let mut replacement = Session::receive(&mut fixture.sessions).await?;
        let request = frame(1, 7, 8);
        requests_send
            .write_all(&request.encode(REQUESTS.frame_cap)?)
            .await?;
        assert_eq!(
            timeout(TEST_TIMEOUT, replacement.request_recv.recv()).await?,
            Some(request)
        );
        assert_eq!(fixture.capacity().available_permits(), 0);
        assert!(!sibling.cancel_token().is_cancelled());
        fixture.close().await?;
    }
    Ok(())
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
        .reserve_session(ServicePeerDirection::Outbound)?;
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
        write_policy: StreamWritePolicy::UntilCancelled,
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
    for (reset, drop_receiver) in [(false, false), (true, false), (false, true), (true, true)] {
        let (mut peer_send, mut peer_recv) = connection.open_bi().await?;
        peer_send
            .write_all(&frame(1, 0, 0).encode(DATA.frame_cap)?)
            .await?;
        let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
        let slots = Arc::new(Semaphore::new(1));
        let context = raw_worker_context(&client, slots.clone());
        let connection_cancel = context.connection_token.clone();
        let pair_cancel = context.stream_token.clone();
        let failure_cause = OrderedStreamFailureCause::default();
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
                Some(failure_cause.clone()),
                None,
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
        if drop_receiver {
            drop(inbound_rx);
            peer_send
                .write_all(&frame(1, 19, 8).encode(DATA.frame_cap)?)
                .await?;
            assert!(timeout(Duration::from_millis(100), pair_cancel.cancelled())
                .await
                .is_err());
        }
        if reset {
            peer_send.reset(0u32.into())?;
        } else {
            peer_send.finish()?;
        }
        timeout(Duration::from_secs(2), &mut worker)
            .await
            .expect("closing the reader interrupts a flow-controlled request write")?;
        assert_eq!(failure_cause.get(), Some(OrderedStreamFailure::RemoteClose));
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
        Arc::new(ServiceRegistry::new(vec![Arc::new(SessionService {
            streams: &[DATA, REQUESTS],
            capacity: None,
            opening: SessionOpening::EitherSide,
            fail_first_reservation: std::sync::atomic::AtomicBool::new(false),
            sessions,
        })])?),
    );
    let mut limits = local.clamp(&local.initial_limits());
    limits.prelude_timeout = Duration::from_millis(100);
    let peer = ZakuraPeerId::new(client.node_id().as_bytes().to_vec())?;
    let (freshness, _freshness_rx) = watch::channel(Instant::now());
    let cancel = CancellationToken::new();
    let mut pending = PendingSessions::default();
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
                .reserve_or_share(
                    &handler.registry.session_layout(DATA).unwrap(),
                    &handler.registry,
                    ServicePeerDirection::Inbound
                )
                .is_err());
            tokio::time::sleep(limits.prelude_timeout).await;
        }
    }
    assert!(
        !cancel.is_cancelled(),
        "different pair identifiers retire only the incomplete session"
    );
    assert!(pending
        .reserve_or_share(
            &handler.registry.session_layout(DATA).unwrap(),
            &handler.registry,
            ServicePeerDirection::Inbound
        )
        .is_err());
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
    let service = Arc::new(SessionService {
        streams: &[DATA, REQUESTS],
        capacity: None,
        opening: SessionOpening::InitiatorOnly,
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
    let mut pending = PendingSessions::default();
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

#[tokio::test]
async fn session_retirement_preserves_remote_cause_and_local_neutrality() -> Result<(), BoxError> {
    for streams in [&[DATA][..], &[DATA, REQUESTS][..]] {
        for local in [false, true] {
            let mut fixture =
                RawFixture::start_with_streams(1, Duration::from_secs(3), streams).await?;
            let (mut peer_send, _peer_recv) = fixture
                .offer(DATA, (streams.len() > 1).then_some(73))
                .await?;
            let _requests = if streams.len() > 1 {
                Some(fixture.offer(REQUESTS, Some(73)).await?)
            } else {
                None
            };
            let mut peer = timeout(TEST_TIMEOUT, fixture.sessions.recv())
                .await?
                .ok_or("missing session")?;
            let cancel = peer.service_cancel_token();
            let connection_cancel = peer.cancel_token();
            let (mut recv, send) = peer.take_stream(DATA.kind).unwrap();
            // In a pair, the unused request member waits for the data member.
            drop(peer);
            assert!(timeout(Duration::from_millis(100), cancel.cancelled())
                .await
                .is_err());
            if local {
                cancel.cancel();
            } else {
                peer_send.finish()?;
            }
            timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
            assert!(timeout(TEST_TIMEOUT, recv.recv()).await?.is_none());
            assert_eq!(
                recv.failure(),
                (!local).then_some(OrderedStreamFailure::RemoteClose)
            );
            assert!(!connection_cancel.is_cancelled());
            drop(recv);
            drop(send);
            fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
            fixture.close().await?;
        }
    }
    Ok(())
}

/// Model the request owner's partial-write abort contract at the worker boundary.
#[derive(Debug)]
struct CancellingWriteClaim {
    cancel: CancellationToken,
    failure: OrderedStreamFailureCause,
    expected: Option<OrderedStreamFailure>,
}

impl crate::zakura::FrameWriteClaim for CancellingWriteClaim {
    fn try_start(&self) -> bool {
        true
    }

    fn written(&self) {
        panic!("the peer cannot accept this frame within its receive window");
    }
}

impl Drop for CancellingWriteClaim {
    fn drop(&mut self) {
        assert_eq!(self.failure.get(), self.expected);
        self.cancel.cancel();
    }
}

#[tokio::test]
async fn failed_write_records_cause_before_claim_cancels_session() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (router, client, connection, remote) = raw_connection().await?;
    for expected in [
        Some(OrderedStreamFailure::RemoteClose),
        Some(OrderedStreamFailure::WriteTimeout),
        None,
    ] {
        let (mut peer_send, mut peer_recv) = connection.open_bi().await?;
        peer_send
            .write_all(&frame(1, 0, 0).encode(DATA.frame_cap)?)
            .await?;
        let (send, recv) = timeout(TEST_TIMEOUT, remote.accept_bi()).await??;
        let slots = Arc::new(Semaphore::new(1));
        let mut context = raw_worker_context(&client, slots.clone());
        if expected == Some(OrderedStreamFailure::WriteTimeout) {
            context.write_policy = StreamWritePolicy::Timeout(Duration::from_millis(250));
        }
        let connection_cancel = context.connection_token.clone();
        let cancel = context.stream_token.clone();
        let failure = OrderedStreamFailureCause::default();
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: REQUESTS.kind,
            stream_version: REQUESTS.version,
            request_id: None,
            max_frame_bytes: DATA.frame_cap,
        };
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
        let (outbound_tx, outbound_rx) = worker_framed_channel(1);
        let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker_with_policy(
            send,
            recv,
            prelude,
            context,
            inbound_tx,
            outbound_rx,
            1,
            Some(failure.clone()),
            None,
        )));
        assert_eq!(
            timeout(TEST_TIMEOUT, inbound_rx.recv()).await?,
            Some(frame(1, 0, 0))
        );
        assert!(outbound_tx.try_reserve_guarded().unwrap().send_request(
            frame(1, 17, 1024 * 1024),
            Arc::new(CancellingWriteClaim {
                cancel: cancel.clone(),
                failure: failure.clone(),
                expected
            }),
        ));
        // Partial bytes prove the claim started. Withhold the remaining QUIC
        // credit so every case exercises a failed or cancelled partial write.
        timeout(TEST_TIMEOUT, peer_recv.read_exact(&mut [0; 1])).await??;
        match expected {
            Some(OrderedStreamFailure::RemoteClose) => peer_recv.stop(0u32.into())?,
            Some(OrderedStreamFailure::WriteTimeout) => {}
            None => cancel.cancel(),
        }
        timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
        assert_eq!(failure.get(), expected);
        timeout(TEST_TIMEOUT, worker).await??;
        assert_eq!(slots.available_permits(), 1);
        assert!(!connection_cancel.is_cancelled());
    }
    connection.close(0u32.into(), b"done");
    timeout(TEST_TIMEOUT, client.close()).await?;
    timeout(TEST_TIMEOUT, router.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn abandoned_application_session_releases_capacity_and_preserves_sibling(
) -> Result<(), BoxError> {
    for streams in [&[DATA][..], &[DATA, REQUESTS][..]] {
        let mut fixture =
            RawFixture::start_with_streams(1, Duration::from_secs(3), streams).await?;
        let (mut sibling_send, _sibling_recv) = fixture.offer(SIBLING, None).await?;
        let mut sibling = timeout(TEST_TIMEOUT, fixture.siblings.recv())
            .await?
            .ok_or("no sibling")?;
        let (mut sibling_recv, _sibling_send) = sibling.take_stream(SIBLING.kind).unwrap();
        let mut remote_handles = Vec::new();
        for stream in streams {
            remote_handles.push(
                fixture
                    .offer(*stream, (streams.len() > 1).then_some(71))
                    .await?,
            );
        }
        let peer = timeout(TEST_TIMEOUT, fixture.sessions.recv())
            .await?
            .ok_or("no session")?;
        let cancel = peer.service_cancel_token();
        drop(peer);
        timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
        fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
        // A late duplicate cancellation cannot release the reservation twice.
        cancel.cancel();
        let ping = frame(1, 19, 8);
        sibling_send
            .write_all(&ping.encode(SIBLING.frame_cap)?)
            .await?;
        assert_eq!(
            timeout(TEST_TIMEOUT, sibling_recv.recv()).await?,
            Some(ping)
        );
        assert_eq!(fixture.capacity().available_permits(), 1);
        assert!(!sibling.cancel_token().is_cancelled());
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn application_receive_half_or_sender_clone_keeps_session_alive() -> Result<(), BoxError> {
    for streams in [&[DATA][..], &[DATA, REQUESTS][..]] {
        for keep_receiver in [true, false] {
            let mut fixture =
                RawFixture::start_with_streams(1, Duration::from_secs(3), streams).await?;
            let (mut peer_send, mut peer_recv) = fixture
                .offer(DATA, (streams.len() > 1).then_some(74))
                .await?;
            let _requests = if streams.len() > 1 {
                Some(fixture.offer(REQUESTS, Some(74)).await?)
            } else {
                None
            };
            let mut peer = timeout(TEST_TIMEOUT, fixture.sessions.recv())
                .await?
                .ok_or("no session")?;
            let cancel = peer.service_cancel_token();
            let (mut recv, send) = peer.take_stream(DATA.kind).unwrap();
            drop(peer);
            let ping = frame(1, 19, 8);
            if keep_receiver {
                drop(send);
                assert!(timeout(Duration::from_millis(100), cancel.cancelled())
                    .await
                    .is_err());
                peer_send.write_all(&ping.encode(DATA.frame_cap)?).await?;
                assert_eq!(timeout(TEST_TIMEOUT, recv.recv()).await?, Some(ping));
                assert!(recv.failure().is_none());
                assert_eq!(fixture.capacity().available_permits(), 0);
                drop(recv);
            } else {
                let clone = send.clone();
                drop(send);
                drop(recv);
                assert!(timeout(Duration::from_millis(100), cancel.cancelled())
                    .await
                    .is_err());
                // Incoming traffic cannot retire a session with a retained sender.
                peer_send.write_all(&ping.encode(DATA.frame_cap)?).await?;
                assert!(timeout(Duration::from_millis(100), cancel.cancelled())
                    .await
                    .is_err());
                timeout(TEST_TIMEOUT, clone.send(ping.clone())).await??;
                assert_eq!(
                    read_frame(
                        &mut peer_recv,
                        DATA.frame_cap,
                        &[],
                        None,
                        TEST_TIMEOUT,
                        Some(TEST_TIMEOUT)
                    )
                    .await?,
                    ping
                );
                assert_eq!(fixture.capacity().available_permits(), 0);
                drop(clone);
            }
            timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
            fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
            fixture.close().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn abandoned_application_drains_queued_writes_before_retirement() -> Result<(), BoxError> {
    for streams in [&[DATA][..], &[DATA, REQUESTS][..]] {
        let mut fixture =
            RawFixture::start_with_streams(1, Duration::from_secs(3), streams).await?;
        let (mut peer_send, mut peer_recv) = fixture
            .offer(DATA, (streams.len() > 1).then_some(72))
            .await?;
        let mut requests = if streams.len() > 1 {
            Some(fixture.offer(REQUESTS, Some(72)).await?)
        } else {
            None
        };
        let mut peer = timeout(TEST_TIMEOUT, fixture.sessions.recv())
            .await?
            .ok_or("no session")?;
        let cancel = peer.service_cancel_token();
        let (recv, send) = peer.take_stream(DATA.kind).unwrap();
        drop(recv);
        let first = frame(1, 19, 1024 * 1024);
        let second = frame(1, 20, 1024 * 1024);
        timeout(TEST_TIMEOUT, send.send(first.clone())).await??;
        // A second enqueue proves the first frame left the bounded application queue.
        timeout(TEST_TIMEOUT, send.send(second.clone())).await??;
        drop(send);
        // The empty request stream must let the data stream drain before the
        // whole session retires, even though all application handles are gone.
        drop(peer);
        assert!(timeout(Duration::from_millis(100), cancel.cancelled())
            .await
            .is_err());
        // The 64 KB QUIC window keeps data writes blocked while this frame reaches
        // the dropped receiver, including on the otherwise idle request member.
        let incoming = frame(1, 23, 8).encode(DATA.frame_cap)?;
        match requests.as_mut() {
            Some((request_send, _)) => request_send.write_all(&incoming).await?,
            None => peer_send.write_all(&incoming).await?,
        }
        assert!(timeout(Duration::from_millis(100), cancel.cancelled())
            .await
            .is_err());
        assert_eq!(fixture.capacity().available_permits(), 0);
        for expected in [first, second] {
            assert_eq!(
                read_frame(
                    &mut peer_recv,
                    DATA.frame_cap,
                    &[],
                    None,
                    TEST_TIMEOUT,
                    Some(TEST_TIMEOUT)
                )
                .await?,
                expected
            );
        }
        timeout(TEST_TIMEOUT, cancel.cancelled()).await?;
        fixture.wait_for_slots(1, TEST_TIMEOUT).await?;
        assert_eq!(
            timeout(TEST_TIMEOUT, peer_recv.read(&mut [0; 1])).await??,
            None
        );
        assert!(fixture.connection.close_reason().is_none());
        fixture.close().await?;
    }
    Ok(())
}
