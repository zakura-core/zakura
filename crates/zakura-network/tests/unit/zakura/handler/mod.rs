mod bounded_decoding;
pub(super) mod connection;
mod frame_lifetime;
#[cfg(not(zakura_sim))]
mod handshake_cost;
mod quic_progress;
mod read_close_cause;
mod transport_observer;
use super::*;
use crate::zakura::P2P_V2_ALPN;
use crate::{
    protocol::internal::{InventoryResponse, Response},
    zakura::{
        legacy_gossip::{
            LegacyGossipError, LegacyRequestFrame, LegacyRequestKind, LegacyResponseCodec,
            RequestedInventory,
        },
        testkit::{await_until, LocalEndpointFactory, ZakuraTestNode},
        Event, HeaderSyncMisbehavior, PeerSession, ServicePeerLimits,
        LOCAL_MAX_CONTROL_FRAME_BYTES, LOCAL_MAX_MESSAGE_BYTES, MAX_BS_FRAME_BYTES,
        MAX_HS_MESSAGE_BYTES, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_DISCOVERY,
        ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_HEADER_SYNC_STREAM_VERSION,
        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
    },
    P2pStack,
};
use zakura_chain::{
    block::{self, Block},
    serialization::{ZcashDeserialize, MAX_PROTOCOL_MESSAGE_LEN},
    transaction::{self, UnminedTxId},
};
use zakura_test::vectors::{BLOCK_MAINNET_GENESIS_BYTES, BLOCK_TESTNET_141042_BYTES};

/// The stream window check compares against block sync's frame cap, so no
/// other native stream kind may carry a larger frame.
#[test]
fn block_sync_carries_the_largest_native_frame() {
    use crate::zakura::header_sync::v9::{CommonMessage, RegtestMessage};

    let streams: Vec<Stream> = super::super::legacy_gossip::legacy_gossip_streams()
        .iter()
        .chain(&CommonMessage::LAYOUT)
        .chain(&RegtestMessage::LAYOUT)
        .copied()
        .collect();
    for stream in &streams {
        assert!(
            stream.frame_cap <= MAX_BS_FRAME_BYTES,
            "stream kind {} carries {} bytes, more than a block-sync frame",
            stream.kind,
            stream.frame_cap
        );
    }
    // Discovery's frame is its largest peers payload and a frame header.
    let discovery = super::super::discovery::wire::PEERS_MAX_BYTES + FRAME_HEADER_BYTES;
    assert!(discovery <= MAX_BS_FRAME_BYTES as usize);
}

/// Regression for the mainnet dual-stack body-sync stall on
/// `temp-zakura-sync-test-7` during run `20260714T073939Z-cca353fd1287`.
/// Body sync stopped at height 2,725,606 with available download capacity.
/// Header sync continued to tip while block sync lost all peers.
///
/// The transport caused the stall. Block sync parks a peer that misses its
/// no-progress liveness deadline. The transport redials the peer before the
/// `no_progress_peer_cooldown` expires. The transport asks block sync for
/// demand only during connection setup.
/// Block sync refuses the parked peer.
/// Header sync and discovery keep the connection healthy.
/// The transport therefore does not redial it after the cooldown.
/// Repeating this sequence reduced the block-sync peer count to zero.
/// The node still retained healthy connections.
///
/// The test drives `serve_connection` and `BlockSyncService` over a local QUIC connection.
/// It parks the peer through the liveness path.
/// After the cooldown, block sync must receive a stream on the existing connection.
#[tokio::test]
async fn parked_block_sync_peer_gets_a_stream_when_its_cooldown_lapses() -> Result<(), BoxError> {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const COOLDOWN: Duration = Duration::from_secs(15);

    let _guard = zakura_test::init();

    let genesis = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])?;
    let anchor = (block::Height(0), genesis.hash());
    let frontiers = FullStateFrontiers {
        finalized_height: block::Height(0),
        verified_block_tip: block::Height(0),
        verified_block_hash: genesis.hash(),
    };
    let node = |seed| {
        ZakuraTestNode::builder(seed)
            .header_sync_driver(Config::default().network, anchor, frontiers, Some(anchor))
            .spawn()
    };
    let dialer = node(140).await?;
    let listener = node(141).await?;

    let listener_addr = listener.node_addr().await;
    let listener_peer = ZakuraPeerId::new(listener_addr.id.as_bytes().to_vec())?;
    let block_sync = dialer
        .block_sync()
        .expect("the header-sync driver spawns the block-sync reactor");

    // Block sync evicts and parks the peer after its no-progress deadline.
    // Keep the cooldown longer than connection setup so the first assertion
    // tests a live park even when the dial is slow.
    let parked_at = std::time::Instant::now();
    block_sync.park_peer_for_test(&listener_peer, COOLDOWN);
    dialer
        .connect_native_to_addr(listener_addr, CONNECT_TIMEOUT)
        .await?;

    // The park remains active.
    // Withhold block sync from this connection.
    assert!(
        parked_at.elapsed() < COOLDOWN,
        "connection setup outlasted the test cooldown: {:?}",
        parked_at.elapsed(),
    );
    assert_eq!(
        block_sync.peer_snapshot().outbound_peers,
        0,
        "a parked peer must not be given a block-sync stream while its cooldown runs",
    );

    // The connection stays healthy after the cooldown.
    // The transport must recheck demand to restore the block-sync peer.
    await_until(
        "block sync opens a stream to the peer whose park expired",
        Duration::from_secs(30),
        || block_sync.peer_snapshot().outbound_peers == 1,
    )
    .await?;

    dialer.shutdown().await;
    listener.shutdown().await;
    Ok(())
}

#[test]
fn header_capability_mask_tracks_explicit_runtime_enablement() {
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(1),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    );
    handler.supported_capabilities.store(
        ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
        Ordering::Relaxed,
    );

    handler.set_header_sync_enabled(false);
    assert_eq!(
        handler.current_handshake_config().supported_capabilities,
        ZAKURA_CAP_DISCOVERY
    );

    handler.set_header_sync_enabled(true);
    assert_eq!(
        handler.current_handshake_config().supported_capabilities,
        ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC
    );
    assert_eq!(handler.handshake_snapshot().1, 2, "each change bumps once");

    // Repeating the current setting keeps in-flight negotiations current.
    handler.set_header_sync_enabled(true);
    assert_eq!(handler.handshake_snapshot().1, 2);
    handler.set_header_sync_enabled(false);
    handler.set_header_sync_enabled(false);
    assert_eq!(handler.handshake_snapshot().1, 3);
}

#[test]
fn header_capability_epoch_is_applied_once_and_rejects_stale_demand() {
    use zakura_node_services::sync_lifecycle::{
        BlockServiceDemand, HeaderServiceDemand, LifecycleEpoch, SyncServiceDemand,
    };

    let disabled = SyncServiceDemand {
        header: HeaderServiceDemand::Disabled {
            runtime_epoch: LifecycleEpoch::INITIAL,
        },
        block: BlockServiceDemand::ServingAndApplying {
            apply_epoch: LifecycleEpoch::INITIAL,
        },
    };
    let mut epochs = HeaderCapabilityEpochs::from_initial(&disabled);
    let ready = SyncServiceDemand {
        header: HeaderServiceDemand::Enabled {
            capability_epoch: LifecycleEpoch::new(2),
        },
        ..disabled
    };
    assert_eq!(epochs.observe(&ready), Some(LifecycleEpoch::new(2)));
    assert_eq!(epochs.observe(&ready), None);
    let stale = SyncServiceDemand {
        header: HeaderServiceDemand::Enabled {
            capability_epoch: LifecycleEpoch::new(1),
        },
        ..disabled
    };
    assert_eq!(epochs.observe(&stale), None);
}

#[tokio::test]
async fn enabling_header_sync_reconnects_peers_negotiated_without_it() {
    let supervisor = ZakuraSupervisorHandle::new(4);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    );
    handler.set_header_sync_enabled(false);

    let first_token = CancellationToken::new();
    let second_token = CancellationToken::new();
    register_test_peer(&supervisor, test_peer(31), first_token.clone()).await;
    register_test_peer(&supervisor, test_peer(32), second_token.clone()).await;

    assert_eq!(
        enable_header_sync_and_renegotiate(
            &handler,
            &supervisor,
            zakura_node_services::sync_lifecycle::LifecycleEpoch::new(1),
        )
        .await,
        2
    );
    assert_ne!(
        handler.current_handshake_config().supported_capabilities & ZAKURA_CAP_HEADER_SYNC,
        0
    );
    assert!(first_token.is_cancelled());
    assert!(second_token.is_cancelled());
}

/// With no configured `zakura.listen_addr`, the native endpoint must bind
/// loopback-only. Otherwise an unspecified bind (`0.0.0.0:0` / `[::]:0`)
/// exposes the experimental P2P_V2_ALPN handshake/session surface on every
/// interface on an OS-assigned ephemeral port, even though the unset state is
/// documented as dial-out only.
#[tokio::test]
async fn unset_listen_addr_binds_loopback_not_unspecified() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let endpoint = QuicEndpoint::bind(
        NodeSecretKey::generate(),
        &limits.quic_bind_config(native_bind_addrs(None)),
        &limits.quic,
    )
    .expect("loopback bind should succeed");

    let sockets = endpoint.local_addrs();
    assert!(
        !sockets.is_empty(),
        "endpoint should bind at least one socket"
    );
    for socket in &sockets {
        assert!(
            socket.ip().is_loopback(),
            "unset listen_addr must bind loopback only, but bound {socket} \
                 (exposes P2P_V2_ALPN on all interfaces)"
        );
    }

    endpoint.shutdown().await;
}

#[tokio::test]
async fn configured_listen_addr_binds_only_requested_family() -> Result<(), BoxError> {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    for addr in [
        ZAKURA_LOOPBACK_BIND_V4.into(),
        ZAKURA_LOOPBACK_BIND_V6.into(),
    ] {
        let endpoint = QuicEndpoint::bind(
            LocalEndpointFactory::secret_key(90210),
            &limits.quic_bind_config(native_bind_addrs(Some(addr))),
            &limits.quic,
        )?;
        let sockets = endpoint.local_addrs();
        assert_eq!(sockets.len(), 1);
        assert_eq!(sockets[0].ip(), addr.ip());
        endpoint.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn occupied_listen_addr_fails_without_changing_ports() -> Result<(), BoxError> {
    let occupied = std::net::UdpSocket::bind(ZAKURA_LOOPBACK_BIND_V4)?;
    let addr = occupied.local_addr()?;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    assert!(QuicEndpoint::bind(
        LocalEndpointFactory::secret_key(90211),
        &limits.quic_bind_config(native_bind_addrs(Some(addr))),
        &limits.quic,
    )
    .is_err());
    Ok(())
}

#[tokio::test]
async fn wildcard_bind_advertises_interface_addresses_with_the_bound_port() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let endpoint = QuicEndpoint::bind(
        LocalEndpointFactory::secret_key(90212),
        &limits.quic_bind_config(vec!["0.0.0.0:0".parse().expect("valid address")]),
        &limits.quic,
    )
    .expect("wildcard bind should succeed");
    let port = endpoint.local_addrs()[0].port();

    let addrs = local_direct_addrs(&endpoint);
    assert!(
        addrs.contains(&SocketAddr::from((Ipv4Addr::LOCALHOST, port))),
        "loopback is up on every test host, so it must be listed: {addrs:?}"
    );
    assert!(addrs
        .iter()
        .all(|addr| addr.is_ipv4() && !addr.ip().is_unspecified() && addr.port() == port));

    endpoint.shutdown().await;
}

#[test]
fn discovery_uses_external_ip_with_the_native_listen_port() {
    let secret_key = NodeSecretKey::generate();
    let mut config = Config::default();
    config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
    config.external_addr = Some("203.0.113.42:8233".parse().expect("test address parses"));
    config.zakura.bootstrap_peers.clear();

    assert_eq!(
        discovery_direct_addrs(&config, secret_key.public()),
        vec!["203.0.113.42:8234".parse().expect("test address parses")]
    );
}

#[test]
fn a_listener_that_peers_cannot_dial_is_reported() {
    let secret_key = NodeSecretKey::generate();
    let local_node_id = secret_key.public();
    let mut config = Config::default();
    config.zakura.bootstrap_peers.clear();
    let reported = |config: &Config| {
        let (_connected_tx, connected) = watch::channel(Vec::new());
        let discovery = discovery::build_discovery_handle(
            secret_key.clone(),
            None,
            discovery_direct_addrs(config, local_node_id),
            Vec::new(),
            &ZakuraHandshakeConfig::for_network_with_dev_cohort(&config.network, None),
            config.zakura.max_connections,
            0,
            connected,
        )
        .expect("the test self-record builds");
        advertises_no_discovery_address(config, &discovery.current_self_record().body.direct_addrs)
    };

    config.zakura.listen_addr = None;
    config.external_addr = None;
    assert!(
        !reported(&config),
        "a node that does not listen advertises nothing"
    );

    config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
    assert!(
        reported(&config),
        "a wildcard listener advertises no address"
    );

    config.external_addr = Some("93.184.216.34:8233".parse().expect("test address parses"));
    assert!(!reported(&config));

    config.zakura.listen_addr = Some("10.0.0.5:8234".parse().expect("test address parses"));
    assert!(
        !reported(&config),
        "the record drops the private address and keeps the public one"
    );

    config.external_addr = None;
    assert!(reported(&config), "peers must not dial a private address");

    config.zakura.listen_addr = Some("0.0.0.0:0".parse().expect("test address parses"));
    config.external_addr = Some("93.184.216.34:8233".parse().expect("test address parses"));
    assert!(
        reported(&config),
        "peers dial the external IP at the listen port, so port 0 is unusable"
    );
}

#[test]
fn discovery_uses_matching_local_bootstrap_address_and_counts_only_remote_peers() {
    let local_secret_key = NodeSecretKey::generate();
    let remote_secret_key = NodeSecretKey::generate();
    let local_node_id = local_secret_key.public();
    let remote_node_id = remote_secret_key.public();
    let local_entry = format!("{local_node_id}@198.51.100.7:8234");
    let remote_entry = format!("{remote_node_id}@198.51.100.8:8234");
    let mut config = Config::default();
    config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
    config.external_addr = None;
    config.zakura.bootstrap_peers = vec![
        local_entry,
        remote_entry.clone(),
        remote_entry,
        "malformed".to_string(),
    ];

    assert_eq!(
        discovery_direct_addrs(&config, local_node_id),
        vec!["198.51.100.7:8234".parse().expect("test address parses")]
    );
    assert_eq!(
        remote_bootstrap_peer_count(&config.zakura.bootstrap_peers, local_node_id),
        1
    );
}

#[derive(Debug, Clone)]
struct CaptureConnection {
    alpn: &'static [u8],
    connection_tx: mpsc::Sender<Connection>,
    stream_tx: mpsc::Sender<(SendStream, RecvStream)>,
}

impl Acceptor for CaptureConnection {
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![self.alpn.to_vec()]
    }

    fn handle(&self, connection: Connection) -> BoxFuture<'static, ()> {
        let capture = self.clone();
        Box::pin(async move {
            let _ = capture.connection_tx.send(connection.clone()).await;
            for _ in 0..2 {
                let Ok(streams) = connection.accept_bi().await else {
                    break;
                };
                let _ = capture.stream_tx.send(streams).await;
            }
        })
    }
}

#[derive(Debug)]
struct NoopService;

impl Service for NoopService {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn streams(&self) -> &[Stream] {
        &[]
    }

    fn add_peer(&self, _peer: Peer) {}

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

#[derive(Debug)]
struct DeclaredStreamService {
    streams: Vec<Stream>,
}

impl Service for DeclaredStreamService {
    fn name(&self) -> &'static str {
        "declared-stream"
    }

    fn streams(&self) -> &[Stream] {
        &self.streams
    }

    fn add_peer(&self, _peer: Peer) {}

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

#[derive(Debug)]
struct OrderedStreamService {
    stream: Stream,
    sessions: mpsc::UnboundedSender<(FramedRecv, FramedSend)>,
}

impl Service for OrderedStreamService {
    fn name(&self) -> &'static str {
        "ordered-stream"
    }

    fn streams(&self) -> &[Stream] {
        std::slice::from_ref(&self.stream)
    }

    fn add_peer(&self, mut peer: Peer) {
        if let Some(session) = peer.take_stream(self.stream.kind) {
            let _ = self.sessions.send(session);
        }
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

#[derive(Debug)]
struct GenerationGuardedRecordingService {
    streams: Vec<Stream>,
    active: std::sync::Mutex<HashMap<ZakuraPeerId, (ZakuraConnId, CancellationToken)>>,
    disconnected: std::sync::Mutex<Vec<(ZakuraPeerId, ZakuraConnId)>>,
}

impl GenerationGuardedRecordingService {
    fn new(streams: Vec<Stream>) -> Arc<Self> {
        Arc::new(Self {
            streams,
            active: std::sync::Mutex::new(HashMap::new()),
            disconnected: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn active_conn(&self, peer: &ZakuraPeerId) -> Option<ZakuraConnId> {
        self.active
            .lock()
            .expect("recording service active map is never poisoned")
            .get(peer)
            .map(|(conn_id, _token)| *conn_id)
    }

    fn active_token_cancelled(&self, peer: &ZakuraPeerId) -> Option<bool> {
        self.active
            .lock()
            .expect("recording service active map is never poisoned")
            .get(peer)
            .map(|(_conn_id, token)| token.is_cancelled())
    }

    fn disconnected(&self) -> Vec<(ZakuraPeerId, ZakuraConnId)> {
        self.disconnected
            .lock()
            .expect("recording service disconnected list is never poisoned")
            .clone()
    }
}

impl Service for GenerationGuardedRecordingService {
    fn name(&self) -> &'static str {
        "generation-recording"
    }

    fn streams(&self) -> &[Stream] {
        &self.streams
    }

    fn add_peer(&self, peer: Peer) {
        let mut active = self
            .active
            .lock()
            .expect("recording service active map is never poisoned");
        if active
            .get(&peer.id)
            .is_some_and(|(active_conn_id, _token)| *active_conn_id > peer.conn_id)
        {
            peer.service_cancel_token().cancel();
            return;
        }
        if let Some((_old_conn_id, old_token)) =
            active.insert(peer.id.clone(), (peer.conn_id, peer.service_cancel_token()))
        {
            old_token.cancel();
        }
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let removed = {
            let mut active = self
                .active
                .lock()
                .expect("recording service active map is never poisoned");
            if active
                .get(peer)
                .is_some_and(|(active_conn_id, _token)| *active_conn_id == conn_id)
            {
                active.remove(peer)
            } else {
                None
            }
        };

        if let Some((_active_conn_id, token)) = removed {
            token.cancel();
            self.disconnected
                .lock()
                .expect("recording service disconnected list is never poisoned")
                .push((peer.clone(), conn_id));
        }
    }
}

fn test_peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32-byte node id is valid")
}

fn test_conn_id() -> ZakuraConnId {
    static NEXT_TEST_CONN_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_TEST_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

fn registered_conn_id(registration: ZakuraRegistration) -> ZakuraConnId {
    match registration {
        ZakuraRegistration::Registered { conn_id, .. } => conn_id,
        other => panic!("expected peer registration to succeed, got {other:?}"),
    }
}

#[tokio::test]
async fn handoff_wait_returns_exact_generation_and_rejects_duplicates() {
    let supervisor = ZakuraSupervisorHandle::new(1);
    let peer_id = test_peer(6);
    let ZakuraConnectionRegistrationWaitStart::Waiting(registration_wait) = supervisor
        .begin_connection_registration_wait(&peer_id)
        .await
    else {
        panic!("the first handoff must reserve the peer identity");
    };
    assert!(
        matches!(
            supervisor
                .begin_connection_registration_wait(&peer_id)
                .await,
            ZakuraConnectionRegistrationWaitStart::Duplicate,
        ),
        "a concurrent handoff cannot share one connection registration",
    );

    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let registration = supervisor
        .register(
            test_conn_id(),
            peer_id.clone(),
            Some("192.0.2.6".parse().expect("test IP parses")),
            [6; TRANSCRIPT_HASH_BYTES],
            ZakuraPeerHandle::new_for_tests(peer_id.clone(), outbound_tx),
            CancellationToken::new(),
            ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await;
    let conn_id = registered_conn_id(registration);

    assert_eq!(
        registration_wait.wait(Duration::from_secs(1)).await,
        Some(conn_id),
    );
    assert!(
        matches!(
            supervisor
                .begin_connection_registration_wait(&peer_id)
                .await,
            ZakuraConnectionRegistrationWaitStart::Duplicate,
        ),
        "an incumbent connection makes a later legacy handoff a duplicate",
    );
}

#[test]
fn native_duplicate_tie_breaker_converges_for_simultaneous_open() {
    let node_a = LocalEndpointFactory::secret_key(1).public();
    let node_b = LocalEndpointFactory::secret_key(2).public();
    let a_outbound =
        native_connection_transcript_hash(ServicePeerDirection::Outbound, &node_a, &node_b);
    let a_inbound =
        native_connection_transcript_hash(ServicePeerDirection::Inbound, &node_a, &node_b);
    let b_outbound =
        native_connection_transcript_hash(ServicePeerDirection::Outbound, &node_b, &node_a);
    let b_inbound =
        native_connection_transcript_hash(ServicePeerDirection::Inbound, &node_b, &node_a);

    assert_eq!(a_outbound, b_inbound);
    assert_eq!(a_inbound, b_outbound);
    assert_ne!(a_outbound, a_inbound);

    let winning_key = a_outbound.min(a_inbound);
    let losing_key = a_outbound.max(a_inbound);
    let peer = test_peer(7);
    let mut supervisor_a = ZakuraPeerSupervisor::default();
    let mut supervisor_b = ZakuraPeerSupervisor::default();
    assert!(matches!(
        supervisor_a.register_authenticated(peer.clone(), a_outbound),
        AuthenticatedPeerRegistration::Registered
    ));
    let _ = supervisor_a.register_authenticated(peer.clone(), a_inbound);
    assert!(matches!(
        supervisor_b.register_authenticated(peer.clone(), b_inbound),
        AuthenticatedPeerRegistration::Registered
    ));
    let _ = supervisor_b.register_authenticated(peer.clone(), b_outbound);

    assert!(matches!(
        supervisor_a.register_authenticated(peer.clone(), winning_key),
        AuthenticatedPeerRegistration::Duplicate
    ));
    assert!(matches!(
        supervisor_b.register_authenticated(peer.clone(), winning_key),
        AuthenticatedPeerRegistration::Duplicate
    ));
    assert!(matches!(
        supervisor_a.register_authenticated(peer.clone(), losing_key),
        AuthenticatedPeerRegistration::Duplicate
    ));
    assert!(matches!(
        supervisor_b.register_authenticated(peer, losing_key),
        AuthenticatedPeerRegistration::Duplicate
    ));
}

/// A disconnected peer leaves request selection at once, before its
/// connection task deregisters it.
#[tokio::test]
async fn disconnected_peer_leaves_outbound_selection_before_cleanup() {
    let supervisor = ZakuraSupervisorHandle::new(1);
    let peer = test_peer(12);
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    registered_conn_id(
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                Some("203.0.113.12".parse().expect("test ip parses")),
                [0; TRANSCRIPT_HASH_BYTES],
                ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx),
                CancellationToken::new(),
                ZAKURA_CAP_LEGACY_GOSSIP,
            )
            .await,
    );
    assert_eq!(supervisor.outbound_peer_handles().await.len(), 1);

    assert!(supervisor.disconnect_peer(&peer).await);
    assert_eq!(supervisor.registered_ids().await, vec![peer]);
    assert!(supervisor.outbound_peer_handles().await.is_empty());
}

#[tokio::test]
async fn upgraded_winner_registers_second_loser_cleanup_is_generation_guarded() {
    let supervisor = ZakuraSupervisorHandle::new(1);
    let peer = test_peer(11);
    let remote_ip: IpAddr = "203.0.113.11".parse().expect("test ip parses");
    let losing_trace_conn = 100;
    let winning_trace_conn = 1;
    let losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
    let winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
    let losing_token = CancellationToken::new();
    let winning_token = CancellationToken::new();

    async fn register_with_hash(
        supervisor: &ZakuraSupervisorHandle,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        remote_ip: IpAddr,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                conn_id,
                peer.clone(),
                Some(remote_ip),
                transcript_hash,
                outbound_handle,
                token,
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }

    let losing_conn = registered_conn_id(
        register_with_hash(
            &supervisor,
            losing_trace_conn,
            &peer,
            remote_ip,
            losing_hash,
            losing_token.clone(),
        )
        .await,
    );
    let winning_conn = registered_conn_id(
        register_with_hash(
            &supervisor,
            winning_trace_conn,
            &peer,
            remote_ip,
            winning_hash,
            winning_token.clone(),
        )
        .await,
    );
    assert!(
        winning_trace_conn < losing_trace_conn,
        "the test deliberately inverts raw trace-id order",
    );
    assert!(
        winning_conn > losing_conn,
        "service generations follow successful registration order",
    );

    assert!(
        losing_token.is_cancelled(),
        "upgraded winner explicitly cancels the incumbent loser",
    );
    assert!(
        !winning_token.is_cancelled(),
        "the winning replacement remains live",
    );

    supervisor.deregister(&peer, losing_conn).await;

    {
        let state = supervisor.inner.lock().await;
        let entry = state
            .active_by_peer
            .get(&peer)
            .expect("winner remains registered after loser cleanup");
        assert_eq!(entry.conn_id, winning_conn);
        assert_eq!(entry.host, Some(remote_ip));
        assert_eq!(state.ip_slots.registered(remote_ip), 1);
        state.debug_assert_accounting();
    }
    assert_eq!(supervisor.registered_ids().await, vec![peer.clone()]);
    assert_eq!(supervisor.outbound_peer_handles().await.len(), 1);

    supervisor.deregister(&peer, winning_conn).await;
    let state = supervisor.inner.lock().await;
    assert!(state.active_by_peer.is_empty());
    assert_eq!(state.ip_slots.registered_total(), 0);
    state.debug_assert_accounting();
}

#[tokio::test]
async fn upgraded_replacement_from_full_different_ip_is_rejected() {
    let supervisor = ZakuraSupervisorHandle::new(1);
    let peer_a = test_peer(21);
    let peer_b = test_peer(22);
    let ip1: IpAddr = "203.0.113.21".parse().expect("test ip parses");
    let ip2: IpAddr = "203.0.113.22".parse().expect("test ip parses");
    let peer_a_losing_conn = test_conn_id();
    let peer_b_conn = test_conn_id();
    let peer_a_winning_conn = test_conn_id();
    let peer_a_losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
    let peer_a_winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
    let peer_b_hash = [0x40; TRANSCRIPT_HASH_BYTES];
    let peer_a_losing_token = CancellationToken::new();
    let peer_b_token = CancellationToken::new();
    let peer_a_winning_token = CancellationToken::new();

    let peer_a_losing_generation = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            peer_a_losing_conn,
            &peer_a,
            Some(ip1),
            peer_a_losing_hash,
            peer_a_losing_token.clone(),
        )
        .await,
    );
    let peer_b_generation = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            peer_b_conn,
            &peer_b,
            Some(ip2),
            peer_b_hash,
            peer_b_token.clone(),
        )
        .await,
    );

    let registration = register_test_peer_with_hash_and_ip(
        &supervisor,
        peer_a_winning_conn,
        &peer_a,
        Some(ip2),
        peer_a_winning_hash,
        peer_a_winning_token.clone(),
    )
    .await;
    assert!(
        matches!(
            registration,
            ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
        ),
        "an upgraded same-peer replacement from a different full IP bucket must be rejected",
    );
    assert!(
        !peer_a_losing_token.is_cancelled(),
        "the incumbent stays live because the target IP bucket has no room",
    );
    assert!(
        !peer_b_token.is_cancelled(),
        "the unrelated peer that fills the target IP bucket stays live",
    );
    assert!(
        !peer_a_winning_token.is_cancelled(),
        "the rejected replacement was never registered",
    );

    let state = supervisor.inner.lock().await;
    assert_eq!(
        state
            .active_by_peer
            .get(&peer_a)
            .expect("peer A incumbent remains registered")
            .conn_id,
        peer_a_losing_generation
    );
    assert_eq!(
        state
            .active_by_peer
            .get(&peer_b)
            .expect("peer B remains registered")
            .conn_id,
        peer_b_generation
    );
    assert_eq!(state.ip_slots.registered(ip1), 1);
    assert_eq!(state.ip_slots.registered(ip2), 1);
    state.debug_assert_accounting();
}

#[tokio::test]
async fn loser_cleanup_does_not_disconnect_upgraded_winner_service_session() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let peer = test_peer(23);
    let remote_ip: IpAddr = "203.0.113.23".parse().expect("test ip parses");
    let losing_conn = test_conn_id();
    let winning_conn = test_conn_id();
    let losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
    let winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
    let losing_token = CancellationToken::new();
    let winning_token = CancellationToken::new();

    let losing_conn = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            losing_conn,
            &peer,
            Some(remote_ip),
            losing_hash,
            losing_token.clone(),
        )
        .await,
    );
    let winning_conn = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            winning_conn,
            &peer,
            Some(remote_ip),
            winning_hash,
            winning_token.clone(),
        )
        .await,
    );
    assert!(
        winning_conn > losing_conn,
        "winner must have a later service generation",
    );

    let stream = Stream {
        kind: 61,
        version: 1,
        frame_cap: 1024,
        capability: ZAKURA_CAP_LEGACY_GOSSIP,
        ..Stream::PERSISTENT
    };
    let service = GenerationGuardedRecordingService::new(vec![stream]);
    let registry = Arc::new(
        ServiceRegistry::new(vec![service.clone()]).expect("test service declares a valid stream"),
    );
    let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(1);
    let (outbound_tx, _outbound_rx) = crate::zakura::framed_channel(1);
    let streams = HashMap::from([(stream.kind, (inbound_rx, outbound_tx))]);
    let winning_service_token = CancellationToken::new();

    registry.add_peer(Peer::new_with_conn_id_and_direction(
        winning_conn,
        peer.clone(),
        Some(remote_ip),
        ZAKURA_CAP_LEGACY_GOSSIP,
        ServicePeerDirection::Inbound,
        streams,
        winning_service_token.clone(),
    ));
    assert_eq!(service.active_conn(&peer), Some(winning_conn));

    let (_stale_inbound_tx, stale_inbound_rx) = crate::zakura::framed_channel(1);
    let (stale_outbound_tx, _stale_outbound_rx) = crate::zakura::framed_channel(1);
    let stale_streams = HashMap::from([(stream.kind, (stale_inbound_rx, stale_outbound_tx))]);
    let stale_service_token = CancellationToken::new();
    registry.add_peer(Peer::new_with_conn_id_and_direction(
        losing_conn,
        peer.clone(),
        Some(remote_ip),
        ZAKURA_CAP_LEGACY_GOSSIP,
        ServicePeerDirection::Inbound,
        stale_streams,
        stale_service_token.clone(),
    ));
    assert_eq!(
        service.active_conn(&peer),
        Some(winning_conn),
        "stale loser add must not overwrite the winner's service session",
    );
    assert!(
        matches!(service.active_token_cancelled(&peer), Some(false)),
        "stale loser add must not cancel the winner's service token",
    );

    registry.remove_peer(&peer, losing_conn, ZAKURA_CAP_LEGACY_GOSSIP);
    supervisor.deregister(&peer, losing_conn).await;

    assert_eq!(
        service.active_conn(&peer),
        Some(winning_conn),
        "stale loser cleanup must not remove the winner's service session",
    );
    assert!(
        service.disconnected().is_empty(),
        "no service disconnect should be emitted for the winner generation",
    );
    assert!(
        matches!(service.active_token_cancelled(&peer), Some(false)),
        "the winner's service session token must remain live",
    );

    {
        let state = supervisor.inner.lock().await;
        let entry = state
            .active_by_peer
            .get(&peer)
            .expect("winner remains registered after loser cleanup");
        assert_eq!(entry.conn_id, winning_conn);
        assert_eq!(state.ip_slots.registered(remote_ip), 1);
        state.debug_assert_accounting();
    }

    registry.remove_peer(&peer, winning_conn, ZAKURA_CAP_LEGACY_GOSSIP);
    supervisor.deregister(&peer, winning_conn).await;
}

#[tokio::test]
async fn registered_peer_cleanup_guard_drop_deregisters_and_allows_redial() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let peer = test_peer(24);
    let transcript_hash = [0x24; TRANSCRIPT_HASH_BYTES];
    let disconnect_token = CancellationToken::new();
    let conn_id = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            None,
            transcript_hash,
            disconnect_token.clone(),
        )
        .await,
    );

    let duplicate = register_test_peer_with_hash_and_ip(
        &supervisor,
        test_conn_id(),
        &peer,
        None,
        transcript_hash,
        CancellationToken::new(),
    )
    .await;
    assert!(
        matches!(duplicate, ZakuraRegistration::Duplicate { .. }),
        "an exact same-direction redial is a duplicate while the incumbent is registered",
    );

    let stream = Stream {
        kind: 62,
        version: 1,
        frame_cap: 1024,
        capability: ZAKURA_CAP_LEGACY_GOSSIP,
        ..Stream::PERSISTENT
    };
    let service = GenerationGuardedRecordingService::new(vec![stream]);
    let registry = Arc::new(
        ServiceRegistry::new(vec![service.clone()]).expect("test service declares a valid stream"),
    );
    let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(1);
    let (outbound_tx, _outbound_rx) = crate::zakura::framed_channel(1);
    registry.add_peer(Peer::new_with_conn_id_and_direction(
        conn_id,
        peer.clone(),
        None,
        ZAKURA_CAP_LEGACY_GOSSIP,
        ServicePeerDirection::Inbound,
        HashMap::from([(stream.kind, (inbound_rx, outbound_tx))]),
        disconnect_token.clone(),
    ));
    assert_eq!(service.active_conn(&peer), Some(conn_id));

    let mut cleanup_guard = RegisteredPeerCleanupGuard::new(
        supervisor.clone(),
        registry,
        peer.clone(),
        conn_id,
        disconnect_token.clone(),
    );
    cleanup_guard.add_admitted_capabilities(ZAKURA_CAP_LEGACY_GOSSIP);
    drop(cleanup_guard);

    assert!(
        disconnect_token.is_cancelled(),
        "drop cleanup cancels the registered connection token",
    );
    assert_eq!(
        service.active_conn(&peer),
        None,
        "drop cleanup removes admitted service state synchronously",
    );
    assert_eq!(service.disconnected(), vec![(peer.clone(), conn_id)]);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if supervisor.registered_ids().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop cleanup deregisters the supervisor entry promptly");

    let redial = register_test_peer_with_hash_and_ip(
        &supervisor,
        test_conn_id(),
        &peer,
        None,
        transcript_hash,
        CancellationToken::new(),
    )
    .await;
    assert!(
        matches!(redial, ZakuraRegistration::Registered { .. }),
        "after drop cleanup, the deterministic same-direction redial can register again",
    );
}

#[tokio::test]
async fn registration_generation_exhaustion_rejects_without_mutating_state() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let peer = test_peer(25);

    {
        let mut state = supervisor.inner.lock().await;
        state.next_registration_id = u64::MAX;
    }

    let registration = register_test_peer_with_hash_and_ip(
        &supervisor,
        test_conn_id(),
        &peer,
        None,
        [0x25; TRANSCRIPT_HASH_BYTES],
        CancellationToken::new(),
    )
    .await;

    assert!(
        matches!(
            registration,
            ZakuraRegistration::Rejected(ZakuraRejectReason::TemporaryUnavailable)
        ),
        "exhausted registration generations reject the new connection instead of panicking",
    );
    assert!(
        supervisor.registered_ids().await.is_empty(),
        "failed registration must not mutate active supervisor state",
    );
}

/// The `[network.zakura.quic]` defaults must keep the transport values
/// that regulation sizing and the stream-conformance properties assume.
#[test]
fn quic_config_defaults_match_the_zakura_transport_constants() {
    let quic = QuicConfig::default();
    assert_eq!(
        quic.stream_receive_window_bytes,
        DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW
    );
    assert_eq!(quic.receive_window_bytes, DEFAULT_ZAKURA_RECEIVE_WINDOW);
    assert_eq!(quic.send_window_bytes, DEFAULT_ZAKURA_SEND_WINDOW);
    assert_eq!(quic.idle_timeout(), DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT);
    assert_eq!(
        quic.keep_alive_interval(),
        DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL
    );
}

#[test]
fn ordered_stream_collision_winner_is_mirror_stable() {
    // Both endpoints compute the collision winner from the same node IDs.
    // Their answers must be complementary.
    // One endpoint keeps its opened stream.
    // The other endpoint adopts the peer's stream.
    let node_a = LocalEndpointFactory::secret_key(1).public();
    let node_b = LocalEndpointFactory::secret_key(2).public();
    assert_ne!(node_a, node_b);
    assert_ne!(
        i_open_collision_winner(&node_a, &node_b),
        i_open_collision_winner(&node_b, &node_a),
        "exactly one side must win a same-kind ordered-stream collision",
    );
    // The winner is deterministic: the lexicographically smaller node id.
    let a_wins = node_a.as_bytes() < node_b.as_bytes();
    assert_eq!(i_open_collision_winner(&node_a, &node_b), a_wins);
}

fn header_sync_test_session(peer: ZakuraPeerId) -> (PeerSession, crate::zakura::FramedRecv) {
    let (send, recv) = crate::zakura::framed_channel(32);
    (
        PeerSession::from_parts(peer, send, CancellationToken::new()),
        recv,
    )
}

fn header_sync_startup(shutdown: CancellationToken) -> HeaderSyncStartup {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
        crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
    );
    startup.shutdown = shutdown;
    startup
}

async fn register_test_peer(
    supervisor: &ZakuraSupervisorHandle,
    peer: ZakuraPeerId,
    disconnect_token: CancellationToken,
) {
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
    let registration = supervisor
        .register(
            test_conn_id(),
            peer.clone(),
            None,
            [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
            outbound_handle,
            disconnect_token,
            ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
        )
        .await;

    assert!(
        matches!(registration, ZakuraRegistration::Registered { .. }),
        "test peer should register once"
    );
}

async fn register_test_peer_with_hash_and_ip(
    supervisor: &ZakuraSupervisorHandle,
    conn_id: ZakuraConnId,
    peer: &ZakuraPeerId,
    remote_ip: Option<IpAddr>,
    transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
    disconnect_token: CancellationToken,
) -> ZakuraRegistration {
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
    supervisor
        .register(
            conn_id,
            peer.clone(),
            remote_ip,
            transcript_hash,
            outbound_handle,
            disconnect_token,
            ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
        )
        .await
}

#[test]
fn local_limits_clamp_negotiated_values_down() {
    let config = Config::default();
    let limits = ZakuraLocalLimits::from_config(&config);
    let negotiated = ZakuraAcceptedLimits {
        max_frame_bytes: u32::MAX,
        max_message_bytes: u32::MAX,
        max_open_streams: u16::MAX,
        max_inbound_queue_depth: u16::MAX,
        idle_timeout_millis: u32::MAX,
    };

    let clamped = limits.clamp(&negotiated);

    assert_eq!(clamped.max_frame_bytes, limits.max_frame_bytes);
    assert_eq!(clamped.max_message_bytes, limits.max_message_bytes);
    assert_eq!(clamped.max_open_streams, limits.max_open_streams);
    assert_eq!(
        clamped.max_inbound_queue_depth,
        limits.max_inbound_queue_depth
    );
    assert!(clamped.idle_timeout < limits.quic_idle_timeout);
}

#[test]
fn the_largest_valid_cwnd_cap_derives_a_bindable_stream_window() {
    let mut config = Config::default();
    let fixed = super::super::BLOCK_SYNC_REACTOR_BUDGET.receive
        - config.zakura.block_sync.bbr_max_cwnd_bytes;
    config.zakura.block_sync.bbr_max_cwnd_bytes =
        zakura_quic::MAX_STREAM_RECEIVE_WINDOW_BYTES - fixed;
    assert_eq!(config.zakura.block_sync.validate(), Ok(()));

    let limits = ZakuraLocalLimits::from_config(&config);

    assert_eq!(
        u64::from(limits.quic.stream_receive_window_bytes),
        zakura_quic::MAX_STREAM_RECEIVE_WINDOW_BYTES
    );
    assert_eq!(limits.quic.validate(), Ok(()));
}

#[test]
fn inbound_queue_depth_is_split_across_ordered_streams() {
    assert_eq!(per_stream_inbound_queue_depth(64, 2), 32);
    assert_eq!(per_stream_inbound_queue_depth(63, 2), 31);
    assert_eq!(per_stream_inbound_queue_depth(64, 0), 64);
    assert!(per_stream_inbound_queue_depth(63, 2) * 2 <= 63);
}

#[test]
fn request_response_only_peers_do_not_use_ordered_stream_freshness_reaper() {
    assert!(!should_run_freshness_reaper(0, 1));
    assert!(should_run_freshness_reaper(1, 1));
    assert!(should_run_freshness_reaper(1, 0));
    assert!(should_run_freshness_reaper(0, 0));
}

/// Teardown's drain has one deadline for each wait. Workers that ignore
/// cancellation and end one after another, each before a fresh
/// per-worker timeout would fire, can't hold the connection's admission
/// permit past twice the drain timeout, however many there are.
#[tokio::test(start_paused = true)]
async fn trickling_workers_cannot_extend_the_teardown_drain() {
    let step = STREAM_WORKER_DRAIN_TIMEOUT * 9 / 10;
    for count in [4, 64] {
        let admission = Arc::new(Semaphore::new(1));
        let permit = admission
            .clone()
            .try_acquire_owned()
            .expect("the test takes the only permit");
        let mut workers = JoinSet::new();
        for worker in 1..=count {
            workers.spawn(zakura_rt::time::sleep(step * worker));
        }
        let cancelled = Instant::now();
        let teardown = tokio::spawn(async move {
            let drained = drain_stream_workers(&mut workers).await;
            drop(permit);
            (drained, workers.len())
        });

        let _returned = admission.acquire().await.expect("the test never closes it");
        let held = cancelled.elapsed();
        assert!(
            held <= 2 * STREAM_WORKER_DRAIN_TIMEOUT,
            "{count} trickling workers held the admission permit for {held:?}"
        );
        let (drained, left) = teardown.await.expect("the teardown task never panics");
        assert!(!drained, "the later workers outlast the first wait");
        assert_eq!(left, 0, "the abort ends every sleeping worker");
    }
}

#[tokio::test]
async fn v2_p2p_false_leaves_header_sync_disabled() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Legacy);

    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?;

    assert!(endpoint.is_none());
    Ok(())
}

#[tokio::test]
async fn an_unfundable_block_sync_config_fails_before_the_endpoint_binds() {
    let _guard = zakura_test::init();
    let mut config = Config::for_test(P2pStack::Dual);
    config.zakura.block_sync.peer_limits.max_inbound_peers = 3_000;
    config.zakura.block_sync.peer_limits.max_outbound_peers = 634;

    let error = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await
    .expect_err("an unfundable block-sync config must not start the endpoint");
    assert!(
        error
            .to_string()
            .contains("invalid zakura.block_sync config"),
        "unexpected error: {error}",
    );
}

/// A `Config` built in code skips config-load validation. The node's
/// limits replace its stream window with the largest reactor share, so a
/// window below a block-sync frame still starts the endpoint, and no
/// native frame exceeds its share.
#[tokio::test]
async fn a_programmatic_stream_window_takes_the_reactor_shares() {
    let _guard = zakura_test::init();
    let mut config = Config::for_test(P2pStack::Dual);
    config.zakura.quic.stream_receive_window_bytes = 64 * 1024;

    let limits = ZakuraLocalLimits::from_config(&config);
    assert_eq!(
        u64::from(limits.quic.stream_receive_window_bytes),
        limits.reactor_shares().stream_receive_window()
    );
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await
    .expect("the reactor shares fund every native frame");
    drop(endpoint);
}

/// Startup refuses a bootstrap identity whose dial would need more slots
/// than the dial reserve (8 at the defaults), counting only the
/// addresses the bound endpoint would dial.
#[tokio::test]
async fn startup_refuses_a_bootstrap_dial_wider_than_the_reserve() {
    let _guard = zakura_test::init();
    let remote = NodeSecretKey::generate().public();
    let config_with = |addrs: &[String]| {
        let mut config = Config::for_test(P2pStack::Zakura);
        config.zakura.bootstrap_peers = addrs
            .iter()
            .map(|addr| format!("{remote}@{addr}"))
            .collect();
        config
    };
    let v4 = |port: u16| format!("127.0.0.1:{port}");
    let mapped = |port: u16| format!("[::ffff:127.0.0.1]:{port}");
    let spawn = |config: Config| async move {
        spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await
    };

    let nine = (8233..8242).map(v4).collect::<Vec<_>>();
    let error = spawn(config_with(&nine))
        .await
        .expect_err("a nine-address dial can't fit the eight-slot reserve");
    assert!(
        error.to_string().contains("zakura.bootstrap_peers"),
        "unexpected error: {error}",
    );

    // Sixteen configured addresses, but the mapped ones fold into their
    // IPv4 forms, so the dial makes eight attempts.
    let folded = (8233..8241)
        .flat_map(|port| [v4(port), mapped(port)])
        .collect::<Vec<_>>();
    let endpoint = spawn(config_with(&folded))
        .await
        .expect("eight attempts fit the reserve");
    drop(endpoint);
}

/// Config load checks only native kinds, so startup must refuse a custom
/// kind whose frame exceeds its share.
#[tokio::test]
async fn a_custom_frame_above_its_share_fails_startup_and_names_the_kind() {
    const CUSTOM_STREAM: Stream = Stream {
        kind: 64,
        version: 1,
        frame_cap: LOCAL_MAX_MESSAGE_BYTES,
        capability: 1 << 16,
        ..Stream::PERSISTENT
    };

    let _guard = zakura_test::init();
    let mut config = Config::for_test(P2pStack::Dual);
    config.zakura.quic.stream_receive_window_bytes = MAX_BS_FRAME_BYTES;
    config
        .zakura
        .validate_stream_receive_window()
        .expect("the window holds every native frame");
    let (sessions, _session_rx) = mpsc::unbounded_channel();

    let error = spawn_zakura_endpoint_with_services(
        &config,
        |_supervisor, _trace| Arc::new(NoopService),
        None,
        vec![CustomService {
            service: Arc::new(OrderedStreamService {
                stream: CUSTOM_STREAM,
                sessions,
            }),
            provides: Vec::new(),
            seeks: Vec::new(),
            tx_relay_sessions: None,
        }],
    )
    .await
    .expect_err("a custom frame above its share must not start the endpoint");
    assert!(
        error.to_string().contains("stream kind 64's receive share"),
        "unexpected error: {error}",
    );
}

#[tokio::test]
async fn v2_p2p_true_starts_header_sync_handle() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);

    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");

    assert!(endpoint.header_sync().is_some());
    endpoint.shutdown().await;
    Ok(())
}

/// A full transport stops discovery dials even while admission permits
/// remain, so the dialer can't spin on `ConnectionLimit` (zakura-quic
/// ADM-12).
#[tokio::test]
async fn a_full_transport_reports_no_dial_capacity() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut config = Config::for_test(P2pStack::Dual);
    // No bootstrap dials, so nothing else takes the admission permit.
    config.zakura.bootstrap_peers = Vec::new();
    config.zakura.max_connections = 2;
    config.zakura.max_pending_handshakes = 1;
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");
    assert!(endpoint.discovery_admission_capacity() > 0);

    // Three dials to a socket that never answers hold every live slot.
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let target = NodeAddr::with_addrs(
        zakura_quic::NodeSecretKey::generate().public(),
        vec![silent.local_addr()?],
    );
    let mut dials = Vec::new();
    for _ in 0..3 {
        let (quic, target) = (endpoint.quic.clone(), target.clone());
        dials.push(tokio::spawn(async move {
            quic.connect(target, &[P2P_V2_ALPN]).await
        }));
    }
    while endpoint.quic.connection_count() < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(endpoint.handler.admission.available_permits() > 0);
    assert_eq!(endpoint.discovery_admission_capacity(), 0);
    for dial in dials {
        dial.abort();
    }
    endpoint.shutdown().await;
    Ok(())
}

/// Only limits a dial hits before sending a packet are local: the
/// pending-handshake limit follows a completed QUIC handshake with the
/// peer, so redials must count it.
#[test]
fn only_limits_before_the_first_packet_are_pre_dial() {
    assert!(ZakuraHandlerError::ResourceLimit(ADMISSION_LIMIT).is_pre_dial_limit());
    assert!(ZakuraHandlerError::ResourceLimit(TRANSPORT_CONNECTIONS_LIMIT).is_pre_dial_limit());
    assert!(!ZakuraHandlerError::ResourceLimit("pending handshake").is_pre_dial_limit());
    assert!(!ZakuraHandlerError::Timeout("native dial").is_pre_dial_limit());
}

#[tokio::test]
async fn endpoint_shutdown_stops_header_sync_task() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");
    let header_sync = endpoint
        .header_sync()
        .expect("header sync handle exists for a v2 endpoint");

    endpoint.shutdown().await;

    let (session, _recv) = header_sync_test_session(
        ZakuraPeerId::new(vec![5u8; 32]).expect("32-byte node id is valid"),
    );
    let send_result = tokio::time::timeout(
        Duration::from_secs(1),
        header_sync.send(Event::PeerConnected(session)),
    )
    .await
    .expect("send returns promptly after header-sync shutdown");
    assert!(send_result.is_err());

    Ok(())
}

/// A maintained native dial loop (shared by configured bootstrap peers, the
/// legacy->Zakura upgrade hand-off, and `spawn_native_dial`) targets an
/// unreachable peer, so its `maintain` policy retries forever and never
/// exits on its own. `ZakuraEndpoint::shutdown` must still stop it: the task
/// holds an endpoint clone, so the supervisor registration watch stays open
/// and only the endpoint shutdown token can end the loop. Without that
/// signal the detached loop outlives shutdown and keeps dialing.
#[tokio::test]
async fn endpoint_shutdown_stops_maintained_native_dial_loop() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");

    // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): guaranteed unreachable, so the
    // maintained loop stays in connect/backoff and never finishes on its own.
    let unreachable_addr: SocketAddr = "192.0.2.1:65535".parse().expect("valid test address");
    let unreachable = NodeAddr::with_addrs(
        LocalEndpointFactory::secret_key(987_654).public(),
        [unreachable_addr],
    );
    let dial = endpoint.spawn_native_dial(unreachable);

    // Let the maintained loop start before tearing the endpoint down.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !dial.is_finished(),
        "maintained dial loop to an unreachable peer should still be running"
    );

    endpoint.shutdown().await;

    tokio::time::timeout(Duration::from_secs(5), dial)
        .await
        .expect("maintained native dial loop must exit after endpoint shutdown")
        .expect("native dial task must not panic");
    Ok(())
}

/// The discovery candidate dialer is a long-lived loop that holds an
/// endpoint clone, so its supervisor registration watch never closes on its
/// own. `ZakuraEndpoint::shutdown` must still stop it via the endpoint
/// shutdown token; otherwise it outlives teardown, polling the discovery
/// book and attempting dials against a torn-down router.
#[tokio::test]
async fn endpoint_shutdown_stops_discovery_candidate_dialer() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");

    let limits = ZakuraLocalLimits::from_config(&config);
    let handshake = ZakuraHandshakeConfig::for_network(&config.network);
    let discovery = crate::zakura::discovery::build_discovery_handle(
        NodeSecretKey::generate(),
        None,
        Vec::new(),
        crate::zakura::discovery::default_advertised_services(),
        &handshake,
        limits.max_connections,
        0,
        endpoint.supervisor().subscribe(),
    )?;
    let dialer = crate::zakura::discovery::spawn_native_discovery_dialer(
        endpoint.clone(),
        discovery,
        limits,
        Vec::new(),
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !dialer.is_finished(),
        "discovery candidate dialer should still be running"
    );

    endpoint.shutdown().await;

    tokio::time::timeout(Duration::from_secs(5), dialer)
        .await
        .expect("discovery candidate dialer must exit after endpoint shutdown")
        .expect("discovery dialer task must not panic");
    Ok(())
}

/// A malicious legacy responder can return a syntactically valid
/// `P2pV2UpgradeAccept` (a unique, real iroh node id with a parseable but
/// unreachable direct address) that never completes native Zakura
/// registration. After the upgrade hand-off wait times out, no
/// maintain-forever native dial task or `upgrade_dials` entry may survive
/// its in-flight attempt;
/// otherwise repeating the failed upgrade with distinct node ids grows
/// maintained dials and outbound QUIC traffic without bound.
///
/// Regression test for `claude-legacy-upgrade-maintained-dial-leak`. Uses a
/// paused clock so the 15s appear-timeout elapses without real waiting; the
/// dial target is RFC 5737 TEST-NET-1, which never connects.
#[tokio::test(start_paused = true)]
async fn failed_legacy_upgrade_does_not_leak_maintained_dial() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");

    // The Accept the attacker would advertise: a real 32-byte node id
    // (so `node_addr_from_hints` builds a `NodeAddr` and the dial spawns)
    // pointing at an unreachable address that never registers.
    let node_id = LocalEndpointFactory::secret_key(0x0BAD_C0DE)
        .public()
        .as_bytes()
        .to_vec();
    let peer_id = ZakuraPeerId::new(node_id.clone()).expect("32-byte node id is valid");
    let direct_addresses = vec![b"192.0.2.1:1".to_vec()];

    let connector = crate::zakura::ZakuraHandshakeConnector::new_with_endpoint(endpoint.clone());
    let handoff = connector
        .spawn_zakura_dial_to_hints_and_wait(&peer_id, &node_id, &direct_addresses)
        .await;

    assert!(
        matches!(handoff, crate::zakura::ZakuraNativeHandoff::Failed),
        "an unreachable upgrade peer must not report a completed hand-off",
    );
    // The cancelled dial starts no new attempt, so its in-flight one is
    // the last.
    upgrade_dials_end(&endpoint).await;

    endpoint.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn maintained_upgrade_dial_keeps_its_original_handoff_owner() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = Config::for_test(P2pStack::Dual);
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");
    let node_id = LocalEndpointFactory::secret_key(0x0BAD_CAFE).public();
    let peer_id = ZakuraPeerId::new(node_id.as_bytes().to_vec())?;
    let node_addr = NodeAddr::with_addrs(
        node_id,
        ["192.0.2.2:1".parse().expect("test direct address parses")],
    );

    assert_eq!(
        endpoint.start_upgrade_native_dial(node_addr.clone()),
        crate::zakura::ZakuraUpgradeDialStart::Started,
    );
    assert_eq!(
        endpoint.start_upgrade_native_dial(node_addr),
        crate::zakura::ZakuraUpgradeDialStart::AlreadyRunning,
    );

    endpoint.cancel_upgrade_native_dial(&peer_id);
    endpoint.shutdown().await;
    Ok(())
}

/// Spawns a Zakura endpoint, and a loopback QUIC peer that serves an ALPN
/// no Zakura node offers. Returns both and the peer's hint.
async fn endpoint_and_other_cohort(
    config: Config,
    seed: u64,
) -> Result<(ZakuraEndpoint, QuicEndpoint, NodeAddr), BoxError> {
    let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(NoopService) as Arc<dyn Service>
    })
    .await?
    .expect("v2_p2p is enabled in test config");
    let other_cohort = LocalEndpointFactory::new().endpoint(seed).await?;
    let (connection_tx, _connection_rx) = mpsc::channel(1);
    let (stream_tx, _stream_rx) = mpsc::channel(1);
    other_cohort.serve(CaptureConnection {
        alpn: b"/zakura/another-cohort/0",
        connection_tx,
        stream_tx,
    })?;
    let hint = LocalEndpointFactory::node_addr(&other_cohort).await;
    Ok((endpoint, other_cohort, hint))
}

/// Waits until `endpoint` owns no upgrade dial. A cancelled dial ends
/// within the bound of its in-flight attempt.
async fn upgrade_dials_end(endpoint: &ZakuraEndpoint) {
    timeout(Duration::from_secs(30), async {
        while endpoint.upgrade_dial_count() > 0 {
            zakura_rt::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the upgrade dial ends within one bounded attempt");
}

/// An upgrade dial to a peer-supplied address that speaks another ALPN
/// ends at once, and no upgrade dial to the same hint starts again until
/// the ten-minute floor ends (zakura-quic DIAL-5, V12 F-562, F-563). The
/// node ID alone is unauthenticated, so the same ID at other addresses
/// still dials.
#[tokio::test]
async fn alpn_mismatched_upgrade_dial_ends_and_holds_the_hint_floor() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let (endpoint, _other_cohort, hint) =
        endpoint_and_other_cohort(Config::for_test(P2pStack::Dual), 0x0A17_0001).await?;
    let peer_id = ZakuraPeerId::new(hint.id.as_bytes().to_vec())?;

    assert_eq!(
        endpoint.start_upgrade_native_dial(hint.clone()),
        crate::zakura::ZakuraUpgradeDialStart::Started,
    );
    upgrade_dials_end(&endpoint).await;

    tokio::time::pause();
    let reordered = NodeAddr::with_addrs(hint.id, hint.direct.iter().rev().copied());
    assert_eq!(
        endpoint.start_upgrade_native_dial(reordered),
        crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
    );
    tokio::time::advance(ZAKURA_ALPN_MISMATCH_BACKOFF - Duration::from_secs(1)).await;
    assert_eq!(
        endpoint.start_upgrade_native_dial(hint.clone()),
        crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
    );
    let planted = NodeAddr::with_addrs(
        hint.id,
        ["192.0.2.3:1".parse().expect("test direct address parses")],
    );
    assert_eq!(
        endpoint.start_upgrade_native_dial(planted),
        crate::zakura::ZakuraUpgradeDialStart::Started,
        "a floor from one hint blocked the same node ID at other addresses",
    );
    endpoint.cancel_upgrade_native_dial(&peer_id);
    upgrade_dials_end(&endpoint).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        endpoint.start_upgrade_native_dial(hint),
        crate::zakura::ZakuraUpgradeDialStart::Started,
    );

    endpoint.cancel_upgrade_native_dial(&peer_id);
    endpoint.shutdown().await;
    Ok(())
}

/// A hand-off cancelled while a silent address delays the dial still
/// starts the floor when another address of the hint mismatches.
#[tokio::test]
async fn cancelled_upgrade_dial_still_holds_a_delayed_alpn_floor() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut config = Config::for_test(P2pStack::Dual);
    config.zakura.quic.handshake_timeout_secs = Some(2);
    let (endpoint, _other_cohort, hint) = endpoint_and_other_cohort(config, 0x0A17_0002).await?;
    let peer_id = ZakuraPeerId::new(hint.id.as_bytes().to_vec())?;
    // A bound socket that never answers holds its attempt to the
    // handshake deadline.
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let hint = NodeAddr::with_addrs(
        hint.id,
        std::iter::once(silent.local_addr()?).chain(hint.direct.iter().copied()),
    );

    assert_eq!(
        endpoint.start_upgrade_native_dial(hint.clone()),
        crate::zakura::ZakuraUpgradeDialStart::Started,
    );
    // The legacy handshake timeout drops the hand-off before the silent
    // attempt ends.
    zakura_rt::time::sleep(Duration::from_millis(500)).await;
    endpoint.cancel_upgrade_native_dial(&peer_id);
    upgrade_dials_end(&endpoint).await;

    assert_eq!(
        endpoint.start_upgrade_native_dial(hint),
        crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
        "cancelling the hand-off discarded the mismatch",
    );
    endpoint.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn custom_ordered_service_round_trips_over_zakura() -> Result<(), BoxError> {
    const CUSTOM_STREAM: Stream = Stream {
        kind: 64,
        version: 1,
        frame_cap: 64 * 1024,
        capability: 1 << 16,
        ..Stream::PERSISTENT
    };

    let _guard = zakura_test::init();
    let service_id = ZakuraServiceId::new("zakura.test.ordered.v1")?;
    let listen_addr = "127.0.0.1:0".parse().expect("valid loopback address");
    let server_identity = tempfile::tempdir()?;
    let client_identity = tempfile::tempdir()?;
    let (server_sessions, mut server_session_rx) = mpsc::unbounded_channel();
    let (client_sessions, mut client_session_rx) = mpsc::unbounded_channel();

    let mut server_config = Config::for_test(P2pStack::Dual);
    server_config.identity_dir = server_identity.path().to_owned();
    server_config.zakura.listen_addr = Some(listen_addr);
    server_config.zakura.bootstrap_peers.clear();
    let server = spawn_zakura_endpoint_with_services(
        &server_config,
        |_supervisor, _trace| Arc::new(NoopService),
        None,
        vec![CustomService {
            service: Arc::new(OrderedStreamService {
                stream: CUSTOM_STREAM,
                sessions: server_sessions,
            }),
            provides: vec![service_id.clone()],
            seeks: Vec::new(),
            tx_relay_sessions: None,
        }],
    )
    .await?
    .expect("test server uses Zakura");
    let server_addr = server.node_addr().await;
    let server_direct = server_addr
        .direct
        .iter()
        .copied()
        .find(|addr| addr.ip().is_loopback())
        .ok_or("test server has no loopback address")?;

    let mut client_config = Config::for_test(P2pStack::Dual);
    client_config.identity_dir = client_identity.path().to_owned();
    client_config.zakura.listen_addr = Some(listen_addr);
    client_config.zakura.bootstrap_peers = vec![format!("{}@{server_direct}", server_addr.id)];
    let client = spawn_zakura_endpoint_with_services(
        &client_config,
        |_supervisor, _trace| Arc::new(NoopService),
        None,
        vec![CustomService {
            service: Arc::new(OrderedStreamService {
                stream: CUSTOM_STREAM,
                sessions: client_sessions,
            }),
            provides: Vec::new(),
            seeks: vec![service_id],
            tx_relay_sessions: None,
        }],
    )
    .await?
    .expect("test client uses Zakura");

    let round_trip = timeout(Duration::from_secs(10), async {
        let (mut server_recv, server_send) = server_session_rx
            .recv()
            .await
            .ok_or_else(|| -> BoxError { "server custom stream did not open".into() })?;
        let (mut client_recv, client_send) = client_session_rx
            .recv()
            .await
            .ok_or_else(|| -> BoxError { "client custom stream did not open".into() })?;
        let outbound = Frame {
            message_type: 1_001,
            flags: 3,
            payload: b"custom client frame".to_vec(),
        };
        client_send.send(outbound.clone()).await?;
        let received = server_recv
            .recv()
            .await
            .ok_or_else(|| -> BoxError { "server custom stream closed".into() })?;
        assert_eq!(received, outbound);

        let response = Frame {
            message_type: 2_002,
            flags: 5,
            payload: b"custom server frame".to_vec(),
        };
        server_send.send(response.clone()).await?;
        let received = client_recv
            .recv()
            .await
            .ok_or_else(|| -> BoxError { "client custom stream closed".into() })?;
        assert_eq!(received, response);
        Ok::<_, BoxError>(())
    })
    .await
    .map_err(|_| -> BoxError { "custom ordered stream round trip timed out".into() })?;
    client.shutdown().await;
    server.shutdown().await;
    round_trip
}

#[tokio::test]
async fn lazy_escalation_service_demand_respects_directional_caps() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let shutdown = CancellationToken::new();
    let mut startup = header_sync_startup(shutdown.clone());
    startup.config.peer_limits = ServicePeerLimits {
        max_inbound_peers: 0,
        max_outbound_peers: 1,
        ..ServicePeerLimits::default()
    };
    let (header_sync, _actions, reactor_task) = spawn_header_sync_reactor(startup)?;
    let service = HeaderSyncService::new(header_sync);
    let peer = test_peer(17);

    assert!(
        !service.wants_peer(&peer, ZAKURA_CAP_HEADER_SYNC, ServicePeerDirection::Inbound,),
        "a full inbound cap must not open a new header-sync stream"
    );
    assert!(
        service.wants_peer(
            &peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        ),
        "an outbound slot should still allow lazy header-sync escalation"
    );

    shutdown.cancel();
    reactor_task.await?;
    Ok(())
}

#[tokio::test]
async fn header_session_waits_for_coordinator_capability_epoch() -> Result<(), BoxError> {
    use zakura_node_services::sync_lifecycle::{
        BlockServiceDemand, HeaderServiceDemand, LifecycleEpoch, SyncServiceDemand,
    };

    let shutdown = CancellationToken::new();
    let (header_sync, _actions, reactor_task) =
        spawn_header_sync_reactor(header_sync_startup(shutdown.clone()))?;
    let disabled = SyncServiceDemand {
        header: HeaderServiceDemand::Disabled {
            runtime_epoch: LifecycleEpoch::INITIAL,
        },
        block: BlockServiceDemand::ServingOnly {
            apply_epoch: LifecycleEpoch::INITIAL,
        },
    };
    let (demand_tx, demand_rx) = watch::channel(disabled);
    let service = HeaderSyncService::new(header_sync).with_service_demand(Some(demand_rx));
    let peer = test_peer(171);
    assert!(!service.wants_peer(
        &peer,
        ZAKURA_CAP_HEADER_SYNC,
        ServicePeerDirection::Outbound,
    ));
    let SessionDemand::WaitForChange(changed) = service.session_demand(
        test_conn_id(),
        &peer,
        ZAKURA_CAP_HEADER_SYNC,
        ServicePeerDirection::Outbound,
    ) else {
        panic!("disabled coordinator demand must park the ordered header session");
    };

    demand_tx
        .send(SyncServiceDemand {
            header: HeaderServiceDemand::Enabled {
                capability_epoch: LifecycleEpoch::new(1),
            },
            ..disabled
        })
        .expect("the header service retains its demand receiver");
    tokio::time::timeout(Duration::from_secs(1), changed)
        .await
        .expect("the capability epoch wakes ordered-session demand");
    assert!(service.wants_peer(
        &peer,
        ZAKURA_CAP_HEADER_SYNC,
        ServicePeerDirection::Outbound,
    ));

    shutdown.cancel();
    reactor_task.await?;
    Ok(())
}

#[tokio::test]
async fn header_sync_peer_connected_has_immediately_ready_outbound_source() -> Result<(), BoxError>
{
    let shutdown = CancellationToken::new();
    let mut startup = header_sync_startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(1),
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
        frontiers: zakura_header_chain::FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            anchor.hash,
        ),
        oldest_retained_height: anchor.height,
        alarms: Default::default(),
    };
    let (_snapshot_tx, snapshot_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshot_rx);
    let (header_sync, _actions, reactor_task) = spawn_header_sync_reactor(startup)?;
    let service = HeaderSyncService::new(header_sync);
    let peer = test_peer(18);
    let connection_cancel = CancellationToken::new();
    let service_cancel = connection_cancel.child_token();
    let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(8);
    let (outbound_tx, mut outbound_rx) = crate::zakura::framed_channel(8);
    let session_id = 7;
    let streams = HashMap::from([(
        ZAKURA_STREAM_HEADER_SYNC,
        ServiceStream::new(
            session_id,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            inbound_rx,
            outbound_tx,
            service_cancel,
        ),
    )]);

    service.add_peer(Peer::new_with_service_streams(
        test_conn_id(),
        peer,
        None,
        ZAKURA_CAP_HEADER_SYNC,
        ServicePeerDirection::Inbound,
        streams,
        connection_cancel.clone(),
        CloseCause::new(),
    ));

    let frame = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("the first outbound source is immediately ready")
        .expect("the admitted source remains open");
    assert!(matches!(
        crate::zakura::wire_codec::decode_frame::<crate::zakura::header_sync::v9::CommonMessage>(
            &frame
        )
        .expect("the first outbound frame decodes"),
        crate::zakura::header_sync::v9::Message::Status(_)
    ));
    assert!(!connection_cancel.is_cancelled());

    shutdown.cancel();
    reactor_task.await?;
    Ok(())
}

#[tokio::test]
async fn supervisor_disconnect_peer_cancels_registered_token() -> Result<(), BoxError> {
    let supervisor = ZakuraSupervisorHandle::new(4);
    let peer = test_peer(8);
    let disconnect_token = CancellationToken::new();
    register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

    assert!(supervisor.disconnect_peer(&peer).await);
    tokio::time::timeout(Duration::from_secs(1), disconnect_token.cancelled())
        .await
        .expect("disconnect token is cancelled promptly");
    assert!(!supervisor.disconnect_peer(&test_peer(9)).await);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn duplicate_evicts_stale_incumbent_but_keeps_fresh_one() -> Result<(), BoxError> {
    // A duplicate connection for an identity that already has one either
    // means the peer restarted (incumbent is a dead, stale connection that
    // should be evicted so the redial can reclaim the slot) or that two
    // connections raced at startup (simultaneous open, both fresh, must NOT
    // be evicted or they flap). Incumbent age distinguishes the two.
    let supervisor = ZakuraSupervisorHandle::new(4);

    async fn register_duplicate(
        supervisor: &ZakuraSupervisorHandle,
        peer: &ZakuraPeerId,
        token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                None,
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                token,
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }

    // Fresh incumbent: an immediate duplicate is a simultaneous-open race and
    // must not evict it.
    let fresh_peer = test_peer(8);
    let fresh_incumbent = CancellationToken::new();
    register_test_peer(&supervisor, fresh_peer.clone(), fresh_incumbent.clone()).await;
    let registration = register_duplicate(&supervisor, &fresh_peer, CancellationToken::new()).await;
    assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
    assert!(
        !fresh_incumbent.is_cancelled(),
        "a young incumbent is kept so simultaneous-open races do not flap",
    );

    // Stale incumbent: once it is older than the threshold, a duplicate
    // evicts it so a restarted peer's redial can reclaim the slot.
    let stale_peer = test_peer(9);
    let stale_incumbent = CancellationToken::new();
    register_test_peer(&supervisor, stale_peer.clone(), stale_incumbent.clone()).await;
    tokio::time::advance(ZAKURA_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
    let newcomer = CancellationToken::new();
    let registration = register_duplicate(&supervisor, &stale_peer, newcomer.clone()).await;
    assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
    assert!(
        stale_incumbent.is_cancelled(),
        "a stale incumbent is evicted so the restarted peer's redial reclaims the slot",
    );
    assert!(
        !newcomer.is_cancelled(),
        "the rejected newcomer's token is never registered, so it is left to redial",
    );

    Ok(())
}

// SECURITY AUDIT (candidate claude-per-ip-cap-blocks-stale-duplicate-eviction /
// codex-ip-cap-blocks-stale-duplicate-eviction /
// subset-admission-identity-state-ip-cap-blocks-stale-duplicate-eviction):
// SR-4 liveness.
//
// Outbound/native-direct dials register with `remote_ip = Some(ip)`, so the
// per-IP cap precheck runs. A same-peer redial from an IP already at the cap
// must NOT be rejected as a resource limit before the duplicate stale-eviction
// path runs: the incumbent already occupies the only per-IP slot, so a duplicate
// for the same identity cannot consume an additional slot. If the precheck
// rejects it first, a dead incumbent keeps its own peer blocked until the QUIC
// idle timeout (~150s) instead of being evicted in milliseconds.
#[tokio::test(start_paused = true)]
async fn same_peer_duplicate_at_per_ip_cap_still_evicts_stale_incumbent() -> Result<(), BoxError> {
    async fn register_from_ip(
        supervisor: &ZakuraSupervisorHandle,
        peer: &ZakuraPeerId,
        ip: IpAddr,
        token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                Some(ip),
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                token,
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }

    let supervisor = ZakuraSupervisorHandle::new(1);
    let ip: IpAddr = "203.0.113.9".parse().expect("test ip parses");
    let peer = test_peer(42);

    // Incumbent: a native-direct dial registers from the IP and fills the
    // cap-1 per-IP slot.
    let incumbent = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer, ip, incumbent.clone()).await;
    assert!(
        matches!(registration, ZakuraRegistration::Registered { .. }),
        "the first connection from the IP registers",
    );

    // The incumbent ages past the stale-eviction threshold: it is now a likely
    // dead connection left behind by a peer restart.
    tokio::time::advance(ZAKURA_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;

    // The same peer redials from the same IP. The IP is already at cap 1, but
    // the duplicate must still reach the stale-eviction path and cancel the dead
    // incumbent so the redial reclaims the slot in milliseconds.
    let newcomer = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer, ip, newcomer.clone()).await;
    assert!(
        matches!(registration, ZakuraRegistration::Duplicate { .. }),
        "a same-peer redial from a capped IP must reach duplicate handling, not be \
             rejected as a resource limit before stale eviction can run",
    );
    assert!(
        incumbent.is_cancelled(),
        "the stale incumbent must be evicted so the restarted peer's redial reclaims \
             the slot in milliseconds instead of waiting for the QUIC idle timeout",
    );
    assert!(
        !newcomer.is_cancelled(),
        "the rejected newcomer's token is never registered, so it is left to redial",
    );

    Ok(())
}

#[tokio::test]
async fn ipv4_embedded_ipv6_addresses_share_the_supervisor_ip_bucket() {
    async fn register_from_ip(
        supervisor: &ZakuraSupervisorHandle,
        peer: ZakuraPeerId,
        ip: IpAddr,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                Some(ip),
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                CancellationToken::new(),
                ZAKURA_CAP_DISCOVERY,
            )
            .await
    }

    let ipv4: IpAddr = "93.184.216.34".parse().expect("IPv4 test address parses");
    let mapped: IpAddr = "::ffff:93.184.216.34"
        .parse()
        .expect("mapped test address parses");
    let compatible: IpAddr = "::93.184.216.34"
        .parse()
        .expect("compatible test address parses");
    let six_to_four: IpAddr = "2002:5db8:d822::"
        .parse()
        .expect("6to4 test address parses");
    let teredo: IpAddr = "2001:0:c000:22d::a247:27dd"
        .parse()
        .expect("Teredo test address parses");
    let nat64: IpAddr = "64:ff9b::5db8:d822"
        .parse()
        .expect("NAT64 test address parses");
    let supervisor = ZakuraSupervisorHandle::new(1);

    assert!(matches!(
        register_from_ip(&supervisor, test_peer(60), six_to_four).await,
        ZakuraRegistration::Registered { .. }
    ));

    for alias in [ipv4, mapped, compatible, six_to_four, teredo, nat64] {
        assert!(
            !supervisor.can_accept_remote_ip_with_in_flight(alias, 0),
            "{alias} must share the occupied IPv4 identity bucket"
        );
    }

    assert!(matches!(
        register_from_ip(&supervisor, test_peer(61), ipv4).await,
        ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
    ));
}

// A restarted or resyncing peer redials from its stable IP while its previous
// connection still lingers as the incumbent. Waiting out the full
// `ZAKURA_DUPLICATE_EVICT_MIN_AGE` (300s) locks that peer out of block sync
// until the dead connection's QUIC idle timeout reaps it. A same-IP duplicate
// therefore evicts the incumbent on the much shorter
// `ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE` gate — while still keeping a
// just-registered incumbent so simultaneous-open races do not flap, and while
// a *different*-IP duplicate keeps the long gate.
#[tokio::test(start_paused = true)]
async fn same_ip_duplicate_uses_short_eviction_gate() -> Result<(), BoxError> {
    async fn register_from_ip(
        supervisor: &ZakuraSupervisorHandle,
        peer: &ZakuraPeerId,
        ip: Option<IpAddr>,
        token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                ip,
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                token,
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }

    let supervisor = ZakuraSupervisorHandle::new(4);
    let ip_a: IpAddr = "203.0.113.10".parse().expect("test ip parses");
    let peer = test_peer(50);

    // Incumbent registered from ip_a.
    let incumbent = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer, Some(ip_a), incumbent.clone()).await;
    assert!(matches!(
        registration,
        ZakuraRegistration::Registered { .. }
    ));

    // A same-IP duplicate below the short gate keeps the incumbent so a genuine
    // simultaneous-open race (both connections fresh) does not flap.
    tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE - Duration::from_secs(1)).await;
    let early = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer, Some(ip_a), early.clone()).await;
    assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
    assert!(
        !incumbent.is_cancelled(),
        "a same-IP duplicate below the short gate keeps the incumbent",
    );

    // Past the short same-IP gate — but far below the 300s gate — a same-IP
    // duplicate evicts the stale incumbent so the restarted peer's redial
    // reclaims the slot in seconds.
    tokio::time::advance(Duration::from_secs(2)).await;
    let redial = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer, Some(ip_a), redial.clone()).await;
    assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
    assert!(
        incumbent.is_cancelled(),
        "a same-IP duplicate past the short gate evicts the stale incumbent",
    );
    assert!(
        !redial.is_cancelled(),
        "the rejected newcomer's token is never registered, so it is left to redial",
    );

    // Contrast: a *different*-IP duplicate keeps the long gate, so at the same
    // small age it does not evict — only a same-IP redial gets the fast path.
    let peer2 = test_peer(51);
    let ip_b: IpAddr = "203.0.113.11".parse().expect("test ip parses");
    let ip_c: IpAddr = "203.0.113.12".parse().expect("test ip parses");
    let incumbent2 = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer2, Some(ip_b), incumbent2.clone()).await;
    assert!(matches!(
        registration,
        ZakuraRegistration::Registered { .. }
    ));
    tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
    let diff_ip = CancellationToken::new();
    let registration = register_from_ip(&supervisor, &peer2, Some(ip_c), diff_ip.clone()).await;
    assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
    assert!(
        !incumbent2.is_cancelled(),
        "a different-IP duplicate keeps the long gate and does not evict a young incumbent",
    );

    Ok(())
}

#[tokio::test]
async fn header_sync_misbehavior_action_does_not_disconnect_peer() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let reactor_shutdown = CancellationToken::new();
    let startup = header_sync_startup(reactor_shutdown.clone());
    let (header_sync, _reactor_actions, reactor_task) = spawn_header_sync_reactor(startup)?;
    let supervisor = ZakuraSupervisorHandle::new(4);
    let peer = test_peer(10);
    let disconnect_token = CancellationToken::new();
    register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

    let (actions_tx, actions_rx) = mpsc::channel(4);
    let driver_shutdown = CancellationToken::new();
    let driver_task = tokio::spawn(drive_header_sync_actions(
        actions_rx,
        header_sync,
        supervisor,
        driver_shutdown.clone(),
    ));
    actions_tx
        .send(HeaderSyncAction::Misbehavior {
            peer,
            reason: HeaderSyncMisbehavior::MalformedMessage,
        })
        .await?;

    assert!(
        tokio::time::timeout(Duration::from_millis(100), disconnect_token.cancelled())
            .await
            .is_err(),
        "misbehavior is record-only and must not cancel the registered connection",
    );

    driver_shutdown.cancel();
    driver_task.await?;
    reactor_shutdown.cancel();
    reactor_task.await?;
    Ok(())
}

/// Only a body that proves tampering closes the supplier's connection.
#[tokio::test]
async fn block_sync_misbehavior_disconnects_only_a_proven_tampered_body() {
    use crate::zakura::BlockSyncMisbehavior;
    use zakura_header_chain::BodyCommitmentKind;

    let mismatch = |kind| {
        BlockSyncMisbehavior::BodyPayloadMismatch(zakura_header_chain::BodyPayloadMismatch {
            evidence: zakura_header_chain::EvidenceId::from_digest([1; 32]),
            requested: block::Hash([2; 32]),
            delivered: block::Hash([2; 32]),
            kind,
            source: zakura_header_chain::SourceId::from_digest([3; 32]),
        })
    };
    let cases = [
        (mismatch(BodyCommitmentKind::AuthDataRoot), true),
        (
            mismatch(BodyCommitmentKind::CHAIN_HISTORY_BLOCK_TX_AUTH_COMMITMENT),
            true,
        ),
        (mismatch(BodyCommitmentKind::TransactionMerkleRoot), true),
        // A different header at the requested height can be a fork response.
        (mismatch(BodyCommitmentKind::HeaderHash), false),
        // These depend on local trees or supplied auxiliary roots.
        (
            mismatch(BodyCommitmentKind::Other("final_sapling_root")),
            false,
        ),
        (
            mismatch(BodyCommitmentKind::Other("chain_history_root")),
            false,
        ),
        (BlockSyncMisbehavior::InvalidBlock, false),
        (BlockSyncMisbehavior::StatusSpam, false),
    ];
    let supervisor = ZakuraSupervisorHandle::new(cases.len());
    for (index, (reason, disconnects)) in cases.into_iter().enumerate() {
        let peer = test_peer(u8::try_from(index + 20).expect("the case count fits in u8"));
        let disconnect_token = CancellationToken::new();
        register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

        assert_eq!(
            supervisor
                .enforce_block_sync_misbehavior(&peer, &reason)
                .await,
            disconnects,
            "{reason:?}"
        );
        assert_eq!(disconnect_token.is_cancelled(), disconnects, "{reason:?}");
        if disconnects {
            assert!(
                !supervisor
                    .enforce_block_sync_misbehavior(&peer, &reason)
                    .await,
                "a connection already closing is not closed again: {reason:?}"
            );
        }
    }
    assert!(
        !supervisor
            .enforce_block_sync_misbehavior(
                &test_peer(250),
                &mismatch(BodyCommitmentKind::AuthDataRoot)
            )
            .await,
        "a peer that already left has nothing to close"
    );
}

/// Exercise the production worker with a full application receive channel.
/// Sending must stay live, QUIC must eventually stop the bounded sender, and
/// another stream must retain connection credit until the application resumes.
#[tokio::test]
async fn paused_ordered_reads_preserve_writes_and_sibling_credit() -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/paused-reader/0";
    const FRAME_BYTES: usize = 1024 * 1024;
    let local = ZakuraLocalLimits::from_config(&Config::default());
    // The 1 MiB frames fill the stream receive window, plus a margin the
    // sender cannot write.
    let window_frames = local.quic.stream_receive_window_bytes as usize / FRAME_BYTES;
    let frame_count = window_frames + 8;
    // After negotiation the connection window holds y_ctl beside the
    // largest stream window, so a paused stream leaves sibling credit.
    let server = LocalEndpointFactory::with_limits(&local)
        .quic_config(QuicConfig {
            receive_window_bytes: local
                .quic
                .receive_window_bytes
                .saturating_add(local.quic.stream_receive_window_bytes),
            ..local.quic.clone()
        })
        .endpoint(52)
        .await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    // A small sender buffer makes write completion reflect receiver credit.
    let client = LocalEndpointFactory::with_limits(&local)
        .quic_config(QuicConfig {
            send_window_bytes: 64 * 1024,
            ..local.quic.clone()
        })
        .endpoint(53)
        .await?;
    let address = LocalEndpointFactory::node_addr(&server).await;
    let connection = timeout(Duration::from_secs(10), client.connect(address, &[ALPN])).await??;
    let (mut sender, mut receiver) = connection.open_bi().await?;
    let frame = Frame {
        message_type: 3,
        flags: 0,
        payload: vec![0; FRAME_BYTES],
    };
    let limits = test_connection_limits();
    let stream = Stream {
        kind: ZAKURA_STREAM_BLOCK_SYNC,
        version: ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        frame_cap: 2_000_009,
        capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ..Stream::PERSISTENT
    };
    let encoded = frame.encode(stream.frame_cap)?;
    // Opening a QUIC stream becomes visible to the receiver after the first bytes.
    sender.write_all(&encoded[..1]).await?;
    let (send, recv) = timeout(Duration::from_secs(5), stream_rx.recv())
        .await?
        .unwrap();
    let cancel = CancellationToken::new();
    let stream_cancel = cancel.child_token();
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
    let context = StreamWorkerContext {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: test_peer(52),
        stream_id: 1,
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        limits,
        inbound_frame_cap: stream.frame_cap,
        messages: None,
        queue_depths: None,
        write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
        session_resources: None,
        outbound_frame_cap: stream.frame_cap,
        message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(128))),
        cadence: None,
        precheck: PrecheckSlot::default(),
        resources: StreamResources::unconstrained(),
        connection_token: cancel.clone(),
        stream_token: stream_cancel.clone(),
        close_cause: CloseCause::new(),
        freshness_tx,
    };
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: None,
        max_frame_bytes: stream.frame_cap,
    };
    let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
    let (outbound_tx, outbound_rx) = worker_framed_channel(1);
    let mut worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
        persistent_stream_worker(send, recv, prelude, context, inbound_tx, outbound_rx, 1),
    ));
    let (progress_tx, mut progress_rx) = watch::channel(0);
    let mut sending = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        sender.write_all(&encoded[1..]).await.unwrap();
        progress_tx.send_replace(1);
        for sent in 2..=frame_count {
            sender.write_all(&encoded).await.unwrap();
            progress_tx.send_replace(sent);
        }
        sender
    }));
    timeout(Duration::from_secs(10), async {
        while *progress_rx.borrow_and_update() < window_frames {
            progress_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("the sender fills the default stream receive window");
    assert!(
        timeout(Duration::from_millis(100), &mut sending)
            .await
            .is_err(),
        "QUIC must stop the sender while application reads are paused"
    );
    assert_eq!(inbound_rx.len(), 1);

    let response = Frame {
        message_type: 4,
        flags: 0,
        payload: vec![7; 9],
    };
    let producer = crate::zakura::regulation::SlotBudget::new(1).unwrap();
    let ownership = Arc::new(producer.try_reserve().unwrap());
    outbound_tx.try_reserve_guarded().unwrap().send(
        response.clone(),
        crate::zakura::transport::FrameGuard::new(ownership),
    );
    let written = timeout(
        Duration::from_secs(2),
        read_frame(
            &mut receiver,
            stream.frame_cap,
            FrameFilter::new(None, InboundReader::Persistent),
            Duration::from_secs(2),
            None,
        ),
    )
    .await??;
    assert_eq!(written, response);
    assert_eq!(
        producer.reserved(),
        0,
        "the response write releases serving capacity"
    );

    let (mut sibling_send, _sibling_recv) = connection.open_bi().await?;
    timeout(Duration::from_secs(2), sibling_send.write_all(b"sibling")).await??;
    let (_, mut sibling_read) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    let mut bytes = [0; 7];
    timeout(Duration::from_secs(2), sibling_read.read_exact(&mut bytes)).await??;
    assert_eq!(&bytes, b"sibling");

    timeout(Duration::from_secs(10), async {
        for _ in 0..frame_count {
            assert_eq!(inbound_rx.recv().await.unwrap().frame, frame);
        }
    })
    .await
    .expect("resuming application reads drains all frames in order");
    let mut sender = timeout(Duration::from_secs(2), &mut sending).await??;
    assert_eq!(*progress_rx.borrow(), frame_count);
    // Cancellation must also work while the reader waits on a full channel.
    let empty = Frame {
        message_type: 2,
        flags: 0,
        payload: vec![0; 9],
    }
    .encode(stream.frame_cap)?;
    sender.write_all(&empty).await?;
    sender.write_all(&empty).await?;
    timeout(Duration::from_secs(2), async {
        while inbound_rx.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    stream_cancel.cancel();
    timeout(Duration::from_secs(2), &mut worker).await??;
    assert!(
        !cancel.is_cancelled(),
        "local stream cancellation preserves the connection"
    );
    connection.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn stream_cancel_closes_ordered_worker_without_connection_cancel() -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/stream-cancel/0";

    let _guard = zakura_test::init();
    let local = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_limits(&local)
        .quic_config(QuicConfig {
            send_window_bytes: 64 * 1024,
            ..local.quic.clone()
        })
        .endpoint(50)
        .await?;
    let (conn_tx, mut conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::with_limits(&local)
        .quic_config(QuicConfig {
            stream_receive_window_bytes: 64 * 1024,
            ..local.quic.clone()
        })
        .endpoint(51)
        .await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects to loopback capture endpoint")?;
    let server_conn = timeout(Duration::from_secs(10), conn_rx.recv())
        .await
        .expect("server connection is captured")
        .expect("capture handler sends the accepted connection");
    drop(server_conn);
    let (mut client_send, mut client_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the worker stream")?;
    let test_frame = Frame {
        message_type: 1,
        flags: 0,
        payload: Vec::new(),
    }
    .encode(LOCAL_MAX_CONTROL_FRAME_BYTES)?;
    timeout(Duration::from_secs(1), client_send.write_all(&test_frame))
        .await
        .expect("client writes the worker stream frame")?;
    let (server_send, server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the worker stream")
        .expect("capture handler sends the worker stream");

    let mut limits = test_connection_limits();
    limits.idle_timeout = Duration::from_millis(50);
    let stream_kind = ZAKURA_STREAM_BLOCK_SYNC;
    let connection_token = CancellationToken::new();
    let stream_token = connection_token.child_token();
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
    let permit = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("test semaphore starts with one permit");
    let stream = Stream {
        kind: stream_kind,
        version: ZAKURA_STREAM_VERSION_1,
        frame_cap: 2_000_009,
        capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ..Stream::PERSISTENT
    };
    let context = StreamWorkerContext {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: test_peer(55),
        stream_id: 1,
        _permit: permit,
        limits,
        inbound_frame_cap: inbound_frame_cap_for_stream(&limits, stream),
        messages: None,
        queue_depths: None,
        write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
        session_resources: None,
        outbound_frame_cap: application_frame_cap(&limits, stream),
        message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(128))),
        cadence: None,
        precheck: PrecheckSlot::default(),
        resources: StreamResources::unconstrained(),
        connection_token: connection_token.clone(),
        stream_token: stream_token.clone(),
        close_cause: CloseCause::new(),
        freshness_tx,
    };
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind,
        stream_version: ZAKURA_STREAM_VERSION_1,
        request_id: None,
        max_frame_bytes: inbound_frame_cap_for_stream(&limits, stream),
    };
    let mut workers = JoinSet::new();
    let (session_exit_tx, mut session_exit_rx) = mpsc::unbounded_channel();
    let admitted = spawn_persistent_stream_worker(
        &mut workers,
        server_send,
        server_recv,
        stream,
        prelude,
        context,
        1,
        true,
        session_exit_tx,
    );

    let response = Frame {
        message_type: 3,
        flags: 0,
        payload: vec![7; 1024 * 1024],
    };
    admitted.streams[0].send.try_send(response.clone()).unwrap();
    // Reading the header proves the write has started. The much smaller
    // QUIC windows keep the remaining payload blocked until we drain it.
    let mut header = [0; FRAME_HEADER_BYTES];
    timeout(Duration::from_secs(2), client_recv.read_exact(&mut header)).await??;
    assert_eq!(
        &header[..],
        &response.encode(stream.frame_cap)?[..FRAME_HEADER_BYTES]
    );
    admitted.cancel_token.cancel();
    let mut payload = vec![0; response.payload.len()];
    assert!(
        timeout(Duration::from_secs(2), client_recv.read_exact(&mut payload))
            .await?
            .is_err(),
        "session cancellation resets an unfinished frame"
    );
    // The exit must be reported, or the connection loop never prunes the dead
    // generation and never reopens the stream.
    let exited = timeout(Duration::from_secs(1), session_exit_rx.recv())
        .await
        .expect("stream cancellation reports worker exit")
        .expect("exit channel stays open");
    assert_eq!(exited.stream, stream);
    assert_eq!(exited.session_id, 1);
    assert!(exited.opened_locally);
    timeout(Duration::from_secs(1), workers.join_next())
        .await
        .expect("stream cancellation stops the worker promptly")
        .expect("worker exit is observed")?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !connection_token.is_cancelled(),
        "stream-local cancellation must not cancel the shared connection"
    );

    let (mut sibling_send, _sibling_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("connection survives to open a sibling stream")?;
    timeout(Duration::from_secs(1), sibling_send.write_all(&test_frame))
        .await
        .expect("client writes the sibling stream frame")?;
    timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the sibling stream")
        .expect("capture handler sends the sibling stream");

    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// Regression for the ~150s mainnet Zakura peer churn
/// (`run-f406a987-churn-20260619T032010Z`): a long-lived ordered stream that
/// is legitimately quiet between frames -- e.g. the gossip stream while
/// syncing far below the tip -- must not tear down the whole connection it
/// shares with an actively-transferring stream.
///
/// Trace forensics showed every serving peer's connection `closed.neutral`
/// with reason `cancelled` at a lifetime of exactly 150.1-150.3s (the QUIC /
/// app idle timeout), then a reconnect + duplicate-arbitration storm, even
/// though block sync was downloading at ~99 blk/s the whole time. The cause:
/// the ordered-stream reader used `idle_timeout` as its per-frame read
/// deadline AND treated that deadline as a fatal error, so the first quiet
/// ordered stream `connection_token.cancel()`ed the entire connection at
/// `idle_timeout` -- dropping the active sibling transfer -- even though the
/// connection-level freshness reaper (which resets on ANY stream's activity)
/// would never have reaped it. That is why the close reason is `cancelled`
/// (worker-initiated) and not `idle_timeout` (reaper).
///
/// This drives the production `persistent_stream_worker` over a real local
/// QUIC connection with two ordered streams that share one
/// `connection_token` and one freshness watch channel, exactly as
/// `serve_connection` wires them, plus the real `freshness_reaper`. One
/// stream is kept busy (mirrors block sync); the other stays quiet (mirrors
/// gossip). The connection must survive well past `idle_timeout`.
#[tokio::test]
async fn quiet_ordered_stream_does_not_cancel_connection_with_active_sibling(
) -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/quiet-ordered-stream/0";

    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(80).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(81).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects to loopback capture endpoint")?;

    let frame = Frame {
        message_type: 1,
        flags: 0,
        payload: Vec::new(),
    }
    .encode(LOCAL_MAX_CONTROL_FRAME_BYTES)?;

    // Both streams need one initial frame so the server's `accept_bi`
    // observes the stream and we can spawn its worker; after that only the
    // active stream keeps sending. The client send halves are retained for
    // the whole test so neither stream is `Closed` -- the quiet stream must
    // sit in an inter-frame read, not a peer-initiated close.
    let (mut active_send, _active_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the active stream")?;
    timeout(Duration::from_secs(1), active_send.write_all(&frame))
        .await
        .expect("client writes the active stream's first frame")?;
    let (active_server_send, active_server_recv) =
        timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the active stream")
            .expect("capture handler forwards the active stream");

    let (mut quiet_send, _quiet_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the quiet stream")?;
    timeout(Duration::from_secs(1), quiet_send.write_all(&frame))
        .await
        .expect("client writes the quiet stream's only frame")?;
    let (quiet_server_send, quiet_server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the quiet stream")
        .expect("capture handler forwards the quiet stream");

    // Shared per-connection state, exactly as `serve_connection` builds it:
    // one cancellation token and one freshness watch channel feeding every
    // worker and the reaper.
    let idle_timeout = Duration::from_millis(250);
    let mut limits = test_connection_limits();
    limits.idle_timeout = idle_timeout;
    let stream_kind = LEGACY_GOSSIP_STREAM_KIND;
    let connection_token = CancellationToken::new();
    let (freshness_tx, freshness_rx) = watch::channel(Instant::now());

    let spawn_ordered_worker = |send: SendStream, recv: RecvStream, stream_id: u64| {
        let permit = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("test semaphore starts with one permit");
        let stream = Stream {
            kind: stream_kind,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::PERSISTENT
        };
        let context = StreamWorkerContext {
            conn: ZakuraConnTrace::without_peer(1),
            peer_id: test_peer(80),
            stream_id,
            _permit: permit,
            limits,
            inbound_frame_cap: inbound_frame_cap_for_stream(&limits, stream),
            messages: None,
            queue_depths: None,
            write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
            session_resources: None,
            outbound_frame_cap: application_frame_cap(&limits, stream),
            message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(
                limits.message_rate_per_second,
            ))),
            cadence: None,
            precheck: PrecheckSlot::default(),
            resources: StreamResources::unconstrained(),
            connection_token: connection_token.clone(),
            stream_token: connection_token.child_token(),
            close_cause: CloseCause::new(),
            freshness_tx: freshness_tx.clone(),
        };
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version: ZAKURA_STREAM_VERSION_1,
            request_id: None,
            max_frame_bytes: inbound_frame_cap_for_stream(&limits, stream),
        };
        // Drain the inbound side so the worker never blocks forwarding a
        // read frame to a full service channel (which would itself stop the
        // reader and confound the test). The outbound side is unused here.
        let (inbound_tx, mut inbound_rx) = mpsc::channel(16);
        let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
        tokio::spawn(async move { while inbound_rx.recv().await.is_some() {} });
        tokio::spawn(persistent_stream_worker(
            send,
            recv,
            prelude,
            context,
            inbound_tx,
            outbound_rx,
            16,
        ))
    };

    let _active_worker = spawn_ordered_worker(active_server_send, active_server_recv, 1);
    let _quiet_worker = spawn_ordered_worker(quiet_server_send, quiet_server_recv, 2);

    // Model `serve_connection`'s idle race: the connection ends either when a
    // worker cancels the shared token or when the freshness reaper fires
    // (which `serve_connection` follows with `connection_token.cancel()`).
    // So `connection_token.is_cancelled()` captures BOTH teardown modes.
    let serve = tokio::spawn({
        let connection_token = connection_token.clone();
        async move {
            tokio::select! {
                biased;
                _ = connection_token.cancelled() => {}
                _ = freshness_reaper(freshness_rx, idle_timeout) => connection_token.cancel(),
            }
        }
    });

    // The active sibling transfers a frame well within `idle_timeout`, so the
    // connection-level freshness stays fresh and the reaper must never fire.
    let keepalive_frame = frame.clone();
    let keepalive = tokio::spawn(async move {
        for _ in 0..16 {
            tokio::time::sleep(Duration::from_millis(80)).await;
            if active_send.write_all(&keepalive_frame).await.is_err() {
                break;
            }
        }
    });

    // Wait well past `idle_timeout`; the buggy reader tears the connection
    // down at ~`idle_timeout` (~250ms) despite the active sibling.
    tokio::time::sleep(idle_timeout * 4).await;
    assert!(
        !connection_token.is_cancelled(),
        "a quiet ordered stream must not cancel a connection whose sibling stream is \
             actively transferring; the shared connection_token was cancelled within {:?}",
        idle_timeout * 4,
    );

    connection_token.cancel();
    keepalive.abort();
    let _ = serve.await;
    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// Regression for `claude-outbound-write-ignores-message-cap` (persistent
/// ordered-stream facet).
///
/// `write_ordered_frame` sized outbound persistent-stream frames against the
/// negotiated frame cap only. The handshake clamps `max_frame_bytes` and
/// `max_message_bytes` independently, so a peer can negotiate a small message
/// cap with a large frame cap. A persistent service that queues a frame
/// larger than that message cap then has it encoded and written, and the peer
/// rejects it as oversize and disconnects us, wasting the encode/write.
/// `write_ordered_frame` must mirror `write_response_frame` and reject an
/// over-message-cap frame locally before encoding/writing it, while still
/// writing frames within the cap.
#[tokio::test]
async fn write_ordered_frame_rejects_payload_over_message_cap() -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/ordered-message-cap/0";

    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(74).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, _stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(75).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects to the ordered-message-cap endpoint")?;
    let (mut send, _recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens an ordered stream")?;

    // A small negotiated message cap with a large frame cap, as the handshake
    // permits.
    let mut limits = test_connection_limits();
    limits.max_message_bytes = 256;
    let stream = Stream {
        kind: DISCOVERY_STREAM_KIND,
        version: ZAKURA_STREAM_VERSION_1,
        frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
        capability: ZAKURA_CAP_DISCOVERY,
        ..Stream::PERSISTENT
    };
    let frame_cap = application_frame_cap(&limits, stream);

    // A payload above the message cap but well within the frame cap, so only
    // the message cap can reject it.
    let oversized = Frame {
        message_type: 1,
        flags: 0,
        payload: vec![0xab; 4096],
    };
    assert!(
        oversized.payload.len() <= frame_cap as usize,
        "test payload must fit the frame cap so only the message cap can reject it"
    );

    let result = write_ordered_frame(&mut send, oversized, limits, frame_cap).await;
    assert!(
        result.is_err(),
        "write_ordered_frame must reject a payload over the negotiated max_message_bytes \
             before encoding/writing it, mirroring write_response_frame; got {result:?}"
    );

    // A payload within the negotiated message cap must still be written.
    let within_cap = Frame {
        message_type: 1,
        flags: 0,
        payload: vec![0xcd; 128],
    };
    write_ordered_frame(&mut send, within_cap, limits, frame_cap)
        .await
        .expect("a frame within the negotiated message cap must still be written");

    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

// Regression for `claude-control-payload-late-hard-cap`: the native control
// hello/ack reads passed the configured `max_control_frame_bytes` (1 MiB
// default) to `read_control_payload`, so a peer could force allocation/read
// of a control payload between the 16 KiB hard cap and 1 MiB before
// ZakuraControlHello/Ack::decode rejected it. The reader must instead reject
// an oversized control length on the length prefix alone, before reading the
// body, while still accepting payloads within the 16 KiB hard cap.
#[tokio::test]
async fn read_control_payload_enforces_hard_cap_before_reading_body() -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/control-hard-cap/0";

    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(70).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(71).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects to the control-hard-cap endpoint")?;

    // A control length above the 16 KiB hard cap but below the 1 MiB frame
    // cap the production responder/initiator pass. The body is never sent: a
    // correct reader must reject on the length prefix alone.
    let oversized_len =
        u32::try_from(MAX_CONTROL_PAYLOAD_BYTES + 1).expect("control hard cap + 1 fits in u32");
    let (mut over_send, _over_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the oversized control stream")?;
    timeout(
        Duration::from_secs(1),
        over_send.write_all(&oversized_len.to_le_bytes()),
    )
    .await
    .expect("client writes the oversized control length")?;
    let _ = over_send.finish();

    let (_over_server_send, mut over_server_recv) =
        timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the oversized control stream")
            .expect("capture handler forwards the oversized control stream");

    // Pass exactly what production passes: the configured frame cap (1 MiB),
    // NOT the 16 KiB hard cap. The fix clamps internally to the hard cap.
    let oversized = read_control_payload(
        &mut over_server_recv,
        LOCAL_MAX_CONTROL_FRAME_BYTES,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        matches!(oversized, Err(ZakuraHandlerError::Oversize)),
        "a control length over the 16 KiB hard cap must be rejected as Oversize \
             on the length prefix, before the body is read; got {oversized:?}"
    );

    // A control payload within the hard cap must still be read normally, so
    // the clamp does not break legitimate sub-16 KiB control frames.
    let valid_body = vec![0xa5u8; 128];
    let valid_len = u32::try_from(valid_body.len()).expect("128 fits in u32");
    let (mut ok_send, _ok_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the in-cap control stream")?;
    timeout(
        Duration::from_secs(1),
        ok_send.write_all(&valid_len.to_le_bytes()),
    )
    .await
    .expect("client writes the in-cap control length")?;
    timeout(Duration::from_secs(1), ok_send.write_all(&valid_body))
        .await
        .expect("client writes the in-cap control body")?;
    let _ = ok_send.finish();

    let (_ok_server_send, mut ok_server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the in-cap control stream")
        .expect("capture handler forwards the in-cap control stream");
    let read_back = read_control_payload(
        &mut ok_server_recv,
        LOCAL_MAX_CONTROL_FRAME_BYTES,
        Duration::from_secs(2),
    )
    .await
    .expect("a control payload within the hard cap is read");
    assert_eq!(
        read_back, valid_body,
        "an in-cap control payload must round-trip unchanged"
    );

    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn initiator_refuses_an_ack_that_widens_its_capability_offer() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(72).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: P2P_V2_ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(73).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;
    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[P2P_V2_ALPN]),
    )
    .await??;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let mut config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    config.supported_capabilities = ZAKURA_CAP_HEADER_SYNC;
    let local_peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;

    // Each round acks the hello on a fresh control stream with `extra`
    // added to the offer.
    for (extra, accepted) in [(0, true), (ZAKURA_CAP_BLOCK_SYNC_REGULATED, false)] {
        let handshake = run_native_initiator_handshake_without_trace(
            &client_conn,
            &limits,
            &config,
            &local_peer,
        );
        let respond = async {
            let (mut send, mut recv) = stream_rx.recv().await.ok_or("missing control stream")?;
            let hello = ZakuraControlHello::decode(
                &read_control_payload(
                    &mut recv,
                    LOCAL_MAX_CONTROL_FRAME_BYTES,
                    Duration::from_secs(2),
                )
                .await?,
            )?;
            let ack = ZakuraControlAck {
                magic: CONTROL_ACK_MAGIC,
                control_version: CONTROL_VERSION,
                selected_zakura_protocol: hello.selected_zakura_protocol,
                peer_nonce: [9; 32],
                remote_peer_nonce: hello.peer_nonce,
                accepted_capabilities: hello.capabilities | extra,
                accepted_channels: 0,
                accepted_limits: hello.initial_limits,
            };
            write_control_payload(&mut send, &ack.encode()?, Duration::from_secs(2)).await?;
            Ok::<_, BoxError>(send)
        };
        let (negotiated, responded) = timeout(Duration::from_secs(5), async {
            tokio::join!(handshake, respond)
        })
        .await?;
        // The responder returns its send half so dropping it can't reset
        // the ack before the initiator reads it.
        responded?;
        if accepted {
            assert_eq!(negotiated?.accepted_capabilities, ZAKURA_CAP_HEADER_SYNC);
        } else {
            assert!(
                matches!(
                    negotiated,
                    Err(ZakuraHandlerError::Validation(
                        ZakuraValidationError::UnofferedCapability
                    ))
                ),
                "an ack outside the offer must fail the handshake; got {negotiated:?}"
            );
        }
    }

    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// A responder that paces its ack's length and payload, each within the
/// per-step deadline, still fails the initiator at the overall deadline.
#[tokio::test]
async fn initiator_bounds_a_paced_ack_by_one_deadline() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(74).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    server.serve(CaptureConnection {
        alpn: P2P_V2_ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(75).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;
    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[P2P_V2_ALPN]),
    )
    .await??;
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.control_timeout = Duration::from_secs(1);
    let config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    let local_peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;

    let handshake =
        run_native_initiator_handshake_without_trace(&client_conn, &limits, &config, &local_peer);
    let respond = async {
        let (mut send, mut recv) = stream_rx.recv().await.ok_or("missing control stream")?;
        let hello = ZakuraControlHello::decode(
            &read_control_payload(
                &mut recv,
                LOCAL_MAX_CONTROL_FRAME_BYTES,
                Duration::from_secs(2),
            )
            .await?,
        )?;
        let ack = ZakuraControlAck {
            magic: CONTROL_ACK_MAGIC,
            control_version: CONTROL_VERSION,
            selected_zakura_protocol: hello.selected_zakura_protocol,
            peer_nonce: [9; 32],
            remote_peer_nonce: hello.peer_nonce,
            accepted_capabilities: hello.capabilities,
            accepted_channels: 0,
            accepted_limits: hello.initial_limits,
        }
        .encode()?;
        let mut len = Vec::with_capacity(CONTROL_LENGTH_BYTES);
        len.write_u32::<LittleEndian>(u32::try_from(ack.len())?)?;
        zakura_rt::time::sleep(Duration::from_millis(600)).await;
        send.write_all(&len).await?;
        zakura_rt::time::sleep(Duration::from_millis(600)).await;
        // The initiator may already have closed the stream.
        let _ = send.write_all(&ack).await;
        Ok::<_, BoxError>(send)
    };
    let (negotiated, responded) = timeout(Duration::from_secs(5), async {
        tokio::join!(handshake, respond)
    })
    .await?;
    responded?;
    assert!(
        matches!(
            negotiated,
            Err(ZakuraHandlerError::Timeout("control handshake"))
        ),
        "a paced ack must fail at the overall deadline; got {negotiated:?}"
    );

    client_conn.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// A reader's context for one stream of the regulation test family.
fn tabled_context(
    messages: Option<&'static [MessageRule]>,
    rate_per_second: u32,
) -> StreamWorkerContext {
    let buckets = MessageRateBuckets::new();
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
    let stream = Stream {
        messages,
        ..Stream::PERSISTENT
    };
    let cancel = CancellationToken::new();
    StreamWorkerContext {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: test_peer(52),
        stream_id: 1,
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        limits: test_connection_limits(),
        inbound_frame_cap: 1024,
        messages,
        queue_depths: None,
        write_policy: StreamWritePolicy::UntilCancelled,
        session_resources: None,
        outbound_frame_cap: 1024,
        message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(rate_per_second))),
        cadence: cadence_scope(&buckets, stream, 900),
        precheck: PrecheckSlot::default(),
        resources: StreamResources::unconstrained(),
        connection_token: cancel.clone(),
        stream_token: cancel.child_token(),
        close_cause: CloseCause::new(),
        freshness_tx,
    }
}

fn probe_frame(message_type: u16, payload_len: usize) -> Frame {
    Frame {
        message_type,
        flags: 0,
        payload: vec![1; payload_len],
    }
}

#[test]
fn a_metered_row_past_its_byte_bucket_closes_the_connection() {
    const METERED: MessageRule = MessageRule {
        name: "metered",
        message_type: 40,
        payload: crate::zakura::PayloadLen::between(1, 512),
        role: crate::zakura::MessageRole::Metered {
            rate: crate::zakura::ByteRate {
                bytes_per_second: 1,
                opening_allowance: 512,
                sender_allowance: 512,
            },
        },
    };
    // With no receive window, the receiver's depth is the two allowances.
    let context = tabled_context(Some(&[METERED]), 1);
    for _ in 0..2 {
        assert_eq!(
            admit_inbound_message(&probe_frame(40, 512), &context, 900),
            InboundMessageAdmission::Admit
        );
    }
    assert_eq!(
        admit_inbound_message(&probe_frame(40, 1), &context, 900),
        InboundMessageAdmission::ByteRateOverrun
    );
    assert_eq!(
        ordered_read_close(&ZakuraHandlerError::ByteRateOverrun),
        Some((ZAKURA_CLOSE_RATE_LIMIT, "ordered_byte_rate_overrun"))
    );
}

#[test]
fn tabled_streams_charge_only_the_rows_that_declare_a_cadence() {
    use crate::zakura::regulation::test_family::{message_type, EVERY_30_SECONDS, RULES};

    // A one-message stream bucket would throttle the second frame of a
    // legacy stream. A tabled stream never touches it.
    let context = tabled_context(Some(RULES), 1);
    for message_type in [message_type::PART, message_type::DONE, message_type::GET] {
        for _ in 0..4096 {
            assert_eq!(
                admit_inbound_message(&probe_frame(message_type, 4), &context, 900),
                InboundMessageAdmission::Admit
            );
        }
    }
    for _ in 0..EVERY_30_SECONDS.capacity {
        assert_eq!(
            admit_inbound_message(&probe_frame(message_type::STATUS, 4), &context, 900),
            InboundMessageAdmission::Admit
        );
    }
    assert_eq!(
        admit_inbound_message(&probe_frame(message_type::STATUS, 4), &context, 900),
        InboundMessageAdmission::Admit
    );
    // Each cadence row has its own bucket.
    assert_eq!(
        admit_inbound_message(&probe_frame(message_type::PING, 4), &context, 900),
        InboundMessageAdmission::Admit
    );

    let legacy = tabled_context(None, 1);
    assert_eq!(
        admit_inbound_message(&probe_frame(message_type::PART, 4), &legacy, 900),
        InboundMessageAdmission::Admit
    );
    assert_eq!(
        admit_inbound_message(&probe_frame(message_type::PART, 4), &legacy, 900),
        InboundMessageAdmission::Throttled
    );
}

#[test]
fn a_read_pause_credits_the_streams_cadence_rows() {
    use crate::zakura::regulation::test_family::{message_type, EVERY_30_SECONDS, RULES};

    let context = tabled_context(Some(RULES), 1);
    let tokens = || {
        context
            .cadence
            .as_ref()
            .unwrap()
            .buckets
            .lock()
            .unwrap()
            .tokens(900, message_type::STATUS)
    };
    let status = probe_frame(message_type::STATUS, 4);
    for _ in 0..EVERY_30_SECONDS.capacity {
        admit_inbound_message(&status, &context, 900);
    }
    assert_eq!(
        admit_inbound_message(&status, &context, 900),
        InboundMessageAdmission::Admit
    );
    assert_eq!(tokens(), Some(0));
    context.credit_read_pause(EVERY_30_SECONDS.refill_interval * 3);
    assert_eq!(tokens(), Some(3));
    for remaining in (0..3).rev() {
        assert_eq!(
            admit_inbound_message(&status, &context, 900),
            InboundMessageAdmission::Admit
        );
        assert_eq!(tokens(), Some(remaining));
    }
    assert_eq!(
        admit_inbound_message(&status, &context, 900),
        InboundMessageAdmission::Admit
    );
}

/// Replay the burst from 23 sends spaced 30 seconds apart during a
/// 660-second stream stall. A sibling can keep the connection alive.
#[tokio::test]
async fn cadence_bursts_beyond_the_outage_capacity_stay_connected() -> Result<(), BoxError> {
    use crate::zakura::regulation::test_family::{decode, message_type, Probe, RULES};

    const ALPN: &[u8] = b"/zakura/testkit/cadence-burst/0";
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(83).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(8);
    let (stream_tx, mut stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(84).await?;
    let stream = Stream {
        kind: 900,
        messages: Some(RULES),
        ..Stream::PERSISTENT
    };
    // The 22-frame control and larger bursts use fresh connection buckets.
    for count in [22u32, 23, 64] {
        let connection = timeout(
            Duration::from_secs(5),
            client.connect(LocalEndpointFactory::node_addr(&server).await, &[ALPN]),
        )
        .await??;
        let (mut sender, _receiver) =
            timeout(Duration::from_secs(2), connection.open_bi()).await??;
        let mut bytes = Vec::new();
        for value in 0..count {
            bytes.extend_from_slice(&message_type::STATUS.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            bytes.extend_from_slice(&4u32.to_le_bytes());
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        timeout(Duration::from_secs(2), sender.write_all(&bytes)).await??;
        let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let context = tabled_context(Some(RULES), 1);
        let cancel = context.connection_token.clone();
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        };
        let mut workers = JoinSet::new();
        let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
        let mut admitted = spawn_persistent_stream_worker(
            &mut workers,
            send,
            recv,
            stream,
            prelude,
            context,
            1,
            false,
            exit_tx,
        );
        for value in 0..count {
            let frame = timeout(Duration::from_secs(2), admitted.streams[0].recv.recv())
                .await?
                .expect("the reader forwards every buffered announcement");
            assert_eq!(decode(&frame), Probe::Status(value));
        }
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        timeout(Duration::from_secs(2), workers.join_next()).await?;
        connection.close(0u32.into(), b"done");
    }
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// The reader checks each response header against the attached
/// reservations and fails before it waits for the payload.
#[tokio::test]
async fn response_prechecks_apply_before_payload_reads() -> Result<(), BoxError> {
    use crate::zakura::regulation::{
        test_family::{message_type, RULES},
        ReservationPool, Reservations, ResponseCap, SharedReservations,
    };

    let stream = Stream {
        kind: 900,
        version: 1,
        frame_cap: 1024,
        capability: 1 << 48,
        messages: Some(RULES),
        ..Stream::PERSISTENT
    };
    let pool = ReservationPool::new(4)?;
    let shared = SharedReservations::new(Reservations::<u32>::new(RULES, 4));
    let filter = FrameFilter::new(stream.messages, InboundReader::Persistent);

    const ALPN: &[u8] = b"/zakura/testkit/response-precheck/0";
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(81).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(8);
    let (stream_tx, mut stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(82).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    // The capture takes one stream per connection, so each case connects.
    let read_header = |message_type: u16, payload_len: u32| {
        let (client, server_addr) = (client.clone(), server_addr.clone());
        async move {
            let connection =
                timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
            let (mut send, _recv) = timeout(Duration::from_secs(2), connection.open_bi()).await??;
            let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
            header.extend_from_slice(&message_type.to_le_bytes());
            header.extend_from_slice(&0u16.to_le_bytes());
            header.extend_from_slice(&payload_len.to_le_bytes());
            timeout(Duration::from_secs(2), send.write_all(&header)).await??;
            Ok::<_, BoxError>((connection, send))
        }
    };
    let rejected = |result: Result<Frame, ZakuraHandlerError>| match result {
        Err(ZakuraHandlerError::RejectedFrame { rejection, .. }) => Some(rejection),
        _ => None,
    };

    // No reservation: a response header is unsolicited.
    let _open = read_header(message_type::PART, 10).await?;
    let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    let result = timeout(
        Duration::from_secs(1),
        read_frame(
            &mut recv,
            stream.frame_cap,
            filter.with_precheck(Some(&shared)),
            Duration::from_secs(5),
            Some(Duration::from_secs(5)),
        ),
    )
    .await
    .expect("a refused header fails without waiting for payload bytes");
    assert_eq!(rejected(result), Some(FrameRejection::Unsolicited));

    // A reservation with ten bytes left: eleven are refused.
    shared.lock().reserve(
        1,
        message_type::GET,
        ResponseCap {
            frames: 1,
            bytes: 10,
        },
        pool.try_entry().unwrap(),
    )?;
    let _open = read_header(message_type::PART, 11).await?;
    let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    let result = timeout(
        Duration::from_secs(1),
        read_frame(
            &mut recv,
            stream.frame_cap,
            filter.with_precheck(Some(&shared)),
            Duration::from_secs(5),
            Some(Duration::from_secs(5)),
        ),
    )
    .await
    .expect("a refused header fails without waiting for payload bytes");
    assert_eq!(
        rejected(result),
        Some(FrameRejection::AboveReservation { bytes: 10 })
    );

    // A peer sends its response header before the service receives its
    // handle. The production reader must wait for precheck attachment.
    let _open = read_header(message_type::PART, 11).await?;
    let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    let mut context = tabled_context(Some(RULES), 128);
    context.limits.idle_timeout = Duration::from_secs(30);
    let cancel = context.connection_token.clone();
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: None,
        max_frame_bytes: stream.frame_cap,
    };
    let mut workers = JoinSet::new();
    let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
    let admitted = spawn_persistent_stream_worker(
        &mut workers,
        send,
        recv,
        stream,
        prelude,
        context,
        1,
        false,
        exit_tx,
    );
    // Give the independent reader time to consume the already sent header
    // if its startup gate is missing.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!cancel.is_cancelled());
    assert!(admitted.streams[0]
        .recv
        .attach_precheck(Arc::new(shared.clone())));
    timeout(Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("the attached precheck rejects the first header without its payload");
    timeout(Duration::from_secs(1), workers.join_next()).await?;

    // Legacy streams have no rows, but their request budget still rejects
    // a header before the peer supplies any payload bytes.
    let mut legacy = LegacyResponseReadState::new(
        LegacyResponseBudget::from_request(LEGACY_REQUEST_PING, &[], test_connection_limits())
            .unwrap(),
    );
    for (payload_len, expected) in [
        (9, FrameRejection::AboveReservation { bytes: 8 }),
        (0, FrameRejection::Unsolicited),
    ] {
        let _open = read_header(LEGACY_RESPONSE_PONG, payload_len).await?;
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            read_frame(
                &mut recv,
                stream.frame_cap,
                FrameFilter::new(None, InboundReader::Requester).with_precheck(Some(&legacy)),
                Duration::from_secs(5),
                Some(Duration::from_secs(5)),
            ),
        )
        .await
        .expect("the legacy budget rejects a header without reading its payload");
        assert_eq!(rejected(result), Some(expected));
        if legacy.frames == 0 {
            legacy
                .validate_frame(
                    1,
                    Frame {
                        message_type: LEGACY_RESPONSE_PONG,
                        flags: 0,
                        payload: 1u64.to_le_bytes().to_vec(),
                    },
                )
                .expect("the first pong fits its request budget");
        }
    }

    // Requests are not responses: the precheck does not apply.
    let (_connection, mut send) = read_header(message_type::GET, 4).await?;
    timeout(Duration::from_secs(2), send.write_all(&[0; 4])).await??;
    let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    let frame = timeout(
        Duration::from_secs(2),
        read_frame(
            &mut recv,
            stream.frame_cap,
            filter.with_precheck(Some(&shared)),
            Duration::from_secs(5),
            Some(Duration::from_secs(5)),
        ),
    )
    .await??;
    assert_eq!(frame.message_type, message_type::GET);
    Ok(())
}

/// Stream 6 queues a fixed number of frames for its routine, however much
/// the live reservations owe. The precheck here admits every body, as a
/// request window of any size would, and the peer sends full-size bodies
/// at the largest negotiable transport depth. The reader still holds only
/// the declared depth plus the frame it waits to forward. The rest waits
/// in QUIC flow control. When the routine takes a body and holds it, the
/// reader admits exactly one more, so the session never holds more than
/// the depth plus two.
#[tokio::test]
async fn block_sync_queues_a_fixed_number_of_bodies_whatever_the_reservations_owe(
) -> Result<(), BoxError> {
    use crate::zakura::{
        block_sync_streams, regulation::ResponsePrecheck, LOCAL_MAX_INBOUND_QUEUE_DEPTH,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Admits every response header and counts what the reader admitted.
    #[derive(Debug, Default)]
    struct OwesEverything {
        frames: AtomicUsize,
        bytes: AtomicUsize,
    }

    impl ResponsePrecheck for OwesEverything {
        fn check(&self, _message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
            self.frames.fetch_add(1, Ordering::SeqCst);
            self.bytes.fetch_add(payload_len, Ordering::SeqCst);
            Ok(())
        }
    }

    // The largest `Block` payload stream 6 accepts: its tag and a
    // 2,000,000-byte body.
    const BODY_PAYLOAD: usize = 2_000_001;
    const SENT: usize = 32;
    let stream = block_sync_streams()[0];
    let depth = stream
        .queue_depths
        .expect("stream 6 declares its inbound depth")
        .inbound;
    let held = depth + 1;
    // The resource ledger's raw-frame term counts these and the routine's
    // body: about 20 MB per session.
    assert!((held + 1) * BODY_PAYLOAD < 21_000_000);
    assert!(held < SENT, "the peer sends more than the queue holds");

    const ALPN: &[u8] = b"/zakura/testkit/block-sync-queue-depth/0";
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(83).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(8);
    let (stream_tx, mut stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(84).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;
    let connection =
        timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
    let (mut peer_send, _peer_recv) =
        timeout(Duration::from_secs(2), connection.open_bi()).await??;
    let writer = tokio::spawn(async move {
        let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + BODY_PAYLOAD);
        frame.extend_from_slice(&3u16.to_le_bytes());
        frame.extend_from_slice(&0u16.to_le_bytes());
        frame.extend_from_slice(&u32::try_from(BODY_PAYLOAD)?.to_le_bytes());
        frame.push(3);
        frame.resize(FRAME_HEADER_BYTES + BODY_PAYLOAD, 0);
        for _ in 0..SENT {
            peer_send.write_all(&frame).await?;
        }
        Ok::<_, BoxError>(peer_send)
    });
    let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();

    let mut context = tabled_context(stream.messages, 1024);
    context.limits.idle_timeout = Duration::from_secs(30);
    context.inbound_frame_cap = stream.frame_cap;
    context.queue_depths = stream.queue_depths;
    let cancel = context.connection_token.clone();
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: None,
        max_frame_bytes: stream.frame_cap,
    };
    let mut workers = JoinSet::new();
    let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
    let mut admitted = spawn_persistent_stream_worker(
        &mut workers,
        send,
        recv,
        stream,
        prelude,
        context,
        usize::from(LOCAL_MAX_INBOUND_QUEUE_DEPTH),
        false,
        exit_tx,
    );
    let precheck = Arc::new(OwesEverything::default());
    assert!(admitted.streams[0].recv.attach_precheck(precheck.clone()));

    let admitted_frames = || precheck.frames.load(Ordering::SeqCst);
    await_until(
        "the reader fills its queue",
        Duration::from_secs(10),
        || admitted_frames() >= held,
    )
    .await?;
    // Give the reader time to read past its bound if it could.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(admitted_frames(), held);
    assert_eq!(precheck.bytes.load(Ordering::SeqCst), held * BODY_PAYLOAD);
    assert!(!cancel.is_cancelled(), "a full queue is backpressure");

    // The routine takes one body and keeps it, as it does while it waits
    // for decode capacity. The reader admits exactly one more.
    let frame = timeout(Duration::from_secs(2), admitted.streams[0].recv.recv())
        .await?
        .expect("the reader forwarded the first body");
    assert_eq!(frame.payload.len(), BODY_PAYLOAD);
    await_until(
        "the reader refills one slot",
        Duration::from_secs(10),
        || admitted_frames() > held,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(admitted_frames(), held + 1);
    assert!(!cancel.is_cancelled());

    writer.abort();
    cancel.cancel();
    timeout(Duration::from_secs(2), workers.join_next()).await?;
    connection.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn message_tables_apply_before_payload_reads() -> Result<(), BoxError> {
    use crate::zakura::{block_sync_streams, MessageRole, PayloadLen};

    const REQUEST: MessageRule = MessageRule {
        name: "request",
        message_type: 2,
        payload: PayloadLen::exact(9),
        role: MessageRole::Request {
            max_in_flight: 1,
            cadence: None,
        },
    };
    const RESPONSE: MessageRule = MessageRule {
        name: "response",
        message_type: 3,
        payload: PayloadLen::between(1, 99),
        role: MessageRole::Response {
            request: REQUEST.message_type,
            ends_exchange: true,
        },
    };
    let stream = Stream {
        messages: Some(&[REQUEST, RESPONSE]),
        ..block_sync_streams()[0]
    };
    Stream::check_layout(&[stream])?;
    let filter = FrameFilter::new(stream.messages, InboundReader::Persistent);

    const ALPN: &[u8] = b"/zakura/testkit/message-tables/0";
    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(79).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(8);
    let (stream_tx, mut stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(80).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    // Only send headers and keep the send sides open. The reader must reject
    // each one before waiting for a payload that the peer has not supplied.
    let oversize = |cap| move |result: &Result<Frame, ZakuraHandlerError>| matches!(result, Err(ZakuraHandlerError::OversizeFrame { max_frame_bytes, .. }) if *max_frame_bytes == cap);
    let rejected = |expected| move |result: &Result<Frame, ZakuraHandlerError>| matches!(result, Err(ZakuraHandlerError::RejectedFrame { rejection, .. }) if *rejection == expected);
    let cases: [(
        u16,
        u16,
        u32,
        u32,
        Box<dyn Fn(&Result<Frame, ZakuraHandlerError>) -> bool>,
    ); 7] = [
        // Eight frame-header bytes plus the request row's nine-byte maximum.
        (2, 0, 10, stream.frame_cap, Box::new(oversize(17))),
        (2, 0, u32::MAX, stream.frame_cap, Box::new(oversize(17))),
        // A tighter negotiated cap still applies to a legal request.
        (2, 0, 9, 16, Box::new(oversize(16))),
        (3, 0, 100, stream.frame_cap, Box::new(oversize(107))),
        (
            7,
            0,
            1,
            stream.frame_cap,
            Box::new(rejected(FrameRejection::UnknownMessageType)),
        ),
        (
            2,
            1,
            9,
            stream.frame_cap,
            Box::new(rejected(FrameRejection::ReservedFlags)),
        ),
        (
            2,
            0,
            8,
            stream.frame_cap,
            Box::new(rejected(FrameRejection::PayloadTooShort { min: 9 })),
        ),
    ];
    for (message_type, flags, payload_len, frame_cap, expected) in cases {
        let connection = timeout(
            Duration::from_secs(5),
            client.connect(server_addr.clone(), &[ALPN]),
        )
        .await??;
        let (mut send, _recv) = timeout(Duration::from_secs(2), connection.open_bi()).await??;
        let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
        header.extend_from_slice(&message_type.to_le_bytes());
        header.extend_from_slice(&flags.to_le_bytes());
        header.extend_from_slice(&payload_len.to_le_bytes());
        timeout(Duration::from_secs(2), send.write_all(&header)).await??;
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            read_frame(
                &mut recv,
                frame_cap,
                filter,
                Duration::from_secs(5),
                Some(Duration::from_secs(5)),
            ),
        )
        .await
        .expect("a rejected header fails without waiting for payload bytes");
        assert!(
            expected(&result),
            "type {message_type} flags {flags} length {payload_len}: {result:?}"
        );
    }

    // Frames within their rows read normally, at both bounds.
    let connection =
        timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
    let (mut send, _recv) = timeout(Duration::from_secs(2), connection.open_bi()).await??;
    let frames = [
        Frame {
            message_type: 2,
            flags: 0,
            payload: vec![7; 9],
        },
        Frame {
            message_type: 3,
            flags: 0,
            payload: vec![8; 1],
        },
        Frame {
            message_type: 3,
            flags: 0,
            payload: vec![9; 99],
        },
    ];
    for frame in &frames {
        timeout(
            Duration::from_secs(2),
            send.write_all(&frame.encode(stream.frame_cap)?),
        )
        .await??;
    }
    let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
        .await?
        .unwrap();
    for expected in frames {
        let frame = timeout(
            Duration::from_secs(2),
            read_frame(
                &mut recv,
                stream.frame_cap,
                filter,
                Duration::from_secs(2),
                Some(Duration::from_secs(2)),
            ),
        )
        .await??;
        assert_eq!(frame, expected);
    }
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

// claude-late-message-cap-allocation: read_frame checks only
// frame_len > max_frame_bytes before `vec![0; payload_len]`, while the smaller
// max_message_bytes is enforced later in admit_inbound_message. A peer can
// negotiate max_frame_bytes > max_message_bytes (the caps are clamped
// independently), so on every admitted ordered/request stream it could force
// read_frame to allocate and read a payload between the two limits before the
// message cap rejects it. The inbound read path must instead be handed a cap
// already limited to the message size, so an over-message frame is rejected on
// its header alone, before the payload is allocated and read.
#[tokio::test]
async fn inbound_frame_cap_rejects_over_message_frame_before_reading_payload(
) -> Result<(), BoxError> {
    const ALPN: &[u8] = b"/zakura/testkit/late-message-cap/0";
    // The negotiated frame cap is far larger than the message cap: exactly the
    // precondition the finding requires (caps allowed to diverge).
    const MAX_FRAME_BYTES: u32 = 64 * 1024;
    const MAX_MESSAGE_BYTES: u32 = 1024;
    // A payload between the message cap and the frame cap. admit_inbound_message
    // would reject it, but only after read_frame allocated and read it.
    const OVER_MESSAGE_PAYLOAD_LEN: u32 = 2048;
    let stream = Stream {
        kind: LEGACY_GOSSIP_STREAM_KIND,
        version: ZAKURA_STREAM_VERSION_1,
        frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
        capability: ZAKURA_CAP_LEGACY_GOSSIP,
        ..Stream::PERSISTENT
    };

    let limits = ZakuraConnectionLimits {
        max_frame_bytes: MAX_FRAME_BYTES,
        max_message_bytes: MAX_MESSAGE_BYTES,
        ..test_connection_limits()
    };
    // Production now passes the message-limited inbound cap; the raw
    // application cap (what the unfixed read path used) stays at the frame cap.
    let inbound_cap = inbound_frame_cap_for_stream(&limits, stream);
    let raw_cap = application_frame_cap(&limits, stream);
    assert!(
        inbound_cap < raw_cap,
        "the inbound cap must be tighter than the raw frame cap when the caps diverge \
             (inbound_cap={inbound_cap}, raw_cap={raw_cap})"
    );

    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(72).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(2);
    let (stream_tx, mut stream_rx) = mpsc::channel(4);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(73).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    // A frame header (message_type, flags, payload_len) with no payload bytes.
    let frame_header = |payload_len: u32| -> Vec<u8> {
        let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&payload_len.to_le_bytes());
        header
    };

    // CaptureConnection forwards at most two streams per connection, so the
    // two oversized streams share one connection and the in-cap stream uses
    // another.
    let conn_a = timeout(
        Duration::from_secs(10),
        client.connect(server_addr.clone(), &[ALPN]),
    )
    .await
    .expect("client connects for the oversized streams")?;

    // Stream 1: oversized header read with the message-limited inbound cap.
    // The fix rejects it as Oversize from the header alone, before allocating.
    let (mut over_send, _over_recv) = timeout(Duration::from_secs(1), conn_a.open_bi())
        .await
        .expect("client opens the oversized inbound-cap stream")?;
    timeout(
        Duration::from_secs(1),
        over_send.write_all(&frame_header(OVER_MESSAGE_PAYLOAD_LEN)),
    )
    .await
    .expect("client writes the oversized frame header")?;
    let _ = over_send.finish();
    let (_s1_send, mut s1_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the oversized inbound-cap stream")
        .expect("capture handler forwards the oversized inbound-cap stream");
    let rejected = read_frame(
        &mut s1_recv,
        inbound_cap,
        FrameFilter::new(None, InboundReader::Persistent),
        Duration::from_secs(2),
        Some(Duration::from_secs(2)),
    )
    .await;
    assert!(
        matches!(rejected, Err(ZakuraHandlerError::OversizeFrame { .. })),
        "a frame whose payload exceeds max_message_bytes must be rejected as Oversize \
             on the header alone with the inbound (message-limited) cap, before the payload \
             is allocated and read; got {rejected:?}"
    );

    // Stream 2: the SAME oversized header read with the raw frame cap an
    // unfixed path used. It passes the frame-cap size check, so read_frame
    // allocates `vec![0; payload_len]` and reads the body (failing only because
    // the body was never sent) -- i.e. NOT rejected as Oversize. This is the
    // allocate-before-reject amplification the fix removes.
    let (mut raw_send, _raw_recv) = timeout(Duration::from_secs(1), conn_a.open_bi())
        .await
        .expect("client opens the oversized raw-cap stream")?;
    timeout(
        Duration::from_secs(1),
        raw_send.write_all(&frame_header(OVER_MESSAGE_PAYLOAD_LEN)),
    )
    .await
    .expect("client writes the oversized frame header again")?;
    let _ = raw_send.finish();
    let (_s2_send, mut s2_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the oversized raw-cap stream")
        .expect("capture handler forwards the oversized raw-cap stream");
    let allocated = read_frame(
        &mut s2_recv,
        raw_cap,
        FrameFilter::new(None, InboundReader::Persistent),
        Duration::from_secs(2),
        Some(Duration::from_secs(2)),
    )
    .await;
    assert!(
        allocated.is_err()
            && !matches!(
                allocated,
                Err(ZakuraHandlerError::Oversize | ZakuraHandlerError::OversizeFrame { .. })
            ),
        "with the raw frame cap the same oversized frame passes the size check and \
             read_frame proceeds to allocate/read the payload (it is not rejected as \
             Oversize), proving the message cap is enforced too late; got {allocated:?}"
    );

    // Stream 3 (fresh connection): a frame within the message cap must still
    // round-trip with the inbound cap, so the clamp rejects nothing legitimate.
    let conn_b = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects for the in-cap stream")?;
    let valid_payload = vec![0x5au8; (MAX_MESSAGE_BYTES / 2) as usize];
    let mut valid_bytes =
        frame_header(u32::try_from(valid_payload.len()).expect("in-cap payload len fits u32"));
    valid_bytes.extend_from_slice(&valid_payload);
    let (mut ok_send, _ok_recv) = timeout(Duration::from_secs(1), conn_b.open_bi())
        .await
        .expect("client opens the in-cap stream")?;
    timeout(Duration::from_secs(1), ok_send.write_all(&valid_bytes))
        .await
        .expect("client writes the in-cap frame")?;
    let _ = ok_send.finish();
    let (_s3_send, mut s3_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the in-cap stream")
        .expect("capture handler forwards the in-cap stream");
    let frame = read_frame(
        &mut s3_recv,
        inbound_cap,
        FrameFilter::new(None, InboundReader::Persistent),
        Duration::from_secs(2),
        Some(Duration::from_secs(2)),
    )
    .await
    .expect("a frame within the message cap is read with the inbound cap");
    assert_eq!(
        frame.payload, valid_payload,
        "an in-cap frame payload must round-trip unchanged"
    );

    conn_a.close(0u32.into(), b"done");
    conn_b.close(0u32.into(), b"done");
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn invalid_prelude_stream_churn_charges_open_rate_token() -> Result<(), BoxError> {
    // claude-invalid-stream-prelude-churn-bypasses-open-rate: a peer that
    // opens a stream naming an unregistered kind is reset stream-only and the
    // connection is kept. The per-connection stream-open rate token MUST be
    // charged for that protocol-invalid open; otherwise an authenticated peer
    // could churn bad-prelude/unknown-kind/unnegotiated stream opens forever
    // without ever spending open-rate budget. This drives admit_bi_stream
    // over a real QUIC transport and asserts the token was consumed even
    // though the stream itself was rejected as unknown-kind.
    const ALPN: &[u8] = b"/zakura/testkit/open-rate-churn/0";

    let _guard = zakura_test::init();
    let server = LocalEndpointFactory::new().endpoint(90).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    server.serve(CaptureConnection {
        alpn: ALPN,
        connection_tx: conn_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(91).await?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;

    let client_conn = timeout(
        Duration::from_secs(10),
        client.connect(server_addr, &[ALPN]),
    )
    .await
    .expect("client connects to the open-rate-churn endpoint")?;

    // A well-formed prelude that names an unregistered stream kind. It parses
    // cleanly, so admission reaches the unknown-kind reject (a stream-only
    // reset that keeps the connection) rather than the bad-prelude path.
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: 9,
        stream_version: 1,
        request_id: None,
        max_frame_bytes: 1024,
    };
    let (mut client_send, _client_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
        .await
        .expect("client opens the unknown-kind stream")?;
    timeout(
        Duration::from_secs(1),
        client_send.write_all(&prelude.encode()?),
    )
    .await
    .expect("client writes the unknown-kind prelude")?;
    let _ = client_send.finish();

    let (server_send, server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
        .await
        .expect("server accepts the unknown-kind stream")
        .expect("capture handler forwards the unknown-kind stream");

    let supervisor = ZakuraSupervisorHandle::new(16);
    let handler = ZakuraProtocolHandler::new(
        supervisor,
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    );

    let peer_id = test_peer(9);
    let stream_sem = Arc::new(Semaphore::new(16));
    // A full bucket with a known capacity so the token charge is observable
    // as an exact decrement.
    let mut open_limiter = TokenBucket::new(4);
    let mut message_buckets = MessageRateBuckets::new();
    let mut workers = JoinSet::new();
    let connection_token = CancellationToken::new();
    let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());

    let mut admission = StreamAdmission {
        is_initiator: false,
        direction: ServicePeerDirection::Inbound,
        conn: ZakuraConnTrace::placeholder(),
        peer_id: &peer_id,
        stream_sem: &stream_sem,
        resources: &ConnectionResources::unconstrained(&[]),
        open_limiter: &mut open_limiter,
        message_buckets: &mut message_buckets,
        workers: &mut workers,
        limits: test_connection_limits(),
        accepted_capabilities: 0,
        connection_token: connection_token.clone(),
        close_cause: CloseCause::new(),
        freshness_tx,
    };
    let (session_exit_tx, _session_exit_rx) = mpsc::unbounded_channel();
    let admitted = handler
        .admit_bi_stream(
            server_send,
            server_recv,
            &mut admission,
            16,
            session_exit_tx,
            &mut PendingSessions::default(),
        )
        .await;

    assert!(
        admitted.is_none(),
        "an unknown-kind stream must be rejected, not admitted"
    );
    assert!(
        !connection_token.is_cancelled(),
        "an unknown-kind stream is reset stream-only and must keep the connection alive"
    );
    assert_eq!(
        admission.open_limiter.tokens, 3,
        "the protocol-invalid stream open must spend exactly one open-rate token \
             (capacity 4 -> 3); before the fix the unknown-kind reject returned before \
             reaching the limiter, leaving the bucket full at 4"
    );

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[test]
fn local_application_frame_cap_admits_default_header_sync_response() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let default_header_sync_frame_bytes =
        u32::try_from(MAX_HS_MESSAGE_BYTES.saturating_add(FRAME_HEADER_BYTES))
            .expect("header-sync frame cap fits in u32");

    assert!(limits.max_frame_bytes >= default_header_sync_frame_bytes);
    assert!(limits.initial_limits().max_frame_bytes >= default_header_sync_frame_bytes);
}

#[test]
fn ordered_stream_stopped_write_is_stream_local() {
    let error: BoxError = zakura_quic::WriteError::Stopped(VarInt::from_u32(0)).into();

    assert!(ordered_stream_write_was_stopped(&error));
}

#[test]
fn declared_stream_frame_caps_are_authoritative() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let negotiated = limits.clamp(&ZakuraAcceptedLimits {
        max_frame_bytes: u32::MAX,
        max_message_bytes: u32::MAX,
        max_open_streams: u16::MAX,
        max_inbound_queue_depth: u16::MAX,
        idle_timeout_millis: u32::MAX,
    });
    const CUSTOM_FRAME_CAP: u32 = 64 * 1024;
    let custom_stream = Stream {
        kind: 42,
        version: ZAKURA_STREAM_VERSION_1,
        frame_cap: CUSTOM_FRAME_CAP,
        capability: 1 << 20,
        ..Stream::REQUEST_RESPONSE
    };

    assert_eq!(
        application_frame_cap(&negotiated, custom_stream),
        CUSTOM_FRAME_CAP,
        "a custom declaration below the negotiated cap must remain authoritative"
    );
    assert_eq!(
        peer_accepted_frame_cap(&negotiated, custom_stream, CUSTOM_FRAME_CAP / 2),
        CUSTOM_FRAME_CAP / 2,
        "writes on peer-opened streams must also honor the peer's advertised receive cap"
    );
    assert_eq!(
        peer_accepted_frame_cap(&negotiated, custom_stream, CUSTOM_FRAME_CAP * 2),
        CUSTOM_FRAME_CAP,
        "a peer cannot raise the locally declared stream cap"
    );

    let at_cap = Frame {
        message_type: 1,
        flags: 0,
        payload: vec![
            0;
            usize::try_from(CUSTOM_FRAME_CAP)
                .expect("custom test cap fits usize")
                .saturating_sub(FRAME_HEADER_BYTES)
        ],
    };
    let over_cap = Frame {
        message_type: 1,
        flags: 0,
        payload: vec![0; at_cap.payload.len() + 1],
    };

    assert!(
        at_cap
            .encode(application_frame_cap(&negotiated, custom_stream))
            .is_ok(),
        "a frame exactly at the declared custom cap must be accepted"
    );
    assert!(
        over_cap
            .encode(application_frame_cap(&negotiated, custom_stream))
            .is_err(),
        "a frame one byte over the declared custom cap must be rejected"
    );
}

#[test]
fn block_sync_needs_two_slots_per_ordered_stream() {
    let block_sync = crate::zakura::block_sync_streams()[0];
    let other = Stream {
        kind: 5,
        messages: None,
        ..block_sync
    };
    assert_eq!(minimum_ordered_queue_capacity(&[other]), 1);
    assert_eq!(minimum_ordered_queue_capacity(&[block_sync]), 2);
    assert_eq!(minimum_ordered_queue_capacity(&[block_sync, other]), 4);
}

#[test]
fn token_bucket_rejects_churn_until_refill() {
    let clock = crate::zakura::testkit::TestClock::new();
    let mut bucket = TokenBucket::with_clock(2, clock.clone());

    assert!(bucket.try_take());
    assert!(bucket.try_take());
    assert!(!bucket.try_take());
    clock.advance(Duration::from_millis(500));
    assert!(bucket.try_take());
    assert!(!bucket.try_take());
    clock.advance(Duration::from_millis(500));
    assert!(bucket.try_take());
}

#[test]
fn supported_stream_accepts_registered_kinds_at_declared_version_only() {
    let streams = vec![
        Stream {
            kind: LEGACY_GOSSIP_STREAM_KIND,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: 1024,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::PERSISTENT
        },
        Stream {
            kind: LEGACY_REQUEST_STREAM_KIND,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: 1024,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::REQUEST_RESPONSE
        },
        Stream {
            kind: DISCOVERY_STREAM_KIND,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: 1024,
            capability: ZAKURA_CAP_DISCOVERY,
            ..Stream::PERSISTENT
        },
        Stream {
            kind: HEADER_SYNC_STREAM_KIND,
            version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
            frame_cap: 1024,
            capability: ZAKURA_CAP_HEADER_SYNC,
            ..Stream::PERSISTENT
        },
        Stream {
            kind: ZAKURA_STREAM_BLOCK_SYNC,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: MAX_BS_FRAME_BYTES,
            capability: crate::zakura::ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ..Stream::PERSISTENT
        },
    ];
    let services = streams
        .into_iter()
        .map(|stream| -> Arc<dyn Service> {
            Arc::new(DeclaredStreamService {
                streams: vec![stream],
            })
        })
        .collect();
    let registry =
        ServiceRegistry::new(services).expect("test registry declares unique stream kinds");

    for (kind, version) in [
        (LEGACY_GOSSIP_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
        (LEGACY_REQUEST_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
        (DISCOVERY_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
        (HEADER_SYNC_STREAM_KIND, ZAKURA_HEADER_SYNC_STREAM_VERSION),
        (ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_VERSION_1),
    ] {
        assert!(
            is_supported_stream(&registry, kind, version),
            "registered kind {kind} at declared version {version} must be supported"
        );
        assert!(
            !is_supported_stream(&registry, kind, 0),
            "registered kind {kind} at version 0 must be rejected"
        );
        assert!(
            !is_supported_stream(&registry, kind, u16::MAX),
            "registered kind {kind} at an unsupported version must be rejected"
        );
    }

    assert!(
        !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
        "header-sync v1 is rejected because native header sync uses exact version matching"
    );
    assert!(
        !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, 5),
        "header-sync v5 is rejected after the expanded Ironwood root-record wire break"
    );
    assert!(
        !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, 8),
        "the predecessor header-sync stream version is rejected"
    );
    for retired in [1 << 4, 1 << 5] {
        assert!(
            registry
                .persistent_streams_for_negotiated(retired)
                .is_empty(),
            "a retired header-sync capability opens no header-sync stream"
        );
    }

    assert_eq!(stream_kind_label(2), "gossip");
    assert_eq!(stream_kind_label(3), "legacy_request");
    assert_eq!(stream_kind_label(4), "discovery");
    assert_eq!(stream_kind_label(5), "header_sync");
    assert_eq!(stream_kind_label(6), "block_sync");

    for kind in [0u16, 1, 7, 255, u16::MAX] {
        assert!(
            !is_supported_stream(&registry, kind, ZAKURA_STREAM_VERSION_1),
            "unknown kind {kind} must be rejected even at version 1"
        );
    }

    for kind in [7u16, 255, u16::MAX] {
        assert_eq!(stream_kind_label(kind), "unknown");
    }
}

#[test]
fn legacy_transport_response_validator_accepts_codec_frames() -> Result<(), BoxError> {
    let block = Arc::new(Block::zcash_deserialize(
        BLOCK_TESTNET_141042_BYTES.as_slice(),
    )?);
    let header = block::CountedHeader {
        header: block.header.clone(),
    };
    let tx_id = legacy_tx_id(7);

    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::BlocksByHash(vec![block.hash()]),
        LegacyRequestKind::Blocks,
        Response::Blocks(vec![InventoryResponse::Available((block.into(), None))]),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::TransactionsById(vec![tx_id]),
        LegacyRequestKind::Transactions,
        Response::Transactions(vec![InventoryResponse::Missing(tx_id)]),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::FindBlocks {
            known_blocks: vec![block_hash(1)],
            stop: None,
        },
        LegacyRequestKind::FindBlocks,
        Response::BlockHashes(vec![block_hash(2)]),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::FindHeaders {
            known_blocks: vec![block_hash(1)],
            stop: None,
        },
        LegacyRequestKind::FindHeaders,
        Response::BlockHeaders(vec![header.header.clone().into()]),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::MempoolTransactionIds,
        LegacyRequestKind::MempoolTransactionIds,
        Response::TransactionIds(vec![tx_id]),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::Ping,
        LegacyRequestKind::Ping,
        Response::Pong(Duration::ZERO),
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::PushTransaction(empty_v5_transaction(8).into()),
        LegacyRequestKind::PushTransaction,
        Response::Nil,
    )?;

    // The inbound service answers an empty FindBlocks/FindHeaders/
    // MempoolTransactionIds with `Response::Nil`, so a lone nil frame must
    // round-trip (encode -> transport validate -> decode) for those data
    // request kinds too, not only PushTransaction.
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::FindBlocks {
            known_blocks: vec![block_hash(1)],
            stop: None,
        },
        LegacyRequestKind::FindBlocks,
        Response::Nil,
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::FindHeaders {
            known_blocks: vec![block_hash(1)],
            stop: None,
        },
        LegacyRequestKind::FindHeaders,
        Response::Nil,
    )?;
    assert_codec_frames_validate_at_transport(
        LegacyRequestFrame::MempoolTransactionIds,
        LegacyRequestKind::MempoolTransactionIds,
        Response::Nil,
    )?;

    Ok(())
}

/// Completed chunks hand their decoded allocation to the response codec,
/// while requested-inventory binding still runs at response assembly.
#[test]
fn legacy_transport_reuses_decoded_inventory() -> Result<(), BoxError> {
    use zakura_node_services::block_memory::BlockHandle;

    let limits = test_connection_limits();
    let request_id = 99;
    let block = Arc::new(Block::zcash_deserialize(
        BLOCK_TESTNET_141042_BYTES.as_slice(),
    )?);
    let transaction = transaction::UnminedTx::from(block.transactions[0].clone());
    let missing_hash = block_hash(99);
    let missing_id = legacy_tx_id(99);
    let cases = [
        (
            LegacyRequestFrame::BlocksByHash(vec![block.hash(), missing_hash]),
            LegacyRequestKind::Blocks,
            Response::Blocks(vec![
                InventoryResponse::Available((block.clone().into(), None)),
                InventoryResponse::Missing(missing_hash),
            ]),
            RequestedInventory::Blocks([block.hash(), missing_hash].into_iter().collect()),
        ),
        (
            LegacyRequestFrame::TransactionsById(vec![transaction.id(), missing_id]),
            LegacyRequestKind::Transactions,
            Response::Transactions(vec![
                InventoryResponse::Available((transaction.clone(), None)),
                InventoryResponse::Missing(missing_id),
            ]),
            RequestedInventory::Transactions([transaction.id(), missing_id].into_iter().collect()),
        ),
    ];

    for (request, kind, response, requested) in cases {
        let request = request.encode_frame()?;
        let budget =
            LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        let item_bytes = match kind {
            LegacyRequestKind::Blocks => BLOCK_TESTNET_141042_BYTES.len(),
            LegacyRequestKind::Transactions => transaction.size(),
            _ => unreachable!("the cases only fetch inventory"),
        };
        let frame_cap = u32::try_from(
            FRAME_HEADER_BYTES + LEGACY_RESPONSE_CHUNK_HEADER_BYTES + item_bytes.div_ceil(2),
        )?;
        let frames = LegacyResponseCodec::encode_response(
            request_id,
            response.clone(),
            frame_cap,
            limits.max_message_bytes,
        )?;
        assert!(frames.len() > 2, "the available item spans multiple chunks");
        let mut state = LegacyResponseReadState::new(budget);
        let mut items = Vec::new();
        for frame in frames {
            if let Some(item) = state
                .validate_frame(request_id, frame)
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?
            {
                items.push(item);
            }
        }
        state
            .finish()
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        assert_eq!(
            items.len(),
            2,
            "raw chunks are replaced by one decoded item"
        );

        let decoded = match &items[0] {
            LegacyResponseItem::Block(block) => LegacyResponseItem::Block(block.clone()),
            LegacyResponseItem::Transaction(transaction) => {
                LegacyResponseItem::Transaction(transaction.clone())
            }
            LegacyResponseItem::Frame(_) => panic!("the reader decoded the available item"),
        };
        let unrelated = match kind {
            LegacyRequestKind::Blocks => RequestedInventory::Blocks(Default::default()),
            LegacyRequestKind::Transactions => RequestedInventory::Transactions(Default::default()),
            _ => unreachable!("the cases only fetch inventory"),
        };
        assert!(matches!(
            LegacyResponseCodec::decode_response(request_id, kind, [decoded], Some(&unrelated),),
            Err(LegacyGossipError::UnsolicitedBlock(_)
                | LegacyGossipError::UnsolicitedTransaction(_))
        ));

        // Keep a handle only to verify that assembly reuses the allocation.
        let (decoded_block, decoded_transaction) = match &items[0] {
            LegacyResponseItem::Block(block) => (Some(block.clone()), None),
            LegacyResponseItem::Transaction(transaction) => {
                (None, Some(transaction.transaction().clone()))
            }
            LegacyResponseItem::Frame(_) => unreachable!("the item is decoded"),
        };
        let assembled =
            LegacyResponseCodec::decode_response(request_id, kind, items, Some(&requested))?;
        assert_eq!(assembled, response);
        match assembled {
            Response::Blocks(blocks) => {
                let InventoryResponse::Available((block, None)) = &blocks[0] else {
                    panic!("the available block remains shared");
                };
                let BlockHandle::Shared(block) = &**block else {
                    panic!("decoded block is shared");
                };
                assert!(Arc::ptr_eq(block, &decoded_block.unwrap()));
            }
            Response::Transactions(transactions) => {
                let InventoryResponse::Available((transaction, None)) = &transactions[0] else {
                    panic!("the first transaction is available");
                };
                assert!(Arc::ptr_eq(
                    transaction.transaction(),
                    &decoded_transaction.unwrap(),
                ));
            }
            _ => unreachable!("the cases only fetch inventory"),
        }
    }
    Ok(())
}

/// A block response whose stream ends mid-item is request-local, but a list
/// frame interleaved into the unfinished item stays connection-fatal.
#[test]
fn stream_ending_mid_item_is_local_and_interleaving_is_fatal() {
    let request_id: u64 = 7;
    let state = || {
        LegacyResponseReadState::new(LegacyResponseBudget {
            kind: LegacyResponseKind::Blocks,
            max_items: 1,
            max_frames: 4,
            max_bytes: MAX_PROTOCOL_MESSAGE_LEN,
            max_message_bytes: MAX_PROTOCOL_MESSAGE_LEN,
        })
    };
    let mut payload = request_id.to_le_bytes().to_vec();
    payload.push(0);
    payload.extend_from_slice(&[0; 16]);
    let first_chunk = Frame {
        message_type: LEGACY_RESPONSE_BLOCK,
        flags: 0,
        payload,
    };

    let mut ended = state();
    ended
        .validate_frame(request_id, first_chunk.clone())
        .expect("a non-final chunk is accepted");
    let finished = ended.finish();
    assert!(
        matches!(finished, Err(OutboundRequestError::Local(_))),
        "a stream ending mid-item must be Local; got {finished:?}"
    );

    let mut interleaved = state();
    interleaved
        .validate_frame(request_id, first_chunk.clone())
        .expect("a non-final chunk is accepted");
    let mut missing = request_id.to_le_bytes().to_vec();
    missing.push(0);
    let missing = Frame {
        message_type: LEGACY_RESPONSE_MISSING_BLOCKS,
        flags: 0,
        payload: missing,
    };
    let result = interleaved.validate_frame(request_id, missing.clone());
    assert!(
        matches!(result, Err(OutboundRequestError::Fatal(_))),
        "a frame interleaved into an unfinished item must stay Fatal; got {result:?}"
    );
}

#[test]
fn legacy_header_precheck_tracks_remaining_bytes_across_frames() {
    let mut state = LegacyResponseReadState::new(LegacyResponseBudget {
        kind: LegacyResponseKind::Blocks,
        max_items: 1,
        max_frames: 10,
        max_bytes: 18,
        max_message_bytes: 1024,
    });
    let mut payload = 1u64.to_le_bytes().to_vec();
    payload.push(0); // An empty missing-block inventory.
    let frame = Frame {
        message_type: LEGACY_RESPONSE_MISSING_BLOCKS,
        flags: 0,
        payload,
    };
    for remaining in [18, 9, 0] {
        let filter = FrameFilter::new(None, InboundReader::Requester).with_precheck(Some(&state));
        assert_eq!(
            filter.check_header(frame.message_type, 0, remaining, 1024),
            Ok(1024),
        );
        assert_eq!(
            filter.check_header(frame.message_type, 0, remaining + 1, 1024),
            Err(FrameRejection::AboveReservation {
                bytes: u64::try_from(remaining).unwrap(),
            }),
        );
        if remaining != 0 {
            state.validate_frame(1, frame.clone()).unwrap();
        }
    }
}

/// The reader rejects trailing bytes before handing off decoded inventory.
#[test]
fn legacy_completed_item_rejects_trailing_bytes() -> Result<(), BoxError> {
    let limits = test_connection_limits();
    let request_id = 99;
    let transaction = transaction::UnminedTx::from(empty_v5_transaction(24));
    let block = Arc::new(Block::zcash_deserialize(
        BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
    )?);
    let cases = [
        (
            LegacyRequestFrame::TransactionsById(vec![transaction.id()]),
            Response::Transactions(vec![InventoryResponse::Available((transaction, None))]),
        ),
        (
            LegacyRequestFrame::BlocksByHash(vec![block.hash()]),
            Response::Blocks(vec![InventoryResponse::Available((block.into(), None))]),
        ),
    ];
    for (request, response) in cases {
        let request = request.encode_frame()?;
        let budget =
            LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        let mut frames = LegacyResponseCodec::encode_response(
            request_id,
            response,
            limits.max_frame_bytes,
            limits.max_message_bytes,
        )?;
        let [frame] = frames.as_mut_slice() else {
            panic!("a small inventory item encodes as one frame");
        };

        let mut state = LegacyResponseReadState::new(budget);
        state
            .validate_frame(request_id, frame.clone())
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;

        frame.payload.extend_from_slice(&[0; 16]);
        let mut state = LegacyResponseReadState::new(budget);
        assert!(matches!(
            state.validate_frame(request_id, frame.clone()),
            Err(OutboundRequestError::Fatal(_)),
        ));
    }

    Ok(())
}

fn assert_codec_frames_validate_at_transport(
    request: LegacyRequestFrame,
    request_kind: LegacyRequestKind,
    response: Response,
) -> Result<(), BoxError> {
    let limits = test_connection_limits();
    let request_id = 99;
    let request_frame = request.encode_frame()?;
    let budget = LegacyResponseBudget::from_request(
        request_frame.message_type,
        &request_frame.payload,
        limits,
    )
    .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
    let frames = LegacyResponseCodec::encode_response(
        request_id,
        response,
        limits.max_frame_bytes,
        limits.max_message_bytes,
    )?;
    let expected =
        LegacyResponseCodec::decode_response(request_id, request_kind, frames.clone(), None)?;

    let mut state = LegacyResponseReadState::new(budget);
    let mut items = Vec::new();
    for frame in &frames {
        FrameFilter::new(None, InboundReader::Requester)
            .with_precheck(Some(&state))
            .check_header(
                frame.message_type,
                frame.flags,
                frame.payload.len(),
                usize::MAX,
            )?;
        if let Some(item) = state
            .validate_frame(request_id, frame.clone())
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?
        {
            items.push(item);
        }
    }
    state
        .finish()
        .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
    assert_eq!(
        LegacyResponseCodec::decode_response(request_id, request_kind, items, None)?,
        expected,
    );

    Ok(())
}

fn test_connection_limits() -> ZakuraConnectionLimits {
    let max_protocol_message_len =
        u32::try_from(MAX_PROTOCOL_MESSAGE_LEN).expect("protocol message length fits in u32");

    ZakuraConnectionLimits {
        max_frame_bytes: max_protocol_message_len,
        max_message_bytes: max_protocol_message_len,
        max_open_streams: 16,
        max_inbound_queue_depth: 16,
        idle_timeout: Duration::from_secs(1),
        prelude_timeout: Duration::from_secs(1),
        control_timeout: Duration::from_secs(1),
        stream_open_rate_per_second: 16,
        message_rate_per_second: 128,
    }
}

fn block_hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

fn legacy_tx_id(byte: u8) -> UnminedTxId {
    UnminedTxId::from_legacy_id(transaction::Hash([byte; 32]))
}

fn empty_v5_transaction(byte: u8) -> transaction::Transaction {
    transaction::Transaction::V5 {
        network_upgrade: zakura_chain::parameters::NetworkUpgrade::Nu5,
        lock_time: transaction::LockTime::min_lock_time_timestamp(),
        expiry_height: block::Height(u32::from(byte)),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

#[test]
fn same_kind_streams_share_one_connection_message_budget() {
    // FLUP-014: two workers serving the SAME kind on ONE connection must
    // draw from a single shared bucket, so opening a second stream does not
    // hand the peer a fresh full budget. Driven by TestClock so the aggregate
    // is asserted on return values, not timing or metrics.
    let clock = crate::zakura::testkit::TestClock::new();
    let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
        MessageRateBuckets::new();
    let rate = 4;

    // Two streams of the same kind get clones of the SAME bucket.
    let stream_a = message_bucket_for(&mut buckets, 1, rate, clock.clone());
    let stream_b = message_bucket_for(&mut buckets, 1, rate, clock.clone());
    assert!(
        Arc::ptr_eq(&stream_a, &stream_b),
        "same kind must reuse one shared bucket"
    );
    assert_eq!(
        buckets.len(),
        1,
        "no extra bucket created for the same kind"
    );

    // The aggregate across both handles is capped at the single-bucket budget.
    let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
        bucket
            .lock()
            .expect("test bucket mutex is never poisoned")
            .try_take()
    };
    let mut accepted = 0;
    for _ in 0..rate as usize * 4 {
        if take(&stream_a) {
            accepted += 1;
        }
        if take(&stream_b) {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, rate as usize,
        "the second same-kind stream must NOT get a fresh full budget"
    );

    // After a full refill window the shared budget recovers to one capacity,
    // still shared across both streams (not doubled).
    clock.advance(Duration::from_secs(1));
    let mut refilled = 0;
    for _ in 0..rate as usize * 4 {
        if take(&stream_a) {
            refilled += 1;
        }
        if take(&stream_b) {
            refilled += 1;
        }
    }
    assert_eq!(
        refilled, rate as usize,
        "refill restores one shared budget, not one-per-stream"
    );
}

#[test]
fn different_kinds_get_independent_message_budgets() {
    // FLUP-014: the bucket is keyed per stream-kind, so distinct known kinds
    // do not contend for the same budget.
    let clock = crate::zakura::testkit::TestClock::new();
    let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
        MessageRateBuckets::new();

    let request = message_bucket_for(&mut buckets, 1, 2, clock.clone());
    let gossip = message_bucket_for(&mut buckets, 2, 2, clock.clone());
    assert!(
        !Arc::ptr_eq(&request, &gossip),
        "distinct kinds must not share a bucket"
    );
    assert_eq!(buckets.len(), 2);

    let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
        bucket
            .lock()
            .expect("test bucket mutex is never poisoned")
            .try_take()
    };
    // Drain the request budget; gossip must be untouched.
    assert!(take(&request));
    assert!(take(&request));
    assert!(!take(&request));
    assert!(take(&gossip));
    assert!(take(&gossip));
    assert!(!take(&gossip));
}

#[test]
fn idle_invariant_keeps_app_timeout_below_quic_timeout() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());

    validate_idle_invariant(&limits).expect("default Zakura limits satisfy idle invariant");
    assert!(
        (limits.initial_limits().idle_timeout_millis as u128)
            < limits.quic_idle_timeout.as_millis()
    );
}

// SECURITY AUDIT (candidate claude-legacy-requester-response-frame-growth /
// trace-gossip-response-reassembler-frame-vector-growth): SR-4 amplification.
//
// `write_outbound_request_frame_inner` accumulates every validated response
// Frame into a `Vec<Frame>` until the responder closes the stream, then hands
// the whole vector to `decode_response`. The only thing bounding how much it
// retains is `LegacyResponseBudget::max_bytes`, which `validate_frame` checks
// as a cumulative byte budget. For Blocks/Transactions that budget was
// derived as `item_count * max_message_bytes`, so a request naming the
// protocol-max inventory count (`MAX_TX_INV_IN_SENT_MESSAGE` = 25_000) handed
// a hostile responder a ~50 GiB retained-frame budget before any decode.
//
// The inbound responder already caps a single response's cumulative payload
// at `COMPAT_RESPONSE_MAX_AGGREGATE_BYTES` (8 * MAX_PROTOCOL_MESSAGE_LEN), so
// an honest peer never sends more than that. The requester must clamp its
// retained-frame budget to the same operational aggregate. This test asserts
// the budget is bounded regardless of requested item count; it FAILS before
// the fix (budget ~= 50 GiB) and passes after. Do not weaken it to pass.
#[test]
fn requester_response_budget_is_capped_for_large_inventory_request() {
    let limits = test_connection_limits();
    assert_eq!(
        limits.max_message_bytes as usize, MAX_PROTOCOL_MESSAGE_LEN,
        "fixture should negotiate the protocol-max message cap so the unclamped \
             budget is maximal",
    );

    let max_items =
        usize::try_from(MAX_TX_INV_IN_SENT_MESSAGE).expect("inventory cap fits in usize");

    // A BlocksByHash request naming the protocol-max inventory count must not
    // grant a retained-frame byte budget above the operational aggregate cap.
    let blocks = LegacyRequestFrame::BlocksByHash(vec![block_hash(7); max_items])
        .encode_frame()
        .expect("max-inventory blocks request encodes");
    let blocks_budget =
        LegacyResponseBudget::from_request(blocks.message_type, &blocks.payload, limits)
            .expect("budget derives from a max-inventory blocks request");
    assert!(
        blocks_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
        "BlocksByHash retained-frame budget {} must be clamped to the aggregate cap {}",
        blocks_budget.max_bytes,
        COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
    );

    // The transaction-fetch path scales identically and must be clamped too.
    let txs = LegacyRequestFrame::TransactionsById(vec![legacy_tx_id(7); max_items])
        .encode_frame()
        .expect("max-inventory transactions request encodes");
    let txs_budget = LegacyResponseBudget::from_request(txs.message_type, &txs.payload, limits)
        .expect("budget derives from a max-inventory transactions request");
    assert!(
        txs_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
        "TransactionsById retained-frame budget {} must be clamped to the aggregate cap {}",
        txs_budget.max_bytes,
        COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
    );

    // The clamp must only remove the unbounded tail: a modest request still
    // has to accept at least one full negotiated message, otherwise we would
    // wrongly reject honest single-item responses.
    let small = LegacyRequestFrame::BlocksByHash(vec![block_hash(1)])
        .encode_frame()
        .expect("single-item blocks request encodes");
    let small_budget =
        LegacyResponseBudget::from_request(small.message_type, &small.payload, limits)
            .expect("budget derives from a single-item blocks request");
    assert!(
        small_budget.max_bytes >= limits.max_message_bytes as usize,
        "a single-item request must still permit one full response message; budget was {}",
        small_budget.max_bytes,
    );
    assert!(
        small_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
        "even a single-item budget stays within the aggregate cap",
    );
}

/// A lone, well-formed NIL answer to an inventory fetch is request-local, so
/// the peer stays connected and the caller falls back; NIL to a Ping, NIL
/// with the wrong request id, and NIL mixed with other frames in either
/// order stay connection-fatal.
/// A `FindBlocks` response may carry the responder's 500-hash limit, and
/// one more hash is connection-fatal.
#[test]
fn find_blocks_response_is_bounded_by_responder_limit() {
    let limits = test_connection_limits();
    let request_id = 5;
    let request = LegacyRequestFrame::FindBlocks {
        known_blocks: vec![block_hash(1)],
        stop: None,
    }
    .encode_frame()
    .expect("a one-hash locator is within the request limits");

    for (count, fatal) in [
        (MAX_FIND_BLOCKS_RESPONSE_HASHES, false),
        (MAX_FIND_BLOCKS_RESPONSE_HASHES + 1, true),
    ] {
        let hashes = (0..count)
            .map(|index| {
                let mut hash = [0; 32];
                hash[..8].copy_from_slice(
                    &u64::try_from(index)
                        .expect("at most 501 hashes, so the index fits u64")
                        .to_le_bytes(),
                );
                block::Hash(hash)
            })
            .collect();
        let frame = LegacyResponseCodec::encode_response(
            request_id,
            Response::BlockHashes(hashes),
            limits.max_frame_bytes,
            limits.max_message_bytes,
        )
        .expect("501 hashes are within the 25,000-item encoding cap")
        .pop()
        .expect("a 501-hash list fits in one frame under the test limits");
        let mut state = LegacyResponseReadState::new(
            LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                .expect("a FindBlocks request has a known response kind"),
        );

        let result = state.validate_frame(request_id, frame.clone());
        if fatal {
            assert!(
                matches!(result, Err(OutboundRequestError::Fatal(_))),
                "{count} hashes must be Fatal; got {result:?}"
            );
        } else {
            assert!(result.is_ok(), "{count} hashes must pass; got {result:?}");
        }
    }

    // A repeated hash is rejected as the frame arrives, before the stream
    // ends, so a peer cannot avoid it by holding the stream open.
    let frame = LegacyResponseCodec::encode_response(
        request_id,
        Response::BlockHashes(vec![block_hash(1), block_hash(2), block_hash(1)]),
        limits.max_frame_bytes,
        limits.max_message_bytes,
    )
    .expect("three hashes are within the 25,000-item encoding cap")
    .pop()
    .expect("a three-hash list fits in one frame under the test limits");
    let mut state = LegacyResponseReadState::new(
        LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
            .expect("a FindBlocks request has a known response kind"),
    );
    let result = state.validate_frame(request_id, frame.clone());
    assert!(
        matches!(result, Err(OutboundRequestError::Fatal(_))),
        "a repeated hash must be Fatal; got {result:?}"
    );
}

#[test]
fn nil_response_is_local_for_inventory_and_fatal_for_ping() {
    let limits = test_connection_limits();
    let request_id = 99;

    let cases = [
        (LegacyRequestFrame::BlocksByHash(vec![block_hash(1)]), false),
        (
            LegacyRequestFrame::TransactionsById(vec![legacy_tx_id(2)]),
            false,
        ),
        (LegacyRequestFrame::Ping, true),
    ];

    for (request, fatal) in cases {
        let request_frame = request.encode_frame().expect("request frame encodes");
        let budget = || {
            LegacyResponseBudget::from_request(
                request_frame.message_type,
                &request_frame.payload,
                limits,
            )
            .expect("budget derives from request")
        };
        let nil = |id| {
            LegacyResponseCodec::encode_response(
                id,
                Response::Nil,
                limits.max_frame_bytes,
                limits.max_message_bytes,
            )
            .expect("nil response encodes")
            .pop()
            .expect("nil encodes to one frame")
        };

        let mut lone = LegacyResponseReadState::new(budget());
        let result = lone
            .validate_frame(request_id, nil(request_id))
            .and_then(|_| lone.finish());
        if fatal {
            assert!(
                matches!(result, Err(OutboundRequestError::Fatal(_))),
                "NIL to {request:?} must be Fatal; got {result:?}",
            );
        } else {
            assert!(
                matches!(result, Err(OutboundRequestError::Local(_))),
                "a lone NIL to {request:?} must be Local; got {result:?}",
            );
        }

        let wrong_id =
            LegacyResponseReadState::new(budget()).validate_frame(request_id, nil(request_id + 1));
        assert!(
            matches!(wrong_id, Err(OutboundRequestError::Fatal(_))),
            "a wrong-id NIL to {request:?} must stay Fatal; got {wrong_id:?}",
        );

        if let LegacyRequestFrame::BlocksByHash(hashes) = &request {
            let missing = || {
                LegacyResponseCodec::encode_response(
                    request_id,
                    Response::Blocks(vec![InventoryResponse::Missing(hashes[0])]),
                    limits.max_frame_bytes,
                    limits.max_message_bytes,
                )
                .expect("missing response encodes")
                .pop()
                .expect("a missing entry encodes to one frame")
            };

            let mut after_items = LegacyResponseReadState::new(budget());
            after_items
                .validate_frame(request_id, missing())
                .expect("a missing entry is accepted");
            let result = after_items.validate_frame(request_id, nil(request_id));
            assert!(
                matches!(result, Err(OutboundRequestError::Fatal(_))),
                "NIL after inventory frames must stay Fatal; got {result:?}",
            );

            let mut before_items = LegacyResponseReadState::new(budget());
            before_items
                .validate_frame(request_id, nil(request_id))
                .expect("a first NIL is held until the stream ends");
            let result = before_items.validate_frame(request_id, missing());
            assert!(
                matches!(result, Err(OutboundRequestError::Fatal(_))),
                "inventory frames after NIL must stay Fatal; got {result:?}",
            );
        }
    }
}

#[tokio::test]
async fn nat_traversal_fails_startup_and_names_the_spec() {
    let mut config = Config::for_test(P2pStack::Zakura);
    config.zakura.nat_traversal = true;
    let error = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
        .await
        .expect_err("nat_traversal = true must fail at startup");
    let message = error.to_string();
    assert!(message.contains("hole punching"), "{message}");
    assert!(message.contains("docs/specs/zakura-quic.md"), "{message}");
}

#[tokio::test]
async fn inbound_connection_is_charged_to_its_admitted_ip() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(30), async {
        #[derive(Clone, Debug)]
        struct CaptureAccepted {
            handler: ZakuraProtocolHandler,
            accepted: mpsc::Sender<Connection>,
        }
        impl Acceptor for CaptureAccepted {
            fn admit(&self, incoming: &IncomingInfo) -> Admit {
                self.handler.admit(incoming)
            }

            fn cancel_admission(&self, incoming: &IncomingInfo) {
                self.handler.cancel_admission(incoming);
            }

            fn finish_admission(&self, handed_off: bool) {
                self.handler.finish_admission(handed_off);
            }

            fn alpns(&self) -> Vec<Vec<u8>> {
                self.handler.alpns()
            }

            fn handle(&self, connection: Connection) -> BoxFuture<'static, ()> {
                let serving = self.handler.handle(connection.clone());
                let capture = self.clone();
                Box::pin(async move {
                    if capture.accepted.send(connection).await.is_ok() {
                        serving.await;
                    }
                })
            }
        }
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        // Both same-identity connections sit in their control handshakes at
        // once, and each counts against the IP (zakura-quic ADM-3), so the
        // cap must hold two.
        let supervisor = ZakuraSupervisorHandle::new(2);
        let server = LocalEndpointFactory::with_limits(&limits)
            .endpoint(883)
            .await?;
        let (accepted_tx, mut accepted_rx) = mpsc::channel(3);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        )
        .with_local_node_id(server.local_id());
        server.serve(CaptureAccepted {
            handler,
            accepted: accepted_tx,
        })?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;
        // Separate endpoints reuse one identity but bind different UDP ports.
        let first = LocalEndpointFactory::with_limits(&limits)
            .endpoint(884)
            .await?;
        let second = LocalEndpointFactory::with_limits(&limits)
            .endpoint(884)
            .await?;
        let first_conn = first.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
        let first_accepted = accepted_rx.recv().await.expect("first accept is captured");
        let second_conn = second.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
        let second_accepted = accepted_rx.recv().await.expect("second accept is captured");
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(first_accepted.admitted_ip(), ip);
        assert_eq!(second_accepted.admitted_ip(), ip);
        let config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let peer = ZakuraPeerId::new(second.local_id().as_bytes().to_vec())?;
        // Complete the second connection's control handshake first.
        run_native_initiator_handshake_without_trace(&second_conn, &limits, &config, &peer).await?;
        while supervisor.registered_ids().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            supervisor.inner.lock().await.ip_slots.registered(ip),
            1,
            "the registered connection must consume the IP slot"
        );
        // Duplicate admission can close the transport before the client reads the ack.
        let _ = run_native_initiator_handshake_without_trace(&first_conn, &limits, &config, &peer)
            .await;
        assert!(matches!(first_conn.closed().await,
                zakura_quic::ConnectionError::ApplicationClosed(close)
                    if close.error_code == VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL)
                        && close.reason.as_ref() == b"duplicate"));
        assert_eq!(supervisor.registered_ids().await, vec![peer]);
        assert_eq!(
            supervisor.inner.lock().await.ip_slots.registered(ip),
            1,
            "closing the duplicate must preserve the incumbent's IP slot"
        );

        // A second identity takes the IP's other slot.
        let third = LocalEndpointFactory::with_limits(&limits)
            .endpoint(885)
            .await?;
        let third_conn = third.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
        let third_peer = ZakuraPeerId::new(third.local_id().as_bytes().to_vec())?;
        run_native_initiator_handshake_without_trace(&third_conn, &limits, &config, &third_peer)
            .await?;
        while supervisor.registered_ids().await.len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let fourth = LocalEndpointFactory::with_limits(&limits)
            .endpoint(886)
            .await?;
        // A full IP with no handshake in flight still gets one QUIC
        // handshake, in case it is a registered node redialing.
        // Registration then rejects the new identity.
        let fourth_peer = ZakuraPeerId::new(fourth.local_id().as_bytes().to_vec())?;
        let rejected = match fourth.connect(server_addr, &[P2P_V2_ALPN]).await {
            Ok(fourth_conn) => run_native_initiator_handshake_without_trace(
                &fourth_conn,
                &limits,
                &config,
                &fourth_peer,
            )
            .await
            .is_err(),
            Err(error) => matches!(error, zakura_quic::ConnectError::Refused),
        };
        assert!(
            rejected,
            "a new identity from the same IP must be turned away at the IP cap"
        );
        assert_eq!(supervisor.registered_ids().await.len(), 2);
        fourth.shutdown().await;
        third.shutdown().await;
        first.shutdown().await;
        second.shutdown().await;
        server.shutdown().await;
        Ok(())
    })
    .await
    .expect("admitted IP accounting test timed out")
}

// SECURITY AUDIT (candidate claude-inbound-per-ip-cap-bypassed /
// codex-inbound-per-ip-cap-bypass): SR-4 admission.
//
// `accept_connection` used to register every inbound peer with
// `remote_ip = None`, and `register` only consults the per-IP counts when
// `remote_ip` is `Some`, so the per-IP connection cap was enforced for native
// outbound dials (which pass a real IP) but entirely bypassed for inbound
// accepts: one source IP could authenticate as many distinct node ids and
// fill the global connection budget despite `max_connections_per_ip`
// (default 1). Inbound accepts now pass the admitted source IP into
// `register`, and the acceptor refuses a source IP at its cap before any
// handshake work (zakura-quic ADM-3).
//
// This guard drives the real production acceptor over loopback QUIC: two
// distinct authenticated identities dial from the same source IP
// (127.0.0.1). The per-IP cap is 1 while global admission keeps its default
// (well above 1), so the second identity can only be turned away by the
// per-IP cap, not the global gate. It never registers, leaving exactly one
// registered peer.
#[tokio::test]
async fn inbound_accept_enforces_per_ip_cap() -> Result<(), BoxError> {
    let _guard = zakura_test::init();

    // The register-level invariant the accept path now relies on: with a real
    // source IP, the per-IP cap rejects a second distinct identity with
    // `ResourceLimit`.
    async fn try_register(
        supervisor: &ZakuraSupervisorHandle,
        peer: &ZakuraPeerId,
        remote_ip: Option<IpAddr>,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                remote_ip,
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                CancellationToken::new(),
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }
    let ip: IpAddr = "203.0.113.7".parse().expect("test ip parses");
    let supervisor = ZakuraSupervisorHandle::new(1);
    assert!(
        matches!(
            try_register(&supervisor, &test_peer(1), Some(ip)).await,
            ZakuraRegistration::Registered { .. }
        ),
        "first identity from the IP registers",
    );
    assert!(
        matches!(
            try_register(&supervisor, &test_peer(2), Some(ip)).await,
            ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
        ),
        "a second distinct identity from the same IP must be Rejected(ResourceLimit) at cap 1",
    );

    // End-to-end: drive the production accept path with a per-IP cap of 1 and a
    // strictly larger global cap so per-IP admission is what turns away the
    // second same-IP identity.
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    assert!(
        limits.max_connections > 1,
        "global admission cap must exceed the per-IP cap so the second same-IP identity is \
             turned away by the per-IP cap rather than the global gate",
    );
    let server_ep = LocalEndpointFactory::with_limits(&limits)
        .endpoint(880)
        .await?;
    let supervisor = ZakuraSupervisorHandle::new(1);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    )
    .with_local_node_id(server_ep.local_id());
    server_ep.serve(handler)?;
    // The factory binds IPv4 loopback only, so both clients share one source
    // IP (127.0.0.1) -- the single-source-IP shape of the finding.
    let server_addr = LocalEndpointFactory::node_addr(&server_ep).await;

    // Establish the QUIC connection only; the native handshake is driven
    // separately so a per-IP rejection mid-handshake (the second identity)
    // can be observed instead of aborting the test.
    async fn connect_native(
        server_addr: &NodeAddr,
        seed: u64,
        limits: &ZakuraLocalLimits,
    ) -> Result<(QuicEndpoint, Connection), BoxError> {
        let endpoint = LocalEndpointFactory::with_limits(limits)
            .endpoint(seed)
            .await?;

        let connection = endpoint
            .connect(server_addr.clone(), &[P2P_V2_ALPN])
            .await?;
        Ok((endpoint, connection))
    }
    async fn run_handshake(
        endpoint: &QuicEndpoint,
        connection: &Connection,
        limits: &ZakuraLocalLimits,
    ) -> Result<(), BoxError> {
        let config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        let local_peer_id = ZakuraPeerId::new(endpoint.local_id().as_bytes().to_vec())?;
        run_native_initiator_handshake_without_trace(connection, limits, &config, &local_peer_id)
            .await?;
        Ok(())
    }

    // First identity registers and claims the only per-IP slot. Hold the
    // endpoint/connection so the peer stays registered for the second dial.
    let (_ep1, _conn1) = connect_native(&server_addr, 881, &limits).await?;
    run_handshake(&_ep1, &_conn1, &limits).await?;
    let mut first_registered = 0;
    for _ in 0..200 {
        first_registered = supervisor.registered_ids().await.len();
        if first_registered >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        first_registered, 1,
        "the first inbound identity from the source IP must register (a real source IP was \
             resolved from the endpoint and counted against the per-IP cap)",
    );

    // Second distinct identity from the same source IP. A full IP still
    // gets one handshake, in case it is the first node redialing, so the
    // QUIC handshake may complete; registration then rejects the new
    // identity. (Before the fix the accept passed remote_ip = None, so
    // this identity registered and one source IP could exhaust the global
    // budget.)
    let rejected = match connect_native(&server_addr, 882, &limits).await {
        Ok((endpoint, connection)) => run_handshake(&endpoint, &connection, &limits)
            .await
            .is_err(),
        Err(error) => matches!(
            error.downcast_ref::<zakura_quic::ConnectError>(),
            Some(zakura_quic::ConnectError::Refused)
        ),
    };
    let registered = supervisor.registered_ids().await.len();
    assert!(
        rejected && registered == 1,
        "a second distinct identity from the same source IP must be turned away by the \
             per-IP cap (rejected={rejected}, registered={registered}); before the fix the \
             inbound accept passed remote_ip = None, so both identities registered and one \
             source IP could exhaust the connection budget",
    );

    server_ep.shutdown().await;
    Ok(())
}

// V12 F-305593: the whole-dial timeout used to be a fixed 10 s, which cut
// off staggered attempts and longer handshake deadlines.
#[test]
fn native_dial_timeout_covers_the_last_staggered_attempt() {
    let control_timeout = Duration::from_secs(10);
    let slow = QuicConfig {
        dial_stagger_ms: 5_000,
        handshake_timeout_secs: Some(60),
        ..QuicConfig::default()
    };
    assert_eq!(
        native_dial_timeout(&slow, 4, control_timeout),
        Duration::from_secs(75)
    );
    let unset = QuicConfig {
        handshake_timeout_secs: None,
        ..QuicConfig::default()
    };
    assert_eq!(
        native_dial_timeout(&unset, 1, control_timeout),
        unset.idle_timeout()
    );
    assert_eq!(
        native_dial_timeout(&QuicConfig::default(), 1, control_timeout),
        control_timeout
    );
}

/// ADM-3 rule 2 before TLS: a full IP gets one handshake at a time for a
/// possible same-node redial; more are refused when the source is
/// validated and retried when it may be spoofed; and one IP can't take
/// more than its share of the pending-handshake budget.
#[tokio::test]
async fn per_ip_admission_retries_unvalidated_and_allows_one_redial() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let ip: IpAddr = "203.0.113.9".parse().expect("test ip parses");
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let supervisor = ZakuraSupervisorHandle::new(1);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    let peer = test_peer(1);
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let registered = supervisor
        .register(
            test_conn_id(),
            peer.clone(),
            Some(ip),
            [1; TRANSCRIPT_HASH_BYTES],
            ZakuraPeerHandle::new_for_tests(peer, outbound_tx),
            CancellationToken::new(),
            ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await;
    assert!(matches!(registered, ZakuraRegistration::Registered { .. }));

    let attempt = |validated: bool, pending_from_ip: usize| IncomingInfo {
        remote: SocketAddr::new(ip, 8234),
        validated,
        pending_total: pending_from_ip,
        pending_from_ip,
    };
    assert_eq!(handler.admit(&attempt(false, 0)), Admit::Retry);
    assert_eq!(handler.admit(&attempt(true, 0)), Admit::Accept);
    assert_eq!(handler.admit(&attempt(true, 1)), Admit::Refuse);
    assert_eq!(handler.admit(&attempt(false, 1)), Admit::Retry);

    let other: IpAddr = "198.51.100.3".parse().expect("test ip parses");
    let share = limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP;
    let busy = |pending_from_ip: usize| IncomingInfo {
        remote: SocketAddr::new(other, 8234),
        validated: true,
        pending_total: pending_from_ip,
        pending_from_ip,
    };
    let open = ZakuraSupervisorHandle::new(64);
    let handler = ZakuraProtocolHandler::new(
        open,
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits,
    );
    assert_eq!(
        handler.admit(&IncomingInfo {
            validated: false,
            ..busy(share - 2)
        }),
        Admit::Accept
    );
    assert_eq!(
        handler.admit(&IncomingInfo {
            validated: false,
            ..busy(share - 1)
        }),
        Admit::Retry
    );
    assert_eq!(handler.admit(&busy(share - 1)), Admit::Accept);
    assert_eq!(handler.admit(&busy(share)), Admit::Refuse);
    Ok(())
}

/// Spoofed Initials carry unvalidated sources. Draining a victim's
/// unvalidated cadence, or filling the unvalidated ledger with other
/// addresses, earns the spoofed sources a stateless Retry, never a
/// refusal, and leaves a validated Initial from the victim its own
/// cadence (zakura#2337 review).
#[test]
fn spoofed_initials_never_lock_out_a_validated_source() {
    let victim: IpAddr = "203.0.113.9".parse().expect("test ip parses");
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    let attempt = |ip: IpAddr, validated: bool| IncomingInfo {
        remote: SocketAddr::new(ip, 8234),
        validated,
        pending_total: 0,
        pending_from_ip: 0,
    };
    // Each spoofed attempt fails at once and returns its transport
    // reservation, with no cadence refund.
    let spoof = |ip: IpAddr| {
        let admitted = handler.admit(&attempt(ip, false));
        if admitted == Admit::Accept {
            handler.finish_admission(false);
        }
        admitted
    };
    for _ in 0..DEFAULT_ZAKURA_MAX_CONNS_PER_IP {
        assert_eq!(spoof(victim), Admit::Accept);
    }
    assert_eq!(spoof(victim), Admit::Retry);
    assert_eq!(handler.admit(&attempt(victim, true)), Admit::Accept);
    handler.finish_admission(false);

    let ledger = limits
        .max_connections
        .saturating_add(limits.max_pending_handshakes);
    for host in 0..ledger {
        let ip = IpAddr::V4(std::net::Ipv4Addr::from(
            0x0a00_0000 + u32::try_from(host).expect("the ledger fits u32"),
        ));
        for _ in 0..DEFAULT_ZAKURA_MAX_CONNS_PER_IP {
            spoof(ip);
        }
    }
    let fresh: IpAddr = "198.51.100.3".parse().expect("test ip parses");
    assert_eq!(spoof(fresh), Admit::Retry, "the unvalidated ledger is full");
    assert_eq!(handler.admit(&attempt(fresh, true)), Admit::Accept);
    handler.finish_admission(false);
}

#[test]
fn last_per_ip_slot_requires_validation() {
    let ip = "203.0.113.9".parse().unwrap();
    for cap in [1, 2, 16] {
        let supervisor = ZakuraSupervisorHandle::new(cap);
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let share = (limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP).max(1);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits,
        );
        let slots = cap.min(share);
        for controls in 0..slots {
            let _guards: Vec<_> = (0..controls)
                .map(|_| supervisor.enter_control_handshake(ip))
                .collect();
            let incoming = IncomingInfo {
                remote: SocketAddr::new(ip, 8234),
                validated: false,
                pending_total: slots - controls - 1,
                pending_from_ip: slots - controls - 1,
            };
            assert_eq!(handler.admit(&incoming), Admit::Retry);
            assert_eq!(
                handler.admit(&IncomingInfo {
                    validated: true,
                    ..incoming
                }),
                Admit::Accept
            );
        }
    }
}

async fn register_test_peer_from(
    supervisor: &ZakuraSupervisorHandle,
    peer: &ZakuraPeerId,
    ip: &str,
    token: CancellationToken,
) -> ZakuraRegistration {
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    supervisor
        .register(
            test_conn_id(),
            peer.clone(),
            Some(ip.parse().expect("test ip parses")),
            [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
            ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx),
            token,
            ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await
}

/// #645: every address in a global IPv6 /64 shares one per-IP cap, so a
/// host that holds a /64 can't fill the node by using a fresh address per
/// identity. Another /64 and unique-local addresses keep their own caps.
#[tokio::test]
async fn ipv6_slash_64_shares_one_per_ip_cap() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let register = |byte: u8, ip: &'static str| {
        let supervisor = supervisor.clone();
        async move {
            register_test_peer_from(&supervisor, &test_peer(byte), ip, CancellationToken::new())
                .await
        }
    };
    assert!(matches!(
        register(1, "2001:db8:a:b::1").await,
        ZakuraRegistration::Registered { .. }
    ));
    assert!(matches!(
        register(2, "2001:db8:a:b::2").await,
        ZakuraRegistration::Registered { .. }
    ));
    assert!(matches!(
        register(3, "2001:db8:a:b:ffff:ffff:ffff:3").await,
        ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
    ));
    let full_slash_64 = "2001:db8:a:b::99".parse().expect("test ip parses");
    assert!(!supervisor.can_accept_remote_ip_with_in_flight(full_slash_64, 0));

    let other_slash_64 = "2001:db8:a:c::1".parse().expect("test ip parses");
    assert!(supervisor.can_accept_remote_ip_with_in_flight(other_slash_64, 1));
    assert!(!supervisor.can_accept_remote_ip_with_in_flight(other_slash_64, 2));
    assert!(matches!(
        register(4, "2001:db8:a:c::1").await,
        ZakuraRegistration::Registered { .. }
    ));
    for (byte, unique_local) in [(5, "fd00::1"), (6, "fd00::2"), (7, "fd00::3")] {
        assert!(
            matches!(
                register(byte, unique_local).await,
                ZakuraRegistration::Registered { .. }
            ),
            "{unique_local} has its own cap",
        );
    }
}

/// Registration charges the /64, but reports the peer's own address,
/// which discovery matches exactly against the addresses the peer
/// advertises.
#[tokio::test]
async fn registration_reports_the_address_not_its_slash_64() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let registration = register_test_peer_from(
        &supervisor,
        &test_peer(1),
        "2001:db8:a:b::7",
        CancellationToken::new(),
    )
    .await;
    let ZakuraRegistration::Registered { remote_ip, .. } = registration else {
        panic!("a fresh peer registers: {registration:?}");
    };
    assert_eq!(
        remote_ip,
        Some("2001:db8:a:b::7".parse().expect("test ip parses"))
    );
    assert_eq!(
        supervisor
            .ip_slots
            .registered("2001:db8:a:b::".parse().expect("test ip parses")),
        1
    );
}

/// A restarted peer that comes back on another address of its /64, as
/// RFC 8981 temporary addresses do, still takes the same-IP redial path:
/// it passes a full cap and evicts its stale incumbent on the short gate.
#[tokio::test(start_paused = true)]
async fn redial_from_the_same_slash_64_is_a_same_ip_duplicate() {
    let supervisor = ZakuraSupervisorHandle::new(1);
    let peer = test_peer(60);
    let incumbent = CancellationToken::new();
    assert!(matches!(
        register_test_peer_from(&supervisor, &peer, "2001:db8:a:b::1", incumbent.clone()).await,
        ZakuraRegistration::Registered { .. }
    ));
    tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
    assert!(matches!(
        register_test_peer_from(
            &supervisor,
            &peer,
            "2001:db8:a:b::2",
            CancellationToken::new()
        )
        .await,
        ZakuraRegistration::Duplicate { .. }
    ));
    assert!(incumbent.is_cancelled());
}

/// The acceptor charges control handshakes and the transport's pending
/// handshakes to the /64 too, so one /64 gets one share of the handshake
/// budget however many addresses it uses.
#[test]
fn handshakes_from_one_slash_64_count_together() {
    let supervisor = ZakuraSupervisorHandle::new(2);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    );
    let ip = |ip: &str| -> IpAddr { ip.parse().expect("test ip parses") };
    assert_eq!(
        handler.pending_group(ip("2001:db8:a:b::7")),
        ip("2001:db8:a:b::")
    );
    let _controls = [
        supervisor.enter_control_handshake(ip("2001:db8:a:b::1")),
        supervisor.enter_control_handshake(ip("2001:db8:a:b::2")),
    ];
    let attempt = |remote: &str| IncomingInfo {
        remote: SocketAddr::new(ip(remote), 8234),
        validated: true,
        pending_total: 0,
        pending_from_ip: 0,
    };
    assert_eq!(handler.admit(&attempt("2001:db8:a:b::3")), Admit::Refuse);
    assert_eq!(handler.admit(&attempt("2001:db8:a:c::3")), Admit::Accept);
}

/// ADM-3's inbound share (#1367): once inbound connections or inbound
/// handshakes fill three quarters of their caps, the acceptor turns new
/// inbound attempts away, and a dial still finds the reserved quarter.
#[test]
fn inbound_load_leaves_the_outbound_reserve() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    assert_eq!(limits.max_inbound_connections(), 69);
    assert_eq!(limits.max_inbound_handshakes(), 24);
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(limits.max_connections),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    let attempt = |validated: bool, pending_total: usize| IncomingInfo {
        remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
        validated,
        pending_total,
        pending_from_ip: 0,
    };

    // Pending transport handshakes count against the inbound share, which
    // fills before the whole budget does.
    assert_eq!(handler.admit(&attempt(true, 23)), Admit::Accept);
    handler.finish_admission(false);
    assert_eq!(handler.admit(&attempt(true, 24)), Admit::Refuse);
    assert_eq!(handler.admit(&attempt(false, 24)), Admit::Retry);

    // So do inbound control handshakes, and dials keep the rest.
    let inbound_handshakes: Vec<_> = (0..24)
        .map(|_| {
            handler
                .claim_pending_handshake(true)
                .expect("inbound share has room")
        })
        .collect();
    assert_eq!(handler.admit(&attempt(true, 0)), Admit::Refuse);
    assert_eq!(handler.pending_handshakes.available_permits(), 8);
    drop(inbound_handshakes);

    // Inbound connections stop at their share, and dials keep the rest.
    let inbound: Vec<_> = (0..69)
        .map(|_| {
            handler
                .claim_connection(true)
                .expect("inbound share has room")
        })
        .collect();
    assert!(handler.claim_connection(true).is_none());
    assert_eq!(
        handler.supervisor.ip_slots.counts().inbound_connections(),
        69
    );
    assert_eq!(handler.admit(&attempt(true, 0)), Admit::Refuse);
    let dials: Vec<_> = (0..23)
        .map(|_| {
            handler
                .claim_connection(false)
                .expect("dial reserve has room")
        })
        .collect();
    assert!(handler.claim_connection(false).is_none());
    assert_eq!(handler.supervisor.ip_slots.counts().connections(), 92);
    assert_eq!(handler.admission.available_permits(), 0);
    drop((inbound, dials));
    assert_eq!(handler.supervisor.ip_slots.counts().connections(), 0);
    assert_eq!(
        handler.supervisor.ip_slots.counts().inbound_connections(),
        0
    );
    assert_eq!(handler.admit(&attempt(true, 0)), Admit::Accept);
}

#[test]
fn pending_transport_reservations_bound_outbound_control_claims() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(limits.max_connections),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits,
    );
    for pending in 0..24 {
        let incoming = IncomingInfo {
            remote: SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::new(
                    192,
                    0,
                    2,
                    u8::try_from(pending + 1).unwrap(),
                )),
                8234,
            ),
            validated: true,
            pending_total: pending,
            pending_from_ip: 0,
        };
        assert_eq!(handler.admit(&incoming), Admit::Accept);
    }
    let dials: Vec<_> = (0..8)
        .map(|_| handler.claim_pending_handshake(false).unwrap())
        .collect();
    assert!(handler.claim_pending_handshake(false).is_none());
    let transferred = handler
        .claim_pending_handshake_with_transfer(true, true)
        .unwrap();
    handler.finish_admission(true);
    assert!(handler.claim_pending_handshake(false).is_none());
    drop(transferred);
    let ninth = handler.claim_pending_handshake(false).unwrap();
    assert!(handler.claim_pending_handshake(false).is_none());
    for _ in 0..23 {
        handler.finish_admission(false);
    }
    drop((ninth, dials));
    let counts = handler.supervisor.ip_slots.counts();
    assert_eq!(counts.transport_pending(), 0);
    assert_eq!(counts.pending(), 0);
}

/// Dials may use inbound's unused share, and once they fill the whole cap
/// inbound attempts are refused, as before the reserve.
#[test]
fn dials_may_use_the_inbound_share() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(limits.max_connections),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    let _dials: Vec<_> = (0..limits.max_connections)
        .map(|_| {
            handler
                .claim_connection(false)
                .expect("dials may take every slot")
        })
        .collect();
    assert_eq!(
        handler.supervisor.ip_slots.counts().connections(),
        limits.max_connections
    );
    assert_eq!(
        handler.admit(&IncomingInfo {
            remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
            validated: true,
            pending_total: 0,
            pending_from_ip: 0,
        }),
        Admit::Refuse
    );
}

/// The real inbound paths hold the inbound share: `claim_inbound` holds a
/// connection slot from it until the claim drops, and the responder
/// handshake holds a handshake slot from it while it runs, so dials keep
/// the reserve in both.
#[tokio::test]
async fn inbound_paths_hold_the_inbound_share() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = 4;
    limits.max_pending_handshakes = 4;
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(64),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    assert_eq!(limits.max_inbound_connections(), 3);
    assert_eq!(limits.max_inbound_handshakes(), 3);

    let server = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x1367_0001)
        .await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(8);
    let (stream_tx, _stream_rx) = mpsc::channel(8);
    server.serve(CaptureConnection {
        alpn: P2P_V2_ALPN,
        connection_tx,
        stream_tx,
    })?;
    let server_addr = LocalEndpointFactory::node_addr(&server).await;
    let client = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x1367_0002)
        .await?;
    let mut client_connections = Vec::new();
    let mut inbound = Vec::new();
    for _ in 0..5 {
        client_connections.push(client.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?);
        inbound.push(
            timeout(Duration::from_secs(10), connection_rx.recv())
                .await?
                .expect("the capture acceptor sends each accepted connection"),
        );
    }

    // Connection slots: the fourth inbound claim finds its share full
    // while a dial still finds a slot.
    let claims: Vec<_> = inbound[..3]
        .iter()
        .map(|connection| handler.claim_inbound(connection, false))
        .collect();
    assert!(claims.iter().all(Result::is_ok));
    assert!(matches!(
        handler.claim_inbound(&inbound[3], false),
        Err(InboundRefusal::Admission)
    ));
    assert_eq!(handler.admission.available_permits(), 1);
    drop(claims);
    assert_eq!(handler.inbound_admission.available_permits(), 3);
    assert_eq!(handler.admission.available_permits(), 4);

    // Handshake slots: a responder handshake waiting for a hello that
    // never comes holds one inbound and one shared slot.
    let responder = |connection: Connection| {
        let handler = handler.clone();
        async move {
            let peer = ZakuraPeerId::new(connection.remote_id().as_bytes().to_vec())?;
            let conn = ZakuraConnTrace::new(&handler.trace, trace_conn_id(&connection), &peer);
            handler
                .run_native_responder_handshake_with_permit(
                    &connection,
                    &peer,
                    &conn,
                    handler
                        .claim_pending_handshake(true)
                        .expect("the inbound handshake share has room"),
                )
                .await
                .map(|_| ())
        }
    };
    let waiting = tokio::spawn(responder(inbound[0].clone()));
    timeout(Duration::from_secs(10), async {
        while handler.inbound_handshakes.available_permits() == 3 {
            zakura_rt::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(handler.inbound_handshakes.available_permits(), 2);
    assert_eq!(handler.pending_handshakes.available_permits(), 3);
    waiting.abort();
    let _ = waiting.await;
    assert_eq!(handler.inbound_handshakes.available_permits(), 3);
    assert_eq!(handler.pending_handshakes.available_permits(), 4);

    // With the inbound share full, an inbound claim is refused for its
    // pending handshake without keeping a connection or shared slot.
    let full = handler
        .inbound_handshakes
        .clone()
        .try_acquire_many_owned(3)
        .expect("the test holds free permits");
    assert!(matches!(
        handler.claim_inbound(&inbound[4], false),
        Err(InboundRefusal::PendingHandshake)
    ));
    assert_eq!(handler.pending_handshakes.available_permits(), 4);
    assert_eq!(handler.admission.available_permits(), 4);
    drop(full);
    Ok(())
}

/// Dials can take the room an inbound attempt saw at admission while its
/// TLS runs. The node then refuses the attempt itself, so the attempt's
/// cadence token returns to its address.
#[tokio::test]
async fn a_local_refusal_after_tls_returns_the_cadence_token() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.max_connections = 4;
    limits.max_pending_handshakes = 4;
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(1),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    let server = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x1367_0003)
        .await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, _stream_rx) = mpsc::channel(1);
    server.serve(CaptureConnection {
        alpn: P2P_V2_ALPN,
        connection_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x1367_0004)
        .await?;
    let _client_connection = client
        .connect(
            LocalEndpointFactory::node_addr(&server).await,
            &[P2P_V2_ALPN],
        )
        .await?;
    let inbound = timeout(Duration::from_secs(10), connection_rx.recv())
        .await?
        .expect("the capture acceptor sends the accepted connection");
    let ip = inbound.admitted_ip();
    let validated = inbound.source_validated();
    let max_hosts = limits
        .max_connections
        .saturating_add(limits.max_pending_handshakes);
    let charge = |handler: &ZakuraProtocolHandler| {
        handler.supervisor.ip_slots.counts().admit_cadence(
            ip,
            validated,
            handler.supervisor.max_connections_per_ip,
            max_hosts,
            Instant::now(),
        )
    };

    // `admit` reserved the transport and took the address's one token.
    assert!(handler
        .supervisor
        .ip_slots
        .counts()
        .claim_transport(limits.max_pending_handshakes));
    assert_eq!(
        charge(&handler),
        crate::zakura::HandshakeCadenceAdmission::Admit
    );
    // Dials then took the inbound share.
    assert_eq!(limits.max_inbound_connections(), 3);
    let dials = handler
        .inbound_admission
        .clone()
        .try_acquire_many_owned(3)
        .expect("the test holds free permits");

    timeout(Duration::from_secs(10), handler.handle(inbound)).await?;
    assert_eq!(
        charge(&handler),
        crate::zakura::HandshakeCadenceAdmission::Admit
    );
    assert_eq!(
        charge(&handler),
        crate::zakura::HandshakeCadenceAdmission::Exhausted
    );
    drop(dials);
    Ok(())
}

#[test]
fn outbound_reserve_rounds_down_but_keeps_one_slot() {
    for (cap, inbound) in [
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 3),
        (7, 6),
        (8, 6),
        (16, 12),
        (32, 24),
        (92, 69),
    ] {
        assert_eq!(without_outbound_reserve(cap), inbound, "cap {cap}");
    }
}

// V12 F-67: the handler refused every attempt at the global pending cap,
// before the transport's Retry threshold could apply, so a valid config
// with `retry_threshold >= max_pending_handshakes` never sent a Retry.
#[test]
fn full_pending_budget_retries_unvalidated_attempts() {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(64),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    );
    // Spread the pending total over other IPs so the per-IP share stays open.
    let attempt = |validated: bool| IncomingInfo {
        remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
        validated,
        pending_total: limits.max_pending_handshakes,
        pending_from_ip: 0,
    };
    assert_eq!(handler.admit(&attempt(false)), Admit::Retry);
    assert_eq!(handler.admit(&attempt(true)), Admit::Refuse);
}

// V12 F-305585: the transport stops counting an inbound connection when TLS
// finishes, and the control handshake that follows used to count only
// against the global `pending_handshakes` budget. One IP could therefore
// stall many TLS-complete connections and exceed its per-IP cap. Those
// connections now count against their IP until registration.
#[tokio::test]
async fn stalled_control_handshakes_count_against_the_per_ip_cap() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server_ep = LocalEndpointFactory::with_limits(&limits)
        .endpoint(890)
        .await?;
    let supervisor = ZakuraSupervisorHandle::new(2);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    )
    .with_local_node_id(server_ep.local_id());
    server_ep.serve(handler)?;
    let server_addr = LocalEndpointFactory::node_addr(&server_ep).await;

    // Two identities from 127.0.0.1 finish TLS and never send the control hello.
    let mut stalled = Vec::new();
    for seed in [891, 892] {
        let endpoint = LocalEndpointFactory::with_limits(&limits)
            .endpoint(seed)
            .await?;
        let connection = endpoint
            .connect(server_addr.clone(), &[P2P_V2_ALPN])
            .await?;
        stalled.push((endpoint, connection));
    }
    let loopback: IpAddr = Ipv4Addr::LOCALHOST.into();
    let control_count = || supervisor.ip_slots.charged(loopback);
    await_until(
        "two stalled control handshakes",
        Duration::from_secs(5),
        || control_count() == 2,
    )
    .await?;

    // A third identity from the same IP is refused before TLS.
    let third = LocalEndpointFactory::with_limits(&limits)
        .endpoint(893)
        .await?;
    let refused = matches!(
        third.connect(server_addr.clone(), &[P2P_V2_ALPN]).await,
        Err(zakura_quic::ConnectError::Refused)
    );
    assert!(
        refused,
        "stalled control handshakes must fill the per-IP cap"
    );

    // Closing the stalled connections releases their per-IP slots.
    drop(stalled);
    await_until(
        "released control handshakes",
        Duration::from_secs(15),
        || control_count() == 0,
    )
    .await?;

    server_ep.shutdown().await;
    Ok(())
}
