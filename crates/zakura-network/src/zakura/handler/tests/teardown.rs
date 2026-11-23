//! Every exit returns a connection's transport charges to their baseline.
//!
//! - conn-21: a replaced registration keeps its per-IP charge, and each of
//!   its holders keeps its own charge, until that holder ends.
//! - conn-22: after a disconnect, a panic and a cancellation, the IP slots,
//!   frame grants, queued output, owners, connection slots and handshake
//!   permits return to the values they had before the peer arrived.

use super::*;
use crate::zakura::transport::OwnerCounts;
use crate::zakura::{
    Peer, Service, ServiceRegistry, SessionOpening, SessionPolicy, StreamQueueDepths,
    ZakuraEndpoint,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// A replaced connection still runs until its cleanup guard ends it. Until
/// then its IP stays charged for it, beside the replacement's charge, so the
/// IP's count equals its live connections. Its frame and worker hold their own
/// transport charges, and each holder returns only its own charge when it
/// drops.
#[tokio::test]
async fn a_replaced_connection_keeps_each_charge_until_its_holder_drops() {
    const FRAME: u64 = 512;
    let supervisor = ZakuraSupervisorHandle::new(1);
    let registry = Arc::new(ServiceRegistry::new(Vec::new()).expect("an empty registry is valid"));
    let peer = test_peer(26);
    let ip: IpAddr = "203.0.113.26".parse().expect("test ip parses");
    let stream = Stream {
        kind: 970,
        version: 1,
        frame_cap: 1024,
        capability: 1 << 52,
        ..Stream::PERSISTENT
    };
    let node = NodeResources::new(
        ReactorShares::from_quic(&QuicConfig::default()),
        [stream],
        1024,
    );

    // The old connection registers, installs a stream worker and admits one frame.
    let old_token = CancellationToken::new();
    let old_conn = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            Some(ip),
            [0x80; TRANSCRIPT_HASH_BYTES],
            old_token.clone(),
        )
        .await,
    );
    let old_guard = RegisteredPeerCleanupGuard::new(
        supervisor.clone(),
        registry.clone(),
        peer.clone(),
        old_conn,
        old_token.clone(),
    );
    let old_connection = node.connection(stream.capability);
    let old_worker = old_connection
        .stream(stream.kind, old_connection.owner())
        .expect("the connection negotiated the stream's kind");
    old_worker.owner.advance(OwnerPhase::Installed);
    let old_frame = old_worker
        .raw
        .try_admit(FRAME)
        .expect("an empty gate admits the frame");

    // A winning transcript replaces it from the same IP.
    let new_token = CancellationToken::new();
    let new_conn = registered_conn_id(
        register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            Some(ip),
            [0x10; TRANSCRIPT_HASH_BYTES],
            new_token.clone(),
        )
        .await,
    );
    let new_guard = RegisteredPeerCleanupGuard::new(
        supervisor.clone(),
        registry,
        peer.clone(),
        new_conn,
        new_token.clone(),
    );
    assert!(
        old_token.is_cancelled(),
        "the replacement ends the old connection"
    );
    assert!(!new_token.is_cancelled());

    let current = |supervisor: ZakuraSupervisorHandle| async move {
        let state = supervisor.inner.lock().await;
        state.debug_assert_accounting();
        state
            .active_by_peer
            .get(&test_peer(26))
            .map(|entry| entry.conn_id)
    };
    assert_eq!(current(supervisor.clone()).await, Some(new_conn));
    assert_eq!(
        supervisor.ip_slots.registered(ip),
        2,
        "the IP is charged once per live connection"
    );
    assert_eq!(node.usage().raw_bytes(), FRAME);
    assert_eq!(node.usage().owners.installed, 1);

    // The worker ends first. Only its owner count returns.
    drop(old_worker);
    drop(old_connection);
    assert_eq!(node.usage().owners, OwnerCounts::default());
    assert_eq!(
        node.usage().raw_bytes(),
        FRAME,
        "the frame keeps its charge"
    );
    assert_eq!(supervisor.ip_slots.registered(ip), 2);

    // The old connection's cleanup returns its IP charge only. The current
    // generation stays with the replacement.
    old_guard.cleanup_registered_peer().await;
    assert_eq!(supervisor.ip_slots.registered(ip), 1);
    assert_eq!(current(supervisor.clone()).await, Some(new_conn));
    assert!(!new_token.is_cancelled());
    assert_eq!(
        node.usage().raw_bytes(),
        FRAME,
        "the frame outlives its connection"
    );

    // The frame's last holder drops.
    drop(old_frame);
    assert_eq!(node.usage().raw_bytes(), 0);

    // The replacement's cleanup returns the last charge.
    new_guard.cleanup_registered_peer().await;
    assert_eq!(supervisor.ip_slots.registered_total(), 0);
    assert_eq!(current(supervisor.clone()).await, None);
    assert!(supervisor.inner.lock().await.ip_charges.is_empty());
}

/// The holding service's stream. Its inbound queue holds one frame beside
/// the one its session holds.
const HOLD: Stream = Stream {
    kind: 971,
    version: 1,
    frame_cap: 4096,
    capability: 1 << 53,
    queue_depths: Some(StreamQueueDepths {
        inbound: 1,
        outbound: 2,
    }),
    ..Stream::PERSISTENT
};

/// Frames each session queues to its peer: one the peer's session holds, one
/// in the peer's inbound queue, and the rest in transport buffers.
const QUEUED_FRAMES: usize = 4;

/// A service whose sessions queue frames to the peer, then receive one frame
/// and hold it until their session ends or the test makes them panic.
#[derive(Debug)]
struct HoldingService {
    streams: [Stream; 1],
    /// Sessions that hold a received frame.
    holding: watch::Sender<usize>,
    /// While true, every holding session panics.
    panic: watch::Sender<bool>,
}

impl HoldingService {
    fn new() -> Self {
        Self {
            streams: [HOLD],
            holding: watch::channel(0).0,
            panic: watch::channel(false).0,
        }
    }
}

impl Service for HoldingService {
    fn name(&self) -> &'static str {
        "teardown_holder"
    }

    fn streams(&self) -> &[Stream] {
        &self.streams
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::InitiatorOnly,
            reopen: false,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((mut recv, send)) = peer.take_stream(HOLD.kind) else {
            peer.service_cancel_token().cancel();
            return;
        };
        let cancel = peer.service_cancel_token();
        let holding = self.holding.clone();
        let mut panic = self.panic.subscribe();
        tokio::spawn(async move {
            for _ in 0..QUEUED_FRAMES {
                let frame = Frame {
                    message_type: 1,
                    flags: 0,
                    payload: vec![7; 2048],
                };
                // A full queue leaves the rest unsent, which is enough.
                if send.try_send(frame).is_err() {
                    break;
                }
            }
            let frame = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                frame = recv.recv() => frame,
            };
            let Some(_held) = frame else { return };
            holding.send_modify(|holding| *holding += 1);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {}
                _ = panic.wait_for(|panic| *panic) => {
                    panic!("the holding session panics while it holds a frame");
                }
            }
            holding.send_modify(|holding| *holding -= 1);
            // `recv`, `send` and the held frame drop here.
            drop((recv, send));
        });
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

/// A node with a [`HoldingService`] behind a real handler.
struct HoldingNode {
    endpoint: ZakuraEndpoint,
    handler: ZakuraProtocolHandler,
    service: Arc<HoldingService>,
    limits: ZakuraLocalLimits,
}

impl HoldingNode {
    async fn spawn(seed: u64) -> Result<Self, BoxError> {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let quic = LocalEndpointFactory::with_limits(&limits)
            .endpoint(seed)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(4);
        let service = Arc::new(HoldingService::new());
        let services: Vec<Arc<dyn Service>> = vec![service.clone()];
        let handler = ZakuraProtocolHandler::new_with_registry(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
            Arc::new(ServiceRegistry::new(services)?),
        )
        .with_local_node_id(quic.local_id());
        quic.serve(handler.clone())?;
        Ok(Self {
            endpoint: ZakuraEndpoint::from_parts(quic, supervisor, handler.clone()),
            handler,
            service,
            limits,
        })
    }

    fn id(&self) -> ZakuraPeerId {
        ZakuraPeerId::new(self.endpoint.quic.local_id().as_bytes().to_vec())
            .expect("an endpoint id is a valid peer id")
    }

    /// Every transport charge this node's handler holds.
    async fn level(&self) -> TransportLevel {
        let handler = &self.handler;
        let usage = handler.resources.usage();
        let loopback = handler
            .supervisor
            .ip_slots
            .charged(Ipv4Addr::LOCALHOST.into());
        let (registered, connections, inbound_connections, pending, inbound_pending) = {
            let counts = handler.supervisor.ip_slots.counts();
            (
                counts.registered_total(),
                counts.connections(),
                counts.inbound_connections(),
                counts.pending(),
                counts.inbound_pending(),
            )
        };
        let state = handler.supervisor.inner.lock().await;
        TransportLevel {
            registered,
            loopback,
            ip_charges: state.ip_charges.len(),
            active: state.active_by_peer.len(),
            connections,
            inbound_connections,
            pending,
            inbound_pending,
            owners: usage.owners,
            raw_bytes: usage.raw_bytes(),
            output_bytes: usage.output_bytes(),
            compat_bytes: usage.compat_response_bytes,
            permits: [
                handler.pending_handshakes.available_permits(),
                handler.inbound_handshakes.available_permits(),
            ],
        }
    }

    /// Wait until this node's transport returns to `baseline`.
    async fn settles_to(&self, baseline: &TransportLevel, after: &str) -> Result<(), BoxError> {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let level = self.level().await;
            if &level == baseline {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "after {after}, the transport holds {level:?}, not its baseline {baseline:?}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn shutdown(self) {
        self.endpoint.quic.shutdown().await;
    }
}

/// Every transport charge of one node, as [`HoldingNode::level`] reads it.
#[derive(Debug, PartialEq, Eq)]
struct TransportLevel {
    registered: usize,
    /// Registered and handshaking connections charged to the dialer's IP.
    loopback: usize,
    ip_charges: usize,
    active: usize,
    connections: usize,
    inbound_connections: usize,
    pending: usize,
    inbound_pending: usize,
    owners: OwnerCounts,
    raw_bytes: u64,
    output_bytes: u64,
    compat_bytes: u64,
    /// Available handshake and inbound handshake permits.
    permits: [usize; 2],
}

/// How the peer leaves.
#[derive(Clone, Copy, Debug)]
enum Exit {
    /// The peer closes the connection.
    RemoteDisconnect,
    /// This node cancels the connection.
    LocalCancel,
    /// This node's session panics while it holds a frame, then this node
    /// cancels the connection.
    PanicThenCancel,
}

/// Connect a dialer to a fresh node, let both sides hold frames, end the
/// connection by `exit`, and wait for the node's transport to return to the
/// level it had before the dialer arrived.
async fn charges_return_after(exit: Exit, seed: u64) -> Result<(), BoxError> {
    let node = HoldingNode::spawn(seed).await?;
    let dialer = HoldingNode::spawn(seed + 1).await?;
    let baseline = node.level().await;

    let addr = LocalEndpointFactory::node_addr(&node.endpoint.quic).await;
    let dialing = dialer.endpoint.clone();
    let limits = dialer.limits.clone();
    let _dial = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        let _ = dialing.dial(addr, &limits).await;
    }));

    // Both sides hold a received frame, so the node holds a frame grant, a
    // queued frame and its stream owners.
    let mut holding = node.service.holding.subscribe();
    timeout(TEST_TIMEOUT, holding.wait_for(|holding| *holding == 1)).await??;
    let mut dialer_holding = dialer.service.holding.subscribe();
    timeout(
        TEST_TIMEOUT,
        dialer_holding.wait_for(|holding| *holding == 1),
    )
    .await??;
    let held = node.level().await;
    assert_eq!(held.registered, 1);
    assert_eq!(held.ip_charges, 1);
    assert!(
        held.raw_bytes > 0,
        "the held frame keeps its grant: {held:?}"
    );
    assert!(held.owners.installed > 0);
    assert_ne!(held.connections, baseline.connections);

    match exit {
        Exit::RemoteDisconnect => {
            assert!(dialer.endpoint.supervisor.disconnect_peer(&node.id()).await);
        }
        Exit::LocalCancel => {
            assert!(node.endpoint.supervisor.disconnect_peer(&dialer.id()).await);
        }
        Exit::PanicThenCancel => {
            node.service.panic.send_replace(true);
            // The panic drops the session's frame and handles. Its frame
            // grants return while the connection stays up.
            let deadline = Instant::now() + TEST_TIMEOUT;
            while node.level().await.raw_bytes != 0 {
                if Instant::now() >= deadline {
                    return Err("the panicked session's frame grants never returned".into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                node.level().await.registered,
                1,
                "a panicked session leaves its connection up"
            );
            assert!(node.endpoint.supervisor.disconnect_peer(&dialer.id()).await);
        }
    }

    node.settles_to(&baseline, &format!("{exit:?}")).await?;
    dialer.shutdown().await;
    node.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_remote_disconnect_returns_every_transport_charge() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    charges_return_after(Exit::RemoteDisconnect, 97_100).await
}

#[tokio::test]
async fn a_local_cancel_returns_every_transport_charge() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    charges_return_after(Exit::LocalCancel, 97_110).await
}

#[tokio::test]
async fn a_session_panic_returns_its_frame_grants_and_teardown_the_rest() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    charges_return_after(Exit::PanicThenCancel, 97_120).await
}
