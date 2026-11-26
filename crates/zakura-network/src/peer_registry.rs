//! Active peer connection metadata for local diagnostics.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::task::AtomicWaker;
use zakura_chain::block;
use zakura_quic::{NodeId, MAX_EARLIER_NODE_IDS};

mod delivery;
mod delivery_trace;

use delivery::DeliveryEstimate;
pub(crate) use delivery_trace::{DeliverySampleKind, DeliveryTrace};

use crate::{
    peer::ConnectionInfo,
    protocol::external::types::Version,
    zakura::{ZakuraConnId, ZakuraPeerId},
    PeerSocketAddr,
};

/// A snapshot of one active peer connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectedPeer {
    /// The remote socket address for this direct connection.
    pub addr: PeerSocketAddr,
    /// The sanitized user agent advertised during the handshake.
    pub user_agent: Arc<str>,
    /// The protocol version advertised by the peer.
    pub version: Version,
    /// The best chain height the peer advertised during the handshake.
    pub start_height: block::Height,
    /// Whether the remote peer initiated the connection.
    pub is_inbound: bool,
    /// The latest heartbeat round-trip time.
    pub rtt: Option<Duration>,
    /// The latest heartbeat send time.
    pub ping_sent_at: Option<Instant>,
}

impl ConnectedPeer {
    /// Build a registry entry from a completed legacy handshake.
    pub(crate) fn from_connection_info(connection_info: &ConnectionInfo) -> Option<Self> {
        Some(Self {
            addr: connection_info.connected_addr.diagnostic_remote_addr()?,
            user_agent: sanitize_subversion(&connection_info.remote.user_agent).into(),
            version: connection_info.remote.version,
            start_height: connection_info.remote.start_height,
            is_inbound: connection_info.connected_addr.is_inbound(),
            rtt: None,
            ping_sent_at: None,
        })
    }
}

/// Shared registry for active legacy and Zakura peer connections.
#[derive(Clone, Debug, Default)]
pub(crate) struct PeerRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

#[derive(Debug, Default)]
struct RegistryState {
    next_legacy_generation: u64,
    active_connections: HashMap<ConnectionKey, ConnectedPeer>,
    /// How long each active legacy connection takes to answer, for choosing block sources.
    delivery_estimates: HashMap<ConnectionKey, DeliveryEstimate>,
    native_connections: HashMap<ZakuraPeerId, NativeConnection>,
    retained_native_metadata: HashMap<ZakuraPeerId, ConnectedPeer>,
}

/// The current authenticated native connection of one peer.
#[derive(Clone, Debug)]
struct NativeConnection {
    /// The connection's id.
    conn_id: ZakuraConnId,

    /// The remote IP of the QUIC connection that proved the peer's node ID, if the
    /// transport reported one.
    remote_ip: Option<IpAddr>,

    /// The node IDs the peer had before it added keys, which the connection's handshake
    /// proved as well (zakura-quic TLS-7). At most [`MAX_EARLIER_NODE_IDS`].
    earlier: Vec<NodeId>,
}

/// The native connection that proves a peer is the node known by some node ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeProof {
    /// The remote IP of the QUIC connection, if the transport reported one. It is the
    /// connection's own, not the legacy handshake address attached to it, which a peer
    /// that only relayed the upgrade could have supplied.
    pub(crate) remote_ip: Option<IpAddr>,

    /// The peer id the connection is registered under, when it is not the one asked
    /// for. The peer has added keys since it had that node ID, and its handshake proved
    /// it still holds the keys that ID names.
    pub(crate) moved_to: Option<ZakuraPeerId>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ConnectionKey {
    Legacy(u64),
    Native(ZakuraPeerId, ZakuraConnId),
}

impl PeerRegistry {
    /// Return a point-in-time snapshot of active connections.
    pub(crate) fn connected_peers(&self) -> Vec<ConnectedPeer> {
        let mut connected_peers: Vec<_> = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active_connections
            .values()
            .cloned()
            .collect();
        connected_peers.sort_unstable_by(|left, right| {
            left.addr
                .cmp(&right.addr)
                .then(left.is_inbound.cmp(&right.is_inbound))
                .then(left.version.0.cmp(&right.version.0))
                .then(left.user_agent.cmp(&right.user_agent))
        });
        connected_peers
    }

    /// Return the remote address of every active connection, and whether the remote
    /// peer initiated it.
    pub(crate) fn connection_addrs(&self) -> Vec<(PeerSocketAddr, bool)> {
        self.inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active_connections
            .values()
            .map(|peer| (peer.addr, peer.is_inbound))
            .collect()
    }

    /// Returns the authenticated native connection that proves its peer is the node
    /// known as `peer_id`, or `None` when no native connection does.
    ///
    /// That is the connection registered under `peer_id`. Without one, it is a connection
    /// whose handshake proved `peer_id` as a node ID the peer had before it added keys:
    /// the peer holds the keys `peer_id` names and signed with them. When several such
    /// connections exist, the one registered first answers, so the answer doesn't move
    /// between them.
    pub(crate) fn native_connection_proving(&self, peer_id: &ZakuraPeerId) -> Option<NativeProof> {
        let state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if let Some(connection) = state.native_connections.get(peer_id) {
            return Some(NativeProof {
                remote_ip: connection.remote_ip,
                moved_to: None,
            });
        }
        // Only a moved or disconnected peer gets here. The scan reads at most
        // `MAX_EARLIER_NODE_IDS` IDs for each native connection.
        let earlier = peer_id.node_id()?;
        state
            .native_connections
            .iter()
            .filter(|(_, connection)| connection.earlier.contains(&earlier))
            .min_by_key(|(_, connection)| connection.conn_id)
            .map(|(current, connection)| NativeProof {
                remote_ip: connection.remote_ip,
                moved_to: Some(current.clone()),
            })
    }

    /// Register one legacy connection until the returned guard drops.
    pub(crate) fn register_legacy(
        &self,
        connected_peer: ConnectedPeer,
    ) -> (PeerRegistryGuard, PeerRegistryUpdater) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        let generation = state.next_legacy_generation;
        state.next_legacy_generation = state
            .next_legacy_generation
            .checked_add(1)
            .expect("legacy connection generation cannot overflow in one process");
        let key = ConnectionKey::Legacy(generation);
        state.active_connections.insert(key.clone(), connected_peer);
        drop(state);

        (
            PeerRegistryGuard {
                registry: self.clone(),
                key: key.clone(),
            },
            PeerRegistryUpdater {
                registry: self.clone(),
                key,
                delivery_trace: DeliveryTrace::default(),
                score_waker: Arc::default(),
            },
        )
    }

    /// Attach legacy handshake metadata to one authenticated native connection.
    ///
    /// Returns `false` without changing active or retained metadata when another
    /// `conn_id` currently owns `peer_id`.
    #[must_use]
    pub(crate) fn attach_native_metadata(
        &self,
        peer_id: ZakuraPeerId,
        conn_id: ZakuraConnId,
        connection_metadata: ConnectedPeer,
        retain_for_redial: bool,
    ) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if state
            .native_connections
            .get(&peer_id)
            .map(|connection| connection.conn_id)
            != Some(conn_id)
        {
            return false;
        }
        if retain_for_redial {
            state
                .retained_native_metadata
                .insert(peer_id.clone(), connection_metadata.clone());
        }
        state
            .active_connections
            .insert(ConnectionKey::Native(peer_id, conn_id), connection_metadata);
        true
    }

    /// Register one authenticated native `conn_id`, whose QUIC connection comes from
    /// `remote_ip`, if the transport reported it. `proven_earlier` are the node IDs the
    /// peer had before it added keys, as far as the connection's handshake proved them.
    ///
    /// Retained outbound metadata follows a reconnect to its new `conn_id`.
    pub(crate) fn register_native_connection(
        &self,
        peer_id: ZakuraPeerId,
        conn_id: ZakuraConnId,
        remote_ip: Option<IpAddr>,
        proven_earlier: &[NodeId],
    ) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        let connection = NativeConnection {
            conn_id,
            remote_ip,
            // A handshake proves no more, so the bound doesn't rest on the caller.
            earlier: proven_earlier
                .iter()
                .take(MAX_EARLIER_NODE_IDS)
                .copied()
                .collect(),
        };
        if let Some(previous) = state.native_connections.insert(peer_id.clone(), connection) {
            state
                .active_connections
                .remove(&ConnectionKey::Native(peer_id.clone(), previous.conn_id));
        }
        if let Some(connection_metadata) = state.retained_native_metadata.get(&peer_id).cloned() {
            state
                .active_connections
                .insert(ConnectionKey::Native(peer_id, conn_id), connection_metadata);
        }
    }

    /// Remove one exact authenticated native `conn_id`.
    pub(crate) fn deregister_native_connection(
        &self,
        peer_id: &ZakuraPeerId,
        conn_id: ZakuraConnId,
    ) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if state
            .native_connections
            .get(peer_id)
            .map(|connection| connection.conn_id)
            == Some(conn_id)
        {
            state.native_connections.remove(peer_id);
            state
                .active_connections
                .remove(&ConnectionKey::Native(peer_id.clone(), conn_id));
        }
    }

    /// Forget retained metadata when the maintained native upgrade dial ends.
    pub(crate) fn forget_retained_native_metadata(&self, peer_id: &ZakuraPeerId) {
        self.inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .retained_native_metadata
            .remove(peer_id);
    }

    fn update_connection(&self, key: &ConnectionKey, update: impl FnOnce(&mut ConnectedPeer)) {
        if let Some(connected_peer) = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active_connections
            .get_mut(key)
        {
            update(connected_peer);
        }
    }

    fn remove_connection(&self, key: &ConnectionKey) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        state.active_connections.remove(key);
        state.delivery_estimates.remove(key);
    }
}

/// Updates heartbeat metrics for one exact connection generation.
#[derive(Clone, Debug)]
pub(crate) struct PeerRegistryUpdater {
    registry: PeerRegistry,
    key: ConnectionKey,
    /// Where this connection's delivery samples are traced.
    delivery_trace: DeliveryTrace,
    /// The task to wake when a delivery sample is recorded, shared by this updater's clones.
    /// Empty unless the peer set is holding a second block request that this peer could serve.
    score_waker: Arc<AtomicWaker>,
}

impl PeerRegistryUpdater {
    /// Makes this connection trace its delivery samples with `delivery_trace`.
    pub(crate) fn with_delivery_trace(mut self, delivery_trace: DeliveryTrace) -> Self {
        self.delivery_trace = delivery_trace;
        self
    }

    pub(crate) fn rtt(&self) -> Option<Duration> {
        self.registry
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active_connections
            .get(&self.key)
            .and_then(|peer| peer.rtt)
    }

    pub(crate) fn record_ping_sent(&self, now: Instant) {
        self.registry
            .update_connection(&self.key, |connected_peer| {
                connected_peer.ping_sent_at = Some(now);
            });
    }

    pub(crate) fn record_response(&self, rtt: Duration) {
        self.registry
            .update_connection(&self.key, |connected_peer| {
                connected_peer.rtt = Some(rtt);
                connected_peer.ping_sent_at = None;
            });
        self.record_delivery_sample(DeliverySampleKind::Ping, rtt, Instant::now());
    }

    /// Records how long the peer took, measured at `now`, to answer a ping or a block request.
    ///
    /// Does nothing once the connection has closed, or for a native connection, whose blocks
    /// are not chosen this way.
    ///
    /// Each recorded sample is written to the connection's delivery trace.
    pub(crate) fn record_delivery_sample(
        &self,
        kind: DeliverySampleKind,
        sample: Duration,
        now: Instant,
    ) {
        if !matches!(self.key, ConnectionKey::Legacy(_)) {
            return;
        }
        let mut state = self
            .registry
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if !state.active_connections.contains_key(&self.key) {
            return;
        }
        let estimate = state
            .delivery_estimates
            .entry(self.key.clone())
            .or_default();
        let score_before = estimate.score();
        estimate.record(sample, now);
        let estimate = *estimate;
        drop(state);

        let score = estimate
            .score()
            .expect("an estimate that just recorded a sample has a score");
        self.delivery_trace
            .sample(kind, sample, score_before, score, estimate.weight());
        self.score_waker.wake();
    }

    /// Registers the task polling with `cx` to be woken the next time a delivery sample is
    /// recorded for this connection. Only the last registered task is woken, once.
    ///
    /// Register before reading [`Self::delivery_score`], so that a sample recorded between the
    /// read and the task going to sleep still wakes it.
    pub(crate) fn notify_when_score_changes(&self, cx: &std::task::Context<'_>) {
        self.score_waker.register(cx.waker());
    }

    /// Traces that the peer announced a block and did not deliver it to a first request, or to a
    /// second request when `second_request` is set. The score does not change.
    pub(crate) fn trace_block_missed(&self, second_request: bool) {
        self.delivery_trace.block_missed(second_request);
    }

    /// The expected time for this peer to answer a block request, from its recent answers, or
    /// `None` before its first measured answer.
    pub(crate) fn delivery_score(&self) -> Option<Duration> {
        self.registry
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .delivery_estimates
            .get(&self.key)
            .and_then(DeliveryEstimate::score)
    }
}

/// Removes one exact legacy connection generation on drop.
#[derive(Debug)]
pub(crate) struct PeerRegistryGuard {
    registry: PeerRegistry,
    key: ConnectionKey,
}

impl Drop for PeerRegistryGuard {
    fn drop(&mut self) {
        self.registry.remove_connection(&self.key);
    }
}

/// Returns `user_agent` with every character a peer could use to forge log or trace
/// text removed.
fn sanitize_subversion(user_agent: &str) -> String {
    user_agent
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, ' ' | '.' | '-' | '/' | ':')
        })
        .collect()
}

/// The longest user agent a trace row carries. Real ones, such as `/MagicBean:6.2.0/`,
/// are under 30 characters; a peer may send up to 256.
const TRACE_USER_AGENT_MAX_CHARS: usize = 64;

/// Returns `user_agent` sanitized and cut to [`TRACE_USER_AGENT_MAX_CHARS`], for trace rows.
///
/// A peer chooses its user agent and can trigger many rows, so the cut bounds how much
/// trace file each row it causes can take.
pub(crate) fn trace_user_agent(user_agent: &str) -> String {
    let mut sanitized = sanitize_subversion(user_agent);
    // Sanitized text is ASCII, so this byte index is a character boundary.
    sanitized.truncate(TRACE_USER_AGENT_MAX_CHARS);
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(port: u16) -> ConnectedPeer {
        ConnectedPeer {
            addr: format!("127.0.0.1:{port}")
                .parse()
                .expect("test address parses"),
            user_agent: Arc::from("/Zakura:1.0.0/"),
            version: Version(1),
            start_height: block::Height(0),
            is_inbound: false,
            rtt: None,
            ping_sent_at: None,
        }
    }

    fn peer_id(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is valid")
    }

    #[test]
    fn legacy_guard_removes_only_its_generation() {
        let registry = PeerRegistry::default();
        let (first, _) = registry.register_legacy(peer(1));
        let (_second, _) = registry.register_legacy(peer(2));

        drop(first);

        assert_eq!(registry.connected_peers(), vec![peer(2)]);
    }

    #[test]
    fn native_replacement_ignores_stale_disconnect() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(1);
        registry.register_native_connection(peer_id.clone(), 1, None, &[]);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), true));
        registry.register_native_connection(peer_id.clone(), 2, None, &[]);

        registry.deregister_native_connection(&peer_id, 1);
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 2);
        assert!(registry.connected_peers().is_empty());
    }

    #[test]
    fn a_legacy_connection_s_delivery_score_lasts_as_long_as_the_connection() {
        let registry = PeerRegistry::default();
        let (guard, updater) = registry.register_legacy(peer(1));
        assert_eq!(updater.delivery_score(), None);

        updater.record_response(Duration::from_millis(30));
        assert_eq!(updater.delivery_score(), Some(Duration::from_millis(30)));

        drop(guard);
        assert_eq!(updater.delivery_score(), None);
        updater.record_delivery_sample(
            DeliverySampleKind::Block,
            Duration::from_millis(5),
            Instant::now(),
        );
        assert_eq!(
            updater.delivery_score(),
            None,
            "a closed connection keeps no estimate"
        );
    }

    #[tokio::test]
    async fn delivery_samples_and_missed_blocks_are_traced() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = zakura_jsonl_trace::JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let registry = PeerRegistry::default();
        let (_connection, updater) = registry.register_legacy(peer(1));
        let updater = updater.with_delivery_trace(DeliveryTrace::new(
            zakura_jsonl_trace::JsonlEventEmitter::new(guard.tracer(), "test-node"),
            7,
        ));

        let now = Instant::now();
        updater.record_delivery_sample(DeliverySampleKind::Ping, Duration::from_millis(10), now);
        updater.record_delivery_sample(DeliverySampleKind::Block, Duration::from_millis(30), now);
        updater.trace_block_missed(true);
        drop(updater);
        guard.shutdown().await;

        let rows: Vec<serde_json::Value> =
            std::fs::read_to_string(dir.path().join("delivery_score.jsonl"))
                .expect("delivery trace is written")
                .lines()
                .map(|line| serde_json::from_str(line).expect("trace row is valid JSON"))
                .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["event"], "delivery_sample");
        assert_eq!(rows[0]["connection_id"], 7);
        assert_eq!(rows[0]["kind"], "ping");
        assert_eq!(rows[0]["sample_ms"], 10);
        assert!(
            rows[0].get("score_before_ms").is_none(),
            "the first sample has no earlier score"
        );
        assert_eq!(rows[0]["score_ms"], 10);
        assert_eq!(rows[0]["weight"], 1.0);
        assert_eq!(rows[1]["kind"], "block");
        assert_eq!(rows[1]["sample_ms"], 30);
        assert_eq!(rows[1]["score_before_ms"], 10);
        assert_eq!(
            rows[1]["score_ms"], 20,
            "two equal-weight samples are averaged"
        );
        assert_eq!(rows[1]["weight"], 2.0);
        assert_eq!(rows[2]["event"], "block_missed");
        assert_eq!(rows[2]["connection_id"], 7);
        assert_eq!(rows[2]["request"], "second");
        assert!(
            rows[2].get("score_ms").is_none(),
            "a missed block leaves the score alone"
        );
    }

    /// A pin or any other caller that knows a peer by a node ID finds the connection
    /// registered under it, or one whose handshake proved it as an earlier ID. A
    /// connection that proved neither answers for nothing.
    #[test]
    fn a_native_connection_proves_its_id_and_the_earlier_ids_of_its_handshake() {
        let node = |byte: u8| NodeId::from_bytes(&[byte; 32]);
        let id = |byte: u8| ZakuraPeerId::from_node_id(node(byte));
        let ip = |last: u8| Some(IpAddr::from([192, 0, 2, last]));
        let registry = PeerRegistry::default();
        assert_eq!(registry.native_connection_proving(&id(1)), None);

        // The peer known as 1 added a key and registers as 2.
        registry.register_native_connection(id(2), 7, ip(2), &[node(1)]);
        assert_eq!(
            registry.native_connection_proving(&id(2)),
            Some(NativeProof {
                remote_ip: ip(2),
                moved_to: None,
            })
        );
        assert_eq!(
            registry.native_connection_proving(&id(1)),
            Some(NativeProof {
                remote_ip: ip(2),
                moved_to: Some(id(2)),
            })
        );
        assert_eq!(registry.native_connection_proving(&id(3)), None);
        // A legacy peer id is not a node ID, so no handshake proved it.
        let legacy = ZakuraPeerId::new(vec![1; 20]).expect("test peer id is valid");
        assert_eq!(registry.native_connection_proving(&legacy), None);

        // A connection registered under the exact ID answers first.
        registry.register_native_connection(id(1), 9, ip(1), &[]);
        assert_eq!(
            registry.native_connection_proving(&id(1)),
            Some(NativeProof {
                remote_ip: ip(1),
                moved_to: None,
            })
        );
        registry.deregister_native_connection(&id(1), 9);

        // Of two connections that prove the ID, the one registered first, which has
        // the lower connection id, answers.
        registry.register_native_connection(id(4), 3, ip(4), &[node(1)]);
        assert_eq!(
            registry
                .native_connection_proving(&id(1))
                .and_then(|proof| proof.moved_to),
            Some(id(4))
        );
        registry.deregister_native_connection(&id(4), 3);
        registry.deregister_native_connection(&id(2), 7);
        assert_eq!(registry.native_connection_proving(&id(1)), None);
    }

    /// The registry keeps no more earlier IDs for a connection than a handshake proves.
    #[test]
    fn a_native_connection_keeps_a_bounded_number_of_earlier_ids() {
        let node = |byte: u8| NodeId::from_bytes(&[byte; 32]);
        let registry = PeerRegistry::default();
        let many: Vec<NodeId> = (1..).take(MAX_EARLIER_NODE_IDS + 2).map(node).collect();
        registry.register_native_connection(peer_id(0xFF), 1, None, &many);

        let state = registry.inner.lock().expect("the mutex is not poisoned");
        assert_eq!(
            state.native_connections[&peer_id(0xFF)].earlier,
            many[..MAX_EARLIER_NODE_IDS]
        );
    }

    #[test]
    fn native_metadata_can_arrive_after_registration() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(2);
        registry.register_native_connection(peer_id.clone(), 1, None, &[]);
        assert!(registry.connected_peers().is_empty());

        assert!(registry.attach_native_metadata(peer_id, 1, peer(1), true));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);
    }

    #[test]
    fn stale_native_metadata_cannot_replace_active_or_retained_metadata() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(3);
        registry.register_native_connection(peer_id.clone(), 1, None, &[]);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), true));
        registry.register_native_connection(peer_id.clone(), 2, None, &[]);

        assert!(!registry.attach_native_metadata(peer_id.clone(), 1, peer(2), true));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 2);
        registry.register_native_connection(peer_id, 3, None, &[]);
        assert_eq!(registry.connected_peers(), vec![peer(1)]);
    }

    #[test]
    fn transient_native_metadata_does_not_survive_reconnect() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(4);
        registry.register_native_connection(peer_id.clone(), 1, None, &[]);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), false));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 1);
        registry.register_native_connection(peer_id, 2, None, &[]);
        assert!(registry.connected_peers().is_empty());
    }

    #[test]
    fn heartbeat_updates_do_not_revive_dropped_generation() {
        let registry = PeerRegistry::default();
        let (guard, updater) = registry.register_legacy(peer(1));
        updater.record_ping_sent(Instant::now());
        updater.record_response(Duration::from_millis(5));
        assert_eq!(
            registry.connected_peers()[0].rtt,
            Some(Duration::from_millis(5)),
        );

        drop(guard);
        updater.record_response(Duration::from_millis(10));
        assert!(registry.connected_peers().is_empty());
    }

    #[test]
    fn subversion_sanitizer_matches_zcashd_allowed_characters() {
        assert_eq!(
            sanitize_subversion("/Magic Bean:2.1.1-1/\u{1b}[31m\n"),
            "/Magic Bean:2.1.1-1/31m"
        );
    }

    #[test]
    fn trace_user_agent_is_sanitized_and_cut_to_its_limit() {
        assert_eq!(trace_user_agent("/MagicBean:6.2.0/\n"), "/MagicBean:6.2.0/");

        let long = format!("/{}/", "a".repeat(254));
        let traced = trace_user_agent(&long);
        assert_eq!(traced.len(), TRACE_USER_AGENT_MAX_CHARS);
        assert!(long.starts_with(&traced));
    }
}
