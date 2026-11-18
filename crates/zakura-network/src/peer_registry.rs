//! Active peer connection metadata for local diagnostics.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use zakura_chain::block;

mod delivery;
mod delivery_trace;

use delivery::{BlockSourceHistory, ClosedScores, DeliveryEstimate};
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
    closed_scores: Option<ClosedScores>,
    native_connections: HashMap<ZakuraPeerId, NativeConnection>,
    retained_native_metadata: HashMap<ZakuraPeerId, ConnectedPeer>,
}

/// The current authenticated native connection of one peer.
#[derive(Clone, Copy, Debug)]
struct NativeConnection {
    /// The connection's id.
    conn_id: ZakuraConnId,

    /// The remote IP of the QUIC connection that proved the peer's node ID, if the
    /// transport reported one.
    remote_ip: Option<IpAddr>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ConnectionKey {
    Legacy(u64),
    Native(ZakuraPeerId, ZakuraConnId),
}

impl PeerRegistry {
    /// Retain estimates across legacy connections only when routing by delivery score.
    pub(crate) fn new(remember_scores: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState {
                closed_scores: remember_scores.then(ClosedScores::default),
                ..RegistryState::default()
            })),
        }
    }

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

    /// Returns the remote IP of the authenticated native connection of `peer_id`.
    ///
    /// Returns `None` when no native connection proves `peer_id`, and `Some(None)` when
    /// one does but the transport reported no remote IP. The IP is the QUIC connection's
    /// own, not the legacy handshake address attached to it, which a peer that only
    /// relayed the upgrade could have supplied.
    pub(crate) fn native_connection_ip(&self, peer_id: &ZakuraPeerId) -> Option<Option<IpAddr>> {
        self.inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .native_connections
            .get(peer_id)
            .map(|connection| connection.remote_ip)
    }

    #[cfg(test)]
    pub(crate) fn register_legacy(
        &self,
        connected_peer: ConnectedPeer,
    ) -> (PeerRegistryGuard, PeerRegistryUpdater) {
        self.register_legacy_at(connected_peer, DeliveryTrace::default(), Instant::now())
    }

    /// Register a legacy connection and restore its recent score before it can serve requests.
    pub(crate) fn register_legacy_at(
        &self,
        connected_peer: ConnectedPeer,
        delivery_trace: DeliveryTrace,
        now: Instant,
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
        let history = state
            .closed_scores
            .as_mut()
            .and_then(|scores| scores.take(connected_peer.addr.ip(), now))
            .unwrap_or_default();
        let restored = history.estimate;
        if let Some(estimate) = restored {
            state.delivery_estimates.insert(key.clone(), estimate);
        }
        state.active_connections.insert(key.clone(), connected_peer);
        drop(state);

        if let Some(estimate) = restored {
            let score = estimate
                .score()
                .expect("only measured scores are remembered");
            delivery_trace.sample(
                DeliverySampleKind::CarriedOver,
                None,
                None,
                score,
                estimate.weight(),
            );
        }

        (
            PeerRegistryGuard {
                missed_block: history.missed_block,
                registry: self.clone(),
                key: key.clone(),
            },
            PeerRegistryUpdater {
                registry: self.clone(),
                key,
                delivery_trace,
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
    /// `remote_ip`, if the transport reported it.
    ///
    /// Retained outbound metadata follows a reconnect to its new `conn_id`.
    pub(crate) fn register_native_connection(
        &self,
        peer_id: ZakuraPeerId,
        conn_id: ZakuraConnId,
        remote_ip: Option<IpAddr>,
    ) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        let connection = NativeConnection { conn_id, remote_ip };
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

    fn remove_connection(&self, key: &ConnectionKey, missed_block: &crate::peer::MissedBlock) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        let peer = state.active_connections.remove(key);
        let estimate = state.delivery_estimates.remove(key);
        if let (Some(scores), Some(peer)) = (&mut state.closed_scores, peer) {
            scores.remember(
                peer.addr.ip(),
                BlockSourceHistory {
                    estimate,
                    missed_block: missed_block.clone(),
                },
                Instant::now(),
            );
        }
    }
}

/// Updates heartbeat metrics for one exact connection generation.
#[derive(Clone, Debug)]
pub(crate) struct PeerRegistryUpdater {
    registry: PeerRegistry,
    key: ConnectionKey,
    /// Where this connection's delivery samples are traced.
    delivery_trace: DeliveryTrace,
}

impl PeerRegistryUpdater {
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

    /// Records how long the peer took, measured at `now`, to answer a ping, a transaction
    /// request or a small block request.
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
            .sample(kind, Some(sample), score_before, score, estimate.weight());
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
    /// The missed-block flag shared with the request slot for this connection.
    pub(crate) missed_block: crate::peer::MissedBlock,
    registry: PeerRegistry,
    key: ConnectionKey,
}

impl Drop for PeerRegistryGuard {
    fn drop(&mut self) {
        self.registry
            .remove_connection(&self.key, &self.missed_block);
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
        registry.register_native_connection(peer_id.clone(), 1, None);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), true));
        registry.register_native_connection(peer_id.clone(), 2, None);

        registry.deregister_native_connection(&peer_id, 1);
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 2);
        assert!(registry.connected_peers().is_empty());
    }

    #[test]
    fn disabling_score_retention_keeps_only_live_measurements() {
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
        let (_next, next) = registry.register_legacy(peer(2));
        assert_eq!(next.delivery_score(), None);
    }

    fn peer_at(addr: &str) -> ConnectedPeer {
        ConnectedPeer {
            addr: addr.parse().expect("test socket address"),
            ..peer(0)
        }
    }

    #[test]
    fn a_reconnect_consumes_the_estimate_and_preserves_time_since_the_last_sample() {
        let registry = PeerRegistry::new(true);
        let now = Instant::now();
        let (first, stale) = registry.register_legacy(peer(1));
        stale.record_delivery_sample(DeliverySampleKind::Block, Duration::from_millis(800), now);
        drop(first);
        let later = now + Duration::from_secs(600);
        let (_next, next) = registry.register_legacy_at(peer(2), DeliveryTrace::default(), later);
        let (_parallel, parallel) = registry.register_legacy(peer(3));
        assert_eq!(parallel.delivery_score(), None);
        stale.record_response(Duration::from_millis(1));
        assert_eq!(stale.delivery_score(), None);
        assert_eq!(next.delivery_score(), Some(Duration::from_millis(800)));

        let ping = Duration::from_millis(10);
        next.record_delivery_sample(DeliverySampleKind::Ping, ping, later);
        let mut uninterrupted = DeliveryEstimate::default();
        uninterrupted.record(Duration::from_millis(800), now);
        uninterrupted.record(ping, later);
        assert_eq!(next.delivery_score(), uninterrupted.score());
    }

    #[test]
    fn recent_scores_follow_ipv4_addresses_and_ipv6_prefixes() {
        for (original, reconnect, age, restored) in [
            (
                "192.0.2.1:1",
                "[::ffff:192.0.2.1]:2",
                delivery::REMEMBER_FOR,
                true,
            ),
            ("[::ffff:192.0.2.1]:1", "192.0.2.1:2", Duration::ZERO, true),
            ("192.0.2.1:1", "192.0.2.2:2", Duration::ZERO, false),
            (
                "[2001:db8:1:2::1]:1",
                "[2001:db8:1:2:ffff::5]:2",
                Duration::ZERO,
                true,
            ),
            (
                "[2001:db8:1:2::1]:1",
                "[2001:db8:1:3::1]:1",
                Duration::ZERO,
                false,
            ),
            (
                "192.0.2.1:1",
                "192.0.2.1:2",
                delivery::REMEMBER_FOR + Duration::from_nanos(1),
                false,
            ),
        ] {
            let registry = PeerRegistry::new(true);
            let now = Instant::now();
            let (guard, updater) = registry.register_legacy(peer_at(original));
            updater.record_delivery_sample(
                DeliverySampleKind::Block,
                Duration::from_millis(800),
                now,
            );
            drop(guard);
            let (_guard, next) = registry.register_legacy_at(
                peer_at(reconnect),
                DeliveryTrace::default(),
                now + age,
            );
            assert_eq!(
                next.delivery_score(),
                restored.then_some(Duration::from_millis(800)),
                "{original} to {reconnect} after {age:?}"
            );
        }
    }

    #[test]
    fn the_last_scored_connection_to_close_supplies_the_remembered_score() {
        let registry = PeerRegistry::new(true);
        let now = Instant::now();
        let (older, slow) = registry.register_legacy(peer(1));
        let (newer, fast) = registry.register_legacy(peer(2));
        slow.record_delivery_sample(DeliverySampleKind::Block, Duration::from_millis(800), now);
        fast.record_delivery_sample(
            DeliverySampleKind::Ping,
            Duration::from_millis(10),
            now + Duration::from_secs(1),
        );
        drop(newer);
        drop(older);
        let (_next, next) = registry.register_legacy(peer(3));
        assert_eq!(next.delivery_score(), Some(Duration::from_millis(800)));
    }

    #[test]
    fn a_reconnected_source_remembers_a_miss_even_without_a_score() {
        let now = Instant::now();
        for enabled in [false, true] {
            let registry = PeerRegistry::new(enabled);
            let (old, _) = registry.register_legacy(peer(1));
            old.missed_block.request(now).missed();
            drop(old);
            let (next, updater) = registry.register_legacy_at(
                peer(2),
                DeliveryTrace::default(),
                now + delivery::REMEMBER_FOR,
            );
            assert_eq!(updater.delivery_score(), None);
            assert_eq!(next.missed_block.is_set(), enabled);
            drop(next);
            let (expired, _) = registry.register_legacy_at(
                peer(3),
                DeliveryTrace::default(),
                now + delivery::REMEMBER_FOR + Duration::from_nanos(1),
            );
            assert!(
                !expired.missed_block.is_set(),
                "reconnecting does not renew a miss"
            );
        }
    }

    #[test]
    fn a_disconnect_before_the_result_keeps_the_miss_and_a_later_delivery_clears_it() {
        let registry = PeerRegistry::new(true);
        let now = Instant::now();
        let (old, _) = registry.register_legacy(peer(1));
        let stale_slot = old.missed_block.clone();
        let pending = old.missed_block.request(now);
        let delayed = old.missed_block.request(now);
        drop(old);
        let (next, _) = registry.register_legacy(peer(2));
        assert!(!next.missed_block.is_set());
        pending.missed();
        assert!(
            next.missed_block.is_set(),
            "the result can arrive after reconnect"
        );
        drop(pending);
        next.missed_block.delivered();
        delayed.missed();
        stale_slot.request(Instant::now()).missed();
        assert!(
            !next.missed_block.is_set(),
            "a stale failure cannot undo the new connection's delivery"
        );
        drop(delayed);
        drop(next);
        let (again, _) = registry.register_legacy(peer(3));
        assert!(!again.missed_block.is_set());
    }

    #[test]
    fn score_samples_and_missed_requests_have_separate_expiry_times() {
        let now = Instant::now();
        let fresh = now + delivery::REMEMBER_FOR + Duration::from_secs(1);
        for recent_miss in [true, false] {
            let registry = PeerRegistry::new(true);
            let (old, score) = registry.register_legacy(peer(1));
            let (sampled_at, missed_at) = if recent_miss {
                (now, fresh)
            } else {
                (fresh, now)
            };
            score.record_delivery_sample(
                DeliverySampleKind::Ping,
                Duration::from_millis(5),
                sampled_at,
            );
            old.missed_block.request(missed_at).missed();
            drop(old);
            let (next, next_score) =
                registry.register_legacy_at(peer(2), DeliveryTrace::default(), fresh);
            assert_eq!(next_score.delivery_score().is_some(), !recent_miss);
            assert_eq!(next.missed_block.is_set(), recent_miss);
        }
    }

    #[tokio::test]
    async fn delivery_samples_and_missed_blocks_are_traced() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = zakura_jsonl_trace::JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let registry = PeerRegistry::new(true);
        let trace = |id| {
            DeliveryTrace::new(
                zakura_jsonl_trace::JsonlEventEmitter::new(guard.tracer(), "test-node"),
                id,
            )
        };
        let now = Instant::now();
        let (connection, updater) = registry.register_legacy_at(peer(1), trace(7), now);
        updater.record_delivery_sample(DeliverySampleKind::Ping, Duration::from_millis(10), now);
        updater.record_delivery_sample(DeliverySampleKind::Block, Duration::from_millis(30), now);
        updater.trace_block_missed(true);
        drop(connection);
        let (_restored, next) = registry.register_legacy_at(peer(2), trace(8), now);
        let (_other, _) = registry.register_legacy_at(peer(3), trace(9), now);
        assert_eq!(next.delivery_score(), Some(Duration::from_millis(20)));
        guard.shutdown().await;

        let rows: Vec<serde_json::Value> =
            std::fs::read_to_string(dir.path().join("delivery_score.jsonl"))
                .expect("delivery trace is written")
                .lines()
                .map(|line| serde_json::from_str(line).expect("trace row is valid JSON"))
                .collect();
        assert_eq!(rows.len(), 4);
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
        assert_eq!(rows[3]["event"], "delivery_sample");
        assert_eq!(rows[3]["connection_id"], 8);
        assert_eq!(rows[3]["kind"], "carried_over");
        assert_eq!(rows[3]["score_ms"], 20);
        assert_eq!(rows[3]["weight"], 2.0);
        assert!(rows[3].get("score_before_ms").is_none());
        assert!(rows[3].get("sample_ms").is_none());
    }

    #[test]
    fn native_metadata_can_arrive_after_registration() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(2);
        registry.register_native_connection(peer_id.clone(), 1, None);
        assert!(registry.connected_peers().is_empty());

        assert!(registry.attach_native_metadata(peer_id, 1, peer(1), true));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);
    }

    #[test]
    fn stale_native_metadata_cannot_replace_active_or_retained_metadata() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(3);
        registry.register_native_connection(peer_id.clone(), 1, None);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), true));
        registry.register_native_connection(peer_id.clone(), 2, None);

        assert!(!registry.attach_native_metadata(peer_id.clone(), 1, peer(2), true));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 2);
        registry.register_native_connection(peer_id, 3, None);
        assert_eq!(registry.connected_peers(), vec![peer(1)]);
    }

    #[test]
    fn transient_native_metadata_does_not_survive_reconnect() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(4);
        registry.register_native_connection(peer_id.clone(), 1, None);
        assert!(registry.attach_native_metadata(peer_id.clone(), 1, peer(1), false));
        assert_eq!(registry.connected_peers(), vec![peer(1)]);

        registry.deregister_native_connection(&peer_id, 1);
        registry.register_native_connection(peer_id, 2, None);
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
