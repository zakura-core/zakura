//! Active peer connection metadata for local diagnostics.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    peer::ConnectionInfo, protocol::external::types::Version, zakura::ZakuraPeerId, PeerSocketAddr,
};

/// A snapshot of one active peer connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectedPeer {
    /// The transient address for this connection.
    pub addr: PeerSocketAddr,
    /// The sanitized user agent advertised during the handshake.
    pub user_agent: Arc<str>,
    /// The protocol version advertised by the peer.
    pub version: Version,
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
            addr: connection_info.connected_addr.get_transient_addr()?,
            user_agent: sanitize_subversion(&connection_info.remote.user_agent).into(),
            version: connection_info.remote.version,
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
    active: HashMap<ConnectionKey, ConnectedPeer>,
    native_generations: HashMap<ZakuraPeerId, u64>,
    native_metadata: HashMap<ZakuraPeerId, ConnectedPeer>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ConnectionKey {
    Legacy(u64),
    Native(ZakuraPeerId, u64),
}

impl PeerRegistry {
    /// Return a point-in-time snapshot of active connections.
    pub(crate) fn peers(&self) -> Vec<ConnectedPeer> {
        let mut peers: Vec<_> = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active
            .values()
            .cloned()
            .collect();
        peers.sort_by(|left, right| {
            left.addr
                .cmp(&right.addr)
                .then(left.is_inbound.cmp(&right.is_inbound))
                .then(left.version.0.cmp(&right.version.0))
                .then(left.user_agent.cmp(&right.user_agent))
        });
        peers
    }

    /// Register one legacy connection until the returned guard drops.
    pub(crate) fn register_legacy(
        &self,
        peer: ConnectedPeer,
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
        state.active.insert(key.clone(), peer);
        drop(state);

        (
            PeerRegistryGuard {
                registry: self.clone(),
                key: key.clone(),
            },
            PeerRegistryUpdater {
                registry: self.clone(),
                key,
            },
        )
    }

    /// Attach a legacy handshake snapshot to an authenticated native peer.
    pub(crate) fn attach_native_metadata(
        &self,
        peer_id: ZakuraPeerId,
        peer: ConnectedPeer,
        retain_for_redial: bool,
    ) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if retain_for_redial {
            state.native_metadata.insert(peer_id.clone(), peer.clone());
        }
        if let Some(generation) = state.native_generations.get(&peer_id).copied() {
            state
                .active
                .insert(ConnectionKey::Native(peer_id, generation), peer);
        }
    }

    /// Record an authenticated native supervisor generation.
    pub(crate) fn native_connected(&self, peer_id: ZakuraPeerId, generation: u64) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if let Some(previous) = state.native_generations.insert(peer_id.clone(), generation) {
            state
                .active
                .remove(&ConnectionKey::Native(peer_id.clone(), previous));
        }
        if let Some(peer) = state.native_metadata.get(&peer_id).cloned() {
            state
                .active
                .insert(ConnectionKey::Native(peer_id, generation), peer);
        }
    }

    /// Remove one exact native supervisor generation.
    pub(crate) fn native_disconnected(&self, peer_id: &ZakuraPeerId, generation: u64) {
        let mut state = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned");
        if state.native_generations.get(peer_id) == Some(&generation) {
            state.native_generations.remove(peer_id);
            state
                .active
                .remove(&ConnectionKey::Native(peer_id.clone(), generation));
        }
    }

    /// Forget metadata when the maintained native upgrade dial ends.
    pub(crate) fn forget_native_metadata(&self, peer_id: &ZakuraPeerId) {
        self.inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .native_metadata
            .remove(peer_id);
    }

    fn update(&self, key: &ConnectionKey, update: impl FnOnce(&mut ConnectedPeer)) {
        if let Some(peer) = self
            .inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active
            .get_mut(key)
        {
            update(peer);
        }
    }

    fn remove(&self, key: &ConnectionKey) {
        self.inner
            .lock()
            .expect("peer registry mutex is never poisoned")
            .active
            .remove(key);
    }
}

/// Updates heartbeat metrics for one exact connection generation.
#[derive(Clone, Debug)]
pub(crate) struct PeerRegistryUpdater {
    registry: PeerRegistry,
    key: ConnectionKey,
}

impl PeerRegistryUpdater {
    pub(crate) fn record_ping_sent(&self, now: Instant) {
        self.registry.update(&self.key, |peer| {
            peer.ping_sent_at = Some(now);
        });
    }

    pub(crate) fn record_response(&self, rtt: Duration) {
        self.registry.update(&self.key, |peer| {
            peer.rtt = Some(rtt);
            peer.ping_sent_at = None;
        });
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
        self.registry.remove(&self.key);
    }
}

fn sanitize_subversion(user_agent: &str) -> String {
    user_agent
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, ' ' | '.' | '-' | '/' | ':')
        })
        .collect()
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

        assert_eq!(registry.peers(), vec![peer(2)]);
    }

    #[test]
    fn native_replacement_ignores_stale_disconnect() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(1);
        registry.attach_native_metadata(peer_id.clone(), peer(1), true);
        registry.native_connected(peer_id.clone(), 1);
        registry.native_connected(peer_id.clone(), 2);

        registry.native_disconnected(&peer_id, 1);
        assert_eq!(registry.peers(), vec![peer(1)]);

        registry.native_disconnected(&peer_id, 2);
        assert!(registry.peers().is_empty());
    }

    #[test]
    fn native_metadata_can_arrive_after_registration() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(2);
        registry.native_connected(peer_id.clone(), 1);
        assert!(registry.peers().is_empty());

        registry.attach_native_metadata(peer_id, peer(1), true);
        assert_eq!(registry.peers(), vec![peer(1)]);
    }

    #[test]
    fn transient_native_metadata_does_not_survive_reconnect() {
        let registry = PeerRegistry::default();
        let peer_id = peer_id(3);
        registry.native_connected(peer_id.clone(), 1);
        registry.attach_native_metadata(peer_id.clone(), peer(1), false);
        assert_eq!(registry.peers(), vec![peer(1)]);

        registry.native_disconnected(&peer_id, 1);
        registry.native_connected(peer_id, 2);
        assert!(registry.peers().is_empty());
    }

    #[test]
    fn heartbeat_updates_do_not_revive_dropped_generation() {
        let registry = PeerRegistry::default();
        let (guard, updater) = registry.register_legacy(peer(1));
        updater.record_ping_sent(Instant::now());
        updater.record_response(Duration::from_millis(5));
        assert_eq!(registry.peers()[0].rtt, Some(Duration::from_millis(5)));

        drop(guard);
        updater.record_response(Duration::from_millis(10));
        assert!(registry.peers().is_empty());
    }

    #[test]
    fn subversion_sanitizer_matches_zcashd_allowed_characters() {
        assert_eq!(
            sanitize_subversion("/Magic Bean:2.1.1-1/\u{1b}[31m\n"),
            "/Magic Bean:2.1.1-1/31m"
        );
    }
}
