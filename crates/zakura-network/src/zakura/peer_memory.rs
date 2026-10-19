//! The node-wide budget for native peers' transport credit.
//!
//! Each live native QUIC connection, from its handshake until it closes, can
//! make this node hold [`zakura_quic::connection_memory_bytes`] in QUIC
//! buffers (zakura-quic CTRL-26). Regulation pauses reads on purpose, so that
//! credit is where backpressure lands, and QUIC can't take it back. Each bound
//! socket can also queue [`zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES`] of
//! datagrams, however many connections it serves. The budget therefore
//! reserves one socket queue per bound socket and funds what is left divided
//! by `connection_memory_bytes` live connections.
//!
//! The transport caps those live connections, admitted or not (ADM-12).
//! `max_pending_handshakes` of them are headroom for handshakes not yet
//! admitted and extra address attempts of one dial, and the node admits the
//! rest. Half the headroom, rounded up, is a dial reserve that inbound attempts
//! can't take, so peers that churn inbound connections can't stop this node's
//! own dials. A closed connection releases its stream buffers and moves to a
//! separate closing allowance of [`ZAKURA_CLOSING_CONNECTIONS_PER_LIVE`] per
//! live slot (ADM-13). Its queued datagrams count against its socket's queue,
//! so the budget charges closing slots nothing more.
//!
//! The budget lowers the connection count, never the windows, so every
//! connection keeps the throughput that regulation sizing assumes (see
//! `regulation::sizing`). Bookkeeping and the overheads that
//! [`zakura_quic::connection_memory_bytes`] lists come on top.

use thiserror::Error;
use zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES;

use super::{
    discovery::effective_discovery_connection_headroom, DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
    DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW,
};

/// Default budget for native QUIC connections' flow-control credit and the
/// bound sockets' datagram queues: 8 GiB. CTRL-26 in the zakura-quic spec lists
/// what it leaves out.
pub const DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Fewest connections a peer memory budget must admit, whatever the bootstrap
/// list (see [`min_budgeted_connections`]).
pub const MIN_ZAKURA_BUDGETED_CONNECTIONS: usize = 16;

/// Returns the fewest connections a budget must admit with
/// `bootstrap_peer_count` remote bootstrap peers, so a lowered cap doesn't
/// leave discovery without a dial slot by construction.
///
/// Discovery keeps its headroom, one slot per bootstrap peer and at least 4,
/// out of its dials, and also counts each connected peer against the rest. So
/// it needs twice the headroom plus one, 19 on Mainnet, to keep one slot with
/// only the bootstrap peers connected. Inbound and other connections share
/// that slot, as they share every slot at any cap. The result is never below
/// [`MIN_ZAKURA_BUDGETED_CONNECTIONS`].
pub fn min_budgeted_connections(bootstrap_peer_count: usize) -> usize {
    effective_discovery_connection_headroom(bootstrap_peer_count)
        .saturating_mul(2)
        .saturating_add(1)
        .max(MIN_ZAKURA_BUDGETED_CONNECTIONS)
}

/// Closing connections the transport keeps for each live slot (ADM-13). The
/// budget charges them nothing: a closed connection's stream buffers are
/// released, and its queued datagrams count against its socket's queue.
pub const ZAKURA_CLOSING_CONNECTIONS_PER_LIVE: usize = 2;

/// Connections the default budget admits at the default windows, with the one
/// socket the default `listen_addr` binds: 124 live connections less 32 of
/// handshake headroom, 92.
pub const DEFAULT_ZAKURA_BUDGETED_CONNECTIONS: usize = max_connections_for_budget(
    DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    1,
    zakura_quic::connection_memory_bytes(DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW),
) - DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;

/// Returns how many connections of `connection_bytes` each fit in
/// `budget_bytes` after reserving one socket datagram queue for each of
/// `sockets`, rounding down.
///
/// # Panics
///
/// If `connection_bytes` is zero.
pub const fn max_connections_for_budget(
    budget_bytes: u64,
    sockets: u64,
    connection_bytes: u64,
) -> usize {
    let reserved = sockets.saturating_mul(SOCKET_DATAGRAM_QUEUE_BYTES);
    let connections = budget_bytes.saturating_sub(reserved) / connection_bytes;
    // Clamp before narrowing on targets where usize is smaller than u64.
    // usize is at most 64 bits on every supported target, so widening
    // usize::MAX to u64 is lossless.
    if connections > usize::MAX as u64 {
        usize::MAX
    } else {
        connections as usize
    }
}

/// Returns how many connections `budget_bytes` admits: the live connections
/// of `connection_bytes` it funds once `sockets` socket queues are reserved
/// (see [`max_connections_for_budget`]), less `headroom` for QUIC connections
/// the node holds but hasn't admitted.
pub fn budget_admitted_connections(
    budget_bytes: u64,
    sockets: u64,
    connection_bytes: u64,
    headroom: usize,
) -> usize {
    max_connections_for_budget(budget_bytes, sockets, connection_bytes).saturating_sub(headroom)
}

/// `[network.zakura] peer_memory_budget_bytes` admits fewer connections than
/// [`min_budgeted_connections`] requires.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error(
    "invalid [network.zakura] peer_memory_budget_bytes: {budget_bytes} bytes funds \
     {funded} live connections of {connection_bytes} bytes each (the \
     [network.zakura.quic] receive_window_bytes plus send_window_bytes) after \
     reserving {socket_queue_bytes} bytes for the bound sockets' datagram queues, \
     and {headroom} of them are max_pending_handshakes headroom, which admits \
     {admitted}; raise peer_memory_budget_bytes or lower those windows or \
     max_pending_handshakes so it admits at least {required}"
)]
pub struct PeerMemoryBudgetError {
    /// The configured budget.
    pub budget_bytes: u64,
    /// Datagram queue bytes reserved for the bound sockets.
    pub socket_queue_bytes: u64,
    /// Transport bytes one connection can hold at the configured windows.
    pub connection_bytes: u64,
    /// Live connections the budget funds.
    pub funded: usize,
    /// Funded live connections kept for handshakes and dial attempts.
    pub headroom: usize,
    /// Connections the budget admits.
    pub admitted: usize,
    /// Connections the budget must admit.
    pub required: usize,
}

/// Returns the connection cap after the budget: `max_connections`, lowered to
/// what `budget_bytes` admits (see [`budget_admitted_connections`]).
///
/// Fails when the budget admits fewer than [`min_budgeted_connections`] for
/// `bootstrap_peer_count`, or fewer than `max_connections` when that is
/// smaller.
pub fn budgeted_max_connections(
    max_connections: usize,
    budget_bytes: u64,
    sockets: u64,
    connection_bytes: u64,
    bootstrap_peer_count: usize,
    headroom: usize,
) -> Result<usize, PeerMemoryBudgetError> {
    let max_connections = max_connections.max(1);
    let funded = max_connections_for_budget(budget_bytes, sockets, connection_bytes);
    let admitted = funded.saturating_sub(headroom);
    let required = max_connections.min(min_budgeted_connections(bootstrap_peer_count));
    if admitted < required {
        return Err(PeerMemoryBudgetError {
            budget_bytes,
            socket_queue_bytes: sockets.saturating_mul(SOCKET_DATAGRAM_QUEUE_BYTES),
            connection_bytes,
            funded,
            headroom,
            admitted,
            required,
        });
    }
    Ok(max_connections.min(admitted))
}

#[cfg(test)]
mod tests {
    use zakura_quic::{NodeId, NodeSecretKey, QuicConfig};

    use super::*;
    use crate::zakura::{
        discovery::discovery_dial_slot_limit, ZakuraConfig, DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS,
        DEFAULT_ZAKURA_MAX_CONNECTIONS, DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
    };
    use crate::{zakura::ZakuraLocalLimits, Config};

    const MIB: u64 = 1024 * 1024;

    /// A budget that admits exactly `connections` at the default windows on
    /// one socket, after the default 32 slots of handshake headroom.
    fn budget_for(connections: u64) -> u64 {
        256 * MIB + (connections + 32) * 64 * MIB
    }

    #[test]
    fn the_default_connection_cap_fits_the_default_budget() {
        let connection_bytes = QuicConfig::default().connection_memory_bytes();
        assert_eq!(connection_bytes, 64 * MIB);
        assert_eq!(DEFAULT_ZAKURA_BUDGETED_CONNECTIONS, 92);
        assert_eq!(
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
            DEFAULT_ZAKURA_BUDGETED_CONNECTIONS
        );
        // Every live connection the transport may hold, admitted or headroom,
        // fits the budget with the one socket the default `listen_addr` binds.
        // Widening usize to u64 is lossless on supported targets.
        let live = DEFAULT_ZAKURA_MAX_CONNECTIONS + DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;
        let committed = live as u64 * connection_bytes + SOCKET_DATAGRAM_QUEUE_BYTES;
        assert!(committed <= DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES);
        assert!(committed + connection_bytes > DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES);

        let config = ZakuraConfig::default();
        assert_eq!(
            config.budgeted_max_connections(),
            Ok(DEFAULT_ZAKURA_MAX_CONNECTIONS)
        );
    }

    #[test]
    fn the_budget_lowers_a_larger_connection_cap() {
        // A config generated before the budget existed still says 256.
        let config = ZakuraConfig {
            max_connections: 256,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(92));

        // Without `listen_addr` the endpoint binds IPv4 and IPv6 loopback, so it
        // reserves two socket queues: (8192 - 512) / 64 - 32.
        let config = ZakuraConfig {
            max_connections: 256,
            listen_addr: None,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(88));

        // Raising the windows lowers the cap instead of the throughput.
        let config = ZakuraConfig {
            quic: QuicConfig {
                receive_window_bytes: 64 << 20,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        // (8 GiB - 256 MiB) / (64 + 32) MiB - 32.
        assert_eq!(config.budgeted_max_connections(), Ok(50));

        // So does raising the handshake headroom: 124 - 64.
        let config = ZakuraConfig {
            max_pending_handshakes: 64,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(60));

        // A smaller cap stays as configured.
        let config = ZakuraConfig {
            max_connections: 4,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(4));
    }

    #[test]
    fn the_floor_leaves_a_discovery_dial_slot_with_every_bootstrap_peer_connected() {
        assert_eq!(
            min_budgeted_connections(DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS.len()),
            19
        );
        for bootstrap_peers in [0, 3, 9, 20, 100] {
            let floor = min_budgeted_connections(bootstrap_peers);
            let headroom = effective_discovery_connection_headroom(bootstrap_peers);
            let slots = |max_connections| {
                discovery_dial_slot_limit(bootstrap_peers, 0, max_connections, headroom, 4)
            };
            assert!(slots(floor) >= 1, "{bootstrap_peers} bootstrap peers");
            if floor > MIN_ZAKURA_BUDGETED_CONNECTIONS {
                assert_eq!(slots(floor - 1), 0, "{bootstrap_peers} bootstrap peers");
            }
        }

        // A custom list of 16 bootstrap peers needs 33 connections.
        let node_ids: Vec<NodeId> = (1..=16)
            .map(|seed| NodeSecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let config = ZakuraConfig {
            bootstrap_peers: node_ids
                .iter()
                .map(|node_id| format!("{node_id}@192.0.2.1:8234"))
                .collect(),
            peer_memory_budget_bytes: budget_for(30),
            ..ZakuraConfig::default()
        };
        let outsider = NodeSecretKey::from_bytes(&[99; 32]).public();
        assert_eq!(
            config
                .budgeted_max_connections_for_node(outsider)
                .unwrap_err()
                .required,
            33
        );
        // Leaving this node's own entry out lowers the floor to match discovery.
        assert_eq!(
            config
                .budgeted_max_connections_for_node(node_ids[0])
                .unwrap_err()
                .required,
            31
        );
        // Without the node key, config load assumes one entry may be its own.
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 31);

        // Extra addresses for one identity and unparsable entries don't count.
        let config = ZakuraConfig {
            bootstrap_peers: (1..=16)
                .map(|port| format!("{}@192.0.2.1:{port}", node_ids[0]))
                .chain(["not a bootstrap peer".to_string()])
                .collect(),
            peer_memory_budget_bytes: budget_for(30),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(30));
    }

    /// A single handshake of headroom still keeps one slot for dials, so
    /// inbound attempts on several sockets can't take every live slot.
    #[test]
    fn the_dial_reserve_is_at_least_one_slot() {
        for (pending, reserve) in [(1, 1), (2, 1), (3, 2), (32, 16)] {
            let mut config = Config::default();
            config.zakura.max_pending_handshakes = pending;
            let bind = ZakuraLocalLimits::from_config(&config).quic_bind_config(Vec::new());
            assert_eq!(bind.dial_reserve, reserve, "{pending} pending handshakes");
        }
        // Zero is treated as one.
        let mut config = Config::default();
        config.zakura.max_pending_handshakes = 0;
        let bind = ZakuraLocalLimits::from_config(&config).quic_bind_config(Vec::new());
        assert_eq!(bind.dial_reserve, 1);
    }

    #[test]
    fn config_load_and_limits_never_outrun_the_node_floor() {
        // Nine identities including this node's own, and a budget that admits
        // 18 after 32 slots of handshake headroom: the endpoint's floor is 17,
        // so config load must accept it too.
        let node_ids: Vec<NodeId> = (1..=9)
            .map(|seed| NodeSecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut config = Config::default();
        config.zakura.bootstrap_peers = node_ids
            .iter()
            .map(|node_id| format!("{node_id}@192.0.2.1:8234"))
            .collect();
        config.zakura.max_connections = 256;
        config.zakura.peer_memory_budget_bytes = budget_for(18);
        assert_eq!(config.zakura.budgeted_max_connections(), Ok(18));
        assert_eq!(
            config.zakura.budgeted_max_connections_for_node(node_ids[0]),
            Ok(18)
        );
        assert_eq!(ZakuraLocalLimits::from_config(&config).max_connections, 18);

        // A budget both checks refuse still caps the limits at its own count.
        config.zakura.peer_memory_budget_bytes = budget_for(16);
        assert!(config.zakura.budgeted_max_connections().is_err());
        assert_eq!(ZakuraLocalLimits::from_config(&config).max_connections, 16);
    }

    #[test]
    fn a_budget_below_the_floor_names_the_key() {
        let config = ZakuraConfig {
            quic: QuicConfig {
                receive_window_bytes: 1 << 30,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        let error = config.budgeted_max_connections().unwrap_err();
        // (8 GiB - 256 MiB) / (1024 + 32) MiB, less 32 of headroom.
        assert_eq!((error.funded, error.headroom, error.admitted), (7, 32, 0));
        // Mainnet's 9 bootstrap peers, counting one as possibly this node.
        assert_eq!(error.required, 17);
        assert!(error
            .to_string()
            .contains("[network.zakura] peer_memory_budget_bytes"));
        assert!(error.to_string().contains("receive_window_bytes"));
        assert!(error.to_string().contains("max_pending_handshakes"));
        assert_eq!(error.socket_queue_bytes, 256 * MIB);

        // 48 funded connections less 32 of headroom leave 16, below the floor.
        let config = ZakuraConfig {
            peer_memory_budget_bytes: budget_for(16),
            ..ZakuraConfig::default()
        };
        let error = config.budgeted_max_connections().unwrap_err();
        assert_eq!((error.funded, error.headroom, error.admitted), (48, 32, 16));

        let config = ZakuraConfig {
            peer_memory_budget_bytes: 0,
            ..ZakuraConfig::default()
        };
        assert!(config.budgeted_max_connections().is_err());

        // A cap below the floor only needs the budget to cover that cap and
        // its headroom.
        let config = ZakuraConfig {
            max_connections: 2,
            peer_memory_budget_bytes: budget_for(2),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(2));
        let config = ZakuraConfig {
            peer_memory_budget_bytes: budget_for(2) - 1,
            ..config
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 2);
    }
}
