//! The node-wide budget for native peers' transport credit.
//!
//! Each live native QUIC connection, from its handshake until it closes, can
//! make this node hold [`zakura_quic::connection_memory_bytes`] in QUIC
//! buffers (zakura-quic CTRL-26). Regulation pauses reads on purpose, so that
//! credit is where backpressure lands, and QUIC can't take it back. Once a
//! connection closes, the driver releases its stream buffers, and it holds at
//! most [`zakura_quic::CLOSING_CONNECTION_MEMORY_BYTES`] until it drains
//! (ADM-13).
//!
//! Each live slot the budget funds also funds
//! [`ZAKURA_CLOSING_CONNECTIONS_PER_LIVE`] closing slots. The transport caps
//! live connections, admitted or not (ADM-12), and closing ones separately,
//! so closed connections never take a live slot. `max_pending_handshakes` of
//! the live slots are headroom for handshakes not yet admitted and extra
//! address attempts of one dial. The node admits the rest. Half the headroom
//! is a dial reserve that inbound attempts can't take, so peers that churn
//! inbound connections can't stop this node's own dials.
//!
//! The budget lowers the connection count, never the windows, so every
//! connection keeps the throughput that regulation sizing assumes (see
//! `regulation::sizing`). The overheads that
//! [`zakura_quic::connection_memory_bytes`] lists come on top.

use thiserror::Error;

use super::{
    discovery::effective_discovery_connection_headroom, DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
    DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW,
};

/// Default budget for native QUIC connections' flow-control credit and
/// queued datagrams: 8 GiB. CTRL-26 in the zakura-quic spec lists what it
/// leaves out.
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

/// Closing connections the budget funds for each live one (ADM-13).
pub const ZAKURA_CLOSING_CONNECTIONS_PER_LIVE: usize = 2;

/// Connections the default budget admits at the default windows: 102 live
/// QUIC connections, each with two closing slots, less 32 of handshake
/// headroom, 70.
pub const DEFAULT_ZAKURA_BUDGETED_CONNECTIONS: usize = max_connections_for_budget(
    DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    live_slot_bytes(zakura_quic::connection_memory_bytes(
        DEFAULT_ZAKURA_RECEIVE_WINDOW,
        DEFAULT_ZAKURA_SEND_WINDOW,
    )),
) - DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;

/// Returns what the budget charges per live slot: a live connection of
/// `connection_bytes` and its [`ZAKURA_CLOSING_CONNECTIONS_PER_LIVE`] closing
/// slots.
pub const fn live_slot_bytes(connection_bytes: u64) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    connection_bytes.saturating_add(
        zakura_quic::CLOSING_CONNECTION_MEMORY_BYTES
            .saturating_mul(ZAKURA_CLOSING_CONNECTIONS_PER_LIVE as u64),
    )
}

/// Returns how many connections of `connection_bytes` each fit in
/// `budget_bytes`, rounding down.
///
/// # Panics
///
/// If `connection_bytes` is zero.
pub const fn max_connections_for_budget(budget_bytes: u64, connection_bytes: u64) -> usize {
    let connections = budget_bytes / connection_bytes;
    // Clamp before narrowing on targets where usize is smaller than u64.
    if connections > usize::MAX as u64 {
        usize::MAX
    } else {
        connections as usize
    }
}

/// `[network.zakura] peer_memory_budget_bytes` admits fewer connections than
/// [`min_budgeted_connections`] requires.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error(
    "invalid [network.zakura] peer_memory_budget_bytes: {budget_bytes} bytes funds \
     {funded} live QUIC connections of {connection_bytes} bytes each (the \
     [network.zakura.quic] receive_window_bytes and send_window_bytes plus the \
     connection's datagram queue, and two closing slots), and {headroom} of them are \
     max_pending_handshakes headroom, which admits {admitted}; raise \
     peer_memory_budget_bytes or lower those windows or max_pending_handshakes so it \
     admits at least {required}"
)]
pub struct PeerMemoryBudgetError {
    /// The configured budget.
    pub budget_bytes: u64,
    /// What the budget charges per live slot at the configured windows (see
    /// [`live_slot_bytes`]).
    pub connection_bytes: u64,
    /// Live QUIC connections the budget funds.
    pub funded: usize,
    /// Funded live connections kept for handshakes and dial attempts.
    pub headroom: usize,
    /// Connections the budget admits.
    pub admitted: usize,
    /// Connections the budget must admit.
    pub required: usize,
}

/// Returns how many connections `budget_bytes` admits for connections of
/// `connection_bytes`: the live slots it funds (see [`live_slot_bytes`]) less
/// `headroom` for QUIC connections the node holds but hasn't admitted.
pub fn budget_admitted_connections(
    budget_bytes: u64,
    connection_bytes: u64,
    headroom: usize,
) -> usize {
    max_connections_for_budget(budget_bytes, live_slot_bytes(connection_bytes))
        .saturating_sub(headroom)
}

/// Returns the connection cap after the budget: `max_connections`, lowered to
/// the live slots `budget_bytes` funds for connections of `connection_bytes`
/// (see [`live_slot_bytes`]), less `headroom` for QUIC connections the node
/// holds but hasn't admitted.
///
/// Fails when the budget admits fewer than [`min_budgeted_connections`] for
/// `bootstrap_peer_count`, or fewer than `max_connections` when that is
/// smaller.
pub fn budgeted_max_connections(
    max_connections: usize,
    budget_bytes: u64,
    connection_bytes: u64,
    bootstrap_peer_count: usize,
    headroom: usize,
) -> Result<usize, PeerMemoryBudgetError> {
    let max_connections = max_connections.max(1);
    let admitted = budget_admitted_connections(budget_bytes, connection_bytes, headroom);
    let connection_bytes = live_slot_bytes(connection_bytes);
    let funded = max_connections_for_budget(budget_bytes, connection_bytes);
    let required = max_connections.min(min_budgeted_connections(bootstrap_peer_count));
    if admitted < required {
        return Err(PeerMemoryBudgetError {
            budget_bytes,
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

    #[test]
    fn the_default_connection_cap_fits_the_default_budget() {
        let connection_bytes = QuicConfig::default().connection_memory_bytes();
        assert_eq!(connection_bytes, 80 * MIB);
        assert_eq!(DEFAULT_ZAKURA_BUDGETED_CONNECTIONS, 70);
        assert_eq!(
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
            DEFAULT_ZAKURA_BUDGETED_CONNECTIONS
        );
        // Every live QUIC connection the transport may hold, with its two
        // closing slots, fits the budget.
        // Widening usize to u64 is lossless on supported targets.
        let held = DEFAULT_ZAKURA_MAX_CONNECTIONS + DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;
        let connection_bytes = live_slot_bytes(connection_bytes);
        assert_eq!(connection_bytes, 80 * MIB + 128 * 1024);
        let committed = held as u64 * connection_bytes;
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
        assert_eq!(config.budgeted_max_connections(), Ok(70));

        // Raising the windows lowers the cap instead of the throughput.
        let config = ZakuraConfig {
            quic: QuicConfig {
                receive_window_bytes: 64 << 20,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        // 8 GiB / (64 + 32 + 16) MiB, less 32 of headroom.
        assert_eq!(config.budgeted_max_connections(), Ok(41));

        // So does raising the handshake headroom.
        let config = ZakuraConfig {
            max_pending_handshakes: 64,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(38));

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
            // 32 slots of handshake headroom plus 30 admitted.
            peer_memory_budget_bytes: (32 + 30) * live_slot_bytes(80 * MIB),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 33);
        // Leaving this node's own entry out lowers the floor to match discovery.
        assert_eq!(
            config
                .budgeted_max_connections_for_node(node_ids[0])
                .unwrap_err()
                .required,
            31
        );

        // Extra addresses for one identity and unparsable entries don't count.
        let config = ZakuraConfig {
            bootstrap_peers: (1..=16)
                .map(|port| format!("{}@192.0.2.1:{port}", node_ids[0]))
                .chain(["not a bootstrap peer".to_string()])
                .collect(),
            peer_memory_budget_bytes: (32 + 30) * live_slot_bytes(80 * MIB),
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
    fn local_limits_keep_the_budget_when_only_the_node_floor_passes() {
        // Nine identities including this node's own, and a budget that admits
        // 18 after 32 slots of handshake headroom: the node-specific floor
        // (17) passes, the generic one (19) doesn't, and the limits must still
        // stop at 18.
        let node_ids: Vec<NodeId> = (1..=9)
            .map(|seed| NodeSecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut config = Config::default();
        config.zakura.bootstrap_peers = node_ids
            .iter()
            .map(|node_id| format!("{node_id}@192.0.2.1:8234"))
            .collect();
        config.zakura.max_connections = 256;
        config.zakura.peer_memory_budget_bytes = (18 + 32) * live_slot_bytes(80 * MIB);
        assert!(config.zakura.budgeted_max_connections().is_err());
        assert_eq!(
            config.zakura.budgeted_max_connections_for_node(node_ids[0]),
            Ok(18)
        );
        assert_eq!(ZakuraLocalLimits::from_config(&config).max_connections, 18);
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
        // 8 GiB / (1024 + 32 + 16) MiB.
        assert_eq!(error.funded, 7);
        assert_eq!(error.admitted, 0);
        assert_eq!(error.required, 19);
        assert!(error
            .to_string()
            .contains("[network.zakura] peer_memory_budget_bytes"));
        assert!(error.to_string().contains("receive_window_bytes"));
        assert!(error.to_string().contains("max_pending_handshakes"));

        // 50 funded connections less 32 of headroom leave 18, below Mainnet's 19.
        let config = ZakuraConfig {
            peer_memory_budget_bytes: 50 * live_slot_bytes(80 * MIB),
            ..ZakuraConfig::default()
        };
        let error = config.budgeted_max_connections().unwrap_err();
        assert_eq!((error.funded, error.headroom, error.admitted), (50, 32, 18));

        let config = ZakuraConfig {
            peer_memory_budget_bytes: 0,
            ..ZakuraConfig::default()
        };
        assert!(config.budgeted_max_connections().is_err());

        // A cap below the floor only needs the budget to cover that cap and
        // its headroom.
        let config = ZakuraConfig {
            max_connections: 2,
            max_pending_handshakes: 2,
            peer_memory_budget_bytes: 4 * live_slot_bytes(80 * MIB),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(2));
        let config = ZakuraConfig {
            peer_memory_budget_bytes: 4 * live_slot_bytes(80 * MIB) - 1,
            ..config
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 2);
    }
}
