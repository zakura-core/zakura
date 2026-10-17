//! The node-wide budget for native peers' transport credit.
//!
//! Each native QUIC connection can make this node hold
//! [`zakura_quic::connection_memory_bytes`] in QUIC buffers (zakura-quic
//! CTRL-26), from its handshake until `quinn-proto` drains it after close.
//! Regulation pauses reads on purpose, so that credit is where backpressure
//! lands, and QUIC can't take it back. The budget therefore funds
//! `peer_memory_budget_bytes / connection_memory_bytes` QUIC connections.
//!
//! The transport caps those connections, admitted or not (zakura-quic
//! ADM-12). `max_pending_handshakes` of them are headroom for handshakes not
//! yet admitted, extra address attempts of one dial and closed connections
//! still draining. The node admits the rest. Half the headroom is a dial
//! reserve that inbound attempts can't take, so peers that churn inbound
//! connections can't stop this node's own dials.
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
/// `bootstrap_peer_count` configured bootstrap peers, so discovery can still
/// dial a discovered peer while every bootstrap peer is connected.
///
/// Discovery keeps its headroom, one slot per bootstrap peer and at least 4,
/// out of its dials, and also counts each connected peer against the rest. So
/// it needs twice the headroom plus one: 19 on Mainnet. The result is never
/// below [`MIN_ZAKURA_BUDGETED_CONNECTIONS`].
pub fn min_budgeted_connections(bootstrap_peer_count: usize) -> usize {
    effective_discovery_connection_headroom(bootstrap_peer_count)
        .saturating_mul(2)
        .saturating_add(1)
        .max(MIN_ZAKURA_BUDGETED_CONNECTIONS)
}

/// Connections the default budget admits at the default windows: 102 QUIC
/// connections less 32 of handshake headroom, 70.
pub const DEFAULT_ZAKURA_BUDGETED_CONNECTIONS: usize = max_connections_for_budget(
    DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    zakura_quic::connection_memory_bytes(DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW),
) - DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;

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
     {funded} QUIC connections of {connection_bytes} bytes each (the \
     [network.zakura.quic] receive_window_bytes and send_window_bytes plus the \
     connection's datagram queue), and {headroom} of them are max_pending_handshakes \
     headroom, which admits {admitted}; raise peer_memory_budget_bytes or lower those \
     windows or max_pending_handshakes so it admits at least {required}"
)]
pub struct PeerMemoryBudgetError {
    /// The configured budget.
    pub budget_bytes: u64,
    /// Transport bytes one connection can hold at the configured windows.
    pub connection_bytes: u64,
    /// QUIC connections the budget funds.
    pub funded: usize,
    /// Funded connections kept for handshakes and draining connections.
    pub headroom: usize,
    /// Connections the budget admits.
    pub admitted: usize,
    /// Connections the budget must admit.
    pub required: usize,
}

/// Returns the connection cap after the budget: `max_connections`, lowered to
/// what `budget_bytes` funds at `connection_bytes` each, less `headroom` for
/// QUIC connections the node holds but hasn't admitted.
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
    let funded = max_connections_for_budget(budget_bytes, connection_bytes);
    let admitted = funded.saturating_sub(headroom);
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
    use zakura_quic::QuicConfig;

    use super::*;
    use crate::zakura::{
        discovery::discovery_dial_slot_limit, ZakuraConfig, DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS,
        DEFAULT_ZAKURA_MAX_CONNECTIONS, DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
    };

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
        // Every QUIC connection the transport may hold fits the budget.
        // Widening usize to u64 is lossless on supported targets.
        let held = DEFAULT_ZAKURA_MAX_CONNECTIONS + DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;
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
        let config = ZakuraConfig {
            bootstrap_peers: vec![String::new(); 16],
            peer_memory_budget_bytes: (32 + 32) * 80 * MIB,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 33);
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
            peer_memory_budget_bytes: 50 * 80 * MIB,
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
            peer_memory_budget_bytes: 4 * 80 * MIB,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(2));
        let config = ZakuraConfig {
            peer_memory_budget_bytes: 4 * 80 * MIB - 1,
            ..config
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 2);
    }
}
