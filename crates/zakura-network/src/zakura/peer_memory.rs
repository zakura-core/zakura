//! The node-wide memory budget for native peers' transport buffers.
//!
//! Each native connection can make this node hold
//! [`zakura_quic::connection_memory_bytes`] in QUIC buffers (zakura-quic
//! CTRL-26). Regulation pauses reads on purpose, so that credit is where
//! backpressure lands, and QUIC can't take it back. The node therefore admits
//! at most `peer_memory_budget_bytes / connection_memory_bytes` connections.
//!
//! The budget lowers the connection count, never the windows, so every
//! connection keeps the throughput that regulation sizing assumes (see
//! `regulation::sizing`).

use thiserror::Error;

use super::{DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW};

/// Default bytes of QUIC buffers all native peers together may make this node
/// hold: 8 GiB.
pub const DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Fewest connections a peer memory budget must admit, so a node that lowers
/// its budget or raises its windows still dials discovered peers.
///
/// Discovery keeps one slot per bootstrap peer out of its dials, 9 on
/// Mainnet, so a smaller floor could leave it no dial slots at all. A smaller
/// `max_connections` lowers this floor to match.
pub const MIN_ZAKURA_BUDGETED_CONNECTIONS: usize = 16;

/// Connections the default budget admits at the default windows: 102.
pub const DEFAULT_ZAKURA_BUDGETED_CONNECTIONS: usize = max_connections_for_budget(
    DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    zakura_quic::connection_memory_bytes(DEFAULT_ZAKURA_RECEIVE_WINDOW, DEFAULT_ZAKURA_SEND_WINDOW),
);

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
/// [`MIN_ZAKURA_BUDGETED_CONNECTIONS`].
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error(
    "invalid [network.zakura] peer_memory_budget_bytes: {budget_bytes} bytes admits \
     {admitted} connections of {connection_bytes} bytes each (the \
     [network.zakura.quic] receive_window_bytes and send_window_bytes plus the \
     connection's datagram queue); raise peer_memory_budget_bytes or lower those \
     windows so it admits at least {required}"
)]
pub struct PeerMemoryBudgetError {
    /// The configured budget.
    pub budget_bytes: u64,
    /// Transport bytes one connection can hold at the configured windows.
    pub connection_bytes: u64,
    /// Connections the budget admits.
    pub admitted: usize,
    /// Connections the budget must admit.
    pub required: usize,
}

/// Returns the connection cap after the budget: `max_connections`, lowered to
/// what `budget_bytes` admits at `connection_bytes` each.
///
/// Fails when the budget admits fewer than [`MIN_ZAKURA_BUDGETED_CONNECTIONS`],
/// or fewer than `max_connections` when that is smaller.
pub fn budgeted_max_connections(
    max_connections: usize,
    budget_bytes: u64,
    connection_bytes: u64,
) -> Result<usize, PeerMemoryBudgetError> {
    let max_connections = max_connections.max(1);
    let admitted = max_connections_for_budget(budget_bytes, connection_bytes);
    let required = max_connections.min(MIN_ZAKURA_BUDGETED_CONNECTIONS);
    if admitted < required {
        return Err(PeerMemoryBudgetError {
            budget_bytes,
            connection_bytes,
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
        discovery::effective_discovery_connection_headroom, ZakuraConfig,
        DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS, DEFAULT_ZAKURA_MAX_CONNECTIONS,
    };

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn the_default_connection_cap_fits_the_default_budget() {
        let connection_bytes = QuicConfig::default().connection_memory_bytes();
        assert_eq!(connection_bytes, 80 * MIB);
        assert_eq!(DEFAULT_ZAKURA_BUDGETED_CONNECTIONS, 102);
        assert_eq!(
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
            DEFAULT_ZAKURA_BUDGETED_CONNECTIONS
        );
        // Widening usize to u64 is lossless on supported targets.
        let committed = DEFAULT_ZAKURA_MAX_CONNECTIONS as u64 * connection_bytes;
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
        assert_eq!(config.budgeted_max_connections(), Ok(102));

        // Raising the windows lowers the cap instead of the throughput.
        let config = ZakuraConfig {
            quic: QuicConfig {
                receive_window_bytes: 256 << 20,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        // 8 GiB / (256 + 32 + 16) MiB.
        assert_eq!(config.budgeted_max_connections(), Ok(26));

        // A smaller cap stays as configured.
        let config = ZakuraConfig {
            max_connections: 4,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(4));
    }

    #[test]
    fn the_floor_leaves_discovery_dial_slots_on_mainnet() {
        let headroom =
            effective_discovery_connection_headroom(DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS.len());
        assert!(MIN_ZAKURA_BUDGETED_CONNECTIONS > headroom);
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
        assert_eq!(error.admitted, 7);
        assert_eq!(error.required, MIN_ZAKURA_BUDGETED_CONNECTIONS);
        assert!(error
            .to_string()
            .contains("[network.zakura] peer_memory_budget_bytes"));
        assert!(error.to_string().contains("receive_window_bytes"));

        // 8 GiB / (512 + 32 + 16) MiB leaves room for 14, below the floor.
        let config = ZakuraConfig {
            quic: QuicConfig {
                receive_window_bytes: 512 << 20,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().admitted, 14);

        let config = ZakuraConfig {
            peer_memory_budget_bytes: 0,
            ..ZakuraConfig::default()
        };
        assert!(config.budgeted_max_connections().is_err());

        // A cap below the floor only needs the budget to cover that cap.
        let config = ZakuraConfig {
            max_connections: 2,
            peer_memory_budget_bytes: 160 * MIB,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(), Ok(2));
        let config = ZakuraConfig {
            peer_memory_budget_bytes: 160 * MIB - 1,
            ..config
        };
        assert_eq!(config.budgeted_max_connections().unwrap_err().required, 2);
    }
}
