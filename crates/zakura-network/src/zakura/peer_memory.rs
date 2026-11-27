//! What native peers' transport credit costs the node.
//!
//! Each live native QUIC connection, from its handshake until it closes, can
//! make this node hold its QUIC windows in buffers (zakura-quic CTRL-26).
//! Regulation pauses reads on purpose, so that credit is where backpressure
//! lands, and QUIC can't take it back. Each bound socket can also queue
//! [`zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES`] of datagrams, however many
//! connections it serves.
//!
//! An admitted connection's receive window is the handshake window plus every
//! reactor's receive share (`ReactorShares::receive_window`), so it costs that
//! window plus its send window. A connection still in its handshake holds only
//! the handshake and send windows. The startup memory plan
//! (`memory::MemoryPlan`) charges these terms against `[memory] budget`.
//!
//! The transport caps those live connections, admitted or not (ADM-12).
//! `max_pending_handshakes` of them are headroom for handshakes not yet
//! admitted and extra address attempts of one dial, and the node admits the
//! rest. Inbound attempts may hold only their connection and handshake shares
//! of those slots, and a dial reserve sized for one full discovery record stays
//! out of their reach, so peers that churn inbound connections can't stop this
//! node's own dials (see `ZakuraLocalLimits::quic_bind_config`). A closed connection releases its stream buffers and moves to a
//! separate closing allowance of [`ZAKURA_CLOSING_CONNECTIONS_PER_LIVE`] per
//! live slot (ADM-13). Its queued datagrams count against its socket's queue,
//! so closing slots cost nothing more.
//!
//! Bookkeeping and the overheads that [`zakura_quic::connection_memory_bytes`]
//! lists come on top.

use zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES;

use super::discovery::effective_discovery_connection_headroom;

const GIB: u64 = 1024 * 1024 * 1024;

/// The P2P budget `B` that a standalone `zakura-network` config holds until
/// `zakurad` resolves `[memory] budget`: 11 GiB. Unit tests and embedders that
/// never resolve a budget size their plan from it.
pub const DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES: u64 = 11 * GIB;

/// Fewest connections the memory plan must fund, whatever the bootstrap list
/// (see [`min_budgeted_connections`]).
pub const MIN_ZAKURA_BUDGETED_CONNECTIONS: usize = 16;

/// Returns the fewest connections the memory plan must fund with
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

/// What one connection's QUIC windows can make the node hold.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ConnectionMemory {
    /// An admitted connection: the handshake window, every reactor's receive
    /// share, and the send window.
    pub admitted_bytes: u64,
    /// A connection in its handshake: the handshake and send windows.
    pub handshake_bytes: u64,
}

impl ConnectionMemory {
    /// The cost of a connection whose unauthenticated handshake holds
    /// `handshake_receive_bytes + handshake_send_bytes`, and whose admitted
    /// state holds `handshake_receive_bytes + shares_receive_bytes + admitted_send_bytes`.
    pub const fn with_handshake_send(
        handshake_receive_bytes: u64,
        handshake_send_bytes: u64,
        shares_receive_bytes: u64,
        admitted_send_bytes: u64,
    ) -> Self {
        let handshake_bytes = handshake_receive_bytes.saturating_add(handshake_send_bytes);
        let admitted_bytes = handshake_receive_bytes
            .saturating_add(shares_receive_bytes)
            .saturating_add(admitted_send_bytes);
        Self {
            admitted_bytes,
            handshake_bytes,
        }
    }
}

/// The QUIC memory that `connections` admitted connections and `headroom`
/// handshakes at `memory`, plus `sockets` socket queues, can make the node hold.
pub const fn budgeted_quic_bytes(
    sockets: u64,
    memory: ConnectionMemory,
    connections: usize,
    headroom: usize,
) -> u128 {
    // Widening u64 and usize to u128 is lossless on supported targets.
    sockets as u128 * SOCKET_DATAGRAM_QUEUE_BYTES as u128
        + headroom as u128 * memory.handshake_bytes as u128
        + connections as u128 * memory.admitted_bytes as u128
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::Network;
    use zakura_quic::QuicConfig;

    use super::*;
    use crate::zakura::{
        discovery::{discovery_dial_slot_limit, MAX_DIRECT_ADDRS_PER_RECORD},
        ZakuraConfig, DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS,
    };
    use crate::{zakura::ZakuraLocalLimits, Config};

    const MIB: u64 = 1024 * 1024;
    const KIB: u64 = 1024;
    /// One admitted Mainnet connection at the derived control share and default shares.
    const CONNECTION: u64 = 109_110_987;
    /// One pending handshake: 64 KiB control receive + 64 KiB handshake send.
    const HANDSHAKE: u64 = 128 * KIB;

    /// A connection costs its handshake window, every reactor's receive share
    /// and its send window. Regtest's larger header share costs more, and
    /// `tx-relay`'s two kinds add their own shares, not a stream window each.
    #[test]
    fn a_connection_costs_its_windows_and_every_reactor_share() {
        let mut config = ZakuraConfig::default();
        config.tx_relay.enabled = false;
        assert_eq!(
            config.connection_memory(&Network::Mainnet, &[]),
            ConnectionMemory {
                admitted_bytes: CONNECTION,
                handshake_bytes: HANDSHAKE,
            }
        );
        let regtest = Network::new_regtest(Default::default());
        assert_eq!(
            config.connection_memory(&regtest, &[]).admitted_bytes,
            111_204_597
        );
        assert_eq!(QuicConfig::default().connection_memory_bytes(), 64 * MIB);

        config.tx_relay.enabled = true;
        assert_eq!(
            config
                .connection_memory(&Network::Mainnet, &[])
                .admitted_bytes,
            CONNECTION + 4 * MIB + 64 * KIB
        );
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
    }

    /// The dial reserve fits one dial of a full discovery record whenever
    /// the slots inbound can't hold allow it, and is always at least one slot
    /// so inbound attempts on several sockets can't take every live slot.
    #[test]
    fn the_dial_reserve_fits_one_full_record() {
        for (connections, pending, reserve) in [
            (92, 32, 8),
            (92, 1, 8),
            (92, 0, 8),
            (8, 4, 3),
            (4, 1, 1),
            (1, 1, 1),
        ] {
            let mut config = Config::default();
            config.zakura.max_connections = Some(connections);
            config.zakura.max_pending_handshakes = pending;
            let bind = ZakuraLocalLimits::from_config(&config).quic_bind_config(Vec::new());
            assert_eq!(
                bind.dial_reserve, reserve,
                "{connections} connections, {pending} pending handshakes"
            );
        }
    }

    /// #2254: the transport counts inbound peers' live slots up to their
    /// connection and handshake shares, so with inbound full, discovery still
    /// has room for every outbound connection the connection slots leave,
    /// once the dial reserve has its full record. Before, inbound could hold
    /// 92 of the default 107 discovery slots and leave discovery 15 of its 23.
    #[test]
    fn discovery_keeps_the_outbound_share_with_inbound_full() {
        for max_connections in 1..=160 {
            for max_pending_handshakes in 1..=64 {
                let mut config = Config::default();
                config.zakura.max_connections = Some(max_connections);
                config.zakura.max_pending_handshakes = max_pending_handshakes;
                let limits = ZakuraLocalLimits::from_config(&config);
                let bind = limits.quic_bind_config(Vec::new());
                let label =
                    format!("{max_connections} connections, {max_pending_handshakes} handshakes");
                let inbound_full = bind.max_inbound_connections.expect("Zakura caps inbound");
                assert_eq!(
                    inbound_full,
                    limits.max_inbound_connections()
                        + limits.max_preferred_connections
                        + limits.max_inbound_handshakes(),
                    "{label}"
                );
                // With inbound at its cap, discovery stops only at the reserve.
                let discovery =
                    (bind.max_connections - bind.dial_reserve).saturating_sub(inbound_full);
                let outbound_share = limits.max_connections - limits.max_inbound_connections();
                let handshake_reserve =
                    limits.max_pending_handshakes - limits.max_inbound_handshakes();
                if handshake_reserve >= MAX_DIRECT_ADDRS_PER_RECORD {
                    assert!(discovery >= outbound_share, "{label}");
                }
                let outbound_slots = bind.max_connections - inbound_full;
                assert_eq!(
                    bind.dial_reserve,
                    outbound_slots.clamp(1, MAX_DIRECT_ADDRS_PER_RECORD),
                    "{label}"
                );
            }
        }
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let bind = limits.quic_bind_config(Vec::new());
        assert_eq!(
            (
                bind.max_connections,
                bind.dial_reserve,
                bind.max_inbound_connections
            ),
            (298, 8, Some(226))
        );
    }
}
