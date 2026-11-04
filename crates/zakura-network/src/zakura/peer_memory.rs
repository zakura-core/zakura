//! The node-wide budget for native peers' transport credit.
//!
//! Each live native QUIC connection, from its handshake until it closes, can
//! make this node hold its QUIC windows in buffers (zakura-quic CTRL-26).
//! Regulation pauses reads on purpose, so that credit is where backpressure
//! lands, and QUIC can't take it back. Each bound socket can also queue
//! [`zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES`] of datagrams, however many
//! connections it serves.
//!
//! An admitted connection's receive window is the handshake window plus every
//! reactor's receive share (`ReactorShares::receive_window`), so the budget
//! charges it that window plus its send window. A connection still in its
//! handshake holds only the handshake and send windows. The budget reserves one
//! socket queue per bound socket and `max_pending_handshakes` handshake
//! connections, and funds what is left divided by the admitted connection's
//! cost.
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
//! The budget lowers the connection count, never the windows or the shares, so
//! every connection keeps the throughput that its reactors' shares derive from.
//! Bookkeeping and the overheads that [`zakura_quic::connection_memory_bytes`]
//! lists come on top.

use thiserror::Error;
use zakura_quic::SOCKET_DATAGRAM_QUEUE_BYTES;

use super::{
    discovery::effective_discovery_connection_headroom,
    header_sync::v9::{CommonMessage, RegtestMessage},
    ReactorBudget, BLOCK_SYNC_REACTOR_BUDGET, DEFAULT_ZAKURA_MAX_CONNECTIONS,
    DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES, DEFAULT_ZAKURA_RECEIVE_WINDOW,
    DEFAULT_ZAKURA_SEND_WINDOW, DISCOVERY_BUDGET, LEGACY_GOSSIP_BUDGET, LEGACY_REQUESTS_BUDGET,
};

const GIB: u64 = 1024 * 1024 * 1024;

/// Default budget for native QUIC connections' flow-control credit and the
/// bound sockets' datagram queues: 15 GiB. CTRL-26 in the zakura-quic spec
/// lists what it leaves out.
///
/// It is what admission charges for [`DEFAULT_ZAKURA_MAX_CONNECTIONS`] at the
/// default windows and reactor shares on the one socket the default
/// `listen_addr` binds, rounded up to a whole GiB. Regtest's larger header
/// share needs the most: 256 MiB + 32 × 64 MiB + 92 × 144,030,963 B =
/// 15,666,767,700 B.
pub const DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES: u64 = {
    let mainnet = default_budget_need(default_connection_memory(CommonMessage::BUDGET));
    let regtest = default_budget_need(default_connection_memory(RegtestMessage::BUDGET));
    let need = if mainnet > regtest { mainnet } else { regtest };
    // Widening u64 to u128 is lossless. The need is about 15 GiB, so its GiB
    // count fits in u64.
    need.div_ceil(GIB as u128) as u64 * GIB
};

/// The QUIC memory admission charges for the default connection count and
/// handshake headroom at `memory`, on one socket.
const fn default_budget_need(memory: ConnectionMemory) -> u128 {
    budgeted_quic_bytes(
        1,
        memory,
        DEFAULT_ZAKURA_MAX_CONNECTIONS,
        DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
    )
}

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

/// What one connection costs the peer memory budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ConnectionMemory {
    /// An admitted connection: the handshake window, every reactor's receive
    /// share, and the send window.
    pub admitted_bytes: u64,
    /// A connection in its handshake: the handshake and send windows.
    pub handshake_bytes: u64,
}

impl ConnectionMemory {
    /// The cost of a connection with these windows, where every admitted
    /// connection negotiates reactors whose receive shares sum to
    /// `shares_receive_bytes`.
    pub const fn new(
        handshake_receive_bytes: u64,
        shares_receive_bytes: u64,
        send_window_bytes: u64,
    ) -> Self {
        let handshake_bytes = handshake_receive_bytes.saturating_add(send_window_bytes);
        Self {
            admitted_bytes: handshake_bytes.saturating_add(shares_receive_bytes),
            handshake_bytes,
        }
    }
}

/// The cost of one connection at the default windows and reactor shares,
/// with `header_sync` as the header sync share: 141,937,353 B on Mainnet and
/// Testnet, and 144,030,963 B on Regtest.
pub const fn default_connection_memory(header_sync: ReactorBudget) -> ConnectionMemory {
    // Widening u32 to u64 is lossless.
    ConnectionMemory::new(
        DEFAULT_ZAKURA_RECEIVE_WINDOW as u64,
        LEGACY_GOSSIP_BUDGET.receive
            + LEGACY_REQUESTS_BUDGET.receive
            + DISCOVERY_BUDGET.receive
            + header_sync.receive
            + BLOCK_SYNC_REACTOR_BUDGET.receive,
        DEFAULT_ZAKURA_SEND_WINDOW,
    )
}

/// Connections the default budget admits at the default windows and shares,
/// with the one socket the default `listen_addr` binds: 96 on Mainnet.
pub const DEFAULT_ZAKURA_BUDGETED_CONNECTIONS: usize = budget_admitted_connections(
    DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    1,
    default_connection_memory(CommonMessage::BUDGET),
    DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
);

// The default budget funds the default connection count on every network.
const _: () = assert!(
    DEFAULT_ZAKURA_BUDGETED_CONNECTIONS >= DEFAULT_ZAKURA_MAX_CONNECTIONS
        && budget_admitted_connections(
            DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
            1,
            default_connection_memory(RegtestMessage::BUDGET),
            DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
        ) >= DEFAULT_ZAKURA_MAX_CONNECTIONS,
    "the default peer memory budget funds the default connection count"
);

/// Returns how many admitted connections `budget_bytes` funds at `memory`,
/// after reserving one socket datagram queue for each of `sockets` and
/// `headroom` connections in their handshakes, rounding down.
///
/// # Panics
///
/// If `memory.admitted_bytes` is zero.
pub const fn budget_admitted_connections(
    budget_bytes: u64,
    sockets: u64,
    memory: ConnectionMemory,
    headroom: usize,
) -> usize {
    // Widening usize to u64 is lossless on supported targets.
    let reserved = sockets
        .saturating_mul(SOCKET_DATAGRAM_QUEUE_BYTES)
        .saturating_add((headroom as u64).saturating_mul(memory.handshake_bytes));
    let connections = budget_bytes.saturating_sub(reserved) / memory.admitted_bytes;
    // Clamp before narrowing on targets where usize is smaller than u64.
    // usize is at most 64 bits on every supported target, so widening
    // usize::MAX to u64 is lossless.
    if connections > usize::MAX as u64 {
        usize::MAX
    } else {
        connections as usize
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

/// `[network.zakura] peer_memory_budget_bytes` admits fewer connections than
/// [`min_budgeted_connections`] requires.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error(
    "invalid [network.zakura] peer_memory_budget_bytes: {budget_bytes} bytes, after \
     reserving {socket_queue_bytes} bytes for the bound sockets' datagram queues and \
     {headroom} max_pending_handshakes connections of {handshake_bytes} bytes each (the \
     [network.zakura.quic] receive_window_bytes plus send_window_bytes), admits \
     {admitted} connections of {connection_bytes} bytes each (those windows plus every \
     reactor's receive share); raise peer_memory_budget_bytes or lower those windows or \
     max_pending_handshakes so it admits at least {required}"
)]
pub struct PeerMemoryBudgetError {
    /// The configured budget.
    pub budget_bytes: u64,
    /// Datagram queue bytes reserved for the bound sockets.
    pub socket_queue_bytes: u64,
    /// Transport bytes one admitted connection can hold.
    pub connection_bytes: u64,
    /// Transport bytes one connection in its handshake can hold.
    pub handshake_bytes: u64,
    /// Connections the budget reserves for handshakes and dial attempts.
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
    memory: ConnectionMemory,
    bootstrap_peer_count: usize,
    headroom: usize,
) -> Result<usize, PeerMemoryBudgetError> {
    let max_connections = max_connections.max(1);
    let admitted = budget_admitted_connections(budget_bytes, sockets, memory, headroom);
    let required = max_connections.min(min_budgeted_connections(bootstrap_peer_count));
    if admitted < required {
        return Err(PeerMemoryBudgetError {
            budget_bytes,
            socket_queue_bytes: sockets.saturating_mul(SOCKET_DATAGRAM_QUEUE_BYTES),
            connection_bytes: memory.admitted_bytes,
            handshake_bytes: memory.handshake_bytes,
            headroom,
            admitted,
            required,
        });
    }
    Ok(max_connections.min(admitted))
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::Network;
    use zakura_quic::{NodeId, NodeSecretKey, QuicConfig};

    use super::*;
    use crate::zakura::{
        discovery::discovery_dial_slot_limit, ZakuraConfig, DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS,
        DEFAULT_ZAKURA_MAX_CONNECTIONS, HEADER_SYNC_BUDGET,
    };
    use crate::{zakura::ZakuraLocalLimits, Config};

    const MIB: u64 = 1024 * 1024;
    /// One admitted Mainnet connection at the default windows and shares.
    const CONNECTION: u64 = 141_937_353;
    /// One handshake at the default windows.
    const HANDSHAKE: u64 = 64 * MIB;

    fn mainnet() -> Network {
        Network::Mainnet
    }

    /// A budget that admits exactly `connections` at the Mainnet defaults on
    /// one socket, after the default 32 handshakes of headroom.
    fn budget_for(connections: u64) -> u64 {
        256 * MIB + 32 * HANDSHAKE + connections * CONNECTION
    }

    #[test]
    fn the_default_budget_funds_the_default_connection_count() {
        let mainnet_memory = default_connection_memory(HEADER_SYNC_BUDGET);
        assert_eq!(
            mainnet_memory,
            ConnectionMemory {
                admitted_bytes: CONNECTION,
                handshake_bytes: HANDSHAKE,
            }
        );
        assert_eq!(
            default_connection_memory(RegtestMessage::BUDGET).admitted_bytes,
            144_030_963
        );
        let config = ZakuraConfig::default();
        assert_eq!(config.connection_memory(&mainnet(), &[]), mainnet_memory);
        assert_eq!(QuicConfig::default().connection_memory_bytes(), HANDSHAKE);

        assert_eq!(DEFAULT_ZAKURA_MAX_CONNECTIONS, 92);
        // Regtest's need, 15,666,767,700 B, rounded up to a whole GiB.
        assert_eq!(DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES, 16_106_127_360);
        assert_eq!(DEFAULT_ZAKURA_BUDGETED_CONNECTIONS, 96);
        // Every admitted connection at its full window, every handshake, and the
        // one socket the default `listen_addr` binds fit on every network.
        for network in [
            mainnet(),
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ] {
            let quic = config.quic_memory_bytes(&network, DEFAULT_ZAKURA_MAX_CONNECTIONS, &[]);
            assert!(
                quic <= u128::from(DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES),
                "{network:?}"
            );
            assert_eq!(
                config.budgeted_max_connections(&network),
                Ok(DEFAULT_ZAKURA_MAX_CONNECTIONS),
                "{network:?}"
            );
        }
        assert_eq!(
            config.quic_memory_bytes(&mainnet(), DEFAULT_ZAKURA_MAX_CONNECTIONS, &[]),
            15_474_155_580
        );
        assert_eq!(
            config.quic_memory_bytes(
                &Network::new_regtest(Default::default()),
                DEFAULT_ZAKURA_MAX_CONNECTIONS,
                &[],
            ),
            15_666_767_700
        );
    }

    #[test]
    fn the_budget_lowers_a_larger_connection_cap() {
        // A config generated before the budget existed still says 256.
        let config = ZakuraConfig {
            max_connections: 256,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(96));

        // Without `listen_addr` the endpoint binds IPv4 and IPv6 loopback, so it
        // reserves two socket queues: (15 GiB - 512 MiB - 32 × 64 MiB) / 141,937,353.
        let config = ZakuraConfig {
            max_connections: 256,
            listen_addr: None,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(94));

        // Raising the windows lowers the cap instead of the throughput:
        // (15 GiB - 256 MiB - 32 × 96 MiB) / (141,937,353 + 32 MiB).
        let config = ZakuraConfig {
            max_connections: 256,
            quic: QuicConfig {
                receive_window_bytes: 64 << 20,
                ..QuicConfig::default()
            },
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(71));

        // So does raising the handshake headroom:
        // (15 GiB - 256 MiB - 64 × 64 MiB) / 141,937,353.
        let config = ZakuraConfig {
            max_connections: 256,
            max_pending_handshakes: 64,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(81));

        // So does a larger reactor share: a 64 MiB cwnd cap adds 32 MiB.
        let mut config = ZakuraConfig {
            max_connections: 256,
            ..ZakuraConfig::default()
        };
        config.block_sync.bbr_max_cwnd_bytes = 64 << 20;
        assert_eq!(
            config.connection_memory(&mainnet(), &[]).admitted_bytes,
            CONNECTION + (32 << 20)
        );
        assert!(config.budgeted_max_connections(&mainnet()).unwrap() < 96);

        // A smaller cap stays as configured.
        let config = ZakuraConfig {
            max_connections: 4,
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(4));
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
                .budgeted_max_connections_for_node(&mainnet(), outsider, &[])
                .unwrap_err()
                .required,
            33
        );
        // Leaving this node's own entry out lowers the floor to match discovery.
        assert_eq!(
            config
                .budgeted_max_connections_for_node(&mainnet(), node_ids[0], &[])
                .unwrap_err()
                .required,
            31
        );
        // Without the node key, config load assumes one entry may be its own.
        assert_eq!(
            config
                .budgeted_max_connections(&mainnet())
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
            peer_memory_budget_bytes: budget_for(30),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(30));
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
        assert_eq!(config.zakura.budgeted_max_connections(&mainnet()), Ok(18));
        assert_eq!(
            config
                .zakura
                .budgeted_max_connections_for_node(&mainnet(), node_ids[0], &[]),
            Ok(18)
        );
        assert_eq!(ZakuraLocalLimits::from_config(&config).max_connections, 18);

        // A budget both checks refuse still caps the limits at its own count.
        config.zakura.peer_memory_budget_bytes = budget_for(16);
        assert!(config.zakura.budgeted_max_connections(&mainnet()).is_err());
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
        let error = config.budgeted_max_connections(&mainnet()).unwrap_err();
        // 32 handshakes of (1024 + 32) MiB alone exceed the 15 GiB budget.
        assert_eq!((error.headroom, error.admitted), (32, 0));
        assert_eq!(error.handshake_bytes, (1024 + 32) * MIB);
        // Mainnet's 9 bootstrap peers, counting one as possibly this node.
        assert_eq!(error.required, 17);
        assert!(error
            .to_string()
            .contains("[network.zakura] peer_memory_budget_bytes"));
        assert!(error.to_string().contains("receive_window_bytes"));
        assert!(error.to_string().contains("max_pending_handshakes"));
        assert_eq!(error.socket_queue_bytes, 256 * MIB);

        // The budget admits 16 after 32 of headroom, below the floor.
        let config = ZakuraConfig {
            peer_memory_budget_bytes: budget_for(16),
            ..ZakuraConfig::default()
        };
        let error = config.budgeted_max_connections(&mainnet()).unwrap_err();
        assert_eq!((error.headroom, error.admitted), (32, 16));
        assert_eq!(error.connection_bytes, CONNECTION);

        let config = ZakuraConfig {
            peer_memory_budget_bytes: 0,
            ..ZakuraConfig::default()
        };
        assert!(config.budgeted_max_connections(&mainnet()).is_err());

        // A cap below the floor only needs the budget to cover that cap and
        // its headroom.
        let config = ZakuraConfig {
            max_connections: 2,
            peer_memory_budget_bytes: budget_for(2),
            ..ZakuraConfig::default()
        };
        assert_eq!(config.budgeted_max_connections(&mainnet()), Ok(2));
        let config = ZakuraConfig {
            peer_memory_budget_bytes: budget_for(2) - 1,
            ..config
        };
        assert_eq!(
            config
                .budgeted_max_connections(&mainnet())
                .unwrap_err()
                .required,
            2
        );
    }
}
