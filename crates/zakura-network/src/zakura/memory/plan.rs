//! Pure, total startup memory sizing plan (`02 §4`, `02 §8`, `mem-1`, `03 §5`).
//!
//! [`MemoryPlan::size`] accounts every byte that Zakura v2 P2P can hold:
//! fixed non-leased terms, per-connection transport base `t`, per-reactor
//! session bases `b_r`, proof-of-work-bounded reserves (`Control`,
//! `Propagation`, `Consensus`), bulk consumer floors, and the remaining shared
//! space `S` divided by `[memory.weight]`.

use std::fmt;

use thiserror::Error;
use zakura_chain::{
    block::MAX_BLOCK_BYTES,
    parameters::{Network, NetworkKind},
};
use zakura_node_services::block_memory::{MAX_DECODED_BLOCK_BYTES, READ_RESULT_BLOCKS_PER_PEER};
use zakura_quic::{NodeId, SOCKET_DATAGRAM_QUEUE_BYTES};

use super::{
    config::{ByteSize, MemoryWeights},
    draft::{draft_worst_checkpoint_window_bytes, BudgetDerivation},
    Consumer, ConsumerSpec, MemoryPool, PoolConfig, Priority,
};
use crate::{
    config::{Config, P2pStack},
    zakura::{
        block_sync::{
            ZakuraBlockSyncConfig, BLOCK_SYNC_REACTOR_BUDGET, BLOCK_SYNC_SERVE_NODE_EXECUTION,
            BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES, BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES,
            DEFAULT_BS_BBR_MIN_CWND_BYTES, LARGEST_SERVED_BLOCK_RESPONSE_BYTES,
        },
        bpv1::{
            PropagationReserve, SessionBase as Bpv1SessionBase, ZakuraBpv1Config, NODE_PROFILE,
            ZAKURA_CAP_BPV1,
        },
        discovery::{discovery_serving_output_bytes, DISCOVERY_BUDGET},
        handler::{
            remote_bootstrap_peer_count, BASE_SEND_WINDOW_BYTES,
            DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES, DEFAULT_ZAKURA_RECEIVE_WINDOW,
            DEFAULT_ZAKURA_SEND_WINDOW,
        },
        header_sync::v9::{
            self as header_sync_v9, HEADER_SYNC_SERVE_NODE_EXECUTION, SERVE_PEER_OUTPUT_BYTES,
        },
        legacy_gossip::{
            LEGACY_GOSSIP_BUDGET, LEGACY_INBOUND_RETAINED_BYTES, LEGACY_REQUESTS_BUDGET,
        },
        peer_memory::min_budgeted_connections,
        tx_relay::wire::{
            MAX_TX_BYTES, SESSION_HELD_OUTPUT_BYTES, TX_RELAY_DATA_BUDGET, TX_RELAY_STATE_BUDGET,
            ZAKURA_CAP_TX_RELAY,
        },
        ServicePeerDirection, ServicePeerLimits, ZakuraLocalLimits, DEFAULT_SERVICE_MAX_PEERS,
        MAX_BS_FRAME_BYTES, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_HEADER_SYNC,
        ZAKURA_CAP_LEGACY_GOSSIP,
    },
};

const MIB: u64 = 1024 * 1024;

/// Default mempool cost limit (`80_000_000 B`, matching `zakurad`'s `[mempool] tx_cost_limit`).
pub const DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES: u64 = 80_000_000;

/// Default state read parser allowance: `zakura-state`'s 64 data and 4
/// control read workers plus its 4 chain-tip blocks, each parsing one
/// maximum-size block (`2 × MAX_BLOCK_BYTES` of raw rows and its decoded
/// result). `zakurad` passes the state's own value
/// (`zakura_state::Config::read_parser_bytes`).
pub const DEFAULT_STATE_READ_PARSER_BYTES: u64 =
    (64 + 4 + 4) * (2 * MAX_BLOCK_BYTES + MAX_DECODED_BLOCK_BYTES);

/// The default `[state] read_result_byte_limit` on the v2 stack, derived from
/// read concurrency rather than peer count (`02 §4`, decision-10):
///
/// - v2 serving: every `NODE_EXECUTION` slot holding one largest response;
/// - legacy serving in `Dual` mode: one largest `getdata` response. Only the
///   legacy connection count bounds legacy reads, so further legacy reads
///   wait on the limit, and a refused read answers `notfound`;
/// - RPC: one largest decoded block.
pub fn derived_read_result_byte_limit(config: &Config, network: &Network) -> u64 {
    let v2_serving = (BLOCK_SYNC_SERVE_NODE_EXECUTION as u64)
        .saturating_mul(MAX_BS_FRAME_BYTES as u64)
        .saturating_add(
            (HEADER_SYNC_SERVE_NODE_EXECUTION as u64)
                .saturating_mul(header_sync_v9::largest_header_page_bytes(network)),
        );
    let legacy_serving = if config.p2p_stack.resolve(network) == P2pStack::Dual {
        READ_RESULT_BLOCKS_PER_PEER.saturating_mul(MAX_DECODED_BLOCK_BYTES)
    } else {
        0
    };
    v2_serving
        .saturating_add(legacy_serving)
        .saturating_add(MAX_DECODED_BLOCK_BYTES)
}

/// Fixed non-leased memory for discovery's peer book and dial-backoff tables:
/// a full book's encoded records at the largest record size
/// (`DEFAULT_MAX_DISCOVERY_BOOK_RECORDS` × `MAX_NODE_RECORD_BYTES`,
/// 96,010,000 B), plus `01 §1`'s 25 MiB estimate for the entries' dial
/// metadata and the 32k dial-backoff entries. That estimate dates from
/// records of at most 753 B, so it also counts those bytes again.
// Widening usize to u64 is lossless on supported targets.
pub const DISCOVERY_TABLES_FIXED_BYTES: u64 = (crate::zakura::DEFAULT_MAX_DISCOVERY_BOOK_RECORDS
    as u64)
    * (crate::zakura::MAX_NODE_RECORD_BYTES as u64)
    + 25 * MIB;

/// Fixed non-leased memory for `tx-relay`'s `WantedIds` and `PendingSet` maps
/// (`01 §1`, `#2623`: ~150 MiB when `tx-relay` is enabled).
pub const TX_RELAY_MAPS_FIXED_BYTES: u64 = 150 * MIB;

/// Maximum concurrent tip blocks reserved for propagation paths with BPv1 off.
const MAX_CONCURRENT_TIP_BLOCKS: u64 = 4;

/// Proof-of-work-bounded propagation reserve covering tip-block paths with
/// BPv1 off (`goal-impl.md`: `4 × 4 × MAX_BLOCK_BYTES = 32 MB`). With BPv1 on,
/// [`PropagationReserve`] sizes the reserve from BPv1's own bounds.
pub const PROPAGATION_RESERVE_BYTES: u64 = MAX_CONCURRENT_TIP_BLOCKS * 4 * MAX_BLOCK_BYTES;

/// Non-leased fixed memory terms accounted in the startup plan (`02 §4`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedTerms {
    /// Bound QUIC socket datagram queues (`bound_sockets × 256 MiB`).
    pub socket_queues: u64,
    /// Pre-handshake incoming packet buffers (`bound_sockets × incoming_buffer_total_bytes`).
    pub incoming_buffers: u64,
    /// Unauthenticated pending handshakes (`max_pending_handshakes × (control_recv + handshake_send)`).
    pub handshakes: u64,
    /// Header-chain retained DAG and reference envelope (`RETAINED_HEADER_MEMORY_ENVELOPE_V1_BYTES`).
    pub header_chain: u64,
    /// Discovery peer book and dial-backoff tables.
    pub discovery_tables: u64,
    /// `tx-relay` maps (`WantedIds` and `PendingSet`s; 0 when `tx-relay` is disabled).
    pub tx_relay_maps: u64,
    /// State reads (`02 §4`): the retained read limit that `zakurad` passes to
    /// `zakura-state`, plus the state's parser allowance.
    pub state_reads: u64,
    /// Mempool storage pool (`tx_cost_limit`).
    pub mempool_pool: u64,
    /// Legacy TCP ingress buffers (`Dual` mode only; 0 in `Zakura`-only mode).
    pub legacy_tcp_ingress: u64,
    /// Decoded legacy compatibility work that the node's one compatibility sink
    /// retains after it drops each frame (`LEGACY_INBOUND_RETAINED_BYTES`).
    /// Every v2 connection negotiates the compatibility streams, in `Zakura`
    /// and `Dual` mode alike.
    pub legacy_inbound: u64,
}

impl FixedTerms {
    /// Every fixed term by its metric and trace label (`03 §5`).
    pub const fn entries(&self) -> [(&'static str, u64); 10] {
        [
            ("socket_queues", self.socket_queues),
            ("incoming_buffers", self.incoming_buffers),
            ("handshakes", self.handshakes),
            ("header_chain", self.header_chain),
            ("discovery_tables", self.discovery_tables),
            ("tx_relay_maps", self.tx_relay_maps),
            ("state_reads", self.state_reads),
            ("mempool_pool", self.mempool_pool),
            ("legacy_tcp_ingress", self.legacy_tcp_ingress),
            ("legacy_inbound", self.legacy_inbound),
        ]
    }

    /// Sum of every fixed non-leased memory term.
    pub fn total(&self) -> u64 {
        self.entries()
            .into_iter()
            .fold(0, |total, (_, bytes)| total.saturating_add(bytes))
    }
}

/// One reactor's session base `b_r` (`02 §3`, `02 §8`), split by what holds it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReactorBase {
    /// Unsolicited receive bytes and frame slack. A connection's receive
    /// credit grants them for every kind it negotiated, whether or not the
    /// reactor's service admits a session.
    pub receive: u64,
    /// The serving floor and per-session bookkeeping. The node holds them only
    /// while the reactor's service runs a session on the connection.
    pub session: u64,
}

impl ReactorBase {
    /// The whole session base `b_r`.
    pub const fn total(&self) -> u64 {
        self.receive.saturating_add(self.session)
    }
}

/// Per-connection transport base `t` and per-reactor session bases `b_r` (`02 §8`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionBases {
    /// Transport base `t` held by every admitted connection regardless of
    /// reactors: control receive share + base send window + discovery unsolicited receive.
    pub transport_base: u64,
    /// Header-sync session base `b_header_sync`: unsolicited receive and frame
    /// slack, plus one largest served page.
    pub header_sync: ReactorBase,
    /// Block-sync session base `b_block_sync`: unsolicited receive and frame
    /// slack, plus the per-session serving bookkeeping slot.
    pub block_sync: ReactorBase,
    /// `tx-relay` session base `b_tx_relay` (0 when disabled): data and state
    /// unsolicited receive, plus `SESSION_HELD_OUTPUT_BYTES`.
    pub tx_relay: ReactorBase,
    /// Legacy compatibility streams base `b_legacy_compat` (kinds 2 and 3 unsolicited receive).
    pub legacy_compat: ReactorBase,
    /// BPv1 session base `b_bpv1` (0 when disabled): control and data
    /// unsolicited receive, plus the output gates, held frames and early
    /// records of one session ([`Bpv1SessionBase`]).
    pub bpv1: ReactorBase,
    /// Sum of unsolicited receive shares for any embedder custom stream kinds.
    pub custom_kinds: ReactorBase,
}

/// The label of the embedder custom kinds' base in [`SessionBases::entries`].
/// It has no capability bit, and no metric (`03 §5`).
const CUSTOM_KINDS: &str = "custom_kinds";

/// The reactors whose session bases [`SessionBases`] names, by capability bit.
const SESSION_REACTOR_CAPABILITIES: u64 = {
    let zero = ReactorBase {
        receive: 0,
        session: 0,
    };
    let entries = SessionBases {
        transport_base: 0,
        header_sync: zero,
        block_sync: zero,
        tx_relay: zero,
        legacy_compat: zero,
        bpv1: zero,
        custom_kinds: zero,
    }
    .entries();
    let mut capabilities = 0;
    let mut index = 0;
    while index < entries.len() {
        capabilities |= entries[index].1;
        index += 1;
    }
    capabilities
};

impl SessionBases {
    /// Every base besides `t`, with its label and its reactor's capability
    /// bit (0 for [`CUSTOM_KINDS`]). Sizing, admission, metrics, traces and
    /// refusals read the bases from this one table, so a new reactor joins
    /// them here (BP-pt-3).
    pub const fn entries(&self) -> [(&'static str, u64, ReactorBase); 6] {
        [
            ("header_sync", ZAKURA_CAP_HEADER_SYNC, self.header_sync),
            (
                "block_sync",
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                self.block_sync,
            ),
            ("tx_relay", ZAKURA_CAP_TX_RELAY, self.tx_relay),
            (
                "legacy_compat",
                ZAKURA_CAP_LEGACY_GOSSIP,
                self.legacy_compat,
            ),
            ("bpv1", ZAKURA_CAP_BPV1, self.bpv1),
            (CUSTOM_KINDS, 0, self.custom_kinds),
        ]
    }

    /// Cost of one connection carrying the reactors required to follow the chain:
    /// discovery (in `t`), header sync, and block sync.
    pub const fn required_chain_connection_bytes(&self) -> u64 {
        self.transport_base
            .saturating_add(self.header_sync.total())
            .saturating_add(self.block_sync.total())
    }

    /// Cost of one connection carrying every supported reactor on this node
    /// (`t + Σ_r b_r`, `02 §8` rule 3).
    pub fn full_connection_bytes(&self) -> u64 {
        self.entries()
            .into_iter()
            .fold(self.transport_base, |total, (_, _, base)| {
                total.saturating_add(base.total())
            })
    }

    /// The bases that `Consumer::Sessions` funds when it admits a connection
    /// (`02 §8` rule 3).
    ///
    /// - `negotiated`: the capabilities both sides accepted. The connection's
    ///   receive credit covers each negotiated reactor's unsolicited bytes, so
    ///   each one adds its `receive` part.
    /// - `admitted`: the capabilities whose services take a session from this
    ///   peer now. Each one adds its `session` part. A service that admits a
    ///   session later funds it then, through [`Self::session_top_up`].
    /// - `custom_kinds_negotiated`: whether the connection negotiated any
    ///   embedder custom kind.
    pub fn connection_cost(
        &self,
        negotiated: u64,
        admitted: u64,
        custom_kinds_negotiated: bool,
    ) -> ConnectionSessionCost {
        let admitted = admitted & negotiated;
        let [header_sync, block_sync, tx_relay, legacy_compat, bpv1, _] = self
            .entries()
            .map(|(_, capability, base)| reactor_cost(base, negotiated, admitted, capability));
        ConnectionSessionCost {
            transport_base: self.transport_base,
            header_sync,
            block_sync,
            tx_relay,
            legacy_compat,
            bpv1,
            custom_kinds: if custom_kinds_negotiated {
                self.custom_kinds.total()
            } else {
                0
            },
        }
    }

    /// The reactor capabilities whose `session` parts
    /// [`Self::connection_cost`] funds for the same `negotiated` and
    /// `admitted` capabilities.
    pub const fn funded_sessions(negotiated: u64, admitted: u64) -> u64 {
        admitted & negotiated & SESSION_REACTOR_CAPABILITIES
    }

    /// The `session` part a connection still owes when a service admits a
    /// session for `capability`, given the reactor sessions it has funded so
    /// far. Returns the reactor's capability bit and the bytes, or `None` when
    /// nothing is owed: the reactor is funded, has no session part, or isn't
    /// one of the reactors these bases name.
    pub fn session_top_up(&self, funded_sessions: u64, capability: u64) -> Option<(u64, u64)> {
        let (_, bit, base) = self
            .entries()
            .into_iter()
            .find(|(_, bit, _)| capability & bit != 0)?;
        (funded_sessions & bit == 0 && base.session != 0).then_some((bit, base.session))
    }
}

/// One reactor's admission charge: its receive part when negotiated, plus its
/// session part when its service admits the peer.
const fn reactor_cost(base: ReactorBase, negotiated: u64, admitted: u64, capability: u64) -> u64 {
    let receive = if negotiated & capability != 0 {
        base.receive
    } else {
        0
    };
    let session = if admitted & capability != 0 {
        base.session
    } else {
        0
    };
    receive.saturating_add(session)
}

/// Exact transport base `t` and reactor session bases `b_r` required to admit
/// one connection (`02 §8`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSessionCost {
    /// Transport base `t`.
    pub transport_base: u64,
    /// Header-sync base (0 if not negotiated).
    pub header_sync: u64,
    /// Block-sync base (0 if not negotiated).
    pub block_sync: u64,
    /// `tx-relay` base (0 if not negotiated or disabled).
    pub tx_relay: u64,
    /// Legacy compatibility base (0 if not negotiated).
    pub legacy_compat: u64,
    /// BPv1 base (0 if not negotiated or disabled).
    pub bpv1: u64,
    /// Embedder custom kinds base (0 if none negotiated).
    pub custom_kinds: u64,
}

impl ConnectionSessionCost {
    /// Total bytes (`t + Σ_r b_r`) that `Consumer::Sessions` must fund together
    /// before admitting the connection (`02 §8` rule 3).
    pub const fn total(&self) -> u64 {
        self.transport_base
            .saturating_add(self.header_sync)
            .saturating_add(self.block_sync)
            .saturating_add(self.tx_relay)
            .saturating_add(self.legacy_compat)
            .saturating_add(self.bpv1)
            .saturating_add(self.custom_kinds)
    }
}

/// Bytes of `Consumer::Sessions`' protected share that inbound connections may
/// lease: all but a quarter, which stays for this node's dials (`02 §8` rule
/// 1). The transport keeps the same quarter of its connection slots
/// ([`crate::zakura::ZakuraLocalLimits::max_inbound_connections`]).
pub const fn inbound_sessions_share(sessions_share: u64) -> u64 {
    sessions_share - sessions_share / 4
}

/// The part of `Consumer::Sessions`' protected share a connection leases
/// from (`02 §8` rule 1): its direction, or the places held for preferred
/// gateways.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSide {
    /// A connection the remote peer dialed.
    Inbound,
    /// A connection this node dialed.
    Outbound,
    /// A connection to a preferred mining gateway, in either direction.
    Gateway,
}

impl SessionSide {
    /// The side of a connection in `direction`, to a preferred gateway when
    /// `gateway` is set.
    pub const fn new(direction: ServicePeerDirection, gateway: bool) -> Self {
        match (gateway, direction) {
            (true, _) => Self::Gateway,
            (false, ServicePeerDirection::Inbound) => Self::Inbound,
            (false, ServicePeerDirection::Outbound) => Self::Outbound,
        }
    }
}

/// Bytes `Consumer::Sessions` has leased, by the [`SessionSide`] of the
/// connection that holds them (`02 §8` rule 1).
///
/// It mirrors the connection-slot checks. Ordinary connections leave
/// [`Self::gateway_reserve`] bytes free for the places held for preferred
/// gateways, and inbound ones also leave a quarter of the rest for dials. A
/// gateway may lease anything left in the share.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionDirections {
    /// Bytes leased for ordinary connections the remote peer dialed.
    pub inbound: u64,
    /// Bytes leased for ordinary connections this node dialed.
    pub outbound: u64,
    /// Bytes leased for connections to preferred gateways.
    pub gateways: u64,
    /// Bytes that ordinary connections leave for the held gateway places
    /// ([`MemoryPlan::gateway_reserve`]).
    pub gateway_reserve: u64,
}

impl SessionDirections {
    /// Whether a connection on `side` may lease `cost` more bytes of
    /// `sessions_share`, the sessions consumer's protected share.
    ///
    /// Every side stays within the share (`02 §5` rule 4). Ordinary
    /// connections also stop short of the gateway reserve, and inbound ones
    /// at [`inbound_sessions_share`] of what that leaves, so inbound peers
    /// can't take the dial reserve.
    pub const fn admits(&self, side: SessionSide, cost: u64, sessions_share: u64) -> bool {
        let ordinary = self.inbound.saturating_add(self.outbound);
        if ordinary.saturating_add(self.gateways).saturating_add(cost) > sessions_share {
            return false;
        }
        let ordinary_share = sessions_share.saturating_sub(self.gateway_reserve);
        match side {
            SessionSide::Gateway => true,
            SessionSide::Outbound => ordinary.saturating_add(cost) <= ordinary_share,
            SessionSide::Inbound => {
                ordinary.saturating_add(cost) <= ordinary_share
                    && self.inbound.saturating_add(cost) <= inbound_sessions_share(ordinary_share)
            }
        }
    }

    /// Record `cost` bytes leased on `side` if [`Self::admits`] allows them.
    pub fn try_charge(&mut self, side: SessionSide, cost: u64, sessions_share: u64) -> bool {
        if !self.admits(side, cost, sessions_share) {
            return false;
        }
        let held = self.side_mut(side);
        *held = held.saturating_add(cost);
        true
    }

    /// Return `cost` bytes that a connection on `side` released.
    pub fn release(&mut self, side: SessionSide, cost: u64) {
        let held = self.side_mut(side);
        debug_assert!(*held >= cost, "a connection releases only bytes it charged");
        *held = held.saturating_sub(cost);
    }

    fn side_mut(&mut self, side: SessionSide) -> &mut u64 {
        match side {
            SessionSide::Inbound => &mut self.inbound,
            SessionSide::Outbound => &mut self.outbound,
            SessionSide::Gateway => &mut self.gateways,
        }
    }
}

/// Breakdown returned in [`MemoryPlanError::BudgetTooSmall`] (`02 §8`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetTooSmall {
    /// Host-to-P2P derivation breakdown (`budget`, `base`, `sidecar`, `ρ`, `B`).
    pub derivation: BudgetDerivation,
    /// Resolved P2P logical budget `B`.
    pub budget_b: u64,
    /// Minimum required bytes (`fixed + reserves + Σ bulk_floors`).
    pub required_bytes: u64,
    /// Breakdown of fixed non-leased terms.
    pub fixed: FixedTerms,
    /// Per-connection/session base costs.
    pub bases: SessionBases,
    /// Each consumer's reserve or floor, by [`Consumer::index`].
    pub floors: [u64; Consumer::COUNT],
    /// Minimum budgeted connections required for discovery + chain following.
    pub min_connections: usize,
    /// Operator settings raised above their defaults that raise a reserve.
    /// A reserve is held in full, so it never starts with a warning: the
    /// refusal names these settings instead (BP-pt-2).
    pub raised_reserve_settings: Vec<RaisedSetting>,
}

/// One operator setting above its default that a [`BudgetTooSmall`] names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaisedSetting {
    /// TOML setting path.
    pub setting: &'static str,
    /// Operator-configured value.
    pub configured_value: String,
    /// Default value.
    pub default_value: String,
}

impl fmt::Display for BudgetTooSmall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let derivation = &self.derivation;
        let floors_of = |consumers: &[Consumer]| {
            consumers
                .iter()
                .fold(0u64, |sum, c| sum.saturating_add(self.floors[c.index()]))
        };
        write!(
            f,
            "insufficient [memory] budget: host budget={} B, base={} B, sidecar={} B, rho={}/{}, \
             resolved P2P budget B={} B, but required minimum is {} B (fixed={} B [",
            derivation.host_budget_bytes,
            derivation.base_bytes,
            derivation.sidecar_bytes,
            derivation.rho_numerator,
            derivation.rho_denominator,
            self.budget_b,
            self.required_bytes,
            self.fixed.total(),
        )?;
        write_terms(f, self.fixed.entries())?;
        write!(f, "], reserves={} B [", floors_of(&Consumer::RESERVED))?;
        write_terms(
            f,
            Consumer::RESERVED.map(|c| (c.label(), self.floors[c.index()])),
        )?;
        write!(f, "], floors={} B [", floors_of(&Consumer::BULK))?;
        for (index, consumer) in Consumer::BULK.into_iter().enumerate() {
            let separator = if index == 0 { "" } else { ", " };
            write!(
                f,
                "{separator}{}={} B",
                consumer.label(),
                self.floors[consumer.index()]
            )?;
            if consumer == Consumer::Sessions {
                write!(
                    f,
                    " ({} min conns × {} B)",
                    self.min_connections,
                    self.bases.full_connection_bytes()
                )?;
            }
        }
        write!(f, "], connection bases [t={} B", self.bases.transport_base)?;
        for (name, _, base) in self.bases.entries() {
            write!(f, ", {name}={} B", base.total())?;
        }
        f.write_str("]); ")?;
        for raised in &self.raised_reserve_settings {
            write!(
                f,
                "{} = {} raises a reserve (default {}); ",
                raised.setting, raised.configured_value, raised.default_value
            )?;
        }
        f.write_str("raise [memory] budget or add host RAM")?;
        if !self.raised_reserve_settings.is_empty() {
            f.write_str(", or lower the raised settings")?;
        }
        Ok(())
    }
}

/// Writes `terms` as `name=bytes B`, separated by commas.
fn write_terms(
    f: &mut fmt::Formatter<'_>,
    terms: impl IntoIterator<Item = (&'static str, u64)>,
) -> fmt::Result {
    for (index, (name, bytes)) in terms.into_iter().enumerate() {
        let separator = if index == 0 { "" } else { ", " };
        write!(f, "{separator}{name}={bytes} B")?;
    }
    Ok(())
}

/// Why [`MemoryPlan::size`] refused a configuration (`02 §8`).
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MemoryPlanError {
    /// A reactor's unsolicited receive base cannot hold one maximal frame (`mem-1`).
    #[error("stream kind {kind} fails frame-cap check: {reason}")]
    InvalidFrameCap {
        /// Stream kind whose unsolicited base is too small.
        kind: u16,
        /// Validation failure description.
        reason: String,
    },
    /// Invalid `[memory.weight]` configuration.
    #[error("{0}")]
    InvalidWeights(String),
    /// `fixed + reserves + Σ floors > B` under default settings (`02 §8`).
    #[error("{0}")]
    BudgetTooSmall(Box<BudgetTooSmall>),
}

/// One operator setting whose worst-case memory pushes the startup plan past
/// the resolved P2P budget `B` (`02 §8`, `mem-14`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OverrideWarning {
    /// TOML setting path that pushes the worst case past `B`.
    pub setting: String,
    /// Operator-configured value for this setting.
    pub configured_value: String,
    /// Bytes by which the worst case with this setting exceeds `B`.
    pub excess_bytes: u64,
    /// Value the plan would have chosen under default settings.
    pub plan_value: String,
}

impl fmt::Display for OverrideWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} = {} exceeds [memory] budget by {} B (plan would choose {})",
            self.setting, self.configured_value, self.excess_bytes, self.plan_value
        )
    }
}

/// Complete, pure startup memory sizing plan (`02 §8`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryPlan {
    /// Derivation of `B` from host/configured budget (`budget`, `base`, `sidecar`, `ρ`, `B`).
    pub derivation: BudgetDerivation,
    /// Source label (`"auto"` or `"config"`).
    pub source: &'static str,
    /// Network label (`"mainnet"`, `"testnet"`, `"regtest"`).
    pub network_name: &'static str,
    /// Non-leased fixed terms.
    pub fixed: FixedTerms,
    /// Per-connection transport base `t` and per-reactor session bases `b_r`.
    pub bases: SessionBases,
    /// Embedder custom stream kinds that [`SessionBases::custom_kinds`] covers.
    pub custom_kinds: Vec<u16>,
    /// The most `Consumer::Sessions`' leases may hold: its protected share
    /// (`02 §5` rule 4), or its cap when that is lower.
    pub sessions_share: u64,
    /// Resulting pool configuration passed directly to [`MemoryPool::new`].
    pub pool_config: PoolConfig,
    /// Minimum chain-following connections funded by the sessions floor.
    pub min_connections: usize,
    /// Effective connection policy ceiling `N_cap`.
    pub connection_cap: usize,
    /// Full-reactor connections funded by the sessions consumer's protected share
    /// (`min(N_cap, sessions_share / (t + Σ b_r))`, `02 §8`).
    pub planned_full_connections: usize,
    /// Required-chain-reactor connections funded by the sessions consumer's
    /// protected share (`min(N_cap, sessions_share / (t + b_hsync + b_bsync))`).
    pub planned_required_connections: usize,
    /// Effective look-ahead cap (default or overridden by `[memory.cap] look_ahead`).
    pub look_ahead_cap: u64,
    /// Effective per-peer serving cap (default or overridden by `[memory.cap] serving_per_peer`).
    pub serving_per_peer_cap: u64,
    /// Effective tx pending-bytes cap (mempool cost limit or overridden by `[memory.cap] tx`).
    pub tx_cap: u64,
    /// Warnings for operator overrides whose worst case can exceed `B` (`02 §8`, `mem-14`).
    /// Always empty under default settings.
    pub override_warnings: Vec<OverrideWarning>,
}

impl MemoryPlan {
    /// Pure, total sizing function (`02 §8`): computes the complete memory plan
    /// for logical P2P budget `budget_b`, `config`, and `network`.
    pub fn size(
        budget_b: u64,
        config: &Config,
        network: &Network,
    ) -> Result<Self, MemoryPlanError> {
        Self::size_for_node(budget_b, config, network, None, &[])
    }

    /// Like [`Self::size`], excluding `local_node_id` from the bootstrap peer
    /// count and accounting for any embedder `custom_kinds`.
    pub fn size_for_node(
        budget_b: u64,
        config: &Config,
        network: &Network,
        local_node_id: Option<NodeId>,
        custom_kinds: &[u16],
    ) -> Result<Self, MemoryPlanError> {
        let memory = &config.zakura.memory_budget;
        memory
            .weights
            .validate()
            .map_err(MemoryPlanError::InvalidWeights)?;

        let shares = config.zakura.reactor_shares(network);
        validate_reactor_unsolicited_frame_caps(config, network, &shares)?;

        let terms = PlanTerms::compute(config, network, local_node_id, custom_kinds);
        let override_warnings = override_warnings(config, network, &terms, budget_b);

        let mut derivation = memory.derivation;
        derivation.p2p_budget_b = budget_b;

        // Startup refuses when fixed + reserves + floors exceed `B` even with
        // every raised setting back at its default (`02 §8`). Otherwise it
        // starts, and `override_warnings` names each raised setting whose
        // excess pushes the plan past `B`. Lowering a setting never raises a
        // term, so the unraised plan matters only when this one doesn't fit.
        if terms.required_bytes() > budget_b {
            let unraised = PlanTerms::compute(
                &without_raised_settings(config, network),
                network,
                local_node_id,
                custom_kinds,
            );
            let smallest = if unraised.required_bytes() < terms.required_bytes() {
                &unraised
            } else {
                &terms
            };
            if smallest.required_bytes() > budget_b {
                return Err(MemoryPlanError::BudgetTooSmall(Box::new(
                    smallest.budget_too_small(config, derivation, budget_b),
                )));
            }
        }

        let pool_config = terms.pool_config(budget_b, memory.weights);
        let sessions_protected =
            super::MemoryPoolMachine::new(pool_config).protected_share(Consumer::Sessions);
        let connections = |cost: u64| {
            usize::try_from(sessions_protected / cost.max(1))
                .unwrap_or(usize::MAX)
                .min(terms.connection_cap)
        };

        let network_name = match network.kind() {
            NetworkKind::Mainnet => "mainnet",
            NetworkKind::Testnet => "testnet",
            NetworkKind::Regtest => "regtest",
        };

        Ok(Self {
            derivation,
            source: memory.source,
            network_name,
            fixed: terms.fixed,
            bases: terms.bases,
            custom_kinds: custom_kinds.to_vec(),
            sessions_share: sessions_protected.min(terms.cap(Consumer::Sessions)),
            pool_config,
            min_connections: terms.min_connections,
            connection_cap: terms.connection_cap,
            planned_full_connections: connections(terms.bases.full_connection_bytes()),
            planned_required_connections: connections(
                terms.bases.required_chain_connection_bytes(),
            ),
            look_ahead_cap: terms.look_ahead_cap,
            serving_per_peer_cap: terms.serving_per_peer_cap,
            tx_cap: terms.cap(Consumer::Tx),
            override_warnings,
        })
    }

    /// Shared pool capacity `S = B - fixed - reserves - Σ bulk_floors`.
    #[inline]
    pub const fn shared_capacity(&self) -> u64 {
        self.pool_config.shared_capacity()
    }

    /// Minimum bytes required by fixed terms, reserves, and bulk floors.
    #[inline]
    pub fn required_minimum_bytes(&self) -> u64 {
        self.fixed
            .total()
            .saturating_add(self.pool_config.total_floors())
    }

    /// Consumer specification stored in this plan.
    #[inline]
    pub const fn consumer_spec(&self, consumer: Consumer) -> ConsumerSpec {
        self.pool_config.consumers[consumer.index()]
    }

    /// Consumer's weighted share of `S` (bytes above its floor).
    pub fn consumer_share_of_s(&self, consumer: Consumer) -> u64 {
        super::MemoryPoolMachine::new(self.pool_config).share_of_s(consumer)
    }

    /// Consumer's protected total (`floor + share_of_S`).
    pub fn consumer_protected_share(&self, consumer: Consumer) -> u64 {
        super::MemoryPoolMachine::new(self.pool_config).protected_share(consumer)
    }

    /// The endpoint's local limits under this plan: the configured limits with
    /// the connection cap `N_cap` (`02 §8`). `Consumer::Sessions` decides how
    /// many connections memory funds below that cap.
    pub fn local_limits(&self, config: &Config) -> ZakuraLocalLimits {
        let mut limits = ZakuraLocalLimits::from_config(config);
        limits.max_connections = self.connection_cap.max(1);
        limits
    }

    /// Bytes of the sessions share that ordinary connections leave for
    /// `hold` places held for preferred gateways: one full connection each,
    /// but never so much that the share funds fewer than `min_connections`
    /// ordinary ones.
    pub fn gateway_reserve(&self, hold: usize) -> u64 {
        let floor = self.consumer_spec(Consumer::Sessions).floor;
        (hold as u64)
            .saturating_mul(self.bases.full_connection_bytes())
            .min(self.sessions_share.saturating_sub(floor))
    }

    /// Preferred gateway places the sessions share can fund at once: whole
    /// connections in [`Self::gateway_reserve`], at most
    /// [`MAX_PREFERRED_GATEWAYS`](crate::zakura::MAX_PREFERRED_GATEWAYS).
    pub fn funded_gateway_places(&self) -> usize {
        let max = crate::zakura::MAX_PREFERRED_GATEWAYS;
        let funded = self
            .gateway_reserve(max)
            .checked_div(self.bases.full_connection_bytes())
            .unwrap_or(u64::MAX);
        // The minimum is at most `max`, a usize, so the narrowing is lossless.
        funded.min(max as u64) as usize
    }

    /// Construct a live [`MemoryPool`] sized from this plan.
    pub fn build_pool(&self) -> MemoryPool {
        MemoryPool::new(self.pool_config)
    }

    /// Publish `zakura.memory.plan_bytes{term}` startup gauges (`03 §5`).
    pub fn publish_metrics(&self) {
        let mut terms = vec![
            ("budget_b".to_owned(), self.pool_config.budget_b),
            ("fixed_total".to_owned(), self.fixed.total()),
        ];
        terms.extend(
            self.fixed
                .entries()
                .map(|(name, bytes)| (format!("fixed_{name}"), bytes)),
        );
        terms.extend(Consumer::ALL.map(|consumer| {
            let prefix = match consumer.priority() {
                Priority::Reserved => "reserve",
                Priority::Bulk => "floor",
            };
            (
                format!("{prefix}_{}", consumer.label()),
                self.consumer_spec(consumer).floor,
            )
        }));
        terms.push(("shared_s".to_owned(), self.shared_capacity()));
        terms.push(("transport_base".to_owned(), self.bases.transport_base));
        terms.extend(
            self.bases
                .entries()
                .into_iter()
                .filter(|(name, ..)| *name != CUSTOM_KINDS)
                .map(|(name, _, base)| (format!("session_{name}"), base.total())),
        );
        terms.push((
            "full_connection_base".to_owned(),
            self.bases.full_connection_bytes(),
        ));
        for (term, bytes) in terms {
            metrics::gauge!("zakura.memory.plan_bytes", "term" => term).set(bytes as f64);
        }
    }
}

impl fmt::Display for MemoryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MemoryPlan(network={}, B={} B, fixed={} B, floors+reserves={} B, \
             propagation={} B, S={} B, t={} B, bpv1_session={} B, full_conn={} B, \
             planned_full_conns={}/{})",
            self.network_name,
            self.pool_config.budget_b,
            self.fixed.total(),
            self.pool_config.total_floors(),
            self.consumer_spec(Consumer::Propagation).floor,
            self.shared_capacity(),
            self.bases.transport_base,
            self.bases.bpv1.total(),
            self.bases.full_connection_bytes(),
            self.planned_full_connections,
            self.connection_cap,
        )
    }
}

/// Every term [`MemoryPlan::size_for_node`] derives from one config (`02 §4`,
/// `02 §8`).
#[derive(Clone, Copy, Debug)]
struct PlanTerms {
    connection_cap: usize,
    min_connections: usize,
    bound_sockets: u64,
    handshake_cost: u64,
    fixed: FixedTerms,
    bases: SessionBases,
    /// Each consumer's floor and cap, by [`Consumer::index`]. Weights come
    /// from `[memory.weight]` when [`Self::pool_config`] builds the pool.
    consumers: [ConsumerSpec; Consumer::COUNT],
    default_look_ahead: u64,
    look_ahead_cap: u64,
    default_serving_per_peer: u64,
    serving_per_peer_cap: u64,
    default_tx_cap: u64,
}

impl PlanTerms {
    fn compute(
        config: &Config,
        network: &Network,
        local_node_id: Option<NodeId>,
        custom_kinds: &[u16],
    ) -> Self {
        let memory = &config.zakura.memory_budget;
        let shares = config.zakura.reactor_shares(network);

        let connection_cap = config.zakura.effective_max_connections();
        let bootstrap_peers =
            remote_bootstrap_peer_count(&config.zakura.bootstrap_peers, local_node_id);
        let min_connections = min_budgeted_connections(bootstrap_peers).min(connection_cap);
        // Preferred gateways connect above `N_cap`, so the caps that scale
        // with connections count their held places too.
        let admitted_cap = connection_cap.saturating_add(super::super::MAX_PREFERRED_GATEWAYS);

        let bound_sockets = config.zakura.bound_socket_count();
        let handshakes_count =
            u64::try_from(config.zakura.max_pending_handshakes.max(1)).unwrap_or(u64::MAX);
        let handshake_cost = shares
            .control_receive_bytes
            .saturating_add(config.zakura.handshake_send_window_bytes());

        let largest_header_page = header_sync_v9::largest_header_page_bytes(network);
        let state_reads = memory
            .read_result_byte_limit
            .unwrap_or_else(|| derived_read_result_byte_limit(config, network))
            .saturating_add(memory.state_read_parser_bytes);

        let legacy_tcp_ingress = if config.p2p_stack.resolve(network) == P2pStack::Dual {
            config.tcp_ingress_bound_bytes()
        } else {
            0
        };

        let fixed = FixedTerms {
            socket_queues: bound_sockets.saturating_mul(SOCKET_DATAGRAM_QUEUE_BYTES),
            incoming_buffers: bound_sockets
                .saturating_mul(config.zakura.quic.incoming_buffer_total_bytes),
            handshakes: handshakes_count.saturating_mul(handshake_cost),
            header_chain: zakura_header_chain::RETAINED_HEADER_MEMORY_ENVELOPE_V1_BYTES,
            discovery_tables: DISCOVERY_TABLES_FIXED_BYTES,
            tx_relay_maps: if config.zakura.tx_relay.enabled {
                TX_RELAY_MAPS_FIXED_BYTES
            } else {
                0
            },
            state_reads,
            mempool_pool: memory.mempool_tx_cost_limit,
            legacy_tcp_ingress,
            legacy_inbound: LEGACY_INBOUND_RETAINED_BYTES,
        };

        // Per-connection transport base `t` and per-reactor session bases `b_r` (`02 §8`).
        let header_sync_budget = header_sync_v9::budget(network);
        let block_sync_budget = super::super::block_sync_reactor_budget(
            config.zakura.block_sync.bbr_max_cwnd_bytes,
            config.zakura.block_sync.floor_bypass_slots,
        )
        .unwrap_or(BLOCK_SYNC_REACTOR_BUDGET);

        let custom_kinds_receive = custom_kinds
            .iter()
            .map(|&kind| shares.share(kind).receive.unsolicited)
            .fold(0u64, u64::saturating_add);

        let bases = SessionBases {
            transport_base: shares
                .control_receive_bytes
                .saturating_add(BASE_SEND_WINDOW_BYTES)
                .saturating_add(DISCOVERY_BUDGET.receive.unsolicited),
            header_sync: ReactorBase {
                receive: header_sync_budget.receive.unsolicited,
                session: largest_header_page,
            },
            // Serving leases each response it queues, so the base holds only
            // the per-session bookkeeping slot, not a served block.
            block_sync: ReactorBase {
                receive: block_sync_budget.receive.unsolicited,
                session: BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES,
            },
            tx_relay: if config.zakura.tx_relay.enabled {
                ReactorBase {
                    receive: TX_RELAY_DATA_BUDGET
                        .receive
                        .unsolicited
                        .saturating_add(TX_RELAY_STATE_BUDGET.receive.unsolicited),
                    session: SESSION_HELD_OUTPUT_BYTES,
                }
            } else {
                ReactorBase::default()
            },
            legacy_compat: ReactorBase {
                receive: LEGACY_GOSSIP_BUDGET
                    .receive
                    .unsolicited
                    .saturating_add(LEGACY_REQUESTS_BUDGET.receive.unsolicited),
                session: 0,
            },
            bpv1: if config.zakura.bpv1.enabled {
                let base = Bpv1SessionBase::new(&config.zakura.bpv1);
                ReactorBase {
                    receive: base.receive,
                    session: base.session,
                }
            } else {
                ReactorBase::default()
            },
            custom_kinds: ReactorBase {
                receive: custom_kinds_receive,
                session: 0,
            },
        };

        // Bulk consumers (`Priority::Bulk`, `02 §4` & `02 §8`).
        let floor_block_sync = DEFAULT_BS_BBR_MIN_CWND_BYTES;
        let default_look_ahead = config.zakura.block_sync.max_reorder_lookahead_bytes;
        let look_ahead_cap = memory
            .caps
            .look_ahead
            .map_or(default_look_ahead, ByteSize::bytes)
            .max(floor_block_sync);
        let block_sync_inflight_cap =
            (admitted_cap as u64).saturating_mul(config.zakura.block_sync.bbr_max_cwnd_bytes);
        let cap_block_sync = block_sync_inflight_cap.saturating_add(look_ahead_cap);

        let floor_serving = LARGEST_SERVED_BLOCK_RESPONSE_BYTES;
        let default_serving_per_peer =
            DEFAULT_ZAKURA_SEND_WINDOW.saturating_add(SERVE_PEER_OUTPUT_BYTES);
        let serving_per_peer_cap = memory
            .caps
            .serving_per_peer
            .map_or(default_serving_per_peer, ByteSize::bytes)
            .max(floor_serving);
        let cap_serving = (admitted_cap as u64).saturating_mul(serving_per_peer_cap);

        // The floor funds `min_connections` connections that each run every
        // reactor, because admission funds every reactor a peer supports
        // together (`02 §8` rules 3 and 4). Reactor caps and `N_cap` are
        // ceilings, not reservations, so they never raise the floor.
        let floor_sessions = (min_connections as u64).saturating_mul(bases.full_connection_bytes());
        // Receive parts follow negotiation, so every connection may hold
        // them. Session parts follow each reactor's sessions.
        let cap_sessions = bases.entries().into_iter().fold(
            (admitted_cap as u64).saturating_mul(bases.transport_base),
            |cap, (_, capability, base)| {
                let sessions = reactor_session_cap(config, capability, admitted_cap) as u64;
                cap.saturating_add((admitted_cap as u64).saturating_mul(base.receive))
                    .saturating_add(sessions.saturating_mul(base.session))
            },
        );

        let floor_tx = u64::from(MAX_TX_BYTES);
        let default_tx_cap = memory.mempool_tx_cost_limit.max(floor_tx);
        let tx_cap = memory.caps.tx.map_or(default_tx_cap, ByteSize::bytes);

        // Reserved consumers (`Priority::Reserved`, `floor == cap`, `02 §4`).
        let reserve_propagation = if config.zakura.bpv1.enabled {
            PropagationReserve::new(&config.zakura.bpv1, NODE_PROFILE).total()
        } else {
            PROPAGATION_RESERVE_BYTES
        };
        let reserve_consensus = draft_worst_checkpoint_window_bytes(network)
            .max(BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES);
        let mut consumers = [ConsumerSpec::default(); Consumer::COUNT];
        for (consumer, floor, cap) in [
            (
                Consumer::Control,
                discovery_serving_output_bytes(admitted_cap),
                0,
            ),
            (Consumer::Propagation, reserve_propagation, 0),
            (Consumer::Consensus, reserve_consensus, 0),
            (Consumer::BlockSync, floor_block_sync, cap_block_sync),
            (Consumer::Serving, floor_serving, cap_serving),
            (Consumer::Sessions, floor_sessions, cap_sessions),
            (Consumer::Tx, floor_tx, tx_cap),
        ] {
            consumers[consumer.index()] = match consumer.priority() {
                Priority::Reserved => ConsumerSpec::reserved(floor),
                Priority::Bulk => ConsumerSpec::bulk(floor, cap, 0),
            };
        }

        Self {
            connection_cap,
            min_connections,
            bound_sockets,
            handshake_cost,
            fixed,
            bases,
            consumers,
            default_look_ahead,
            look_ahead_cap,
            default_serving_per_peer,
            serving_per_peer_cap,
            default_tx_cap,
        }
    }

    const fn floor(&self, consumer: Consumer) -> u64 {
        self.consumers[consumer.index()].floor
    }

    const fn cap(&self, consumer: Consumer) -> u64 {
        self.consumers[consumer.index()].cap
    }

    /// `fixed + reserves + Σ floors`, which must fit in `B` (`02 §8`).
    fn required_bytes(&self) -> u64 {
        self.consumers
            .iter()
            .fold(self.fixed.total(), |total, spec| {
                total.saturating_add(spec.floor)
            })
    }

    /// The pool these terms size under `budget_b`, sharing `S` by `weights`.
    fn pool_config(&self, budget_b: u64, weights: MemoryWeights) -> PoolConfig {
        Consumer::ALL
            .into_iter()
            .fold(
                PoolConfig::new(budget_b, self.fixed.total()),
                |pool, consumer| {
                    let ConsumerSpec { floor, cap, .. } = self.consumers[consumer.index()];
                    pool.with_consumer(
                        consumer,
                        ConsumerSpec::bulk(floor, cap, weights.of(consumer)),
                    )
                },
            )
            .with_serving_per_peer_cap(self.serving_per_peer_cap)
    }

    fn budget_too_small(
        &self,
        config: &Config,
        derivation: BudgetDerivation,
        budget_b: u64,
    ) -> BudgetTooSmall {
        BudgetTooSmall {
            derivation,
            budget_b,
            required_bytes: self.required_bytes(),
            fixed: self.fixed,
            bases: self.bases,
            floors: self.consumers.map(|spec| spec.floor),
            min_connections: self.min_connections,
            raised_reserve_settings: raised_reserve_settings(config),
        }
    }
}

/// The settings in `config` above their defaults that raise a reserve. With
/// BPv1 on, its part buffer and its concurrent blocks size the `Propagation`
/// reserve ([`PropagationReserve`]).
fn raised_reserve_settings(config: &Config) -> Vec<RaisedSetting> {
    let bpv1 = &config.zakura.bpv1;
    if !bpv1.enabled {
        return Vec::new();
    }
    let default = ZakuraBpv1Config::default();
    let mut raised = Vec::new();
    if bpv1.part_retention_budget_bytes > default.part_retention_budget_bytes {
        raised.push(RaisedSetting {
            setting: "[network.zakura.bpv1] part_retention_budget_bytes",
            configured_value: ByteSize(bpv1.part_retention_budget_bytes).to_string(),
            default_value: ByteSize(default.part_retention_budget_bytes).to_string(),
        });
    }
    if bpv1.max_concurrent_blocks > default.max_concurrent_blocks {
        raised.push(RaisedSetting {
            setting: "[network.zakura.bpv1] max_concurrent_blocks",
            configured_value: bpv1.max_concurrent_blocks.to_string(),
            default_value: default.max_concurrent_blocks.to_string(),
        });
    }
    raised
}

/// The most sessions the reactor with `capability` runs at once under a
/// connection cap of `connection_cap`.
fn reactor_session_cap(config: &Config, capability: u64, connection_cap: usize) -> usize {
    let zakura = &config.zakura;
    match capability {
        ZAKURA_CAP_HEADER_SYNC => zakura.header_sync.serving_sessions(connection_cap),
        ZAKURA_CAP_BLOCK_SYNC_REGULATED => zakura.block_sync.block_sync_connections(connection_cap),
        ZAKURA_CAP_BPV1 => zakura
            .bpv1
            .max_sessions
            .map_or(connection_cap, |max| max.min(connection_cap)),
        _ => connection_cap,
    }
}

/// One warning for each raised operator setting whose worst case pushes the
/// plan `terms` past `budget_b` (`02 §8`, `mem-14`). Only a setting raised past
/// its default warns: a lowered one can't add to the default worst case
/// (fix-35).
fn override_warnings(
    config: &Config,
    network: &Network,
    terms: &PlanTerms,
    budget_b: u64,
) -> Vec<OverrideWarning> {
    let zakura = &config.zakura;
    let memory = &zakura.memory_budget;
    let default_quic = zakura_quic::QuicConfig::default();
    let default_bs = ZakuraBlockSyncConfig::default();
    let bytes = |bytes: u64| ByteSize(bytes).to_string();
    let required_bytes = terms.required_bytes();
    // The plan's worst case without `consumer`'s floor.
    let without = |consumer: Consumer| required_bytes.saturating_sub(terms.floor(consumer));
    let mut warnings = Vec::new();

    // What the plan would fund under default settings, so warnings can cite
    // the plan's baseline values.
    let baseline = terms.pool_config(budget_b, MemoryWeights::default());
    let plan_full_connections = usize::try_from(
        super::MemoryPoolMachine::new(baseline).protected_share(Consumer::Sessions)
            / terms.bases.full_connection_bytes().max(1),
    )
    .unwrap_or(usize::MAX)
    .min(DEFAULT_SERVICE_MAX_PEERS);
    let plan_funds = format!("unset (plan funds {plan_full_connections})");

    // Session caps: what a raised cap lets the sessions consumer hold.
    let sessions_worst = without(Consumer::Sessions).saturating_add(terms.cap(Consumer::Sessions));
    if let Some(configured) = zakura
        .max_connections
        .filter(|&conns| conns > DEFAULT_SERVICE_MAX_PEERS)
    {
        if sessions_worst > budget_b {
            warnings.push(warning(
                "[network.zakura] max_connections",
                configured.to_string(),
                sessions_worst - budget_b,
                format!("{plan_full_connections} connections"),
            ));
        }
    }
    let peer_limits_sessions = |limits: &ServicePeerLimits| {
        limits.has_explicit_peer_caps().then(|| {
            limits
                .max_inbound()
                .saturating_add(limits.max_outbound())
                .min(terms.connection_cap)
        })
    };
    for (setting, sessions) in [
        (
            "[network.zakura.header_sync.peer_limits]",
            peer_limits_sessions(&zakura.header_sync.peer_limits),
        ),
        (
            "[network.zakura.block_sync.peer_limits]",
            peer_limits_sessions(&zakura.block_sync.peer_limits),
        ),
        (
            "[network.zakura.bpv1] max_sessions",
            zakura
                .bpv1
                .max_sessions
                .map(|max| max.min(terms.connection_cap)),
        ),
    ] {
        if let Some(sessions) = sessions.filter(|&sessions| sessions > plan_full_connections) {
            if sessions_worst > budget_b {
                warnings.push(warning(
                    setting,
                    format!("{sessions} sessions"),
                    sessions_worst - budget_b,
                    plan_funds.clone(),
                ));
            }
        }
    }

    // Consumer caps: each raised cap against the plan without that floor.
    let without_bs = without(Consumer::BlockSync);
    if let Some(cap) = memory.caps.look_ahead {
        let worst_case = without_bs.saturating_add(terms.look_ahead_cap);
        if terms.look_ahead_cap > terms.default_look_ahead && worst_case > budget_b {
            let plan_choice = terms
                .default_look_ahead
                .min(budget_b.saturating_sub(without_bs));
            warnings.push(warning(
                "[memory.cap] look_ahead",
                cap.to_string(),
                worst_case - budget_b,
                bytes(plan_choice),
            ));
        }
    }
    if let Some(cap) = memory.caps.serving_per_peer {
        let without_serving = without(Consumer::Serving);
        let single_worst = without_serving.saturating_add(terms.serving_per_peer_cap);
        let multi_worst = without_serving.saturating_add(
            terms
                .serving_per_peer_cap
                .saturating_mul(plan_full_connections.max(1) as u64),
        );
        if cap.bytes() > terms.default_serving_per_peer
            && (single_worst > budget_b || multi_worst > budget_b)
        {
            let excess_bytes = if single_worst > budget_b {
                single_worst - budget_b
            } else {
                multi_worst - budget_b
            };
            warnings.push(warning(
                "[memory.cap] serving_per_peer",
                cap.to_string(),
                excess_bytes,
                bytes(terms.default_serving_per_peer),
            ));
        }
    }
    if let Some(cap) = memory.caps.tx {
        let without_tx = without(Consumer::Tx);
        let tx_cap = terms.cap(Consumer::Tx);
        let worst_case = without_tx.saturating_add(tx_cap);
        if tx_cap > terms.default_tx_cap && worst_case > budget_b {
            let plan_choice = terms
                .default_tx_cap
                .min(budget_b.saturating_sub(without_tx));
            warnings.push(warning(
                "[memory.cap] tx",
                cap.to_string(),
                worst_case - budget_b,
                bytes(plan_choice),
            ));
        }
    }
    let lookahead = zakura.block_sync.max_reorder_lookahead_bytes;
    if lookahead > default_bs.max_reorder_lookahead_bytes && memory.caps.look_ahead.is_none() {
        let worst_case = without_bs.saturating_add(terms.look_ahead_cap);
        if worst_case > budget_b {
            warnings.push(warning(
                "[network.zakura.block_sync] max_reorder_lookahead_bytes",
                bytes(lookahead),
                worst_case - budget_b,
                bytes(default_bs.max_reorder_lookahead_bytes),
            ));
        }
    }
    let bbr_max = zakura.block_sync.bbr_max_cwnd_bytes;
    if bbr_max > default_bs.bbr_max_cwnd_bytes {
        let worst_case = without_bs.saturating_add(bbr_max);
        if worst_case > budget_b {
            warnings.push(warning(
                "[network.zakura.block_sync] bbr_max_cwnd_bytes",
                bytes(bbr_max),
                worst_case - budget_b,
                bytes(default_bs.bbr_max_cwnd_bytes),
            ));
        }
    }

    // Fixed terms: each raised setting warns once the whole plan passes `B`,
    // by at most what it adds.
    if required_bytes > budget_b {
        let over = required_bytes - budget_b;
        if zakura.max_pending_handshakes > DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES {
            let default_handshakes =
                (DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES as u64).saturating_mul(terms.handshake_cost);
            warnings.push(warning(
                "[network.zakura] max_pending_handshakes",
                zakura.max_pending_handshakes.to_string(),
                terms
                    .fixed
                    .handshakes
                    .saturating_sub(default_handshakes)
                    .min(over),
                DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES.to_string(),
            ));
        }
        let incoming = zakura.quic.incoming_buffer_total_bytes;
        if incoming > default_quic.incoming_buffer_total_bytes {
            let default_incoming = terms
                .bound_sockets
                .saturating_mul(default_quic.incoming_buffer_total_bytes);
            warnings.push(warning(
                "[network.zakura.quic] incoming_buffer_total_bytes",
                bytes(incoming),
                terms
                    .fixed
                    .incoming_buffers
                    .saturating_sub(default_incoming)
                    .min(over),
                bytes(default_quic.incoming_buffer_total_bytes),
            ));
        }
        let receive_window = zakura.quic.receive_window();
        if receive_window > DEFAULT_ZAKURA_RECEIVE_WINDOW {
            warnings.push(warning(
                "[network.zakura.quic] receive_window_bytes",
                bytes(u64::from(receive_window)),
                over,
                bytes(u64::from(DEFAULT_ZAKURA_RECEIVE_WINDOW)),
            ));
        }
        if zakura.quic.send_window_bytes > DEFAULT_ZAKURA_SEND_WINDOW {
            warnings.push(warning(
                "[network.zakura.quic] send_window_bytes",
                bytes(zakura.quic.send_window_bytes),
                over,
                bytes(DEFAULT_ZAKURA_SEND_WINDOW),
            ));
        }
        if memory.mempool_tx_cost_limit > DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES {
            warnings.push(warning(
                "[mempool] tx_cost_limit",
                bytes(memory.mempool_tx_cost_limit),
                (memory.mempool_tx_cost_limit - DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES).min(over),
                bytes(DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES),
            ));
        }
        let derived_reads = derived_read_result_byte_limit(config, network);
        if let Some(limit) = memory
            .read_result_byte_limit
            .filter(|&limit| limit > derived_reads)
        {
            warnings.push(warning(
                "[state] read_result_byte_limit",
                bytes(limit),
                (limit - derived_reads).min(over),
                bytes(derived_reads),
            ));
        }
    }
    warnings
}

/// One [`OverrideWarning`].
fn warning(
    setting: &str,
    configured_value: String,
    excess_bytes: u64,
    plan_value: String,
) -> OverrideWarning {
    OverrideWarning {
        setting: setting.to_owned(),
        configured_value,
        excess_bytes,
        plan_value,
    }
}

/// `config` with every setting that raises a fixed term or a reserve above
/// its default lowered back to the default. Lowered settings stay.
fn without_raised_settings(config: &Config, network: &Network) -> Config {
    let mut config = config.clone();
    let derived_reads = derived_read_result_byte_limit(&config, network);
    let zakura = &mut config.zakura;
    let default_quic = zakura_quic::QuicConfig::default();
    zakura.max_pending_handshakes = zakura
        .max_pending_handshakes
        .min(DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES);
    zakura.quic.incoming_buffer_total_bytes = zakura
        .quic
        .incoming_buffer_total_bytes
        .min(default_quic.incoming_buffer_total_bytes);
    zakura.quic.receive_window_bytes = zakura
        .quic
        .receive_window_bytes
        .map(|window| window.min(DEFAULT_ZAKURA_RECEIVE_WINDOW));
    zakura.quic.send_window_bytes = zakura
        .quic
        .send_window_bytes
        .min(DEFAULT_ZAKURA_SEND_WINDOW);
    zakura.max_connections = zakura
        .max_connections
        .map(|cap| cap.min(DEFAULT_SERVICE_MAX_PEERS));
    let memory = &mut zakura.memory_budget;
    memory.mempool_tx_cost_limit = memory
        .mempool_tx_cost_limit
        .min(DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES);
    memory.read_result_byte_limit = memory
        .read_result_byte_limit
        .map(|limit| limit.min(derived_reads));
    config
}

/// Verify that every native reactor passes `check_frame_cap` using its
/// unsolicited receive base alone (`02 §3`, `mem-1`).
fn validate_reactor_unsolicited_frame_caps(
    config: &Config,
    network: &Network,
    shares: &crate::zakura::ReactorShares,
) -> Result<(), MemoryPlanError> {
    use crate::zakura::{
        block_sync::BLOCK_SYNC_SERVICE_STREAMS, discovery::DISCOVERY_SERVICE_STREAMS,
        header_sync::v9 as hs_v9, legacy_gossip::LEGACY_GOSSIP_SERVICE_STREAMS,
        tx_relay::wire::TX_RELAY_STREAMS,
    };

    let check_streams = |streams: &[crate::zakura::Stream]| -> Result<(), MemoryPlanError> {
        for stream in streams {
            let budget = shares.share(stream.kind);
            if let Err(err) = budget.check_frame_cap(stream.frame_cap) {
                return Err(MemoryPlanError::InvalidFrameCap {
                    kind: stream.kind,
                    reason: err.to_string(),
                });
            }
        }
        Ok(())
    };

    check_streams(&LEGACY_GOSSIP_SERVICE_STREAMS)?;
    check_streams(&DISCOVERY_SERVICE_STREAMS)?;
    check_streams(hs_v9::layout(network))?;
    check_streams(&BLOCK_SYNC_SERVICE_STREAMS)?;
    if config.zakura.tx_relay.enabled {
        check_streams(&TX_RELAY_STREAMS)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::memory::draft::derive_p2p_budget;

    const GIB: u64 = 1024 * MIB;

    fn bases() -> SessionBases {
        let base = |receive, session| ReactorBase { receive, session };
        SessionBases {
            transport_base: 320 * KIB,
            header_sync: base(256 * KIB, 256 * KIB),
            block_sync: base(MIB, 4 * KIB),
            tx_relay: base(64 * KIB, 32 * KIB),
            legacy_compat: base(128 * KIB, 0),
            bpv1: base(512 * KIB, 96 * KIB),
            custom_kinds: base(256 * KIB, 0),
        }
    }

    const KIB: u64 = 1024;

    /// The P2P budget `B` that `host_budget_bytes` derives without a sidecar.
    fn host_b(host_budget_bytes: u64) -> u64 {
        derive_p2p_budget(host_budget_bytes, false).p2p_budget_b
    }

    /// A Zakura-only config on `network`.
    fn zakura_cfg(network: &Network) -> Config {
        Config {
            network: network.clone(),
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        }
    }

    /// The smallest `B` that `config` starts on Mainnet.
    fn minimum(config: &Config) -> u64 {
        MemoryPlan::size(256 * GIB, config, &Network::Mainnet)
            .expect("256 GiB fits")
            .required_minimum_bytes()
    }

    /// fix-37: a negotiated reactor adds its receive part, because receive
    /// credit covers it, and its session part only when its service admits
    /// the peer. Custom kinds count only when negotiated.
    #[test]
    fn connection_cost_charges_sessions_only_for_admitted_reactors() {
        let bases = bases();
        let all = SESSION_REACTOR_CAPABILITIES;

        let minimal = bases.connection_cost(0, all, false);
        assert_eq!(minimal.total(), bases.transport_base);

        let negotiated_only = bases.connection_cost(all, 0, false);
        assert_eq!(
            negotiated_only.total(),
            320 * KIB + 256 * KIB + MIB + 64 * KIB + 128 * KIB + 512 * KIB
        );
        assert_eq!(SessionBases::funded_sessions(all, 0), 0);

        let admitted = ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_BLOCK_SYNC_REGULATED;
        let cost = bases.connection_cost(all, admitted, false);
        assert_eq!(cost.header_sync, bases.header_sync.total());
        assert_eq!(cost.block_sync, bases.block_sync.total());
        assert_eq!(cost.tx_relay, bases.tx_relay.receive);
        assert_eq!(cost.bpv1, bases.bpv1.receive);
        assert_eq!(cost.custom_kinds, 0);
        assert_eq!(SessionBases::funded_sessions(all, admitted), admitted);

        let everything = bases.connection_cost(all, all, true);
        assert_eq!(everything.total(), bases.full_connection_bytes());
    }

    /// fix-37: a service that admits a session after admission funds its
    /// session part once.
    #[test]
    fn session_top_up_funds_each_reactor_once() {
        let bases = bases();
        let funded = ZAKURA_CAP_HEADER_SYNC;
        assert_eq!(bases.session_top_up(funded, ZAKURA_CAP_HEADER_SYNC), None);
        assert_eq!(
            bases.session_top_up(funded, ZAKURA_CAP_TX_RELAY),
            Some((ZAKURA_CAP_TX_RELAY, 32 * KIB))
        );
        // BPv1's session base follows each admitted session (`b_bpv1`).
        assert_eq!(
            bases.session_top_up(funded, ZAKURA_CAP_BPV1),
            Some((ZAKURA_CAP_BPV1, 96 * KIB))
        );
        // No session part, or no reactor these bases name.
        assert_eq!(bases.session_top_up(0, ZAKURA_CAP_LEGACY_GOSSIP), None);
        assert_eq!(bases.session_top_up(0, 0), None);
    }

    /// fix-32: inbound connections stop at three quarters of the sessions
    /// share, so the last quarter stays for dials; both directions stay within
    /// the share.
    #[test]
    fn session_directions_keep_the_dial_reserve() {
        let share = 400;
        let mut directions = SessionDirections::default();
        assert!(directions.try_charge(SessionSide::Inbound, 300, share));
        assert!(!directions.try_charge(SessionSide::Inbound, 1, share));
        assert!(directions.try_charge(SessionSide::Outbound, 100, share));
        assert!(!directions.try_charge(SessionSide::Outbound, 1, share));
        assert_eq!(directions.inbound + directions.outbound, share);

        directions.release(SessionSide::Inbound, 300);
        // Outbound connections may use the inbound part too.
        assert!(directions.try_charge(SessionSide::Outbound, 300, share));
        assert!(!directions.try_charge(SessionSide::Inbound, 1, share));
        assert_eq!(
            directions,
            SessionDirections {
                outbound: 400,
                ..SessionDirections::default()
            }
        );
    }

    /// Ordinary connections leave the gateway reserve free, as ordinary
    /// slots leave the held places, and inbound ones keep their quarter for
    /// dials out of what remains. A gateway may lease all that is left in
    /// the share, in either direction.
    #[test]
    fn session_directions_keep_the_gateway_reserve() {
        let share = 400;
        let mut directions = SessionDirections {
            gateway_reserve: 200,
            ..SessionDirections::default()
        };
        assert!(directions.try_charge(SessionSide::Inbound, 150, share));
        assert!(!directions.try_charge(SessionSide::Inbound, 1, share));
        assert!(directions.try_charge(SessionSide::Outbound, 50, share));
        assert!(!directions.try_charge(SessionSide::Outbound, 1, share));
        assert!(directions.try_charge(SessionSide::Gateway, 200, share));
        assert!(!directions.try_charge(SessionSide::Gateway, 1, share));

        // A smaller hold frees its reserve to ordinary connections, but the
        // gateways already admitted keep their bytes.
        directions.gateway_reserve = 0;
        assert!(!directions.try_charge(SessionSide::Outbound, 1, share));
        directions.release(SessionSide::Gateway, 200);
        assert!(directions.try_charge(SessionSide::Outbound, 200, share));
    }

    /// The reserve funds one full connection per held place, but never
    /// takes the floor that funds `min_connections` ordinary connections.
    #[test]
    fn the_gateway_reserve_keeps_the_sessions_floor() {
        let config = zakura_cfg(&Network::Mainnet);
        for budget in [8 * GIB, 16 * GIB, 64 * GIB] {
            let plan = MemoryPlan::size(budget, &config, &Network::Mainnet).expect("fits");
            let floor = plan.consumer_spec(Consumer::Sessions).floor;
            let full = plan.bases.full_connection_bytes();
            assert_eq!(plan.gateway_reserve(0), 0);
            for hold in 1..=crate::zakura::MAX_PREFERRED_GATEWAYS {
                let reserve = plan.gateway_reserve(hold);
                assert!(reserve <= hold as u64 * full, "{budget}");
                assert!(plan.sessions_share - reserve >= floor, "{budget}");
            }
        }
        let plan = MemoryPlan::size(64 * GIB, &config, &Network::Mainnet).expect("fits");
        assert_eq!(
            plan.gateway_reserve(crate::zakura::MAX_PREFERRED_GATEWAYS),
            crate::zakura::MAX_PREFERRED_GATEWAYS as u64 * plan.bases.full_connection_bytes()
        );
    }

    /// fix-34: without a node ID every bootstrap peer is remote, as production
    /// counts them.
    #[test]
    fn size_counts_every_bootstrap_peer_without_a_node_id() {
        let config = zakura_cfg(&Network::Mainnet);
        let outsider = zakura_quic::NodeSecretKey::generate().public();
        let without_id = MemoryPlan::size(64 * GIB, &config, &Network::Mainnet).expect("fits");
        let outside_node =
            MemoryPlan::size_for_node(64 * GIB, &config, &Network::Mainnet, Some(outsider), &[])
                .expect("fits");
        assert_eq!(without_id.min_connections, outside_node.min_connections);
    }

    /// The floor counts distinct parseable bootstrap identities other than
    /// this node's own, exactly as discovery does.
    #[test]
    fn the_bootstrap_floor_counts_each_remote_identity_once() {
        use zakura_quic::NodeSecretKey;

        let ids: Vec<NodeId> = (1..=16)
            .map(|seed| NodeSecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let entry = |id: &NodeId, port: u16| format!("{id}@192.0.2.1:{port}");
        let min_connections = |bootstrap_peers: Vec<String>, local_node_id: NodeId| {
            let mut config = zakura_cfg(&Network::Mainnet);
            config.zakura.bootstrap_peers = bootstrap_peers;
            MemoryPlan::size_for_node(
                256 * GIB,
                &config,
                &Network::Mainnet,
                Some(local_node_id),
                &[],
            )
            .expect("256 GiB fits")
            .min_connections
        };
        let outsider = NodeSecretKey::from_bytes(&[99; 32]).public();
        let listed: Vec<String> = ids.iter().map(|id| entry(id, 8234)).collect();

        // 16 remote identities need 33 connections.
        assert_eq!(min_connections(listed.clone(), outsider), 33);
        // Leaving this node's own entry out lowers the floor to match discovery.
        assert_eq!(min_connections(listed, ids[0]), 31);
        // Extra addresses of one identity and unparsable entries don't count.
        let repeated = ids
            .iter()
            .flat_map(|id| [entry(id, 8234), entry(id, 8235)])
            .chain(["not a bootstrap peer".to_owned()])
            .collect();
        assert_eq!(min_connections(repeated, outsider), 33);
    }

    /// fix-35 and fix-36: an override never lets startup pass a budget the
    /// unraised settings need, and an explicit connection cap keeps the base
    /// sessions floor.
    #[test]
    fn overrides_refuse_below_the_unraised_minimum_and_keep_the_floor() {
        let config = zakura_cfg(&Network::Mainnet);
        let default_minimum = minimum(&config);

        let mut raised = config.clone();
        raised.zakura.max_connections = Some(2_000);
        raised.zakura.max_pending_handshakes = 50_000;
        assert!(matches!(
            MemoryPlan::size(default_minimum - 1, &raised, &Network::Mainnet),
            Err(MemoryPlanError::BudgetTooSmall(_))
        ));

        // A lower connection cap lowers the reserves that scale with it, but
        // the sessions floor still funds `min_connections` full connections.
        let mut lowered = config.clone();
        lowered.zakura.max_connections = Some(64);
        let lowered_minimum = minimum(&lowered);
        assert!(matches!(
            MemoryPlan::size(lowered_minimum - 1, &lowered, &Network::Mainnet),
            Err(MemoryPlanError::BudgetTooSmall(_))
        ));
        let lowered_plan = MemoryPlan::size(lowered_minimum, &lowered, &Network::Mainnet)
            .expect("the minimum fits");
        let default_plan = MemoryPlan::size(default_minimum, &config, &Network::Mainnet)
            .expect("the minimum fits");
        assert_eq!(
            lowered_plan.consumer_spec(Consumer::Sessions).floor,
            default_plan.consumer_spec(Consumer::Sessions).floor
        );
        assert!(lowered_plan.override_warnings.is_empty());
    }

    /// fix-63: state reads are one fixed term, the derived read limit plus the
    /// state's parser allowance. `Dual` mode adds one legacy response.
    #[test]
    fn state_reads_count_the_derived_read_limit_and_parser_allowance() {
        let plan_for = |stack| {
            let config = Config {
                network: Network::Mainnet,
                p2p_stack: stack,
                ..Config::default()
            };
            let plan = MemoryPlan::size(256 * GIB, &config, &Network::Mainnet).expect("fits");
            assert_eq!(
                plan.fixed.state_reads,
                derived_read_result_byte_limit(&config, &Network::Mainnet)
                    + DEFAULT_STATE_READ_PARSER_BYTES
            );
            plan.fixed.state_reads
        };
        assert_eq!(
            plan_for(P2pStack::Dual) - plan_for(P2pStack::Zakura),
            READ_RESULT_BLOCKS_PER_PEER * MAX_DECODED_BLOCK_BYTES
        );
    }

    /// F-904: the compatibility sink's decoded work outlives its frames, so
    /// neither receive credit nor a lease holds it. The plan counts the sink's
    /// whole budget as a fixed term on every v2 stack.
    #[test]
    fn legacy_inbound_counts_the_compatibility_sink_budget() {
        for stack in [P2pStack::Zakura, P2pStack::Dual] {
            let config = Config {
                network: Network::Mainnet,
                p2p_stack: stack,
                ..Config::default()
            };
            let plan = MemoryPlan::size(256 * GIB, &config, &Network::Mainnet).expect("fits");
            assert_eq!(plan.fixed.legacy_inbound, LEGACY_INBOUND_RETAINED_BYTES);
            assert!(plan
                .fixed
                .entries()
                .contains(&("legacy_inbound", LEGACY_INBOUND_RETAINED_BYTES)));
        }
    }

    /// fix-63: an operator-set read limit above the derived one warns once the
    /// plan passes `B`; a lower one lowers the requirement without a warning.
    #[test]
    fn an_operator_read_limit_is_an_override() {
        let config = zakura_cfg(&Network::Mainnet);
        let default_minimum = minimum(&config);

        let mut raised = config.clone();
        raised.zakura.memory_budget.read_result_byte_limit = Some(64 * GIB);
        let plan = MemoryPlan::size(default_minimum, &raised, &Network::Mainnet)
            .expect("a raised setting starts with a warning");
        assert!(plan
            .override_warnings
            .iter()
            .any(|warning| warning.setting == "[state] read_result_byte_limit"));

        let mut lowered = config.clone();
        lowered.zakura.memory_budget.read_result_byte_limit = Some(MAX_DECODED_BLOCK_BYTES);
        let plan = MemoryPlan::size(default_minimum, &lowered, &Network::Mainnet).expect("fits");
        assert!(plan.override_warnings.is_empty());
        assert!(plan.required_minimum_bytes() < default_minimum);
    }

    /// mem-1 (`plan-fits`):
    /// Pure sweep from 256 MiB to 256 GiB on every network (Mainnet, Testnet,
    /// Regtest). Verifies:
    /// - For every accepted `B`, `fixed + reserves + floors <= B`.
    /// - `N` (both full and required connection counts) never decreases as `B` grows.
    /// - Every reactor passes `check_frame_cap`.
    /// - Below the smallest accepted `B`, the plan refuses and names `budget`,
    ///   `base`, `ρ`, `B`, and every term.
    #[test]
    fn mem1_plan_fits_sweep_all_networks() {
        // 64 MiB steps up to 8 GiB, 512 MiB steps up to 32 GiB, then 4 GiB
        // steps up to 256 GiB.
        let sweep_points: Vec<u64> = (256 * MIB..=8 * GIB)
            .step_by(64 * MIB as usize)
            .chain((8 * GIB + 512 * MIB..=32 * GIB).step_by(512 * MIB as usize))
            .chain((36 * GIB..=256 * GIB).step_by(4 * GIB as usize))
            .collect();

        let networks = [
            Network::Mainnet,
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ];

        let mut mainnet_minimums = Vec::new();
        for network in networks {
            for (row, (stack, tx_relay, bpv1)) in [
                (P2pStack::Zakura, false, false),
                (P2pStack::Zakura, true, false),
                (P2pStack::Zakura, false, true),
                (P2pStack::Zakura, true, true),
                (P2pStack::Dual, false, false),
            ]
            .into_iter()
            .enumerate()
            {
                let mut config = Config {
                    network: network.clone(),
                    p2p_stack: stack,
                    ..Config::default()
                };
                config.zakura.apply_network_defaults(&network);
                config.zakura.tx_relay.enabled = tx_relay;
                config.zakura.bpv1.enabled = bpv1;

                let mut first_accepted: Option<MemoryPlan> = None;
                let mut prev_full_n = 0usize;
                let mut prev_req_n = 0usize;

                for &budget_b in &sweep_points {
                    match MemoryPlan::size(budget_b, &config, &network) {
                        Ok(plan) => {
                            assert!(
                                plan.fixed.total() + plan.pool_config.total_floors() <= budget_b,
                                "fixed + reserves + floors exceeded B for {network:?}"
                            );
                            assert_eq!(
                                plan.fixed.total()
                                    + plan.pool_config.total_floors()
                                    + plan.shared_capacity(),
                                budget_b
                            );
                            assert!(
                                plan.planned_full_connections >= prev_full_n,
                                "full N decreased as B grew on {network:?}: {} < {}",
                                plan.planned_full_connections,
                                prev_full_n
                            );
                            assert!(
                                plan.planned_required_connections >= prev_req_n,
                                "required N decreased as B grew on {network:?}: {} < {}",
                                plan.planned_required_connections,
                                prev_req_n
                            );
                            assert!(
                                plan.planned_required_connections >= plan.min_connections,
                                "required N below min_connections on {network:?}"
                            );
                            assert!(
                                plan.planned_full_connections >= plan.min_connections,
                                "full N below min_connections on {network:?}"
                            );
                            prev_full_n = plan.planned_full_connections;
                            prev_req_n = plan.planned_required_connections;
                            if first_accepted.is_none() {
                                first_accepted = Some(plan);
                            }
                        }
                        Err(MemoryPlanError::BudgetTooSmall(err)) => {
                            assert!(
                                first_accepted.is_none(),
                                "plan refused after an earlier smaller B succeeded on {network:?}"
                            );
                            assert!(err.required_bytes > err.budget_b);
                        }
                        Err(other) => {
                            panic!("unexpected plan error on {network:?}: {other}");
                        }
                    }
                }

                let first = first_accepted.expect("plan must accept 256 GiB");
                let exact_min = first.required_minimum_bytes();
                let plan = MemoryPlan::size(exact_min, &config, &network).unwrap_or_else(|err| {
                    panic!("exact minimum {exact_min} must be accepted on {network:?}: {err}")
                });
                // fix-33: the smallest accepted budget funds `min_connections`
                // connections carrying every reactor.
                assert_eq!(
                    plan.consumer_spec(Consumer::Sessions).floor,
                    plan.min_connections as u64 * plan.bases.full_connection_bytes(),
                    "{network:?} row {row}"
                );
                // fix-40: Regtest has no checkpoints, so its consensus reserve
                // is the submission window, and it starts on a far smaller
                // budget than Mainnet.
                match network.kind() {
                    NetworkKind::Mainnet => mainnet_minimums.push(exact_min),
                    NetworkKind::Regtest => {
                        assert_eq!(
                            plan.consumer_spec(Consumer::Consensus).floor,
                            BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES
                        );
                        assert!(exact_min < mainnet_minimums[row], "row {row}");
                    }
                    NetworkKind::Testnet => {}
                }
                let err = MemoryPlan::size(exact_min - 1, &config, &network)
                    .expect_err("one byte below minimum must refuse")
                    .to_string();
                for needle in [
                    "host budget=",
                    "base=",
                    "rho=",
                    "resolved P2P budget B=",
                    "socket_queues=",
                    "header_chain=",
                    "consensus=",
                    "sessions=",
                ] {
                    assert!(
                        err.contains(needle),
                        "refusal message missing {needle:?}: {err}"
                    );
                }
            }
        }
    }

    /// Sanity check the Mainnet host budgets listed in `03 §2` (4, 8, 16, 32, 64 GiB)
    /// under draft Stage 0 values and D2 (chain sets minimum host), with BPv1
    /// off and on.
    #[test]
    fn mainnet_host_budget_progression_matches_d2() {
        for bpv1 in [false, true] {
            let mut config = zakura_cfg(&Network::Mainnet);
            config.zakura.bpv1.enabled = bpv1;
            host_budget_progression(&config);
        }
    }

    fn host_budget_progression(config: &Config) {
        let config = config.clone();

        // 4 GiB and 8 GiB refuse under draft Stage 0 base (~4.6 GiB) + worst checkpoint window (~3.7 GiB).
        for small_gib in [4u64, 8] {
            let b = host_b(small_gib * GIB);
            assert!(
                matches!(
                    MemoryPlan::size(b, &config, &Network::Mainnet),
                    Err(MemoryPlanError::BudgetTooSmall(_))
                ),
                "{small_gib} GiB should refuse under draft values (D2)"
            );
        }

        // 16, 32, and 64 GiB all fit and monotonically grow S and funded connections.
        let mut prev_s = 0u64;
        let mut prev_n = 0usize;
        for host_gib in [16u64, 32, 64] {
            let b = host_b(host_gib * GIB);
            let plan = MemoryPlan::size(b, &config, &Network::Mainnet)
                .unwrap_or_else(|err| panic!("{host_gib} GiB must fit: {err}"));
            assert!(plan.override_warnings.is_empty());
            assert!(plan.shared_capacity() >= prev_s);
            assert!(plan.planned_full_connections >= prev_n);
            prev_s = plan.shared_capacity();
            prev_n = plan.planned_full_connections;
        }
    }

    /// With BPv1 on, the `Propagation` reserve is BPv1's retained
    /// bound and each BPv1 session funds `b_bpv1`. The base send window
    /// already holds one block's BPv1 parts. With BPv1 off, every term stays
    /// as before.
    #[test]
    fn bpv1_sizes_its_reserve_session_base_and_send_window() {
        use crate::zakura::bpv1::block_push_bytes;

        let off = zakura_cfg(&Network::Mainnet);
        let mut on = off.clone();
        on.zakura.bpv1.enabled = true;
        let budget_b = host_b(16 * GIB);
        let off_plan = MemoryPlan::size(budget_b, &off, &Network::Mainnet).expect("16 GiB fits");
        let on_plan = MemoryPlan::size(budget_b, &on, &Network::Mainnet).expect("16 GiB fits");

        assert_eq!(
            off_plan.consumer_spec(Consumer::Propagation),
            ConsumerSpec::reserved(PROPAGATION_RESERVE_BYTES)
        );
        assert_eq!(off_plan.bases.bpv1, ReactorBase::default());
        assert_eq!(
            on_plan.consumer_spec(Consumer::Propagation),
            ConsumerSpec::reserved(PropagationReserve::new(&on.zakura.bpv1, NODE_PROFILE).total())
        );
        let base = Bpv1SessionBase::new(&on.zakura.bpv1);
        assert_eq!(
            on_plan.bases.bpv1,
            ReactorBase {
                receive: base.receive,
                session: base.session,
            }
        );
        assert!(BASE_SEND_WINDOW_BYTES >= block_push_bytes(NODE_PROFILE));
        assert_eq!(on_plan.bases.transport_base, off_plan.bases.transport_base);

        // `max_sessions` is unset by default, so it never warns. Set above
        // what the plan funds, it warns when its worst case passes `B`; set
        // below, it never does.
        assert!(on_plan.override_warnings.is_empty());
        for (max_sessions, warns) in [(256, true), (8, false)] {
            let mut capped = on.clone();
            capped.zakura.bpv1.max_sessions = Some(max_sessions);
            let plan = MemoryPlan::size(budget_b, &capped, &Network::Mainnet).expect("fits");
            assert_eq!(
                plan.override_warnings
                    .iter()
                    .any(|warning| warning.setting == "[network.zakura.bpv1] max_sessions"),
                warns,
                "max_sessions = {max_sessions}: {:?}",
                plan.override_warnings
            );
        }
    }

    /// BP-pt-2: a raised BPv1 setting that grows the `Propagation` reserve past
    /// `B` still refuses, because a reserve is held in full, but the refusal
    /// names the setting and its value. A default BPv1 refusal names none.
    #[test]
    fn a_raised_bpv1_reserve_refuses_and_names_its_setting() {
        let mut on = zakura_cfg(&Network::Mainnet);
        on.zakura.bpv1.enabled = true;
        let budget_b = host_b(16 * GIB);
        MemoryPlan::size(budget_b, &on, &Network::Mainnet).expect("the default reserve fits");

        let refusal =
            |config: &Config, budget_b| match MemoryPlan::size(budget_b, config, &Network::Mainnet)
            {
                Err(MemoryPlanError::BudgetTooSmall(refusal)) => refusal,
                other => panic!("expected a refusal: {other:?}"),
            };
        let mut part_buffer = on.clone();
        part_buffer.zakura.bpv1.part_retention_budget_bytes = 12 * GIB;
        let mut blocks = on.clone();
        blocks.zakura.bpv1.max_concurrent_blocks = 2_000;
        for (config, setting, value) in [
            (
                &part_buffer,
                "[network.zakura.bpv1] part_retention_budget_bytes",
                "12 GiB",
            ),
            (
                &blocks,
                "[network.zakura.bpv1] max_concurrent_blocks",
                "2000",
            ),
        ] {
            let refusal = refusal(config, budget_b);
            assert_eq!(
                refusal
                    .raised_reserve_settings
                    .iter()
                    .map(|raised| (raised.setting, raised.configured_value.as_str()))
                    .collect::<Vec<_>>(),
                [(setting, value)]
            );
            let message = refusal.to_string();
            assert!(
                message.contains(&format!("{setting} = {value} raises a reserve")),
                "{message}"
            );
        }
        assert!(refusal(&on, GIB).raised_reserve_settings.is_empty());
    }

    /// mem-14 (`overrides-warn`):
    /// - Default settings never warn and refuse below minimum `B`.
    /// - Operator overrides whose worst case exceeds `B` start anyway and emit
    ///   one warning per offending setting naming the setting, excess bytes,
    ///   and the plan's baseline value.
    /// - Lowering caps or setting `max_connections` within `B` produces zero
    ///   warnings.
    /// - Static/session overrides that push live memory past `B` saturate `S`
    ///   at 0, and the pool this plan sizes reports a live overrun; default
    ///   settings never do. `memory::tests` checks the overrun warning's rate
    ///   limit.
    #[test]
    fn mem14_operator_overrides_warn_at_startup_and_overrun_live() {
        use zakura_rt::time::Instant;

        use crate::zakura::memory::MemoryPoolMachine;

        let budget_16g = host_b(16 * GIB);

        // 1. Default settings fit with zero warnings and never overrun live.
        let default_cfg = zakura_cfg(&Network::Mainnet);
        let default_plan =
            MemoryPlan::size(budget_16g, &default_cfg, &Network::Mainnet).expect("16 GiB fits");
        assert!(default_plan.override_warnings.is_empty());
        let mut default_machine = MemoryPoolMachine::new(default_plan.pool_config);
        let now = Instant::now();
        default_machine
            .try_lease(
                Consumer::Sessions,
                default_plan.consumer_protected_share(Consumer::Sessions),
            )
            .expect("default sessions protected share fits");
        assert!(
            default_machine.check_overrun(now).is_none(),
            "default settings must never produce a live overrun"
        );

        // 2. Operator overrides that stay within B produce zero warnings.
        let mut modest_cfg = default_cfg.clone();
        modest_cfg.zakura.max_connections = Some(64);
        modest_cfg.zakura.memory_budget.caps.look_ahead = Some(ByteSize(512 * MIB));
        modest_cfg.zakura.memory_budget.caps.serving_per_peer = Some(ByteSize(64 * MIB));
        modest_cfg.zakura.memory_budget.caps.tx = Some(ByteSize(32 * MIB));
        let modest_plan =
            MemoryPlan::size(budget_16g, &modest_cfg, &Network::Mainnet).expect("modest fits");
        assert!(
            modest_plan.override_warnings.is_empty(),
            "overrides within B must not warn: {:?}",
            modest_plan.override_warnings
        );

        // 3. Operator overrides whose worst case exceeds B start anyway and
        //    report each offending setting, its excess bytes, and plan value.
        let mut overrun_cfg = default_cfg.clone();
        overrun_cfg.zakura.max_connections = Some(2_000);
        overrun_cfg.zakura.memory_budget.caps.look_ahead = Some(ByteSize(32 * GIB));
        overrun_cfg.zakura.memory_budget.caps.tx = Some(ByteSize(32 * GIB));
        overrun_cfg.zakura.max_pending_handshakes = 50_000;
        let overrun_plan = MemoryPlan::size(budget_16g, &overrun_cfg, &Network::Mainnet)
            .expect("operator overrides start anyway with warnings");
        assert_eq!(
            overrun_plan.shared_capacity(),
            0,
            "S saturates at 0 so short-lived leases stay bounded by remaining B"
        );
        for expected_setting in [
            "[network.zakura] max_connections",
            "[memory.cap] look_ahead",
            "[memory.cap] tx",
            "[network.zakura] max_pending_handshakes",
        ] {
            let warning = overrun_plan
                .override_warnings
                .iter()
                .find(|w| w.setting == expected_setting)
                .unwrap_or_else(|| {
                    panic!(
                        "missing warning for {expected_setting}: {:?}",
                        overrun_plan.override_warnings
                    )
                });
            assert!(warning.excess_bytes > 0, "{warning}");
            assert!(!warning.plan_value.is_empty(), "{warning}");
        }

        // 4. The overridden plan hands the pool a live overrun.
        assert!(
            overrun_plan.fixed.total() + overrun_plan.pool_config.total_floors() > budget_16g,
            "the overridden fixed terms and floors exceed B"
        );
        let mut live_machine = MemoryPoolMachine::new(overrun_plan.pool_config);
        for consumer in Consumer::ALL {
            let floor = overrun_plan.consumer_spec(consumer).floor;
            if floor > 0 {
                live_machine
                    .try_lease(consumer, floor)
                    .expect("every consumer's floor is respected");
            }
        }
        let first = live_machine
            .check_overrun(Instant::now())
            .expect("live total above B produces an overrun warning");
        assert!(first.excess_bytes > 0);
    }
}
