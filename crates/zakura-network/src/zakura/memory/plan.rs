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
    Consumer, ConsumerSpec, MemoryPool, PoolConfig,
};
use crate::{
    config::{Config, P2pStack},
    zakura::{
        block_sync::{
            ZakuraBlockSyncConfig, BLOCK_SYNC_REACTOR_BUDGET, BLOCK_SYNC_SERVE_NODE_EXECUTION,
            BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES, BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES,
            DEFAULT_BS_BBR_MIN_CWND_BYTES, LARGEST_SERVED_BLOCK_RESPONSE_BYTES,
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
        legacy_gossip::{LEGACY_GOSSIP_BUDGET, LEGACY_REQUESTS_BUDGET},
        peer_memory::min_budgeted_connections,
        tx_relay::wire::{
            MAX_TX_BYTES, SESSION_HELD_OUTPUT_BYTES, TX_RELAY_DATA_BUDGET, TX_RELAY_STATE_BUDGET,
            ZAKURA_CAP_TX_RELAY,
        },
        ServicePeerDirection, ZakuraLocalLimits, DEFAULT_SERVICE_MAX_PEERS, MAX_BS_FRAME_BYTES,
        ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP,
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

/// Fixed non-leased memory for discovery's peer book and dial-backoff tables
/// (`01 §1`: ~25 MiB for 10k records + 32k backoff entries).
pub const DISCOVERY_TABLES_FIXED_BYTES: u64 = 25 * MIB;

/// Fixed non-leased memory for `tx-relay`'s `WantedIds` and `PendingSet` maps
/// (`01 §1`, `#2623`: ~150 MiB when `tx-relay` is enabled).
pub const TX_RELAY_MAPS_FIXED_BYTES: u64 = 150 * MIB;

/// Maximum concurrent tip blocks reserved for propagation paths on `main`
/// (before BPv1 lands in Commit 15).
const MAX_CONCURRENT_TIP_BLOCKS: u64 = 4;

/// Proof-of-work-bounded propagation reserve covering tip-block paths on `main`
/// (`goal-impl.md`: `4 × 4 × MAX_BLOCK_BYTES = 32 MB`). Commit 15 extends this
/// with BPv1 parts, early records, and prepared encodings when BPv1 merges.
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
}

impl FixedTerms {
    /// Sum of every fixed non-leased memory term.
    pub const fn total(&self) -> u64 {
        self.socket_queues
            .saturating_add(self.incoming_buffers)
            .saturating_add(self.handshakes)
            .saturating_add(self.header_chain)
            .saturating_add(self.discovery_tables)
            .saturating_add(self.tx_relay_maps)
            .saturating_add(self.state_reads)
            .saturating_add(self.mempool_pool)
            .saturating_add(self.legacy_tcp_ingress)
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
    /// Sum of unsolicited receive shares for any embedder custom stream kinds.
    pub custom_kinds: ReactorBase,
}

/// The reactors whose session bases [`SessionBases`] names, by capability bit.
const SESSION_REACTOR_CAPABILITIES: u64 = ZAKURA_CAP_HEADER_SYNC
    | ZAKURA_CAP_BLOCK_SYNC_REGULATED
    | ZAKURA_CAP_TX_RELAY
    | ZAKURA_CAP_LEGACY_GOSSIP;

impl SessionBases {
    /// Cost of one connection carrying the reactors required to follow the chain:
    /// discovery (in `t`), header sync, and block sync.
    pub const fn required_chain_connection_bytes(&self) -> u64 {
        self.transport_base
            .saturating_add(self.header_sync.total())
            .saturating_add(self.block_sync.total())
    }

    /// Cost of one connection carrying every supported reactor on this node
    /// (`t + Σ_r b_r`, `02 §8` rule 3).
    pub const fn full_connection_bytes(&self) -> u64 {
        self.required_chain_connection_bytes()
            .saturating_add(self.tx_relay.total())
            .saturating_add(self.legacy_compat.total())
            .saturating_add(self.custom_kinds.total())
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
    pub const fn connection_cost(
        &self,
        negotiated: u64,
        admitted: u64,
        custom_kinds_negotiated: bool,
    ) -> ConnectionSessionCost {
        let admitted = admitted & negotiated;
        ConnectionSessionCost {
            transport_base: self.transport_base,
            header_sync: reactor_cost(
                self.header_sync,
                negotiated,
                admitted,
                ZAKURA_CAP_HEADER_SYNC,
            ),
            block_sync: reactor_cost(
                self.block_sync,
                negotiated,
                admitted,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ),
            tx_relay: reactor_cost(self.tx_relay, negotiated, admitted, ZAKURA_CAP_TX_RELAY),
            legacy_compat: reactor_cost(
                self.legacy_compat,
                negotiated,
                admitted,
                ZAKURA_CAP_LEGACY_GOSSIP,
            ),
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
    pub const fn session_top_up(
        &self,
        funded_sessions: u64,
        capability: u64,
    ) -> Option<(u64, u64)> {
        let (bit, base) = if capability & ZAKURA_CAP_HEADER_SYNC != 0 {
            (ZAKURA_CAP_HEADER_SYNC, self.header_sync)
        } else if capability & ZAKURA_CAP_BLOCK_SYNC_REGULATED != 0 {
            (ZAKURA_CAP_BLOCK_SYNC_REGULATED, self.block_sync)
        } else if capability & ZAKURA_CAP_TX_RELAY != 0 {
            (ZAKURA_CAP_TX_RELAY, self.tx_relay)
        } else if capability & ZAKURA_CAP_LEGACY_GOSSIP != 0 {
            (ZAKURA_CAP_LEGACY_GOSSIP, self.legacy_compat)
        } else {
            return None;
        };
        if funded_sessions & bit != 0 || base.session == 0 {
            None
        } else {
            Some((bit, base.session))
        }
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

/// Bytes `Consumer::Sessions` has leased, by the direction of the connection
/// that holds them (`02 §8` rule 1).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionDirections {
    /// Bytes leased for connections the remote peer dialed.
    pub inbound: u64,
    /// Bytes leased for connections this node dialed.
    pub outbound: u64,
}

impl SessionDirections {
    /// Whether a connection in `direction` may lease `cost` more bytes of
    /// `sessions_share`, the sessions consumer's protected share.
    ///
    /// Both directions stay within the share (`02 §5` rule 4). Inbound
    /// connections also stop at [`inbound_sessions_share`], so inbound peers
    /// can't take the dial reserve.
    pub const fn admits(
        &self,
        direction: ServicePeerDirection,
        cost: u64,
        sessions_share: u64,
    ) -> bool {
        let total = self
            .inbound
            .saturating_add(self.outbound)
            .saturating_add(cost);
        if total > sessions_share {
            return false;
        }
        match direction {
            ServicePeerDirection::Inbound => {
                self.inbound.saturating_add(cost) <= inbound_sessions_share(sessions_share)
            }
            ServicePeerDirection::Outbound => true,
        }
    }

    /// Record `cost` bytes leased in `direction` if [`Self::admits`] allows them.
    pub fn try_charge(
        &mut self,
        direction: ServicePeerDirection,
        cost: u64,
        sessions_share: u64,
    ) -> bool {
        if !self.admits(direction, cost, sessions_share) {
            return false;
        }
        let held = self.direction_mut(direction);
        *held = held.saturating_add(cost);
        true
    }

    /// Return `cost` bytes that a connection in `direction` released.
    pub fn release(&mut self, direction: ServicePeerDirection, cost: u64) {
        let held = self.direction_mut(direction);
        debug_assert!(*held >= cost, "a connection releases only bytes it charged");
        *held = held.saturating_sub(cost);
    }

    fn direction_mut(&mut self, direction: ServicePeerDirection) -> &mut u64 {
        match direction {
            ServicePeerDirection::Inbound => &mut self.inbound,
            ServicePeerDirection::Outbound => &mut self.outbound,
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
    /// Total reserved consumer bytes.
    pub reserves_total: u64,
    /// Control reserve.
    pub reserve_control: u64,
    /// Propagation reserve.
    pub reserve_propagation: u64,
    /// Consensus reserve (worst checkpoint window).
    pub reserve_consensus: u64,
    /// Total bulk consumer floors.
    pub bulk_floors_total: u64,
    /// Block-sync floor.
    pub floor_block_sync: u64,
    /// Serving floor.
    pub floor_serving: u64,
    /// Sessions floor.
    pub floor_sessions: u64,
    /// Tx floor.
    pub floor_tx: u64,
    /// Minimum budgeted connections required for discovery + chain following.
    pub min_connections: usize,
}

impl fmt::Display for BudgetTooSmall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bases = &self.bases;
        write!(
            f,
            "insufficient [memory] budget: host budget={} B, base={} B, sidecar={} B, rho={}/{}, \
             resolved P2P budget B={} B, but required minimum is {} B \
             (fixed={} B [socket_queues={} B, incoming_buffers={} B, handshakes={} B, \
             header_chain={} B, discovery_tables={} B, tx_relay_maps={} B, state_reads={} B, \
             mempool_pool={} B, legacy_tcp_ingress={} B], \
             reserves={} B [control={} B, propagation={} B, consensus={} B], \
             floors={} B [block_sync={} B, serving={} B, sessions={} B ({} min conns × {} B), tx={} B], \
             connection bases [t={} B, header_sync={} B, block_sync={} B, tx_relay={} B, \
             legacy_compat={} B, custom_kinds={} B]); \
             raise [memory] budget or add host RAM",
            self.derivation.host_budget_bytes,
            self.derivation.base_bytes,
            self.derivation.sidecar_bytes,
            self.derivation.rho_numerator,
            self.derivation.rho_denominator,
            self.budget_b,
            self.required_bytes,
            self.fixed.total(),
            self.fixed.socket_queues,
            self.fixed.incoming_buffers,
            self.fixed.handshakes,
            self.fixed.header_chain,
            self.fixed.discovery_tables,
            self.fixed.tx_relay_maps,
            self.fixed.state_reads,
            self.fixed.mempool_pool,
            self.fixed.legacy_tcp_ingress,
            self.reserves_total,
            self.reserve_control,
            self.reserve_propagation,
            self.reserve_consensus,
            self.bulk_floors_total,
            self.floor_block_sync,
            self.floor_serving,
            self.floor_sessions,
            self.min_connections,
            bases.full_connection_bytes(),
            self.floor_tx,
            bases.transport_base,
            bases.header_sync.total(),
            bases.block_sync.total(),
            bases.tx_relay.total(),
            bases.legacy_compat.total(),
            bases.custom_kinds.total(),
        )
    }
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
        // The same plan with every raised setting back at its default. Startup
        // refuses only when neither fits: what a raised setting adds past `B`
        // warns instead (`02 §8`).
        let unraised = PlanTerms::compute(
            &without_raised_settings(config, network),
            network,
            local_node_id,
            custom_kinds,
        );
        let PlanTerms {
            connection_cap,
            min_connections,
            bound_sockets,
            handshake_cost,
            fixed,
            bases,
            reserve_control,
            reserve_propagation,
            reserve_consensus,
            floor_block_sync,
            default_look_ahead,
            look_ahead_cap,
            cap_block_sync,
            floor_serving,
            default_serving_per_peer,
            serving_per_peer_cap,
            cap_serving,
            floor_sessions,
            cap_sessions,
            floor_tx,
            default_tx_cap,
            tx_cap,
        } = terms;
        let reserves_total = terms.reserves_total();

        // Compute what the plan would have chosen under default settings so
        // operator override warnings can cite the plan's baseline values (`02 §8`).
        let full_cost = bases.full_connection_bytes().max(1);
        let req_cost = bases.required_chain_connection_bytes().max(1);
        let baseline_pool_config = PoolConfig::new(budget_b, fixed.total())
            .with_consumer(Consumer::Control, ConsumerSpec::reserved(reserve_control))
            .with_consumer(
                Consumer::Propagation,
                ConsumerSpec::reserved(reserve_propagation),
            )
            .with_consumer(
                Consumer::Consensus,
                ConsumerSpec::reserved(reserve_consensus),
            )
            .with_consumer(
                Consumer::BlockSync,
                ConsumerSpec::bulk(
                    floor_block_sync,
                    cap_block_sync,
                    MemoryWeights::default().block_sync,
                ),
            )
            .with_consumer(
                Consumer::Serving,
                ConsumerSpec::bulk(floor_serving, cap_serving, MemoryWeights::default().serving),
            )
            .with_consumer(
                Consumer::Sessions,
                ConsumerSpec::bulk(
                    floor_sessions,
                    cap_sessions,
                    MemoryWeights::default().sessions,
                ),
            )
            .with_consumer(
                Consumer::Tx,
                ConsumerSpec::bulk(floor_tx, tx_cap, MemoryWeights::default().tx),
            );
        let baseline_sessions_share =
            super::MemoryPoolMachine::new(baseline_pool_config).protected_share(Consumer::Sessions);
        let plan_default_full_connections = usize::try_from(baseline_sessions_share / full_cost)
            .unwrap_or(usize::MAX)
            .min(DEFAULT_SERVICE_MAX_PEERS);

        // Detect operator overrides (`02 §8`, `mem-14`).
        let explicit_max_connections = config.zakura.max_connections;
        let explicit_hsync_caps = config
            .zakura
            .header_sync
            .peer_limits
            .has_explicit_peer_caps();
        let explicit_bsync_caps = config
            .zakura
            .block_sync
            .peer_limits
            .has_explicit_peer_caps();
        let default_quic = zakura_quic::QuicConfig::default();
        let default_bs = ZakuraBlockSyncConfig::default();
        let explicit_handshakes =
            config.zakura.max_pending_handshakes > DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES;
        let explicit_incoming = config.zakura.quic.incoming_buffer_total_bytes
            > default_quic.incoming_buffer_total_bytes;
        let explicit_recv_win = config.zakura.quic.receive_window() > DEFAULT_ZAKURA_RECEIVE_WINDOW;
        let explicit_send_win = config.zakura.quic.send_window_bytes > DEFAULT_ZAKURA_SEND_WINDOW;
        let explicit_mempool_cost =
            memory.mempool_tx_cost_limit > DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES;
        let explicit_lookahead_cfg = config.zakura.block_sync.max_reorder_lookahead_bytes
            > default_bs.max_reorder_lookahead_bytes;
        let explicit_bbr_max =
            config.zakura.block_sync.bbr_max_cwnd_bytes > default_bs.bbr_max_cwnd_bytes;

        let required_bytes = terms.required_bytes();

        let mut override_warnings = Vec::new();
        let non_session_static = fixed
            .total()
            .saturating_add(reserves_total)
            .saturating_add(floor_block_sync)
            .saturating_add(floor_serving)
            .saturating_add(floor_tx);

        // Only a setting raised past its default warns: a lowered one can't add
        // to the default worst case (fix-35).
        if let Some(configured_conns) =
            explicit_max_connections.filter(|&conns| conns > DEFAULT_SERVICE_MAX_PEERS)
        {
            let worst_case = non_session_static.saturating_add(cap_sessions);
            if worst_case > budget_b {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura] max_connections".to_owned(),
                    configured_value: configured_conns.to_string(),
                    excess_bytes: worst_case - budget_b,
                    plan_value: format!("{plan_default_full_connections} connections"),
                });
            }
        }
        if explicit_hsync_caps {
            let raw_hsync = config
                .zakura
                .header_sync
                .peer_limits
                .max_inbound()
                .saturating_add(config.zakura.header_sync.peer_limits.max_outbound());
            let effective_hsync = raw_hsync.min(connection_cap);
            let hsync_worst = non_session_static.saturating_add(cap_sessions);
            if effective_hsync > plan_default_full_connections && hsync_worst > budget_b {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.header_sync.peer_limits]".to_owned(),
                    configured_value: format!("{effective_hsync} sessions"),
                    excess_bytes: hsync_worst - budget_b,
                    plan_value: format!("unset (plan funds {plan_default_full_connections})"),
                });
            }
        }
        if explicit_bsync_caps {
            let raw_bsync = config
                .zakura
                .block_sync
                .peer_limits
                .max_inbound()
                .saturating_add(config.zakura.block_sync.peer_limits.max_outbound());
            let effective_bsync = raw_bsync.min(connection_cap);
            let bsync_worst = non_session_static.saturating_add(cap_sessions);
            if effective_bsync > plan_default_full_connections && bsync_worst > budget_b {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.block_sync.peer_limits]".to_owned(),
                    configured_value: format!("{effective_bsync} sessions"),
                    excess_bytes: bsync_worst - budget_b,
                    plan_value: format!("unset (plan funds {plan_default_full_connections})"),
                });
            }
        }
        if let Some(cap) = memory.caps.look_ahead {
            let without_bs = fixed
                .total()
                .saturating_add(reserves_total)
                .saturating_add(floor_serving)
                .saturating_add(floor_sessions)
                .saturating_add(floor_tx);
            let worst_case = without_bs.saturating_add(look_ahead_cap);
            if look_ahead_cap > default_look_ahead && worst_case > budget_b {
                let plan_choice = default_look_ahead.min(budget_b.saturating_sub(without_bs));
                override_warnings.push(OverrideWarning {
                    setting: "[memory.cap] look_ahead".to_owned(),
                    configured_value: cap.to_string(),
                    excess_bytes: worst_case - budget_b,
                    plan_value: ByteSize(plan_choice).to_string(),
                });
            }
        }
        if let Some(cap) = memory.caps.serving_per_peer {
            let without_serving = fixed
                .total()
                .saturating_add(reserves_total)
                .saturating_add(floor_block_sync)
                .saturating_add(floor_sessions)
                .saturating_add(floor_tx);
            let single_worst = without_serving.saturating_add(serving_per_peer_cap);
            let multi_worst = without_serving.saturating_add(
                serving_per_peer_cap.saturating_mul(plan_default_full_connections.max(1) as u64),
            );
            if cap.bytes() > default_serving_per_peer
                && (single_worst > budget_b || multi_worst > budget_b)
            {
                let excess_bytes = if single_worst > budget_b {
                    single_worst - budget_b
                } else {
                    multi_worst - budget_b
                };
                override_warnings.push(OverrideWarning {
                    setting: "[memory.cap] serving_per_peer".to_owned(),
                    configured_value: cap.to_string(),
                    excess_bytes,
                    plan_value: ByteSize(default_serving_per_peer).to_string(),
                });
            }
        }
        if let Some(cap) = memory.caps.tx {
            let without_tx = fixed
                .total()
                .saturating_add(reserves_total)
                .saturating_add(floor_block_sync)
                .saturating_add(floor_serving)
                .saturating_add(floor_sessions);
            let worst_case = without_tx.saturating_add(tx_cap);
            if tx_cap > default_tx_cap && worst_case > budget_b {
                let plan_choice = default_tx_cap.min(budget_b.saturating_sub(without_tx));
                override_warnings.push(OverrideWarning {
                    setting: "[memory.cap] tx".to_owned(),
                    configured_value: cap.to_string(),
                    excess_bytes: worst_case - budget_b,
                    plan_value: ByteSize(plan_choice).to_string(),
                });
            }
        }
        if explicit_lookahead_cfg && memory.caps.look_ahead.is_none() {
            let without_bs = fixed
                .total()
                .saturating_add(reserves_total)
                .saturating_add(floor_serving)
                .saturating_add(floor_sessions)
                .saturating_add(floor_tx);
            let worst_case = without_bs.saturating_add(look_ahead_cap);
            if worst_case > budget_b {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.block_sync] max_reorder_lookahead_bytes".to_owned(),
                    configured_value: ByteSize(
                        config.zakura.block_sync.max_reorder_lookahead_bytes,
                    )
                    .to_string(),
                    excess_bytes: worst_case - budget_b,
                    plan_value: ByteSize(default_bs.max_reorder_lookahead_bytes).to_string(),
                });
            }
        }
        if explicit_bbr_max {
            let without_bs = fixed
                .total()
                .saturating_add(reserves_total)
                .saturating_add(floor_serving)
                .saturating_add(floor_sessions)
                .saturating_add(floor_tx);
            let worst_case = without_bs.saturating_add(config.zakura.block_sync.bbr_max_cwnd_bytes);
            if worst_case > budget_b {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.block_sync] bbr_max_cwnd_bytes".to_owned(),
                    configured_value: ByteSize(config.zakura.block_sync.bbr_max_cwnd_bytes)
                        .to_string(),
                    excess_bytes: worst_case - budget_b,
                    plan_value: ByteSize(default_bs.bbr_max_cwnd_bytes).to_string(),
                });
            }
        }
        if required_bytes > budget_b {
            if explicit_handshakes {
                let default_handshakes =
                    (DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES as u64).saturating_mul(handshake_cost);
                let delta = fixed.handshakes.saturating_sub(default_handshakes);
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura] max_pending_handshakes".to_owned(),
                    configured_value: config.zakura.max_pending_handshakes.to_string(),
                    excess_bytes: delta.min(required_bytes - budget_b),
                    plan_value: DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES.to_string(),
                });
            }
            if explicit_incoming {
                let default_incoming =
                    bound_sockets.saturating_mul(default_quic.incoming_buffer_total_bytes);
                let delta = fixed.incoming_buffers.saturating_sub(default_incoming);
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.quic] incoming_buffer_total_bytes".to_owned(),
                    configured_value: ByteSize(config.zakura.quic.incoming_buffer_total_bytes)
                        .to_string(),
                    excess_bytes: delta.min(required_bytes - budget_b),
                    plan_value: ByteSize(default_quic.incoming_buffer_total_bytes).to_string(),
                });
            }
            if explicit_recv_win {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.quic] receive_window_bytes".to_owned(),
                    configured_value: ByteSize(u64::from(config.zakura.quic.receive_window()))
                        .to_string(),
                    excess_bytes: required_bytes - budget_b,
                    plan_value: ByteSize(u64::from(DEFAULT_ZAKURA_RECEIVE_WINDOW)).to_string(),
                });
            }
            if explicit_send_win {
                override_warnings.push(OverrideWarning {
                    setting: "[network.zakura.quic] send_window_bytes".to_owned(),
                    configured_value: ByteSize(config.zakura.quic.send_window_bytes).to_string(),
                    excess_bytes: required_bytes - budget_b,
                    plan_value: ByteSize(DEFAULT_ZAKURA_SEND_WINDOW).to_string(),
                });
            }
            if explicit_mempool_cost {
                let delta = memory
                    .mempool_tx_cost_limit
                    .saturating_sub(DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES);
                override_warnings.push(OverrideWarning {
                    setting: "[mempool] tx_cost_limit".to_owned(),
                    configured_value: ByteSize(memory.mempool_tx_cost_limit).to_string(),
                    excess_bytes: delta.min(required_bytes - budget_b),
                    plan_value: ByteSize(DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES).to_string(),
                });
            }
            let derived_reads = derived_read_result_byte_limit(config, network);
            if let Some(limit) = memory
                .read_result_byte_limit
                .filter(|&limit| limit > derived_reads)
            {
                override_warnings.push(OverrideWarning {
                    setting: "[state] read_result_byte_limit".to_owned(),
                    configured_value: ByteSize(limit).to_string(),
                    excess_bytes: (limit - derived_reads).min(required_bytes - budget_b),
                    plan_value: ByteSize(derived_reads).to_string(),
                });
            }
        }

        let mut derivation = memory.derivation;
        derivation.p2p_budget_b = budget_b;

        // Startup refuses when fixed + reserves + floors exceed `B` even with
        // every raised setting back at its default (`02 §8`). Otherwise it
        // starts, and `override_warnings` names each raised setting whose
        // excess pushes the plan past `B`.
        let smallest = if unraised.required_bytes() < required_bytes {
            &unraised
        } else {
            &terms
        };
        if smallest.required_bytes() > budget_b {
            return Err(MemoryPlanError::BudgetTooSmall(Box::new(
                smallest.budget_too_small(derivation, budget_b),
            )));
        }

        let pool_config = PoolConfig::new(budget_b, fixed.total())
            .with_consumer(Consumer::Control, ConsumerSpec::reserved(reserve_control))
            .with_consumer(
                Consumer::Propagation,
                ConsumerSpec::reserved(reserve_propagation),
            )
            .with_consumer(
                Consumer::Consensus,
                ConsumerSpec::reserved(reserve_consensus),
            )
            .with_consumer(
                Consumer::BlockSync,
                ConsumerSpec::bulk(floor_block_sync, cap_block_sync, memory.weights.block_sync),
            )
            .with_consumer(
                Consumer::Serving,
                ConsumerSpec::bulk(floor_serving, cap_serving, memory.weights.serving),
            )
            .with_consumer(
                Consumer::Sessions,
                ConsumerSpec::bulk(floor_sessions, cap_sessions, memory.weights.sessions),
            )
            .with_consumer(
                Consumer::Tx,
                ConsumerSpec::bulk(floor_tx, tx_cap, memory.weights.tx),
            );

        let machine = super::MemoryPoolMachine::new(pool_config);
        let sessions_protected = machine.protected_share(Consumer::Sessions);

        let planned_full_connections = usize::try_from(sessions_protected / full_cost)
            .unwrap_or(usize::MAX)
            .min(connection_cap);
        let planned_required_connections = usize::try_from(sessions_protected / req_cost)
            .unwrap_or(usize::MAX)
            .min(connection_cap);

        let network_name = match network.kind() {
            NetworkKind::Mainnet => "mainnet",
            NetworkKind::Testnet => "testnet",
            NetworkKind::Regtest => "regtest",
        };

        Ok(Self {
            derivation,
            source: memory.source,
            network_name,
            fixed,
            bases,
            custom_kinds: custom_kinds.to_vec(),
            sessions_share: sessions_protected.min(cap_sessions),
            pool_config,
            min_connections,
            connection_cap,
            planned_full_connections,
            planned_required_connections,
            look_ahead_cap,
            serving_per_peer_cap,
            tx_cap,
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
    pub const fn required_minimum_bytes(&self) -> u64 {
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
        let mut limits = ZakuraLocalLimits::for_node(config, &self.custom_kinds);
        limits.max_connections = self.connection_cap.max(1);
        limits
    }

    /// Construct a live [`MemoryPool`] sized from this plan.
    pub fn build_pool(&self) -> MemoryPool {
        MemoryPool::new(self.pool_config)
    }

    /// Publish `zakura.memory.plan_bytes{term}` startup gauges (`03 §5`).
    pub fn publish_metrics(&self) {
        let terms: [(&'static str, u64); 25] = [
            ("budget_b", self.pool_config.budget_b),
            ("fixed_total", self.fixed.total()),
            ("fixed_socket_queues", self.fixed.socket_queues),
            ("fixed_incoming_buffers", self.fixed.incoming_buffers),
            ("fixed_handshakes", self.fixed.handshakes),
            ("fixed_header_chain", self.fixed.header_chain),
            ("fixed_discovery_tables", self.fixed.discovery_tables),
            ("fixed_tx_relay_maps", self.fixed.tx_relay_maps),
            ("fixed_state_reads", self.fixed.state_reads),
            ("fixed_mempool_pool", self.fixed.mempool_pool),
            ("fixed_legacy_tcp_ingress", self.fixed.legacy_tcp_ingress),
            (
                "reserve_control",
                self.consumer_spec(Consumer::Control).floor,
            ),
            (
                "reserve_propagation",
                self.consumer_spec(Consumer::Propagation).floor,
            ),
            (
                "reserve_consensus",
                self.consumer_spec(Consumer::Consensus).floor,
            ),
            (
                "floor_block_sync",
                self.consumer_spec(Consumer::BlockSync).floor,
            ),
            ("floor_serving", self.consumer_spec(Consumer::Serving).floor),
            (
                "floor_sessions",
                self.consumer_spec(Consumer::Sessions).floor,
            ),
            ("floor_tx", self.consumer_spec(Consumer::Tx).floor),
            ("shared_s", self.shared_capacity()),
            ("transport_base", self.bases.transport_base),
            ("session_header_sync", self.bases.header_sync.total()),
            ("session_block_sync", self.bases.block_sync.total()),
            ("session_tx_relay", self.bases.tx_relay.total()),
            ("session_legacy_compat", self.bases.legacy_compat.total()),
            ("full_connection_base", self.bases.full_connection_bytes()),
        ];
        for (term, bytes) in terms {
            metrics::gauge!("zakura.memory.plan_bytes", "term" => term).set(bytes as f64);
        }
    }
}

impl fmt::Display for MemoryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MemoryPlan(network={}, B={} B, fixed={} B, floors+reserves={} B, S={} B, \
             t={} B, full_conn={} B, planned_full_conns={}/{})",
            self.network_name,
            self.pool_config.budget_b,
            self.fixed.total(),
            self.pool_config.total_floors(),
            self.shared_capacity(),
            self.bases.transport_base,
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
    reserve_control: u64,
    reserve_propagation: u64,
    reserve_consensus: u64,
    floor_block_sync: u64,
    default_look_ahead: u64,
    look_ahead_cap: u64,
    cap_block_sync: u64,
    floor_serving: u64,
    default_serving_per_peer: u64,
    serving_per_peer_cap: u64,
    cap_serving: u64,
    floor_sessions: u64,
    cap_sessions: u64,
    floor_tx: u64,
    default_tx_cap: u64,
    tx_cap: u64,
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
            (connection_cap as u64).saturating_mul(config.zakura.block_sync.bbr_max_cwnd_bytes);
        let cap_block_sync = block_sync_inflight_cap
            .saturating_add(look_ahead_cap)
            .max(floor_block_sync);

        let floor_serving = LARGEST_SERVED_BLOCK_RESPONSE_BYTES;
        let default_serving_per_peer =
            DEFAULT_ZAKURA_SEND_WINDOW.saturating_add(SERVE_PEER_OUTPUT_BYTES);
        let serving_per_peer_cap = memory
            .caps
            .serving_per_peer
            .map_or(default_serving_per_peer, ByteSize::bytes)
            .max(floor_serving);
        let cap_serving = (connection_cap as u64)
            .saturating_mul(serving_per_peer_cap)
            .max(floor_serving);

        // The floor funds `min_connections` connections that each run every
        // reactor, because admission funds every reactor a peer supports
        // together (`02 §8` rules 3 and 4). Reactor caps and `N_cap` are
        // ceilings, not reservations, so they never raise the floor.
        let floor_sessions = (min_connections as u64).saturating_mul(bases.full_connection_bytes());
        let header_sync_cap = config.zakura.header_sync.serving_sessions(connection_cap);
        let block_sync_cap = config
            .zakura
            .block_sync
            .block_sync_connections(connection_cap);
        // Receive parts follow negotiation, so every connection may hold
        // them. Session parts follow each reactor's sessions.
        let every_connection = bases
            .transport_base
            .saturating_add(bases.header_sync.receive)
            .saturating_add(bases.block_sync.receive)
            .saturating_add(bases.tx_relay.total())
            .saturating_add(bases.legacy_compat.total())
            .saturating_add(bases.custom_kinds.total());
        let cap_sessions = (connection_cap as u64)
            .saturating_mul(every_connection)
            .saturating_add((header_sync_cap as u64).saturating_mul(bases.header_sync.session))
            .saturating_add((block_sync_cap as u64).saturating_mul(bases.block_sync.session))
            .max(floor_sessions);

        let floor_tx = u64::from(MAX_TX_BYTES);
        let default_tx_cap = memory.mempool_tx_cost_limit.max(floor_tx);
        let tx_cap = memory
            .caps
            .tx
            .map_or(default_tx_cap, ByteSize::bytes)
            .max(floor_tx);

        Self {
            connection_cap,
            min_connections,
            bound_sockets,
            handshake_cost,
            fixed,
            bases,
            // Reserved consumers (`Priority::Reserved`, `floor == cap`, `02 §4`).
            reserve_control: discovery_serving_output_bytes(connection_cap),
            reserve_propagation: PROPAGATION_RESERVE_BYTES,
            reserve_consensus: draft_worst_checkpoint_window_bytes(network)
                .max(BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES),
            floor_block_sync,
            default_look_ahead,
            look_ahead_cap,
            cap_block_sync,
            floor_serving,
            default_serving_per_peer,
            serving_per_peer_cap,
            cap_serving,
            floor_sessions,
            cap_sessions,
            floor_tx,
            default_tx_cap,
            tx_cap,
        }
    }

    const fn reserves_total(&self) -> u64 {
        self.reserve_control
            .saturating_add(self.reserve_propagation)
            .saturating_add(self.reserve_consensus)
    }

    const fn bulk_floors_total(&self) -> u64 {
        self.floor_block_sync
            .saturating_add(self.floor_serving)
            .saturating_add(self.floor_sessions)
            .saturating_add(self.floor_tx)
    }

    /// `fixed + reserves + Σ floors`, which must fit in `B` (`02 §8`).
    const fn required_bytes(&self) -> u64 {
        self.fixed
            .total()
            .saturating_add(self.reserves_total())
            .saturating_add(self.bulk_floors_total())
    }

    fn budget_too_small(&self, derivation: BudgetDerivation, budget_b: u64) -> BudgetTooSmall {
        BudgetTooSmall {
            derivation,
            budget_b,
            required_bytes: self.required_bytes(),
            fixed: self.fixed,
            bases: self.bases,
            reserves_total: self.reserves_total(),
            reserve_control: self.reserve_control,
            reserve_propagation: self.reserve_propagation,
            reserve_consensus: self.reserve_consensus,
            bulk_floors_total: self.bulk_floors_total(),
            floor_block_sync: self.floor_block_sync,
            floor_serving: self.floor_serving,
            floor_sessions: self.floor_sessions,
            floor_tx: self.floor_tx,
            min_connections: self.min_connections,
        }
    }
}

/// `config` with every setting that raises a fixed term or a reserve above
/// its default lowered back to the default. Lowered settings stay.
fn without_raised_settings(config: &Config, network: &Network) -> Config {
    let mut config = config.clone();
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
    zakura.memory_budget.mempool_tx_cost_limit = zakura
        .memory_budget
        .mempool_tx_cost_limit
        .min(DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES);
    let derived_reads = derived_read_result_byte_limit(&config, network);
    let zakura = &mut config.zakura;
    zakura.memory_budget.read_result_byte_limit = zakura
        .memory_budget
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
    use crate::zakura::memory::draft::resolved_p2p_budget_bytes;

    const GIB: u64 = 1024 * MIB;

    fn bases() -> SessionBases {
        let base = |receive, session| ReactorBase { receive, session };
        SessionBases {
            transport_base: 320 * KIB,
            header_sync: base(256 * KIB, 256 * KIB),
            block_sync: base(MIB, 4 * KIB),
            tx_relay: base(64 * KIB, 32 * KIB),
            legacy_compat: base(128 * KIB, 0),
            custom_kinds: base(256 * KIB, 0),
        }
    }

    const KIB: u64 = 1024;

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
            320 * KIB + 256 * KIB + MIB + 64 * KIB + 128 * KIB
        );
        assert_eq!(SessionBases::funded_sessions(all, 0), 0);

        let admitted = ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_BLOCK_SYNC_REGULATED;
        let cost = bases.connection_cost(all, admitted, false);
        assert_eq!(cost.header_sync, bases.header_sync.total());
        assert_eq!(cost.block_sync, bases.block_sync.total());
        assert_eq!(cost.tx_relay, bases.tx_relay.receive);
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
        assert!(directions.try_charge(ServicePeerDirection::Inbound, 300, share));
        assert!(!directions.try_charge(ServicePeerDirection::Inbound, 1, share));
        assert!(directions.try_charge(ServicePeerDirection::Outbound, 100, share));
        assert!(!directions.try_charge(ServicePeerDirection::Outbound, 1, share));
        assert_eq!(directions.inbound + directions.outbound, share);

        directions.release(ServicePeerDirection::Inbound, 300);
        // Outbound connections may use the inbound part too.
        assert!(directions.try_charge(ServicePeerDirection::Outbound, 300, share));
        assert!(!directions.try_charge(ServicePeerDirection::Inbound, 1, share));
        assert_eq!(
            directions,
            SessionDirections {
                inbound: 0,
                outbound: 400
            }
        );
    }

    /// fix-33: the smallest accepted budget funds `min_connections` connections
    /// carrying every reactor.
    #[test]
    fn the_sessions_floor_funds_min_full_connections() {
        for network in [Network::Mainnet, Network::new_default_testnet()] {
            let config = Config {
                network: network.clone(),
                p2p_stack: P2pStack::Zakura,
                ..Config::default()
            };
            let probe = MemoryPlan::size(256 * GIB, &config, &network).expect("256 GiB fits");
            let plan = MemoryPlan::size(probe.required_minimum_bytes(), &config, &network)
                .expect("the exact minimum fits");
            assert_eq!(
                plan.consumer_spec(Consumer::Sessions).floor,
                plan.min_connections as u64 * plan.bases.full_connection_bytes()
            );
            assert!(plan.planned_full_connections >= plan.min_connections);
        }
    }

    /// fix-34: without a node ID every bootstrap peer is remote, as production
    /// counts them.
    #[test]
    fn size_counts_every_bootstrap_peer_without_a_node_id() {
        let config = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
        let outsider = zakura_quic::NodeSecretKey::generate().public();
        let without_id = MemoryPlan::size(64 * GIB, &config, &Network::Mainnet).expect("fits");
        let outside_node =
            MemoryPlan::size_for_node(64 * GIB, &config, &Network::Mainnet, Some(outsider), &[])
                .expect("fits");
        assert_eq!(without_id.min_connections, outside_node.min_connections);
    }

    /// fix-35 and fix-36: an override never lets startup pass a budget the
    /// unraised settings need, and an explicit connection cap keeps the base
    /// sessions floor.
    #[test]
    fn overrides_refuse_below_the_unraised_minimum_and_keep_the_floor() {
        let config = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
        let probe = MemoryPlan::size(256 * GIB, &config, &Network::Mainnet).expect("fits");
        let minimum = probe.required_minimum_bytes();

        let mut raised = config.clone();
        raised.zakura.max_connections = Some(2_000);
        raised.zakura.max_pending_handshakes = 50_000;
        assert!(matches!(
            MemoryPlan::size(minimum - 1, &raised, &Network::Mainnet),
            Err(MemoryPlanError::BudgetTooSmall(_))
        ));

        // A lower connection cap lowers the reserves that scale with it, but
        // the sessions floor still funds `min_connections` full connections.
        let mut lowered = config.clone();
        lowered.zakura.max_connections = Some(64);
        let lowered_minimum = MemoryPlan::size(256 * GIB, &lowered, &Network::Mainnet)
            .expect("fits")
            .required_minimum_bytes();
        assert!(matches!(
            MemoryPlan::size(lowered_minimum - 1, &lowered, &Network::Mainnet),
            Err(MemoryPlanError::BudgetTooSmall(_))
        ));
        let lowered_plan = MemoryPlan::size(lowered_minimum, &lowered, &Network::Mainnet)
            .expect("the minimum fits");
        assert_eq!(
            lowered_plan.consumer_spec(Consumer::Sessions).floor,
            probe.consumer_spec(Consumer::Sessions).floor
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

    /// fix-63: an operator-set read limit above the derived one warns once the
    /// plan passes `B`; a lower one lowers the requirement without a warning.
    #[test]
    fn an_operator_read_limit_is_an_override() {
        let config = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
        let minimum = MemoryPlan::size(256 * GIB, &config, &Network::Mainnet)
            .expect("fits")
            .required_minimum_bytes();

        let mut raised = config.clone();
        raised.zakura.memory_budget.read_result_byte_limit = Some(64 * GIB);
        let plan = MemoryPlan::size(minimum, &raised, &Network::Mainnet)
            .expect("a raised setting starts with a warning");
        assert!(plan
            .override_warnings
            .iter()
            .any(|warning| warning.setting == "[state] read_result_byte_limit"));

        let mut lowered = config.clone();
        lowered.zakura.memory_budget.read_result_byte_limit = Some(MAX_DECODED_BLOCK_BYTES);
        let plan = MemoryPlan::size(minimum, &lowered, &Network::Mainnet).expect("fits");
        assert!(plan.override_warnings.is_empty());
        assert!(plan.required_minimum_bytes() < minimum);
    }

    /// fix-40: Regtest has no checkpoints, so its consensus reserve is the
    /// submission window and it starts on a far smaller budget than Mainnet.
    #[test]
    fn regtest_needs_no_checkpoint_window() {
        let regtest = Network::new_regtest(Default::default());
        let config = Config {
            network: regtest.clone(),
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
        let plan = MemoryPlan::size(256 * GIB, &config, &regtest).expect("fits");
        assert_eq!(
            plan.consumer_spec(Consumer::Consensus).floor,
            BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES
        );
        let mainnet = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
        let mainnet_plan = MemoryPlan::size(256 * GIB, &mainnet, &Network::Mainnet).expect("fits");
        assert!(plan.required_minimum_bytes() < mainnet_plan.required_minimum_bytes());
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
        let mut sweep_points: Vec<u64> = Vec::new();
        // Dense sweep from 256 MiB to 8 GiB in 64 MiB steps.
        let mut b = 256 * MIB;
        while b <= 8 * GIB {
            sweep_points.push(b);
            b += 64 * MIB;
        }
        // 8 GiB to 32 GiB in 512 MiB steps.
        b = 8 * GIB + 512 * MIB;
        while b <= 32 * GIB {
            sweep_points.push(b);
            b += 512 * MIB;
        }
        // 32 GiB to 256 GiB in 4 GiB steps.
        b = 36 * GIB;
        while b <= 256 * GIB {
            sweep_points.push(b);
            b += 4 * GIB;
        }

        let networks = [
            Network::Mainnet,
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ];

        for network in networks {
            for (stack, tx_relay) in [
                (P2pStack::Zakura, false),
                (P2pStack::Zakura, true),
                (P2pStack::Dual, false),
            ] {
                let mut config = Config {
                    network: network.clone(),
                    p2p_stack: stack,
                    ..Config::default()
                };
                config.zakura.apply_network_defaults(&network);
                config.zakura.tx_relay.enabled = tx_relay;

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
                assert!(
                    MemoryPlan::size(exact_min, &config, &network).is_ok(),
                    "exact minimum {exact_min} must be accepted on {network:?}"
                );
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
    /// under draft Stage 0 values and D2 (chain sets minimum host).
    #[test]
    fn mainnet_host_budget_progression_matches_d2() {
        let config = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };

        // 4 GiB and 8 GiB refuse under draft Stage 0 base (~4.6 GiB) + worst checkpoint window (~3.7 GiB).
        for small_gib in [4u64, 8] {
            let b = resolved_p2p_budget_bytes(small_gib * GIB, false);
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
            let b = resolved_p2p_budget_bytes(host_gib * GIB, false);
            let plan = MemoryPlan::size(b, &config, &Network::Mainnet)
                .unwrap_or_else(|err| panic!("{host_gib} GiB must fit: {err}"));
            assert!(plan.override_warnings.is_empty());
            assert!(plan.shared_capacity() >= prev_s);
            assert!(plan.planned_full_connections >= prev_n);
            prev_s = plan.shared_capacity();
            prev_n = plan.planned_full_connections;
        }
    }

    /// mem-14 (`overrides-warn`):
    /// - Default settings never warn and refuse below minimum `B`.
    /// - Operator overrides whose worst case exceeds `B` start anyway and emit
    ///   one warning per offending setting naming the setting, excess bytes,
    ///   and the plan's baseline value.
    /// - Lowering caps or setting `max_connections` within `B` produces zero
    ///   warnings.
    /// - Static/session overrides that push live memory past `B` saturate `S`
    ///   at 0 and trigger rate-limited overrun warnings (at most once every 10
    ///   minutes) under a mocked clock; default settings never trigger one.
    #[test]
    fn mem14_operator_overrides_warn_at_startup_and_rate_limit_live_overrun() {
        use std::time::Duration;

        use zakura_rt::time::Instant;

        use crate::zakura::memory::{MemoryPoolMachine, OVERRUN_WARN_INTERVAL};

        let budget_16g = resolved_p2p_budget_bytes(16 * GIB, false);

        // 1. Default settings fit with zero warnings and never overrun live.
        let default_cfg = Config {
            network: Network::Mainnet,
            p2p_stack: P2pStack::Zakura,
            ..Config::default()
        };
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

        // 4. Live overrun on the overridden plan fires at most once per 10
        //    minutes under a mocked clock (`Instant` arithmetic).
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
        let t0 = Instant::now();
        let first = live_machine
            .check_overrun(t0)
            .expect("live total above B produces an overrun warning");
        assert!(first.excess_bytes > 0);
        assert!(
            live_machine
                .check_overrun(t0 + Duration::from_secs(60))
                .is_none(),
            "suppressed within 10 minutes"
        );
        assert!(
            live_machine
                .check_overrun(t0 + OVERRUN_WARN_INTERVAL - Duration::from_secs(1))
                .is_none(),
            "still suppressed 1s before 10 minutes"
        );
        let second = live_machine
            .check_overrun(t0 + OVERRUN_WARN_INTERVAL)
            .expect("emits again at 10 minutes while overrun persists");
        assert_eq!(second.live_bytes, first.live_bytes);
    }
}
