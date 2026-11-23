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
    draft::{draft_worst_checkpoint_window_bytes, BudgetDerivation},
    Consumer, ConsumerSpec, MemoryPool, PoolConfig,
};
use crate::{
    config::{Config, P2pStack},
    zakura::{
        block_sync::{
            BLOCK_SYNC_REACTOR_BUDGET, BLOCK_SYNC_SERVE_NODE_EXECUTION,
            BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES, BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES,
            DEFAULT_BS_BBR_MIN_CWND_BYTES, LARGEST_SERVED_BLOCK_RESPONSE_BYTES,
        },
        discovery::{discovery_serving_output_bytes, DISCOVERY_BUDGET},
        handler::{
            remote_bootstrap_peer_count, BASE_SEND_WINDOW_BYTES, DEFAULT_ZAKURA_SEND_WINDOW,
        },
        header_sync::v9::{
            self as header_sync_v9, HEADER_SYNC_SERVE_NODE_EXECUTION, SERVE_PEER_OUTPUT_BYTES,
        },
        legacy_gossip::{LEGACY_GOSSIP_BUDGET, LEGACY_REQUESTS_BUDGET},
        peer_memory::min_budgeted_connections,
        tx_relay::wire::{
            MAX_TX_BYTES, SESSION_HELD_OUTPUT_BYTES, TX_RELAY_DATA_BUDGET, TX_RELAY_STATE_BUDGET,
        },
        MAX_BS_FRAME_BYTES,
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

/// Complete, pure startup memory sizing plan (`02 §8`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    /// Effective look-ahead cap (default or lowered by `[memory.cap] look_ahead`).
    pub look_ahead_cap: u64,
    /// Effective per-peer serving cap (default or lowered by `[memory.cap] serving_per_peer`).
    pub serving_per_peer_cap: u64,
    /// Effective tx pending-bytes cap (mempool cost limit or lowered by `[memory.cap] tx`).
    pub tx_cap: u64,
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
        let PlanTerms {
            connection_cap,
            min_connections,
            fixed,
            bases,
            reserve_control,
            reserve_propagation,
            reserve_consensus,
            floor_block_sync,
            look_ahead_cap,
            cap_block_sync,
            floor_serving,
            serving_per_peer_cap,
            cap_serving,
            floor_sessions,
            cap_sessions,
            floor_tx,
            tx_cap,
        } = terms;

        let mut derivation = memory.derivation;
        derivation.p2p_budget_b = budget_b;

        // Startup refuses when fixed + reserves + floors exceed `B` (`02 §8`).
        if terms.required_bytes() > budget_b {
            return Err(MemoryPlanError::BudgetTooSmall(Box::new(
                terms.budget_too_small(derivation, budget_b),
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

        let full_cost = bases.full_connection_bytes().max(1);
        let req_cost = bases.required_chain_connection_bytes().max(1);
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
            pool_config,
            min_connections,
            connection_cap,
            planned_full_connections,
            planned_required_connections,
            look_ahead_cap,
            serving_per_peer_cap,
            tx_cap,
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
    fixed: FixedTerms,
    bases: SessionBases,
    reserve_control: u64,
    reserve_propagation: u64,
    reserve_consensus: u64,
    floor_block_sync: u64,
    look_ahead_cap: u64,
    cap_block_sync: u64,
    floor_serving: u64,
    serving_per_peer_cap: u64,
    cap_serving: u64,
    floor_sessions: u64,
    cap_sessions: u64,
    floor_tx: u64,
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

        let connection_cap = config.zakura.max_connections.max(1);
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
            .map_or(default_look_ahead, |cap| {
                cap.bytes().min(default_look_ahead)
            })
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
            .map_or(default_serving_per_peer, |cap| {
                cap.bytes().min(default_serving_per_peer)
            })
            .max(floor_serving);
        let cap_serving = (connection_cap as u64)
            .saturating_mul(serving_per_peer_cap)
            .max(floor_serving);

        // The floor funds `min_connections` connections that each run every
        // reactor, because admission funds every reactor a peer supports
        // together (`02 §8` rules 3 and 4). Reactor caps and `N_cap` are
        // ceilings, not reservations, so they never raise the floor.
        let floor_sessions = (min_connections as u64).saturating_mul(bases.full_connection_bytes());
        let cap_sessions = (connection_cap as u64)
            .saturating_mul(bases.full_connection_bytes())
            .max(floor_sessions);

        let floor_tx = u64::from(MAX_TX_BYTES);
        let default_tx_cap = memory.mempool_tx_cost_limit.max(floor_tx);
        let tx_cap = memory
            .caps
            .tx
            .map_or(default_tx_cap, |cap| cap.bytes().min(default_tx_cap))
            .max(floor_tx);

        Self {
            connection_cap,
            min_connections,
            fixed,
            bases,
            // Reserved consumers (`Priority::Reserved`, `floor == cap`, `02 §4`).
            reserve_control: discovery_serving_output_bytes(connection_cap),
            reserve_propagation: PROPAGATION_RESERVE_BYTES,
            reserve_consensus: draft_worst_checkpoint_window_bytes(network)
                .max(BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES),
            floor_block_sync,
            look_ahead_cap,
            cap_block_sync,
            floor_serving,
            serving_per_peer_cap,
            cap_serving,
            floor_sessions,
            cap_sessions,
            floor_tx,
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
            assert!(plan.shared_capacity() >= prev_s);
            assert!(plan.planned_full_connections >= prev_n);
            prev_s = plan.shared_capacity();
            prev_n = plan.planned_full_connections;
        }
    }
}
