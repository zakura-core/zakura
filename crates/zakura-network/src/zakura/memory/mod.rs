//! Node-wide logical-byte memory pool for Zakura v2 P2P.
//!
//! Implements the hierarchical floor / weight-derived share / cap pool rule
//! described in `02-design.md §5`:
//!
//! - **Reserved consumers** (`Control`, `Propagation`, `Consensus`) reserve
//!   their full proof-of-work- or layout-bounded caps outside the shared pool
//!   `S` and never wait behind bulk traffic.
//! - **Bulk consumers** (`BlockSync`, `Serving`, `Sessions`, `Tx`) each keep a
//!   reserved floor outside `S` and share `S` proportionally to their weights.
//!   Short-lived bulk consumers may borrow idle space from `S` while no
//!   consumer below its share is waiting. `Sessions` holds long-lived leases
//!   and never borrows above its share (rule 4).
//! - **Serving** applies the same pool rule one level down across serving
//!   peers (`PeerSubPool`), so a stalled reader holds at most its fair part of
//!   serving memory (`#1888`). One lock guards both levels.
//!
//! [`MemoryPoolMachine`] decides; [`MemoryPool`] locks, waits and applies the
//! machine's wakeups.

mod config;
mod credit;
pub mod draft;
mod peer_pool;
mod plan;
mod pool;
mod serving_pool;
#[cfg(test)]
mod tests;

use std::time::Duration;

pub use config::{
    ByteSize, ByteSizeParseError, MemoryBudget, MemoryCaps, MemoryConfig, MemoryWeights,
    ResolvedMemoryBudget,
};
pub(crate) use credit::{CloseCreditOnDrop, ConnectionCredit};
pub use draft::{derive_p2p_budget, resolved_p2p_budget_bytes, BudgetDerivation};
pub use plan::{
    derived_read_result_byte_limit, inbound_sessions_share, BudgetTooSmall, ConnectionSessionCost,
    FixedTerms, MemoryPlan, MemoryPlanError, OverrideWarning, ReactorBase, SessionBases,
    SessionDirections, DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES, DEFAULT_STATE_READ_PARSER_BYTES,
    DISCOVERY_TABLES_FIXED_BYTES, PROPAGATION_RESERVE_BYTES, TX_RELAY_MAPS_FIXED_BYTES,
};
pub use pool::{
    ConnectionSessionLeases, ConsumerSnapshot, LeaseDenied, MemoryPool, MemoryPoolMachine,
    OverrunReport, PoolLease, PoolSnapshot, PoolWaitToken, Refusal, Released, Wake, WakeClass,
};
#[cfg(any(test, zakura_sim))]
pub use pool::{PoolEvent, PoolObserver};
pub(crate) use serving_pool::ServingLease;
pub use serving_pool::ServingPool;

/// Minimum interval between runtime budget-overrun warnings (`02 §8`, mem-14).
pub const OVERRUN_WARN_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Scheduling priority of a memory pool consumer (`02 §4`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Priority {
    /// Reserved in full (`floor == cap`). Never waits on bulk consumers.
    Reserved,
    /// Shares the shared space `S` by floor, weight, and cap.
    Bulk,
}

/// One consumer accounted by the node-wide memory pool (`02 §4`).
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Consumer {
    /// Control shares and discovery serving base.
    Control = 0,
    /// Tip-block propagation (BPv1 / tip paths).
    Propagation = 1,
    /// Commit-window blocks above the verified tip.
    Consensus = 2,
    /// Block-sync request leases above the commit window.
    BlockSync = 3,
    /// Serving output and send-window leases.
    Serving = 4,
    /// Connection transport bases and per-reactor session bases (no borrowing).
    Sessions = 5,
    /// Transaction-relay request leases carried into mempool pending bytes.
    Tx = 6,
}

impl Consumer {
    /// Total number of consumers in the pool.
    pub const COUNT: usize = 7;

    /// Every consumer in canonical index order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Control,
        Self::Propagation,
        Self::Consensus,
        Self::BlockSync,
        Self::Serving,
        Self::Sessions,
        Self::Tx,
    ];

    /// All bulk consumers in canonical order.
    pub const BULK: [Self; 4] = [Self::BlockSync, Self::Serving, Self::Sessions, Self::Tx];

    /// All reserved consumers in canonical order.
    pub const RESERVED: [Self; 3] = [Self::Control, Self::Propagation, Self::Consensus];

    /// Fixed array index for this consumer.
    #[inline]
    pub const fn index(self) -> usize {
        self as usize
    }

    /// Priority class for this consumer (`02 §4`).
    #[inline]
    pub const fn priority(self) -> Priority {
        match self {
            Self::Control | Self::Propagation | Self::Consensus => Priority::Reserved,
            Self::BlockSync | Self::Serving | Self::Sessions | Self::Tx => Priority::Bulk,
        }
    }

    /// Whether this consumer may borrow idle bytes from `S` above its own share.
    ///
    /// Reserved consumers hold fixed reserves (`floor == cap`), and `Sessions`
    /// holds long-lived leases that cannot return quickly (rule 4, `02 §5`).
    #[inline]
    pub const fn can_borrow(self) -> bool {
        match self {
            Self::BlockSync | Self::Serving | Self::Tx => true,
            Self::Control | Self::Propagation | Self::Consensus | Self::Sessions => false,
        }
    }

    /// Whether this consumer's leases are long-lived, so a request its own
    /// leases block never waits for them to return (rule 4, `02 §5`).
    #[inline]
    pub const fn long_lived(self) -> bool {
        matches!(self, Self::Sessions)
    }

    /// Fixed metric and trace label for this consumer (`03 §5`).
    #[inline]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Propagation => "propagation",
            Self::Consensus => "consensus",
            Self::BlockSync => "block_sync",
            Self::Serving => "serving",
            Self::Sessions => "sessions",
            Self::Tx => "tx",
        }
    }
}

/// Static configuration for one consumer in a [`PoolConfig`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConsumerSpec {
    /// Reserved floor bytes held outside `S`.
    pub floor: u64,
    /// Maximum bytes this consumer may lease (`>= floor`; for reserved
    /// consumers, clamped to `floor`).
    pub cap: u64,
    /// Weight in the shared pool `S` (ignored for reserved consumers).
    pub weight: u64,
}

impl ConsumerSpec {
    /// Specification for a reserved consumer (`floor == cap`, zero weight).
    pub const fn reserved(bytes: u64) -> Self {
        Self {
            floor: bytes,
            cap: bytes,
            weight: 0,
        }
    }

    /// Specification for a bulk consumer with floor, cap, and weight.
    pub const fn bulk(floor: u64, cap: u64, weight: u64) -> Self {
        let effective_cap = if cap < floor { floor } else { cap };
        Self {
            floor,
            cap: effective_cap,
            weight,
        }
    }
}

/// Complete static configuration of a [`MemoryPoolMachine`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolConfig {
    /// Total P2P memory budget `B` in logical bytes.
    pub budget_b: u64,
    /// Fixed non-leased bytes (socket queues, handshakes, tables, etc.).
    pub fixed_bytes: u64,
    /// Per-consumer specifications, indexed by [`Consumer::index`].
    pub consumers: [ConsumerSpec; Consumer::COUNT],
}

impl PoolConfig {
    /// Build a pool config with zeroed consumers, ready for per-consumer setup.
    pub const fn new(budget_b: u64, fixed_bytes: u64) -> Self {
        Self {
            budget_b,
            fixed_bytes,
            consumers: [ConsumerSpec {
                floor: 0,
                cap: 0,
                weight: 0,
            }; Consumer::COUNT],
        }
    }

    /// Set the specification for `consumer` (builder-style).
    pub const fn with_consumer(mut self, consumer: Consumer, spec: ConsumerSpec) -> Self {
        self.consumers[consumer.index()] = match consumer.priority() {
            Priority::Reserved => ConsumerSpec::reserved(spec.floor),
            Priority::Bulk => ConsumerSpec::bulk(spec.floor, spec.cap, spec.weight),
        };
        self
    }

    /// Sum of all consumers' floors (reserves plus bulk floors).
    pub const fn total_floors(&self) -> u64 {
        let mut sum = 0u64;
        let mut idx = 0;
        while idx < Consumer::COUNT {
            sum = sum.saturating_add(self.consumers[idx].floor);
            idx += 1;
        }
        sum
    }

    /// Shared pool capacity `S = B - fixed - reserves - Σ bulk_floors`,
    /// saturating at 0 when operator overrides push static terms past `B`.
    pub const fn shared_capacity(&self) -> u64 {
        self.budget_b
            .saturating_sub(self.fixed_bytes.saturating_add(self.total_floors()))
    }
}
