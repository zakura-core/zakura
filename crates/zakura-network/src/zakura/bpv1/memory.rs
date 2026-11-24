//! BPv1's memory under the node budget (`02 §3`, `§4`, `§8`).
//!
//! - The `Propagation` reserve covers what BPv1 holds for its concurrent
//!   blocks: the block table's parts and metadata, the proposer's encodings,
//!   and assembled blocks until their verdict. Proof-of-work bounds each of
//!   them, so the reserve is held in full and never waits on bulk.
//! - Each admitted session holds its base from `Consumer::Sessions`: its
//!   unsolicited receive credit, its output gates for `Want` serving and
//!   pushes, its held frames and its early records.
//! - [`Bpv1Memory`] is the IO shell's side: it holds the `Propagation`
//!   leases and resizes them to the sizes the machines report.

use zakura_rt::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, MutexGuard, PoisonError,
};

use super::{
    blocks::BlockTable,
    config::{ZakuraBpv1Config, BPV1_SESSION_HELD_OUTPUT_BYTES},
    peer::EARLY_CONTROL_BYTES,
    profile::{Profile, PART_PROOF_BYTES},
    proposer::MAX_PREPARED_ENCODINGS,
    wire::{BPV1_CONTROL_BUDGET, BPV1_DATA_BUDGET},
};
use crate::zakura::{
    memory::{draft::ADMISSION_CHARGE_PER_MAX_BLOCK, Consumer, MemoryPool, PoolLease},
    FRAME_HEADER_BYTES,
};

/// Bytes a `Part` payload carries besides its padded part and proof: the
/// block hash, the index and the sibling count.
const PART_PREFIX_BYTES: u64 = 32 + 4 + 1;

/// Bytes one assembled block holds from assembly until its verdict:
/// block-sync admission's per-block charge for the largest block,
/// `wire + 4 × wire`.
pub const ASSEMBLED_BLOCK_BYTES: u64 = ADMISSION_CHARGE_PER_MAX_BLOCK;

/// The `Propagation` reserve's terms with BPv1 on.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PropagationReserve {
    /// What the block table retains: the part buffer budget, the metadata
    /// budget, and a largest `HeaderMeta` and local encoding per concurrent
    /// block ([`BlockTable::retained_bound`]).
    pub table: u64,
    /// The proposer's prepared encodings plus as many encodes in flight.
    pub proposer: u64,
    /// One assembled block per concurrent block, until its verdict.
    pub assembled: u64,
}

impl PropagationReserve {
    /// The reserve's terms for `config` under `profile`.
    pub fn new(config: &ZakuraBpv1Config, profile: Profile) -> Self {
        let block_config = config.block_config(profile, Default::default());
        let encodings = (2 * MAX_PREPARED_ENCODINGS) as u64;
        Self {
            table: BlockTable::retained_bound(&block_config),
            proposer: encodings.saturating_mul(profile.max_coding().encoded_bytes()),
            // Widening usize to u64 is lossless on supported targets.
            assembled: (config.max_concurrent_blocks as u64).saturating_mul(ASSEMBLED_BLOCK_BYTES),
        }
    }

    /// The whole reserve.
    pub const fn total(&self) -> u64 {
        self.table
            .saturating_add(self.proposer)
            .saturating_add(self.assembled)
    }
}

/// One BPv1 session's base, which `Consumer::Sessions` funds at admission.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SessionBase {
    /// Unsolicited receive credit on both streams. Pushes and pulled parts
    /// share the data stream's `Part` row, so the whole share stays
    /// unsolicited.
    pub receive: u64,
    /// The output gates that queue `Want` serving, pushes and
    /// announcements, the frames each stream's writer holds outside them,
    /// and the early records (`early_peer_bytes_cap` of data records plus the
    /// control records at their cap).
    pub session: u64,
}

impl SessionBase {
    /// The base of one session under `config`.
    pub fn new(config: &ZakuraBpv1Config) -> Self {
        Self {
            receive: BPV1_CONTROL_BUDGET
                .receive
                .unsolicited
                .saturating_add(BPV1_DATA_BUDGET.receive.unsolicited),
            session: BPV1_CONTROL_BUDGET
                .output
                .saturating_add(BPV1_DATA_BUDGET.output)
                .saturating_add(BPV1_SESSION_HELD_OUTPUT_BYTES)
                .saturating_add(config.early_node_bytes_cap(1))
                .saturating_add(EARLY_CONTROL_BYTES),
        }
    }
}

/// The most frame bytes one block's parts take on one connection, when one
/// peer pushes or serves every coded part of the largest block. The base
/// send window holds this much, so one block never waits on window growth.
pub fn block_push_bytes(profile: Profile) -> u64 {
    let coding = profile.max_coding();
    // Widening usize to u64 is lossless on supported targets.
    let frame = u64::from(coding.part_bytes)
        .saturating_add(PART_PREFIX_BYTES)
        .saturating_add(PART_PROOF_BYTES)
        .saturating_add(FRAME_HEADER_BYTES as u64);
    u64::from(coding.stripes)
        .saturating_mul(u64::from(coding.n))
        .saturating_mul(frame)
}

/// The `Propagation` leases BPv1's IO shell holds, by what they cover.
///
/// The machines report the bytes they hold, and the shell resizes the
/// matching lease. The reserve covers every lease, so a refusal of the table
/// or proposer lease is a sizing bug: it counts
/// `zakura.p2p.bpv1.propagation.lease_refused` and the simulator asserts it.
pub(super) struct Bpv1Memory {
    pool: MemoryPool,
    /// What `table` holds, read under the table lock to skip `table`'s lock
    /// while the size doesn't change.
    table_bytes: AtomicU64,
    /// Covers [`BlockTable::lease_bytes`].
    table: Mutex<PoolLease>,
    /// Covers [`super::proposer::ProposerState::prepared_bytes`].
    proposer: Mutex<PoolLease>,
}

impl Bpv1Memory {
    /// Leases that fund BPv1 from `pool`'s `Consumer::Propagation`.
    pub(super) fn new(pool: MemoryPool) -> Self {
        Self {
            table_bytes: AtomicU64::new(0),
            table: Mutex::new(pool.empty_lease(Consumer::Propagation, None)),
            proposer: Mutex::new(pool.empty_lease(Consumer::Propagation, None)),
            pool,
        }
    }

    /// Resizes the table lease to `bytes`. The caller holds the table lock,
    /// so sizes apply in the order the table reports them.
    pub(super) fn fund_table(&self, bytes: u64) {
        if self.table_bytes.load(Ordering::Acquire) == bytes {
            return;
        }
        let mut lease = lock(&self.table);
        resize(&mut lease, bytes);
        self.table_bytes.store(lease.bytes(), Ordering::Release);
    }

    /// Resizes the proposer lease to `bytes`. The caller holds the proposer
    /// lock, so sizes apply in the order the proposer reports them.
    pub(super) fn fund_proposer(&self, bytes: u64) {
        resize(&mut lock(&self.proposer), bytes);
    }

    /// A lease of `bytes` for one encode or one assembled block, or `None`
    /// when the reserve is full. A caller that gets `None` skips the work as
    /// a local limit: the template gets no commitment, or the block falls
    /// back to native block sync.
    pub(super) fn lease(&self, bytes: u64) -> Option<PoolLease> {
        let lease = self
            .pool
            .try_acquire(Consumer::Propagation, None, bytes)
            .ok();
        if lease.is_none() {
            metrics::counter!("zakura.p2p.bpv1.propagation.work_refused").increment(1);
        }
        lease
    }
}

/// Resizes `lease` to `bytes`, keeping its size when the reserve refuses.
fn resize(lease: &mut PoolLease, bytes: u64) {
    if lease.try_grow_to(bytes).is_err() {
        metrics::counter!("zakura.p2p.bpv1.propagation.lease_refused").increment(1);
        #[cfg(zakura_sim)]
        panic!("Propagation refused a {bytes}-byte BPv1 lease that its reserve covers (mem-5)");
    }
}

/// Locks `mutex`, taking the value even if a panicking holder poisoned it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
