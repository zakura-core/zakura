//! BPv1's memory under the node budget (`02 §3`, `§4`, `§8`).
//!
//! - The `Propagation` reserve covers what BPv1 holds for its concurrent
//!   blocks: the block table's parts and metadata, the proposer's encodings,
//!   and assembled blocks until their verdict. Proof-of-work bounds each of
//!   them, so the reserve is held in full and never waits on bulk.
//! - Each admitted session holds its base from `Consumer::Sessions`: its
//!   unsolicited receive credit, its output gates for `Want` serving and
//!   pushes, its held frames and its early records.
//! - [`Leased`] is the IO shell's side: it holds each machine beside the
//!   `Propagation` lease that covers it and resizes the lease to the size
//!   the machine reports. [`Bpv1Memory`] leases each encode and each
//!   assembled block.

use std::ops::Deref;

use super::{
    blocks::BlockTable,
    config::{ZakuraBpv1Config, BPV1_SESSION_HELD_OUTPUT_BYTES},
    peer::EARLY_CONTROL_BYTES,
    profile::{Profile, PART_PROOF_BYTES},
    proposer::{ProposerState, MAX_PREPARED_ENCODINGS},
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
    /// The proposer's prepared encodings plus as many encodes in flight,
    /// which [`ProposerState`] bounds.
    pub proposer: u64,
    /// Assembled blocks until their verdict: one per concurrent block, plus
    /// one per block a next block displaces (`bpv1-83`). A displaced block
    /// keeps its assembled block in the verifier until its verdict, and each
    /// block above the next height is displaced at most once
    /// ([`BlockTable::max_owed_verdicts`]).
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
            assembled: (BlockTable::max_owed_verdicts(&block_config) as u64)
                .saturating_mul(ASSEMBLED_BLOCK_BYTES),
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
/// send window holds this much, so one block never waits on window growth;
/// BPv1 checks that when it starts.
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

/// The pool that funds BPv1's `Propagation` leases.
pub(super) struct Bpv1Memory {
    pool: MemoryPool,
}

impl Bpv1Memory {
    /// Funds BPv1 from `pool`'s `Consumer::Propagation`.
    pub(super) fn new(pool: MemoryPool) -> Self {
        Self { pool }
    }

    /// `machine` with an empty `Propagation` lease, which its first change
    /// sizes.
    pub(super) fn leased<M>(&self, machine: M) -> Leased<M> {
        Leased {
            machine,
            lease: self.pool.empty_lease(Consumer::Propagation, None),
        }
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

/// A pure machine whose held bytes one `Propagation` lease covers.
pub(super) trait LeaseSized {
    /// The bytes the machine's lease covers now.
    fn lease_bytes(&self) -> u64;
}

impl LeaseSized for BlockTable {
    fn lease_bytes(&self) -> u64 {
        BlockTable::lease_bytes(self)
    }
}

impl LeaseSized for ProposerState {
    fn lease_bytes(&self) -> u64 {
        self.prepared_bytes()
    }
}

/// A machine and the `Propagation` lease that covers what it holds.
///
/// The shell keeps it inside the machine's lock, so the lock order is
/// structural: the machine's lock, then the pool's. Reads go through
/// `Deref`. Only [`Self::change`] changes the machine, and it resizes the
/// lease before it returns, so sizes apply in the order the machine reports
/// them.
///
/// The reserve covers every such lease, so a refused resize is a sizing bug:
/// it counts `zakura.p2p.bpv1.propagation.lease_refused`, and tests and the
/// simulator panic on it.
pub(super) struct Leased<M> {
    machine: M,
    lease: PoolLease,
}

impl<M: LeaseSized> Leased<M> {
    /// Runs `change` on the machine, then resizes the lease to what the
    /// machine now holds. A refused resize keeps the lease's size.
    pub(super) fn change<R>(&mut self, change: impl FnOnce(&mut M) -> R) -> R {
        let result = change(&mut self.machine);
        let bytes = self.machine.lease_bytes();
        if self.lease.bytes() != bytes && self.lease.try_grow_to(bytes).is_err() {
            metrics::counter!("zakura.p2p.bpv1.propagation.lease_refused").increment(1);
            #[cfg(any(test, zakura_sim))]
            panic!("Propagation refused a {bytes}-byte BPv1 lease that its reserve covers (mem-5)");
        }
        result
    }

    /// The bytes the machine holds beyond its lease, which only a refused
    /// resize leaves.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn unfunded_bytes(&self) -> u64 {
        self.machine
            .lease_bytes()
            .saturating_sub(self.lease.bytes())
    }
}

impl<M> Deref for Leased<M> {
    type Target = M;

    fn deref(&self) -> &M {
        &self.machine
    }
}
