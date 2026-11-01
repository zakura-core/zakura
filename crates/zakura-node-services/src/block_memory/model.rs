//! Decoder-v1 histories for the decoded-block owner, without transport or workers.
//!
//! Each four-byte operation selects an action, source slot, destination slot,
//! and pool. Eight slots retain real block, header, and transaction handles.
//! Three pools have distinct identities; two enforce equal byte limits and one
//! enforces a count limit. The model derives charges from its own handle graph.
//! Weak allocation probes independently read the real payloads and charge owner.
//! Checks cover completed operations and an explicit drain before pool destruction.

use std::{collections::BTreeMap, panic::AssertUnwindSafe, sync::Weak};

use serde::{Deserialize, Serialize};

use super::*;

/// Weak reads of the actual standalone payload allocation.
pub struct PayloadProbe<T>(Weak<RetainedPayload<T>>);

impl<T> PayloadProbe<T> {
    /// Observe a returned allocation without extending its lifetime.
    pub fn new(payload: &OwnedPayload<T>) -> Self {
        Self(Arc::downgrade(&payload.0))
    }

    /// Count actual retained handles at a stable boundary.
    pub fn holders(&self) -> usize {
        self.0.strong_count()
    }
}

impl<T: ZcashSerialize> PayloadProbe<T> {
    /// Measure the live payload's serialization independently of admission bookkeeping.
    /// This measures content, not the allocation's decoded memory size.
    pub fn serialized_bytes(&self) -> Option<usize> {
        self.0
            .upgrade()
            .map(|allocation| allocation.payload.zcash_serialized_size())
    }
}

const SLOTS: usize = 8;

/// One pool's current counters, read after a completed operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolView {
    /// Attributed decoded bytes.
    pub bytes: u64,
    /// Allocations, when the pool enforces a count limit.
    pub blocks: Option<u64>,
    /// Whether this generation has closed admission.
    pub closed: bool,
}

/// Independent reads of one admitted allocation, including escaped raw payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationView {
    /// The causal admission operation, independent of pointer addresses.
    pub id: usize,
    /// Strong handles to the allocation's settlement owner.
    pub holders: usize,
    /// Strong references to the actual header allocation.
    pub header_holders: usize,
    /// Strong references to each actual transaction allocation.
    pub transaction_holders: Vec<usize>,
    /// Current pool identity, read from the allocation's charge.
    pub pool: Option<usize>,
    /// The owner's cached quantity, when the owner remains live.
    pub cached_bytes: Option<u64>,
    /// A fresh payload traversal, independent of the cached quantity.
    pub measured_bytes: Option<u64>,
}

/// Pool counters and actual retained allocations at one completed boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionView {
    /// Pools in constructor order, including closed generations.
    pub pools: Vec<PoolView>,
    /// All admitted allocations, including those whose owner has ended.
    pub allocations: Vec<AllocationView>,
    /// The operation result; refusal never follows the implementation's answer.
    pub outcome: String,
}

/// Causal input and independent observations for one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionStep {
    /// One-based operation number.
    pub step: usize,
    /// Original bytes, including a short final operation.
    pub input: Vec<u8>,
    /// Whether this step drops every handle before observing the live pools.
    pub drain: bool,
    /// Actual counters and allocation reads, absent when capture failed.
    pub actual: Option<RetentionView>,
    /// State derived from the independent handle graph.
    pub expected: RetentionView,
    /// A driver panic or unavailable observation is not a property detection.
    pub capture_error: Option<String>,
}

/// Weak reads of one allocation and its payloads, without retaining its charge.
/// Callers must establish a stable boundary before comparing these reads with counters.
pub struct Probe {
    owner: Weak<RetainedBlock>,
    header: Weak<Header>,
    transactions: Vec<Weak<Transaction>>,
}

impl Probe {
    /// Observe an admitted allocation without adding a resource-holding reference.
    pub fn new(owner: &OwnedBlock) -> Self {
        Self {
            owner: Arc::downgrade(&owner.0),
            header: Arc::downgrade(&owner.0.block.header),
            transactions: owner
                .0
                .block
                .transactions
                .iter()
                .map(Arc::downgrade)
                .collect(),
        }
    }

    /// Read actual payload references and the current charge owner without waiting.
    /// A held or poisoned charge lock, or an unknown pool, makes the read unavailable.
    /// `id` and pool order supply the caller's stable trace identities.
    pub fn read(&self, id: usize, pools: &[DecodedBlockPool]) -> Result<AllocationView, String> {
        let holders = self.owner.strong_count();
        let owner = self.owner.upgrade();
        let pool = if let Some(owner) = &owner {
            let charge = owner
                .charge
                .try_lock()
                .map_err(|_| format!("allocation {id} charge is unavailable at this boundary"))?;
            Some(
                pools
                    .iter()
                    .position(|pool| Arc::ptr_eq(&pool.0, &charge.pool.0))
                    .ok_or_else(|| format!("allocation {id} names an unregistered pool"))?,
            )
        } else {
            None
        };
        Ok(AllocationView {
            id,
            holders,
            header_holders: self.header.strong_count(),
            transaction_holders: self.transactions.iter().map(Weak::strong_count).collect(),
            pool,
            cached_bytes: owner.as_ref().map(|owner| owner.bytes),
            measured_bytes: owner
                .as_ref()
                .map(|owner| owner.block.attributed_memory_size_bytes()),
        })
    }
}

#[derive(Clone)]
enum Handle {
    Block(BlockHandle),
    Header(HeaderHandle),
    Transaction(TransactionHandle),
}

impl Handle {
    fn owner(&self) -> Option<&OwnedBlock> {
        match self {
            Self::Block(BlockHandle::Owned(block)) | Self::Header(HeaderHandle::Block(block)) => {
                Some(block)
            }
            Self::Transaction(TransactionHandle::Owned(transaction)) => Some(&transaction.block),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct ModelHandle {
    allocation: usize,
    is_block: bool,
}

struct Model {
    slots: [Option<ModelHandle>; SLOTS],
    allocations: BTreeMap<usize, usize>,
    closed: [bool; 3],
    bytes: u64,
    transactions: usize,
}

impl Model {
    fn holders(&self, allocation: usize) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|handle| handle.allocation == allocation)
            .count()
    }

    fn used(&self, pool: usize) -> usize {
        self.allocations
            .iter()
            .filter(|(id, owner)| **owner == pool && self.holders(**id) > 0)
            .count()
    }

    fn admission(&self, pool: usize) -> Result<(), &'static str> {
        if self.closed[pool] {
            Err("decoded block pool closed")
        } else if self.used(pool) >= if pool == 2 { 2 } else { 3 } {
            Err("decoded block capacity exhausted")
        } else {
            Ok(())
        }
    }

    fn apply(&mut self, id: usize, op: [u8; 4]) -> String {
        let source = usize::from(op[1]) % SLOTS;
        let destination = usize::from(op[2]) % SLOTS;
        let pool = usize::from(op[3]) % 3;
        match op[0] % 7 {
            0 => {
                if let Err(reason) = self.admission(pool) {
                    return reason.into();
                }
                self.allocations.insert(id, pool);
                self.slots[destination] = Some(ModelHandle {
                    allocation: id,
                    is_block: true,
                });
            }
            1 => self.slots[destination] = self.slots[source],
            2 | 3 => {
                let Some(mut handle) = self.slots[source].filter(|handle| handle.is_block) else {
                    return "skipped".into();
                };
                handle.is_block = false;
                self.slots[destination] = Some(handle);
            }
            4 => self.slots[source] = None,
            5 => {
                let Some(handle) = self.slots[source] else {
                    return "skipped".into();
                };
                if self.allocations[&handle.allocation] != pool {
                    if let Err(reason) = self.admission(pool) {
                        return reason.into();
                    }
                    self.allocations.insert(handle.allocation, pool);
                }
            }
            6 => self.closed[pool] = true,
            _ => unreachable!(),
        }
        "applied".into()
    }

    fn view(&self, outcome: String) -> RetentionView {
        RetentionView {
            pools: (0..3)
                .map(|pool| {
                    let count = u64::try_from(self.used(pool)).expect("at most eight handles");
                    PoolView {
                        bytes: count * self.bytes,
                        blocks: (pool == 2).then_some(count),
                        closed: self.closed[pool],
                    }
                })
                .collect(),
            allocations: self
                .allocations
                .iter()
                .map(|(&id, &pool)| {
                    let holders = self.holders(id);
                    let live = holders > 0;
                    AllocationView {
                        id,
                        holders,
                        header_holders: usize::from(live),
                        transaction_holders: vec![usize::from(live); self.transactions],
                        pool: live.then_some(pool),
                        cached_bytes: live.then_some(self.bytes),
                        measured_bytes: live.then_some(self.bytes),
                    }
                })
                .collect(),
            outcome,
        }
    }
}

struct Driver {
    pools: [DecodedBlockPool; 3],
    slots: [Option<Handle>; SLOTS],
    probes: BTreeMap<usize, Probe>,
}

fn block() -> Block {
    Arc::try_unwrap(block::genesis::regtest_genesis_block()).expect("fresh genesis has one owner")
}

impl Driver {
    fn apply(&mut self, id: usize, op: [u8; 4]) -> String {
        let source = usize::from(op[1]) % SLOTS;
        let destination = usize::from(op[2]) % SLOTS;
        let pool = usize::from(op[3]) % 3;
        match op[0] % 7 {
            0 => match self.pools[pool].try_admit(block(), Admission::WithinLimit) {
                Ok(owner) => {
                    self.probes.insert(id, Probe::new(&owner));
                    self.slots[destination] = Some(Handle::Block(owner.into()));
                }
                Err(rejected) => return rejected.reason.to_string(),
            },
            1 => self.slots[destination] = self.slots[source].clone(),
            2 | 3 => {
                let Some(Handle::Block(block)) = &self.slots[source] else {
                    return "skipped".into();
                };
                let handle = if op[0] % 7 == 2 {
                    Handle::Header(block.retained_header())
                } else {
                    Handle::Transaction(block.transaction(0).expect("genesis has a transaction"))
                };
                self.slots[destination] = Some(handle);
            }
            4 => self.slots[source] = None,
            5 => {
                let Some(owner) = self.slots[source].as_ref().and_then(Handle::owner) else {
                    return "skipped".into();
                };
                if let Err(reason) = owner.transfer_to(&self.pools[pool], Admission::WithinLimit) {
                    return reason.to_string();
                }
            }
            6 => self.pools[pool].close(),
            _ => unreachable!(),
        }
        "applied".into()
    }

    fn view(&self, outcome: String) -> Result<RetentionView, String> {
        Ok(RetentionView {
            pools: self
                .pools
                .iter()
                .map(|pool| PoolView {
                    bytes: pool.reserved_bytes(),
                    blocks: pool.reserved_blocks(),
                    closed: pool.is_closed(),
                })
                .collect(),
            allocations: self
                .probes
                .iter()
                .map(|(&id, probe)| probe.read(id, &self.pools))
                .collect::<Result<_, _>>()?,
            outcome,
        })
    }
}

/// Run at most 512 causal bytes through production owners and an independent model.
///
/// Operations are admit, clone, retain header, retain transaction, drop, transfer,
/// and close (opcode modulo seven). Short operations are zero-padded for execution
/// and retained verbatim in evidence. Every successful history ends with a drain.
/// Driver panics and unavailable charge reads remain capture errors.
pub fn trace_retention_bytes(input: &[u8]) -> Vec<RetentionStep> {
    let fixture = block();
    let bytes = fixture.attributed_memory_size_bytes();
    let transactions = fixture.transactions.len();
    drop(fixture);
    let mut model = Model {
        slots: [None; SLOTS],
        allocations: BTreeMap::new(),
        closed: [false; 3],
        bytes,
        transactions,
    };
    let mut driver = Driver {
        pools: [
            DecodedBlockPool::new(3 * bytes),
            DecodedBlockPool::new(3 * bytes),
            DecodedBlockPool::for_block_count(NonZeroU64::new(2).unwrap()),
        ],
        slots: std::array::from_fn(|_| None),
        probes: BTreeMap::new(),
    };
    let mut steps = Vec::new();
    for (index, input) in input[..input.len().min(512)]
        .chunks(4)
        .map(Some)
        .chain([None])
        .enumerate()
    {
        let step = index + 1;
        let mut op = [0; 4];
        if let Some(input) = input {
            op[..input.len()].copy_from_slice(input);
        }
        let drain = input.is_none();
        let expected_outcome = if drain {
            model.slots.fill(None);
            "drained".into()
        } else {
            model.apply(step, op)
        };
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let outcome = if drain {
                driver.slots.fill(None);
                "drained".into()
            } else {
                driver.apply(step, op)
            };
            driver.view(outcome)
        }));
        let (actual, capture_error) = match observed {
            Ok(Ok(view)) => (Some(view), None),
            Ok(Err(error)) => (None, Some(error)),
            Err(error) => (
                None,
                Some(
                    error
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            error
                                .downcast_ref::<&str>()
                                .map(|message| (*message).into())
                        })
                        .unwrap_or_else(|| "non-string owner driver panic".into()),
                ),
            ),
        };
        let expected = model.view(expected_outcome);
        let failed = actual.as_ref() != Some(&expected);
        steps.push(RetentionStep {
            step,
            input: input.unwrap_or_default().to_vec(),
            drain,
            actual,
            expected,
            capture_error,
        });
        if failed {
            break;
        }
    }
    steps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_parts_follow_transfers_and_settle_before_pool_destruction() {
        let input = [
            0, 0, 0, 0, 2, 0, 1, 0, 3, 0, 2, 0, 4, 0, 0, 0, 5, 1, 0, 2, 4, 1, 0, 0, 6, 0, 0, 2,
        ];
        let steps = trace_retention_bytes(&input);
        assert_eq!(steps.len(), input.len() / 4 + 1);
        for step in &steps {
            assert_eq!(step.actual.as_ref(), Some(&step.expected), "{step:?}");
        }
        assert_eq!(steps[3].expected.allocations[0].holders, 2);
        assert_eq!(steps[4].expected.pools[2].blocks, Some(1));
        assert!(steps[6].expected.pools[2].closed);
        assert!(steps[6].expected.pools[2].bytes > 0);
        assert!(steps
            .last()
            .unwrap()
            .expected
            .pools
            .iter()
            .all(|pool| pool.bytes == 0));
    }

    #[test]
    fn weak_payload_probes_detect_an_ownerless_raw_transaction() {
        let pool = DecodedBlockPool::new(u64::MAX);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let probe = Probe::new(&owner);
        let raw = owner.0.block.transactions[0].clone();
        drop(owner);
        let view = probe.read(1, &[pool]).unwrap();
        assert_eq!(view.holders, 0);
        assert_eq!(view.transaction_holders, vec![1]);
        drop(raw);
    }

    #[test]
    fn locked_charge_is_unavailable_without_waiting() {
        let pool = DecodedBlockPool::new(u64::MAX);
        let owner = pool.try_admit(block(), Admission::WithinLimit).unwrap();
        let probe = Probe::new(&owner);
        let _guard = owner.0.charge.lock().unwrap();
        assert!(probe.read(1, &[pool]).unwrap_err().contains("unavailable"));
    }

    #[test]
    fn generated_handle_histories_agree_through_drain() {
        for seed in 0..64_u64 {
            let mut state = seed + 1;
            let mut input = vec![0; 512];
            for byte in &mut input {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                *byte = state.to_le_bytes()[7];
            }
            let steps = trace_retention_bytes(&input);
            assert_eq!(steps.len(), 129, "seed {seed}: {:?}", steps.last());
            for step in steps {
                assert_eq!(
                    step.actual.as_ref(),
                    Some(&step.expected),
                    "seed {seed}: {step:?}"
                );
            }
        }
    }
}
