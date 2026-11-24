//! Block sync's resource bounds (properties.md, Block sync and Resource
//! allowances).
//!
//! [`OwnerModel`] is one owner model of block sync's budget, requests,
//! handoffs, holders and releases. It folds [`OwnerInput`]s, the seam its
//! evidence comes through, and never reads a production acceptance result.
//! Most inputs are effects the block-sync observer reports and the tap logs
//! ([`OwnerInput::from_fact`]). The verified tip comes from the block
//! service's published views ([`OwnerModel::observe_storage`]), and the
//! links from the harness's transport ([`OwnerInput::Links`]). The model
//! takes the bounds' limits from the node's configuration, the run's
//! workers and the fixture, and checks the budget's own reported maximum
//! against them.
//!
//! Three properties read it:
//! - [`PAYLOAD_INVENTORY_AGREES`]: every captured runtime payload has a
//!   policy entry.
//! - [`RETAINED_BYTE_BOUND`]: after every step, the wire and decoded bytes
//!   the sequencer retains stay within the pool's live `BlockSync` and
//!   `Consensus` leases (`mem-2`).
//! - [`BUDGET_LEDGERS_AGREE`]: at rest, no handoff is open, the byte budget
//!   equals the work queue's tally and the routines' unreceived estimates,
//!   and the pool's `BlockSync` and `Consensus` leases equal those estimates
//!   plus the retained bytes plus the receive credit that still drains.
//!
//! The pool gauge ([`super::memory_pool`]) checks the pool's own bounds,
//! teardown and `memory/consensus-once-per-height`.
//!
//! `B` is [`ESTIMATE_CEILING`], and `k` is
//! `advertised_max_blocks_per_response()`, the most bodies one request asks
//! for. The harnesses run at `k = 1`.
//!
//! Each check records its quantities' peaks in the run's report, since
//! properties.md asks the report for each allowance's largest value. The
//! retained bound is an instance of the shared resource bound
//! ([`super::shared::bounds`]).

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::{block::MAX_BLOCK_BYTES, parameters::checkpoint::constants};
use zakura_network::zakura::{FreeSlots, ZakuraBlockSyncConfig};

use crate::{
    observe::{
        BlockSyncFact, BlockSyncHolder, BlockSyncPayload, BlockSyncRetained, StorageFact, WorkOwner,
    },
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Unit,
        Verdict,
    },
    Observation, ObservationLog,
};

use super::shared::bounds::{bound, Bound, Measured, Reading};

/// Runtime payload identities and decoded sizes match policy at each captured sequencer boundary.
///
/// Sequencer IO enumerates its payload table after applying transition effects.
/// Policy entries supply the expected identities and cached sizes. The tap records
/// both sources with stable runtime IDs. This check covers captured boundaries;
/// driver and backend retention require separate evidence.
pub const PAYLOAD_INVENTORY_AGREES: &str = "block-sync/payload-inventory-agrees";

/// The wire and decoded bytes the sequencer retains stay within the pool's
/// live `BlockSync` and `Consensus` leases (`03 §4`, `mem-2`).
pub const RETAINED_BYTE_BOUND: &str = "block-sync/retained-byte-bound";

/// At rest, the byte budget, the work queue, the routines' unreceived
/// estimates and the pool's `BlockSync` and `Consensus` leases agree
/// (`03 §4`).
pub const BUDGET_LEDGERS_AGREE: &str = "block-sync/budget-ledgers-agree";

/// Heights above the verified tip that skip block sync's look-ahead gates:
/// one worst-case checkpoint range, `MAX_CHECKPOINT_HEIGHT_GAP + 1`
/// (`admission.rs`'s `COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS`).
pub const COMMIT_WINDOW_BLOCKS: u64 = constants::MAX_CHECKPOINT_HEIGHT_GAP as u64 + 1;

/// The largest estimate one request height reserves: `B`, the most a body
/// can be. Block sync clamps every estimate to `[1,024, B]`, and a missing
/// size hint reserves `B` (properties.md, Units).
pub const ESTIMATE_CEILING: u64 = MAX_BLOCK_BYTES;

/// The smallest estimate one request height reserves: the work queue
/// raises every hint to its size floor, `DEFAULT_BS_SIZE_FLOOR_BYTES`
/// (`work_queue.rs`), which the crate doesn't export.
pub const ESTIMATE_FLOOR: u64 = 1_024;

/// The limits the model checks inputs against, from the node's
/// configuration and the fixture.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    /// `M`, block sync's configured in-flight bytes:
    /// `max_inflight_block_bytes`. The byte budget records charges without
    /// refusing, so `M` only paces the harness's handoff pauses.
    pub budget: u64,
    /// The most bodies one request asks for:
    /// `advertised_max_blocks_per_response()`.
    pub per_request: u64,
    /// The largest body the run can retain, in wire bytes. The routine keeps
    /// only a body that hashes to a header it requested, and every header
    /// the node holds comes from the fixture, so the fixture's largest body
    /// bounds every retained one.
    pub largest_body: u64,
}

impl Limits {
    /// The limits of a node that runs `config` on a fixture whose largest
    /// body is `largest_body` wire bytes.
    pub fn of(config: &ZakuraBlockSyncConfig, largest_body: u64) -> Limits {
        Limits {
            budget: config.inflight_block_bytes(),
            per_request: u64::from(config.advertised_max_blocks_per_response()),
            largest_body,
        }
    }

    /// `M + 2·k·B`: the byte budget's count past which the harness ends its
    /// handoff pauses.
    pub fn pause_bound(&self) -> u64 {
        let request = self.per_request.saturating_mul(ESTIMATE_CEILING);
        self.budget.saturating_add(request.saturating_mul(2))
    }
}

/// A holder of one retained body, by the tap's allocation number or the
/// submission's token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BodyHolder {
    /// The sequencer's input queue.
    Queued(u64),
    /// The reorder buffer.
    Reorder(u64),
    /// `applying`.
    Applying(u64),
    /// The verifier driver, through a submission attached to `applying`. It
    /// shares `applying`'s allocation.
    Submitted(u64),
    /// The verifier driver, through a submission detached from `applying`.
    Detached(u64),
}

impl BodyHolder {
    /// The allocation this holder counts toward the sequencer's retained
    /// bytes, `reorder + applying + sequencer_input_queued` (properties.md,
    /// Retained bytes). Submissions count toward neither: an attached one
    /// shares `applying`'s allocation, and a detached one is
    /// `detached-submission-bounded`'s.
    fn retained_allocation(self) -> Option<u64> {
        match self {
            BodyHolder::Queued(allocation)
            | BodyHolder::Reorder(allocation)
            | BodyHolder::Applying(allocation) => Some(allocation),
            BodyHolder::Submitted(_) | BodyHolder::Detached(_) => None,
        }
    }
}

/// A request, by its session and its request ID within that session.
pub type RequestKey = (u64, u64);

/// The key of the request `owner` names.
fn request_key(owner: &WorkOwner) -> RequestKey {
    (owner.session_id, owner.request_id)
}

/// One height of a routine's outstanding request, as its snapshot lists it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestedHeight {
    /// The request.
    pub request: RequestKey,
    /// The height.
    pub height: u32,
    /// The size estimate the request reserved for it.
    pub estimate: u64,
    /// Whether its body arrived.
    pub received: bool,
}

/// One input the owner model folds.
///
/// Most inputs come from an effect the block-sync observer reports
/// ([`OwnerInput::from_fact`]). They are the node's own state, so no frame
/// history can stand in for them. [`OwnerInput::VerifiedTip`] comes from
/// the block service's published views, [`OwnerInput::Links`] from the
/// harness's transport, and [`OwnerInput::SessionSlots`] from the service's
/// session slots probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerInput {
    /// The service admitted a session, whose routine is the one for
    /// connection `connection`, generation `generation`.
    SessionAdmitted {
        /// The session's connection.
        connection: u64,
        /// Its routine's generation.
        generation: u64,
    },
    /// The routine for connection `connection`, generation `generation`,
    /// reported a snapshot.
    Routine {
        /// The routine's connection.
        connection: u64,
        /// The routine's generation.
        generation: u64,
        /// True for its last snapshot, after it returned its work.
        ended: bool,
        /// Each height of its outstanding requests.
        requests: Vec<RequestedHeight>,
    },
    /// A request's write moved one step. A one-height request's estimate is
    /// its height's.
    Request {
        /// The request.
        request: RequestKey,
        /// Its heights, in order.
        heights: Vec<u32>,
        /// The estimate it reserved for all of them.
        estimate: u64,
    },
    /// The byte budget's counter went from `before` to `after`.
    BudgetChanged {
        /// The counter before.
        before: u64,
        /// The counter after.
        after: u64,
        /// The budget's maximum, as the budget reports it.
        limit: u64,
    },
    /// A routine is handing the body at `height` to the sequencer while its
    /// request still charges `charge` to the budget.
    HandoffStarted {
        /// The request the body answered.
        owner: WorkOwner,
        /// The body's height.
        height: u32,
        /// The charge.
        charge: u64,
    },
    /// The routine released that charge.
    HandoffEnded {
        /// The request the body answered.
        owner: WorkOwner,
        /// The body's height.
        height: u32,
        /// The charge.
        charge: u64,
    },
    /// `holder` took the body at `height`, `bytes` wire bytes, which
    /// answered `request`.
    Held {
        /// The holder.
        holder: BodyHolder,
        /// The request the body answered.
        request: RequestKey,
        /// The body's height.
        height: u32,
        /// Its wire bytes.
        bytes: u64,
        /// Decoded attributed bytes when the holder is `Queued` and carries a
        /// pre-decoded block (0 otherwise).
        decoded_bytes: u64,
    },
    /// `holder` let its body go.
    Released {
        /// The holder.
        holder: BodyHolder,
    },
    /// Live combined `Consumer::BlockSync + Consumer::Consensus` bytes leased
    /// in the node's memory pool, reported by [`PoolTap`].
    PoolBlockLeases {
        /// Live leased bytes across `BlockSync` and `Consensus`.
        leased: u64,
        /// Subset of `leased` currently parked in `ConnectionCredit` QUIC
        /// receive-credit drain queues (`02 §6`).
        draining: u64,
    },
    /// The work queue's items charge `reserved` bytes in all.
    WorkQueue {
        /// The sum of every item's reserved bytes.
        reserved: u64,
    },
    /// The block service published a view whose verified tip is at
    /// `height`.
    VerifiedTip {
        /// The tip's height.
        height: u32,
    },
    /// The harness has made `made` links to the node, and `open` of them
    /// are still open.
    Links {
        /// The links made.
        made: u64,
        /// The links not yet closed.
        open: u64,
    },
    /// A serving response took its execution lease, or released it once
    /// every holder dropped it. The range read that serves the response
    /// holds the lease and a clone of the session's sender, so a read in
    /// flight keeps that session's slot.
    ServingLease {
        /// The serving session's connection.
        connection: u64,
        /// The serving session's routine generation.
        generation: u64,
        /// The range's first height.
        start: u32,
        /// The range's requested count.
        count: u32,
        /// True when it took the lease, false when it released it.
        acquired: bool,
    },
    /// The node's block-sync service had `free` session slots. The first
    /// reading, taken before the harness makes any link, is the baseline.
    SessionSlots {
        /// The free slots of each kind.
        free: FreeSlots,
    },
    /// The sequencer's snapshot lists `holders`: each holder other than its
    /// input queue, with its body's height and wire bytes.
    Snapshot {
        /// All runtime payloads, independently read from storage.
        payloads: Vec<BlockSyncPayload>,
        /// Payload identities and sizes referenced by policy entries.
        expected_payloads: Vec<BlockSyncPayload>,
        /// The listed holders, in the snapshot's order.
        holders: Vec<(BodyHolder, u32, u64)>,
    },
}

impl OwnerInput {
    /// The input a logged block-sync event gives, if any.
    pub fn from_fact(fact: &BlockSyncFact) -> Option<OwnerInput> {
        Some(match fact {
            BlockSyncFact::SessionAdmitted {
                connection,
                generation,
                ..
            } => OwnerInput::SessionAdmitted {
                connection: *connection,
                generation: *generation,
            },
            BlockSyncFact::Routine {
                connection,
                generation,
                ended,
                requests,
                ..
            } => OwnerInput::Routine {
                connection: *connection,
                generation: *generation,
                ended: *ended,
                requests: requests
                    .iter()
                    .flat_map(|request| {
                        let key = request_key(&request.owner);
                        request.heights.iter().map(move |height| RequestedHeight {
                            request: key,
                            height: height.height,
                            estimate: height.estimated_bytes,
                            received: height.received,
                        })
                    })
                    .collect(),
            },
            BlockSyncFact::Request {
                owner,
                heights,
                estimated_bytes,
                ..
            } => OwnerInput::Request {
                request: request_key(owner),
                heights: heights.clone(),
                estimate: *estimated_bytes,
            },
            BlockSyncFact::Budget {
                before,
                after,
                limit,
                ..
            } => OwnerInput::BudgetChanged {
                before: *before,
                after: *after,
                limit: *limit,
            },
            BlockSyncFact::HandoffStarted {
                owner,
                height,
                charge,
                ..
            } => OwnerInput::HandoffStarted {
                owner: owner.clone(),
                height: *height,
                charge: *charge,
            },
            BlockSyncFact::HandoffEnded {
                owner,
                height,
                charge,
                ..
            } => OwnerInput::HandoffEnded {
                owner: owner.clone(),
                height: *height,
                charge: *charge,
            },
            BlockSyncFact::Holder {
                body,
                holder,
                change,
            } => {
                let (holder, decoded_bytes) = match holder {
                    BlockSyncHolder::Queued {
                        allocation,
                        decoded_attributed_memory_bytes,
                    } => (
                        BodyHolder::Queued(*allocation),
                        *decoded_attributed_memory_bytes,
                    ),
                    BlockSyncHolder::Reorder { allocation } => {
                        (BodyHolder::Reorder(*allocation), 0)
                    }
                    BlockSyncHolder::Applying { allocation } => {
                        (BodyHolder::Applying(*allocation), 0)
                    }
                    BlockSyncHolder::Submitted { token } => (BodyHolder::Submitted(*token), 0),
                    BlockSyncHolder::Detached { token } => (BodyHolder::Detached(*token), 0),
                };
                if change == "retained" {
                    OwnerInput::Held {
                        holder,
                        request: request_key(&body.owner),
                        height: body.height,
                        bytes: body.bytes,
                        decoded_bytes,
                    }
                } else {
                    OwnerInput::Released { holder }
                }
            }
            BlockSyncFact::ServingLease {
                connection,
                generation,
                start,
                count,
                step,
                ..
            } => OwnerInput::ServingLease {
                connection: *connection,
                generation: *generation,
                start: *start,
                count: *count,
                acquired: step == "acquired",
            },
            BlockSyncFact::WorkQueue { items } => OwnerInput::WorkQueue {
                reserved: items.iter().map(|item| item.reserved_bytes).sum(),
            },
            BlockSyncFact::Sequencer {
                payloads,
                reorder,
                applying,
                submissions,
            } => {
                let expected_payloads = reorder
                    .iter()
                    .chain(applying)
                    .map(|body| BlockSyncPayload {
                        id: body.payload_id,
                        decoded_attributed_memory_bytes: body.policy_decoded_bytes,
                    })
                    .collect();
                let retained = |holder: fn(u64) -> BodyHolder, body: &BlockSyncRetained| {
                    (holder(body.allocation), body.body.height, body.body.bytes)
                };
                let reorder = reorder
                    .iter()
                    .map(|body| retained(BodyHolder::Reorder, body));
                let applying = applying
                    .iter()
                    .map(|body| retained(BodyHolder::Applying, body));
                let submissions = submissions.iter().map(|submission| {
                    let holder = if submission.detached {
                        BodyHolder::Detached(submission.token)
                    } else {
                        BodyHolder::Submitted(submission.token)
                    };
                    (holder, submission.body.height, submission.body.bytes)
                });
                OwnerInput::Snapshot {
                    payloads: payloads.clone(),
                    expected_payloads,
                    holders: reorder.chain(applying).chain(submissions).collect(),
                }
            }
            _ => return None,
        })
    }
}

/// A body a routine is handing to the sequencer while its request still
/// charges the budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenHandoff {
    /// The request the body answered.
    pub owner: WorkOwner,
    /// The body's height.
    pub height: u32,
    /// The budget bytes it still charges.
    pub charge: u64,
}

/// One retained allocation: its holders and its body's wire bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Allocation {
    holders: u32,
    bytes: u64,
}

/// Block sync's owners and quantities, folded from [`OwnerInput`]s. See the
/// module doc.
#[derive(Clone, Debug, Default)]
pub struct OwnerModel {
    /// The bounds' limits.
    pub limits: Limits,
    /// The byte budget's counter.
    reserved: u64,
    /// The sessions the service admitted, by connection and routine
    /// generation.
    admitted: BTreeSet<(u64, u64)>,
    /// The routines that reported a snapshot and haven't ended, by
    /// connection and generation.
    live: BTreeSet<(u64, u64)>,
    /// The routines that reported their last snapshot.
    ended: BTreeSet<(u64, u64)>,
    /// Each routine's outstanding request heights, from its latest
    /// snapshot.
    requests: BTreeMap<(u64, u64), Vec<RequestedHeight>>,
    /// The estimate each request reserved for each of its heights, as a
    /// snapshot or a one-height request's write first reported it.
    estimates: BTreeMap<(RequestKey, u32), u64>,
    /// Each open handoff, oldest first.
    handoffs: Vec<OpenHandoff>,
    /// Each body holder, with its body's height and wire bytes.
    holders: BTreeMap<BodyHolder, (u32, u64)>,
    /// Each retained allocation.
    allocations: BTreeMap<u64, Allocation>,
    /// The wire bytes of every retained allocation.
    retained: u64,
    /// The wire bytes of the bodies in the sequencer's input queue.
    queued: u64,
    /// The work queue's reserved tally, from its latest snapshot.
    queue_reserved: u64,
    /// The highest verified tip a published view named, or the tip the run
    /// started at.
    verified_tip: u32,
    /// The links the harness made and how many are still open, once it
    /// reported them.
    links: Option<(u64, u64)>,
    /// Each open serving lease, by session connection, generation, first
    /// height and count, with how many are open.
    serving: BTreeMap<(u64, u64, u32, u32), u64>,
    /// The session slots free before any link, and at the latest reading.
    session_slots: Option<(FreeSlots, FreeSlots)>,
    /// The largest value of each quantity, for the run's report.
    peaks: Peaks,
    /// The latest live `BlockSync` and `Consensus` lease total `PoolTap`
    /// reported.
    pool_block_leases: u64,
    /// Subset of `pool_block_leases` held in `ConnectionCredit` QUIC
    /// receive-credit drain queues (`02 §6`).
    pool_draining_receive: u64,
    /// Decoded attributed bytes held by each `Queued` body allocation.
    queued_decoded: BTreeMap<u64, u64>,
    /// Decoded attributed bytes held by the sequencer's retained payloads in
    /// the latest snapshot.
    sequencer_decoded: u64,
    /// The first budget input the model couldn't apply.
    budget_gap: Option<String>,
    /// The first holder or handoff input the model couldn't apply.
    holder_gap: Option<String>,
    /// The first serving lease input the model couldn't apply.
    serving_gap: Option<String>,
    /// Whether a completed sequencer boundary supplied a runtime inventory.
    payloads_observed: bool,
    /// First mismatch between actual payload storage and policy membership.
    payload_gap: Option<String>,
}

/// The largest value each bound's quantity reached.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Peaks {
    /// The byte budget's largest count.
    pub reserved: u64,
    /// The most open handoffs at once.
    pub handoffs: u64,
    /// The most retained wire bytes.
    pub retained: u64,
    /// The most retained wire and decoded bytes.
    pub retained_with_decoded: u64,
    /// The most wire bytes in the sequencer's input queue.
    pub queued: u64,
    /// The most session slots held at once.
    pub session_slots: u64,
    /// The largest combined `BlockSync + Consensus` memory-pool lease total.
    pub pool_block_leases: u64,
}

impl OwnerModel {
    /// An empty model with `limits`, for a node whose verified tip starts at
    /// `verified_tip`.
    pub fn new(limits: Limits, verified_tip: u32) -> OwnerModel {
        OwnerModel {
            limits,
            verified_tip,
            ..OwnerModel::default()
        }
    }

    /// Folds the input a logged block-sync event gives, if any.
    pub fn observe(&mut self, fact: &BlockSyncFact) {
        if let Some(input) = OwnerInput::from_fact(fact) {
            self.apply(input);
        }
    }

    /// Folds the input a logged block storage fact gives, if any: a
    /// published view's verified tip.
    pub fn observe_storage(&mut self, fact: &StorageFact) {
        if let StorageFact::ViewPublished { verified_best, .. } = fact {
            self.apply(OwnerInput::VerifiedTip {
                height: verified_best.height,
            });
        }
    }

    /// Folds `input`.
    pub fn apply(&mut self, input: OwnerInput) {
        self.fold(input);
        let held = self.retained.saturating_add(self.retained_decoded());
        self.peaks.retained_with_decoded = self.peaks.retained_with_decoded.max(held);
    }

    /// Folds `input` without updating the retained peak.
    fn fold(&mut self, input: OwnerInput) {
        match input {
            OwnerInput::SessionAdmitted {
                connection,
                generation,
            } => {
                self.admitted.insert((connection, generation));
            }
            OwnerInput::Routine {
                connection,
                generation,
                ended,
                requests,
            } => {
                let routine = (connection, generation);
                if ended {
                    self.live.remove(&routine);
                    self.ended.insert(routine);
                } else {
                    self.live.insert(routine);
                }
                for height in &requests {
                    self.estimates
                        .entry((height.request, height.height))
                        .or_insert(height.estimate);
                }
                self.requests.insert(routine, requests);
            }
            OwnerInput::Request {
                request,
                heights,
                estimate,
            } => {
                if let [height] = heights[..] {
                    self.estimates.entry((request, height)).or_insert(estimate);
                }
            }
            OwnerInput::BudgetChanged {
                before,
                after,
                limit,
            } => {
                if before != self.reserved {
                    self.note_budget_gap(format!(
                        "the byte budget changed from {before}, but its last change left {}",
                        self.reserved
                    ));
                }
                // The pool is block sync's only admission bound, so the
                // byte budget records charges with no maximum.
                if limit != u64::MAX {
                    self.note_budget_gap(format!(
                        "the byte budget reports a maximum of {limit}, but it has none"
                    ));
                }
                self.reserved = after;
                self.peaks.reserved = self.peaks.reserved.max(after);
            }
            OwnerInput::HandoffStarted {
                owner,
                height,
                charge,
            } => {
                self.handoffs.push(OpenHandoff {
                    owner,
                    height,
                    charge,
                });
                let open = u64::try_from(self.handoffs.len()).unwrap_or(u64::MAX);
                self.peaks.handoffs = self.peaks.handoffs.max(open);
            }
            OwnerInput::HandoffEnded {
                owner,
                height,
                charge,
            } => {
                let ended = OpenHandoff {
                    owner,
                    height,
                    charge,
                };
                match self.handoffs.iter().position(|open| *open == ended) {
                    Some(at) => {
                        self.handoffs.remove(at);
                    }
                    None => self.note_holder_gap(format!(
                        "the handoff at height {height} ended without starting"
                    )),
                }
            }
            OwnerInput::Held {
                holder,
                request,
                height,
                bytes,
                decoded_bytes,
            } => self.hold(holder, request, height, bytes, decoded_bytes),
            OwnerInput::Released { holder } => self.release(holder),
            OwnerInput::PoolBlockLeases { leased, draining } => {
                self.pool_block_leases = leased;
                self.pool_draining_receive = draining;
                self.peaks.pool_block_leases = self.peaks.pool_block_leases.max(leased);
            }
            OwnerInput::WorkQueue { reserved } => self.queue_reserved = reserved,
            OwnerInput::VerifiedTip { height } => {
                self.verified_tip = self.verified_tip.max(height);
            }
            OwnerInput::Links { made, open } => self.links = Some((made, open)),
            OwnerInput::ServingLease {
                connection,
                generation,
                start,
                count,
                acquired,
            } => self.lease((connection, generation, start, count), acquired),
            OwnerInput::SessionSlots { free } => {
                let baseline = self.session_slots.map_or(free, |(baseline, _)| baseline);
                self.session_slots = Some((baseline, free));
                self.peaks.session_slots = self.peaks.session_slots.max(self.session_slots());
            }
            OwnerInput::Snapshot {
                holders,
                mut payloads,
                mut expected_payloads,
            } => {
                self.payloads_observed = true;
                self.sequencer_decoded = payloads
                    .iter()
                    .map(|p| p.decoded_attributed_memory_bytes)
                    .fold(0, u64::saturating_add);
                payloads.sort_unstable();
                expected_payloads.sort_unstable();
                if payloads != expected_payloads && self.payload_gap.is_none() {
                    let first = payloads
                        .iter()
                        .zip(&expected_payloads)
                        .position(|(actual, expected)| actual != expected)
                        .unwrap_or(payloads.len().min(expected_payloads.len()));
                    self.payload_gap = Some(format!(
                        "runtime payload inventory differs at {first}: actual {:?}, policy {:?}; counts {} and {}",
                        payloads.get(first), expected_payloads.get(first),
                        payloads.len(), expected_payloads.len(),
                    ));
                }
                self.compare_snapshot(holders);
            }
        }
    }

    /// Folds `holder` taking the body at `height`, `bytes` wire bytes, which
    /// answered `request`.
    fn hold(
        &mut self,
        holder: BodyHolder,
        request: RequestKey,
        height: u32,
        bytes: u64,
        decoded_bytes: u64,
    ) {
        if bytes > self.limits.largest_body {
            self.note_holder_gap(format!(
                "{holder:?} holds {bytes} bytes at height {height}, more than the fixture's \
                 largest body, {}",
                self.limits.largest_body
            ));
        }
        if self.holders.insert(holder, (height, bytes)).is_some() {
            self.note_holder_gap(format!("{holder:?} took a second body"));
        }
        if let Some(allocation) = holder.retained_allocation() {
            if !self.estimates.contains_key(&(request, height)) {
                self.note_holder_gap(format!(
                    "{holder:?} holds the body at height {height}, but no snapshot or write \
                     reported its request's estimate"
                ));
                return;
            }
            let entry = self
                .allocations
                .entry(allocation)
                .or_insert(Allocation { holders: 0, bytes });
            if entry.holders == 0 {
                self.retained = self.retained.saturating_add(bytes);
            }
            entry.holders += 1;
            self.peaks.retained = self.peaks.retained.max(self.retained);
        }
        if let BodyHolder::Queued(allocation) = holder {
            self.queued = self.queued.saturating_add(bytes);
            self.peaks.queued = self.peaks.queued.max(self.queued);
            if decoded_bytes > 0 {
                self.queued_decoded.insert(allocation, decoded_bytes);
            }
        }
    }

    /// Folds `holder` letting its body go.
    fn release(&mut self, holder: BodyHolder) {
        let Some((_, bytes)) = self.holders.remove(&holder) else {
            self.note_holder_gap(format!("{holder:?} released a body it never took"));
            return;
        };
        if let BodyHolder::Queued(allocation) = holder {
            self.queued = self.queued.saturating_sub(bytes);
            self.queued_decoded.remove(&allocation);
        }
        if let Some(allocation) = holder.retained_allocation() {
            if let Some(entry) = self.allocations.get_mut(&allocation) {
                entry.holders -= 1;
                if entry.holders == 0 {
                    self.retained = self.retained.saturating_sub(entry.bytes);
                    self.allocations.remove(&allocation);
                }
            }
        }
    }

    /// Records a holder gap unless the sequencer's snapshot lists exactly
    /// the holders the model folded, other than the input queue's. A holder
    /// event the observer dropped shows here: the model would otherwise
    /// count too few retained bytes and still hold.
    fn compare_snapshot(&mut self, mut listed: Vec<(BodyHolder, u32, u64)>) {
        let folded: Vec<_> = self
            .holders
            .iter()
            .filter(|(holder, _)| !matches!(holder, BodyHolder::Queued(_)))
            .map(|(holder, (height, bytes))| (*holder, *height, *bytes))
            .collect();
        listed.sort_unstable();
        if listed == folded {
            return;
        }
        let unfolded: Vec<_> = listed
            .iter()
            .filter(|held| !folded.contains(held))
            .collect();
        let unlisted: Vec<_> = folded
            .iter()
            .filter(|held| !listed.contains(held))
            .collect();
        self.note_holder_gap(format!(
            "the sequencer's snapshot lists {unfolded:?} that its holder events don't, and \
             omits {unlisted:?} that they hold"
        ));
    }

    /// Folds the serving lease `lease` taken, if `acquired`, or released.
    fn lease(&mut self, lease: (u64, u64, u32, u32), acquired: bool) {
        if acquired {
            *self.serving.entry(lease).or_default() += 1;
            return;
        }
        match self.serving.get_mut(&lease) {
            Some(open) if *open > 1 => *open -= 1,
            Some(_) => {
                self.serving.remove(&lease);
            }
            None => {
                let (connection, generation, start, count) = lease;
                self.serving_gap.get_or_insert(format!(
                    "serving on connection {connection}, generation {generation}, released \
                     a lease on {count} heights from {start} that it never took"
                ));
            }
        }
    }

    /// Records the first budget input the model couldn't apply.
    fn note_budget_gap(&mut self, gap: String) {
        self.budget_gap.get_or_insert(gap);
    }

    /// Records the first holder or handoff input the model couldn't apply.
    fn note_holder_gap(&mut self, gap: String) {
        self.holder_gap.get_or_insert(gap);
    }

    /// The byte budget's counter.
    pub fn reserved(&self) -> u64 {
        self.reserved
    }

    /// The open handoffs.
    pub fn handoffs(&self) -> &[OpenHandoff] {
        &self.handoffs
    }

    /// The routines that reported a snapshot and haven't ended. A routine
    /// reports one before each wait for input, and it hands a body over only
    /// after such a wait, so one in a handoff has.
    pub fn live_routines(&self) -> u64 {
        u64::try_from(self.live.len()).unwrap_or(u64::MAX)
    }

    /// The estimates of every routine's unreceived request heights, by each
    /// routine's latest snapshot. A routine reports one before it parks, so
    /// at rest they're current.
    pub fn unreceived_estimates(&self) -> u64 {
        self.requests
            .values()
            .flatten()
            .filter(|height| !height.received)
            .map(|height| height.estimate)
            .sum()
    }

    /// The wire bytes the sequencer retains: each body held by its input
    /// queue, reorder buffer or `applying`, once per allocation.
    pub fn retained(&self) -> u64 {
        self.retained
    }

    /// The decoded attributed bytes held across `Queued` bodies and the
    /// sequencer's retained payloads.
    pub fn retained_decoded(&self) -> u64 {
        self.queued_decoded
            .values()
            .copied()
            .fold(0, u64::saturating_add)
            .saturating_add(self.sequencer_decoded)
    }

    /// The latest live `BlockSync` and `Consensus` lease total `PoolTap`
    /// reported.
    pub fn pool_block_leases(&self) -> u64 {
        self.pool_block_leases
    }

    /// The wire bytes in the sequencer's input queue.
    pub fn queued(&self) -> u64 {
        self.queued
    }

    /// The work queue's reserved tally, from its latest snapshot.
    pub fn queue_reserved(&self) -> u64 {
        self.queue_reserved
    }

    /// The highest verified tip a published view named, or the tip the run
    /// started at. A higher tip puts fewer heights above the window, so
    /// reading the highest one can't add to `R + E`.
    pub fn verified_tip(&self) -> u32 {
        self.verified_tip
    }

    /// True once the harness has made links and closed every one: all peers
    /// have left.
    pub fn peers_left(&self) -> bool {
        self.links.is_some_and(|(made, open)| made > 0 && open == 0)
    }

    /// The admitted sessions whose routine hasn't reported its last
    /// snapshot, and the routines that reported a snapshot and haven't
    /// ended.
    pub fn unended(&self) -> usize {
        self.admitted
            .union(&self.live)
            .filter(|routine| !self.ended.contains(routine))
            .count()
    }

    /// The serving leases taken and not yet released: responses whose
    /// range read or encoding is still in flight.
    pub fn serving_leases(&self) -> u64 {
        self.serving.values().sum()
    }

    /// The session slots the node's service holds beyond its baseline, of
    /// every kind together, or 0 before any reading.
    pub fn session_slots(&self) -> u64 {
        let Some((baseline, free)) = self.session_slots else {
            return 0;
        };
        let held = |baseline: usize, free: usize| {
            u64::try_from(baseline.saturating_sub(free)).unwrap_or(u64::MAX)
        };
        held(baseline.inbound, free.inbound)
            .saturating_add(held(baseline.outbound, free.outbound))
            .saturating_add(held(baseline.setup, free.setup))
            .saturating_add(held(baseline.inbound_setup, free.inbound_setup))
    }

    /// The largest value each bound's quantity reached.
    pub fn peaks(&self) -> Peaks {
        self.peaks
    }

    /// The first budget input the model couldn't apply, if any. The budget's
    /// counter isn't known after it.
    pub fn budget_gap(&self) -> Option<&str> {
        self.budget_gap.as_deref()
    }

    /// The first holder or handoff input the model couldn't apply, if any.
    /// Holders and handoffs aren't known after it.
    pub fn holder_gap(&self) -> Option<&str> {
        self.holder_gap.as_deref()
    }

    /// The first serving lease input the model couldn't apply, if any. The
    /// open serving leases aren't known after it.
    pub fn serving_gap(&self) -> Option<&str> {
        self.serving_gap.as_deref()
    }

    /// The first input the model couldn't apply, budget inputs first.
    pub(crate) fn gap(&self) -> Option<String> {
        self.budget_gap()
            .or_else(|| self.holder_gap())
            .map(str::to_owned)
    }
}

/// A view that holds an [`OwnerModel`]. The resource properties read it.
pub trait OwnerView {
    /// Runs `read` on the owner model after the step.
    fn read_owners<T>(&self, read: impl FnOnce(&OwnerModel) -> T) -> T;
}

/// The three block-sync resource properties, all active (`03 §4`). The pool
/// gauge's [`crate::domain::memory_pool::POOL_BOUNDED`] bounds the leases,
/// and [`crate::domain::memory_pool::TEARDOWN_RETURNS_ZERO`] checks
/// teardown.
pub fn resource_properties<S: HasView>() -> Vec<Property<S>>
where
    S::View: OwnerView,
{
    vec![
        Property {
            id: PropertyId::new(PAYLOAD_INVENTORY_AGREES),
            claim: "runtime payload identities and measured decoded sizes match policy at each captured sequencer boundary",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_payload_inventory::<S>,
            witness: Some(TriggerHistory::Log(payload_retained)),
        },
        bound::<S, RetainedBound>(),
        Property {
            id: PropertyId::new(BUDGET_LEDGERS_AGREE),
            claim: "at rest, no handoff is open, the byte budget equals the work queue's \
                    reserved tally and the routines' unreceived estimates, and the pool's \
                    block-sync and consensus leases equal those estimates plus the retained \
                    bytes plus the receive credit that still drains",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: Status::Active,
            check: check_ledgers::<S>,
            witness: Some(TriggerHistory::Log(body_handed_off)),
        },
    ]
}

fn check_payload_inventory<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let (observed, gap) = cx
        .view()
        .read_owners(|owners| (owners.payloads_observed, owners.payload_gap.clone()));
    if let Some(gap) = gap {
        return cx.violated("runtime payload inventory matches policy", gap);
    }
    if observed {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn payload_retained(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(
        |fact| matches!(fact, BlockSyncFact::Sequencer { payloads, .. } if !payloads.is_empty()),
    )
}

/// [`RETAINED_BYTE_BOUND`] (`mem-2`), checked after every step.
///
/// Each retained body holds a lease of its wire bytes, grown to wire plus
/// decoded bytes when it decodes. A body above its request's estimate grows
/// its lease or is dropped, so the sequencer's wire and decoded bytes stay
/// within the live `BlockSync` and `Consensus` leases.
struct RetainedBound;

impl<S: HasView> Bound<S> for RetainedBound
where
    S::View: OwnerView,
{
    const ID: &'static str = RETAINED_BYTE_BOUND;
    const CLAIM: &'static str =
        "retained wire and decoded bytes stay within live block-sync and consensus leases (mem-2)";
    const UNIT: Unit = Unit::ResidentBytes;
    const NEEDS: &'static [Source] = &[Source::View];
    const WITNESS: fn(&ObservationLog) -> bool = commit_window_filled;
    type Terms = u64;
    type Peaks = [(&'static str, u64); 4];

    fn read(cx: &CheckCtx<'_, S>) -> Reading<u64, Self::Peaks> {
        let (wire, decoded, leases, peaks, gap) = cx.view().read_owners(|owners| {
            (
                owners.retained(),
                owners.retained_decoded(),
                owners.pool_block_leases(),
                owners.peaks(),
                owners.holder_gap().map(str::to_owned),
            )
        });
        if let Some(gap) = gap {
            return Reading::MissingEvidence(gap);
        }
        Reading::Measured(Measured {
            quantity: wire.saturating_add(decoded),
            limit: leases,
            peaks: [
                ("retained-wire-bytes", peaks.retained),
                (
                    "retained-wire-and-decoded-bytes",
                    peaks.retained_with_decoded,
                ),
                ("input-queue-wire-bytes", peaks.queued),
                ("pool-block-sync-leases", peaks.pool_block_leases),
            ],
            terms: decoded,
        })
    }

    fn violation(measured: &Measured<u64, Self::Peaks>) -> (String, String) {
        (
            format!(
                "retained wire and decoded bytes stay within the pool's block-sync and \
                 consensus leases, {} bytes",
                measured.limit
            ),
            format!(
                "it retains {} bytes, {} of them decoded",
                measured.quantity, measured.terms
            ),
        )
    }
}

/// Checks [`BUDGET_LEDGERS_AGREE`] after a step that left the run at rest.
///
/// Every path that releases a request's charge updates the work queue and
/// the budget with no `await` between them. A routine hands a body to the
/// sequencer through a send permit it reserved, so no routine parks
/// between a handoff's start and its release either. A routine reports a
/// snapshot before it parks, so at rest each routine's latest snapshot
/// lists what its requests still charge.
///
/// The pool's `BlockSync` and `Consensus` leases are then exact: each
/// outstanding request leases its estimate, each retained body its wire
/// bytes plus its decoded bytes, and each receive-credit drain its unspent
/// credit. A body above its estimate grows its lease or is dropped, so no
/// tolerance applies.
fn check_ledgers<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    if !crate::at_rest(cx.status) {
        return Verdict::NotApplicable;
    }
    let (reserved, queued, unreceived, handoffs, pool_leased, draining, retained, gap) =
        cx.view().read_owners(|owners| {
            (
                owners.reserved(),
                owners.queue_reserved(),
                owners.unreceived_estimates(),
                owners.handoffs().len(),
                owners.pool_block_leases(),
                owners.pool_draining_receive,
                owners.retained().saturating_add(owners.retained_decoded()),
                owners.gap(),
            )
        });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    if handoffs > 0 {
        return cx.violated(
            "no handoff is open at rest",
            format!("{handoffs} handoffs are open, and the budget counts {reserved}"),
        );
    }
    if reserved != queued {
        return cx.violated(
            format!("the byte budget equals the work queue's tally, {queued}"),
            format!("the budget counts {reserved}"),
        );
    }
    if reserved != unreceived {
        return cx.violated(
            format!("the byte budget equals the routines' unreceived estimates, {unreceived}"),
            format!("the budget counts {reserved}"),
        );
    }
    let expected = reserved.saturating_add(retained).saturating_add(draining);
    if pool_leased != expected {
        return cx.violated(
            format!(
                "the pool's block-sync and consensus leases equal in-flight estimates \
                 ({reserved}) + retained bytes ({retained}) + draining receive credit \
                 ({draining}) = {expected}"
            ),
            format!("the pool counts {pool_leased}"),
        );
    }
    Verdict::Holds
}

/// The block-sync events in `log`.
fn block_sync_facts(log: &ObservationLog) -> impl Iterator<Item = &BlockSyncFact> {
    log.entries().iter().filter_map(|entry| match &entry.obs {
        Observation::BlockSync { event, .. } => Some(event),
        _ => None,
    })
}

/// [`RETAINED_BYTE_BOUND`]'s trigger history: the commit window could be
/// full. A sequencer snapshot held at least `COMMIT_WINDOW_BLOCKS - 1`
/// bodies in its reorder buffer and `applying`, as many as the window holds
/// while the height above the verified tip is missing. The count includes
/// bodies above the window, so it doesn't show that every window height
/// holds a body.
fn commit_window_filled(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(|fact| match fact {
        BlockSyncFact::Sequencer {
            reorder, applying, ..
        } => {
            u64::try_from(reorder.len() + applying.len()).unwrap_or(u64::MAX)
                >= COMMIT_WINDOW_BLOCKS - 1
        }
        _ => false,
    })
}

/// [`BUDGET_LEDGERS_AGREE`]'s trigger history: a routine handed a charged
/// body to the sequencer, the release that moves the two ledgers apart.
fn body_handed_off(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(|fact| matches!(fact, BlockSyncFact::HandoffStarted { .. }))
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use zakura_scheduler::{SimInstant, Status as SchedulerStatus};

    use super::*;
    use crate::{
        check::{Phase, StepCtx},
        domain::memory_pool::{teardown_returns_zero, PoolGaugeModel, PoolView},
        observe::PoolFact,
        Identity,
    };

    /// A harness whose view pairs an owner model with a pool gauge model.
    struct Scripted(Rc<(OwnerModel, PoolGaugeModel)>);

    impl HasView for Scripted {
        type View = Rc<(OwnerModel, PoolGaugeModel)>;

        fn view(&self) -> Self::View {
            self.0.clone()
        }
    }

    impl OwnerView for Rc<(OwnerModel, PoolGaugeModel)> {
        fn read_owners<T>(&self, read: impl FnOnce(&OwnerModel) -> T) -> T {
            read(&self.0)
        }
    }

    impl PoolView for Rc<(OwnerModel, PoolGaugeModel)> {
        fn read_pool<T>(&self, read: impl FnOnce(&PoolGaugeModel) -> T) -> T {
            read(&self.1)
        }
    }

    /// [`TEARDOWN_RETURNS_ZERO`]'s verdict on `(owners, pool)` after a step
    /// that left the run quiescent, and any missing evidence it reported.
    fn teardown(owners: OwnerModel, pool: PoolGaugeModel) -> (Verdict, Option<String>) {
        let harness = Scripted(Rc::new((owners, pool)));
        let registered = vec![teardown_returns_zero::<Scripted>()];
        let property = &registered[0];
        let log = ObservationLog::default();
        let pick = Identity::advance_time(SimInstant::ZERO);
        let step = StepCtx {
            step: 7,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            budget: u64::MAX,
            now: SimInstant::ZERO,
            status: SchedulerStatus::Quiescent,
            quiescent: true,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &harness, (property, &registered), None);
        let verdict = (property.check)(&mut cx);
        (verdict, cx.into_parts().missing)
    }

    /// A model whose only link closed, with no routine and nothing charged,
    /// and whose service had `free` inbound slots before the link.
    fn settled(free: usize) -> OwnerModel {
        let mut owners = OwnerModel::new(Limits::default(), 0);
        owners.apply(OwnerInput::Links { made: 1, open: 0 });
        owners.apply(slots(free));
        owners
    }

    /// A reading of `inbound` free inbound slots, with every other kind at
    /// one.
    fn slots(inbound: usize) -> OwnerInput {
        OwnerInput::SessionSlots {
            free: FreeSlots {
                inbound,
                outbound: 1,
                setup: 1,
                inbound_setup: 1,
            },
        }
    }

    /// The `(expected, actual)` text of `verdict`'s violation.
    fn violation(verdict: &Verdict) -> (&str, &str) {
        match verdict {
            Verdict::Violated(violation) => {
                (violation.expected.as_str(), violation.actual.as_str())
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_settled_teardown_with_every_slot_returned_holds() {
        let mut owners = settled(2);
        owners.apply(slots(1));
        owners.apply(slots(2));
        assert_eq!(
            teardown(owners, PoolGaugeModel::default()),
            (Verdict::Holds, None)
        );
    }

    /// The scripted bypass: one session slot held once teardown settles.
    #[test]
    fn a_held_session_slot_violates_once_teardown_settles() {
        let mut owners = settled(2);
        owners.apply(slots(1));
        let (verdict, _) = teardown(owners, PoolGaugeModel::default());
        assert_eq!(
            violation(&verdict),
            (
                "held session slots is zero once teardown settles",
                "it counts 1"
            )
        );
    }

    /// The pool-gauge bypass: one pool lease still live once teardown settles.
    #[test]
    fn a_leaked_pool_lease_violates_once_teardown_settles() {
        let owners = settled(2);
        let mut pool = PoolGaugeModel::default();
        pool.observe(&PoolFact::Granted {
            lease_id: 1,
            consumer: zakura_network::zakura::memory::Consumer::BlockSync,
            peer: Some(1),
            bytes: 2_000,
            consumer_leased: 2_000,
            consumer_cap: 10_000,
            consumer_share: 5_000,
            total_leased: 2_000,
            pool_limit: 20_000,
        });
        let (verdict, _) = teardown(owners, pool);
        let (expected, actual) = violation(&verdict);
        assert!(expected.contains("returns to zero"), "{expected}");
        assert!(actual.contains("2000 B"), "{actual}");
    }

    /// A serving lease on height 1 of connection 3, taken if `acquired`.
    fn lease(acquired: bool) -> OwnerInput {
        OwnerInput::ServingLease {
            connection: 3,
            generation: 1,
            start: 1,
            count: 1,
            acquired,
        }
    }

    /// A serving read in flight holds its session's slot, so teardown waits
    /// for its lease, and judges the slots once the read returns them.
    #[test]
    fn teardown_waits_for_an_open_serving_lease() {
        let mut owners = settled(2);
        owners.apply(lease(true));
        owners.apply(slots(1));
        assert_eq!(
            teardown(owners.clone(), PoolGaugeModel::default()),
            (Verdict::NotApplicable, None)
        );
        owners.apply(lease(false));
        owners.apply(slots(2));
        assert_eq!(
            teardown(owners, PoolGaugeModel::default()),
            (Verdict::Holds, None)
        );
    }

    #[test]
    fn a_lease_released_without_its_acquisition_is_missing_evidence() {
        let mut owners = settled(1);
        owners.apply(lease(false));
        let (verdict, missing) = teardown(owners, PoolGaugeModel::default());
        assert_eq!(verdict, Verdict::NotApplicable);
        assert!(missing.is_some_and(|gap| gap.contains("never took")));
    }
}
