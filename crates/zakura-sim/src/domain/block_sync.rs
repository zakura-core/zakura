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
//! Five properties read it:
//! - [`RETAINED_BYTE_BOUND`]: retained wire bytes stay within
//!   `L + 401·b + Q·b`, after every step;
//! - [`RESERVED_BYTE_BOUND`]: the byte budget stays within `M + 2·B + h·B`,
//!   after every step, where `h` counts the charged handoffs open then, at
//!   most one per live routine and none at rest (D2 in the lane 3 plan);
//! - [`BUDGET_LEDGERS_AGREE`]: at rest, no handoff is open, and the byte
//!   budget equals both the work queue's reserved tally and the routines'
//!   unreceived estimates;
//! - [`LOOKAHEAD_OVERSHOOT_BOUND`]: at rest, above-window retained bytes
//!   plus outstanding above-window estimates stay within
//!   `L + H + (W + 1)·B`;
//! - [`TEARDOWN_RETURNS_BASELINE`]: once every peer has left, every
//!   routine has ended and the sequencer's input queue is empty, at rest,
//!   every charge is back to zero.
//!
//! D2's `2·B` assumes each request asks for one body
//! (`BS_BLOCKS_PER_RESPONSE = 1`). [`Limits::reserved_bound`] and
//! [`Limits::lookahead_bound`] allow one request's estimate,
//! `advertised_max_blocks_per_response()·B`, per term, so the bounds stay
//! sound if that constant grows.
//!
//! Each check records its quantities' peaks in the run's report, since
//! properties.md asks the report for each allowance's largest value.
//!
//! # The look-ahead bound's coefficients
//!
//! A height is above the window when it exceeds the verified tip plus 401.
//! Admission plans each above-window request from one snapshot
//! (`admission.rs`, `admit`). It refuses once retained bytes plus the work
//! queue's estimates above the download floor reach `L`, and otherwise
//! sizes the take to the room left. Both totals cover their above-window
//! parts: every retained body counts, and every outstanding above-window
//! height is above the floor. So one routine's admission keeps `R + E`
//! within `L`, except that a take always keeps its first item, up to one
//! request's estimate: the floor's first-item margin, one `B`. Each routine
//! admits against its own snapshot, so up to `W` routines can each add one
//! request's estimate past the room that the others' snapshots missed: the
//! race term, `W·B`. A body's retained bytes replace its request's estimate
//! when it arrives, so a body larger than its estimate adds its excess,
//! which the hint-error term `H` counts. The model reads the highest
//! verified tip any view published. A higher tip moves heights into the
//! window, which lowers `R + E` by at least as much as `H`, so a lagging or
//! reset tip can't fail the check.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::{block::MAX_BLOCK_BYTES, parameters::checkpoint::constants};
use zakura_network::zakura::ZakuraBlockSyncConfig;
use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    observe::{BlockSyncFact, BlockSyncHolder, BlockSyncRetained, StorageFact, WorkOwner},
    property::{
        CheckCtx, HasView, Inconclusive, Kind, Property, PropertyId, Source, Status, Unit, Verdict,
    },
    Observation, ObservationLog,
};

/// Retained wire bytes stay within the derived bound.
pub const RETAINED_BYTE_BOUND: &str = "block-sync/retained-byte-bound";

/// Reserved request bytes stay within the derived bound.
pub const RESERVED_BYTE_BOUND: &str = "block-sync/reserved-byte-bound";

/// The two reserved-byte ledgers agree.
pub const BUDGET_LEDGERS_AGREE: &str = "block-sync/budget-ledgers-agree";

/// Above-window bytes exceed the look-ahead limit only by size-hint error
/// and the admission race.
pub const LOOKAHEAD_OVERSHOOT_BOUND: &str = "block-sync/lookahead-overshoot-bound";

/// Teardown returns every block-sync charge.
pub const TEARDOWN_RETURNS_BASELINE: &str = "block-sync/teardown-returns-baseline";

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

/// The limits the bounds add up, from the node's configuration and the
/// fixture.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    /// `M`, the byte budget's limit: `max_inflight_block_bytes`.
    pub budget: u64,
    /// `L`, the look-ahead limit the sequencer's final gate reads:
    /// `effective_max_reorder_lookahead_bytes()`.
    pub lookahead: u64,
    /// `Q`, the sequencer input queue's depth in bodies:
    /// `submitted_apply_limit()`.
    pub queue: u64,
    /// The most bodies one request asks for:
    /// `advertised_max_blocks_per_response()`.
    pub per_request: u64,
    /// The largest body the run can retain, in wire bytes. The routine keeps
    /// only a body that hashes to a header it requested, and every header
    /// the node holds comes from the fixture, so the fixture's largest body
    /// bounds every retained one.
    pub largest_body: u64,
    /// `W`, the runtime's workers, which run the peer routines.
    pub workers: u64,
}

impl Limits {
    /// The limits of a node that runs `config` on `workers` workers, on a
    /// fixture whose largest body is `largest_body` wire bytes.
    pub fn of(config: &ZakuraBlockSyncConfig, workers: u16, largest_body: u64) -> Limits {
        Limits {
            budget: config.max_inflight_block_bytes,
            lookahead: config.effective_max_reorder_lookahead_bytes(),
            queue: u64::try_from(config.submitted_apply_limit()).unwrap_or(u64::MAX),
            per_request: u64::from(config.advertised_max_blocks_per_response()),
            largest_body,
            workers: u64::from(workers),
        }
    }

    /// `M + 2·k·B + h·B`, the most the byte budget may count while
    /// `handoffs` charged handoffs are open, with `k` the most bodies one
    /// request asks for (properties.md, Reserved bytes).
    ///
    /// Only a floor request may charge past `M`, and only one that starts at
    /// the download floor or the next height. Each height has one owner, so
    /// at most two such charges are live, each at most one request's
    /// estimate, `k·B`. D2 writes `2·B` because `k` is 1
    /// (`BS_BLOCKS_PER_RESPONSE`). While a routine hands a body to the
    /// sequencer, both the sequencer and the budget count it, and the
    /// sequencer can advance the floor, so another routine can overdraft for
    /// the new floor's next height. Each open handoff adds one more `B`.
    pub fn reserved_bound(&self, handoffs: u64) -> u64 {
        let overdraft = self.per_request.saturating_mul(ESTIMATE_CEILING);
        self.budget
            .saturating_add(overdraft.saturating_mul(2))
            .saturating_add(handoffs.saturating_mul(ESTIMATE_CEILING))
    }

    /// `L + H + (W + 1)·k·B`, the most above-window retained bytes and
    /// outstanding above-window estimates may add up to when the retained
    /// bodies exceed their estimates by `hint_error` in all (properties.md,
    /// Over-admission). The module doc derives the coefficients.
    pub fn lookahead_bound(&self, hint_error: u64) -> u64 {
        let request = self.per_request.saturating_mul(ESTIMATE_CEILING);
        self.lookahead
            .saturating_add(hint_error)
            .saturating_add(self.workers.saturating_add(1).saturating_mul(request))
    }

    /// `L + 401·b + Q·b`, the most the sequencer may retain (properties.md,
    /// Retained bytes), with `b` the fixture's largest body.
    ///
    /// The sequencer's final gate admits a body above the commit window only
    /// while the reorder buffer and `applying` stay within `L`. At most 401
    /// commit-window heights skip the gate, one body each, and the input
    /// queue holds at most `Q` bodies.
    pub fn retained_bound(&self) -> u64 {
        let bodies = COMMIT_WINDOW_BLOCKS.saturating_add(self.queue);
        self.lookahead
            .saturating_add(bodies.saturating_mul(self.largest_body))
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
/// the block service's published views, and [`OwnerInput::Links`] from the
/// harness's transport.
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
    },
    /// `holder` let its body go.
    Released {
        /// The holder.
        holder: BodyHolder,
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
    /// The sequencer's snapshot lists `holders`: each holder other than its
    /// input queue, with its body's height and wire bytes.
    Snapshot {
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
                let holder = match holder {
                    BlockSyncHolder::Queued { allocation, .. } => BodyHolder::Queued(*allocation),
                    BlockSyncHolder::Reorder { allocation } => BodyHolder::Reorder(*allocation),
                    BlockSyncHolder::Applying { allocation } => BodyHolder::Applying(*allocation),
                    BlockSyncHolder::Submitted { token } => BodyHolder::Submitted(*token),
                    BlockSyncHolder::Detached { token } => BodyHolder::Detached(*token),
                };
                if change == "retained" {
                    OwnerInput::Held {
                        holder,
                        request: request_key(&body.owner),
                        height: body.height,
                        bytes: body.bytes,
                    }
                } else {
                    OwnerInput::Released { holder }
                }
            }
            BlockSyncFact::WorkQueue { items } => OwnerInput::WorkQueue {
                reserved: items.iter().map(|item| item.reserved_bytes).sum(),
            },
            BlockSyncFact::Sequencer {
                reorder,
                applying,
                submissions,
            } => {
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
                    holders: reorder.chain(applying).chain(submissions).collect(),
                }
            }
            _ => return None,
        })
    }
}

/// A body a routine is handing to the sequencer while its request still
/// charges the budget: one unit of D2's `h`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenHandoff {
    /// The request the body answered.
    pub owner: WorkOwner,
    /// The body's height.
    pub height: u32,
    /// The budget bytes it still charges.
    pub charge: u64,
}

/// One retained allocation: its holders, and its body's height, wire bytes,
/// and wire bytes past its request's estimate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Allocation {
    holders: u32,
    height: u32,
    bytes: u64,
    excess: u64,
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
    /// The largest value of each quantity, for the run's report.
    peaks: Peaks,
    /// The first budget input the model couldn't apply.
    budget_gap: Option<String>,
    /// The first holder or handoff input the model couldn't apply.
    holder_gap: Option<String>,
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
    /// The most wire bytes in the sequencer's input queue, the `Q·b`
    /// allowance's quantity.
    pub queued: u64,
}

/// The look-ahead bound's quantities at one boundary (properties.md,
/// Over-admission).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lookahead {
    /// `R`: wire bytes of retained above-window bodies, once per
    /// allocation.
    pub retained: u64,
    /// `E`: estimates of outstanding above-window request heights.
    pub estimated: u64,
    /// `H`: the wire bytes by which each retained above-window body exceeds
    /// its request's estimate, summed.
    pub hint_error: u64,
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
                if limit != self.limits.budget {
                    self.note_budget_gap(format!(
                        "the byte budget reports a maximum of {limit}, but the node's config \
                         sets {}",
                        self.limits.budget
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
            } => self.hold(holder, request, height, bytes),
            OwnerInput::Released { holder } => self.release(holder),
            OwnerInput::WorkQueue { reserved } => self.queue_reserved = reserved,
            OwnerInput::VerifiedTip { height } => {
                self.verified_tip = self.verified_tip.max(height);
            }
            OwnerInput::Links { made, open } => self.links = Some((made, open)),
            OwnerInput::Snapshot { holders } => self.compare_snapshot(holders),
        }
    }

    /// Folds `holder` taking the body at `height`, `bytes` wire bytes, which
    /// answered `request`.
    fn hold(&mut self, holder: BodyHolder, request: RequestKey, height: u32, bytes: u64) {
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
            let estimate = self.estimates.get(&(request, height)).copied();
            let Some(estimate) = estimate else {
                self.note_holder_gap(format!(
                    "{holder:?} holds the body at height {height}, but no snapshot or write \
                     reported its request's estimate"
                ));
                return;
            };
            let entry = self.allocations.entry(allocation).or_insert(Allocation {
                holders: 0,
                height,
                bytes,
                excess: bytes.saturating_sub(estimate),
            });
            if entry.holders == 0 {
                self.retained = self.retained.saturating_add(bytes);
            }
            entry.holders += 1;
            self.peaks.retained = self.peaks.retained.max(self.retained);
        }
        if let BodyHolder::Queued(_) = holder {
            self.queued = self.queued.saturating_add(bytes);
            self.peaks.queued = self.peaks.queued.max(self.queued);
        }
    }

    /// Folds `holder` letting its body go.
    fn release(&mut self, holder: BodyHolder) {
        let Some((_, bytes)) = self.holders.remove(&holder) else {
            self.note_holder_gap(format!("{holder:?} released a body it never took"));
            return;
        };
        if let BodyHolder::Queued(_) = holder {
            self.queued = self.queued.saturating_sub(bytes);
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

    /// D2's `h` after a step that left the scheduler at `status`: the open
    /// handoffs, at most one per live routine, since a routine hands one
    /// body at a time; none at rest, since a handoff never parks.
    pub fn counted_handoffs(&self, status: SchedulerStatus) -> u64 {
        if at_rest(status) {
            return 0;
        }
        u64::try_from(self.handoffs.len())
            .unwrap_or(u64::MAX)
            .min(self.live_routines())
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

    /// The look-ahead bound's quantities: heights above the verified tip
    /// plus 401 count.
    pub fn lookahead(&self) -> Lookahead {
        let window_high = u64::from(self.verified_tip).saturating_add(COMMIT_WINDOW_BLOCKS);
        let above = |height: u32| u64::from(height) > window_high;
        let mut quantities = Lookahead::default();
        for allocation in self.allocations.values() {
            if above(allocation.height) {
                quantities.retained = quantities.retained.saturating_add(allocation.bytes);
                quantities.hint_error = quantities.hint_error.saturating_add(allocation.excess);
            }
        }
        quantities.estimated = self
            .requests
            .values()
            .flatten()
            .filter(|height| !height.received && above(height.height))
            .map(|height| height.estimate)
            .sum();
        quantities
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

    /// The first input the model couldn't apply, budget inputs first.
    fn gap(&self) -> Option<String> {
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

/// The five resource properties, all active. A harness registers them on a
/// view that holds the node's [`OwnerModel`].
pub fn resource_properties<S: HasView>() -> Vec<Property<S>>
where
    S::View: OwnerView,
{
    vec![
        Property {
            id: PropertyId::new(RETAINED_BYTE_BOUND),
            claim: "retained wire bytes stay within L + 401·B + Q·B",
            kind: Kind::Bound {
                unit: Unit::WireBytes,
            },
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_retained::<S>,
            witness: Some(commit_window_filled),
        },
        Property {
            id: PropertyId::new(RESERVED_BYTE_BOUND),
            claim: "reserved request bytes stay within M + 2·B + h·B",
            kind: Kind::Bound {
                unit: Unit::EstimateBytes,
            },
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_reserved::<S>,
            witness: Some(budget_charged),
        },
        Property {
            id: PropertyId::new(BUDGET_LEDGERS_AGREE),
            claim: "at rest, no handoff is open, and the byte budget equals the work queue's \
                    reserved tally and the routines' unreceived estimates",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: Status::Active,
            check: check_ledgers::<S>,
            witness: Some(body_handed_off),
        },
        Property {
            id: PropertyId::new(LOOKAHEAD_OVERSHOOT_BOUND),
            claim: "at rest, above-window retained bytes plus outstanding above-window \
                    estimates stay within L + H + (W + 1)·B",
            kind: Kind::Bound {
                unit: Unit::WireBytes,
            },
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: Status::Active,
            check: check_lookahead::<S>,
            witness: Some(above_window_requested),
        },
        Property {
            id: PropertyId::new(TEARDOWN_RETURNS_BASELINE),
            claim: "once every peer has left, every routine has ended and the input \
                    queue is empty, at rest, no block-sync charge remains",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: Status::Active,
            check: check_teardown::<S>,
            witness: Some(routine_ended_owing),
        },
    ]
}

/// True if no task can run after a step that left the scheduler at
/// `status`: every task finished or waits on time or an external source.
/// A worker-starved run still has a ready task, so it isn't at rest.
fn at_rest(status: SchedulerStatus) -> bool {
    matches!(
        status,
        SchedulerStatus::Quiescent
            | SchedulerStatus::WaitingOnTime
            | SchedulerStatus::WaitingOnExternal
    )
}

/// Checks [`RETAINED_BYTE_BOUND`] after a step. A holder gap, such as a
/// sequencer snapshot that disagrees with the holder events, leaves the
/// retained bytes unknown, so the check reports missing evidence.
fn check_retained<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let (retained, bound, peaks, gap) = cx.view().read_owners(|owners| {
        (
            owners.retained(),
            owners.limits.retained_bound(),
            owners.peaks(),
            owners.holder_gap().map(str::to_owned),
        )
    });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    cx.peak("retained-wire-bytes", peaks.retained);
    cx.peak("input-queue-wire-bytes", peaks.queued);
    if retained <= bound {
        return Verdict::Holds;
    }
    cx.violated(
        format!("the sequencer retains at most L + 401·b + Q·b = {bound} wire bytes"),
        format!("it retains {retained}"),
    )
}

/// Checks [`RESERVED_BYTE_BOUND`] after a step, with `h` from
/// [`OwnerModel::counted_handoffs`]. A handoff that never ends, such as a
/// leaked charge, counts only while its routine could still be in it.
fn check_reserved<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let status = cx.status;
    let (reserved, handoffs, open, peaks, limits, gap) = cx.view().read_owners(|owners| {
        (
            owners.reserved(),
            owners.counted_handoffs(status),
            owners.handoffs().len(),
            owners.peaks(),
            owners.limits,
            owners.gap(),
        )
    });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    cx.peak("reserved-estimate-bytes", peaks.reserved);
    cx.peak(
        "past-budget-estimate-bytes",
        peaks.reserved.saturating_sub(limits.budget),
    );
    cx.peak("open-handoffs", peaks.handoffs);
    let bound = limits.reserved_bound(handoffs);
    if reserved <= bound {
        return Verdict::Holds;
    }
    cx.violated(
        format!(
            "the byte budget counts at most M + 2·k·B + h·B = {bound} bytes, with M = {}, \
             k = {} and h = {handoffs}",
            limits.budget, limits.per_request
        ),
        format!("it counts {reserved}, with {open} handoffs open"),
    )
}

/// Checks [`BUDGET_LEDGERS_AGREE`] after a step that left the run at rest.
///
/// Every path that releases a request's charge updates the work queue and
/// the budget with no `await` between them. A routine hands a body to the
/// sequencer through a send permit it reserved, so no routine parks
/// between a handoff's start and its release either. A routine reports a
/// snapshot before it parks, so at rest each routine's latest snapshot
/// lists what its requests still charge. So at rest no handoff is open,
/// and the budget equals the work queue's tally and the routines'
/// unreceived estimates.
fn check_ledgers<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    if !at_rest(cx.status) {
        return Verdict::NotApplicable;
    }
    let (reserved, queued, unreceived, handoffs, gap) = cx.view().read_owners(|owners| {
        (
            owners.reserved(),
            owners.queue_reserved(),
            owners.unreceived_estimates(),
            owners.handoffs().len(),
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
    Verdict::Holds
}

/// Checks [`LOOKAHEAD_OVERSHOOT_BOUND`] after a step that left the run at
/// rest, the boundary at which every routine's snapshot is current.
fn check_lookahead<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    if !at_rest(cx.status) {
        return Verdict::NotApplicable;
    }
    let (quantities, limits, gap) = cx
        .view()
        .read_owners(|owners| (owners.lookahead(), owners.limits, owners.gap()));
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    let Lookahead {
        retained,
        estimated,
        hint_error,
    } = quantities;
    let total = retained.saturating_add(estimated);
    cx.peak("above-window-wire-bytes", retained);
    cx.peak("above-window-estimate-bytes", estimated);
    cx.peak("hint-error-bytes", hint_error);
    cx.peak(
        "past-lookahead-bytes",
        total.saturating_sub(limits.lookahead),
    );
    let bound = limits.lookahead_bound(hint_error);
    if total <= bound {
        return Verdict::Holds;
    }
    cx.violated(
        format!(
            "above-window bytes R + E stay within L + H + (W + 1)·k·B = {bound}, with \
             L = {}, H = {hint_error}, W = {} and k = {}",
            limits.lookahead, limits.workers, limits.per_request
        ),
        format!("R = {retained} and E = {estimated}"),
    )
}

/// Checks [`TEARDOWN_RETURNS_BASELINE`] after a step.
///
/// The trigger is every link the harness made having closed. Settlement is
/// established apart from the charges: every admitted session's routine,
/// and every routine that reported a snapshot, has reported its last one,
/// the sequencer's input queue is empty, and the run is at rest. The input
/// queue is part of settlement because the sequencer can rest in a submit,
/// waiting for storage, with bodies still queued: those bodies are the
/// sequencer's work, which no peer's leaving ends. Then the budget, the
/// work queue's tally and the routines' unreceived estimates must all be
/// zero, and no handoff may be open. Before settlement the property is
/// unsettled, so a run that ends there, including one with a leaked
/// routine or queued body, is `Inconclusive { TeardownOpen }`, not a
/// violation.
fn check_teardown<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let status = cx.status;
    let (left, unended, queued, charges, gap) = cx.view().read_owners(|owners| {
        (
            owners.peers_left(),
            owners.unended(),
            owners.queued(),
            [
                ("the byte budget", owners.reserved()),
                ("the work queue's tally", owners.queue_reserved()),
                (
                    "the routines' unreceived estimates",
                    owners.unreceived_estimates(),
                ),
                (
                    "open handoffs",
                    u64::try_from(owners.handoffs().len()).unwrap_or(u64::MAX),
                ),
            ],
            owners.gap(),
        )
    });
    if !left {
        return Verdict::NotApplicable;
    }
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    if unended > 0 || queued > 0 || !at_rest(status) {
        cx.unsettled(Inconclusive::TeardownOpen);
        return Verdict::NotApplicable;
    }
    match charges.iter().find(|(_, value)| *value > 0) {
        None => Verdict::Holds,
        Some((owner, value)) => cx.violated(
            format!("{owner} is zero once teardown settles"),
            format!("it counts {value}"),
        ),
    }
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

/// [`RESERVED_BYTE_BOUND`]'s trigger history: a floor request charged the
/// budget past what it could reserve, the only change that can take the
/// budget past `M`.
fn budget_charged(log: &ObservationLog) -> bool {
    block_sync_facts(log)
        .any(|fact| matches!(fact, BlockSyncFact::Budget { change, .. } if change == "charged"))
}

/// [`BUDGET_LEDGERS_AGREE`]'s trigger history: a routine handed a charged
/// body to the sequencer, the release that moves the two ledgers apart.
fn body_handed_off(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(|fact| matches!(fact, BlockSyncFact::HandoffStarted { .. }))
}

/// [`LOOKAHEAD_OVERSHOOT_BOUND`]'s trigger history: a routine published a
/// request for a height above the window of the highest verified tip that
/// any view had published by then.
fn above_window_requested(log: &ObservationLog) -> bool {
    let mut tip = 0;
    log.entries().iter().any(|entry| match &entry.obs {
        Observation::Storage {
            fact: StorageFact::ViewPublished { verified_best, .. },
            ..
        } => {
            tip = tip.max(u64::from(verified_best.height));
            false
        }
        Observation::BlockSync {
            event: BlockSyncFact::Request { heights, step, .. },
            ..
        } => {
            step == "published"
                && heights
                    .iter()
                    .any(|height| u64::from(*height) > tip.saturating_add(COMMIT_WINDOW_BLOCKS))
        }
        _ => false,
    })
}

/// [`TEARDOWN_RETURNS_BASELINE`]'s trigger history: a routine's
/// second-to-last snapshot listed an unreceived request height before its
/// last one, so the routine ended owing work it had to return.
fn routine_ended_owing(log: &ObservationLog) -> bool {
    let mut owing = BTreeSet::new();
    block_sync_facts(log).any(|fact| match fact {
        BlockSyncFact::Routine {
            connection,
            generation,
            requests,
            ended,
            ..
        } => {
            let routine = (*connection, *generation);
            if *ended {
                return owing.contains(&routine);
            }
            let unreceived = requests
                .iter()
                .any(|request| request.heights.iter().any(|height| !height.received));
            if unreceived {
                owing.insert(routine);
            } else {
                owing.remove(&routine);
            }
            false
        }
        _ => false,
    })
}
