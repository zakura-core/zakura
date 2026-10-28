//! Block sync's resource bounds (properties.md, Block sync and Resource
//! allowances).
//!
//! [`OwnerModel`] is one owner model of block sync's requests, estimates,
//! holders and releases. It folds [`OwnerInput`]s, the seam its evidence
//! comes through, and never reads a production acceptance result. Today
//! every input is an effect the block-sync observer reports and the tap
//! logs ([`OwnerInput::from_fact`]). The model takes the bounds' limits
//! from the node's configuration and the fixture, and checks the budget's
//! own reported maximum against them.
//!
//! Three properties read it:
//! - [`RETAINED_BYTE_BOUND`]: retained wire bytes stay within
//!   `L + 401·b + Q·b`, after every step;
//! - [`RESERVED_BYTE_BOUND`]: the byte budget stays within `M + 2·B + h·B`,
//!   after every step, where `h` counts the charged handoffs open then
//!   (D2 in the lane 3 plan);
//! - [`BUDGET_LEDGERS_AGREE`]: at each post-step quiescence boundary, the
//!   byte budget equals the work queue's reserved tally.

use std::collections::BTreeMap;

use zakura_chain::{block::MAX_BLOCK_BYTES, parameters::checkpoint::constants};
use zakura_network::zakura::ZakuraBlockSyncConfig;
use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    observe::{BlockSyncFact, BlockSyncHolder, WorkOwner},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Unit, Verdict},
    Observation, ObservationLog,
};

/// Retained wire bytes stay within the derived bound.
pub const RETAINED_BYTE_BOUND: &str = "block-sync/retained-byte-bound";

/// Reserved request bytes stay within the derived bound.
pub const RESERVED_BYTE_BOUND: &str = "block-sync/reserved-byte-bound";

/// The two reserved-byte ledgers agree.
pub const BUDGET_LEDGERS_AGREE: &str = "block-sync/budget-ledgers-agree";

/// Heights above the verified tip that skip block sync's look-ahead gates:
/// one worst-case checkpoint range, `MAX_CHECKPOINT_HEIGHT_GAP + 1`
/// (`admission.rs`'s `COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS`).
pub const COMMIT_WINDOW_BLOCKS: u64 = constants::MAX_CHECKPOINT_HEIGHT_GAP as u64 + 1;

/// The largest estimate one request height reserves: `B`, the most a body
/// can be. Block sync clamps every estimate to `[1,024, B]`, and a missing
/// size hint reserves `B` (properties.md, Units).
pub const ESTIMATE_CEILING: u64 = MAX_BLOCK_BYTES;

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
    /// The largest body the run can retain, in wire bytes. The routine keeps
    /// only a body that hashes to a header it requested, and every header
    /// the node holds comes from the fixture, so the fixture's largest body
    /// bounds every retained one.
    pub largest_body: u64,
}

impl Limits {
    /// The limits of a node that runs `config`, on a fixture whose largest
    /// body is `largest_body` wire bytes.
    pub fn of(config: &ZakuraBlockSyncConfig, largest_body: u64) -> Limits {
        Limits {
            budget: config.max_inflight_block_bytes,
            lookahead: config.effective_max_reorder_lookahead_bytes(),
            queue: u64::try_from(config.submitted_apply_limit()).unwrap_or(u64::MAX),
            largest_body,
        }
    }

    /// `M + 2·B + h·B`, the most the byte budget may count while `handoffs`
    /// charged handoffs are open (properties.md, Reserved bytes).
    ///
    /// Only a floor request may charge past `M`, and only one that starts at
    /// the download floor or the next height. Each height has one owner, so
    /// at most two such charges are live, each at most `B`. While a routine
    /// hands a body to the sequencer, both the sequencer and the budget
    /// count it, and the sequencer can advance the floor, so another routine
    /// can overdraft for the new floor's next height. Each open handoff adds
    /// one more `B`.
    pub fn reserved_bound(&self, handoffs: u64) -> u64 {
        self.budget
            .saturating_add(handoffs.saturating_add(2).saturating_mul(ESTIMATE_CEILING))
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

/// One input the owner model folds.
///
/// Every input today comes from an effect the block-sync observer reports
/// ([`OwnerInput::from_fact`]). Each routine's snapshot gives its
/// outstanding requests, with each height's estimate and whether its body
/// arrived. The stream-6 exchange model can feed [`OwnerInput::Requests`]
/// from frame history instead, so a request counts once its `GET_BLOCKS`
/// frame leaves and a height once its `BLOCK` frame arrives. The budget,
/// handoff, holder and work-queue inputs are the node's own state, so they
/// stay observer effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerInput {
    /// The routine for connection `connection`, generation `generation`,
    /// has `requests` outstanding, and no others.
    Requests {
        /// The routine's connection.
        connection: u64,
        /// The routine's generation.
        generation: u64,
        /// Its outstanding requests.
        requests: Vec<OpenRequest>,
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
    /// `holder` took the body at `height`, `bytes` wire bytes.
    Held {
        /// The holder.
        holder: BodyHolder,
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
}

impl OwnerInput {
    /// The input a logged block-sync event gives, if any.
    pub fn from_fact(fact: &BlockSyncFact) -> Option<OwnerInput> {
        Some(match fact {
            BlockSyncFact::Routine {
                connection,
                generation,
                requests,
                ..
            } => OwnerInput::Requests {
                connection: *connection,
                generation: *generation,
                requests: requests
                    .iter()
                    .map(|request| OpenRequest {
                        owner: request.owner.clone(),
                        heights: request
                            .heights
                            .iter()
                            .map(|height| (height.height, height.estimated_bytes, height.received))
                            .collect(),
                    })
                    .collect(),
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
                    BlockSyncHolder::Queued { allocation } => BodyHolder::Queued(*allocation),
                    BlockSyncHolder::Reorder { allocation } => BodyHolder::Reorder(*allocation),
                    BlockSyncHolder::Applying { allocation } => BodyHolder::Applying(*allocation),
                    BlockSyncHolder::Submitted { token } => BodyHolder::Submitted(*token),
                    BlockSyncHolder::Detached { token } => BodyHolder::Detached(*token),
                };
                if change == "retained" {
                    OwnerInput::Held {
                        holder,
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
            _ => return None,
        })
    }
}

/// An outstanding request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRequest {
    /// The request.
    pub owner: WorkOwner,
    /// Each height with the estimate the request reserved for it, and
    /// whether its body arrived.
    pub heights: Vec<(u32, u64, bool)>,
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

/// Block sync's owners and quantities, folded from [`OwnerInput`]s. See the
/// module doc.
#[derive(Clone, Debug, Default)]
pub struct OwnerModel {
    /// The bounds' limits.
    pub limits: Limits,
    /// The byte budget's counter.
    reserved: u64,
    /// Each routine's outstanding requests, by connection and generation.
    requests: BTreeMap<(u64, u64), Vec<OpenRequest>>,
    /// Each open handoff, oldest first.
    handoffs: Vec<OpenHandoff>,
    /// Each body holder, with its body's height and wire bytes.
    holders: BTreeMap<BodyHolder, (u32, u64)>,
    /// Each retained allocation's holder count and wire bytes.
    allocations: BTreeMap<u64, (u32, u64)>,
    /// The wire bytes of every retained allocation.
    retained: u64,
    /// The work queue's reserved tally, from its latest snapshot.
    queue_reserved: u64,
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
}

impl OwnerModel {
    /// An empty model with `limits`.
    pub fn new(limits: Limits) -> OwnerModel {
        OwnerModel {
            limits,
            ..OwnerModel::default()
        }
    }

    /// Folds the input a logged block-sync event gives, if any.
    pub fn observe(&mut self, fact: &BlockSyncFact) {
        if let Some(input) = OwnerInput::from_fact(fact) {
            self.apply(input);
        }
    }

    /// Folds `input`.
    pub fn apply(&mut self, input: OwnerInput) {
        match input {
            OwnerInput::Requests {
                connection,
                generation,
                requests,
            } => {
                if requests.is_empty() {
                    self.requests.remove(&(connection, generation));
                } else {
                    self.requests.insert((connection, generation), requests);
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
                height,
                bytes,
            } => {
                if bytes > self.limits.largest_body {
                    self.note_holder_gap(format!(
                        "{holder:?} holds {bytes} bytes at height {height}, more than the \
                         fixture's largest body, {}",
                        self.limits.largest_body
                    ));
                }
                if self.holders.insert(holder, (height, bytes)).is_some() {
                    self.note_holder_gap(format!("{holder:?} took a second body"));
                }
                if let Some(allocation) = holder.retained_allocation() {
                    let entry = self.allocations.entry(allocation).or_insert((0, bytes));
                    if entry.0 == 0 {
                        self.retained = self.retained.saturating_add(bytes);
                    }
                    entry.0 += 1;
                    self.peaks.retained = self.peaks.retained.max(self.retained);
                }
            }
            OwnerInput::Released { holder } => {
                if self.holders.remove(&holder).is_none() {
                    self.note_holder_gap(format!("{holder:?} released a body it never took"));
                    return;
                }
                if let Some(allocation) = holder.retained_allocation() {
                    if let Some(entry) = self.allocations.get_mut(&allocation) {
                        entry.0 -= 1;
                        if entry.0 == 0 {
                            self.retained = self.retained.saturating_sub(entry.1);
                            self.allocations.remove(&allocation);
                        }
                    }
                }
            }
            OwnerInput::WorkQueue { reserved } => self.queue_reserved = reserved,
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

    /// The open handoffs, D2's `h`.
    pub fn handoffs(&self) -> &[OpenHandoff] {
        &self.handoffs
    }

    /// The wire bytes the sequencer retains: each body held by its input
    /// queue, reorder buffer or `applying`, once per allocation.
    pub fn retained(&self) -> u64 {
        self.retained
    }

    /// The work queue's reserved tally, from its latest snapshot.
    pub fn queue_reserved(&self) -> u64 {
        self.queue_reserved
    }

    /// Each outstanding request, by its routine's latest snapshot.
    pub fn requests(&self) -> impl Iterator<Item = &OpenRequest> {
        self.requests.values().flatten()
    }

    /// The estimates outstanding requests reserved for heights whose bodies
    /// haven't arrived.
    pub fn outstanding_estimates(&self) -> u64 {
        self.requests()
            .flat_map(|request| &request.heights)
            .filter(|(_, _, received)| !received)
            .map(|(_, estimate, _)| estimate)
            .sum()
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
}

/// A view that holds an [`OwnerModel`]. The resource properties read it.
pub trait OwnerView {
    /// Runs `read` on the owner model after the step.
    fn read_owners<T>(&self, read: impl FnOnce(&OwnerModel) -> T) -> T;
}

/// The three resource properties, all active. A harness registers them on
/// a view that holds the node's [`OwnerModel`].
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
            witness: Some(budget_short),
        },
        Property {
            id: PropertyId::new(BUDGET_LEDGERS_AGREE),
            claim: "at quiescence, the byte budget equals the work queue's reserved tally",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Schedule],
            status: Status::Active,
            check: check_ledgers::<S>,
            witness: Some(body_handed_off),
        },
    ]
}

/// Checks [`RETAINED_BYTE_BOUND`] after a step.
fn check_retained<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let (retained, bound, gap) = cx.view().read_owners(|owners| {
        (
            owners.retained(),
            owners.limits.retained_bound(),
            owners.holder_gap().map(str::to_owned),
        )
    });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    if retained <= bound {
        return Verdict::Holds;
    }
    cx.violated(
        format!("the sequencer retains at most L + 401·b + Q·b = {bound} wire bytes"),
        format!("it retains {retained}"),
    )
}

/// Checks [`RESERVED_BYTE_BOUND`] after a step, with `h` the charged
/// handoffs open then.
fn check_reserved<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    let (reserved, handoffs, limits, gap) = cx.view().read_owners(|owners| {
        let gap = owners
            .budget_gap()
            .or_else(|| owners.holder_gap())
            .map(str::to_owned);
        let handoffs = u64::try_from(owners.handoffs().len()).unwrap_or(u64::MAX);
        (owners.reserved(), handoffs, owners.limits, gap)
    });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    let bound = limits.reserved_bound(handoffs);
    if reserved <= bound {
        return Verdict::Holds;
    }
    cx.violated(
        format!(
            "the byte budget counts at most M + 2·B + h·B = {bound} bytes, with M = {} and \
             h = {handoffs}",
            limits.budget
        ),
        format!("it counts {reserved}"),
    )
}

/// Checks [`BUDGET_LEDGERS_AGREE`] at a step after which no task can run.
///
/// Every path that releases a request's charge updates the work queue and
/// the budget with no `await` between them. A routine hands a body to the
/// sequencer through a send permit it reserved, so no routine parks
/// between a handoff's start and its release either. At quiescence the
/// budget therefore equals the work queue's tally, with no handoff open.
fn check_ledgers<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: OwnerView,
{
    if cx.status == SchedulerStatus::Running {
        return Verdict::NotApplicable;
    }
    let (reserved, queued, handoffs, gap) = cx.view().read_owners(|owners| {
        let gap = owners
            .budget_gap()
            .or_else(|| owners.holder_gap())
            .map(str::to_owned);
        (
            owners.reserved(),
            owners.queue_reserved(),
            owners.handoffs().len(),
            gap,
        )
    });
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    if reserved == queued {
        return Verdict::Holds;
    }
    cx.violated(
        format!("the byte budget equals the work queue's tally, {queued}"),
        format!("the budget counts {reserved}, with {handoffs} handoffs open"),
    )
}

/// The block-sync events in `log`.
fn block_sync_facts(log: &ObservationLog) -> impl Iterator<Item = &BlockSyncFact> {
    log.entries().iter().filter_map(|entry| match &entry.obs {
        Observation::BlockSync { event, .. } => Some(event),
        _ => None,
    })
}

/// [`RETAINED_BYTE_BOUND`]'s trigger history: the commit window filled. A
/// sequencer snapshot held a body at every window height but the missing
/// one above the verified tip, `COMMIT_WINDOW_BLOCKS - 1` bodies in its
/// reorder buffer and `applying`, so every further body had to pass the
/// look-ahead gate.
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

/// [`RESERVED_BYTE_BOUND`]'s trigger history: the budget ran short. A
/// change left less than [`ESTIMATE_CEILING`] free, so a request for a
/// body of unknown size could proceed only by charging past `M`, which only
/// a floor request may do.
fn budget_short(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(|fact| match fact {
        BlockSyncFact::Budget { after, limit, .. } => {
            after.saturating_add(ESTIMATE_CEILING) > *limit
        }
        _ => false,
    })
}

/// [`BUDGET_LEDGERS_AGREE`]'s trigger history: a routine handed a charged
/// body to the sequencer, the release that moves the two ledgers apart.
fn body_handed_off(log: &ObservationLog) -> bool {
    block_sync_facts(log).any(|fact| matches!(fact, BlockSyncFact::HandoffStarted { .. }))
}
