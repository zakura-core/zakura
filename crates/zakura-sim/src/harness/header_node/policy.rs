//! Outcome predicates (M4-B2): which storage operations a hold picks, what
//! it waits for, and what it offers once it opens.
//!
//! [`Predicates`] counts the storage facts and link effects the run has
//! observed. Only [`Predicates::record`] and [`Predicates::record_link`]
//! change it, and the run calls them from `observe`. Everything else reads
//! those counts and the time a caller passes in, so the sources built on it
//! keep `enabled` and `boundaries` pure, as replay needs.
//!
//! A trigger counts effects, never picks: a `Control` pick only queues its
//! action, so [`Trigger::Closed`] counts connections that actually closed. No
//! trigger shows that a caller has handled a reply, only that it took it
//! ([`Trigger::ReplyTaken`]), since that needs a witness from the reactor
//! itself.

use std::{collections::BTreeMap, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{controlled::PendingOp, Fault, Outcome};

use super::{after, Base};
use crate::{
    observe::{StorageFact, StorageRequest},
    Boundary,
};

/// A storage operation's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpKind {
    /// `continuation_locator`.
    Locator,
    /// `vct_repair_context`.
    VctContext,
    /// `acquire_header_path`.
    Acquire,
    /// `read_header_path`.
    Read,
    /// `release_header_path`.
    Release,
    /// `prepare_header_target`.
    Prepare,
    /// `apply_header_target`.
    Apply,
    /// `selected_chain_contains`.
    Membership,
}

impl OpKind {
    /// `request`'s kind, and the session it's attributed to, if any.
    /// Locators and membership reads carry none.
    fn of(request: &StorageRequest) -> (OpKind, Option<u64>) {
        match request {
            StorageRequest::ContinuationLocator => (OpKind::Locator, None),
            StorageRequest::VctRepairContext { owner, .. } => {
                (OpKind::VctContext, Some(owner.session_id))
            }
            StorageRequest::AcquireHeaderPath { session_id, .. } => {
                (OpKind::Acquire, Some(*session_id))
            }
            StorageRequest::ReadHeaderPath { lease, .. } => {
                (OpKind::Read, lease.as_ref().map(|lease| lease.session_id))
            }
            StorageRequest::ReleaseHeaderPath { lease, .. } => (
                OpKind::Release,
                lease.as_ref().map(|lease| lease.session_id),
            ),
            StorageRequest::PrepareHeaderTarget { owner, .. } => {
                (OpKind::Prepare, Some(owner.session_id))
            }
            StorageRequest::ApplyHeaderTarget { owner, .. } => {
                (OpKind::Apply, Some(owner.session_id))
            }
            StorageRequest::SelectedChainContains { .. } => (OpKind::Membership, None),
        }
    }
}

/// Which operations a hold or trigger names. Every set field must match.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Select {
    /// Only this kind.
    #[serde(default)]
    pub kind: Option<OpKind>,
    /// Only the `nth` submission of `kind`, from 1. Locators carry no
    /// session, so this is how a scenario ties one to its request.
    #[serde(default)]
    pub nth: Option<u32>,
    /// Only operations attributed to this session.
    #[serde(default)]
    pub session: Option<u64>,
    /// Only the held operation itself, in a hold's trigger. Matches nothing
    /// in a hold's own selection or a control step's trigger.
    #[serde(default)]
    pub held: bool,
}

impl Select {
    /// The `nth` submission of `kind`.
    pub fn nth(kind: OpKind, nth: u32) -> Self {
        Select {
            kind: Some(kind),
            nth: Some(nth),
            ..Select::default()
        }
    }

    /// The held operation itself.
    pub fn held() -> Self {
        Select {
            held: true,
            ..Select::default()
        }
    }
}

/// What a hold or a control step waits for. An event trigger holds once its
/// event has happened, and [`Trigger::Count`] once it has happened `n`
/// times.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Trigger {
    /// A selected operation was submitted.
    Submitted(Select),
    /// A selected operation's caller dropped its call.
    CallerDropped(Select),
    /// A selected operation completed. Its reply is only queued then.
    Completed(Select),
    /// A selected operation's caller consumed its reply. That doesn't mean
    /// the caller has acted on it: a caller can reach a scheduling point
    /// between taking a reply and handling it, as the reactor does when it
    /// drops a locator's permit before finishing the query. So this isn't
    /// a callback-handled witness, and none of these triggers is one.
    ReplyTaken(Select),
    /// A connection to this peer closed, counted once per connection,
    /// whether a `Control` close or a service tearing it down closed it.
    Closed(String),
    /// A link to this peer was made, including at setup.
    Linked(String),
    /// This mirror's chain grew: a `Control` growth committed its heavier
    /// tip and published it on the mirror's watch.
    Grown(String),
    /// This many milliseconds after the held operation's submission, or
    /// after the run's start in a control step. Time stops there.
    VirtualTime(u64),
    /// The event trigger has happened at least `n` times. Any other trigger
    /// counts once while it holds.
    Count(u32, Box<Trigger>),
    /// Every trigger holds.
    All(Vec<Trigger>),
}

/// An outcome a hold may offer once it opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Allow {
    /// Run the operation against the engine.
    Engine,
    /// The adapter's timeout, legal once its deadline passes.
    Timeout,
    /// Storage unavailable.
    Unavailable,
    /// A local resource fault.
    LocalResource,
    /// The adapter's input check refused the request.
    Rejected,
}

impl Allow {
    /// The storage outcome.
    fn outcome(self) -> Outcome {
        match self {
            Allow::Engine => Outcome::Engine,
            Allow::Timeout => Outcome::Fault(Fault::Timeout),
            Allow::Unavailable => Outcome::Fault(Fault::Unavailable),
            Allow::LocalResource => Outcome::Fault(Fault::LocalResource),
            Allow::Rejected => Outcome::Rejected,
        }
    }
}

/// Offers nothing for the operations `select` picks until `until` holds,
/// then only `then`. Held operations ignore the base bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hold {
    /// The operations held. The first hold that matches an operation at its
    /// submission holds it.
    pub select: Select,
    /// When the hold opens.
    pub until: Trigger,
    /// What it offers once open, among the outcomes legal then.
    pub then: Vec<Allow>,
    /// If set, the operation must complete within this many milliseconds of
    /// its submission. An open hold only offers its outcomes, so a test that
    /// needs an upper bound sets one past the opening.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

/// One kind of fact a trigger counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Submitted,
    CallerDropped,
    Completed,
    ReplyTaken,
}

/// A link effect a task made, as [`Trigger::Closed`] and
/// [`Trigger::Linked`] count it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkEffect {
    /// A link was made.
    Linked,
    /// A link's connection closed. Recorded once per connection.
    Closed,
    /// A mirror's chain grew and published its new tip.
    Grown,
}

/// What [`Predicates`] knows about one operation from its submission.
#[derive(Clone, Copy, Debug)]
struct Op {
    kind: OpKind,
    nth: u32,
    session: Option<u64>,
    /// The hold that picked it.
    hold: Option<usize>,
}

/// The time a trigger is judged at, and the operation it's judged for.
#[derive(Clone, Copy, Debug)]
struct Context {
    /// The held operation, for [`Select::held`].
    held: Option<u64>,
    /// Time since the held operation's submission, or since the run's start.
    since: Duration,
}

/// The holds and everything they count. See the module doc.
#[derive(Clone, Debug, Default)]
pub struct Predicates {
    holds: Vec<Hold>,
    ops: BTreeMap<u64, Op>,
    submitted: BTreeMap<OpKind, u32>,
    events: Vec<(Event, u64)>,
    links: Vec<(LinkEffect, String)>,
}

impl Predicates {
    /// Predicates for `holds`, with nothing observed yet.
    pub fn new(holds: Vec<Hold>) -> Self {
        Predicates {
            holds,
            ..Predicates::default()
        }
    }

    /// Counts one storage fact of the node's port.
    pub fn record(&mut self, fact: &StorageFact) {
        let (event, id) = match fact {
            StorageFact::Submitted { id, request, .. } => {
                let (kind, session) = OpKind::of(request);
                let nth = self.submitted.entry(kind).or_default();
                *nth += 1;
                let mut op = Op {
                    kind,
                    nth: *nth,
                    session,
                    hold: None,
                };
                op.hold = self
                    .holds
                    .iter()
                    .position(|hold| !hold.select.held && op.matches(&hold.select));
                self.ops.insert(*id, op);
                (Event::Submitted, *id)
            }
            StorageFact::CallerDropped { id } => (Event::CallerDropped, *id),
            StorageFact::Completed { id, .. } => (Event::Completed, *id),
            StorageFact::ReplyTaken { id } => (Event::ReplyTaken, *id),
            _ => return,
        };
        self.events.push((event, id));
    }

    /// Counts one link effect on the link to `peer`.
    pub fn record_link(&mut self, effect: LinkEffect, peer: &str) {
        self.links.push((effect, peer.to_owned()));
    }

    /// Whether operation `id` matches `select` in `cx`.
    fn selects(&self, select: &Select, id: u64, cx: Context) -> bool {
        if select.held && cx.held != Some(id) {
            return false;
        }
        self.ops.get(&id).is_some_and(|op| op.matches(select))
    }

    /// How many times `trigger` has happened in `cx`.
    fn count(&self, trigger: &Trigger, cx: Context) -> usize {
        let events = |kind: Event, select: &Select| {
            self.events
                .iter()
                .filter(|(event, id)| *event == kind && self.selects(select, *id, cx))
                .count()
        };
        let links = |kind: LinkEffect, peer: &str| {
            self.links
                .iter()
                .filter(|(effect, linked)| *effect == kind && linked == peer)
                .count()
        };
        match trigger {
            Trigger::Submitted(select) => events(Event::Submitted, select),
            Trigger::CallerDropped(select) => events(Event::CallerDropped, select),
            Trigger::Completed(select) => events(Event::Completed, select),
            Trigger::ReplyTaken(select) => events(Event::ReplyTaken, select),
            Trigger::Closed(peer) => links(LinkEffect::Closed, peer),
            Trigger::Linked(peer) => links(LinkEffect::Linked, peer),
            Trigger::Grown(peer) => links(LinkEffect::Grown, peer),
            Trigger::VirtualTime(_) | Trigger::Count(..) | Trigger::All(_) => {
                usize::from(self.holds_in(trigger, cx))
            }
        }
    }

    /// Whether `trigger` holds in `cx`.
    fn holds_in(&self, trigger: &Trigger, cx: Context) -> bool {
        match trigger {
            Trigger::VirtualTime(ms) => cx.since >= Duration::from_millis(*ms),
            Trigger::Count(n, inner) => self.count(inner, cx) >= *n as usize, // u32 fits usize
            Trigger::All(all) => all.iter().all(|inner| self.holds_in(inner, cx)),
            _ => self.count(trigger, cx) > 0,
        }
    }

    /// Whether a control step's `trigger` holds at `now`, where
    /// [`Trigger::VirtualTime`] counts from the run's start.
    pub fn control_holds(&self, trigger: &Trigger, now: SimInstant) -> bool {
        self.holds_in(
            trigger,
            Context {
                held: None,
                since: now.since_start(),
            },
        )
    }

    /// Where a control step's `trigger` opens later than `now`.
    pub fn control_boundaries(&self, trigger: &Trigger, now: SimInstant, out: &mut Vec<Boundary>) {
        let mut times = Vec::new();
        virtual_times(trigger, &mut times);
        out.extend(
            times
                .into_iter()
                .map(|ms| SimInstant::from_start(Duration::from_millis(ms)))
                .filter(|at| *at > now)
                .map(Boundary::Opens),
        );
    }

    /// The hold on `op`, and the context its trigger is judged in.
    fn hold_on(&self, op: &PendingOp) -> Option<(&Hold, Context)> {
        let hold = self.ops.get(&op.id.0)?.hold?;
        Some((
            &self.holds[hold],
            Context {
                held: Some(op.id.0),
                since: op.since_submit,
            },
        ))
    }

    /// Whether `outcome` is offered for `op`: by its hold once open, or by
    /// `base` for an operation no hold picked.
    pub fn offers(&self, base: Base, op: &PendingOp, outcome: Outcome) -> bool {
        match self.hold_on(op) {
            Some((hold, cx)) => {
                self.holds_in(&hold.until, cx)
                    && hold.then.iter().any(|allow| allow.outcome() == outcome)
            }
            None => base.offers(&op.enabled, outcome),
        }
    }

    /// Where time must stop for `op` at `now`: a held operation's unreached
    /// virtual times and its deadline, or else the base `bound`.
    pub fn boundaries(
        &self,
        bound: Option<Duration>,
        op: &PendingOp,
        now: SimInstant,
        out: &mut Vec<Boundary>,
    ) {
        let by = |d: Duration| Boundary::Deadline(after(now, d.saturating_sub(op.since_submit)));
        let Some((hold, _)) = self.hold_on(op) else {
            out.extend(bound.map(by));
            return;
        };
        let mut times = Vec::new();
        virtual_times(&hold.until, &mut times);
        for at in times.into_iter().map(Duration::from_millis) {
            if at > op.since_submit {
                out.push(Boundary::Opens(after(now, at - op.since_submit)));
            }
        }
        out.extend(hold.deadline_ms.map(Duration::from_millis).map(by));
    }
}

impl Op {
    /// Whether this operation matches `select`'s kind, ordinal and session.
    fn matches(&self, select: &Select) -> bool {
        select.kind.is_none_or(|kind| kind == self.kind)
            && select.nth.is_none_or(|nth| nth == self.nth)
            && select
                .session
                .is_none_or(|session| Some(session) == self.session)
    }
}

/// Every [`Trigger::VirtualTime`] inside `trigger`, in milliseconds.
fn virtual_times(trigger: &Trigger, out: &mut Vec<u64>) {
    match trigger {
        Trigger::VirtualTime(ms) => out.push(*ms),
        Trigger::Count(_, inner) => virtual_times(inner, out),
        Trigger::All(all) => all.iter().for_each(|inner| virtual_times(inner, out)),
        _ => {}
    }
}
