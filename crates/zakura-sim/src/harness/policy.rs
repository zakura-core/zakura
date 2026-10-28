//! Outcome predicates (M4-B2, D4): which storage operations a hold picks,
//! what it waits for, and what it offers once it opens.
//!
//! The grammar is shared by every node whose storage the run completes. A
//! [`Family`] names its operation kinds, the outcomes a hold may offer, and
//! the key that tells its operations apart. The header node's family is
//! `header_node::policy::Header`, and the block node's is
//! `block_node::policy::Block`.
//!
//! [`Predicates`] counts the operations, events and link effects the run has
//! observed. Only its recording methods change it, and the run calls them
//! from `observe`. Everything else reads those counts and the time a caller
//! passes in, so the sources built on it keep `enabled` and `boundaries`
//! pure, as replay needs.
//!
//! A trigger counts effects, never picks: a `Control` pick only queues its
//! action, so [`Trigger::Closed`] counts connections that actually closed.
//! Only [`Trigger::Handled`] shows that the reactor has handled a reply, as
//! its tap witnesses. [`Trigger::ReplyTaken`] shows only that it took it,
//! and [`Trigger::ReplyQueued`] only that the reply reached its queue.
//!
//! A node records only the events it can witness, so each node kind rejects
//! a scenario that waits for an event it never records, rather than leave
//! that hold shut: `header_node::policy::undriven` and
//! `block_node::policy::undriven` say which.

use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_scheduler::SimInstant;

use crate::Boundary;

/// One node kind's operations and outcomes.
pub trait Family {
    /// An operation's kind, which a [`Select`] names.
    type Kind: Copy + Debug + Eq + Ord;
    /// An outcome a hold may offer.
    type Allow: Copy + Debug + Eq;
    /// The storage outcome a completion picks.
    type Outcome: Copy + Eq;
    /// What tells one operation apart from every other in a run.
    type Key: Copy + Debug + Eq + Ord;

    /// Whether `allow` offers `outcome`.
    fn allows(allow: Self::Allow, outcome: Self::Outcome) -> bool;
}

/// `t` plus `d`, saturating at the largest instant.
pub(crate) fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// Which operations a hold or trigger names. Every set field must match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "K: Deserialize<'de>"))]
pub struct Select<K> {
    /// Only this kind.
    #[serde(default)]
    pub kind: Option<K>,
    /// Only the `nth` submission of `kind`, from 1. Some operations carry no
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

impl<K> Default for Select<K> {
    /// Every operation.
    fn default() -> Self {
        Select {
            kind: None,
            nth: None,
            session: None,
            held: false,
        }
    }
}

impl<K> Select<K> {
    /// The `nth` submission of `kind`.
    pub fn nth(kind: K, nth: u32) -> Self {
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
pub enum Trigger<K> {
    /// A selected operation was submitted. An operation with stages counts
    /// once per stage, under that stage's kind.
    Submitted(Select<K>),
    /// A selected operation's caller dropped its call.
    CallerDropped(Select<K>),
    /// A selected operation completed. Its reply is only queued then.
    Completed(Select<K>),
    /// A selected operation's reply reached its caller's queue: block
    /// sync's handle, for a block operation. A completion queues no reply
    /// when that queue is closed. Nothing has taken the reply yet.
    ReplyQueued(Select<K>),
    /// A selected operation's caller consumed its reply. That doesn't mean
    /// the caller has acted on it: a caller can reach a scheduling point
    /// between taking a reply and handling it, as the header reactor does
    /// when it drops a locator's permit before finishing the query. So this
    /// isn't a callback-handled witness. [`Trigger::Handled`] is.
    ReplyTaken(Select<K>),
    /// The reactor finished handling a selected port call's completion, as
    /// its tap reports. A call paired with an operation is selected through
    /// that operation. One paired with none, such as a preparation the
    /// reactor never submitted, is selected by the call's own kind and
    /// session, and never by `nth` or `held`, which name operations. Never
    /// holds without a tap.
    Handled(Select<K>),
    /// A connection to this peer closed, counted once per connection,
    /// whether a `Control` close or a service tearing it down closed it.
    Closed(String),
    /// A link to this peer was made, including at setup.
    Linked(String),
    /// This actor's chain grew: a `Control` growth committed its heavier
    /// tip and published it on the actor's watch. A block-sync mirror
    /// counts once it holds the new bodies too. The block-sync node under
    /// test counts once its engine switches its header tip, before it
    /// commits any new body.
    Grown(String),
    /// This actor's chain switched to a fork: a `Control` reset committed
    /// the fork's tip and published it on the actor's watch. A block-sync
    /// mirror counts once it holds the fork's bodies too. The block-sync
    /// node under test counts once its engine switches its header tip to
    /// the fork, before its verified chain resets.
    Reset(String),
    /// This actor prepared a `Control` chain change's headers, which it
    /// applies in a later step. Only the block-sync node under test takes a
    /// change in two steps, so only it counts, once per preparation, a
    /// repeated one included.
    Prepared(String),
    /// This many milliseconds after the held operation's submission, or
    /// after the run's start in a control step. Time stops there.
    VirtualTime(u64),
    /// The event trigger has happened at least `n` times. Any other trigger
    /// counts once while it holds.
    Count(u32, Box<Trigger<K>>),
    /// Every trigger holds.
    All(Vec<Trigger<K>>),
}

impl<K> Trigger<K> {
    /// The first trigger within this one, itself included, for which
    /// `matches` holds, looking inside [`Trigger::Count`] and
    /// [`Trigger::All`].
    pub fn find(&self, matches: &impl Fn(&Trigger<K>) -> bool) -> Option<&Trigger<K>> {
        if matches(self) {
            return Some(self);
        }
        match self {
            Trigger::Count(_, inner) => inner.find(matches),
            Trigger::All(all) => all.iter().find_map(|inner| inner.find(matches)),
            _ => None,
        }
    }
}

/// Offers nothing for the operations `select` picks until `until` holds,
/// then only `then`. Held operations ignore the base bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "K: Deserialize<'de>, A: Deserialize<'de>")
)]
pub struct Hold<K, A> {
    /// The operations held. The first hold that matches an operation at its
    /// submission holds it.
    pub select: Select<K>,
    /// When the hold opens.
    pub until: Trigger<K>,
    /// What it offers once open, among the outcomes legal then.
    pub then: Vec<A>,
    /// If set, the operation must complete within this many milliseconds of
    /// its submission. An open hold only offers its outcomes, so a test that
    /// needs an upper bound sets one past the opening.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

/// One kind of operation fact a trigger counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    Submitted,
    CallerDropped,
    Completed,
    ReplyQueued,
    ReplyTaken,
}

/// A link effect a task made, as [`Trigger::Closed`], [`Trigger::Linked`],
/// [`Trigger::Grown`], [`Trigger::Reset`] and [`Trigger::Prepared`] count
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkEffect {
    /// A link was made.
    Linked,
    /// A link's connection closed. Recorded once per connection.
    Closed,
    /// An actor's chain grew and published its new tip.
    Grown,
    /// An actor's chain switched to a fork and published its tip.
    Reset,
    /// An actor prepared a chain change's headers, to apply them later.
    Prepared,
}

/// What [`Predicates`] knows about one stage of an operation from that
/// stage's submission.
#[derive(Clone, Copy, Debug)]
struct Op<K> {
    kind: K,
    nth: u32,
    session: Option<u64>,
    /// The hold that picked it.
    hold: Option<usize>,
}

/// What [`Predicates`] knows about one port call from its dispatch and end.
#[derive(Clone, Copy, Debug)]
struct Call<K, Key> {
    kind: K,
    /// The session its work belongs to.
    session: u64,
    /// The operation it's paired with, and that operation's stage when the
    /// call ended.
    op: Option<(Key, usize)>,
}

/// The time a trigger is judged at, and the operation it's judged for.
#[derive(Clone, Copy, Debug)]
struct Context<Key> {
    /// The held operation, for [`Select::held`].
    held: Option<Key>,
    /// Time since the held operation's submission, or since the run's start.
    since: Duration,
}

/// A hold on an operation, and the context its trigger is judged in.
type HoldOn<'a, F> = (
    &'a Hold<<F as Family>::Kind, <F as Family>::Allow>,
    Context<<F as Family>::Key>,
);

/// The holds and everything they count. See the module doc.
#[derive(Clone, Debug)]
pub struct Predicates<F: Family> {
    holds: Vec<Hold<F::Kind, F::Allow>>,
    /// Each operation's stages, in order.
    ops: BTreeMap<F::Key, Vec<Op<F::Kind>>>,
    calls: BTreeMap<u64, Call<F::Kind, F::Key>>,
    submitted: BTreeMap<F::Kind, u32>,
    /// Each operation event with its operation and the stage it happened
    /// in, so a later stage never changes how an earlier event is judged.
    events: Vec<(Event, F::Key, usize)>,
    /// Each handled call, in order.
    handled: Vec<u64>,
    links: Vec<(LinkEffect, String)>,
}

impl<F: Family> Predicates<F> {
    /// Predicates for `holds`, with nothing observed yet.
    pub fn new(holds: Vec<Hold<F::Kind, F::Allow>>) -> Self {
        Predicates {
            holds,
            ops: BTreeMap::new(),
            calls: BTreeMap::new(),
            submitted: BTreeMap::new(),
            events: Vec::new(),
            handled: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Counts operation `key`'s submission as `kind`, attributed to
    /// `session`. The first hold whose selection matches it holds it. An
    /// operation submitted again enters its next stage: it counts toward
    /// the new kind, and the holds pick it afresh. Its earlier stages'
    /// events still count under their own stage.
    pub(crate) fn submit(&mut self, key: F::Key, kind: F::Kind, session: Option<u64>) {
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
        let stages = self.ops.entry(key).or_default();
        stages.push(op);
        let stage = stages.len() - 1;
        self.events.push((Event::Submitted, key, stage));
    }

    /// Operation `key`'s current stage, or 0 for an operation never
    /// submitted, which no selection matches.
    fn stage(&self, key: F::Key) -> usize {
        self.ops
            .get(&key)
            .map_or(0, |stages| stages.len().saturating_sub(1))
    }

    /// Counts operation `key`'s next stage as a submission of `kind`, in the
    /// session its first stage was attributed to. Does nothing for an
    /// operation never submitted.
    pub(crate) fn restage(&mut self, key: F::Key, kind: F::Kind) {
        if let Some(session) = self
            .ops
            .get(&key)
            .and_then(|stages| stages.first())
            .map(|op| op.session)
        {
            self.submit(key, kind, session);
        }
    }

    /// Counts one `event` of operation `key`, in its current stage.
    pub(crate) fn event(&mut self, event: Event, key: F::Key) {
        let stage = self.stage(key);
        self.events.push((event, key, stage));
    }

    /// Counts port call `call`'s dispatch, as `kind` in `session`.
    pub(crate) fn dispatched(&mut self, call: u64, kind: F::Kind, session: u64) {
        self.calls.insert(
            call,
            Call {
                kind,
                session,
                op: None,
            },
        );
    }

    /// Pairs port call `call` with operation `op`, if any, in its current
    /// stage, when it ends.
    pub(crate) fn call_ended(&mut self, call: u64, op: Option<F::Key>) {
        let op = op.map(|key| (key, self.stage(key)));
        if let Some(ended) = self.calls.get_mut(&call) {
            ended.op = op;
        }
    }

    /// Counts the reactor's handling of port call `call`.
    pub(crate) fn handled(&mut self, call: u64) {
        self.handled.push(call);
    }

    /// Counts one link effect on the link to `peer`.
    pub fn record_link(&mut self, effect: LinkEffect, peer: &str) {
        self.links.push((effect, peer.to_owned()));
    }

    /// Whether operation `key`, in `stage`, matches `select` in `cx`.
    fn selects(
        &self,
        select: &Select<F::Kind>,
        key: F::Key,
        stage: usize,
        cx: Context<F::Key>,
    ) -> bool {
        if select.held && cx.held != Some(key) {
            return false;
        }
        self.ops
            .get(&key)
            .and_then(|stages| stages.get(stage))
            .is_some_and(|op| op.matches(select))
    }

    /// Whether port call `call` matches `select` in `cx`: through its paired
    /// operation, or else by its own kind and session.
    fn selects_call(&self, select: &Select<F::Kind>, call: u64, cx: Context<F::Key>) -> bool {
        let Some(handled) = self.calls.get(&call) else {
            return false;
        };
        match handled.op {
            Some((op, stage)) => self.selects(select, op, stage, cx),
            None => {
                !select.held
                    && select.nth.is_none()
                    && select.kind.is_none_or(|kind| kind == handled.kind)
                    && select
                        .session
                        .is_none_or(|session| session == handled.session)
            }
        }
    }

    /// How many times `trigger` has happened in `cx`.
    fn count(&self, trigger: &Trigger<F::Kind>, cx: Context<F::Key>) -> usize {
        let events = |kind: Event, select: &Select<F::Kind>| {
            self.events
                .iter()
                .filter(|(event, key, stage)| {
                    *event == kind && self.selects(select, *key, *stage, cx)
                })
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
            Trigger::ReplyQueued(select) => events(Event::ReplyQueued, select),
            Trigger::ReplyTaken(select) => events(Event::ReplyTaken, select),
            Trigger::Handled(select) => self
                .handled
                .iter()
                .filter(|call| self.selects_call(select, **call, cx))
                .count(),
            Trigger::Closed(peer) => links(LinkEffect::Closed, peer),
            Trigger::Linked(peer) => links(LinkEffect::Linked, peer),
            Trigger::Grown(peer) => links(LinkEffect::Grown, peer),
            Trigger::Reset(peer) => links(LinkEffect::Reset, peer),
            Trigger::Prepared(peer) => links(LinkEffect::Prepared, peer),
            Trigger::VirtualTime(_) | Trigger::Count(..) | Trigger::All(_) => {
                usize::from(self.holds_in(trigger, cx))
            }
        }
    }

    /// Whether `trigger` holds in `cx`.
    fn holds_in(&self, trigger: &Trigger<F::Kind>, cx: Context<F::Key>) -> bool {
        match trigger {
            Trigger::VirtualTime(ms) => cx.since >= Duration::from_millis(*ms),
            Trigger::Count(n, inner) => self.count(inner, cx) >= *n as usize, // u32 fits usize
            Trigger::All(all) => all.iter().all(|inner| self.holds_in(inner, cx)),
            _ => self.count(trigger, cx) > 0,
        }
    }

    /// Whether a control step's `trigger` holds at `now`, where
    /// [`Trigger::VirtualTime`] counts from the run's start.
    pub fn control_holds(&self, trigger: &Trigger<F::Kind>, now: SimInstant) -> bool {
        self.holds_in(
            trigger,
            Context {
                held: None,
                since: now.since_start(),
            },
        )
    }

    /// Where a control step's `trigger` opens later than `now`.
    pub fn control_boundaries(
        &self,
        trigger: &Trigger<F::Kind>,
        now: SimInstant,
        out: &mut Vec<Boundary>,
    ) {
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

    /// The hold on operation `key`'s current stage, and the context its
    /// trigger is judged in, `since` that stage's submission.
    fn hold_on(&self, key: F::Key, since: Duration) -> Option<HoldOn<'_, F>> {
        let hold = self.ops.get(&key)?.last()?.hold?;
        Some((
            &self.holds[hold],
            Context {
                held: Some(key),
                since,
            },
        ))
    }

    /// Whether operation `key`'s hold offers `outcome`, `since` its
    /// submission: only once open, and only its outcomes. `None` if no hold
    /// picked it, so the base decides.
    pub(crate) fn held_offers(
        &self,
        key: F::Key,
        since: Duration,
        outcome: F::Outcome,
    ) -> Option<bool> {
        let (hold, cx) = self.hold_on(key, since)?;
        Some(
            self.holds_in(&hold.until, cx)
                && hold.then.iter().any(|allow| F::allows(*allow, outcome)),
        )
    }

    /// Where time must stop for operation `key`, `since` its submission, at
    /// `now`: a held operation's unreached virtual times and its deadline,
    /// or else the base `bound`.
    pub(crate) fn op_boundaries(
        &self,
        bound: Option<Duration>,
        key: F::Key,
        since: Duration,
        now: SimInstant,
        out: &mut Vec<Boundary>,
    ) {
        let by = |d: Duration| Boundary::Deadline(after(now, d.saturating_sub(since)));
        let Some((hold, _)) = self.hold_on(key, since) else {
            out.extend(bound.map(by));
            return;
        };
        let mut times = Vec::new();
        virtual_times(&hold.until, &mut times);
        for at in times.into_iter().map(Duration::from_millis) {
            if at > since {
                out.push(Boundary::Opens(after(now, at - since)));
            }
        }
        out.extend(hold.deadline_ms.map(Duration::from_millis).map(by));
    }
}

impl<K: Copy + Eq> Op<K> {
    /// Whether this operation matches `select`'s kind, ordinal and session.
    fn matches(&self, select: &Select<K>) -> bool {
        select.kind.is_none_or(|kind| kind == self.kind)
            && select.nth.is_none_or(|nth| nth == self.nth)
            && select
                .session
                .is_none_or(|session| Some(session) == self.session)
    }
}

/// Every [`Trigger::VirtualTime`] inside `trigger`, in milliseconds.
fn virtual_times<K>(trigger: &Trigger<K>, out: &mut Vec<u64>) {
    match trigger {
        Trigger::VirtualTime(ms) => out.push(*ms),
        Trigger::Count(_, inner) => virtual_times(inner, out),
        Trigger::All(all) => all.iter().for_each(|inner| virtual_times(inner, out)),
        _ => {}
    }
}
