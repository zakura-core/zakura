//! Header-sync nodes for M4: real reactors and services, joined by
//! in-memory links, with one node's storage completed by the run.
//!
//! The node under test runs on a [`ControlledPort`] behind a
//! [`PolicySource`], so the `Plan` picks when and how each storage operation
//! completes. Mirror peers run the same code on a plain [`EnginePort`], whose
//! calls finish on first poll. Each actor's chain is committed before
//! `Sim::new` by [`preload`], since a run's scenario clock would otherwise
//! decide whether the fixture headers validate. [`policy`] holds individual
//! operations, and gates `Control` steps, until chosen events happen. A
//! [`tap::ReactorTap`] logs a node's reactor events beside its storage facts,
//! and keeps its adopted state in an [`adoption::AdoptionEvidence`].
//! A `Shutdown` step stops the node under test on purpose, which its tap
//! expects. Each harness action that ends a link or the node is recorded in
//! the tap's [`ends::EndEvidence`] where it happens. The tap and the topology's links keep the node's
//! [`session`] view, and the tap keeps its header capacity accounting in a
//! [`reservation::ReservationEvidence`], and its resource probe's reads in a
//! [`capacity::CapacityEvidence`].

pub mod adoption;
pub mod authority;
pub mod capacity;
pub mod chain_effect;
pub mod claim;
pub mod ends;
pub mod fault;
pub mod fork_choice;
pub mod hold_wake;
pub mod outstanding;
mod pair;
pub mod policy;
pub mod publication;
pub mod recovery;
pub mod reservation;
pub mod retirement;
pub mod round_life;
pub mod session;
pub mod stale;
pub mod stamp;
pub mod tap;
pub mod wake;

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_header_chain::{Frontier, VctRootRepairState, VctRootRepairStatus};
use zakura_network::zakura::{
    framed_channel,
    observe::HeaderSyncObserver,
    spawn_header_sync_reactor,
    v9::{self, RegtestMessage},
    wire_codec::decode_frame,
    FramedRecv, FramedSend, FullStateFrontiers, HeaderSyncHandle, HeaderSyncService,
    HeaderSyncStartup, Peer, Service, ServicePeerDirection, SessionOrigin, ZakuraHeaderSyncConfig,
    ZakuraPeerId, LOCAL_MAX_MESSAGE_BYTES, ZAKURA_CAP_HEADER_SYNC,
    ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port, PrepareHeaderTarget};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{mpsc, watch, CancellationToken};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{
    adapter::EnginePort,
    controlled::{ControlledPort, OpId, ResetRequest},
    fixture::HeaderFixture,
    Outcome,
};

use self::{
    adoption::{AdoptionEvidence, SessionLife},
    ends::EnvironmentAction,
    hold_wake::{HoldWakeCheck, HoldWakeEvidence},
    policy::{Hold, LinkEffect, Predicates, ReplyHold, Trigger},
    recovery::{
        Disruption, Driver, Hostile, Prefix, RecoveryEvidence, RecoveryInputs, SelectedChain,
        RECOVERED, RELINK, UNREACHED,
    },
    session::SessionEvidence,
    tap::{ReactorTap, TapShutdown},
};
use crate::{
    observe::StorageFact,
    sim::Generators,
    storage::{FaultAttributions, HeaderStorage, StorageAction, HEADER_SERVICE},
    stores::{debug_snapshot, EvidenceStores, Registered, Snapshot},
    ActionSource, Boundary, Identity, Observation, ObservationLog, RecoveryOffer, Sim,
};

/// Frames each direction of a link buffers, enough for a page and control
/// frames in flight.
pub(crate) const LINK_DEPTH: usize = 8;

/// A chain an actor holds before its run: `main` headers on branch 0 from
/// genesis, then `fork`, if any, from that tip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    /// Headers on branch 0, from genesis.
    pub main: u32,
    /// A branch from the end of `main`.
    #[serde(default)]
    pub fork: Option<Fork>,
}

/// A branch of a [`Chain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fork {
    /// The fixture branch, which keeps its headers apart from every other
    /// branch's.
    pub branch: u8,
    /// Its headers.
    pub len: u32,
}

impl Chain {
    /// `main` headers on branch 0.
    pub fn main(main: u32) -> Self {
        Chain { main, fork: None }
    }

    /// Each segment of the chain with the frontier it extends, `main` first.
    /// Empty segments are left out.
    pub fn segments(&self, fixture: &HeaderFixture) -> Vec<(Frontier, Vec<Arc<block::Header>>)> {
        let anchor = fixture.anchor();
        let main = fixture.extend(anchor, self.main, 0);
        let fork_parent = main.last().map_or(anchor, |tip| {
            Frontier::new(block::Height(self.main), tip.hash())
        });
        let mut segments = vec![(anchor, main)];
        if let Some(fork) = self.fork {
            segments.push((
                fork_parent,
                fixture.extend(fork_parent, fork.len, fork.branch),
            ));
        }
        segments.retain(|(_, headers)| !headers.is_empty());
        segments
    }

    /// The fixture branches its headers are on: 0, and its fork's.
    pub fn branches(&self) -> impl Iterator<Item = u8> {
        std::iter::once(0).chain(self.fork.map(|fork| fork.branch))
    }

    /// The chain's tip.
    pub fn tip(&self, fixture: &HeaderFixture) -> Frontier {
        self.segments(fixture)
            .last()
            .map_or(fixture.anchor(), |(parent, headers)| {
                let height = parent
                    .height
                    .0
                    .checked_add(u32::try_from(headers.len()).expect("fixture chains are short"))
                    .expect("fixture heights stay small");
                let tip = headers.last().expect("segments aren't empty");
                Frontier::new(block::Height(height), tip.hash())
            })
    }
}

/// Polls `future` once outside a run, where every engine port call finishes
/// on its first poll.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("an engine port call outside a run finishes on its first poll"),
    }
}

/// Commits `chain` to `port`. Call it before `Sim::new`: setup leaves a
/// controlled port pristine, and fixture headers validate the same against
/// any clock only outside a run.
pub fn preload(port: &EnginePort, fixture: &HeaderFixture, chain: &Chain) {
    preload_requests(port, fixture, chain, |request| request);
}

/// [`preload`], with every header carrying the fixture's tree record
/// ([`HeaderFixture::with_tree_aux`]), as a node that holds the chain's
/// tree roots does. Its publisher serves them to a repair.
pub fn preload_with_tree_aux(port: &EnginePort, fixture: &HeaderFixture, chain: &Chain) {
    preload_requests(port, fixture, chain, |request| {
        fixture.with_tree_aux(request)
    });
}

/// Commits `chain` to `port`, one segment at a time, each segment's
/// request passed through `request` first.
fn preload_requests(
    port: &EnginePort,
    fixture: &HeaderFixture,
    chain: &Chain,
    request: impl Fn(PrepareHeaderTarget) -> PrepareHeaderTarget,
) {
    for (request_id, (parent, headers)) in (1..).zip(chain.segments(fixture)) {
        let target =
            request(fixture.target_request(&port.snapshot(), parent, &headers, request_id));
        let prepared = ready(port.prepare_header_target(target)).expect("fixture headers prepare");
        ready(port.apply_header_target(prepared)).expect("fixture headers apply");
    }
}

/// Which outcomes the policy offers for operations no hold picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Base {
    /// Every outcome the contract allows, for safety sweeps.
    Any,
    /// `engine`, or `rejected` where that's the only legal outcome, so a
    /// correct node converges.
    SuccessOnly,
}

impl Base {
    /// Whether the base offers `outcome` for an operation whose legal
    /// outcomes are `legal`.
    pub fn offers(self, legal: &[Outcome], outcome: Outcome) -> bool {
        match self {
            Base::Any => true,
            Base::SuccessOnly => crate::storage::succeeds(legal, outcome),
        }
    }
}

/// How the run completes the node's storage operations. The scenario
/// records it, so a trace replays it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// The outcomes offered.
    pub base: Base,
    /// If set, each operation must complete within this many milliseconds of
    /// its submission, and time stops there until it does. Convergence runs
    /// keep it well under the reactor's 30 s request deadline, so a slow
    /// storage call can't close a session that nothing reconnects.
    #[serde(default)]
    pub bound_ms: Option<u64>,
    /// Operations held apart from the base, each by the first hold that
    /// picks it.
    #[serde(default)]
    pub holds: Vec<Hold>,
    /// Applies whose replies are held after they commit, each by the first
    /// reply hold that picks it. Left out of a scenario that has none, so
    /// older scenarios keep their encoding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reply_holds: Vec<ReplyHold>,
    /// What a typed test takes away from recovery after its `Recover` step
    /// (hdr-13). Left out of a scenario that declares none, so others keep
    /// their encoding. The grammar can't encode one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disruptions: Vec<Disruption>,
}

impl Policy {
    /// [`Base::SuccessOnly`] within 5 s, the convergence policy.
    pub fn converging() -> Self {
        Policy {
            base: Base::SuccessOnly,
            bound_ms: Some(5_000),
            holds: Vec::new(),
            reply_holds: Vec::new(),
            disruptions: Vec::new(),
        }
    }
}

/// What tasks and the [`Control`] source did to a [`Topology`], shared with
/// the [`PolicySource`] that counts it for [`Trigger::Linked`],
/// [`Trigger::Closed`], [`Trigger::Grown`], [`Trigger::Reset`] and
/// [`Trigger::Released`], and the names it counts tapped reactor facts
/// under.
#[derive(Clone)]
pub struct Effects {
    /// Each node's peer identity and name, which [`Topology::new`] fills,
    /// so [`Trigger::ReservationOpened`] and [`Trigger::SessionAdmitted`]
    /// name the peer as the scenario does. A fact about a peer with no name
    /// here counts for no trigger.
    pub names: Rc<RefCell<Vec<(ZakuraPeerId, String)>>>,
    /// Every link made, in order.
    pub links: Rc<RefCell<Vec<Link>>>,
    /// Each peer whose chain grew, once per committed growth, in order.
    pub grown: Rc<RefCell<Vec<String>>>,
    /// The node under test's name, once per `Reset` step that acted, in
    /// order.
    pub reset: Rc<RefCell<Vec<String>>>,
    /// Each hold a `Release` step opened, by its index, in order.
    pub released: Rc<RefCell<Vec<usize>>>,
    /// The holds whose release the scenario itself plans, by index, which
    /// the drain doesn't retire ([`Topology::own`]).
    pub owned: Rc<RefCell<BTreeSet<usize>>>,
    /// Why a `Control` step couldn't take effect, which ends the run as a
    /// harness failure: the scenario asked for something that can't happen.
    pub failure: Rc<RefCell<Option<String>>>,
    /// When the node under test's header-sync session on each link ended,
    /// since the run's start, by connection ID. Each link's cancellation
    /// hooks record it, inside the `cancel` that ends the session, so no
    /// harness read touches a token. It's evidence, kept in the node's
    /// stores, so the scheduler's shutdown, which cancels every link, adds
    /// nothing.
    pub ended: Registered<SessionEnds>,
}

/// What [`Effects::ended`] holds: when each link's session ended, by
/// connection ID.
#[derive(Debug, Default)]
pub struct SessionEnds(BTreeMap<u64, Duration>);

impl Snapshot for SessionEnds {
    /// Every end, by connection.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(&self.0)
    }
}

impl Effects {
    /// No effects yet, with the session ends kept in `stores`, the node's.
    pub fn new(stores: &EvidenceStores) -> Effects {
        Effects {
            names: Rc::default(),
            links: Rc::default(),
            grown: Rc::default(),
            reset: Rc::default(),
            released: Rc::default(),
            owned: Rc::default(),
            failure: Rc::default(),
            ended: stores.register("session_ends", SessionEnds::default()),
        }
    }

    /// The node's stores, which keep its session ends.
    pub fn stores(&self) -> &EvidenceStores {
        self.ended.stores()
    }
}

/// A [`HeaderStorage`] whose offers follow a [`Policy`]. It changes its
/// [`Predicates`] only in `observe`, so `enabled` and `boundaries` stay
/// pure. An engine completion of a reply-held apply commits it, and its
/// delivery is offered once its reply hold opens. With a [`ReactorTap`], it
/// logs the node's reactor events after its storage facts, paired with them,
/// and its predicates count both.
pub struct PolicySource {
    storage: HeaderStorage,
    tap: Option<ReactorTap>,
    /// Finds which held operations' replies reached the reactor, once
    /// [`PolicySource::with_hold_wakes`] turns it on.
    hold_wake: Option<HoldWakeCheck>,
    port: Arc<ControlledPort>,
    base: Base,
    bound: Option<Duration>,
    predicates: Rc<RefCell<Predicates>>,
    effects: Effects,
    /// How many links are recorded as made.
    linked: usize,
    /// The connections recorded as closed.
    closed: BTreeSet<u64>,
    /// How many growths are recorded.
    grown: usize,
    /// How many resets are recorded.
    reset: usize,
    /// How many releases are recorded.
    released: usize,
    /// The run's recovery, which retires every hold once it starts.
    recovery: RecoveryEvidence,
    /// Set at drain entry, which retires every hold but the owned ones.
    drained: bool,
}

impl PolicySource {
    /// `port`'s storage under `policy`, with its checkpoint registered with
    /// `sim`. Its predicates count `effects` too.
    pub fn new(sim: &mut Sim, port: Arc<ControlledPort>, policy: Policy, effects: Effects) -> Self {
        let stores = effects.stores().clone();
        PolicySource {
            storage: HeaderStorage::in_stores(sim, port.clone(), &stores),
            tap: None,
            hold_wake: None,
            port,
            base: policy.base,
            bound: policy.bound_ms.map(Duration::from_millis),
            predicates: Rc::new(RefCell::new(
                Predicates::new(policy.holds).with_reply_holds(policy.reply_holds),
            )),
            effects,
            linked: 0,
            closed: BTreeSet::new(),
            grown: 0,
            reset: 0,
            released: 0,
            recovery: RecoveryEvidence::new(&stores),
            drained: false,
        }
    }

    /// This source, retiring every hold once `recovery` starts: from then
    /// on each operation is offered only `engine`, or `rejected` where
    /// that's the only legal outcome, each held reply is delivered, and each
    /// must be done within [`recovery::RECOVERY_BOUND`] of the later of its
    /// submission, or its commit, and the `Recover` pick. Under
    /// [`Disruption::SlowStorage`] each is offered only at that deadline. Before that it
    /// counts each hold's catch and each fault it executes in `recovery`'s
    /// prefix ([`recovery::Prefix`]), and once the prefix is full it catches
    /// and offers as recovery will.
    pub fn with_recovery(self, recovery: RecoveryEvidence) -> Self {
        PolicySource { recovery, ..self }
    }

    /// This source, draining `tap` at each observation, after storage, so
    /// each event's storage watermark is already logged and the tap pairs
    /// calls with that step's facts.
    pub fn with_tap(self, tap: ReactorTap) -> Self {
        PolicySource {
            tap: Some(tap),
            ..self
        }
    }

    /// This source, keeping `header-sync/hold-release-wakes`'s facts in
    /// `evidence` ([`hold_wake`]), stamped with its tap's stamps. Call it
    /// after [`PolicySource::with_tap`].
    ///
    /// # Panics
    ///
    /// If the source has no tap, which logs the reactor facts the check
    /// reads.
    pub fn with_hold_wakes(self, evidence: HoldWakeEvidence) -> Self {
        let tap = self
            .tap
            .as_ref()
            .expect("the hold-wake check reads the reactor facts the tap logs");
        evidence.stamp_with(tap.stamps());
        PolicySource {
            hold_wake: Some(HoldWakeCheck::new(evidence)),
            ..self
        }
    }

    /// The predicates, for a [`Control`] gated by the same counts.
    pub fn predicates(&self) -> Rc<RefCell<Predicates>> {
        self.predicates.clone()
    }

    /// Whom each failed prepare and apply of the node's storage blamed.
    pub fn attributions(&self) -> FaultAttributions {
        self.storage.attributions()
    }

    /// Whether operation `id`'s hold is one the scenario owns
    /// ([`Topology::own`]).
    fn owned(&self, predicates: &Predicates, id: OpId) -> bool {
        predicates
            .hold_of(id.0)
            .is_some_and(|hold| self.effects.owned.borrow().contains(&hold))
    }
}

impl ActionSource for PolicySource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let pending = self.port.pending();
        let committed = self.port.committed();
        let predicates = self.predicates.borrow();
        let started = self.recovery.started();
        let recovering = started.is_some();
        let full = self.recovery.prefix().full();
        // Under `SlowStorage`, an operation is offered only once its
        // recovery deadline, `own` after its submission or commit, comes.
        let due = |own: Duration| {
            !self.recovery.disrupts(Disruption::SlowStorage)
                || started.is_none_or(|started| recovery::deadline(now, started, own) <= now)
        };
        let mut offered = Vec::new();
        self.storage.enabled(now, &mut offered);
        out.extend(
            offered
                .into_iter()
                .filter(|(_, local)| match crate::storage::decode(*local) {
                    StorageAction::Complete(id, outcome) => {
                        pending.iter().find(|op| op.id == id).is_some_and(|op| {
                            let success = Base::SuccessOnly.offers(&op.enabled, outcome);
                            if recovering {
                                success && due(op.since_submit)
                            } else {
                                predicates.offers(self.base, op, outcome) && (success || !full)
                            }
                        })
                    }
                    StorageAction::Deliver(id) => {
                        committed.iter().find(|op| op.id == id).is_some_and(|op| {
                            if recovering {
                                due(op.since_commit)
                            } else {
                                predicates.delivers(op)
                            }
                        })
                    }
                }),
        );
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.storage.boundaries(now, out);
        if let Some(started) = self.recovery.started() {
            let by = |own: Duration| Boundary::Deadline(recovery::deadline(now, started, own));
            out.extend(self.port.pending().iter().map(|op| by(op.since_submit)));
            out.extend(self.port.committed().iter().map(|op| by(op.since_commit)));
            return;
        }
        let predicates = self.predicates.borrow();
        for op in self.port.pending() {
            predicates.boundaries(self.bound, &op, now, out);
        }
        for op in self.port.committed() {
            predicates.reply_boundaries(&op, now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        if let StorageAction::Complete(id, outcome) = crate::storage::decode(local) {
            let fault = self
                .port
                .pending()
                .iter()
                .any(|op| op.id == id && !Base::SuccessOnly.offers(&op.enabled, outcome));
            if fault && self.recovery.in_prefix() {
                self.recovery
                    .prefix()
                    .spend(log.step(), Hostile::Fault(id.0));
            }
        }
        match crate::storage::decode(local) {
            StorageAction::Complete(id, Outcome::Engine)
                if !self.drained
                    && self.recovery.started().is_none()
                    && self.predicates.borrow().holds_reply(id) =>
            {
                self.port
                    .commit(id)
                    .expect("`enabled` offered this completion at this step");
            }
            _ => self.storage.execute(local, log),
        }
    }

    /// The storage's recovery set, which holds don't filter, as in the
    /// `Recover` mode `enabled` follows: the same success rule
    /// ([`crate::storage::succeeds`]) and every held reply's delivery.
    ///
    /// The exception is an owned hold ([`Topology::own`]), such as the
    /// release-wake fixture's. Its operation keeps the live rule: nothing
    /// until its planned release, a `Release` step or `Settle`, and then
    /// only the hold's own outcomes. So the fixture's first prepare still
    /// fails with its planned local fault, and never before the waiting
    /// offer its release step waits for.
    fn recovery(&self, now: SimInstant, out: &mut RecoveryOffer) {
        let pending = self.port.pending();
        let predicates = self.predicates.borrow();
        let mut free = RecoveryOffer::default();
        self.storage.recovery(now, &mut free);
        out.actions.extend(free.actions.into_iter().filter(
            |(_, local)| match crate::storage::decode(*local) {
                StorageAction::Complete(id, _) => !self.owned(&predicates, id),
                StorageAction::Deliver(_) => true,
            },
        ));
        out.quiet.extend(free.quiet);
        out.boundaries.extend(free.boundaries);
        let mut live = Vec::new();
        self.storage.enabled(now, &mut live);
        out.actions.extend(live.into_iter().filter(|(_, local)| {
            match crate::storage::decode(*local) {
                StorageAction::Complete(id, outcome) => {
                    self.owned(&predicates, id)
                        && pending
                            .iter()
                            .find(|op| op.id == id)
                            .is_some_and(|op| predicates.offers(self.base, op, outcome))
                }
                StorageAction::Deliver(_) => false,
            }
        }));
        for op in pending.iter().filter(|op| self.owned(&predicates, op.id)) {
            let mut boundaries = Vec::new();
            predicates.boundaries(None, op, now, &mut boundaries);
            out.boundaries.extend(
                boundaries
                    .into_iter()
                    .filter_map(|boundary| match boundary {
                        Boundary::Opens(at) if at > now => Some(at),
                        Boundary::Opens(_) | Boundary::Deadline(_) => None,
                    }),
            );
        }
    }

    /// Retires every hold but the owned ones: from now on an engine
    /// completion of a reply-held apply completes it instead of holding
    /// its reply.
    fn enter_drain(&mut self, log: &mut ObservationLog) {
        self.drained = true;
        self.storage.enter_drain(log);
    }

    /// The storage's stranded work. An owned hold that can never release
    /// is its owner's to report, as `Settle` does.
    fn holds_work(&self) -> Option<String> {
        self.storage.holds_work()
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let before = log.entries().len();
        let stored = self.storage.observe(log);
        let mut tapped_effects = Vec::new();
        let mut queued = Vec::new();
        // Drained even when storage failed, so a port panic the tap holds
        // still reaches the run's failure cell and outranks that failure.
        let tapped = match &mut self.tap {
            Some(tap) => {
                // The storage source appends only its own port's facts, but
                // a run may hold other services' storage too.
                let runs = self.storage.take_runs();
                tap.page_reads(&self.storage.take_page_reads());
                tap.storage_facts(
                    log.entries()[before..]
                        .iter()
                        .filter_map(|entry| match &entry.obs {
                            Observation::Storage {
                                service, seq, fact, ..
                            } if service == HEADER_SERVICE => Some((*seq, fact)),
                            _ => None,
                        }),
                    &runs,
                );
                let publications = self.storage.take_publications();
                tap.engine_runs(&runs, &publications);
                tapped_effects = tap.take_effects();
                queued = tap.take_queued();
                tap.observe(log)
            }
            None => Ok(()),
        };
        // A claim break the tap found at this step reports before the
        // storage source's failure.
        let stored = match (stored, &self.tap) {
            (Err(error), Some(tap)) => tap.fail_after_claims(log.step(), error),
            (stored, _) => stored,
        };
        stored?;
        tapped?;
        let mut predicates = self.predicates.borrow_mut();
        // The node under test's name, which `Topology::new` lists first.
        let node = self
            .effects
            .names
            .borrow()
            .first()
            .map(|(_, name)| name.clone());
        let names = self.effects.names.borrow();
        let name = |storage: &str| {
            names
                .iter()
                .find(|(id, _)| tap::peer(id) == storage)
                .map(|(_, name)| name.clone())
        };
        // Only this source's storage and tap append here.
        for entry in &log.entries()[before..] {
            match &entry.obs {
                Observation::Storage { service, fact, .. } if service == HEADER_SERVICE => {
                    let prefix = self.recovery.in_prefix().then(|| self.recovery.prefix());
                    if prefix.is_some_and(Prefix::full) {
                        predicates.stop_catching();
                    }
                    predicates.record(fact);
                    if let (StorageFact::Submitted { id, .. }, Some(prefix)) = (fact, prefix) {
                        if predicates.holds_op(*id) {
                            prefix.spend(entry.step, Hostile::Caught(*id));
                        }
                    }
                    if let (StorageFact::ShutDown { .. }, Some(node)) = (fact, &node) {
                        predicates.record_link(LinkEffect::StorageStopped, node);
                    }
                    if let Some(check) = &mut self.hold_wake {
                        check.storage(fact, &predicates, name);
                    }
                }
                Observation::Reactor { event, .. } => {
                    predicates.record_reactor(event);
                    if let Some(check) = &mut self.hold_wake {
                        check.reactor(event);
                    }
                }
                _ => {}
            }
        }
        if let Some(check) = &mut self.hold_wake {
            for (peer, session_id, lease) in &queued {
                check.queued(peer, *session_id, *lease);
            }
        }
        for (effect, peer) in tapped_effects {
            if let Some((_, name)) = names.iter().find(|(id, _)| *id == peer) {
                predicates.record_link(effect, name);
            }
        }
        let links = self.effects.links.borrow();
        for link in &links[self.linked..] {
            predicates.record_link(LinkEffect::Linked, &link.peer);
        }
        self.linked = links.len();
        // A connection closes once, whoever cancels it and however often. A
        // service's own cancellation reaches the session view here.
        for link in links.iter() {
            if link.is_closed() && self.closed.insert(link.conn_id) {
                predicates.record_link(LinkEffect::Closed, &link.peer);
                link.record_closed();
            }
        }
        let grown = self.effects.grown.borrow();
        for peer in &grown[self.grown..] {
            predicates.record_link(LinkEffect::Grown, peer);
        }
        self.grown = grown.len();
        let reset = self.effects.reset.borrow();
        for node in &reset[self.reset..] {
            predicates.record_link(LinkEffect::Reset, node);
        }
        self.reset = reset.len();
        let released = self.effects.released.borrow();
        for hold in &released[self.released..] {
            predicates.release(*hold);
        }
        self.released = released.len();
        match self.effects.failure.borrow().as_ref() {
            Some(failure) => Err(format!("ControlFailed: {failure}")),
            None => Ok(()),
        }
    }
}

/// Set once any reactor sends an operation to its action channel, which
/// only `External` dispatch does. Then that reactor's storage calls never
/// reach its port, and the run's storage evidence would be silently empty.
pub type DispatchFlag = Rc<Cell<bool>>;

/// Spawns a task that waits on a reactor's action channel and sets `flag`
/// if anything arrives. It exits then, or when the channel closes.
pub fn guard_dispatch<T: 'static>(
    sim: &mut Sim,
    mut actions: mpsc::Receiver<T>,
    flag: DispatchFlag,
) {
    sim.scheduler().spawn_root("dispatch-guard", async move {
        if actions.recv().await.is_some() {
            flag.set(true);
        }
    });
}

/// One header-sync actor: a real reactor and service on its port.
pub struct HeaderNode {
    /// The actor's role name, `node` or `mirror-<i>`, which `Control`
    /// actions name.
    pub name: String,
    /// Its peer identity, which its links present.
    pub id: ZakuraPeerId,
    /// The reactor's handle.
    pub handle: HeaderSyncHandle,
    /// The port its reactor and publisher share.
    pub port: Arc<dyn Port>,
    service: HeaderSyncService,
    /// The VCT repair state the reactor follows. `Idle` unless a scenario
    /// sets it.
    pub vct_repairs: watch::Sender<VctRootRepairStatus>,
    /// What the reactor was started with.
    pub startup: StartupFacts,
    /// The reactor's shutdown token, which a `Shutdown` step cancels.
    shutdown: CancellationToken,
}

/// The settings a [`HeaderNode`]'s reactor was started with, read from the
/// startup it was handed, so a test can check them against production's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupFacts {
    /// The header-sync config.
    pub config: ZakuraHeaderSyncConfig,
    /// The reactor's request deadline.
    pub request_timeout: Duration,
    /// How often the reactor refreshes its `Status`.
    pub status_refresh_interval: Duration,
    /// The largest frame the reactor writes.
    pub max_frame_bytes: u32,
    /// The connections the node admits, which caps its admitted peers.
    pub max_connections: usize,
}

impl HeaderNode {
    /// Starts a reactor and its service on `port`, whose selected chain ends
    /// at `tip`, as `zakurad` wires them: production config and timers, the
    /// port's own watch as the committed snapshots, and the default dispatch,
    /// which is direct outside test builds. A guard sets `dispatch` if the
    /// reactor dispatches externally instead. Peer IDs come from `seed`. The
    /// reactor reports its events to `observer`, if one is given.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        port: Arc<dyn Port>,
        fixture: &HeaderFixture,
        tip: Frontier,
        dispatch: DispatchFlag,
        observer: Option<Arc<dyn HeaderSyncObserver>>,
    ) -> Self {
        Self::spawn_configured(
            sim,
            name,
            seed,
            port,
            fixture,
            tip,
            dispatch,
            observer,
            ZakuraHeaderSyncConfig::default(),
        )
    }

    /// [`HeaderNode::spawn`] under `config` instead of production's default
    /// config.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_configured(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        port: Arc<dyn Port>,
        fixture: &HeaderFixture,
        tip: Frontier,
        dispatch: DispatchFlag,
        observer: Option<Arc<dyn HeaderSyncObserver>>,
        config: ZakuraHeaderSyncConfig,
    ) -> Self {
        let anchor = fixture.anchor();
        let mut startup = HeaderSyncStartup::new(
            fixture.network().clone(),
            (anchor.height, anchor.hash),
            FullStateFrontiers {
                finalized_height: anchor.height,
                verified_block_tip: anchor.height,
                verified_block_hash: anchor.hash,
            },
            Some((tip.height, tip.hash)),
            config,
            LOCAL_MAX_MESSAGE_BYTES,
            zakura_network::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
        );
        let (vct_repairs, repairs) = watch::channel(VctRootRepairStatus::default());
        startup.committed_snapshots = Some(port.selected_chain());
        startup.vct_root_repairs = Some(repairs);
        startup.header_chain_port = port.clone();
        startup.observer.0 = observer;
        let shutdown = startup.shutdown.clone();
        let facts = StartupFacts {
            config: startup.config.clone(),
            request_timeout: startup.request_timeout,
            status_refresh_interval: startup.status_refresh_interval,
            max_frame_bytes: startup.max_frame_bytes,
            max_connections: startup.max_connections,
        };
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup).expect("a fixture startup is valid");
        guard_dispatch(sim, actions, dispatch);
        sim.scheduler().spawn_root("header-node", async move {
            let _ = reactor.await;
        });
        let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
        HeaderNode {
            name: name.to_owned(),
            id: ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id"),
            service: HeaderSyncService::new(handle.clone()),
            handle,
            port,
            vct_repairs,
            startup: facts,
            shutdown,
        }
    }
}

/// One connection between two nodes, by their index in a [`Topology`].
#[derive(Clone, Debug)]
pub struct Link {
    /// The name of the node under test's peer on it.
    pub peer: String,
    /// The node that dialed.
    pub dialer: usize,
    /// The node that was dialed.
    pub listener: usize,
    /// The connection's ID, the same on both sides.
    pub conn_id: u64,
    /// The header-sync session's ID, the same on both sides.
    pub session_id: u64,
    /// When it was made, since the run's start.
    pub linked_at: Duration,
    /// Each side's connection token, dialer first.
    tokens: [CancellationToken; 2],
    /// The node under test's session view, which learns of the close.
    sessions: Option<SessionEvidence>,
}

impl Link {
    /// Closes the connection on both sides, as a transport does when either
    /// end goes away.
    pub fn close(&self) {
        if let Some(sessions) = &self.sessions {
            sessions.harness_closed(self.conn_id);
        }
        self.record_closed();
        for token in &self.tokens {
            token.cancel();
        }
    }

    /// Tells the node under test's session view that this link closed. It
    /// keeps only the first notice.
    fn record_closed(&self) {
        if let Some(sessions) = &self.sessions {
            sessions.link_closed(self.conn_id);
        }
    }

    /// Whether either side's connection token is cancelled: by
    /// [`Link::close`], or by a service that tears the connection down,
    /// as replacing a session with an unfinished exchange does. A
    /// replacement that retires only the old session leaves its connection
    /// open, so that isn't a closure.
    pub fn is_closed(&self) -> bool {
        self.tokens.iter().any(CancellationToken::is_cancelled)
    }
}

/// Every node in a run and the links between them.
pub struct Topology {
    /// The nodes, the node under test first.
    pub nodes: Vec<HeaderNode>,
    effects: Effects,
    /// The last connection and session ID handed out. A replacement needs
    /// both to be new, so one counter serves both.
    last_id: Cell<u64>,
    /// The node under test's tap, told of its shutdown.
    tap: Option<TapShutdown>,
    /// The node under test's session view, told of each link.
    sessions: Option<SessionEvidence>,
    /// Set once a `Shutdown` step was picked, which stops any churn.
    stopping: Cell<bool>,
    /// The mirror whose pages the harness holds back, if any.
    delayed_page: Option<DelayedPage>,
}

/// A mirror whose first `Headers` frame on each of its links the harness
/// holds for `by` of virtual time before the node under test gets it, with
/// every later frame behind it, as a slow path to the node would. It's how
/// a fixture makes a page arrive just before the node's request deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayedPage {
    /// The mirror's name.
    pub mirror: String,
    /// How long its first page waits.
    pub by: Duration,
}

/// Forwards each frame from `from_mirror` to `to_node`, in order, holding
/// the first `Headers` frame for `by` of virtual time before it goes and
/// reading nothing meanwhile, until the link closes or either stream ends.
async fn delay_first_page(
    mut from_mirror: FramedRecv,
    to_node: FramedSend,
    by: Duration,
    cancel: CancellationToken,
) {
    let mut held = false;
    loop {
        let frame = zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return,
            frame = from_mirror.recv() => frame,
        };
        let Some(frame) = frame else {
            return;
        };
        if !held
            && matches!(
                decode_frame::<RegtestMessage>(&frame),
                Ok(v9::Message::Headers(_))
            )
        {
            held = true;
            zakura_rt::select! {
                biased;
                () = cancel.cancelled() => return,
                () = zakura_rt::time::sleep(by) => {}
            }
        }
        zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return,
            sent = to_node.send(frame) => if sent.is_err() {
                return;
            },
        }
    }
}

/// The far end of a flooding link: the dialer's streams and its connection
/// token, held by a harness task instead of a service. [`Topology::flood`]
/// swaps the token for its generator's own child of it.
struct FarEnd {
    from_node: FramedRecv,
    to_node: FramedSend,
    cancel: CancellationToken,
}

/// Floods the node under test over `far`. It waits for the node's first
/// `Status` on the session, then sends that frame back as fast as the node
/// takes it, reading whatever the node sends meanwhile, until `far`'s token
/// is cancelled, by `link`'s close or the generator's retirement, or
/// either stream ends. The node's receiver awaits room in its
/// events queue for each `Status` it forwards, so the session stays open
/// while the queue is full. A retirement only stops the sending: the flood
/// then [`keep`]s its streams until the link closes, so the node's session
/// outlives it.
async fn flood(far: FarEnd, link: CancellationToken) {
    let FarEnd {
        mut from_node,
        to_node,
        cancel,
    } = far;
    let status = loop {
        let frame = zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return keep(from_node, to_node, &link).await,
            frame = from_node.recv() => frame,
        };
        let Some(frame) = frame else {
            return;
        };
        if matches!(
            decode_frame::<RegtestMessage>(&frame),
            Ok(v9::Message::Status(_))
        ) {
            break frame;
        }
    };
    loop {
        zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return keep(from_node, to_node, &link).await,
            frame = from_node.recv() => if frame.is_none() {
                return;
            },
            sent = to_node.send(status.clone()) => if sent.is_err() {
                return;
            },
        }
    }
}

/// Keeps a retired flood's session open, as a quiet peer would: holds the
/// stream to the node and reads and drops what the node sends, until
/// `link` closes or the node ends the stream. Dropping the streams instead
/// would end the session with an EOF that no harness close records, which
/// the session view would blame on the node.
async fn keep(mut from_node: FramedRecv, to_node: FramedSend, link: &CancellationToken) {
    let _to_node = to_node;
    loop {
        zakura_rt::select! {
            biased;
            () = link.cancelled() => return,
            frame = from_node.recv() => if frame.is_none() {
                return;
            },
        }
    }
}

/// How often one `Churn` step relinks its mirror. A bound lets a run
/// settle once churn stops, so a node that stops taking work under churn
/// is told apart from one still being churned.
pub const CHURN_RELINKS: u32 = 8;

/// Relinks the mirror whose index `start` delivers to the node under test
/// as its dialer, once per scheduling step, [`CHURN_RELINKS`] times or until
/// the node's shutdown. Each relink replaces the mirror's session, so the
/// node's lifecycle queue takes a disconnect and a connect every time.
///
/// Opening a link has scheduling points, so a `Shutdown` can be picked and
/// sweep the node's links while one is half open. The sweep then misses
/// it, so a relink that returns after the pick closes its own link and
/// stops. The pick sets [`Topology::stopping`] before the sweep runs, so a
/// link the check passes is already in the sweep's copy.
async fn churn(topology: Rc<Topology>, mut start: mpsc::Receiver<usize>) {
    let Some(index) = start.recv().await else {
        return;
    };
    drop(start);
    for _ in 0..CHURN_RELINKS {
        if topology.stopping() {
            return;
        }
        let (link, _far) = topology.open(index, 0, true);
        topology.close(&link);
        if topology.stopping() {
            return;
        }
        zakura_rt::task::yield_now().await;
    }
}

impl Topology {
    /// `nodes`, with no links yet. Each link it makes, and each growth,
    /// goes in `effects`, which learns each node's name.
    pub fn new(nodes: Vec<HeaderNode>, effects: Effects) -> Self {
        effects.names.borrow_mut().extend(
            nodes
                .iter()
                .map(|node| (node.id.clone(), node.name.clone())),
        );
        Topology {
            nodes,
            effects,
            last_id: Cell::new(0),
            tap: None,
            sessions: None,
            stopping: Cell::new(false),
            delayed_page: None,
        }
    }

    /// This topology, holding back `delayed`'s mirror's first page on each
    /// of its links ([`DelayedPage`]).
    pub fn with_delayed_page(self, delayed: Option<DelayedPage>) -> Self {
        Topology {
            delayed_page: delayed,
            ..self
        }
    }

    /// Opens the policy's hold number `hold`, from 0, whatever its trigger
    /// says. Opening an open hold does nothing.
    pub(crate) fn release(&self, hold: usize) {
        self.effects.released.borrow_mut().push(hold);
    }

    /// Marks the policy's hold number `hold` as one whose release the
    /// scenario itself plans, through a `Release` step or a source of its
    /// own. The drain doesn't retire it: the hold stays until that release
    /// runs, then offers only its own outcomes. Call it at setup.
    pub(crate) fn own(&self, hold: usize) {
        self.effects.owned.borrow_mut().insert(hold);
    }

    /// Whether [`Topology::own`] marked hold `hold`.
    fn owns(&self, hold: usize) -> bool {
        self.effects.owned.borrow().contains(&hold)
    }

    /// This topology, telling `tap` before it stops the node under test.
    pub fn with_tap(self, tap: TapShutdown) -> Self {
        Topology {
            tap: Some(tap),
            ..self
        }
    }

    /// This topology, reporting each link it opens and closes to
    /// `sessions`, the node under test's session view, which then joins
    /// each admission to its link.
    pub fn with_sessions(self, sessions: SessionEvidence) -> Self {
        sessions.expect_links();
        Topology {
            sessions: Some(sessions),
            ..self
        }
    }

    /// Records why a `Control` step couldn't take effect, which ends the
    /// run as a harness failure.
    pub(crate) fn fail(&self, why: String) {
        self.effects.failure.borrow_mut().get_or_insert(why);
    }

    /// Tells the node under test's tap that a `Shutdown` step was picked, so
    /// a run that ends before the step takes effect fails as an unfinished
    /// stop.
    fn expect_shutdown(&self) {
        self.stopping.set(true);
        if let Some(tap) = &self.tap {
            tap.begin();
        }
        self.environment(&self.nodes[0].name, None, EnvironmentAction::Shutdown);
    }

    /// Records in the node under test's tap that the harness's `action` on
    /// `peer` ends `connection`, as [`ends`] describes.
    fn environment(&self, peer: &str, connection: Option<u64>, action: EnvironmentAction) {
        if let Some(tap) = &self.tap {
            tap.environment(peer, connection, action);
        }
    }

    /// Closes `link` as the harness's own action, which the node under
    /// test's tap records first.
    fn close(&self, link: &Link) {
        self.environment(&link.peer, Some(link.conn_id), EnvironmentAction::Close);
        link.close();
    }

    /// Whether a `Shutdown` step's stop of the node under test is done: its
    /// tap saw the reactor drop its observer without unwinding. Never true
    /// without a tap.
    fn reactor_stopped(&self) -> bool {
        self.tap.as_ref().is_some_and(TapShutdown::stopped)
    }

    /// Stops the node under test, as zakurad's shutdown ends header sync:
    /// closes every link to it, so its sessions end, and cancels its
    /// reactor, which retires its work and drops its pending calls. Its tap
    /// was told at the step's pick.
    fn shutdown(&self) {
        // Cancelling can be a scheduling point, so the links are copied
        // first. Every link has the node under test at one end.
        let links = self.effects.links.borrow().clone();
        for link in &links {
            link.close();
        }
        self.nodes[0].shutdown.cancel();
    }

    /// Extends the selected chain of the node named `peer` by `len` fixture
    /// headers from its tip, through its own port, as if it had mined them.
    /// It records the growth only once the mirror's published tip is the
    /// new one. The mirror's own sync may move its tip while the growth
    /// waits at a scheduling point, which leaves the growth stale. Then it
    /// grows the new tip instead. Anything else, such as a resource stall
    /// that leaves the target unadmitted while the tip stays put, is
    /// recorded as a failure. The node must be a mirror, on an engine port.
    async fn grow(&self, peer: &str, fixture: &HeaderFixture, len: u32) {
        let node = self
            .nodes
            .iter()
            .find(|node| node.name == peer)
            .unwrap_or_else(|| panic!("the scenario has a `{peer}`"));
        let published = || node.port.selected_chain().borrow().clone();
        loop {
            let snapshot =
                published().expect("an engine port publishes its snapshot until shutdown");
            let tip = snapshot.frontiers.header_best;
            let headers = fixture.extend(tip, len, 0);
            let target = Frontier::new(
                block::Height(
                    tip.height
                        .0
                        .checked_add(len)
                        .expect("fixture heights stay small"),
                ),
                headers.last().expect("a growth has headers").hash(),
            );
            let request = fixture.target_request(&snapshot, tip, &headers, 1);
            let applied = match node.port.prepare_header_target(request).await {
                Ok(prepared) => node.port.apply_header_target(prepared).await,
                Err(error) => Err(error),
            };
            let now = published().map(|snapshot| snapshot.frontiers.header_best);
            match (applied, now) {
                (Ok(ApplyHeaderTargetOutcome::Applied), Some(now)) if now == target => {
                    self.effects.grown.borrow_mut().push(peer.to_owned());
                    return;
                }
                // The mirror moved its tip during the growth.
                (_, Some(now)) if now != tip => continue,
                (applied, now) => {
                    *self.effects.failure.borrow_mut() = Some(format!(
                        "growing {peer} to {target:?} gave {applied:?}, with its tip at {now:?}"
                    ));
                    return;
                }
            }
        }
    }

    /// Connects `dialer` to `listener` with a new connection and session,
    /// each side's stream 5 at version 9 over a pair of in-memory channels.
    /// Each side's peer carries the link's counter as its origin's
    /// connection and generation. The session view hears of the link first.
    pub fn link(&self, dialer: usize, listener: usize) -> Link {
        self.open(dialer, listener, false).0
    }

    /// [`Topology::link`], with the dialer's side handed back as its
    /// streams, from the node and to it, instead of joining the dialer's
    /// service, when `far_end` is set.
    ///
    /// A link to a peer that was linked before is a relink, which the node
    /// under test's tap records ([`ends`]) before the link exists, naming
    /// the new connection. Two relinks of one peer can overlap at the
    /// service's scheduling points, so the earlier session the node replaces
    /// is the admission's to say, not this record's.
    fn open(&self, dialer: usize, listener: usize, far_end: bool) -> (Link, Option<FarEnd>) {
        let peer = if dialer == 0 { listener } else { dialer };
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        if self.link_to(&self.nodes[peer].name).is_some() {
            self.environment(&self.nodes[peer].name, Some(id), EnvironmentAction::Relink);
        }
        let (to_listener, from_dialer) = framed_channel(LINK_DEPTH);
        let (to_dialer, from_listener) = framed_channel(LINK_DEPTH);
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let origin = SessionOrigin {
            connection: id,
            quic_stream: None,
            generation: id,
        };
        if let Some(sessions) = &self.sessions {
            sessions.link_opened(
                &self.nodes[peer].id,
                &self.nodes[peer].name,
                origin,
                id,
                &self.nodes[dialer].name,
                if dialer == 0 {
                    ServicePeerDirection::Outbound
                } else {
                    ServicePeerDirection::Inbound
                },
            );
        }
        let sides = [
            (
                dialer,
                listener,
                ServicePeerDirection::Outbound,
                (from_listener, to_listener),
            ),
            (
                listener,
                dialer,
                ServicePeerDirection::Inbound,
                (from_dialer, to_dialer),
            ),
        ];
        // The node's side of the session ends when either connection token is
        // cancelled, or when the node cancels its own session token alone, as
        // it does to refuse or retire the session. A hook runs inside that
        // `cancel` and is no scheduling point, so it records the instant.
        let ended = |token: &CancellationToken| {
            let ended = self.effects.ended.clone();
            token.on_cancel(move || {
                // The run's own teardown cancels after its evidence froze.
                let Some(now) = zakura_scheduler::hooks::now() else {
                    return;
                };
                ended.write(|ended| {
                    ended.0.entry(id).or_insert(now.since_start());
                });
            });
        };
        for token in &tokens {
            ended(token);
        }
        let mut far = None;
        for ((local, remote, direction, streams), token) in sides.into_iter().zip(&tokens) {
            if far_end && local == dialer {
                far = Some(FarEnd {
                    from_node: streams.0,
                    to_node: streams.1,
                    cancel: token.clone(),
                });
                continue;
            }
            let delay = self
                .delayed_page
                .as_ref()
                .filter(|delayed| local != 0 && delayed.mirror == self.nodes[local].name)
                .map(|delayed| delayed.by);
            let streams = match delay {
                Some(by) => {
                    let (from_node, to_node) = streams;
                    let (to_relay, from_mirror) = framed_channel(LINK_DEPTH);
                    zakura_rt::task::spawn_named(
                        "delay-page",
                        delay_first_page(from_mirror, to_node, by, token.clone()),
                    );
                    (from_node, to_relay)
                }
                None => streams,
            };
            let peer = Peer::new_versioned(
                id,
                self.nodes[remote].id.clone(),
                ZAKURA_CAP_HEADER_SYNC,
                direction,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
                id,
                streams,
                token.clone(),
            )
            .with_origin(origin);
            if local == 0 {
                ended(&peer.service_cancel_token());
            }
            self.nodes[local].service.add_peer(peer);
        }
        let link = Link {
            peer: self.nodes[peer].name.clone(),
            dialer,
            listener,
            conn_id: id,
            session_id: id,
            linked_at: zakura_scheduler::hooks::now()
                .expect("links are made inside a run")
                .since_start(),
            tokens,
            sessions: self.sessions.clone(),
        };
        self.effects.links.borrow_mut().push(link.clone());
        (link, far)
    }

    /// The index of the node named `peer`.
    fn index(&self, peer: &str) -> usize {
        self.nodes
            .iter()
            .position(|node| node.name == peer)
            .unwrap_or_else(|| panic!("the scenario has a `{peer}`"))
    }

    /// Links the mirror named `peer` to the node under test again, as its
    /// dialer, with a flooding task at the mirror's end in place of its
    /// service ([`flood`]), registered with `generators`. The node replaces
    /// the mirror's earlier session.
    ///
    /// The task's token is a child of the far end's connection token: the
    /// connection's close still stops it, and its retirement cancels only
    /// the child, so the connection stays open. One token keeps the loop's
    /// preemption points as they were.
    fn flood(&self, peer: &str, generators: &Generators) {
        let (_, far) = self.open(self.index(peer), 0, true);
        let far = far.expect("an open with a far end hands it back");
        let link = far.cancel.clone();
        let stop = link.child_token();
        let far = FarEnd {
            cancel: stop.clone(),
            ..far
        };
        generators.spawn("flood", stop, flood(far, link));
    }

    /// Whether the node under test's shutdown has begun: a `Shutdown` step
    /// was picked, or its reactor is cancelled.
    fn stopping(&self) -> bool {
        self.stopping.get() || self.nodes[0].shutdown.is_cancelled()
    }

    /// Begins the run's end, as the step loop picks it: the node under
    /// test's stop, recorded as a `Shutdown` step's pick records it, unless
    /// a `Shutdown` step already began it. [`Topology::finish`] carries it
    /// out.
    pub(crate) fn begin_finish(&self) {
        if !self.shutdown_picked() {
            self.expect_shutdown();
        }
    }

    /// Ends every node's work, so a run can end idle: closes every link and
    /// cancels the node under test's reactor, as a `Shutdown` step does,
    /// then each mirror's. Run it from a harness task, after
    /// [`Topology::begin_finish`].
    pub(crate) fn finish(&self) {
        self.shutdown();
        for node in &self.nodes[1..] {
            node.shutdown.cancel();
        }
    }

    /// Whether the harness picked a `Shutdown` step, which begins the node
    /// under test's stop. Only the harness does, so this is an environment
    /// end ([`EndedBy`](crate::property::EndedBy)).
    pub(crate) fn shutdown_picked(&self) -> bool {
        self.stopping.get()
    }

    /// Each link's session and how long it lasted, in link order. A session
    /// ends when [`Effects::ended`] records it, or when a later link to the
    /// same peer replaces it, whichever comes first. The node refuses a
    /// session only by cancelling it, so a session still open here was
    /// admitted.
    pub fn sessions(&self) -> Vec<SessionLife> {
        let links = self.effects.links.borrow();
        let ended = self.effects.ended.read(|ended| ended.0.clone());
        links
            .iter()
            .enumerate()
            .map(|(index, link)| {
                let replaced = links[index + 1..]
                    .iter()
                    .find(|later| later.peer == link.peer)
                    .map(|later| later.linked_at);
                let until = match (ended.get(&link.conn_id).copied(), replaced) {
                    (Some(ended), Some(replaced)) => Some(ended.min(replaced)),
                    (ended, replaced) => ended.or(replaced),
                };
                SessionLife {
                    session_id: link.session_id,
                    from: link.linked_at,
                    until,
                }
            })
            .collect()
    }

    /// The newest link between the node under test and the node named
    /// `peer`.
    pub fn link_to(&self, peer: &str) -> Option<Link> {
        let peer = self.nodes.iter().position(|node| node.name == peer)?;
        self.effects
            .links
            .borrow()
            .iter()
            .rev()
            .find(|link| {
                (link.dialer, link.listener) == (0, peer)
                    || (link.dialer, link.listener) == (peer, 0)
            })
            .cloned()
    }
}

/// A harness action on a link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlAction {
    /// Close the peer's newest link.
    Close,
    /// Link the peer again, with a new connection and session, in the
    /// newest link's direction.
    Relink,
    /// Extend the peer's chain by `len` headers from its tip. The peer must
    /// be a mirror.
    Grow {
        /// The new headers.
        len: u32,
    },
    /// Stop the node under test, which the step must name: close every link
    /// to it and cancel its reactor. Its tap expects the reactor to go, and
    /// checks it left no call or ownership open. A later relink, a second
    /// shutdown, or a run that ends idle or at its budget before the reactor
    /// is gone fails the run.
    Shutdown,
    /// Make an operator invalidation or reconsideration on the node under
    /// test's engine, which the step must name. See [`Reset`]. It's
    /// engine-level, so it can name a header production's operator command
    /// would refuse, as `ControlledPort::reset` says.
    Reset(Reset),
    /// Open the policy's hold number `hold`, from 0, whatever its trigger
    /// says. The step must name the node under test. Releasing an open hold
    /// does nothing.
    Release {
        /// The hold's index in the policy.
        hold: usize,
    },
    /// Shut the node under test's storage down, which the step must name,
    /// so its `shut-down` fact is logged in the run. Offered only once a
    /// `Shutdown` step's stop is done.
    StopStorage,
    /// Commit `len` fixture headers past the node under test's selected tip
    /// straight into its engine, which the step must name, as a writer
    /// other than header sync would. No reactor call takes part, so the
    /// reactor hears of it only through its watches. The node's adoption
    /// evidence records it as `Published` once the engine returns, before
    /// any task runs again.
    Commit {
        /// The new headers.
        len: u32,
    },
    /// Replace the named mirror's link to the node under test with a
    /// flooding one. A harness task at the far end sends the node's own
    /// first `Status` back to it as fast as the node's queue takes it, and
    /// reads whatever the node sends.
    Flood,
    /// Start relinking the named mirror to the node under test, once per
    /// scheduling step, [`CHURN_RELINKS`] times or until the node's
    /// shutdown, so lifecycle events stay queued.
    Churn,
    /// End the hostile prefix and recover, with the named mirror as the
    /// source. At its pick the scenario's controls stop, every invalidation
    /// it made is reconsidered, every hold retires, and from the next pick
    /// the run's scheduling is fair. Offered once the task has carried out
    /// every earlier step, and never after a `Shutdown`. See the
    /// [`recovery`] module.
    Recover,
    /// Publish a VCT root repair status on the node under test's repair
    /// watch, which the step must name, as zakura-state publishes one when
    /// it finds tree data missing. The node's storage resolves the repair
    /// only up to its checkpoint ceiling, which is 0 unless the harness
    /// raised it.
    RepairStatus {
        /// The height whose tree data is missing, or `None` for `Idle`.
        unavailable: Option<u32>,
        /// The status's generation.
        generation: u64,
    },
}

/// What a [`ControlAction::Reset`] does, judged at its pick, since only
/// then is the node's chain known. A reset with nothing to do is still
/// taken, but publishes nothing and counts for no trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Reset {
    /// Invalidate the node's selected header `depth` below its selected tip,
    /// or the lowest header above the finalized anchor if the chain is
    /// shorter. Nothing to do while the node is at the anchor.
    Invalidate {
        /// How far below the tip.
        depth: u32,
    },
    /// Reconsider the newest invalidation the scenario made that's still in
    /// force. Nothing to do if there's none.
    Reconsider,
}

impl ControlAction {
    /// The action's name in its identity.
    fn name(self) -> &'static str {
        match self {
            ControlAction::Close => "close",
            ControlAction::Relink => "relink",
            ControlAction::Grow { .. } => "grow",
            ControlAction::Shutdown => "shutdown",
            ControlAction::Reset(_) => "reset",
            ControlAction::Release { .. } => "release",
            ControlAction::StopStorage => "stop-storage",
            ControlAction::Commit { .. } => "commit",
            ControlAction::Flood => "flood",
            ControlAction::Churn => "churn",
            ControlAction::Recover => "recover",
            ControlAction::RepairStatus { .. } => "repair-status",
        }
    }

    /// Whether the step loop carries it out at its pick, outside any task,
    /// rather than the control task. Each changes only storage or the
    /// harness's own state.
    fn run_only(self) -> bool {
        matches!(
            self,
            ControlAction::Reset(_)
                | ControlAction::Release { .. }
                | ControlAction::StopStorage
                | ControlAction::Commit { .. }
                | ControlAction::Recover
                | ControlAction::RepairStatus { .. }
        )
    }
}

/// One scripted [`ControlAction`] on the link between the node under test
/// and `peer`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlStep {
    /// The peer's name.
    pub peer: String,
    /// What to do.
    pub action: ControlAction,
    /// Offered only once this holds. Always offered without one.
    #[serde(default)]
    pub when: Option<Trigger>,
}

/// Offers a scenario's scripted link actions in order, each once, as
/// `peer:<peer>:<action>`, so the `Plan` decides when each happens, once
/// its trigger holds. A harness task carries each one out on its next step,
/// since linking takes locks that tasks hold across scheduling points, where
/// only a task can wait. So a pick only queues the action, and triggers
/// count the effect. Every check on a step runs at its pick, so a run cut
/// off before the task takes the step still fails an invalid one.
///
/// A reset, release, commit or storage stop changes only storage or the
/// harness's own state, so the step loop carries it out at its pick,
/// outside any task, as storage completions are. It's offered only once the
/// task has carried out every earlier step, so the script keeps its order.
/// A churn step hands its mirror to its own churn task, which relinks it
/// from then on.
///
/// Each step it picks before a `Recover` counts in the recovery's hostile
/// prefix ([`recovery::Prefix`]). Once that's full it offers only the
/// `Recover` step, and the steps between are dropped. The script stops at
/// the `Recover` pick. A typed test's closes after the `Recover` step go on
/// from the `recovered` pick, each the harness ending a member, which
/// breaks recovery's membership assumption (hdr-13's loss path).
/// The grammar can't encode one.
pub struct Control {
    script: Vec<ControlStep>,
    topology: Rc<Topology>,
    predicates: Rc<RefCell<Predicates>>,
    /// The next step to offer.
    next: usize,
    /// Set once a `Shutdown` step was picked.
    shut_down: bool,
    to_task: mpsc::Sender<ControlStep>,
    /// How many queued steps the task hasn't carried out yet.
    queued: Rc<Cell<usize>>,
    /// The node under test's port, for the steps the step loop carries out.
    port: Option<Arc<ControlledPort>>,
    /// The chains' fixture, which a commit extends the node's chain from.
    fixture: HeaderFixture,
    /// The node under test's adoption evidence, which records each commit.
    adoption: Option<AdoptionEvidence>,
    /// What a `Recover` step drives.
    recovery: Option<Driver>,
}

impl Control {
    /// A source for `script` on `topology`, whose chains come from
    /// `fixture`, gated by `predicates`, with its task spawned on `sim`.
    pub fn new(
        sim: &mut Sim,
        topology: Rc<Topology>,
        fixture: HeaderFixture,
        script: Vec<ControlStep>,
        predicates: Rc<RefCell<Predicates>>,
    ) -> Self {
        let (to_task, mut steps) = mpsc::channel::<ControlStep>(script.len().max(1));
        let task_topology = topology.clone();
        let queued = Rc::new(Cell::new(0));
        let task_queued = queued.clone();
        // One task per churn step, so each churned mirror competes for
        // steps on its own. The control task starts them in script order.
        let mut to_churn: VecDeque<mpsc::Sender<usize>> = script
            .iter()
            .filter(|step| step.action == ControlAction::Churn)
            .map(|_| {
                let (start, started) = mpsc::channel::<usize>(1);
                sim.scheduler()
                    .spawn_root("churn", churn(topology.clone(), started));
                start
            })
            .collect();
        let task_fixture = fixture.clone();
        let generators = sim.generators();
        sim.scheduler().spawn_root("control", async move {
            let fixture = task_fixture;
            let topology = task_topology;
            while let Some(step) = steps.recv().await {
                let link = || {
                    topology
                        .link_to(&step.peer)
                        .unwrap_or_else(|| panic!("the scenario links `{}`", step.peer))
                };
                match step.action {
                    ControlAction::Close => topology.close(&link()),
                    ControlAction::Relink => {
                        let link = link();
                        topology.link(link.dialer, link.listener);
                    }
                    ControlAction::Grow { len } => topology.grow(&step.peer, &fixture, len).await,
                    ControlAction::Shutdown => topology.shutdown(),
                    ControlAction::Flood => topology.flood(&step.peer, &generators),
                    ControlAction::Churn => {
                        let index = topology.index(&step.peer);
                        let start = to_churn
                            .pop_front()
                            .expect("each churn step has its own task");
                        // The churn task gives up at a shutdown, which can be
                        // picked while this step waits in the queue. Its
                        // mirror then has nothing left to churn.
                        let _ = start.send(index).await;
                    }
                    ControlAction::Reset(_)
                    | ControlAction::Release { .. }
                    | ControlAction::StopStorage
                    | ControlAction::Commit { .. }
                    | ControlAction::Recover
                    | ControlAction::RepairStatus { .. } => {
                        unreachable!("the step loop carries out run-only steps")
                    }
                }
                task_queued.set(task_queued.get() - 1);
            }
        });
        Control {
            script,
            topology,
            predicates,
            next: 0,
            shut_down: false,
            to_task,
            queued,
            port: None,
            fixture,
            adoption: None,
            recovery: None,
        }
    }

    /// This source, recovering from `inputs` at a `Recover` step and
    /// keeping its progress in `evidence`. Without it, a `Recover` step
    /// fails the run.
    pub fn with_recovery(self, inputs: RecoveryInputs, evidence: RecoveryEvidence) -> Self {
        Control {
            recovery: Some(Driver { inputs, evidence }),
            ..self
        }
    }

    /// Whether a `Recover` step was picked.
    fn recovering(&self) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|driver| driver.evidence.started().is_some())
    }

    /// Whether the script goes on after its `Recover` step: from the
    /// `recovered` pick on, so a typed test's closes after it can run.
    fn resumed(&self) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|driver| driver.evidence.record().is_some())
    }

    /// Whether the hostile prefix before the `Recover` pick is full
    /// ([`Prefix`]).
    fn prefix_full(&self) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|driver| driver.evidence.in_prefix() && driver.evidence.prefix().full())
    }

    /// The index of the script step to offer next: the next one, or once
    /// the hostile prefix is full, the `Recover` step at or after it,
    /// skipping the steps between. `None` once the script is done, or with
    /// the prefix full and no `Recover` step left.
    fn next_step(&self) -> Option<usize> {
        if !self.prefix_full() {
            return (self.next < self.script.len()).then_some(self.next);
        }
        self.script
            .iter()
            .skip(self.next)
            .position(|step| step.action == ControlAction::Recover)
            .map(|offset| self.next + offset)
    }

    /// This source, resetting, committing to and stopping `port`, the node
    /// under test's. Without it, a reset, commit or storage stop fails the
    /// run.
    pub fn with_port(self, port: Arc<ControlledPort>) -> Self {
        Control {
            port: Some(port),
            ..self
        }
    }

    /// This source, recording each commit in `adoption`, the node under
    /// test's adoption evidence.
    pub fn with_adoption(self, adoption: AdoptionEvidence) -> Self {
        Control {
            adoption: Some(adoption),
            ..self
        }
    }

    /// Whether `step` may be offered now, apart from its trigger: a
    /// run-only step once the task carried out every earlier step, a
    /// storage stop once the node's stop is done too, and a recovery only
    /// before any shutdown.
    fn ready(&self, step: &ControlStep) -> bool {
        match step.action {
            ControlAction::StopStorage => {
                self.queued.get() == 0 && self.shut_down && self.topology.reactor_stopped()
            }
            ControlAction::Recover => self.queued.get() == 0 && !self.shut_down,
            action if action.run_only() => self.queued.get() == 0,
            _ => true,
        }
    }

    /// Carries out run-only `step` at its pick, or returns why it can't.
    fn run(&mut self, step: &ControlStep) -> Result<(), String> {
        let port = || {
            self.port
                .clone()
                .ok_or_else(|| format!("{:?} needs the node's port", step.action))
        };
        match step.action {
            ControlAction::Reset(reset) => {
                let request = match reset {
                    Reset::Invalidate { depth } => ResetRequest::Invalidate { depth },
                    Reset::Reconsider => ResetRequest::Reconsider,
                };
                if port()?
                    .reset(request)
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    self.topology
                        .effects
                        .reset
                        .borrow_mut()
                        .push(step.peer.clone());
                }
            }
            ControlAction::Release { hold } => self.topology.release(hold),
            ControlAction::StopStorage => port()?.shut_down(),
            ControlAction::Commit { len } => {
                let adoption = self
                    .adoption
                    .as_ref()
                    .ok_or_else(|| "a commit needs the node's adoption evidence".to_owned())?;
                harness_commit(&*port()?, &self.fixture, adoption, len)?;
            }
            ControlAction::Recover => self.recover(&*port()?, &step.peer)?,
            ControlAction::RepairStatus {
                unavailable,
                generation,
            } => {
                let state = match unavailable {
                    Some(height) => VctRootRepairState::Unavailable {
                        height: zakura_chain::block::Height(height),
                    },
                    None => VctRootRepairState::Idle,
                };
                self.topology.nodes[0]
                    .vct_repairs
                    .send(VctRootRepairStatus { state, generation })
                    .map_err(|_| "the node's reactor no longer follows its repairs".to_owned())?;
            }
            _ => unreachable!("only run-only steps run at their pick"),
        }
        Ok(())
    }

    /// Starts recovery from `source` at the `Recover` pick: keeps the node's
    /// baseline, reconsiders every invalidation still in force on `port`,
    /// newest first, and records the start, which stops the script, retires
    /// the holds and turns on fair scheduling.
    fn recover(&self, port: &ControlledPort, source: &str) -> Result<(), String> {
        let driver = self
            .recovery
            .as_ref()
            .ok_or_else(|| "a recovery needs the harness's recovery inputs".to_owned())?;
        if !driver.inputs.mirrors.iter().any(|(name, _)| name == source) {
            return Err(format!("a recover names a mirror, not {source}"));
        }
        let baseline = SelectedChain::of(port.engine());
        while port
            .reset(ResetRequest::Reconsider)
            .map_err(|error| error.to_string())?
            .is_some()
        {}
        let now = zakura_scheduler::hooks::now()
            .ok_or_else(|| "a recovery starts inside a run".to_owned())?;
        driver.evidence.start(source.to_owned(), now, baseline);
        Ok(())
    }
}

/// Commits `len` of `fixture`'s headers past `port`'s selected tip straight
/// into its engine, through the port's harness-commit hook, which records
/// the commit's engine run, then records the engine's new state in
/// `adoption` as `Published`, as a `Commit` step does. Call it from the step
/// loop, outside any task, so the engine's watch sends wake no task before
/// the record.
pub(crate) fn harness_commit(
    port: &ControlledPort,
    fixture: &HeaderFixture,
    adoption: &AdoptionEvidence,
    len: u32,
) -> Result<(), String> {
    let engine = port.engine();
    let snapshot = engine.snapshot();
    let tip = snapshot.frontiers.header_best;
    let headers = fixture.extend(tip, len, 0);
    let hashes = headers.iter().map(|header| header.hash()).collect();
    let request = fixture.target_request(&snapshot, tip, &headers, 1);
    // The port records the commit's engine run, so every check that
    // judges engine runs judges it as it does an apply's.
    port.harness_commit(hashes, |engine| {
        let applied = ready(engine.prepare_header_target(request))
            .and_then(|prepared| ready(engine.apply_header_target(prepared)))
            .map_err(|error| format!("committing {len} headers on {tip:?} failed: {error}"))?;
        if !matches!(applied, ApplyHeaderTargetOutcome::Applied) {
            return Err(format!(
                "committing {len} headers on {tip:?} gave {applied:?}"
            ));
        }
        Ok(())
    })?;
    let published = engine.snapshot();
    adoption.publish(published.state_version, published.header_generation);
    Ok(())
}

impl Control {
    /// The next step and its action, if it may be picked at `now`: before
    /// any `Recover` pick, or from its `recovered` pick on, once
    /// [`Control::ready`] and its trigger hold.
    fn next_offer(&self, now: SimInstant) -> Option<(&ControlStep, (Identity, u64))> {
        // From the `Recover` pick the script stops, until the `recovered`
        // pick lets a typed test's closes after it go on.
        if self.recovering() && !self.resumed() {
            return None;
        }
        let index = self.next_step()?;
        let step = &self.script[index];
        // A full prefix has ended, so its `Recover` comes at once: its
        // trigger can wait on a step the cutoff skipped.
        let open = self.ready(step)
            && (self.prefix_full()
                || step
                    .when
                    .as_ref()
                    .is_none_or(|when| self.predicates.borrow().control_holds(when, now)));
        // A script is far shorter than u64.
        open.then(|| {
            (
                step,
                (Identity::peer(&step.peer, step.action.name()), index as u64),
            )
        })
    }
}

impl ActionSource for Control {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        out.extend(self.next_offer(now).map(|(_, action)| action));
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(driver) = self.recovery.as_ref().filter(|_| self.recovering()) {
            driver.boundaries(now, out);
            // The steps after the `Recover` step go on from the `recovered`
            // pick, their triggers' times with them.
            if !self.resumed() {
                return;
            }
        }
        if let Some(when) = self
            .next_step()
            .and_then(|index| self.script[index].when.as_ref())
        {
            self.predicates.borrow().control_boundaries(when, now, out);
        }
    }

    /// Recovery's `recovered` or relink pick, once the task carried out
    /// every step.
    fn quiet(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(driver) = &self.recovery {
            if self.queued.get() == 0 {
                driver.quiet(now, out);
            }
        }
    }

    /// True from the `Recover` pick on, which turns on the fair recovery
    /// policy, unless the scenario declares [`Disruption::Unfair`].
    fn recovering(&self) -> bool {
        Control::recovering(self)
            && !self
                .recovery
                .as_ref()
                .is_some_and(|driver| driver.evidence.disrupts(Disruption::Unfair))
    }

    /// While a `Recover` establishes, its quiet `recovered` pick, under the
    /// same condition as [`ActionSource::quiet`], but no relink or give-up,
    /// and the end of the source's cooldown as a boundary.
    /// Otherwise the next step, if it's the planned `Release` of an owned
    /// hold ([`Topology::own`]) and may be picked as it would be live, with
    /// its trigger's openings. No other script step runs in the drain.
    fn recovery(&self, now: SimInstant, out: &mut RecoveryOffer) {
        if let Some(driver) = self.recovery.as_ref().filter(|_| self.queued.get() == 0) {
            let mut quiet = Vec::new();
            driver.quiet(now, &mut quiet);
            out.quiet
                .extend(quiet.into_iter().filter(|(_, local)| *local == RECOVERED));
            // The end of the source's cooldown, which `recovered` waits for.
            let mut boundaries = Vec::new();
            driver.boundaries(now, &mut boundaries);
            out.boundaries.extend(
                boundaries
                    .into_iter()
                    .filter_map(|boundary| match boundary {
                        Boundary::Opens(at) if at > now => Some(at),
                        Boundary::Opens(_) | Boundary::Deadline(_) => None,
                    }),
            );
        }
        let owned_release = |step: &ControlStep| match step.action {
            ControlAction::Release { hold } => self.topology.owns(hold),
            _ => false,
        };
        if let Some((_, action)) = self.next_offer(now).filter(|(step, _)| owned_release(step)) {
            out.actions.push(action);
        } else if let Some(when) = self
            .script
            .get(self.next)
            .filter(|step| !self.recovering() && owned_release(step))
            .and_then(|step| step.when.as_ref())
        {
            let mut boundaries = Vec::new();
            self.predicates
                .borrow()
                .control_boundaries(when, now, &mut boundaries);
            out.boundaries.extend(
                boundaries
                    .into_iter()
                    .filter_map(|boundary| match boundary {
                        Boundary::Opens(at) => Some(at),
                        Boundary::Deadline(_) => None,
                    }),
            );
        }
    }

    /// Nothing: a queued step still runs on the control task, and the
    /// script holds nothing else.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// A planned `Release` of an owned hold ([`Topology::own`]) that's still
    /// to come behind a step the drain never offers, such as a peer's
    /// growth, so the hold's operation can never finish. A release that's
    /// next waits only on its own trigger, which node work can still meet,
    /// and once a `Recover` is picked every hold retires.
    fn holds_work(&self) -> Option<String> {
        if self.recovering() {
            return None;
        }
        let owned_release = |step: &ControlStep| match step.action {
            ControlAction::Release { hold } => self.topology.owns(hold).then_some(hold),
            _ => None,
        };
        let next = self.script.get(self.next)?;
        if owned_release(next).is_some() {
            return None;
        }
        let hold = self.script[self.next..].iter().find_map(owned_release)?;
        Some(format!(
            "the planned release of owned hold {hold} waits behind the {} step of {}, which \
             the drain doesn't run",
            next.action.name(),
            next.peer
        ))
    }

    /// Checks the next step and queues it for the control task, or carries
    /// out a run-only one. A shutdown, reset, release or storage stop that
    /// doesn't name the node under test, a second shutdown, and a relink,
    /// reset or release after one fail the run here instead. A shutdown
    /// counts as begun from its pick, before the task takes it.
    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        if let Some(driver) = &self.recovery {
            match local {
                RECOVERED => {
                    if let Err(why) = driver.recovered(log.step()) {
                        self.topology.fail(why);
                    }
                    return;
                }
                UNREACHED => {
                    driver.evidence.give_up(log.step());
                    return;
                }
                RELINK => {
                    let step = ControlStep {
                        peer: driver.evidence.relinked(
                            zakura_scheduler::hooks::now()
                                .unwrap_or_default()
                                .saturating_duration_since(SimInstant::from_start(Duration::ZERO)),
                        ),
                        action: ControlAction::Relink,
                        when: None,
                    };
                    self.queued.set(self.queued.get() + 1);
                    self.to_task
                        .try_send(step)
                        .expect("recovery relinks only once the task took every step");
                    return;
                }
                _ => {}
            }
        }
        // `enabled` offered this step's index, which came from a usize.
        let index = local as usize;
        let step = self.script[index].clone();
        self.next = index + 1;
        if let Some(driver) = &self.recovery {
            if step.action != ControlAction::Recover && driver.evidence.in_prefix() {
                driver
                    .evidence
                    .prefix()
                    .spend(log.step(), Hostile::Step(index));
            }
        }
        let node = &self.topology.nodes[0].name;
        let invalid = match step.action {
            ControlAction::Recover if step.peer == *node => {
                Some("a recover names a mirror, not the node under test".to_owned())
            }
            ControlAction::Shutdown
            | ControlAction::Reset(_)
            | ControlAction::Release { .. }
            | ControlAction::StopStorage
            | ControlAction::Commit { .. }
            | ControlAction::RepairStatus { .. }
                if step.peer != *node =>
            {
                Some(format!(
                    "a {} names the node under test, not {}",
                    step.action.name(),
                    step.peer
                ))
            }
            ControlAction::Shutdown if self.shut_down => {
                Some("the node is already shut down".to_owned())
            }
            ControlAction::Flood | ControlAction::Churn if step.peer == *node => Some(format!(
                "a {} names a mirror, not the node under test",
                step.action.name()
            )),
            ControlAction::Relink if self.shut_down => {
                Some(format!("relinking {} after the shutdown", step.peer))
            }
            ControlAction::Flood | ControlAction::Churn if self.shut_down => Some(format!(
                "a {} of {} after the shutdown",
                step.action.name(),
                step.peer
            )),
            ControlAction::Reset(_)
            | ControlAction::Release { .. }
            | ControlAction::Commit { .. }
            | ControlAction::RepairStatus { .. }
                if self.shut_down =>
            {
                Some(format!(
                    "a {} of {} after the shutdown",
                    step.action.name(),
                    step.peer
                ))
            }
            _ => None,
        };
        if let Some(why) = invalid {
            self.topology.fail(why);
            return;
        }
        if step.action.run_only() {
            if let Err(why) = self.run(&step) {
                self.topology.fail(why);
            }
            return;
        }
        if step.action == ControlAction::Shutdown {
            self.shut_down = true;
            self.topology.expect_shutdown();
        }
        self.queued.set(self.queued.get() + 1);
        self.to_task
            .try_send(step)
            .expect("the channel holds the whole script, and the task outlives the run");
    }
}

impl HeaderNode {
    /// The service its links join, for a harness that builds its own
    /// links, such as `sync/storage`'s.
    pub(crate) fn service(&self) -> &HeaderSyncService {
        &self.service
    }

    /// Cancels the reactor, as zakurad's shutdown ends header sync.
    pub(crate) fn cancel(&self) {
        self.shutdown.cancel();
    }
}
