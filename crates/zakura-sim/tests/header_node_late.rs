//! Late completions for a header-sync node on controlled storage (M4-D):
//! late and stale replies after a timeout, a session close or a
//! replacement (M4.3).
//!
//! A storage `Completed` only queues a reply, so callback isolation is read
//! from the node's reactor tap: the events between a call's `CallEnded` and its
//! `Handled`, the call's bracket, are what the reactor did with that reply.
//! An event is ordered against storage facts by its storage watermark: it
//! came after every fact numbered up to its `storage_seq`, and before every
//! later one.
//!
//! Every held operation has its own `deadline_ms`, counted from its
//! submission, chosen so each trigger fires inside it and no replacement
//! request reaches its 30 s request deadline. Each test requires the
//! history it targets, so a trigger that never fires fails the test.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};

use support::inputs;
use zakura_header_chain::Frontier;
use zakura_sim::{
    check::after_step,
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy,
        },
        header_sync::{
            storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports,
            Storage, StorageScenario,
        },
        run_prepared, Subsystem,
    },
    observe::{ReactorCall, ReactorFact, StorageFact, StorageRequest, StorageResult, WorkOwner},
    Checker, MemorySink, Observation, PlanInput, RunConfig, RunReport, Sim,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

// Harness.

/// `header-sync/storage` with its own step budget and worker count, for runs
/// that wait out long holds. Test-only, so it isn't registered.
struct Late(Rc<Storage>);

/// A storage scenario, the run's step budget and its scheduler's workers.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Budgeted {
    storage: StorageScenario,
    budget: u64,
    workers: u16,
}

impl zakura_sim::property::HasView for Late {
    type View = zakura_sim::harness::header_sync::StorageView;

    /// The storage harness's view.
    fn view(&self) -> Self::View {
        self.0.view()
    }
}

impl Subsystem for Late {
    const NAME: &'static str = "test/header-late";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Budgeted;
    type Prepared = Ports;

    /// The storage run's config, with the scenario's budget and workers.
    fn config(scenario: &Budgeted) -> RunConfig {
        let mut config = storage_config();
        config.budget = scenario.budget;
        config.scheduler.workers = scenario.workers;
        config
    }

    /// The storage harness's ports.
    fn prepare(scenario: &Budgeted) -> Ports {
        Storage::prepare(&scenario.storage)
    }

    /// The storage harness, with the node's reactor tapped.
    fn build(sim: &mut Sim, scenario: &Budgeted, ports: Ports) -> Self {
        Late(Rc::new(Storage::build(sim, &scenario.storage, ports)))
    }

    /// The storage harness's properties.
    fn properties() -> Vec<zakura_sim::property::Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> zakura_sim::property::Sources {
        storage_sources()
    }

    /// Unused: these scenarios are only built by hand.
    fn draw(_: &[u8]) -> Budgeted {
        unreachable!("late scenarios aren't drawn")
    }
}

/// Runs `scenario` on `ports` under `input`, with `extra` checking each step
/// after the harness's own checks. The run must end without a violation or
/// harness failure.
fn run_on(
    scenario: &Budgeted,
    ports: &Ports,
    input: PlanInput,
    extra: &mut dyn Checker,
) -> RunReport {
    let report = run_prepared::<Late>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        Some(extra),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    report
}

/// [`run_on`] on freshly prepared ports, with no extra checks.
fn run(scenario: &Budgeted, input: PlanInput) -> (RunReport, Ports) {
    let ports = Late::prepare(scenario);
    let report = run_on(scenario, &ports, input, &mut zakura_sim::NoChecks);
    (report, ports)
}

/// What a checker saw of the node's port after each step, from step 0.
#[derive(Clone, Copy, Debug)]
struct Seen {
    /// Its selected tip.
    tip: Frontier,
    /// Its live leases.
    leases: usize,
}

/// Records [`Seen`] for `port` after every step, and the first step each
/// `Control` action was offered at.
struct Watch {
    seen: Rc<RefCell<Vec<Seen>>>,
    offered: Rc<RefCell<BTreeMap<String, u64>>>,
}

impl Watch {
    /// A watch on `port`, with step 0 recorded, and its checker.
    fn new(port: Arc<ControlledPort>) -> (Self, impl Checker) {
        let seen = |port: &ControlledPort| Seen {
            tip: port.engine().snapshot().frontiers.header_best,
            leases: port.engine().live_leases(),
        };
        let watch = Watch {
            seen: Rc::new(RefCell::new(vec![seen(&port)])),
            offered: Rc::default(),
        };
        let (all, offered) = (watch.seen.clone(), watch.offered.clone());
        let checker = after_step(move |cx| {
            all.borrow_mut().push(seen(&port));
            for enabled in cx.enabled {
                let identity = enabled.identity.to_string();
                if identity.starts_with("peer:") {
                    offered.borrow_mut().entry(identity).or_insert(cx.step);
                }
            }
            None
        });
        (watch, checker)
    }

    /// What was seen after `step`.
    fn at(&self, step: u64) -> Seen {
        self.seen.borrow()[usize::try_from(step).expect("steps fit usize")]
    }

    /// The first step whose pick was made among actions including
    /// `identity`.
    fn first_offered(&self, identity: &str) -> Option<u64> {
        self.offered.borrow().get(identity).copied()
    }
}

/// The reactor's request timeout in a production startup, which every
/// node's startup keeps.
fn request_timeout() -> Duration {
    let fixture = HeaderFixture::regtest();
    let anchor = fixture.anchor();
    zakura_network::zakura::HeaderSyncStartup::new(
        fixture.network().clone(),
        (anchor.height, anchor.hash),
        zakura_network::zakura::FullStateFrontiers {
            finalized_height: anchor.height,
            verified_block_tip: anchor.height,
            verified_block_hash: anchor.hash,
        },
        Some((anchor.height, anchor.hash)),
        zakura_network::zakura::ZakuraHeaderSyncConfig::default(),
        zakura_network::zakura::LOCAL_MAX_MESSAGE_BYTES,
    )
    .request_timeout
}

// Scenarios.

/// The operations of `kind` attributed to `session`.
fn of(kind: OpKind, session: u64) -> Select {
    Select {
        kind: Some(kind),
        session: Some(session),
        ..Select::default()
    }
}

/// A hold on `select` until `until`, then `then`, completing within
/// `deadline_ms` of its submission.
fn hold(select: Select, until: Trigger, then: Allow, deadline_ms: u64) -> Hold {
    Hold {
        select,
        until,
        then: vec![then],
        deadline_ms: Some(deadline_ms),
    }
}

/// A `Control` step on `peer`'s link, offered once `when` holds.
fn control(peer: &str, action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    }
}

/// Closes `mirror-0`'s first session once `close` holds, grows its chain
/// from A5 to A6 once it's closed, then relinks it on a fresh connection as
/// session 2, which offers A6.
fn close_grow_relink(close: Trigger) -> Vec<ControlStep> {
    vec![
        control("mirror-0", ControlAction::Close, close),
        control(
            "mirror-0",
            ControlAction::Grow { len: 1 },
            Trigger::Closed("mirror-0".to_owned()),
        ),
        control(
            "mirror-0",
            ControlAction::Relink,
            Trigger::Grown("mirror-0".to_owned()),
        ),
    ]
}

/// The node at genesis syncing from `mirror-0` at A5, converging apart from
/// `holds`, with `controls`, over `budget` steps.
fn node_and_mirror(holds: Vec<Hold>, controls: Vec<ControlStep>, budget: u64) -> Budgeted {
    Budgeted {
        storage: StorageScenario {
            policy: Policy {
                holds,
                ..Policy::converging()
            },
            controls,
            ..StorageScenario::default()
        },
        budget,
        workers: storage_config().scheduler.workers,
    }
}

/// The fixture's main chain tip at `height`.
fn main_tip(height: u32) -> Frontier {
    Chain::main(height).tip(&HeaderFixture::regtest())
}

/// `hash`'s bytes in hex, as the run log writes hashes.
fn hex(hash: zakura_chain::block::Hash) -> String {
    hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The node's header generation before any run: its port starts at genesis or
/// at a preloaded chain, which `generation` counts.
fn initial_generation(ports: &Ports) -> u64 {
    ports.node.engine().snapshot().header_generation.get()
}

// History.

/// One entry of the node's history.
#[derive(Clone, Copy, Debug)]
enum Item<'r> {
    /// A fact of its storage, with its number.
    Fact(u64, &'r StorageFact),
    /// An event of its reactor, with its storage watermark.
    Event(u64, &'r ReactorFact),
}

/// An [`Item`] with its step and the virtual time then.
#[derive(Clone, Copy, Debug)]
struct Line<'r> {
    step: u64,
    at: Duration,
    item: Item<'r>,
}

/// One of the node's storage operations, at its submission.
#[derive(Clone, Copy, Debug)]
struct Op<'r> {
    kind: OpKind,
    session: Option<u64>,
    request: &'r StorageRequest,
    seq: u64,
    at: Duration,
}

/// An operation's completion.
#[derive(Clone, Copy, Debug)]
struct Done<'r> {
    step: u64,
    seq: u64,
    outcome: &'r str,
    result: &'r StorageResult,
    delivered: bool,
    generation: u64,
}

/// One of the node's port calls, by the indexes of its events in the history.
#[derive(Clone, Copy, Debug)]
struct Call<'r> {
    kind: OpKind,
    session: u64,
    /// Its `CallEnded`: the index, the storage watermark, how it ended and
    /// its paired operation.
    ended: Option<(usize, u64, &'r str, Option<u64>)>,
    handled: Option<usize>,
}

/// A `Retired` event's parts.
#[derive(Clone, Copy, Debug)]
struct Retirement<'r> {
    index: usize,
    storage_seq: u64,
    session: u64,
    owner: &'r WorkOwner,
    phase: &'r str,
    reason: &'r str,
}

/// The node's storage facts and reactor events, in log order.
struct History<'r> {
    lines: Vec<Line<'r>>,
    /// Its engine's header generation before the run.
    initial_generation: u64,
}

impl<'r> History<'r> {
    /// The node's history in `report`, whose port started at
    /// `initial_generation`.
    fn new(report: &'r RunReport, initial_generation: u64) -> Self {
        let mut at = Duration::ZERO;
        let mut lines = Vec::new();
        for entry in report.log.entries() {
            let item = match &entry.obs {
                Observation::TimeAdvanced { to_ns, .. } => {
                    at = Duration::from_nanos(*to_ns);
                    continue;
                }
                Observation::Storage {
                    instance: 0,
                    seq,
                    fact,
                    ..
                } => Item::Fact(*seq, fact),
                Observation::Reactor {
                    storage_seq, event, ..
                } => Item::Event(*storage_seq, event),
                _ => continue,
            };
            lines.push(Line {
                step: entry.step,
                at,
                item,
            });
        }
        History {
            lines,
            initial_generation,
        }
    }

    /// Every storage fact, with its line and number.
    fn facts(&self) -> impl Iterator<Item = (Line<'r>, u64, &'r StorageFact)> + '_ {
        self.lines.iter().filter_map(|line| match line.item {
            Item::Fact(seq, fact) => Some((*line, seq, fact)),
            Item::Event(..) => None,
        })
    }

    /// Every reactor event, with its index, line and storage watermark.
    fn events(&self) -> impl Iterator<Item = (usize, Line<'r>, u64, &'r ReactorFact)> + '_ {
        self.lines
            .iter()
            .enumerate()
            .filter_map(|(index, line)| match line.item {
                Item::Event(storage_seq, event) => Some((index, *line, storage_seq, event)),
                Item::Fact(..) => None,
            })
    }

    /// Every operation, by ID.
    fn ops(&self) -> BTreeMap<u64, Op<'r>> {
        self.facts()
            .filter_map(|(line, seq, fact)| match fact {
                StorageFact::Submitted { id, request, .. } => {
                    let (kind, session) = kind_of(request);
                    Some((
                        *id,
                        Op {
                            kind,
                            session,
                            request,
                            seq,
                            at: line.at,
                        },
                    ))
                }
                _ => None,
            })
            .collect()
    }

    /// Every completion, by operation ID.
    fn completions(&self) -> BTreeMap<u64, Done<'r>> {
        self.facts()
            .filter_map(|(line, seq, fact)| match fact {
                StorageFact::Completed {
                    id,
                    outcome,
                    result,
                    delivered,
                    generation,
                    ..
                } => Some((
                    *id,
                    Done {
                        step: line.step,
                        seq,
                        outcome,
                        result,
                        delivered: *delivered,
                        generation: *generation,
                    },
                )),
                _ => None,
            })
            .collect()
    }

    /// The first fact `pred` accepts, with its line and number.
    fn fact(&self, pred: impl Fn(&StorageFact) -> bool) -> Option<(Line<'r>, u64)> {
        self.facts()
            .find(|(_, _, fact)| pred(fact))
            .map(|(line, seq, _)| (line, seq))
    }

    /// Every port call, by number.
    fn calls(&self) -> BTreeMap<u64, Call<'r>> {
        let mut calls = BTreeMap::new();
        for (index, _, storage_seq, event) in self.events() {
            match event {
                ReactorFact::Dispatched { call, port } => {
                    let (kind, session) = call_kind(port);
                    calls.insert(
                        *call,
                        Call {
                            kind,
                            session,
                            ended: None,
                            handled: None,
                        },
                    );
                }
                ReactorFact::CallEnded { call, end, op } => {
                    let call = calls
                        .get_mut(call)
                        .expect("the tap logs a call's dispatch before its end");
                    call.ended = Some((index, storage_seq, end.as_str(), *op));
                }
                ReactorFact::Handled { call, .. } => {
                    calls
                        .get_mut(call)
                        .expect("the tap logs a call's dispatch before its handling")
                        .handled = Some(index);
                }
                _ => {}
            }
        }
        calls
    }

    /// The call paired with operation `id`, if any.
    fn call_of(&self, id: u64) -> Option<(u64, Call<'r>)> {
        self.calls()
            .into_iter()
            .find(|(_, call)| matches!(call.ended, Some((.., Some(op))) if op == id))
    }

    /// The reactor events strictly between `call`'s `CallEnded` and its
    /// `Handled`, which ran while the reactor processed its completion.
    fn bracket(&self, call: &Call<'_>) -> Vec<&'r ReactorFact> {
        let (ended, ..) = call.ended.expect("the call ended");
        let handled = call.handled.expect("the call was handled");
        self.lines[ended + 1..handled]
            .iter()
            .filter_map(|line| match line.item {
                Item::Event(_, event) => Some(event),
                Item::Fact(..) => None,
            })
            .collect()
    }

    /// The engine's header generation just before fact `seq`: that of the
    /// last completion before it, or the initial one.
    fn generation_before(&self, seq: u64) -> u64 {
        self.facts()
            .take_while(|(_, fact_seq, _)| *fact_seq < seq)
            .filter_map(|(_, _, fact)| match fact {
                StorageFact::Completed { generation, .. } => Some(*generation),
                _ => None,
            })
            .last()
            .unwrap_or(self.initial_generation)
    }

    /// Every `Retired` event.
    fn retirements(&self) -> Vec<Retirement<'r>> {
        self.events()
            .filter_map(|(index, _, storage_seq, event)| match event {
                ReactorFact::Retired {
                    request,
                    phase,
                    reason,
                    ..
                } => Some(Retirement {
                    index,
                    storage_seq,
                    session: request.session_id,
                    owner: &request.owner,
                    phase,
                    reason,
                }),
                _ => None,
            })
            .collect()
    }

    /// Each `Started` event's index and session.
    fn starts(&self) -> Vec<(usize, u64)> {
        self.events()
            .filter_map(|(index, _, _, event)| match event {
                ReactorFact::Started { request, .. } => Some((index, request.session_id)),
                _ => None,
            })
            .collect()
    }
}

/// `request`'s kind and the session it's attributed to, as policies select
/// them.
fn kind_of(request: &StorageRequest) -> (OpKind, Option<u64>) {
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
        StorageRequest::ApplyHeaderTarget { owner, .. } => (OpKind::Apply, Some(owner.session_id)),
        StorageRequest::SelectedChainContains { .. } => (OpKind::Membership, None),
    }
}

/// A port call's kind and the session its work belongs to.
fn call_kind(port: &ReactorCall) -> (OpKind, u64) {
    match port {
        ReactorCall::Locator { session_id, .. } => (OpKind::Locator, *session_id),
        ReactorCall::VctContext { owner, .. } => (OpKind::VctContext, owner.session_id),
        ReactorCall::Prepare { owner, .. } => (OpKind::Prepare, owner.session_id),
        ReactorCall::Apply { owner, .. } => (OpKind::Apply, owner.session_id),
    }
}

/// The only operation of `kind` attributed to `session`.
fn only(ops: &BTreeMap<u64, Op<'_>>, kind: OpKind, session: u64) -> u64 {
    let ids: Vec<u64> = ops
        .iter()
        .filter(|(_, op)| op.kind == kind && op.session == Some(session))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(ids.len(), 1, "one {kind:?} for session {session}: {ids:?}");
    ids[0]
}

/// The `nth` operation of `kind`, from 1.
fn nth(ops: &BTreeMap<u64, Op<'_>>, kind: OpKind, nth: usize) -> Option<u64> {
    ops.iter()
        .filter(|(_, op)| op.kind == kind)
        .nth(nth - 1)
        .map(|(id, _)| *id)
}

/// Checks how `session`'s requests end, and returns how many `Retired`
/// events it has. Each is `target_admitted` inside the bracket of one of the
/// session's apply calls whose operation committed, or `snapshot_obsolete`
/// for an owner whose generation a commit had already passed. Both can end
/// a request whose own apply committed, since the reactor's committed-watch
/// check can run between the commit and the apply's callback. With
/// `not_before`, each also comes after that storage fact.
fn check_ends(history: &History<'_>, session: u64, not_before: Option<u64>) -> usize {
    let calls = history.calls();
    let completions = history.completions();
    let in_committed_apply_bracket = |index: usize| {
        calls.values().any(|call| match (call.ended, call.handled) {
            (Some((ended, .., Some(op))), Some(handled)) => {
                call.kind == OpKind::Apply
                    && call.session == session
                    && ended < index
                    && index < handled
                    && completions
                        .get(&op)
                        .is_some_and(|done| committed(history, done))
            }
            _ => false,
        })
    };
    let mut ends = 0;
    for retired in history.retirements() {
        if retired.session != session {
            continue;
        }
        if let Some(commit) = not_before {
            assert!(
                retired.storage_seq >= commit,
                "session {session} was retired before its own apply committed: {retired:?}"
            );
        }
        match retired.reason {
            "target_admitted" => assert!(
                in_committed_apply_bracket(retired.index),
                "session {session} was admitted outside its committed apply's bracket: \
                 {retired:?}"
            ),
            "snapshot_obsolete" => assert!(
                retired.owner.scope.header_generation
                    < history.generation_before(retired.storage_seq + 1),
                "session {session} was made obsolete with no commit past it: {retired:?}"
            ),
            _ => panic!("session {session} was retired: {retired:?}"),
        }
        ends += 1;
    }
    ends
}

/// Whether the generation after each completion is at least the one
/// before.
fn never_falls(history: &History<'_>) -> bool {
    let generations: Vec<u64> = history
        .completions()
        .values()
        .map(|done| (done.seq, done.generation))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    generations.windows(2).all(|pair| pair[0] <= pair[1])
}

/// Whether `done` committed: `engine`, `Applied`, and one
/// generation past the one just before it.
fn committed(history: &History<'_>, done: &Done<'_>) -> bool {
    done.outcome == "engine"
        && matches!(done.result, StorageResult::Applied)
        && done.generation == history.generation_before(done.seq) + 1
}

// M4.3: late replies after a timeout.

/// The node's first locator is held until its own caller has dropped it and
/// 1 ms more. Time moves only once no task is ready, so by then the reactor
/// has handled the timeout and asked for `mirror-0`'s locator again, and that
/// second locator waits for the first to complete. The reactor's timeout
/// ends the first call `timed-out`, paired with its operation's
/// `CallerDropped` at its submission plus the startup's request timeout.
/// The late completion then reaches no caller, though a newer one waits,
/// and the later locator call, paired with a later operation, starts
/// `mirror-0`'s request inside its bracket.
#[test]
fn timed_out_locator_reply_is_dropped() {
    let timeout = request_timeout();
    let scenario = node_and_mirror(
        vec![
            hold(
                Select::nth(OpKind::Locator, 1),
                Trigger::All(vec![
                    Trigger::CallerDropped(Select::held()),
                    Trigger::VirtualTime(30_001),
                ]),
                Allow::Engine,
                // The caller drops at 30 s, and the hold opens 1 ms later.
                31_000,
            ),
            hold(
                Select::nth(OpKind::Locator, 2),
                Trigger::Completed(Select::nth(OpKind::Locator, 1)),
                Allow::Engine,
                // Submitted at the first one's timeout, which completes
                // within 1 s of it.
                5_000,
            ),
        ],
        Vec::new(),
        4_000,
    );
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let history = History::new(&report, initial_generation(&ports));
        let ops = history.ops();
        let first = nth(&ops, OpKind::Locator, 1).expect("the node submitted a locator");
        let (number, call) = history
            .call_of(first)
            .expect("the first locator's call is paired with it");
        let (_, storage_seq, end, _) = call.ended.expect("a paired call ended");
        assert_eq!(end, "timed-out");
        let (dropped, seq) = history
            .fact(|fact| matches!(fact, StorageFact::CallerDropped { id } if *id == first))
            .expect("the first locator's caller dropped it");
        assert_eq!(storage_seq, seq, "the call ended at its caller's drop");
        assert_eq!(dropped.at - ops[&first].at, timeout);

        let late = history.completions()[&first];
        assert!(late.seq > seq, "it completed after the drop");
        assert!(!late.delivered, "the late reply reached no caller");
        let second = nth(&ops, OpKind::Locator, 2).expect("the node asked for a locator again");
        assert!(
            ops[&second].seq < late.seq && history.completions()[&second].seq > late.seq,
            "a newer locator was waiting at the late completion"
        );

        let (_, later) = history
            .calls()
            .into_iter()
            .find(|(later, call)| {
                *later > number && call.kind == OpKind::Locator && call.session == 1
            })
            .expect("the node asked for mirror-0's locator again");
        let (.., end, op) = later.ended.expect("the later locator call ended");
        assert_eq!(end, "replied");
        assert_eq!(op, Some(second), "paired with the newer operation");
        assert!(
            history.bracket(&later).iter().any(|event| matches!(
                event,
                ReactorFact::Started { request, .. } if request.session_id == 1
            )),
            "the later locator's bracket starts mirror-0's request"
        );
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            main_tip(5)
        );
    }
}

// M4.3: a session closes while its apply is pending.

/// The node's apply for `mirror-0`'s session 1 is held until that session's
/// link has closed and session 2's locator is submitted, then commits. Session
/// 2's applies wait for it. At the close the reactor retires session 1's
/// applying request, but its apply still commits: at its completion the
/// generation, unchanged since its submission, rises by one, and the node's tip
/// is the target, and the reactor handles its reply. Session 2's requests end
/// admitted in their own apply's bracket, or made obsolete by a commit, and the
/// node reaches `mirror-0`'s grown tip A6.
#[test]
fn apply_pending_at_close_still_commits() {
    let scenario = node_and_mirror(
        vec![
            hold(
                of(OpKind::Apply, 1),
                Trigger::All(vec![
                    Trigger::Closed("mirror-0".to_owned()),
                    Trigger::Submitted(Select::nth(OpKind::Locator, 2)),
                ]),
                Allow::Engine,
                // The close, growth and relink take no time, and session 2's
                // locator comes right after.
                20_000,
            ),
            hold(
                of(OpKind::Apply, 2),
                Trigger::Completed(of(OpKind::Apply, 1)),
                Allow::Engine,
                // The old apply completes within 20 s of its submission,
                // which is before this one's.
                25_000,
            ),
        ],
        close_grow_relink(Trigger::Submitted(of(OpKind::Apply, 1))),
        4_000,
    );
    for input in inputs(16, 256) {
        let ports = Late::prepare(&scenario);
        let (watch, mut checker) = Watch::new(ports.node.clone());
        let report = run_on(&scenario, &ports, input, &mut checker);
        drop(checker);
        let history = History::new(&report, initial_generation(&ports));
        let ops = history.ops();
        let completions = history.completions();
        let old = only(&ops, OpKind::Apply, 1);
        let done = completions[&old];
        assert_eq!(
            history.generation_before(done.seq),
            history.generation_before(ops[&old].seq),
            "nothing committed while the old apply was pending"
        );
        assert!(
            committed(&history, &done),
            "the old apply committed: {done:?}"
        );
        assert_eq!(
            watch.at(done.step).tip,
            main_tip(5),
            "the node's tip is its target"
        );
        let (_, call) = history
            .call_of(old)
            .expect("the old apply's call is paired");
        assert_eq!(call.ended.map(|(_, _, end, _)| end), Some("replied"));
        assert!(call.handled.is_some(), "and the reactor handled it");

        let closed = history
            .retirements()
            .into_iter()
            .find(|retired| retired.session == 1)
            .expect("the close retired session 1's request");
        assert!(
            matches!(closed.reason, "disconnected" | "session_replaced")
                && closed.phase == "applying",
            "{closed:?}"
        );
        assert!(
            closed.storage_seq < done.seq,
            "before the old apply completed"
        );

        let second = nth(&ops, OpKind::Locator, 2).expect("session 2 asked for a locator");
        let (_, locator) = history
            .call_of(second)
            .expect("the second locator's call is paired");
        assert_eq!(locator.session, 2, "the second locator is session 2's");

        let commit = ops
            .iter()
            .filter(|(_, op)| op.kind == OpKind::Apply && op.session == Some(2))
            .filter_map(|(id, _)| completions.get(id))
            .find(|done| committed(&history, done))
            .expect("one of session 2's applies committed");
        assert!(commit.seq > done.seq, "after the old apply");
        check_ends(&history, 2, None);
        assert!(never_falls(&history), "the generation never falls");
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            main_tip(6)
        );
    }
}

/// The node's apply for `mirror-0`'s session 1 is held until session 2's apply
/// is submitted, which puts session 2 in Applying, then fails with
/// `local-resource`, which commits nothing. Session 2's apply is held until the
/// reactor has handled the old one. The old failure lands on nothing: session 2
/// started before the old call ended, the old bracket retires and dispatches
/// nothing, the generation doesn't move, and session 2 isn't retired until its
/// own apply commits. Then it ends admitted in its own bracket or obsolete by
/// that commit, and the node reaches A6 without starting another request.
#[test]
fn old_apply_failure_leaves_the_applying_replacement_alone() {
    let scenario = node_and_mirror(
        vec![
            hold(
                of(OpKind::Apply, 1),
                Trigger::Submitted(of(OpKind::Apply, 2)),
                Allow::LocalResource,
                // Session 2 reaches its apply through a locator and a
                // prepare, each within the 5 s bound.
                20_000,
            ),
            hold(
                of(OpKind::Apply, 2),
                Trigger::Handled(of(OpKind::Apply, 1)),
                Allow::Engine,
                // The old apply is handled within 20 s of its submission,
                // which is before this one's. Session 2's request deadline
                // is 30 s from this submission.
                25_000,
            ),
        ],
        close_grow_relink(Trigger::Submitted(of(OpKind::Apply, 1))),
        4_000,
    );
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let history = History::new(&report, initial_generation(&ports));
        let ops = history.ops();
        let completions = history.completions();
        let old = only(&ops, OpKind::Apply, 1);
        let new = only(&ops, OpKind::Apply, 2);
        let (_, old_call) = history
            .call_of(old)
            .expect("the old apply's call is paired");
        let (_, new_call) = history
            .call_of(new)
            .expect("session 2's apply call is paired");
        let (old_end, ..) = old_call.ended.expect("the old call ended");
        let started = history
            .starts()
            .into_iter()
            .filter(|(_, session)| *session == 2)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert!(
            history.bracket(&old_call).iter().all(|event| !matches!(
                event,
                ReactorFact::Retired { .. } | ReactorFact::Dispatched { .. }
            )),
            "the old bracket retires and dispatches nothing: {:?}",
            history.bracket(&old_call)
        );
        assert_eq!(started.len(), 1, "one request for session 2: {started:?}");
        assert!(
            started[0] < old_end,
            "session 2 started before the old call ended"
        );

        let done = completions[&old];
        assert_eq!(done.outcome, "local-resource");
        assert!(
            done.seq > ops[&new].seq,
            "the old apply failed after session 2's apply"
        );
        assert_eq!(
            done.generation,
            history.generation_before(ops[&new].seq),
            "the generation is unchanged at the old failure"
        );
        let commit = completions[&new];
        assert!(
            committed(&history, &commit),
            "session 2's apply committed: {commit:?}"
        );
        assert!(new_call.handled.is_some(), "and was handled");
        assert!(
            check_ends(&history, 2, Some(commit.seq)) > 0,
            "session 2's request ended"
        );
        assert!(
            history
                .starts()
                .iter()
                .all(|(index, _)| *index <= started[0]),
            "no request for mirror-0 after session 2's"
        );
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            main_tip(6)
        );
    }
}

/// The step of the first pick named `identity` after `after`.
fn pick_after(report: &RunReport, after: u64, pred: impl Fn(&str) -> bool) -> Option<u64> {
    report
        .picks
        .iter()
        .find(|pick| pick.step > after && pred(&pick.identity))
        .map(|pick| pick.step)
}

/// The node's reactor task, the first one spawned. Each step that logged one
/// of its reactor's events picked it, which the test checks.
const NODE_REACTOR: &str = "task:header_sync_reactor.1:";

/// `mirror-0`'s link closes as soon as the node's apply has committed.
/// Whichever the reactor takes first, the ready apply or the disconnect, the
/// commit stays: the generation never falls, the node's tip stays A5 from the
/// commit to the end, and no second apply of A5 appears after `mirror-0`
/// relinks. Some plan closes the link before the node's reactor runs again.
#[test]
fn apply_committed_before_close_stays_committed() {
    let mut scenario = node_and_mirror(
        Vec::new(),
        vec![
            control(
                "mirror-0",
                ControlAction::Close,
                Trigger::Completed(Select::nth(OpKind::Apply, 1)),
            ),
            control(
                "mirror-0",
                ControlAction::Relink,
                Trigger::Closed("mirror-0".to_owned()),
            ),
        ],
        4_000,
    );
    // The control task needs a free worker to close the link before the node's
    // reactor runs again, and two can both be busy.
    scenario.workers = 4;
    let a5 = main_tip(5);
    let mut close_first = 0;
    for input in inputs(64, 2048) {
        let ports = Late::prepare(&scenario);
        let (watch, mut checker) = Watch::new(ports.node.clone());
        let report = run_on(&scenario, &ports, input, &mut checker);
        drop(checker);
        let history = History::new(&report, initial_generation(&ports));
        for (_, line, ..) in history.events() {
            let pick = report
                .picks
                .iter()
                .find(|pick| pick.step == line.step)
                .expect("each event's step has a pick");
            assert!(pick.identity.starts_with(NODE_REACTOR), "{pick:?}");
        }
        let ops = history.ops();
        let applies: Vec<u64> = ops
            .iter()
            .filter(|(_, op)| {
                matches!(
                    op.request,
                    StorageRequest::ApplyHeaderTarget { target, .. } if *target == hex(a5.hash)
                )
            })
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(applies.len(), 1, "one apply of A5: {applies:?}");
        let done = history.completions()[&applies[0]];
        assert!(committed(&history, &done), "{done:?}");
        let last = u64::try_from(watch.seen.borrow().len()).expect("steps fit u64") - 1;
        for step in done.step..=last {
            assert_eq!(watch.at(step).tip, a5, "the commit stays after step {step}");
        }
        assert!(never_falls(&history), "the generation never falls");

        let picked = pick_after(&report, done.step, |pick| pick == "peer:mirror-0:close")
            .expect("the close is picked after the commit");
        // The relink is offered from the step after the one whose task
        // closed the link, since `Closed` counts the effect.
        let closed = watch
            .first_offered("peer:mirror-0:relink")
            .expect("the link closed")
            - 1;
        assert!(closed > picked);
        let closer = report
            .picks
            .iter()
            .find(|pick| pick.step == closed)
            .expect("every step has a pick");
        assert!(
            closer.identity.starts_with("task:control."),
            "the control task closed the link: {closer:?}"
        );
        let pipes = report
            .log
            .entries()
            .iter()
            .filter(|entry| {
                entry.step > closed
                    && matches!(&entry.obs, Observation::TaskExited { task, .. } if task.starts_with("header_sync_pipe"))
            })
            .count();
        assert!(
            pipes >= 2,
            "both ends of the closed connection's pipes exit"
        );
        let reactor_between = report.picks.iter().any(|pick| {
            pick.step > done.step && pick.step < closed && pick.identity.starts_with(NODE_REACTOR)
        });
        close_first += usize::from(!reactor_between);
    }
    assert!(
        close_first > 0,
        "no plan closed the link before the node's reactor ran after the commit"
    );
}

// M4.3: a replacement while a prepare is pending.

/// The node's prepare for `mirror-0`'s session 1 is held until session 2's
/// prepare is submitted. `mirror-0` relinks as session 2 once the old prepare
/// is submitted, which replaces session 1. Session 2's prepare and applies are
/// held until the reactor has handled the old prepare, so no newer commit
/// decides the old result. The old result is inert: session 2 started before
/// the old call ended, the old bracket dispatches no apply and retires nothing,
/// no apply ever carries session 1, session 2 isn't retired before its own
/// commit, and the node converges without another request for `mirror-0`.
#[test]
fn prepare_completed_after_replacement_is_inert() {
    let handled = Trigger::Handled(of(OpKind::Prepare, 1));
    let scenario = node_and_mirror(
        vec![
            hold(
                of(OpKind::Prepare, 1),
                Trigger::Submitted(of(OpKind::Prepare, 2)),
                Allow::Engine,
                // The relink takes no time, then session 2 reaches its
                // prepare through a locator within the 5 s bound.
                20_000,
            ),
            hold(
                of(OpKind::Prepare, 2),
                handled.clone(),
                Allow::Engine,
                // The old prepare is handled within 20 s of its submission,
                // which is before this one's. Session 2's request deadline
                // is 30 s from its last page, which precedes this.
                25_000,
            ),
            hold(
                of(OpKind::Apply, 2),
                handled,
                Allow::Engine,
                // Submitted after its prepare, so the trigger has fired.
                5_000,
            ),
        ],
        vec![control(
            "mirror-0",
            ControlAction::Relink,
            Trigger::Submitted(of(OpKind::Prepare, 1)),
        )],
        4_000,
    );
    let timeout = request_timeout();
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let history = History::new(&report, initial_generation(&ports));
        let ops = history.ops();
        let completions = history.completions();
        let old = only(&ops, OpKind::Prepare, 1);
        let (_, old_call) = history
            .call_of(old)
            .expect("the old prepare's call is paired");
        let (old_end, ..) = old_call.ended.expect("the old call ended");
        let relinked = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:mirror-0:relink")
            .expect("mirror-0 relinks");
        let relinked_at = history
            .lines
            .iter()
            .take_while(|line| line.step <= relinked.step)
            .last()
            .map_or(Duration::ZERO, |line| line.at);
        assert!(
            relinked_at - ops[&old].at < timeout,
            "mirror-0 relinked within 30 s of the old prepare"
        );
        let started: Vec<usize> = history
            .starts()
            .into_iter()
            .filter(|(_, session)| *session == 2)
            .map(|(index, _)| index)
            .collect();
        let bracket = history.bracket(&old_call);
        assert!(
            bracket.iter().all(|event| !matches!(
                event,
                ReactorFact::Retired { .. }
                    | ReactorFact::Dispatched {
                        port: ReactorCall::Apply { .. },
                        ..
                    }
            )),
            "the old bracket applies and retires nothing: {bracket:?}"
        );
        assert_eq!(started.len(), 1, "one request for session 2: {started:?}");
        assert!(
            started[0] < old_end,
            "session 2 started before the old call ended"
        );
        assert!(
            !ops.values()
                .any(|op| op.kind == OpKind::Apply && op.session == Some(1)),
            "no apply carries session 1"
        );
        let new = only(&ops, OpKind::Apply, 2);
        let commit = completions[&new];
        assert!(
            committed(&history, &commit),
            "session 2's apply committed: {commit:?}"
        );
        let (_, new_call) = history
            .call_of(new)
            .expect("session 2's apply call is paired");
        assert!(new_call.handled.is_some(), "and was handled");
        assert!(
            check_ends(&history, 2, Some(commit.seq)) > 0,
            "session 2's request ended"
        );
        assert!(
            history
                .starts()
                .iter()
                .all(|(index, _)| *index <= started[0]),
            "no request for mirror-0 after session 2's"
        );
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            main_tip(5)
        );
    }
}

// M4.3: a stale release.

/// The node serves `mirror-0`. Its publisher's read on `mirror-0`'s first
/// lease, L1, fails, so it releases L1, and that release is held until
/// `mirror-0`'s session 2 has acquired. `mirror-0`'s link closes once the
/// release is submitted, then it relinks, and session 2's acquire waits past
/// L1's 30 s idle deadline.
/// That acquire sweeps L1 as idle and leaves one live lease, its own L2.
/// The stale release then completes, ends no lease and leaves the count at
/// one. L2's read waits for that.
#[test]
fn stale_publisher_release_never_frees_a_replacement_lease() {
    let scenario = Budgeted {
        storage: StorageScenario {
            node: Chain::main(3),
            mirrors: vec![MirrorSpec {
                chain: Chain::default(),
                dialer: Dialer::Node,
            }],
            policy: Policy {
                holds: vec![
                    hold(
                        Select::nth(OpKind::Read, 1),
                        Trigger::Submitted(Select::held()),
                        Allow::Unavailable,
                        5_000,
                    ),
                    hold(
                        of(OpKind::Release, 1),
                        Trigger::Completed(of(OpKind::Acquire, 2)),
                        Allow::Engine,
                        // Session 2's acquire is submitted soon after this,
                        // and completes within 35 s of that.
                        60_000,
                    ),
                    hold(
                        of(OpKind::Acquire, 2),
                        Trigger::VirtualTime(31_000),
                        Allow::Engine,
                        35_000,
                    ),
                    hold(
                        of(OpKind::Read, 2),
                        Trigger::Completed(of(OpKind::Release, 1)),
                        Allow::Engine,
                        // Submitted when session 2's acquire completes, which
                        // is what the release waits for.
                        30_000,
                    ),
                ],
                ..Policy::converging()
            },
            controls: vec![
                control(
                    "mirror-0",
                    ControlAction::Close,
                    Trigger::Submitted(of(OpKind::Release, 1)),
                ),
                control(
                    "mirror-0",
                    ControlAction::Relink,
                    Trigger::Closed("mirror-0".to_owned()),
                ),
            ],
        },
        budget: 4_000,
        workers: storage_config().scheduler.workers,
    };
    for input in inputs(16, 256) {
        let ports = Late::prepare(&scenario);
        let (watch, mut checker) = Watch::new(ports.node.clone());
        let report = run_on(&scenario, &ports, input, &mut checker);
        drop(checker);
        let history = History::new(&report, initial_generation(&ports));
        let ops = history.ops();
        let completions = history.completions();
        let acquired = |id: &u64| match completions.get(id).map(|done| done.result) {
            Some(StorageResult::Acquired { lease, .. }) => Some(lease.lease_id),
            _ => None,
        };
        let l1 = ops
            .iter()
            .filter(|(_, op)| op.kind == OpKind::Acquire && op.session == Some(1))
            .find_map(|(id, _)| acquired(id))
            .expect("session 1 acquired L1");
        let release = only(&ops, OpKind::Release, 1);
        assert!(
            matches!(
                ops[&release].request,
                StorageRequest::ReleaseHeaderPath { lease: Some(lease), .. } if lease.lease_id == l1
            ),
            "the held release is L1's"
        );
        let (a2, l2) = ops
            .iter()
            .filter(|(_, op)| op.kind == OpKind::Acquire && op.session == Some(2))
            .find_map(|(id, _)| acquired(id).map(|lease| (*id, lease)))
            .expect("session 2 acquired L2");
        assert_ne!(l1, l2);
        let swept = completions[&a2];
        let ends_at = |step: u64| -> Vec<(u64, &str)> {
            history
                .facts()
                .filter(|(line, ..)| line.step == step)
                .filter_map(|(_, _, fact)| match fact {
                    StorageFact::CapabilityEnded { lease, release } => {
                        Some((lease.lease_id, release.as_str()))
                    }
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            ends_at(swept.step),
            [(l1, "lease-idle-expired")],
            "session 2's acquire swept L1"
        );
        assert_eq!(watch.at(swept.step).leases, 1, "leaving L2");

        let stale = completions[&release];
        assert!(stale.seq > swept.seq, "the stale release completes after");
        assert!(matches!(stale.result, StorageResult::Released), "{stale:?}");
        assert_eq!(ends_at(stale.step), [], "and ends no lease");
        assert_eq!(watch.at(stale.step).leases, 1, "L2 is still live");
        let read = ops
            .iter()
            .find(|(_, op)| {
                matches!(
                    op.request,
                    StorageRequest::ReadHeaderPath { lease: Some(lease), .. } if lease.lease_id == l2
                )
            })
            .map(|(id, _)| *id)
            .expect("mirror-0 read L2");
        assert!(completions[&read].seq > stale.seq, "L2's read waited");
    }
}
