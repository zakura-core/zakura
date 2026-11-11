//! hdr-7a: `header-sync/capacity-release-wakes` on `header-sync/release-wake`,
//! the wake fixtures. Two filling mirrors' rounds use the node's 500 headers
//! of body window with their prepares held, a waiting mirror's offer waits
//! for room, and a released round's leases coming back must get the waiting
//! offer's locator read to storage before the reactor goes quiet. In the
//! deferred fixture, the cached pass the release wants waits for the pass
//! budget's refill first.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    io::Write,
    path::Path,
    path::PathBuf,
    process::Command,
    time::Duration,
};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::NaReason;
use zakura_sim::{
    domain::header_sync::{capacity_release_wakes, CACHED_PASS_REFILL, CAPACITY_RELEASE_WAKES},
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::{
            policy::OpKind,
            wake::{WakeEvidence, WakeFact},
        },
        header_sync::{
            storage_properties, storage_sources, Ports, ReleaseWake, Storage, StorageView,
            WakeScenario,
        },
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest, StorageState},
    property::{CheckEvent, EndViolation, HasView, Lifecycle, Property, Sources},
    record::RecordSink,
    seed,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    Checker, MemorySink, NullSink, Observation, ObservationLog, Outcome, Plan, PlanInput,
    PropertyOutcome, PropertyReport, RunConfig, RunEnd, RunReport, Sim, StepCtx, Violation,
};

/// The committed release input, `tests/fixtures/header-release-wake.bin`:
/// [`WakeScenario::Release`]'s scenario byte, then Plan bytes. Its run's
/// release comes from a completion drained at the top of the reactor's
/// loop, so only the budget's release wake gets the reactor back to the
/// waiting offer. `cargo xtask planted header-sync/capacity-release-wakes`
/// checks that removing that wake violates the property on it.
const RELEASE_INPUT: &[u8] = include_bytes!("fixtures/header-release-wake.bin");

/// The committed deferred input,
/// `tests/fixtures/header-release-wake-deferred.bin`:
/// [`WakeScenario::Deferred`]'s scenario byte, then Plan bytes. The
/// release's cached pass is deferred to the refill, and the planted cases
/// that drop the refill deadline or the refill itself violate the property
/// on it.
const DEFERRED_INPUT: &[u8] = include_bytes!("fixtures/header-release-wake-deferred.bin");

/// A committed input's file stem, its fixture, and how
/// [`regenerate_release_wake_seeds`] picks its Plan from
/// [`support::byte_inputs`]: the stream's count and longest input, then
/// the index.
type Seed = (&'static str, WakeScenario, usize, usize, usize);

/// Every committed input. The release Plan is the first tested one whose
/// release is drained at the top of the loop. Any Plan defers in the
/// deferred fixture.
const SEEDS: [Seed; 2] = [
    ("header-release-wake", WakeScenario::Release, 400, 512, 155),
    (
        "header-release-wake-deferred",
        WakeScenario::Deferred,
        16,
        256,
        0,
    ),
];

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The input [`regenerate_release_wake_seeds`] builds for `seed`: the
/// fixture's scenario byte, then its Plan.
fn generated((_, scenario, count, max_len, index): Seed) -> Vec<u8> {
    let byte = match scenario {
        WakeScenario::Release => 0,
        WakeScenario::Deferred => 1,
    };
    let mut input = vec![byte];
    input.extend(support::byte_inputs(count, max_len).swap_remove(index));
    input
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[ReleaseWake::SCENARIO_BYTES..])
}

/// `scenario`'s run under `input`, and its wake facts.
fn run(scenario: WakeScenario, input: PlanInput) -> (RunReport, Vec<(u64, WakeFact)>) {
    let ports = ReleaseWake::prepare(&scenario);
    let wake = ports.wake.clone();
    let report =
        run_prepared::<ReleaseWake>(&scenario, ports, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    (report, wake.facts())
}

/// Checks that `report`'s run opened a wake for a waiting offer, got that
/// offer's locator read to storage after it, and held
/// `header-sync/capacity-release-wakes`. Returns the facts between the
/// wake and the read.
fn wake_held<'a>(report: &RunReport, facts: &'a [(u64, WakeFact)]) -> &'a [(u64, WakeFact)] {
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let (opened, peer) = facts
        .iter()
        .enumerate()
        .find_map(|(index, (_, fact))| match fact {
            WakeFact::Opened { peer, .. } => Some((index, peer.clone())),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no release opened a wake: {facts:?}"));
    let read = facts[opened..]
        .iter()
        .position(|(_, fact)| matches!(fact, WakeFact::Submitted { peer: read } if *read == peer))
        .unwrap_or_else(|| panic!("the waiting offer's locator reached storage: {facts:?}"));
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "{outcome:?}"
    );
    &facts[opened + 1..opened + read]
}

/// Whether `facts` hold a deferred cached pass.
fn deferred(facts: &[(u64, WakeFact)]) -> bool {
    facts
        .iter()
        .any(|(_, fact)| matches!(fact, WakeFact::Deferred { .. }))
}

/// `header-sync/capacity-release-wakes` holds on `input` through the
/// registered harness, and the run reaches its trigger history. Returns
/// the facts between the wake and the waiting offer's read.
fn committed_input_holds(scenario: WakeScenario, input: &[u8]) -> Vec<(u64, WakeFact)> {
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    let result = (entry.witness)(
        &entry.scenario(&input[..ReleaseWake::SCENARIO_BYTES]),
        plan(input),
        CAPACITY_RELEASE_WAKES,
        Expect::Holds,
        &[],
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
    let (report, facts) = run(scenario, plan(input));
    wake_held(&report, &facts).to_vec()
}

/// The release input holds, and no cached pass is deferred before the
/// waiting offer's read.
#[test]
fn the_release_wake_input_holds_capacity_release_wakes() {
    let between = committed_input_holds(WakeScenario::Release, RELEASE_INPUT);
    assert!(!deferred(&between), "{between:?}");
}

/// The deferred input holds, and the cached pass the release wants is
/// deferred to the refill before the waiting offer's read.
#[test]
fn the_deferred_wake_input_holds_capacity_release_wakes() {
    let between = committed_input_holds(WakeScenario::Deferred, DEFERRED_INPUT);
    assert!(deferred(&between), "{between:?}");
}

/// Each fixture opens a wake, gets the waiting offer's locator read to
/// storage and holds the property under every plan, not only the committed
/// ones. In the deferred fixture a deferral comes between every time. Each
/// run then ends settled ([`stuck`]).
#[test]
fn the_wake_fixtures_hold_under_every_plan() {
    for input in support::inputs(16, 256) {
        let (report, facts) = run(WakeScenario::Release, input.clone());
        wake_held(&report, &facts);
        let held = stuck(WakeScenario::Release, &report);
        assert!(held.is_empty(), "{held:?}");
        let (report, facts) = run(WakeScenario::Deferred, input);
        assert!(deferred(wake_held(&report, &facts)), "{facts:?}");
        let held = stuck(WakeScenario::Deferred, &report);
        assert!(held.is_empty(), "{held:?}");
    }
}

/// What the fixture still holds at `report`'s end. First, each prepare
/// one of `scenario`'s holds picked that didn't complete, or whose reactor
/// call didn't end. Then the work still in flight at the end that has
/// waited longer than the scenario's policy bound: each of the node's
/// reactor calls not yet ended, and each operation the node's storage
/// still holds, with the virtual time it began. An operation no hold
/// picked completes within the bound, so only held work can wait longer.
///
/// An empty list means the fixture holds nothing at the end, not that the
/// node is idle. Its status refreshes keep starting rounds, so work younger
/// than the bound can still be in flight when the budget runs out, and the
/// drain offers no storage completions to finish it. No hold picks that
/// work, so a drain that completes storage operations, as #1840 proposes,
/// can finish it.
fn stuck(scenario: WakeScenario, report: &RunReport) -> Vec<String> {
    let storage = scenario.storage();
    let bound = Duration::from_millis(
        storage
            .policy
            .bound_ms
            .expect("the fixture bounds unheld operations"),
    );
    let picked: Vec<usize> = storage
        .policy
        .holds
        .iter()
        .map(|hold| {
            assert_eq!(hold.select.kind, Some(OpKind::Prepare), "{hold:?}");
            let nth = hold.select.nth.expect("each hold picks one prepare");
            usize::try_from(nth).expect("a u32 fits usize")
        })
        .collect();
    let mut now = Duration::ZERO;
    let mut calls = BTreeMap::new();
    let mut submitted = HashMap::new();
    let mut prepares = Vec::new();
    let mut completed = HashSet::new();
    let mut call_ended = HashSet::new();
    let mut taps = HashSet::new();
    let mut storages = HashSet::new();
    let mut held = Vec::new();
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = Duration::from_nanos(*to_ns),
            Observation::Reactor {
                instance, event, ..
            } => {
                taps.insert(*instance);
                match event {
                    ReactorFact::Dispatched { call, port } => {
                        calls.insert(*call, (now, format!("call {call}: {port:?}")));
                    }
                    ReactorFact::CallEnded { call, op, .. } => {
                        calls.remove(call);
                        call_ended.extend(*op);
                    }
                    _ => {}
                }
            }
            Observation::Storage {
                service,
                instance,
                fact,
                ..
            } => {
                storages.insert((service.clone(), *instance));
                match fact {
                    StorageFact::Submitted { id, request, .. } => {
                        submitted.insert(*id, now);
                        if matches!(request, StorageRequest::PrepareHeaderTarget { .. }) {
                            prepares.push(*id);
                        }
                    }
                    StorageFact::Completed { id, .. } => {
                        completed.insert(*id);
                    }
                    _ => {}
                }
            }
            Observation::StorageState {
                service,
                instance,
                state,
                ..
            } => {
                storages.insert((service.clone(), *instance));
                if let StorageState::Header(state) = state {
                    held = state.operations.clone();
                }
            }
            _ => {}
        }
    }
    // A call's ID is its tap's, and an operation's is its storage's, which
    // `(service, instance)` names. So bare IDs name the node's calls and
    // operations only while the node's are the one tap and the one storage
    // the run logs.
    assert_eq!(taps.len(), 1, "{taps:?}");
    assert!(
        storages.len() == 1 && storages.iter().all(|(service, _)| service == "header"),
        "{storages:?}"
    );
    let unsettled_holds = picked
        .into_iter()
        .filter_map(|nth| prepares.get(nth - 1))
        .filter(|id| !(completed.contains(*id) && call_ended.contains(*id)))
        .map(|id| {
            format!(
                "held prepare {id}: completed {}, call ended {}",
                completed.contains(id),
                call_ended.contains(id)
            )
        });
    let ops = held.into_iter().map(|op| {
        let since = *submitted
            .get(&op.id)
            .expect("storage logs each operation's submission before its state");
        (since, format!("operation {}: {op:?}", op.id))
    });
    let waited = calls
        .into_values()
        .chain(ops)
        .filter(|(since, _)| now.saturating_sub(*since) > bound)
        .map(|(since, work)| format!("since {since:?}, at {now:?}: {work}"));
    unsettled_holds.chain(waited).collect()
}

/// `header-sync/release-wake` as it was before it let `mirror-1`'s held
/// prepare run: the same storage scenario, budget, ports, properties and
/// sources, without the fixture's settle source.
struct Unsettled(Storage);

impl HasView for Unsettled {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The inner harness's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The storage harness's wake facts' stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Unsettled {
    const NAME: &'static str = "test/release-wake-unsettled";
    const GENERATOR: u32 = ReleaseWake::GENERATOR;
    const SCENARIO_BYTES: usize = ReleaseWake::SCENARIO_BYTES;
    type Scenario = WakeScenario;
    type Prepared = Ports;

    /// [`ReleaseWake`]'s.
    fn config(scenario: &WakeScenario) -> RunConfig {
        ReleaseWake::config(scenario)
    }

    /// [`ReleaseWake`]'s.
    fn prepare(scenario: &WakeScenario) -> Ports {
        ReleaseWake::prepare(scenario)
    }

    /// The storage harness on the fixture's scenario, alone.
    fn build(sim: &mut Sim, scenario: &WakeScenario, ports: Ports) -> Self {
        Unsettled(Storage::build(sim, &scenario.storage(), ports))
    }

    /// [`ReleaseWake`]'s, in its order.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(capacity_release_wakes());
        properties
    }

    /// [`ReleaseWake`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`ReleaseWake`]'s.
    fn draw(prefix: &[u8]) -> WakeScenario {
        ReleaseWake::draw(prefix)
    }
}

/// The pick identity of the fixture's release of `mirror-1`'s hold.
const SETTLE: &str = "peer:node:settle";

/// What a run of the wake fixture showed about its measured boundary and
/// the release after it. The measured boundary is where
/// `header-sync/capacity-release-wakes` concludes the obligation each wake
/// opens, discharged or expired.
struct Measured {
    /// The run's wake facts.
    wake: WakeEvidence,
    /// The property's obligations open after the last step.
    open: usize,
    /// The property's lifecycle events this step, which the registry hands
    /// over before [`Measured::after_step`] sees the step.
    pending: Vec<Lifecycle>,
    /// Each obligation the property opened or concluded, with its step.
    lifecycle: Vec<(u64, Lifecycle)>,
    /// Each step's enabled set, as a digest of its identities in order.
    enabled: Vec<u64>,
    /// The first step whose enabled set offered [`SETTLE`].
    offered: Option<Offered>,
    /// The step and virtual time of each [`SETTLE`] pick.
    settles: Vec<(u64, Duration)>,
}

/// The first offer of [`SETTLE`] in a run.
struct Offered {
    /// The step whose enabled set held it.
    step: u64,
    /// The property's obligations open when that set was built.
    open: usize,
    /// The wake facts found by then.
    facts: Vec<(u64, WakeFact)>,
}

impl Checker for Measured {
    /// Notes the first offer of [`SETTLE`] against the obligations the
    /// last step left open, then the step's own lifecycle events and pick.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let identities: Vec<String> = cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .collect();
        let mut digest = DefaultHasher::new();
        identities.hash(&mut digest);
        self.enabled.push(digest.finish());
        let offers = identities.iter().any(|identity| identity == SETTLE);
        if offers && self.offered.is_none() {
            self.offered = Some(Offered {
                step: cx.step,
                open: self.open,
                facts: self.wake.facts(),
            });
        }
        for to in std::mem::take(&mut self.pending) {
            match to {
                Lifecycle::Obliged => self.open += 1,
                Lifecycle::Discharged | Lifecycle::Expired => {
                    assert!(self.open > 0, "an obligation concludes only once it's open");
                    self.open -= 1;
                }
                Lifecycle::Applicable => continue,
            }
            self.lifecycle.push((cx.step, to));
        }
        if cx.pick.to_string() == SETTLE {
            self.settles.push((cx.step, cx.now.since_start()));
        }
        None
    }

    /// Reports nothing of its own.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }

    /// Keeps the property's lifecycle events for
    /// [`Measured::after_step`].
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.pending
            .extend(events.iter().filter_map(|event| match event {
                CheckEvent::Lifecycle { property, to }
                    if property.as_str() == CAPACITY_RELEASE_WAKES =>
                {
                    Some(*to)
                }
                _ => None,
            }));
    }

    /// Nothing: it keeps no obligations.
    fn unfinished(&self) -> zakura_sim::Unfinished {
        zakura_sim::Unfinished::default()
    }
}

/// `scenario`'s run through harness `S` under `input`, measured.
fn measured<S>(scenario: WakeScenario, input: PlanInput) -> (RunReport, Measured)
where
    S: Subsystem<Scenario = WakeScenario, Prepared = Ports>,
{
    let ports = S::prepare(&scenario);
    let mut measured = Measured {
        wake: ports.wake.clone(),
        open: 0,
        pending: Vec::new(),
        lifecycle: Vec::new(),
        enabled: Vec::new(),
        offered: None,
        settles: Vec::new(),
    };
    let report = run_prepared::<S>(
        &scenario,
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut measured),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    (report, measured)
}

/// hf-5: the fixture releases `mirror-1`'s hold only after its measured
/// boundary. It first offers the release once the clock is past every
/// opened wake's deadline, when no wake obligation is open and at least
/// one has concluded. Before that step each run is the run of the fixture
/// without the release ([`Unsettled`]), step for step: the same picks
/// from the same enabled sets, the same observations, and the same
/// obligations opened and concluded at the same steps. The release is
/// picked once, and the run then ends settled ([`stuck`]). The fixture
/// without it keeps `mirror-1`'s prepare held through the live phase, and
/// only its drain, which retires the hold the fixture doesn't own, runs it.
#[test]
fn the_fixtures_settle_after_their_measured_boundary_and_not_before() {
    let committed = [
        (WakeScenario::Release, plan(RELEASE_INPUT)),
        (WakeScenario::Deferred, plan(DEFERRED_INPUT)),
    ];
    let plans = support::inputs(3, 256).into_iter().flat_map(|input| {
        [
            (WakeScenario::Release, input.clone()),
            (WakeScenario::Deferred, input),
        ]
    });
    for (scenario, input) in committed.into_iter().chain(plans) {
        let (settled, after) = measured::<ReleaseWake>(scenario, input.clone());
        let (unsettled, before) = measured::<Unsettled>(scenario, input);
        let Some(Offered {
            step: offered,
            open,
            facts,
        }) = &after.offered
        else {
            panic!(
                "{scenario:?}: the release was offered: {:?}",
                after.lifecycle
            );
        };
        assert_eq!(*open, 0, "{scenario:?}: no wake is open at the offer");
        let earlier = |measured: &Measured| {
            measured
                .lifecycle
                .iter()
                .filter(|(step, _)| step < offered)
                .copied()
                .collect::<Vec<_>>()
        };
        let concluded = earlier(&after);
        assert!(
            concluded
                .iter()
                .any(|(_, to)| matches!(to, Lifecycle::Discharged | Lifecycle::Expired)),
            "{scenario:?}: a wake concluded before the offer: {concluded:?}"
        );
        assert_eq!(concluded, earlier(&before), "{scenario:?}");
        // The wakes themselves, each with its peer, session, time and stamp.
        let unsettled_facts = before.wake.facts();
        assert_eq!(
            unsettled_facts.get(..facts.len()),
            Some(&facts[..]),
            "{scenario:?}"
        );
        assert!(before.offered.is_none(), "{scenario:?}");
        let index = usize::try_from(offered - 1).expect("a run's steps fit usize");
        assert_eq!(
            settled.picks[..index],
            unsettled.picks[..index],
            "{scenario:?}"
        );
        assert_eq!(
            after.enabled[..index],
            before.enabled[..index],
            "{scenario:?}"
        );
        let prefix = |report: &RunReport| {
            report
                .log
                .entries()
                .iter()
                .take_while(|entry| entry.step < *offered)
                .cloned()
                .collect::<Vec<_>>()
        };
        assert!(prefix(&settled) == prefix(&unsettled), "{scenario:?}");

        let [(settle, settle_at)] = after.settles[..] else {
            panic!("{scenario:?}: one settle: {:?}", after.settles);
        };
        assert_eq!(before.settles, [], "{scenario:?}");
        assert!(*offered <= settle, "{scenario:?}");
        let opened = facts
            .iter()
            .filter_map(|(_, fact)| match fact {
                WakeFact::Opened { at, .. } => Some(at.since_start()),
                _ => None,
            })
            .max()
            .expect("a wake opened before the offer");
        assert!(
            settle_at > opened + CACHED_PASS_REFILL,
            "{scenario:?}: opened at {opened:?}, settled at {settle_at:?}"
        );
        let held = stuck(scenario, &settled);
        assert!(held.is_empty(), "{scenario:?}: {held:?}");
        let drained = unsettled
            .picks
            .iter()
            .filter(|pick| pick.phase == zakura_sim::check::Phase::Drain)
            .any(|pick| pick.identity.starts_with("storage:"));
        assert!(
            drained,
            "{scenario:?}: without the release, only the drain completes the held prepare"
        );
    }
}

/// A fixture run without the wake declared opens no wake, so the property
/// is not applicable there. Nothing then releases `mirror-1`'s held
/// prepare, and the run still ends without a harness failure: a wake that
/// never opens is the node's to answer.
#[test]
fn an_undeclared_fixture_opens_no_wake() {
    let scenario = WakeScenario::Release;
    let ports = Ports {
        wake: WakeEvidence::default(),
        ..ReleaseWake::prepare(&scenario)
    };
    let wake = ports.wake.clone();
    let report = run_prepared::<ReleaseWake>(
        &scenario,
        ports,
        plan(RELEASE_INPUT),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(wake.facts(), []);
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(
            outcome,
            Some(PropertyOutcome::NotApplicable {
                reason: NaReason::NeverApplied
            })
        ),
        "{outcome:?}"
    );
}

/// `header-sync/release-wake` with its live budget cut at the scenario's
/// step count instead of the fixture's, so a test can end the live phase at
/// a chosen point of the fixture and leave the rest to the drain.
struct CutWake(ReleaseWake);

impl HasView for CutWake {
    type View = StorageView;

    /// The fixture's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The fixture's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The fixture's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The fixture's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The fixture's wake facts' stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for CutWake {
    const NAME: &'static str = "test/release-wake-cut";
    const GENERATOR: u32 = ReleaseWake::GENERATOR;
    const SCENARIO_BYTES: usize = ReleaseWake::SCENARIO_BYTES;
    /// The fixture, and the live steps before the drain.
    type Scenario = (WakeScenario, u64);
    type Prepared = Ports;

    /// [`ReleaseWake`]'s, with the scenario's budget.
    fn config((scenario, budget): &(WakeScenario, u64)) -> RunConfig {
        RunConfig {
            budget: *budget,
            ..ReleaseWake::config(scenario)
        }
    }

    /// [`ReleaseWake`]'s.
    fn prepare((scenario, _): &(WakeScenario, u64)) -> Ports {
        ReleaseWake::prepare(scenario)
    }

    /// [`ReleaseWake`]'s fixture, with its settle source.
    fn build(sim: &mut Sim, (scenario, _): &(WakeScenario, u64), ports: Ports) -> Self {
        CutWake(ReleaseWake::build(sim, scenario, ports))
    }

    /// [`ReleaseWake`]'s, in its order.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(capacity_release_wakes());
        properties
    }

    /// [`ReleaseWake`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// The fixture's scenario, at its own budget.
    fn draw(prefix: &[u8]) -> (WakeScenario, u64) {
        let scenario = ReleaseWake::draw(prefix);
        (scenario, ReleaseWake::config(&scenario).budget)
    }
}

/// `scenario` cut after `budget` live steps, under `input`, measured.
fn cut(scenario: WakeScenario, budget: u64, input: PlanInput) -> (RunReport, Measured) {
    let ports = CutWake::prepare(&(scenario, budget));
    let mut measured = Measured {
        wake: ports.wake.clone(),
        open: 0,
        pending: Vec::new(),
        lifecycle: Vec::new(),
        enabled: Vec::new(),
        offered: None,
        settles: Vec::new(),
    };
    let report = run_prepared::<CutWake>(
        &(scenario, budget),
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut measured),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    (report, measured)
}

/// The step at which `measured`'s run opened its first wake after step
/// `after`.
fn wake_after(measured: &Measured, after: u64) -> u64 {
    measured
        .lifecycle
        .iter()
        .find(|(step, to)| *to == Lifecycle::Obliged && *step > after)
        .map(|(step, _)| *step)
        .expect("the fixture opens a wake")
}

/// The step at which `report`'s run picked `identity`.
fn picked(report: &RunReport, identity: &str) -> u64 {
    report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .map(|pick| pick.step)
        .unwrap_or_else(|| panic!("the run picks {identity}"))
}

/// The completion outcome of the run's first prepare, the one the fixture's
/// first hold plans to fail with a local fault.
fn first_prepare_outcome(report: &RunReport) -> Option<String> {
    let id = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    },
                ..
            } => Some(*id),
            _ => None,
        })?;
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Completed {
                        id: done, outcome, ..
                    },
                ..
            } if *done == id => Some(outcome.clone()),
            _ => None,
        })
}

/// C1's history: the live budget ends after the release's wake opens and
/// before its cached-pass refill. The drain finishes the round: the wake concludes
/// and holds, and only then, past the wake's deadline, does `mirror-1`'s
/// held prepare run, so the run ends with nothing held.
#[test]
fn a_budget_cut_between_a_wake_and_its_refill_holds_the_wake() {
    for (scenario, input) in [
        (WakeScenario::Release, RELEASE_INPUT),
        (WakeScenario::Deferred, DEFERRED_INPUT),
    ] {
        let (report, full) = measured::<ReleaseWake>(scenario, plan(input));
        let opened = wake_after(&full, picked(&report, "peer:node:release"));
        let (report, measured) = cut(scenario, opened, plan(input));
        assert_eq!(
            report.end.outcome,
            Outcome::Budget,
            "{scenario:?}: {:?}",
            report.end
        );
        let outcome = report
            .properties
            .iter()
            .find(|property| property.property == CAPACITY_RELEASE_WAKES)
            .map(|property| property.outcome.clone());
        assert!(
            matches!(outcome, Some(PropertyOutcome::Held { .. })),
            "{scenario:?}: {outcome:?}"
        );
        let [(settle, settle_at)] = measured.settles[..] else {
            panic!("{scenario:?}: one settle: {:?}", measured.settles);
        };
        assert!(settle > opened, "{scenario:?}: settled in the drain");
        let concluded = measured
            .lifecycle
            .iter()
            .find(|(step, to)| {
                *step >= opened && matches!(to, Lifecycle::Discharged | Lifecycle::Expired)
            })
            .map(|(step, _)| *step)
            .expect("the release's wake concluded");
        assert!(
            concluded > opened && concluded < settle,
            "{scenario:?}: the wake concluded in the drain, before the settle"
        );
        let wake_opened = measured
            .wake
            .facts()
            .iter()
            .find_map(|(_, fact)| match fact {
                WakeFact::Opened { at, .. } => Some(at.since_start()),
                _ => None,
            })
            .expect("a wake opened");
        assert!(
            settle_at > wake_opened + CACHED_PASS_REFILL,
            "{scenario:?}: settled at {settle_at:?}, past the deadline"
        );
        let held = stuck(scenario, &report);
        assert!(held.is_empty(), "{scenario:?}: {held:?}");
    }
}

/// The fixture's first prepare is planned to fail with a local fault after
/// its `Release` step. A cut just before that step, at it, or after it
/// keeps both the fault and the order: the drain runs the planned release,
/// then the fault, as the full run does. A cut before `mirror-2`'s growth,
/// which the drain never runs, strands the release behind it, which fails
/// the run as source evidence rather than leave the prepare held.
#[test]
fn the_planned_prepare_keeps_its_local_fault_whatever_the_cut() {
    let (full, _) = measured::<ReleaseWake>(WakeScenario::Release, plan(RELEASE_INPUT));
    assert_eq!(
        first_prepare_outcome(&full).as_deref(),
        Some("local-resource")
    );
    let release = picked(&full, "peer:node:release");
    for budget in [release - 1, release, release + 1] {
        let (report, _) = cut(WakeScenario::Release, budget, plan(RELEASE_INPUT));
        assert_eq!(
            report.end.harness, None,
            "cut at {budget}: {:?}",
            report.end
        );
        assert_eq!(
            first_prepare_outcome(&report).as_deref(),
            Some("local-resource"),
            "cut at {budget}"
        );
    }
    let grow = picked(&full, "peer:mirror-2:grow");
    let scenario = (WakeScenario::Release, grow - 1);
    let error = run_prepared::<CutWake>(
        &scenario,
        CutWake::prepare(&scenario),
        plan(RELEASE_INPUT),
        &mut MemorySink::default(),
        None,
    )
    .expect_err("the cut strands the release");
    let HarnessError::Run { failure, .. } = error else {
        panic!("{error}");
    };
    assert!(
        matches!(
            &failure,
            zakura_sim::HarnessFailure::SourceEvidence { detail, .. }
                if detail.starts_with("the planned release of owned hold 0")
        ),
        "{failure}"
    );
}

/// The scenario byte picks the fixture by bit 0 alone.
#[test]
fn the_scenario_byte_picks_the_fixture() {
    for byte in 0..=u8::MAX {
        let expected = if byte & 1 == 1 {
            WakeScenario::Deferred
        } else {
            WakeScenario::Release
        };
        assert_eq!(ReleaseWake::draw(&[byte]), expected, "{byte}");
    }
}

/// The wake facts, the property's report and the full log depend only on
/// the plan: the committed release plan A twice, then B, then A again,
/// gives A's every time, and B's log differs.
#[test]
fn wake_evidence_repeats_a_a_and_a_b_a() {
    let plans: [&[u8]; 2] = [&RELEASE_INPUT[ReleaseWake::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, facts) = run(WakeScenario::Release, Plan::from_fuzz(plan));
            let property = report
                .properties
                .iter()
                .find(|property| property.property == CAPACITY_RELEASE_WAKES)
                .expect("the property reports");
            (
                format!("{:?}", report.log.entries()),
                format!("{facts:?}\n{property:?}"),
            )
        })
        .collect();
    assert!(runs[0].1.contains("Opened"), "{}", runs[0].1);
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_wake_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_RELEASE_WAKE_TRACE";

thread_local! {
    /// The wake facts of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<WakeEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/release-wake`'s own run, keeping its wake facts in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: WakeScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = ReleaseWake::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.wake.clone()));
    run_prepared::<ReleaseWake>(&scenario, ports, input, sink, None)
}

/// The wake facts [`capture_run`] last kept, which must open a wake, as
/// text.
fn captured() -> String {
    let wake = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(wake.any_opened(), "{:?}", wake.facts());
    format!("{:?}\n", wake.facts())
}

/// Each committed input, recorded in this process and replayed exactly in
/// a fresh process of the same test binary, finds the same wake facts with
/// the same stamps.
#[test]
fn the_wake_witnesses_replay_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(ReleaseWake::NAME).expect("the harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        // The parent reads the wake facts from the child's stdout.
        writeln!(std::io::stdout(), "WAKE-BEGIN\n{}WAKE-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    for input in [RELEASE_INPUT, DEFERRED_INPUT] {
        let dir = TempDir::new().expect("a temporary directory");
        record(&entry, RecordInput::Input(input), dir.path()).expect("the witness records");
        let recorded = captured();
        let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "the_wake_witnesses_replay_in_a_fresh_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, dir.path())
            .output()
            .expect("the test binary runs");
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(child.status.success(), "{child:?}");
        let replayed = stdout
            .split_once("WAKE-BEGIN\n")
            .and_then(|(_, rest)| rest.split_once("WAKE-END"))
            .map(|(wake, _)| wake)
            .unwrap_or_else(|| panic!("the child prints its wake facts: {stdout}"));
        assert_eq!(replayed, recorded);
    }
}

/// Each committed input is its [`generated`] input, and runs through the
/// registered harness to a defined end that the explorer classifies clean.
#[test]
fn the_committed_inputs_are_their_plans_and_run_clean() {
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    for (seed, committed) in SEEDS.into_iter().zip([RELEASE_INPUT, DEFERRED_INPUT]) {
        let stem = seed.0;
        assert_eq!(
            committed,
            generated(seed),
            "{stem}: the committed input changed; regenerate it if that's intended"
        );
        assert_eq!(ReleaseWake::draw(committed), seed.1, "{stem}");
        let run = (entry.run_bytes)(committed, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{stem}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
    }
}

/// Rewrites the committed inputs and their sidecars. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_release_wake -- --ignored regenerate_release_wake_seeds`.
#[test]
#[ignore = "rewrites the committed inputs"]
fn regenerate_release_wake_seeds() {
    for seed in SEEDS {
        let (stem, scenario, ..) = seed;
        let input = generated(seed);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let sidecar = format!(
            "# The committed fuzz input `{stem}.bin`: the harness that runs it, the\n\
             # generator version it was built for, and its SHA-256. Its first byte\n\
             # picks the wake fixture, then come the Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_release_wake.rs\"\n\
             \n\
             [[witness]]\n\
             property = \"{CAPACITY_RELEASE_WAKES}\"\n\
             expect = \"holds\"\n",
            harness = ReleaseWake::NAME,
            generator = ReleaseWake::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(scenario).expect("scenarios serialize")
            ),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
