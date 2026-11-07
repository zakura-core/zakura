//! hdr-7b: `header-sync/storage`'s flood capabilities. A `Commit` step
//! puts headers in the node's engine outside header sync, which the reactor
//! hears of only through its watches. A `Flood` step replaces a mirror's
//! link with one whose far end sends the node's `Status` back as fast as
//! its events queue takes it. A `Churn` step relinks a mirror once per
//! scheduling step, so lifecycle events stay queued.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc};

use zakura_sim::{
    harness::{
        header_node::{
            adoption::AdoptionFact, fault::CooldownFact, Chain, ControlAction, ControlStep, Policy,
        },
        header_sync::{
            storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports,
            Storage, StorageScenario, StorageView,
        },
        run_prepared, Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    MemorySink, ObservationLog, PlanInput, RunConfig, RunEnd, RunReport, Sim,
};

/// The live steps a run may take. A relink takes many steps, so churn
/// needs more than the storage default to show [`LOADED_TURNS`] turns.
const BUDGET: u64 = 12_000;

/// Where a [`Flooding`] run leaves its harness, so a test can read the view
/// after the run.
type Slot = Rc<RefCell<Option<Rc<Storage>>>>;

/// [`Storage`], handing itself to a test once built. Test-only, so it isn't
/// registered.
struct Flooding(Rc<Storage>);

impl HasView for Flooding {
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

    /// The storage harness's stamped facts.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Flooding {
    const NAME: &'static str = "test/header-flood";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = (Ports, Slot);

    /// The storage harness's config, with room for [`LOADED_TURNS`] turns
    /// of churn.
    fn config(_: &StorageScenario) -> RunConfig {
        RunConfig {
            budget: BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports.
    fn prepare(scenario: &StorageScenario) -> (Ports, Slot) {
        (Storage::prepare(scenario), Slot::default())
    }

    /// The storage harness, also left in the slot.
    fn build(sim: &mut Sim, scenario: &StorageScenario, (ports, slot): (Ports, Slot)) -> Self {
        let storage = Rc::new(Storage::build(sim, scenario, ports));
        *slot.borrow_mut() = Some(storage.clone());
        Flooding(storage)
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: the scenarios are built by hand.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("flood scenarios aren't drawn")
    }
}

/// `scenario`'s run under `input`, which must end without a violation or a
/// harness failure, and the view after it.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, StorageView) {
    let prepared = Flooding::prepare(scenario);
    let slot = prepared.1.clone();
    let report =
        run_prepared::<Flooding>(scenario, prepared, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let storage = slot.borrow_mut().take().expect("the harness was built");
    (report, storage.view())
}

/// The node at the main chain's 4th header, and one mirror there too, which
/// dials it, with `controls`.
fn scenario(controls: Vec<ControlStep>) -> StorageScenario {
    StorageScenario {
        node: Chain::main(4),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(4),
            dialer: Dialer::Mirror,
        }],
        policy: Policy::converging(),
        controls,
    }
}

/// `action` on `peer`, offered from the run's start.
fn step(peer: &str, action: ControlAction) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: None,
    }
}

/// The node's adoption facts after `view`'s run.
fn facts(view: &StorageView) -> Vec<AdoptionFact> {
    view.adoption
        .facts()
        .into_iter()
        .map(|stamped| stamped.fact)
        .collect()
}

/// How many events the reactor took in each turn it completed, in order: the
/// `EventTaken` facts between consecutive `Pass` facts, starting after the
/// first.
fn events_per_turn(facts: &[AdoptionFact]) -> Vec<usize> {
    let mut turns = Vec::new();
    let mut current: Option<usize> = None;
    for fact in facts {
        match fact {
            AdoptionFact::Pass => {
                if let Some(events) = current.replace(0) {
                    turns.push(events);
                }
            }
            AdoptionFact::EventTaken => {
                if let Some(events) = current.as_mut() {
                    *events += 1;
                }
            }
            _ => {}
        }
    }
    turns
}

/// The turns a flood or churn may take to fill its queue before every
/// turn finds an event.
const FILL_TURNS: usize = 8;

/// The fewest loaded turns a flood or churn run must show.
const LOADED_TURNS: usize = 50;

/// Checks that once `facts` show the queue filled, within [`FILL_TURNS`],
/// every later turn took an event, for at least [`LOADED_TURNS`] turns.
fn loaded(facts: &[AdoptionFact]) {
    let turns = events_per_turn(facts);
    let filled = turns
        .iter()
        .position(|&events| events > 0)
        .unwrap_or_else(|| panic!("no turn took an event: {turns:?}"));
    assert!(filled <= FILL_TURNS, "{turns:?}");
    let after = &turns[filled..];
    assert!(after.len() >= LOADED_TURNS, "{turns:?}");
    assert!(after.iter().all(|&events| events > 0), "{turns:?}");
}

/// A `Commit` step publishes a new state in the node's engine, and the
/// reactor adopts it through its watches after the `Published` fact, never
/// before it.
#[test]
fn a_node_side_commit_is_adopted_after_its_publication() {
    let scenario = scenario(vec![step("node", ControlAction::Commit { len: 2 })]);
    for input in support::inputs(16, 256) {
        let (_, view) = run(&scenario, input);
        let facts = facts(&view);
        let (published, version, generation) = facts
            .iter()
            .enumerate()
            .find_map(|(index, fact)| match fact {
                AdoptionFact::Published {
                    state_version,
                    header_generation,
                } => Some((index, *state_version, *header_generation)),
                _ => None,
            })
            .unwrap_or_else(|| panic!("the commit published nothing: {facts:?}"));
        let adopted = |fact: &AdoptionFact| {
            matches!(fact, AdoptionFact::Adopted(adoption)
                if adoption.state_version >= version)
        };
        assert!(!facts[..published].iter().any(adopted), "{facts:?}");
        let adoption = facts[published..]
            .iter()
            .find_map(|fact| match fact {
                AdoptionFact::Adopted(adoption) if adoption.state_version >= version => {
                    Some(adoption)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("the reactor never adopted the commit: {facts:?}"));
        assert_eq!(adoption.header_generation, generation);
        assert_eq!(adoption.header_best.height.0, 6, "{adoption:?}");
    }
}

/// Under a `Flood` step, the node admits the flooding session, and every
/// reactor turn takes an event once the events queue fills.
#[test]
fn a_wire_flood_keeps_the_events_queue_loaded() {
    let scenario = scenario(vec![step("mirror-0", ControlAction::Flood)]);
    for input in support::inputs(4, 256) {
        let (_, view) = run(&scenario, input);
        loaded(&facts(&view));
        let flood = view.sessions.last().expect("the flood linked a session");
        let cooldowns = view.faults.cooldowns().facts();
        assert!(
            cooldowns.iter().any(|fact| matches!(fact,
                CooldownFact::Admitted { session_id, .. } if *session_id == flood.session_id)),
            "{cooldowns:?}"
        );
    }
}

/// Under a `Churn` step, every reactor turn takes an event once the
/// lifecycle queue fills.
#[test]
fn lifecycle_churn_keeps_the_lifecycle_queue_loaded() {
    let scenario = scenario(vec![step("mirror-0", ControlAction::Churn)]);
    for input in support::inputs(4, 256) {
        let (_, view) = run(&scenario, input);
        loaded(&facts(&view));
        assert!(
            view.sessions.len() > LOADED_TURNS,
            "{}",
            view.sessions.len()
        );
    }
}

/// Without a flood or churn, the reactor has turns that take no event, so
/// the loaded check reads something only the floods give.
#[test]
fn a_quiet_run_has_turns_without_events() {
    let (_, view) = run(&scenario(Vec::new()), support::inputs(1, 256).remove(0));
    let turns = events_per_turn(&facts(&view));
    assert!(turns.contains(&0), "{turns:?}");
}

/// A shutdown picked while churn steps are still queued stops the churn
/// cleanly: the run ends with the node stopped, every property holding and
/// no harness failure, under 32 plans. Before the fix, the control task
/// panicked sending the second churn step to the stopped churn task.
#[test]
fn a_shutdown_during_churn_stops_it_cleanly() {
    let mut scenario = scenario(vec![
        step("mirror-0", ControlAction::Churn),
        step("mirror-1", ControlAction::Churn),
        step("node", ControlAction::Shutdown),
    ]);
    scenario.mirrors.push(MirrorSpec {
        chain: Chain::main(4),
        dialer: Dialer::Mirror,
    });
    for input in support::inputs(32, 256) {
        run(&scenario, input);
    }
}

/// Each capability's run depends only on the plan: the same plan twice
/// gives the same adoption facts and the same log.
#[test]
fn flood_runs_repeat_a_a() {
    let plan = || support::inputs(1, 256).remove(0);
    for action in [
        ("node", ControlAction::Commit { len: 2 }),
        ("mirror-0", ControlAction::Flood),
        ("mirror-0", ControlAction::Churn),
    ] {
        let scenario = scenario(vec![step(action.0, action.1)]);
        let runs: Vec<(String, String)> = (0..2)
            .map(|_| {
                let (report, view) = run(&scenario, plan());
                (
                    format!("{:?}", report.log.entries()),
                    format!("{:?}", facts(&view)),
                )
            })
            .collect();
        assert_eq!(runs[0], runs[1], "{action:?}");
    }
}

/// A `Commit`, `Flood` or `Churn` step that names the wrong actor fails the
/// run as a harness failure that says why.
#[test]
fn misnamed_flood_steps_fail_the_run() {
    for (peer, action, why) in [
        (
            "mirror-0",
            ControlAction::Commit { len: 1 },
            "a commit names the node",
        ),
        ("node", ControlAction::Flood, "a flood names a mirror"),
        ("node", ControlAction::Churn, "a churn names a mirror"),
    ] {
        let scenario = scenario(vec![step(peer, action)]);
        let prepared = Flooding::prepare(&scenario);
        let error = run_prepared::<Flooding>(
            &scenario,
            prepared,
            support::inputs(1, 64).remove(0),
            &mut MemorySink::default(),
            None,
        )
        .expect_err("a misnamed step fails the run");
        assert!(error.to_string().contains(why), "{action:?}: {error}");
    }
}
