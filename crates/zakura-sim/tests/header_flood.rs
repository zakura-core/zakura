//! hdr-7b: `header-sync/storage`'s flood capabilities. A `Commit` step
//! puts headers in the node's engine outside header sync, which the reactor
//! hears of only through its watches. A `Flood` step replaces a mirror's
//! link with one whose far end sends the node's `Status` back as fast as
//! its events queue takes it. A `Churn` step relinks a mirror once per
//! scheduling step, so lifecycle events stay queued.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc};

use zakura_network::zakura::observe::{EventQueue, TakePoint};
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

/// The live steps a run may take. 16 flooding or churning mirrors take
/// many steps a turn, so a flood needs more than the storage default to
/// show 100 turns.
const BUDGET: u64 = 20_000;

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

    /// The storage harness's config, with room for 100 turns of a flood or
    /// churn.
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

/// For each turn the reactor completed, in order, whether its top-of-turn
/// take from `queue` left the queue non-empty: the turns run between
/// consecutive `Pass` facts, starting after the first. Such a take shows
/// `queue` was ready when the turn's `select!` competed with the watch.
fn ready_turns(facts: &[AdoptionFact], queue: EventQueue) -> Vec<bool> {
    let mut turns = Vec::new();
    let mut current: Option<bool> = None;
    for fact in facts {
        match fact {
            AdoptionFact::Pass => {
                if let Some(ready) = current.replace(false) {
                    turns.push(ready);
                }
            }
            AdoptionFact::EventTaken {
                queue: taken,
                point: TakePoint::TurnStart,
                queued,
            } if *taken == queue => {
                if let Some(ready) = current.as_mut() {
                    *ready |= *queued > 0;
                }
            }
            _ => {}
        }
    }
    turns
}

/// How loaded a queue was over a run: the turn it first was ready in,
/// how many turns from then on it was ready in, out of how many, and its
/// longest run of ready turns.
#[derive(Debug)]
struct Load {
    filled: usize,
    ready: usize,
    turns: usize,
    streak: usize,
}

/// How loaded `queue` was over the run `facts` record ([`ready_turns`]).
fn load(facts: &[AdoptionFact], queue: EventQueue) -> Load {
    let turns = ready_turns(facts, queue);
    let filled = turns.iter().position(|&ready| ready).unwrap_or(turns.len());
    let after = &turns[filled..];
    let (mut streak, mut current) = (0, 0);
    for &ready in after {
        current = if ready { current + 1 } else { 0 };
        streak = streak.max(current);
    }
    Load {
        filled,
        ready: after.iter().filter(|&&ready| ready).count(),
        turns: after.len(),
        streak,
    }
}

/// What a flooded queue must show: filled within `fill` turns, then ready
/// in at least `share` percent of at least 100 turns, with a run of at
/// least `streak` ready turns in a row.
struct Loaded {
    fill: usize,
    share: usize,
    streak: usize,
}

impl Loaded {
    /// Whether `load` meets this.
    fn met_by(&self, load: &Load) -> bool {
        load.filled <= self.fill
            && load.turns >= 100
            && load.ready * 100 >= load.turns * self.share
            && load.streak >= self.streak
    }
}

/// 16 mirrors, each flooding or churning as `action` says, with the node.
fn flooded(action: ControlAction) -> StorageScenario {
    let mut flooded = scenario(
        (0..FLOODERS)
            .map(|index| step(&format!("mirror-{index}"), action))
            .collect(),
    );
    flooded.mirrors = vec![
        MirrorSpec {
            chain: Chain::main(4),
            dialer: Dialer::Mirror,
        };
        FLOODERS
    ];
    flooded
}

/// The mirrors a flood or churn test uses. #1566's load test used 63
/// flooding peers. 16 keep each queue loaded here.
const FLOODERS: usize = 16;

/// A `Commit` step publishes a new state in the node's engine, and the
/// reactor reads it from its committed watch and adopts it after the
/// `Published` fact, never before it.
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
        let read = |fact: &AdoptionFact| {
            matches!(fact, AdoptionFact::WatchObserved { state_version }
                if *state_version >= version)
        };
        assert!(!facts[..published].iter().any(adopted), "{facts:?}");
        assert!(!facts[..published].iter().any(read), "{facts:?}");
        assert!(facts[published..].iter().any(read), "{facts:?}");
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

/// Under 16 `Flood` steps, the node admits every flooding session. Once the
/// wire queue fills, each turn's top-of-turn take leaves it non-empty, so
/// it's ready when the `select!` competes with the watch, in at least 98% of
/// turns, with at least 100 ready turns in a row. The lifecycle queue isn't
/// loaded.
#[test]
fn a_wire_flood_keeps_the_events_queue_loaded() {
    let wire = Loaded {
        fill: 32,
        share: 98,
        streak: 100,
    };
    let lifecycle = Loaded {
        fill: 32,
        share: 50,
        streak: 8,
    };
    for input in support::inputs(4, 256) {
        let (_, view) = run(&flooded(ControlAction::Flood), input);
        let facts = facts(&view);
        let load = load(&facts, EventQueue::Wire);
        assert!(wire.met_by(&load), "{load:?}");
        let other = self::load(&facts, EventQueue::Lifecycle);
        assert!(!lifecycle.met_by(&other), "{other:?}");
        let cooldowns = view.faults.cooldowns().facts();
        let admitted = cooldowns
            .iter()
            .filter(|fact| matches!(fact, CooldownFact::Admitted { .. }))
            .count();
        assert!(admitted >= 2 * FLOODERS, "{cooldowns:?}");
    }
}

/// Under 16 `Churn` steps, once the lifecycle queue fills, each turn's
/// top-of-turn take leaves it non-empty in at least 90% of turns, with at
/// least 32 ready turns in a row. The wire queue isn't loaded.
#[test]
fn lifecycle_churn_keeps_the_lifecycle_queue_loaded() {
    let lifecycle = Loaded {
        fill: 32,
        share: 90,
        streak: 32,
    };
    let wire = Loaded {
        fill: 32,
        share: 50,
        streak: 8,
    };
    for input in support::inputs(4, 256) {
        let (_, view) = run(&flooded(ControlAction::Churn), input);
        let facts = facts(&view);
        let load = load(&facts, EventQueue::Lifecycle);
        assert!(lifecycle.met_by(&load), "{load:?}");
        let other = self::load(&facts, EventQueue::Wire);
        assert!(!wire.met_by(&other), "{other:?}");
    }
}

/// Without a flood or churn, the mirror's own messages reach the wire
/// queue, but it empties between them, so neither queue comes near the
/// weakest load a flood test asks for. That's the trickle a flood must be
/// told from.
#[test]
fn a_quiet_run_loads_neither_queue() {
    let weakest = Loaded {
        fill: 32,
        share: 50,
        streak: 8,
    };
    for input in support::inputs(4, 256) {
        let (_, view) = run(&scenario(Vec::new()), input);
        let facts = facts(&view);
        assert!(
            facts.iter().any(|fact| matches!(
                fact,
                AdoptionFact::EventTaken {
                    queue: EventQueue::Wire,
                    ..
                }
            )),
            "{facts:?}"
        );
        for queue in [EventQueue::Wire, EventQueue::Lifecycle] {
            let load = load(&facts, queue);
            assert!(!weakest.met_by(&load), "{queue:?}: {load:?}");
        }
    }
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
