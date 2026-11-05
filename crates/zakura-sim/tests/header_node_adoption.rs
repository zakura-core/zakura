//! hdr-4's adopted-state facts on `header-sync/storage`: the snapshot the
//! node's reactor adopts, the `Status` it queues or defers from it, and the
//! tip it publishes, kept in the tap's view-side evidence store and judged by
//! the generation, status and tip rules.
//!
//! The witness holds the node's first locator read, taken under its starting
//! generation for `mirror-1`'s target, while `mirror-0` grows and the node
//! applies the growth under a new generation. Released after that, the read
//! settles a target the adoption made obsolete, which must not start a round.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, process::Command, rc::Rc};

use support::inputs;
use zakura_sim::{
    harness::{
        header_node::{
            adoption::{Acceptance, AdoptionEvidence, AdoptionFact},
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy,
        },
        header_sync::{
            storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports,
            Storage, StorageScenario, StorageView,
        },
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    property::{HasView, Property, Sources},
    MemorySink, Observation, PlanInput, RunConfig, RunReport, Sim,
};

/// [`Storage`], keeping the last view its checks read, so a test can judge
/// the evidence after the run.
struct Adopting {
    storage: Storage,
    last: Rc<RefCell<Option<StorageView>>>,
}

/// A storage scenario and, if set, the run's step budget.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Budgeted {
    storage: StorageScenario,
    budget: Option<u64>,
}

/// The ports, and where the harness leaves its last view.
#[derive(Clone)]
struct Ready {
    ports: Ports,
    last: Rc<RefCell<Option<StorageView>>>,
}

impl HasView for Adopting {
    type View = StorageView;

    /// The storage harness's view, also kept as the last one read.
    fn view(&self) -> StorageView {
        let view = self.storage.view();
        *self.last.borrow_mut() = Some(view.clone());
        view
    }
}

impl Subsystem for Adopting {
    const NAME: &'static str = "test/header-adoption";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Budgeted;
    type Prepared = Ready;

    /// The storage harness's config, with the scenario's budget if set.
    fn config(scenario: &Budgeted) -> RunConfig {
        let config = storage_config();
        RunConfig {
            budget: scenario.budget.unwrap_or(config.budget),
            ..config
        }
    }

    /// The storage harness's ports.
    fn prepare(scenario: &Budgeted) -> Ready {
        Ready {
            ports: Storage::prepare(&scenario.storage),
            last: Rc::default(),
        }
    }

    /// The storage harness.
    fn build(sim: &mut Sim, scenario: &Budgeted, ready: Ready) -> Self {
        Adopting {
            storage: Storage::build(sim, &scenario.storage, ready.ports),
            last: ready.last,
        }
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: these scenarios are built by hand.
    fn draw(_: &[u8]) -> Budgeted {
        unreachable!("adoption scenarios aren't drawn")
    }
}

/// A run's report and the last view its checks read.
struct Run {
    report: RunReport,
    view: StorageView,
}

impl Run {
    /// The node's adoption evidence.
    fn adoption(&self) -> &AdoptionEvidence {
        &self.view.adoption
    }

    /// The status rule over the node's sessions.
    fn status(&self) -> Acceptance {
        self.adoption().status(&self.view.sessions)
    }
}

/// Runs `scenario` under `input` with the storage harness's budget. The run
/// must end without a violation or harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Run {
    run_for(scenario, None, input)
}

/// [`run`] with `budget` steps, if set.
fn run_for(scenario: &StorageScenario, budget: Option<u64>, input: PlanInput) -> Run {
    let scenario = Budgeted {
        storage: scenario.clone(),
        budget,
    };
    let ready = Adopting::prepare(&scenario);
    let report = run_prepared::<Adopting>(
        &scenario,
        ready.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let view = ready
        .last
        .borrow_mut()
        .take()
        .expect("the checks read the view at every step");
    Run { report, view }
}

/// `action` on `peer`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    }
}

/// The witness: the node at A3, `mirror-0` at A3 dialing in, and the node
/// dialing `mirror-1` at A9. Only `mirror-1` offers more work at first, so the
/// node's first locator read is for its target, under the starting
/// generation. That read is held while `mirror-0` grows to A5 and the node
/// syncs and applies the growth, which adopts a new generation. Released then,
/// it settles a target the adoption made obsolete. The node then syncs A9 from
/// `mirror-1` under a third generation.
fn grow_witness() -> StorageScenario {
    let locator = || Select::nth(OpKind::Locator, 1);
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(3),
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                chain: Chain::main(9),
                dialer: Dialer::Node,
            },
        ],
        policy: Policy {
            holds: vec![Hold {
                select: locator(),
                until: Trigger::Completed(Select::nth(OpKind::Apply, 1)),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![step(
            "mirror-0",
            ControlAction::Grow { len: 2 },
            Trigger::Submitted(locator()),
        )],
    }
}

/// The steps that submitted and completed the node's `nth` operation, from 1,
/// of the kind `is` picks, if it was submitted.
fn nth(
    report: &RunReport,
    nth: usize,
    is: impl Fn(&StorageRequest) -> bool,
) -> Option<(u64, Option<u64>)> {
    let mut submitted = None;
    let mut seen = 0;
    for entry in report.log.entries() {
        let Observation::Storage {
            instance: 0, fact, ..
        } = &entry.obs
        else {
            continue;
        };
        match (fact, submitted) {
            (StorageFact::Submitted { id, request, .. }, None) if is(request) => {
                seen += 1;
                if seen == nth {
                    submitted = Some((*id, entry.step));
                }
            }
            (StorageFact::Completed { id, .. }, Some((op, at))) if *id == op => {
                return Some((at, Some(entry.step)));
            }
            _ => {}
        }
    }
    submitted.map(|(_, at)| (at, None))
}

/// Counts each rule's outcomes over a sweep.
#[derive(Debug, Default)]
struct Tally {
    held: usize,
    inconclusive: usize,
}

impl Tally {
    /// Counts `outcome`, which must not be a violation.
    fn count(&mut self, rule: &str, outcome: Acceptance) {
        match outcome {
            Acceptance::Holds => self.held += 1,
            Acceptance::Inconclusive(_) => self.inconclusive += 1,
            Acceptance::Violated(why) => panic!("the {rule} rule broke: {why}"),
        }
    }
}

/// Runs the witness under 16 plans and tallies `rule`'s outcomes, which
/// `check` reads from each run. Some run must be conclusive and hold.
fn sweep(rule: &str, check: impl Fn(&Run) -> Acceptance) -> Tally {
    let scenario = grow_witness();
    let mut tally = Tally::default();
    for input in inputs(16, 256) {
        let run = run(&scenario, input);
        tally.count(rule, check(&run));
    }
    assert!(tally.held > 0, "{tally:?}");
    tally
}

/// The witness runs as designed. The node's first locator read was
/// dispatched under its starting generation, submitted before the growth's
/// apply completed, and completed after it. The node adopted A3, then A5 and
/// A9 under two newer generations, queued a status for each tip on both
/// sessions, published each tip, and dispatched a fresh locator read after
/// adopting A5, so every rule has something to judge.
#[test]
fn the_grow_witness_holds_a_locator_across_the_growth() {
    let scenario = grow_witness();
    for input in inputs(16, 256) {
        let run = run(&scenario, input);
        let is_locator =
            |request: &StorageRequest| matches!(request, StorageRequest::ContinuationLocator);
        let is_apply =
            |request: &StorageRequest| matches!(request, StorageRequest::ApplyHeaderTarget { .. });
        let (held_at, held_done) = nth(&run.report, 1, is_locator).expect("the node locates");
        let (_, grown) = nth(&run.report, 1, is_apply).expect("the node applies the growth");
        let grown = grown.expect("the growth's apply completes");
        let held_done = held_done.expect("the held locator read completes");
        assert!(
            held_at < grown && grown < held_done,
            "{held_at} {grown} {held_done}"
        );

        let adoptions = run.adoption().adoptions();
        let tips: Vec<u32> = adoptions
            .iter()
            .map(|adoption| adoption.header_best.height.0)
            .collect();
        assert_eq!(tips, [3, 5, 9]);
        let generations: Vec<u64> = adoptions
            .iter()
            .map(|adoption| adoption.header_generation.get())
            .collect();
        assert!(
            generations.windows(2).all(|pair| pair[0] < pair[1]),
            "{generations:?}"
        );

        let facts = run.adoption().facts();
        // The held read is the first one dispatched, under the starting
        // generation, which the growth's adoption replaces while it's held.
        let first_locator = facts
            .iter()
            .find_map(|stamped| match stamped.fact {
                AdoptionFact::LocatorDispatched { generation, .. } => Some(generation.get()),
                _ => None,
            })
            .expect("the node dispatches a locator read");
        assert_eq!(first_locator, generations[0], "{generations:?}");
        let count = |is: &dyn Fn(&AdoptionFact) -> bool| {
            facts.iter().filter(|stamped| is(&stamped.fact)).count()
        };
        assert!(count(&|fact| matches!(fact, AdoptionFact::TipPublished { .. })) >= 3);
        for session in &run.view.sessions {
            let mut tips: Vec<u32> = Vec::new();
            for stamped in &facts {
                if let AdoptionFact::StatusQueued {
                    session_id, status, ..
                } = &stamped.fact
                {
                    let tip = status.selected_tip_height.0;
                    if *session_id == session.session_id && tips.last() != Some(&tip) {
                        tips.push(tip);
                    }
                }
            }
            assert_eq!(tips, [3, 5, 9], "session {}", session.session_id);
        }
        let second = facts
            .iter()
            .filter(|stamped| matches!(stamped.fact, AdoptionFact::Adopted(_)))
            .nth(1)
            .expect("the node adopts A5")
            .at;
        assert!(facts.iter().any(|stamped| {
            stamped.at >= second && matches!(stamped.fact, AdoptionFact::LocatorDispatched { .. })
        }));
        assert_eq!(run.view.sessions.len(), 2);
        assert!(
            run.view
                .sessions
                .iter()
                .all(|session| session.until.is_none()),
            "both links stay up: {:?}",
            run.view.sessions
        );
    }
}

/// The generation rule holds on the witness: every round and locator read
/// after an adoption carries its generation.
#[test]
fn new_work_carries_the_adopted_generation() {
    sweep("generation", |run| run.adoption().generation());
}

/// The status rule holds on the witness: both live sessions get each adopted
/// status in time.
#[test]
fn every_live_session_gets_the_adopted_status() {
    sweep("status", Run::status);
}

/// The tip rule holds on the witness: the initial tip and each adopted tip
/// are published, and every publication reaches the node's handle.
#[test]
fn every_adopted_tip_is_published() {
    sweep("tip", |run| run.adoption().tip());
}

/// Runs cut off early break no rule: whatever they owe when the budget ends
/// is inconclusive, not a violation. The checks' own tests cover which
/// endings are inconclusive.
#[test]
fn a_run_that_ends_early_breaks_no_rule() {
    let scenario = grow_witness();
    for budget in [1, 20, 60, 120, 240, 480] {
        for input in inputs(4, 256) {
            let run = run_for(&scenario, Some(budget), input);
            for outcome in [
                run.adoption().generation(),
                run.adoption().tip(),
                run.status(),
            ] {
                assert!(
                    !matches!(outcome, Acceptance::Violated(_)),
                    "budget {budget}: {outcome:?}"
                );
            }
        }
    }
}

/// The evidence and the run's log depend only on scenario and plan: plan A
/// twice, with a plan B between whose evidence differs, gives A's both times.
#[test]
fn the_evidence_repeats_a_a_and_a_b_a() {
    let scenario = grow_witness();
    let plans = inputs(16, 256);
    let record = |input: &PlanInput| {
        let run = run(&scenario, input.clone());
        (
            format!("{:?}", run.adoption().facts()),
            format!("{:?}", run.report.log.entries()),
        )
    };
    let first = record(&plans[0]);
    let other = plans[1..]
        .iter()
        .find(|input| record(input).0 != first.0)
        .expect("some plan gives different evidence");
    let again = record(&plans[0]);
    let _ = record(other);
    let last = record(&plans[0]);
    assert!(first == again && again == last, "plan A, then B, then A");
}

/// The environment variable that makes [`evidence_for_the_parent`] print the
/// witness's evidence under the plan it names.
const PLAN_INDEX: &str = "ZAKURA_SIM_ADOPTION_PLAN";

/// Prints the witness's evidence under the plan `PLAN_INDEX` names, for
/// [`the_evidence_replays_in_a_fresh_process`]. Does nothing without it.
#[test]
fn evidence_for_the_parent() {
    let Ok(index) = std::env::var(PLAN_INDEX) else {
        return;
    };
    let index: usize = index.parse().expect("the parent passes a plan index");
    let run = run(&grow_witness(), inputs(index + 1, 256).swap_remove(index));
    #[allow(clippy::print_stdout)]
    {
        println!("EVIDENCE {:?}", run.adoption().facts());
    }
}

/// A fresh process running the witness under the same plan gives the same
/// evidence.
#[test]
fn the_evidence_replays_in_a_fresh_process() {
    let index = 2;
    let here = run(&grow_witness(), inputs(index + 1, 256).swap_remove(index));
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args(["--exact", "evidence_for_the_parent", "--nocapture"])
        .env(PLAN_INDEX, index.to_string())
        .output()
        .expect("the test binary runs");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("EVIDENCE "))
        .unwrap_or_else(|| panic!("the child printed its evidence: {stdout}"));
    assert_eq!(line, format!("{:?}", here.adoption().facts()));
}
