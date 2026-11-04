//! Witness tests (traces.md, Witness tests), and the checks that a broken
//! witness test fails.
#![cfg(zakura_sim)]

use std::time::Duration;

use zakura_rt::time::sleep;
use zakura_sim::{
    domain::discovery::WAIT_COMPLETE_WAKES,
    harness::{self, discovery::WaitComplete, Subsystem},
    property::{
        HasView, Kind, LivenessBound, Property, PropertyId, Source, Status, Subject, Verdict,
    },
    witness::{check_witness, witness, Expect, WitnessError, WitnessScenario},
    Observation, ObservationLog, Plan, PlanChoice, PlanInput, PropertyOutcome, RunConfig, Sim,
};

/// The committed input that holds discovery's source back at its
/// `notified` until the sink has run all three marks (`tests/discovery.rs`
/// builds it).
const INPUT: &[u8] = include_bytes!("fixtures/discovery-wait-complete.bin");

/// S4.7's scenario under `input`.
fn lost_wakeup(input: PlanInput) -> WitnessScenario<WaitComplete> {
    WitnessScenario {
        scenario: Default::default(),
        input,
    }
}

/// S4.7: with the fix, the source wakes even when the final mark lands
/// while it's preempted in the waiting code. `cargo xtask planted
/// discovery/wait-complete-wakes` checks that the same input violates the
/// property with `qa/planted/discovery/wait-complete-wakes/lost-wakeup.patch`
/// applied.
#[test]
fn discovery_wait_complete_lost_wakeup() {
    check_witness(
        lost_wakeup(Plan::from_fuzz(INPUT)),
        WAIT_COMPLETE_WAKES,
        Expect::Holds,
    );
}

/// The fixed code holds, so a test that expects the violation fails.
#[test]
fn violated_witness_fails_while_fixed() {
    assert_eq!(
        witness(
            lost_wakeup(Plan::from_fuzz(INPUT)),
            WAIT_COMPLETE_WAKES,
            Expect::Violated
        ),
        Err(WitnessError::Outcome(PropertyOutcome::Held {
            applicable: 1
        }))
    );
}

/// A broken copy of S4.7 whose round-robin order parks the source before
/// the final mark: the property holds, but the run never reaches the race.
#[test]
fn missed_trigger_history_fails() {
    assert_eq!(
        witness(
            lost_wakeup(Plan::from_fuzz(&[])),
            WAIT_COMPLETE_WAKES,
            Expect::Holds
        ),
        Err(WitnessError::LostTriggerHistory)
    );
}

/// A broken copy of S4.7 whose script picks an action that's never enabled
/// ends at a harness failure.
#[test]
fn harness_end_fails() {
    let script = Plan {
        choices: vec![PlanChoice::Pick("task:nowhere:1".to_owned())],
    };
    let result = witness(
        lost_wakeup(PlanInput::Script(script)),
        WAIT_COMPLETE_WAKES,
        Expect::Holds,
    );
    assert!(
        matches!(result, Err(WitnessError::HarnessEnd(_))),
        "{result:?}"
    );
}

/// A test of a property the harness doesn't register fails before it runs.
#[test]
fn unregistered_property_fails() {
    assert_eq!(
        witness(
            lost_wakeup(Plan::from_fuzz(INPUT)),
            "discovery/no-such-property",
            Expect::Holds
        ),
        Err(WitnessError::Unregistered)
    );
}

/// What [`Probe`]'s one task does.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
enum Behavior {
    /// Sleeps 1 ms at a time until the budget ends the run.
    Sleep,
    /// Panics on its first poll.
    Panic,
}

/// One task, checked by properties whose verdicts don't depend on it: one
/// that holds, a `Known` one that holds, one whose obligation never closes,
/// and one with no trigger-history check.
struct Probe;

const HOLDS: &str = "test/holds";
const KNOWN: &str = "test/known";
const OPEN: &str = "test/open";
const NO_HISTORY: &str = "test/no-history";

impl HasView for Probe {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Probe {
    const NAME: &'static str = "test/witness-probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Behavior;
    type Prepared = ();

    fn config(_: &Behavior) -> RunConfig {
        RunConfig {
            budget: 50,
            drain_bound: 10,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Behavior) {}

    fn build(sim: &mut Sim, behavior: &Behavior, (): ()) -> Self {
        let behavior = *behavior;
        sim.scheduler().spawn_root("probe", async move {
            match behavior {
                Behavior::Sleep => loop {
                    sleep(Duration::from_millis(1)).await;
                },
                Behavior::Panic => panic!("the probe panics"),
            }
        });
        Probe
    }

    fn properties() -> Vec<Property<Self>> {
        let holds = |id: &'static str, status| Property {
            id: PropertyId::new(id),
            claim: "always holds",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Schedule],
            status,
            check: |_| Verdict::Holds,
            witness: Some(any_step),
        };
        vec![
            holds(HOLDS, Status::Active),
            holds(KNOWN, Status::Known { issue: 1 }),
            Property {
                id: PropertyId::new(OPEN),
                claim: "an hour-long obligation never closes",
                kind: Kind::Liveness {
                    bound: LivenessBound::VirtualTime(Duration::from_secs(3_600)),
                },
                assumes: &[],
                needs: &[Source::Schedule],
                status: Status::Active,
                check: |cx| {
                    let obligation =
                        cx.obligation(None, Subject::Owner("probe".into()), |_, _| false);
                    cx.oblige(obligation);
                    Verdict::Holds
                },
                witness: Some(any_step),
            },
            Property {
                witness: None,
                ..holds(NO_HISTORY, Status::Active)
            },
        ]
    }

    fn draw(_: &[u8]) -> Behavior {
        Behavior::Sleep
    }
}

/// A trigger history that every run past setup has.
fn any_step(log: &ObservationLog) -> bool {
    log.entries()
        .iter()
        .any(|e| e.step > 0 && matches!(e.obs, Observation::TaskReached { .. }))
}

/// [`Probe`] under `behavior`, with an empty plan.
fn probe(behavior: Behavior) -> WitnessScenario<Probe> {
    WitnessScenario {
        scenario: behavior,
        input: Plan::from_fuzz(&[]),
    }
}

/// The control for the probe tests: its holding property passes.
#[test]
fn holding_probe_passes() {
    check_witness(probe(Behavior::Sleep), HOLDS, Expect::Holds);
    check_witness(probe(Behavior::Sleep), HOLDS, Expect::Applicable);
}

/// A run that ends with the property's obligation still open is
/// inconclusive, which no expectation accepts.
#[test]
fn inconclusive_report_fails() {
    for expect in [Expect::Holds, Expect::Applicable, Expect::Violated] {
        let result = witness(probe(Behavior::Sleep), OPEN, expect);
        assert!(
            matches!(
                result,
                Err(WitnessError::Outcome(PropertyOutcome::Inconclusive { .. }))
            ),
            "{expect:?}: {result:?}"
        );
    }
}

/// A `Violated` witness test for a `Known` property fails with the "fixed?"
/// message once the property holds.
#[test]
fn violated_witness_fails_once_fixed() {
    let result = witness(probe(Behavior::Sleep), KNOWN, Expect::Violated);
    assert_eq!(result, Err(WitnessError::Fixed { issue: 1 }));
    assert!(result
        .unwrap_err()
        .to_string()
        .starts_with("fixed? close the issue and set Active"));
}

/// A task panic violates `sim/no-task-panic`, which is unrelated to the
/// property under test.
#[test]
fn unrelated_violation_fails() {
    for expect in [Expect::Holds, Expect::Applicable, Expect::Violated] {
        assert_eq!(
            witness(probe(Behavior::Panic), HOLDS, expect),
            Err(WitnessError::Unrelated {
                property: "sim/no-task-panic".to_owned()
            }),
            "{expect:?}"
        );
    }
}

/// A property with no trigger-history check can't back a `Holds` or
/// `Violated` witness test. An `Applicable` one claims no bug, so it
/// passes on the property's outcome.
#[test]
fn no_trigger_history_fails() {
    for expect in [Expect::Holds, Expect::Violated] {
        assert_eq!(
            witness(probe(Behavior::Sleep), NO_HISTORY, expect),
            Err(WitnessError::NoTriggerHistory),
            "{expect:?}"
        );
    }
    check_witness(probe(Behavior::Sleep), NO_HISTORY, Expect::Applicable);
}

/// Recording a witness test writes the trace of the run it judged, and
/// judges it with the same rule as `witness`: the fixed code passes Holds
/// and fails Violated, and either way the trace is complete.
#[test]
fn recorded_witness_judges_its_own_trace() {
    let entry = harness::find(WaitComplete::NAME).expect("S4.7's harness is registered");
    let dir = tempfile::tempdir().expect("a temporary directory");

    let (trace, result) = harness::record_witness(
        entry,
        harness::RecordInput::Input(INPUT),
        &dir.path().join("holds"),
        WAIT_COMPLETE_WAKES,
        Expect::Holds,
    )
    .expect("the trace is written");
    assert_eq!(result, Ok(()));
    assert!(trace.end.is_some());

    let (trace, result) = harness::record_witness(
        entry,
        harness::RecordInput::Input(INPUT),
        &dir.path().join("violated"),
        WAIT_COMPLETE_WAKES,
        Expect::Violated,
    )
    .expect("a failed witness test still writes its trace");
    assert_eq!(
        result,
        Err(WitnessError::Outcome(PropertyOutcome::Held {
            applicable: 1
        }))
    );
    assert!(trace.end.is_some());

    let error = harness::record_witness(
        entry,
        harness::RecordInput::Input(INPUT),
        &dir.path().join("unregistered"),
        "discovery/no-such-property",
        Expect::Holds,
    )
    .expect_err("an unregistered property has no witness test");
    assert!(
        matches!(
            error,
            harness::RecordError::Witness(WitnessError::Unregistered)
        ),
        "{error}"
    );
    assert!(!dir.path().join("unregistered").exists());
}
