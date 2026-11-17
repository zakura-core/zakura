//! Witness tests (traces.md, Witness tests), and the checks that a broken
//! witness test fails.
#![cfg(zakura_sim)]

use std::time::Duration;

use zakura_rt::time::sleep;
use zakura_sim::{
    domain::discovery::WAIT_COMPLETE_WAKES,
    harness::{self, discovery::WaitComplete, Subsystem},
    property::{
        HasView, Kind, Pending, Property, PropertyId, Source, Sources, Status, TriggerHistory,
        Verdict,
    },
    witness::{check_witness, witness, Expect, WitnessError, WitnessScenario},
    NaReason, Observation, ObservationLog, Plan, PlanChoice, PlanInput, PropertyOutcome, RunConfig,
    Sim,
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

/// The byte of [`INPUT`] that picks the sink's first step of the final
/// mark. Each later byte picks between the sink and the source, and 1 picks
/// the source.
const FINAL_MARK_STARTS: usize = 147;

/// S4.7's sink side: [`INPUT`] up to the sink's first step of the final
/// mark, then four picks of the source. The source's check lands while the
/// final mark is half done. The sink records before it notifies, so the
/// check passes. A sink that notifies first strands the source: its check
/// fails, and its next `Notified` comes after the last notify.
#[test]
fn discovery_wait_complete_check_inside_final_mark() {
    let mut input = INPUT[..=FINAL_MARK_STARTS].to_vec();
    input.extend([1; 4]);
    check_witness(
        lost_wakeup(Plan::from_fuzz(&input)),
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
    /// Sleeps three times as [`Behavior::Sleep`] does, then waits forever,
    /// and [`OPEN`] marks its check pending, which nothing settles.
    Owe,
    /// Panics on its first poll.
    Panic,
}

/// One task, checked by properties whose verdicts don't depend on it: one
/// that holds, a `Known` one that holds, one that never settles under
/// [`Behavior::Owe`], one with no trigger-history check, and two that share
/// an ID-aware one. Its view is whether the scenario owes.
struct Probe {
    owes: bool,
}

const HOLDS: &str = "test/holds";
const KNOWN: &str = "test/known";
const OPEN: &str = "test/open";
const NO_HISTORY: &str = "test/no-history";
/// The property whose history [`named_step`] reaches.
const NAMED: &str = "test/named";
/// A property that shares [`named_step`] with [`NAMED`].
const NAMED_SIBLING: &str = "test/named-sibling";

impl HasView for Probe {
    type View = bool;

    /// Whether [`OPEN`] owes.
    fn view(&self) -> bool {
        self.owes
    }
}

impl Subsystem for Probe {
    const NAME: &'static str = "test/witness-probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Behavior;
    type Prepared = ();

    /// No harness timer cap, so the drain doesn't follow the sleeper's
    /// timers once nothing is open.
    fn config(_: &Behavior) -> RunConfig {
        RunConfig {
            budget: 50,
            drain_bound: 10,
            harness_timer_cap: Duration::ZERO,
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
                Behavior::Owe => {
                    for _ in 0..3 {
                        sleep(Duration::from_millis(1)).await;
                    }
                    std::future::pending::<()>().await;
                }
                Behavior::Panic => panic!("the probe panics"),
            }
        });
        Probe {
            owes: matches!(behavior, Behavior::Owe),
        }
    }

    /// The schedule, and the view that says whether [`OPEN`] owes.
    fn sources() -> Sources {
        Sources::observed(&[Source::Schedule, Source::View])
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
            witness: Some(TriggerHistory::Log(any_step)),
        };
        vec![
            holds(HOLDS, Status::Active),
            holds(KNOWN, Status::Known { issue: 1 }),
            Property {
                id: PropertyId::new(OPEN),
                claim: "a check that never settles",
                kind: Kind::Safety,
                assumes: &[],
                needs: &[Source::Schedule, Source::View],
                status: Status::Active,
                check: |cx| {
                    if cx.view() {
                        cx.pending(Pending::WatchOpen);
                    }
                    Verdict::NotApplicable
                },
                witness: Some(TriggerHistory::Log(any_step)),
            },
            Property {
                witness: None,
                ..holds(NO_HISTORY, Status::Active)
            },
            Property {
                witness: Some(TriggerHistory::LogOf(named_step)),
                ..holds(NAMED, Status::Active)
            },
            Property {
                witness: Some(TriggerHistory::LogOf(named_step)),
                ..holds(NAMED_SIBLING, Status::Active)
            },
        ]
    }

    fn draw(_: &[u8]) -> Behavior {
        Behavior::Sleep
    }
}

/// [`any_step`], for [`NAMED`] only: one function shared by two IDs that
/// tells them apart by the ID the judge hands it.
fn named_step(id: &PropertyId, log: &ObservationLog) -> bool {
    id.as_str() == NAMED && any_step(log)
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

/// C7: the judge hands a trigger its property's ID, so one function can
/// serve several IDs. [`named_step`] reaches only [`NAMED`]'s history, and
/// the same run loses [`NAMED_SIBLING`]'s.
#[test]
fn a_trigger_sees_its_property_id() {
    check_witness(probe(Behavior::Sleep), NAMED, Expect::Holds);
    assert_eq!(
        witness(probe(Behavior::Sleep), NAMED_SIBLING, Expect::Holds),
        Err(WitnessError::LostTriggerHistory)
    );
}

/// A run that ends with the property's check still pending is
/// `NotApplicable` with incomplete observations, which no expectation
/// accepts.
#[test]
fn an_incomplete_report_fails() {
    for expect in [Expect::Holds, Expect::Applicable, Expect::Violated] {
        let result = witness(probe(Behavior::Owe), OPEN, expect);
        assert!(
            matches!(
                result,
                Err(WitnessError::Outcome(PropertyOutcome::NotApplicable { reason }))
                    if !reason.is_benign()
            ),
            "{expect:?}: {result:?}"
        );
    }
}

/// C6: a property that holds while another is left open isn't a `Holds`
/// witness, since the run can't show nothing else broke. `Applicable`
/// claims no bug, so it still passes on the property's own outcome.
#[test]
fn holds_refuses_an_unrelated_incomplete() {
    assert_eq!(
        witness(probe(Behavior::Owe), HOLDS, Expect::Holds),
        Err(WitnessError::UnrelatedIncomplete {
            property: OPEN.to_owned(),
            reason: NaReason::Unsettled {
                site: Pending::WatchOpen
            },
        })
    );
    check_witness(probe(Behavior::Owe), HOLDS, Expect::Applicable);
}

/// C6: a run whose report leaves out a registered property is incomplete
/// evidence, whatever the target's own outcome.
#[test]
fn a_missing_report_fails() {
    let mut report = harness::run_harness::<Probe>(
        &Behavior::Sleep,
        Plan::from_fuzz(&[]),
        &mut zakura_sim::MemorySink::default(),
        None,
    )
    .expect("the probe runs");
    zakura_sim::witness::judge_report::<Probe>(&report, HOLDS, Expect::Holds)
        .expect("the whole report passes");
    report
        .properties
        .retain(|property| property.property != KNOWN);
    assert_eq!(
        zakura_sim::witness::judge_report::<Probe>(&report, HOLDS, Expect::Holds),
        Err(WitnessError::Unreported {
            property: KNOWN.to_owned()
        })
    );
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
        &[],
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
        &[],
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
        &[],
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
