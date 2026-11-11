//! hdr-12b: `header-sync/state-publication-consistent`'s liveness rules on
//! `header-sync/liveness`'s publication fixtures. Each witness holds and
//! applies, with the hdr-6 controls holding or not applying beside it, and
//! takes the path it names. `cargo xtask planted
//! header-sync/state-publication-consistent` checks that its bypasses break
//! it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command, time::Duration};

use support::{byte_inputs, inputs};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED, RESET_INVALIDATES_WORK,
        STALE_CHAIN_EFFECT_INERT, STALE_COMPLETION_INERT, STATE_PUBLICATION_CONSISTENT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            ends::{EndFact, EnvironmentAction},
            publication::PublicationFact,
        },
        header_sync::liveness::{Liveness, LivenessScenario, STATUS_CHANGES},
        run_prepared, Subsystem,
    },
    MemorySink, Outcome, PlanInput, RunReport,
};

/// What a run's publication and end evidence kept: each publication fact
/// with its stamp, and the stamps of the harness's `Shutdown` step and its
/// first close or relink, if any.
struct Reading {
    facts: Vec<(u64, PublicationFact)>,
    shutdown: Option<u64>,
    /// The stamp of the harness's first `Close` or `Relink` of a mirror.
    environment: Option<u64>,
}

impl Reading {
    /// The run's commits, by state version, with their stamps.
    fn commits(&self) -> Vec<(u64, u64)> {
        self.facts
            .iter()
            .filter_map(|(stamp, fact)| match fact {
                PublicationFact::Committed { state_version, .. } => Some((*stamp, *state_version)),
                _ => None,
            })
            .collect()
    }

    /// How many `Status` debts opened and how many were paid.
    fn debts(&self) -> (usize, usize) {
        let owed = self
            .facts
            .iter()
            .filter(|(_, fact)| matches!(fact, PublicationFact::StatusOwed { .. }))
            .count();
        let paid = self
            .facts
            .iter()
            .filter(|(_, fact)| matches!(fact, PublicationFact::StatusPaid { .. }))
            .count();
        (owed, paid)
    }

    /// Whether every commit before the shutdown was adopted, at that
    /// version or a later one, by a later fact.
    fn every_commit_adopted(&self) -> bool {
        self.commits().into_iter().all(|(stamp, version)| {
            self.shutdown.is_some_and(|shutdown| shutdown < stamp)
                || self.facts.iter().any(|(later, fact)| {
                    *later > stamp
                        && matches!(fact, PublicationFact::Adopted { state_version }
                            if *state_version >= version)
                })
        })
    }
}

/// hdr-12b's publication fixtures.
fn publication_fixtures() -> Vec<LivenessScenario> {
    LivenessScenario::publication().to_vec()
}

/// Runs `fixture` under `input`, which must end idle, after its finish,
/// never at its step budget (Rule 13), and without a violation or harness
/// failure, with what [`Reading`] keeps.
fn run_reading(fixture: LivenessScenario, input: PlanInput) -> (RunReport, Reading) {
    let ports = Liveness::prepare(&fixture);
    let (publication, ends) = (ports.publication.clone(), ports.ends.clone());
    let report = run_prepared::<Liveness>(&fixture, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{fixture:?}: {error}"));
    assert!(
        matches!(report.end.outcome, Outcome::Idle { .. }),
        "{fixture:?}: {:?}",
        report.end
    );
    assert_eq!(report.end.violation, None, "{fixture:?}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{fixture:?}: {:?}", report.end);
    let shutdown = ends.facts().into_iter().find_map(|(stamp, fact)| {
        matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        )
        .then_some(stamp)
    });
    let environment = ends.facts().into_iter().find_map(|(stamp, fact)| {
        matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Close | EnvironmentAction::Relink,
                ..
            }
        )
        .then_some(stamp)
    });
    let reading = Reading {
        facts: publication.facts(),
        shutdown,
        environment,
    };
    (report, reading)
}

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// The regression controls every fixture runs beside the property.
const CONTROLS: [&str; 6] = [
    CHUNK_BUDGET_BOUND,
    TEARDOWN_RETURNS_BASELINE,
    STALE_COMPLETION_INERT,
    RESET_INVALIDATES_WORK,
    STALE_CHAIN_EFFECT_INERT,
    COMMIT_ONLY_AUTHORIZED,
];

/// Asserts that `report` holds the property and applies, and that the
/// controls hold or don't apply beside it.
fn assert_holds_and_applies(fixture: LivenessScenario, report: &RunReport) {
    assert!(
        matches!(
            outcome(report, STATE_PUBLICATION_CONSISTENT),
            PropertyOutcome::Held { applicable } if *applicable > 0
        ),
        "{fixture:?}: {:?}",
        outcome(report, STATE_PUBLICATION_CONSISTENT)
    );
    for control in CONTROLS {
        assert!(
            matches!(
                outcome(report, control),
                PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable
            ),
            "{fixture:?}: {control}: {:?}",
            outcome(report, control)
        );
    }
}

/// Each publication fixture holds the property and applies, with the
/// controls beside it, and takes the path it names: the quiet resets commit
/// twice and both are adopted; a status fixture's every debt is paid, its
/// session's first and, where its `Status` changes, later ones, such as the
/// invalidation's inside the floor; the changing fixture commits at each of
/// its first seconds; and the held apply commits after the shutdown under some plan,
/// which opens nothing.
#[test]
fn every_publication_fixture_holds_and_takes_its_path() {
    let mut after_shutdown = 0;
    let mut ended_owing = [0; 2];
    let mut admitted_stopping = 0;
    let mut admitted_closed = 0;
    for fixture in publication_fixtures() {
        let plans = match fixture {
            LivenessScenario::HeldAcrossShutdown
            | LivenessScenario::StatusShutdownWhileRelinking
            | LivenessScenario::StatusClosedBeforeAdmission
            | LivenessScenario::StatusClosedWhileOwed
            | LivenessScenario::StatusRelinkedWhileOwed => 32,
            _ => 6,
        };
        for input in inputs(plans, 256) {
            let (report, reading) = run_reading(fixture, input);
            assert_holds_and_applies(fixture, &report);
            assert!(reading.every_commit_adopted(), "{fixture:?}");
            let commits = reading.commits().len();
            let (owed, paid) = reading.debts();
            match fixture {
                LivenessScenario::QuietReset => {
                    assert_eq!(commits, 2, "{fixture:?}");
                    assert_eq!((owed, paid), (0, 0), "{fixture:?}");
                }
                LivenessScenario::HeldAcrossShutdown => {
                    let shutdown = reading.shutdown.expect("the fixture's step stops the node");
                    let late = reading.commits().iter().any(|(stamp, _)| *stamp > shutdown);
                    after_shutdown += usize::from(late);
                }
                LivenessScenario::StatusStartup => {
                    assert_eq!(commits, 0, "{fixture:?}");
                    assert_eq!((owed, paid), (1, 1), "{fixture:?}");
                }
                LivenessScenario::StatusInsideFloor => {
                    assert_eq!(commits, 1, "{fixture:?}");
                    assert_eq!((owed, paid), (2, 2), "{fixture:?}");
                }
                LivenessScenario::StatusChanging => {
                    let seconds: Vec<SimInstant> = reading
                        .facts
                        .iter()
                        .filter_map(|(_, fact)| match fact {
                            PublicationFact::Committed { at, .. } => Some(*at),
                            _ => None,
                        })
                        .collect();
                    let expected: Vec<SimInstant> = (1..=STATUS_CHANGES)
                        .map(|second| SimInstant::from_start(Duration::from_secs(second)))
                        .collect();
                    assert_eq!(seconds, expected, "{fixture:?}");
                    assert!(owed >= 2 && paid == owed, "{fixture:?}: {owed} {paid}");
                }
                LivenessScenario::StatusClosedWhileOwed
                | LivenessScenario::StatusRelinkedWhileOwed => {
                    assert_eq!(commits, 1, "{fixture:?}");
                    // The invalidation's debt, the second, is paid or the
                    // harness's step answered it first.
                    let debt = reading
                        .facts
                        .iter()
                        .filter(|(_, fact)| matches!(fact, PublicationFact::StatusOwed { .. }))
                        .nth(1)
                        .map(|(stamp, _)| *stamp)
                        .expect("the invalidation owes a status");
                    let paid_at = reading.facts.iter().find_map(|(stamp, fact)| {
                        (*stamp > debt && matches!(fact, PublicationFact::StatusPaid { .. }))
                            .then_some(*stamp)
                    });
                    let acted = reading.environment.expect("the harness closes or relinks");
                    if acted > debt && paid_at.is_none_or(|paid| acted < paid) {
                        ended_owing
                            [usize::from(fixture == LivenessScenario::StatusRelinkedWhileOwed)] +=
                            1;
                    }
                }
                LivenessScenario::StatusShutdownWhileRelinking => {
                    let shutdown = reading.shutdown.expect("the fixture's step stops the node");
                    let late = reading.facts.iter().any(|(stamp, fact)| {
                        *stamp > shutdown && matches!(fact, PublicationFact::StatusOwed { .. })
                    });
                    admitted_stopping += usize::from(late);
                }
                LivenessScenario::StatusClosedBeforeAdmission => {
                    let closed = reading.environment.expect("the harness closes the link");
                    let late = reading.facts.iter().any(|(stamp, fact)| {
                        *stamp > closed && matches!(fact, PublicationFact::StatusOwed { .. })
                    });
                    admitted_closed += usize::from(late);
                }
                other => unreachable!("{other:?} isn't a publication fixture"),
            }
        }
    }
    assert!(
        after_shutdown > 0,
        "no plan committed the held apply after the shutdown"
    );
    assert!(
        admitted_closed > 0,
        "no plan admitted the session after its link closed"
    );
    assert!(
        admitted_stopping > 0,
        "no plan owed a status after the shutdown began"
    );
    assert!(
        ended_owing.iter().all(|runs| *runs > 0),
        "some plan must close and relink the session while it's owed: {ended_owing:?}"
    );
}

/// A hold fixture keeps no `Status` debt, since only a status fixture arms
/// it, and its commits are adopted or come after the node's stop.
#[test]
fn a_hold_fixture_keeps_no_status_debt() {
    let fixture = LivenessScenario::ReplyHeld;
    for input in inputs(4, 256) {
        let (report, reading) = run_reading(fixture, input);
        assert_eq!(reading.debts(), (0, 0));
        assert!(!reading.commits().is_empty());
        assert!(reading.every_commit_adopted());
        assert!(matches!(
            outcome(&report, STATE_PUBLICATION_CONSISTENT),
            PropertyOutcome::Held { .. }
        ));
    }
}

/// The publication facts and the run's log depend only on scenario and
/// plan: plan A twice, with a plan B between whose facts differ, gives A's
/// both times.
#[test]
fn the_facts_repeat_a_a_and_a_b_a() {
    let fixture = LivenessScenario::StatusChanging;
    let plans = inputs(16, 256);
    let record = |input: &PlanInput| {
        let (report, reading) = run_reading(fixture, input.clone());
        (
            format!("{:?}", reading.facts),
            format!("{:?}", report.log.entries()),
        )
    };
    let first = record(&plans[0]);
    let other = plans[1..]
        .iter()
        .find(|input| record(input).0 != first.0)
        .expect("some plan gives different facts");
    let again = record(&plans[0]);
    let _ = record(other);
    let last = record(&plans[0]);
    assert!(first == again && again == last, "plan A, then B, then A");
}

/// The committed typed witnesses the bypasses run on, by name.
fn committed_witnesses() -> [(&'static str, LivenessScenario); 3] {
    [
        ("live-quiet-reset", LivenessScenario::QuietReset),
        ("status-startup", LivenessScenario::StatusStartup),
        ("status-changing", LivenessScenario::StatusChanging),
    ]
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-publication-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-publication-{name}.{extension}"))
}

/// Each committed witness is its fixture, as JSON, with the fixed Plan
/// bytes, and holds and applies under them.
#[test]
fn the_committed_witnesses_match_their_fixtures() {
    for (name, scenario) in committed_witnesses() {
        let committed: LivenessScenario = serde_json::from_str(
            &fs::read_to_string(fixture(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            committed, scenario,
            "{name}: run `regenerate_liveness_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_liveness_witnesses`"
        );
        let (report, _) = run_reading(scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert_holds_and_applies(scenario, &report);
    }
}

/// Rewrites the committed witnesses from their fixtures.
#[test]
#[ignore = "rewrites the committed liveness witnesses"]
fn regenerate_liveness_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// `record --witness` judges each committed witness as `cargo xtask
/// planted` runs it, expecting a hold, and `replay` reruns the trace Exact
/// in a fresh process.
#[test]
fn the_committed_witnesses_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_witnesses() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario = fs::read_to_string(fixture(name, "json")).expect("the witness is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Liveness::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture(name, "plan").to_str().expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            STATE_PUBLICATION_CONSISTENT,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{name}: {witness}"
        );
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
