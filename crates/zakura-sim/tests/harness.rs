//! The harness registry: every harness has a unique name, rebuilds from its
//! serialized scenario, and reports the step loop's properties, which the
//! coverage manifest lists with the tests that show them working.
#![cfg(zakura_sim)]

use std::{cell::Cell, collections::BTreeSet, path::Path, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use zakura_rt::{task::yield_now, time::sleep};
use zakura_sim::{
    check::{self, StepCtx, Violation},
    harness::{
        self,
        coverage::{Bypass, Witness, COVERAGE},
        run_harness, HarnessEntry, HarnessError, Subsystem, HARNESSES, NO_TASK_PANIC,
    },
    property::{Assume, AssumeRecord, Assumptions, HasView, Status},
    sim::NO_LIVELOCK,
    MemorySink, Outcome, Plan, PropertyOutcome, RunConfig, RunReport, Sim,
};

/// Every scenario prefix of `entry` worth trying: each first byte, with each
/// second byte's low bit, cut to the harness's length.
fn prefixes(entry: &HarnessEntry) -> BTreeSet<Vec<u8>> {
    (0..=u8::MAX)
        .flat_map(|first| [[first, 0], [first, 1]])
        .map(|bytes| {
            let mut prefix = bytes.to_vec();
            prefix.resize(entry.scenario_bytes, 0);
            prefix
        })
        .collect()
}

#[test]
fn harness_names_are_unique_and_found() {
    let names: BTreeSet<_> = HARNESSES.iter().map(|entry| entry.name).collect();
    assert_eq!(names.len(), HARNESSES.len());
    for entry in HARNESSES {
        assert_eq!(harness::find(entry.name).unwrap().name, entry.name);
    }
    assert!(harness::find("no/such-harness").is_err());
}

#[test]
fn every_harness_round_trips_its_scenario() {
    for entry in HARNESSES {
        for prefix in prefixes(entry) {
            let scenario = (entry.draw)(&prefix);
            let text = serde_json::to_string(&scenario).unwrap();
            let back: Value = serde_json::from_str(&text).unwrap();
            let config = (entry.config)(&scenario).unwrap();
            assert_eq!(
                (entry.config)(&back).unwrap(),
                config,
                "{}: {text}",
                entry.name
            );
            let meta = (entry.meta)(&back).unwrap();
            assert_eq!(meta.scenario, scenario, "{}", entry.name);
            assert_eq!(meta.sim, entry.name);
            assert_eq!(meta.generator, entry.generator);
            assert_eq!(meta.scheduler.workers, config.scheduler.workers);
            assert_eq!(meta.budget.budget, config.budget);
        }
    }
}

#[test]
fn a_scenario_that_does_not_fit_is_refused() {
    let entry = harness::find("header-sync/smoke").unwrap();
    for bad in [
        serde_json::json!({ "extra": 1 }),
        serde_json::json!("smoke"),
    ] {
        assert!(matches!((entry.meta)(&bad), Err(HarnessError::Scenario(_))));
        assert!(matches!(
            (entry.config)(&bad),
            Err(HarnessError::Scenario(_))
        ));
    }
}

/// What [`Probe`]'s one task does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Behavior {
    /// Yields forever, so the clock never moves.
    Spin,
    /// Sleeps forever, 1 ms at a time.
    Sleep,
    /// Panics on its first poll.
    Panic,
}

/// One task that behaves as the scenario says, under a scenario that
/// declares `BoundedAdversary`.
struct Probe;

impl HasView for Probe {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Probe {
    const NAME: &'static str = "test/probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Behavior;
    type Prepared = ();

    fn config(_: &Behavior) -> RunConfig {
        RunConfig {
            budget: 1_000,
            drain_bound: 100,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Behavior) {}

    fn build(sim: &mut Sim, behavior: &Behavior, (): ()) -> Self {
        let behavior = *behavior;
        sim.scheduler().spawn_root("probe", async move {
            match behavior {
                Behavior::Spin => loop {
                    yield_now().await;
                },
                Behavior::Sleep => loop {
                    sleep(Duration::from_millis(1)).await;
                },
                Behavior::Panic => panic!("the probe panics"),
            }
        });
        Probe
    }

    fn assumptions(_: &Behavior) -> Assumptions<Self> {
        Assumptions::declare(vec![AssumeRecord {
            assume: Assume::BoundedAdversary,
            params: serde_json::json!({}),
        }])
    }

    fn draw(_: &[u8]) -> Behavior {
        Behavior::Sleep
    }
}

/// Runs [`Probe`] on an empty plan, and checks that `meta.properties` names
/// every property the run reports.
fn probe(behavior: Behavior) -> RunReport {
    let report = run_harness::<Probe>(
        &behavior,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{behavior:?}: {error}"));
    let meta = harness::meta::<Probe>(&behavior).unwrap();
    let declared: Vec<_> = meta.properties.iter().map(|p| p.id.as_str()).collect();
    let reported: Vec<_> = report
        .properties
        .iter()
        .map(|r| r.property.as_str())
        .collect();
    assert_eq!(declared, reported, "{behavior:?}");
    assert_eq!(meta.assumptions, Probe::assumptions(&behavior).records());
    report
}

/// `property`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, property: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == property)
        .unwrap_or_else(|| panic!("the run reports {property}: {:?}", report.properties))
        .outcome
}

#[test]
fn livelock_fires_before_budget() {
    let config = harness::run_config::<Probe>(&Behavior::Spin);
    assert!(config.enforce_no_livelock);
    assert!(config.max_steps_per_instant < config.budget);

    let report = probe(Behavior::Spin);
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("the spin is a livelock");
    assert_eq!(violation.property, NO_LIVELOCK);
    assert_eq!(report.end.outcome, Outcome::Violation);
    assert!(report.end.steps < config.budget, "{:?}", report.end);
    assert!(matches!(
        outcome(&report, NO_LIVELOCK),
        PropertyOutcome::Violated { .. }
    ));
}

/// [`Probe`], but its `BoundedAdversary` assumption never holds.
struct Unbounded;

impl HasView for Unbounded {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Unbounded {
    const NAME: &'static str = "test/unbounded";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Behavior;
    type Prepared = ();

    fn config(behavior: &Behavior) -> RunConfig {
        Probe::config(behavior)
    }

    /// Nothing to set up.
    fn prepare(_: &Behavior) {}

    fn build(sim: &mut Sim, behavior: &Behavior, (): ()) -> Self {
        Probe::build(sim, behavior, ());
        Unbounded
    }

    fn assumptions(behavior: &Behavior) -> Assumptions<Self> {
        Assumptions::declare(Probe::assumptions(behavior).records().to_vec())
            .holds_when(|_, _| false)
    }

    fn draw(_: &[u8]) -> Behavior {
        Behavior::Spin
    }
}

/// Review P1b-1: the livelock limit applies only while `BoundedAdversary`
/// holds, so a spin while it doesn't is no violation, and the property
/// reports that its assumption never held.
#[test]
fn livelock_waits_for_its_assumption() {
    let report = run_harness::<Unbounded>(
        &Behavior::Spin,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap();
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(
        *outcome(&report, NO_LIVELOCK),
        PropertyOutcome::Inconclusive {
            reason: "assumptions-never-held".to_owned()
        }
    );
}

thread_local! {
    /// How many times [`Counted`] declared its assumptions.
    static READS: Cell<u32> = const { Cell::new(0) };
}

/// [`Probe`], counting each read of its assumptions.
struct Counted;

impl HasView for Counted {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

impl Subsystem for Counted {
    const NAME: &'static str = "test/counted";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Behavior;
    type Prepared = ();

    fn config(behavior: &Behavior) -> RunConfig {
        Probe::config(behavior)
    }

    /// Nothing to set up.
    fn prepare(_: &Behavior) {}

    fn build(sim: &mut Sim, behavior: &Behavior, (): ()) -> Self {
        Probe::build(sim, behavior, ());
        Counted
    }

    fn assumptions(behavior: &Behavior) -> Assumptions<Self> {
        READS.set(READS.get() + 1);
        Assumptions::declare(Probe::assumptions(behavior).records().to_vec())
    }

    fn draw(_: &[u8]) -> Behavior {
        Behavior::Sleep
    }
}

/// V12 F-299: a run reads its assumptions once, so the step loop's livelock
/// limit and the checker's `BoundedAdversary` come from one declaration.
#[test]
fn a_run_reads_its_assumptions_once() {
    READS.set(0);
    run_harness::<Counted>(
        &Behavior::Sleep,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap();
    assert_eq!(READS.get(), 1);
}

/// Review (#1749): a spin past the limit at the step where another check
/// ends the run still violates `sim/no-livelock`.
#[test]
fn a_spin_at_another_violations_step_is_still_reported() {
    let spin = probe(Behavior::Spin)
        .end
        .violation
        .expect("the spin is a livelock")
        .step;
    let mut other = check::after_step(|cx: &StepCtx<'_>| {
        (cx.step == spin).then(|| Violation {
            property: "test/other".to_owned(),
            step: cx.step,
            witness: vec![cx.step],
            expected: "no failure at this step".to_owned(),
            actual: "the test's own check failed".to_owned(),
        })
    });
    let report = run_harness::<Probe>(
        &Behavior::Spin,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        Some(&mut other),
    )
    .unwrap();
    let ended = report.end.violation.as_ref().expect("the other check");
    assert_eq!((ended.property.as_str(), ended.step), ("test/other", spin));
    assert!(matches!(
        outcome(&report, NO_LIVELOCK),
        PropertyOutcome::Violated { witness } if *witness == [spin]
    ));
}

#[test]
fn a_moving_clock_holds_no_livelock() {
    let report = probe(Behavior::Sleep);
    assert_eq!(report.end.violation, None);
    assert!(matches!(
        outcome(&report, NO_LIVELOCK),
        PropertyOutcome::Held { applicable } if *applicable > 0
    ));
}

#[test]
fn task_panic_violates_no_task_panic() {
    let report = probe(Behavior::Panic);
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("the panic is a violation");
    assert_eq!(violation.property, NO_TASK_PANIC);
    assert_eq!(violation.actual, "probe.1 panicked");
    assert!(matches!(
        outcome(&report, NO_TASK_PANIC),
        PropertyOutcome::Violated { witness } if *witness == [violation.step]
    ));
}

#[test]
fn a_clean_run_holds_no_task_panic() {
    let entry = harness::find("header-sync/smoke").unwrap();
    let scenario = (entry.draw)(&[]);
    let report = (entry.run)(&scenario, Plan::from_fuzz(&[]), &mut MemorySink::default()).unwrap();
    assert_eq!(report.end.violation, None);
    assert!(matches!(
        outcome(&report, NO_TASK_PANIC),
        PropertyOutcome::Held { applicable } if *applicable > 0
    ));
}

#[test]
fn every_harness_names_its_statuses() {
    for entry in HARNESSES {
        let properties = (entry.properties)();
        assert_eq!(properties[0].id, NO_TASK_PANIC, "{}", entry.name);
        for property in properties {
            assert_eq!(entry.status(&property.id), Some(property.status));
        }
        assert_eq!(entry.status(NO_LIVELOCK), Some(Status::Active));
        assert_eq!(entry.status("no/such-property"), None);
    }
}

#[test]
fn the_coverage_manifest_lists_every_reported_property() {
    let reported: BTreeSet<String> = HARNESSES
        .iter()
        .flat_map(|entry| (entry.properties)())
        .map(|property| property.id)
        .collect();
    let listed: BTreeSet<String> = COVERAGE.iter().map(|row| row.property.to_owned()).collect();
    assert_eq!(listed.len(), COVERAGE.len(), "one row per property");
    assert_eq!(listed, reported);
}

/// Checks that `witness` is a test CI runs: `source` defines it once, the
/// attributes directly above it include `#[test]` and no `#[cfg`, and an
/// ignored one is named in `workflow`, the `tests-unit.yml` text.
fn witness_runs(source: &str, workflow: &str, witness: Witness) -> Result<(), String> {
    let lines: Vec<&str> = source.lines().map(str::trim).collect();
    let signature = format!("fn {}()", witness.test);
    let mut defined = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with(&signature));
    let (at, _) = defined
        .next()
        .ok_or_else(|| format!("{} has no {}", witness.file, witness.test))?;
    if defined.next().is_some() {
        return Err(format!("{} defines {} twice", witness.file, witness.test));
    }
    let attributes: Vec<&str> = lines[..at]
        .iter()
        .rev()
        .take_while(|line| line.starts_with("#[") || line.starts_with("///"))
        .filter(|line| line.starts_with("#["))
        .copied()
        .collect();
    if !attributes
        .iter()
        .any(|a| *a == "#[test]" || a.starts_with("#[tokio::test"))
    {
        return Err(format!("{} isn't a test", witness.test));
    }
    if attributes.iter().any(|a| a.starts_with("#[cfg")) {
        return Err(format!("{} is behind a cfg", witness.test));
    }
    if !workflow.contains(&format!("name: {}", witness.ci)) {
        return Err(format!("no `{}` job", witness.ci));
    }
    let ignored = attributes.iter().any(|a| a.starts_with("#[ignore"));
    if ignored && !workflow.contains(witness.test) {
        return Err(format!("CI never runs the ignored {}", witness.test));
    }
    Ok(())
}

#[test]
fn every_coverage_witness_exists_and_ci_runs_it() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workflow =
        std::fs::read_to_string(crate_dir.join("../../.github/workflows/tests-unit.yml")).unwrap();
    for row in COVERAGE {
        let witnesses = match row.bypass {
            Bypass::Test(bypass) => vec![row.passing.witness(), bypass],
            Bypass::Planted(name) => {
                let patch = crate_dir
                    .join("../../qa/planted")
                    .join(row.property)
                    .join(format!("{name}.patch"));
                assert!(patch.is_file(), "{}: no {}", row.property, patch.display());
                assert!(
                    workflow.contains("cargo xtask planted"),
                    "{}: CI never runs `cargo xtask planted`",
                    row.property
                );
                vec![row.passing.witness()]
            }
            Bypass::Pending(_) => {
                let statuses: Vec<_> = HARNESSES
                    .iter()
                    .filter_map(|entry| entry.status(row.property))
                    .collect();
                assert!(
                    !statuses.is_empty() && statuses.iter().all(|status| *status == Status::Draft),
                    "{}: only a draft's bypass is pending, not {statuses:?}'s",
                    row.property
                );
                vec![row.passing.witness()]
            }
        };
        for witness in witnesses {
            let source = std::fs::read_to_string(crate_dir.join(witness.file))
                .unwrap_or_else(|error| panic!("{}: {}: {error}", row.property, witness.file));
            if let Err(error) = witness_runs(&source, &workflow, witness) {
                panic!("{}: {error}", row.property);
            }
        }
    }
}

/// Review (#1749): the witness check refuses what only looks like a test.
#[test]
fn the_witness_check_refuses_lookalikes() {
    let workflow = "name: zakura_sim build";
    let witness = Witness {
        file: "tests/fake.rs",
        test: "witness",
        ci: "zakura_sim build",
    };
    let check = |source: &str| witness_runs(source, workflow, witness);
    assert_eq!(check("#[test]\nfn witness() {}"), Ok(()));
    for (source, error) in [
        ("fn witness() {}", "witness isn't a test"),
        (
            "#[test]\nfn other() {}\nfn witness() {}",
            "witness isn't a test",
        ),
        (
            "#[cfg(any())]\n#[test]\nfn witness() {}",
            "witness is behind a cfg",
        ),
        (
            "#[ignore]\n#[test]\nfn witness() {}",
            "CI never runs the ignored witness",
        ),
        (
            "#[test]\nfn witness() {}\n#[test]\nfn witness() {}",
            "tests/fake.rs defines witness twice",
        ),
    ] {
        assert_eq!(check(source), Err(error.to_owned()), "{source}");
    }
}
