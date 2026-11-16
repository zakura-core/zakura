//! hdr-9d.1: `header-sync/outstanding-work-bounded` on `header-sync/storage`.
//! Every committed authority and reset witness holds it and applies, under
//! its committed Plan bytes and under generated plans.
//! `cargo xtask planted header-sync/outstanding-work-bounded` checks that its
//! bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf};

use support::{
    inputs,
    steer::{steer, Rule, Steer},
};
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::OUTSTANDING_WORK_BOUNDED,
    harness::{
        header_node::outstanding::OutstandingEvidence,
        header_sync::{Storage, StorageScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{StorageFact, StorageResult},
    MemorySink, Observation, Plan, PlanInput, RunReport, StepCtx,
};

/// Every committed storage witness the outstanding-work gate runs, by its
/// fixture stem.
const WITNESSES: [&str; 10] = [
    "header-authority-forks-sharing-a-header",
    "header-authority-relinked-mid-round",
    "header-reset-held-apply",
    "header-reset-held-reply",
    "header-reset-quiet-reset",
    "header-reset-quiet-reset-deep",
    "header-outstanding-churn",
    "header-outstanding-serving-churn",
    "header-outstanding-serving-replaced",
    "header-outstanding-capacity-waits",
];

/// The committed fixture `<stem>.<extension>`.
fn fixture(stem: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{stem}.{extension}"))
}

/// The committed scenario `stem`.
fn scenario(stem: &str) -> StorageScenario {
    serde_json::from_str(
        &fs::read_to_string(fixture(stem, "json"))
            .unwrap_or_else(|error| panic!("{stem}: {error}")),
    )
    .unwrap_or_else(|error| panic!("{stem}: {error}"))
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure.
fn run(stem: &str, scenario: &StorageScenario, input: PlanInput) -> RunReport {
    let report = run_prepared::<Storage>(
        scenario,
        Storage::prepare(scenario),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{stem}: {error}"));
    assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
    report
}

/// Asserts `report` held the property and judged at least one `Sizes`.
fn held(stem: &str, report: &RunReport) {
    let outcome = &report
        .properties
        .iter()
        .find(|reported| reported.property == OUTSTANDING_WORK_BOUNDED)
        .unwrap_or_else(|| panic!("{stem}: the property reports"))
        .outcome;
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{stem}: {outcome:?}"
    );
}

/// Each committed witness holds the property and applies under its own
/// Plan bytes, which the planted gate runs.
#[test]
fn the_committed_witnesses_hold_outstanding_work_bounded() {
    for stem in WITNESSES {
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        let report = run(stem, &scenario(stem), Plan::from_fuzz(&plan));
        held(stem, &report);
    }
}

/// Each committed witness holds the property under generated plans too, so
/// no honest schedule breaks a row.
#[test]
fn the_witnesses_hold_under_generated_plans() {
    for stem in WITNESSES {
        let scenario = scenario(stem);
        for input in inputs(8, 256) {
            held(stem, &run(stem, &scenario, input));
        }
    }
}

/// The limit fixtures. The slot and locator fixtures have 17 mirrors, each
/// at its own height of the main chain so each offers a distinct target. In
/// the slot fixture storage holds every prepare for the run, and each
/// mirror advertises 235 headers per response, so 17 rounds' reservations
/// fit the 4,000-header chunk budget. The queue fills its 16 slots, and the
/// slot cap, not the budget, holds the 17th offer back. In the locator
/// fixture storage holds every locator read until 16 are submitted, so the
/// reads fill the 16 permits and the 17th waits. In the serving fixture 11
/// mirrors sit below the node, so the node serves each of them, and storage
/// holds every serving read until 8 are submitted, so the reads fill the
/// node's 8 execution slots and the rest wait. Each row reaches its limit
/// and no further. The harness's step budget ends these runs before release
/// and reuse, which the committed witnesses cover at their own scale.
#[test]
fn the_limit_fixtures_reach_and_hold_their_bounds() {
    for (stem, row, limit, sessions) in [
        ("header-outstanding-slot-limit", "work slots", 16, 17),
        ("header-outstanding-locator-limit", "locator reads", 16, 17),
        (
            "header-outstanding-serving-limit",
            "node execution slots",
            8,
            11,
        ),
    ] {
        let scenario = scenario(stem);
        let prepared = Storage::prepare(&scenario);
        let evidence = prepared.outstanding.clone();
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        let report = run_prepared::<Storage>(
            &scenario,
            prepared,
            Plan::from_fuzz(&plan),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{stem}: {error}"));
        assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
        assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
        held(stem, &report);
        let peaks = evidence.peaks();
        assert_eq!(peaks.get(row), Some(&limit), "{stem}: {peaks:?}");
        assert_eq!(
            peaks.get("session handles"),
            Some(&sessions),
            "{stem}: {peaks:?}"
        );
    }
}

/// Checks one capacity-waits run: no violation or harness failure, the
/// property held, the acquire was refused `CapacityBusy` both before and
/// after the commit, and the publisher kept one wait at a time.
fn refused_twice(label: &str, report: &RunReport, evidence: &OutstandingEvidence) {
    assert_eq!(report.end.violation, None, "{label}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{label}: {:?}", report.end);
    held(label, report);
    let refusals: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Completed {
                        result: StorageResult::CapacityBusy { .. },
                        generation,
                        ..
                    },
                ..
            } => Some(*generation),
            _ => None,
        })
        .collect();
    assert!(
        refusals.len() >= 2 && refusals.first() < refusals.last(),
        "{label}: refused before and after the commit: {refusals:?}"
    );
    let peaks = evidence.peaks();
    assert_eq!(
        peaks.get("publisher capacity waits"),
        Some(&1),
        "{label}: {peaks:?}"
    );
}

/// The capacity-waits fixture: one mirror sits below the node, so the node
/// serves it, and storage holds its first serving read, which must finish
/// within 1 s, so the clock can't reach the old lease's 30 s idle deadline
/// while it's held. The mirror then relinks, so its new session's page job
/// finds the old lease still live, and the node's storage refuses the
/// acquire `CapacityBusy` for the mirror's one lease. The publisher
/// registers a wait for that refusal. A commit, offered once storage has
/// queued the refusal, changes the node's selected chain. Either the
/// publisher took the refusal first, so its subscription stalls on the
/// wait and the commit clears the stall, or the commit came first, so the
/// refusal installs no stall. Either way the next pass's pruning must drop
/// a wait no stall references. The retried acquire is refused again under
/// the new generation, after which the read is released. Under its
/// committed Plan and generated ones, the publisher keeps one wait at a
/// time and the property holds.
#[test]
fn the_capacity_waits_fixture_refuses_twice_and_holds() {
    let stem = "header-outstanding-capacity-waits";
    let scenario = scenario(stem);
    let committed =
        fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
    let mut plans = vec![Plan::from_fuzz(&committed)];
    plans.extend(inputs(8, 256));
    for (index, input) in plans.into_iter().enumerate() {
        let prepared = Storage::prepare(&scenario);
        let evidence = prepared.outstanding.clone();
        let report =
            run_prepared::<Storage>(&scenario, prepared, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{stem} plan {index}: {error}"));
        refused_twice(&format!("plan {index}"), &report, &evidence);
    }
}

/// The capacity-waits fixture with the commit put off until nothing else
/// but a time advance can run, so the publisher takes the first refusal
/// and stalls on its wait before the commit. The committed Plan takes the
/// commit as soon as it's offered, the other order. The history holds in
/// both as [`refused_twice`] checks, and `waits-kept` breaks in both.
#[test]
fn a_late_commit_still_refuses_twice() {
    let scenario = scenario("header-outstanding-capacity-waits");
    let rule = |cx: &StepCtx<'_>| -> Result<Steer, String> {
        if !cx.pick.to_string().ends_with(":commit") {
            return Ok(Steer::Fine);
        }
        Ok(cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .find(|name| !name.ends_with(":commit") && !name.starts_with("advance-time"))
            .map_or(Steer::Fine, Steer::Pick))
    };
    let (report, evidence) = steered(&scenario, &rule);
    refused_twice("late commit", &report, &evidence);
}

/// `scenario` steered by `rule` from the default Plan, with its
/// outstanding-work peaks.
fn steered(scenario: &StorageScenario, rule: &Rule<'_>) -> (RunReport, OutstandingEvidence) {
    let mut evidence = None;
    let (report, _, _) = steer(Plan::from_fuzz(&[]), rule, &mut |plan, checker| {
        let prepared = Storage::prepare(scenario);
        evidence = Some(prepared.outstanding.clone());
        let mut sink = MemorySink::default();
        let report =
            match run_prepared::<Storage>(scenario, prepared, plan, &mut sink, Some(checker)) {
                Ok(report) => report,
                Err(HarnessError::Run { report, .. }) => *report,
                Err(error) => panic!("{error}"),
            };
        (report, sink.lines())
    });
    (
        report,
        evidence.expect("the steered run prepared its ports"),
    )
}

/// The capacity-waits fixture with the relink put off: every step that can
/// advance time does while the relink waits. The held read's deadline
/// stops the clock first, so the old lease is still live at the relink,
/// and the history is the same.
#[test]
fn a_delayed_relink_still_finds_the_lease_live() {
    let scenario = scenario("header-outstanding-capacity-waits");
    let rule = |cx: &StepCtx<'_>| -> Result<Steer, String> {
        let names: Vec<String> = cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .collect();
        let relink = names.iter().any(|name| name.ends_with(":relink"));
        Ok(
            match names.iter().find(|name| name.starts_with("advance-time")) {
                Some(advance) if relink => Steer::Pick(advance.clone()),
                _ => Steer::Fine,
            },
        )
    };
    let (report, evidence) = steered(&scenario, &rule);
    refused_twice("delayed relink", &report, &evidence);
}
