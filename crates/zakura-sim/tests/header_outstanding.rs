//! hdr-9d.1: `header-sync/outstanding-work-bounded` on `header-sync/storage`.
//! Every committed authority and reset witness holds it and applies, under
//! its committed Plan bytes and under generated plans.
//! `cargo xtask planted header-sync/outstanding-work-bounded` checks that its
//! bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf};

use support::inputs;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::OUTSTANDING_WORK_BOUNDED,
    harness::{
        header_sync::{Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    MemorySink, Plan, PlanInput, RunReport,
};

/// Every committed storage witness the outstanding-work gate runs, by its
/// fixture stem.
const WITNESSES: [&str; 9] = [
    "header-authority-forks-sharing-a-header",
    "header-authority-relinked-mid-round",
    "header-reset-held-apply",
    "header-reset-held-reply",
    "header-reset-quiet-reset",
    "header-reset-quiet-reset-deep",
    "header-outstanding-churn",
    "header-outstanding-serving-churn",
    "header-outstanding-serving-replaced",
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
/// the slot fixture storage holds
/// every prepare for the run, so the queue fills its 16 slots and the 17th
/// offer waits. In the locator fixture storage holds every locator read
/// until 16 are submitted, so the reads fill the 16 permits and the 17th
/// waits. In the serving fixture 11 mirrors sit below the node, so the
/// node serves each of them, and storage holds every serving read until 8
/// are submitted, so the reads fill the node's 8 execution slots and the
/// rest wait. Each row reaches its limit and no further. The harness's step
/// budget ends these runs before release and reuse, which the committed
/// witnesses cover at their own scale.
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
