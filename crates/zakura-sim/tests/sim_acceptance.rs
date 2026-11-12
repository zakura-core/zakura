//! sim-7.4: the controls of the one judge every acceptance run passes
//! through (the sim-7 plan's Rule 10), and its cleanup table.
#![cfg(zakura_sim)]

use serde_json::{json, Value};
use zakura_sim::{
    record::{CleanupRecord, EndRecord},
    sim_acceptance::{
        judge::cleanup_counts, owned_run_passes, Blocked, CleanupMeaning, Registered, RunExpect,
        CLEANUP,
    },
};

const OWN: &str = "test/own";
const OTHER: &str = "test/other";
const KNOWN: &str = "test/known";

/// The harness's registered properties: two `Active`, one `Known`.
fn registered() -> Registered {
    Registered {
        ids: [OWN, OTHER, KNOWN].map(str::to_owned).into(),
        tolerated: [KNOWN.to_owned()].into(),
    }
}

/// A report of `property` with `outcome`, as an end record holds it.
fn report(property: &str, outcome: Value) -> Value {
    json!({ "property": property, "outcome": outcome })
}

fn held() -> Value {
    json!({ "held": { "applicable": 3 } })
}

fn violated() -> Value {
    json!({ "violated": { "witness": [4] } })
}

fn not_applicable(reason: &str) -> Value {
    json!({ "not-applicable": { "reason": reason } })
}

/// An end record from its JSON parts, with a clean cleanup unless
/// `cleanup` overrides fields.
fn end(outcome: &str, extra: Value, properties: Vec<Value>, cleanup: Value) -> EndRecord {
    let mut end = json!({
        "outcome": outcome,
        "steps": 9,
        "drain": null,
        "cleanup": {
            "cancelled": 0, "panicked": 0, "unwound": 0,
            "unwind_failures": 0, "leaked": 0, "timers_left": 0,
        },
        "properties": properties,
    });
    for (key, value) in extra.as_object().unwrap() {
        end[key] = value.clone();
    }
    for (key, value) in cleanup.as_object().unwrap() {
        end["cleanup"][key] = value.clone();
    }
    serde_json::from_value(end).unwrap()
}

/// A run that ends at its budget with every property holding.
fn clean(cleanup: Value) -> EndRecord {
    end(
        "budget",
        json!({}),
        vec![
            report(OWN, held()),
            report(OTHER, held()),
            report(KNOWN, held()),
        ],
        cleanup,
    )
}

/// A run that ends at a violation of `OWN`, with `other` as `OTHER`'s
/// outcome.
fn violating(other: Value) -> EndRecord {
    end(
        "violation",
        json!({ "violation": {
            "property": OWN, "step": 4, "witness": [4], "witness_keys": ["task:a"],
            "expected": "e", "actual": "a",
        }}),
        vec![
            report(OWN, violated()),
            report(OTHER, other),
            report(KNOWN, held()),
        ],
        json!({}),
    )
}

/// A run that ends at the harness failure `diagnostic`, every property
/// `HarnessFailed`.
fn harness_failed(diagnostic: &str, cleanup: Value) -> EndRecord {
    let na = || not_applicable("harness-failed");
    end(
        "harness",
        json!({ "harness": diagnostic }),
        vec![report(OWN, na()), report(OTHER, na()), report(KNOWN, na())],
        cleanup,
    )
}

fn expect_violation() -> RunExpect {
    RunExpect::Violates {
        property: OWN.to_owned(),
        also: Vec::new(),
    }
}

fn integrity() -> RunExpect {
    RunExpect::HarnessFails {
        diagnostic: "OwnedMismatch".to_owned(),
    }
}

#[test]
fn a_clean_run_passes() {
    assert_eq!(
        owned_run_passes(&clean(json!({})), &registered(), &RunExpect::Passes),
        Ok(())
    );
}

#[test]
fn ordinary_teardown_and_timers_dont_block() {
    let run = clean(json!({ "cancelled": 2, "unwound": 1, "timers_left": 3 }));
    assert_eq!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Ok(())
    );
}

/// A task that panics while shutdown drops it sets `panicked` and can
/// leave `leaked == 0`: the run still blocks.
#[test]
fn a_cleanup_panic_without_a_leak_blocks() {
    let run = clean(json!({ "panicked": 1, "leaked": 0 }));
    assert_eq!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Err(Blocked::Cleanup {
            field: "panicked",
            count: 1
        })
    );
}

#[test]
fn leaks_and_caught_unwinds_block() {
    for field in ["leaked", "unwind_failures"] {
        let run = clean(json!({ field: 1 }));
        assert!(
            matches!(
                owned_run_passes(&run, &registered(), &RunExpect::Passes),
                Err(Blocked::Cleanup { field: f, .. }) if f == field
            ),
            "{field}"
        );
    }
}

/// A seed with no witness whose reports pass but one is incomplete
/// evidence blocks.
#[test]
fn an_incomplete_reason_on_a_passing_run_blocks() {
    let run = end(
        "idle",
        json!({ "status": "quiescent" }),
        vec![
            report(OWN, held()),
            report(OTHER, not_applicable("assumption-lost")),
            report(KNOWN, held()),
        ],
        json!({}),
    );
    assert_eq!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Err(Blocked::Incomplete {
            property: OTHER.to_owned(),
            reason: "assumption-lost".to_owned()
        })
    );
}

#[test]
fn a_benign_reason_passes() {
    let run = end(
        "budget",
        json!({}),
        vec![
            report(OWN, held()),
            report(OTHER, not_applicable("never-applied")),
            report(KNOWN, held()),
        ],
        json!({}),
    );
    assert_eq!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Ok(())
    );
}

#[test]
fn an_unreported_property_blocks() {
    let run = end(
        "budget",
        json!({}),
        vec![report(OWN, held()), report(KNOWN, held())],
        json!({}),
    );
    assert_eq!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Err(Blocked::Unreported(OTHER.to_owned()))
    );
}

/// An expected-violation run whose stop leaves another property
/// `StoppedByViolation` passes, and the same reason on a passing run
/// blocks.
#[test]
fn stopped_by_violation_only_on_the_expected_violation() {
    let stopped = violating(not_applicable("stopped-by-violation"));
    assert_eq!(
        owned_run_passes(&stopped, &registered(), &expect_violation()),
        Ok(())
    );
    let passing = end(
        "budget",
        json!({}),
        vec![
            report(OWN, held()),
            report(OTHER, not_applicable("stopped-by-violation")),
            report(KNOWN, held()),
        ],
        json!({}),
    );
    assert!(matches!(
        owned_run_passes(&passing, &registered(), &RunExpect::Passes),
        Err(Blocked::Incomplete { .. })
    ));
}

#[test]
fn an_unexpected_violation_blocks_and_a_known_one_doesnt() {
    let other = violating(violated());
    assert_eq!(
        owned_run_passes(&other, &registered(), &expect_violation()),
        Err(Blocked::UnexpectedViolation(OTHER.to_owned()))
    );
    let also = RunExpect::Violates {
        property: OWN.to_owned(),
        also: vec![OTHER.to_owned()],
    };
    assert_eq!(owned_run_passes(&other, &registered(), &also), Ok(()));
    let known = end(
        "budget",
        json!({}),
        vec![
            report(OWN, held()),
            report(OTHER, held()),
            report(KNOWN, violated()),
        ],
        json!({}),
    );
    assert_eq!(
        owned_run_passes(&known, &registered(), &RunExpect::Passes),
        Ok(())
    );
}

#[test]
fn a_violation_of_another_property_is_the_wrong_end() {
    let run = violating(held());
    let expect = RunExpect::Violates {
        property: OTHER.to_owned(),
        also: vec![OWN.to_owned()],
    };
    assert!(matches!(
        owned_run_passes(&run, &registered(), &expect),
        Err(Blocked::Outcome { .. })
    ));
    assert!(matches!(
        owned_run_passes(&run, &registered(), &RunExpect::Passes),
        Err(Blocked::Outcome { .. })
    ));
}

/// A correct integrity catch passes. The wrong diagnostic, a dirty
/// cleanup, and `HarnessFailed` on a run expected to pass each block.
#[test]
fn an_expected_harness_failure_has_its_own_rule() {
    let caught = harness_failed("OwnedMismatch", json!({}));
    assert_eq!(
        owned_run_passes(&caught, &registered(), &integrity()),
        Ok(())
    );
    let wrong = harness_failed("SomethingElse", json!({}));
    assert!(matches!(
        owned_run_passes(&wrong, &registered(), &integrity()),
        Err(Blocked::Outcome { .. })
    ));
    let dirty = harness_failed("OwnedMismatch", json!({ "panicked": 1 }));
    assert_eq!(
        owned_run_passes(&dirty, &registered(), &integrity()),
        Err(Blocked::Cleanup {
            field: "panicked",
            count: 1
        })
    );
    let passing = end(
        "budget",
        json!({}),
        vec![
            report(OWN, held()),
            report(OTHER, not_applicable("harness-failed")),
            report(KNOWN, held()),
        ],
        json!({}),
    );
    assert!(matches!(
        owned_run_passes(&passing, &registered(), &RunExpect::Passes),
        Err(Blocked::Incomplete { .. })
    ));
}

/// The table classifies every cleanup field once, in the order the
/// record's counts come.
#[test]
fn the_cleanup_table_names_every_field_once() {
    let names: Vec<&str> = cleanup_counts(&CleanupRecord::default())
        .iter()
        .map(|(name, _)| *name)
        .collect();
    let table: Vec<&str> = CLEANUP.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, table);
    let fields = serde_json::to_value(CleanupRecord::default()).unwrap();
    let mut serialized: Vec<&str> = fields
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let mut sorted = table.clone();
    serialized.sort_unstable();
    sorted.sort_unstable();
    assert_eq!(
        serialized, sorted,
        "the table matches the record's serialized fields"
    );
    let faults: Vec<&str> = CLEANUP
        .iter()
        .filter(|(_, meaning)| *meaning == CleanupMeaning::Fault)
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(faults, ["panicked", "unwind_failures", "leaked"]);
}

#[test]
fn every_block_has_words() {
    let end: EndRecord = clean(json!({ "leaked": 2 }));
    let blocked = owned_run_passes(&end, &registered(), &RunExpect::Passes).unwrap_err();
    assert_eq!(blocked.to_string(), "cleanup `leaked` is 2");
}
