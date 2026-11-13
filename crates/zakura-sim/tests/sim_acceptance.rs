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

/// The first difference between two records names its path, so a record
/// from another run, or another run's result file, is refused with where
/// they differ.
#[test]
fn a_record_differs_from_another_run_at_a_named_path() {
    use zakura_sim::sim_acceptance::header::first_difference;
    let published = json!({
        "subject": "s", "artifacts": "runs/a",
        "verified": [{ "key": "k1", "links": ["runs/a/1"] }], "stray": [],
    });
    assert_eq!(first_difference(&published, &published.clone()), None);
    let mut other_run = published.clone();
    other_run["verified"][0]["links"][0] = json!("runs/b/1");
    assert_eq!(
        first_difference(&published, &other_run).as_deref(),
        Some("verified.0.links.0")
    );
    let mut shorter = published.clone();
    shorter["verified"] = json!([]);
    assert_eq!(
        first_difference(&published, &shorter).as_deref(),
        Some("verified.0")
    );
    let mut extra = published.clone();
    extra["blocked"] = json!([]);
    assert_eq!(
        first_difference(&published, &extra).as_deref(),
        Some("blocked")
    );
}

mod evaluator {
    use std::collections::BTreeMap;

    use zakura_sim::sim_acceptance::evaluate::{
        evaluate, header_refusal, judge_row, HeaderRecordFacts, RowSpec, Verdict,
    };

    fn row(id: &str, results: &[&str], never_excluded: bool) -> RowSpec {
        RowSpec {
            id: id.to_owned(),
            results: results.iter().map(|r| (*r).to_owned()).collect(),
            never_excluded,
        }
    }

    fn excluded() -> Verdict {
        Verdict::Excluded {
            approval: "Adam, 2026-11-11".to_owned(),
        }
    }

    fn blocked() -> Verdict {
        Verdict::Blocked {
            why: "failed".to_owned(),
        }
    }

    fn verdicts(pairs: &[(&str, Verdict)]) -> BTreeMap<String, Verdict> {
        pairs
            .iter()
            .map(|(id, v)| ((*id).to_owned(), v.clone()))
            .collect()
    }

    /// A row with one excluded and one blocked result is blocked.
    #[test]
    fn a_row_takes_its_worst_result() {
        let v = verdicts(&[
            ("a", excluded()),
            ("b", blocked()),
            ("c", Verdict::Verified),
        ]);
        assert_eq!(
            judge_row(&row("M7.2", &["a", "b"], false), &v).verdict,
            blocked()
        );
        assert_eq!(
            judge_row(&row("M7.2", &["a", "c"], false), &v).verdict,
            excluded()
        );
        assert_eq!(
            judge_row(&row("M7.2", &["c"], false), &v).verdict,
            Verdict::Verified
        );
    }

    #[test]
    fn a_missing_result_and_an_empty_row_block() {
        let v = verdicts(&[("a", Verdict::Verified)]);
        assert!(matches!(
            judge_row(&row("M7.1", &["a", "gone"], false), &v).verdict,
            Verdict::Blocked { .. }
        ));
        assert!(matches!(
            judge_row(&row("M7.1", &[], false), &v).verdict,
            Verdict::Blocked { .. }
        ));
    }

    #[test]
    fn an_exclusion_blocks_a_row_that_cant_be_excluded() {
        let v = verdicts(&[("a", excluded())]);
        assert!(matches!(
            judge_row(&row("S4.4", &["a"], true), &v).verdict,
            Verdict::Blocked { .. }
        ));
    }

    #[test]
    fn acceptance_needs_no_block_and_a_positive_applicable_count() {
        let rows = [row("M7.1", &["a"], false), row("M7.2", &["b"], false)];
        let all = verdicts(&[("a", Verdict::Verified), ("b", Verdict::Verified)]);
        let done = evaluate(&rows, &all, 5);
        assert!(done.accepted && done.fully_accepted);
        let nothing_applied = evaluate(&rows, &all, 0);
        assert!(
            !nothing_applied.accepted,
            "a suite where nothing applied isn't accepted"
        );
        let some_excluded = evaluate(
            &rows,
            &verdicts(&[("a", Verdict::Verified), ("b", excluded())]),
            5,
        );
        assert!(some_excluded.accepted && !some_excluded.fully_accepted);
        let one_blocked = evaluate(
            &rows,
            &verdicts(&[("a", Verdict::Verified), ("b", blocked())]),
            5,
        );
        assert!(!one_blocked.accepted);
    }

    fn facts() -> HeaderRecordFacts {
        HeaderRecordFacts {
            subject: "S".to_owned(),
            subreaper: true,
            accepted: true,
            stray: Vec::new(),
            verified: Ok(()),
        }
    }

    /// The header record counts only at S, contained, accepted, with no
    /// stray and bound to its run.
    #[test]
    fn the_header_record_is_refused_unless_every_fact_holds() {
        assert_eq!(header_refusal(&facts(), "S"), None);
        assert!(header_refusal(&facts(), "T").is_some());
        let refused = |change: fn(&mut HeaderRecordFacts)| {
            let mut f = facts();
            change(&mut f);
            header_refusal(&f, "S").is_some()
        };
        assert!(refused(|f| f.subreaper = false));
        assert!(refused(|f| f.accepted = false));
        assert!(refused(|f| f.stray.push("a timed-out child".to_owned())));
        assert!(refused(
            |f| f.verified = Err("differs at `verified.0`".to_owned())
        ));
    }
}

mod sched_mapping {
    use zakura_sim::{
        seed,
        sim_acceptance::sched::{Proof, TestRef, SCHED_IDS, SCHED_MAPPING},
        suite::{SuiteFile, SUITE_FILE},
    };

    /// Whether `test` names a test function in its package's test file.
    fn exists(test: &TestRef) -> bool {
        let file = seed::repo_root()
            .join("crates")
            .join(test.package)
            .join(test.file);
        std::fs::read_to_string(file).is_ok_and(|text| text.contains(&format!("fn {}(", test.test)))
    }

    /// The mapping covers exactly the specs' IDs, once each, in order.
    #[test]
    fn every_sched_id_has_one_mapping_row() {
        let mapped: Vec<&str> = SCHED_MAPPING.iter().map(|(id, _)| *id).collect();
        assert_eq!(mapped, SCHED_IDS);
        assert!(SCHED_MAPPING.iter().all(|(_, proofs)| !proofs.is_empty()));
    }

    /// Every proof names a real test or a committed lane.
    #[test]
    fn every_sched_proof_exists() {
        let text = std::fs::read_to_string(seed::repo_root().join(SUITE_FILE)).unwrap();
        let suite: SuiteFile = toml::from_str(&text).unwrap();
        for (id, proofs) in SCHED_MAPPING {
            for proof in *proofs {
                match proof {
                    Proof::Test(test) => assert!(exists(test), "{id}: {test:?}"),
                    Proof::Lane(lane) => {
                        assert!(
                            suite.lane.iter().any(|l| l.id == *lane),
                            "{id}: lane {lane}"
                        )
                    }
                }
            }
        }
    }

    #[test]
    fn a_misspelled_test_doesnt_exist() {
        assert!(!exists(&TestRef {
            package: "zakura-scheduler",
            file: "tests/fixtures.rs",
            test: "no_such_test",
        }));
    }
}

mod schedule_agreement {
    use serde_json::json;
    use zakura_sim::sim_acceptance::agreement::{
        agreement, first_differing_step, without_lines, HunkMap, Site, StepView,
    };

    /// A patch that removes three lines (10 to 12) of `src/peer.rs`.
    const PATCH: &str = "--- a/src/peer.rs\n+++ b/src/peer.rs\n@@ -10,3 +10,0 @@\n-a\n-b\n-c\n";

    fn reach(step: u64, pick: &str, site: &str) -> StepView {
        StepView {
            step,
            pick: pick.to_owned(),
            draws: Vec::new(),
            obs: vec![json!({ "obs": "task-reached", "task": "t", "at": "lock#1", "site": site })],
        }
    }

    #[test]
    fn lines_shift_past_a_hunk_and_vanish_inside_it() {
        let map = HunkMap::parse(PATCH).unwrap();
        assert_eq!(map.map("src/peer.rs", 5), Some(5));
        assert_eq!(map.map("src/peer.rs", 11), None);
        assert_eq!(map.map("src/peer.rs", 20), Some(17));
        assert_eq!(map.map("src/other.rs", 20), Some(20));
    }

    /// Raw equality would split at the first lock, whose line the removed
    /// lines move. The hunk map lines it up, and the runs first differ at
    /// the declared site.
    #[test]
    fn a_moved_line_agrees_and_the_first_difference_is_at_the_site() {
        let map = HunkMap::parse(PATCH).unwrap();
        let before = [
            reach(1, "task:t", "src/peer.rs:20"),
            reach(2, "task:t", "src/peer.rs:30"),
        ];
        let after = [
            reach(1, "task:t", "src/peer.rs:17"),
            reach(2, "task:t", "src/peer.rs:40"),
        ];
        assert_eq!(
            first_differing_step(&before, &after, &HunkMap::default()),
            Some(1)
        );
        let site = Site {
            at: "src/peer.rs:40".to_owned(),
        };
        assert_eq!(agreement(&before, &after, &map, &site), Ok(2));
    }

    /// An earlier change in the same function, to the same kind of
    /// operation, makes the runs differ before the site, which fails.
    #[test]
    fn an_earlier_difference_than_the_site_fails() {
        let map = HunkMap::parse(PATCH).unwrap();
        let before = [
            reach(1, "task:t", "src/peer.rs:20"),
            reach(2, "task:t", "src/peer.rs:30"),
        ];
        let after = [
            reach(1, "task:t", "src/peer.rs:18"),
            reach(2, "task:t", "src/peer.rs:40"),
        ];
        let site = Site {
            at: "src/peer.rs:40".to_owned(),
        };
        assert!(agreement(&before, &after, &map, &site).is_err());
    }

    /// A line a hunk replaced never matches, so a step there differs.
    #[test]
    fn a_line_inside_a_hunk_never_matches() {
        let map = HunkMap::parse(PATCH).unwrap();
        let before = [reach(1, "task:t", "src/peer.rs:11")];
        let after = [reach(1, "task:t", "src/peer.rs:11")];
        assert_eq!(first_differing_step(&before, &after, &map), Some(1));
    }

    /// The declared site must be the difference itself: a site pick
    /// beside a different draw, or a site observation both runs hold
    /// beside another that differs, fails.
    #[test]
    fn a_site_beside_an_unrelated_difference_fails() {
        let map = HunkMap::default();
        let pick = Site {
            at: "task:t:poll#1=>task-exited".to_owned(),
        };
        let before = [reach(1, "task:t:poll#1", "src/a.rs:1")];
        let mut after = [reach(1, "task:t:poll#1", "src/a.rs:1")];
        after[0].draws = before[0].draws.clone();
        after[0]
            .obs
            .push(json!({ "obs": "task-exited", "task": "t" }));
        assert_eq!(
            agreement(&before, &after, &map, &pick),
            Ok(1),
            "the obs after the pick differ"
        );
        let mut drawn: Vec<StepView> = after.to_vec();
        drawn[0].obs = before[0].obs.clone();
        drawn[0].draws = serde_json::from_value(json!([
            { "draw": "t:rng#1", "bound": "0x0", "value": "0x1", "by": "bytes" }
        ]))
        .unwrap();
        assert!(
            agreement(&before, &drawn, &map, &pick).is_err(),
            "only a draw differs"
        );
        let mut other_task: Vec<StepView> = after.to_vec();
        other_task[0].obs[1] = json!({ "obs": "task-exited", "task": "u" });
        assert!(
            agreement(&before, &other_task, &map, &pick).is_err(),
            "a change after the site's pick that isn't the picked task's"
        );
        let mut wrong_op: Vec<StepView> = before.to_vec();
        wrong_op[0]
            .obs
            .push(json!({ "obs": "task-waiting", "task": "t" }));
        assert!(
            agreement(&before, &wrong_op, &map, &pick).is_err(),
            "the picked task's change isn't the observation the site names"
        );
        let mut other_pick: Vec<StepView> = before.to_vec();
        other_pick[0].pick = "task:u:poll#1".to_owned();
        assert!(
            agreement(&other_pick, &before, &map, &pick).is_err(),
            "a pick that only differs, with nothing of the picked task's changed"
        );
        let at = Site {
            at: "t:lock#1".to_owned(),
        };
        let mut moved = before.to_vec();
        moved[0].obs[0]["at"] = json!("lock#0");
        let mut site_after_other: Vec<StepView> = before.to_vec();
        site_after_other[0]
            .obs
            .insert(0, json!({ "obs": "task-waiting", "task": "u" }));
        assert!(
            agreement(&moved, &site_after_other, &map, &at).is_err(),
            "an earlier different observation in the same step than the site's"
        );
        let shared = Site {
            at: "src/a.rs:1".to_owned(),
        };
        assert!(
            agreement(&before, &after, &map, &shared).is_err(),
            "both runs hold the site's observation"
        );
    }

    /// Two equivalent operations on different lines of two code paths
    /// match once the lines are dropped, so the first difference is the
    /// next one in an operation, not the location.
    #[test]
    fn a_location_only_difference_vanishes_without_lines() {
        let before = [
            reach(1, "task:t", "src/a.rs:10"),
            reach(2, "task:t", "src/a.rs:11"),
        ];
        let mut after = [
            reach(1, "task:t", "src/a.rs:20"),
            reach(2, "task:t", "src/a.rs:21"),
        ];
        after[1].obs[0]["at"] = json!("cancelled#1");
        let map = HunkMap::default();
        assert_eq!(first_differing_step(&before, &after, &map), Some(1));
        let (before, after) = (
            without_lines(before.to_vec()),
            without_lines(after.to_vec()),
        );
        assert_eq!(first_differing_step(&before, &after, &map), Some(2));
        let site = Site {
            at: "t:cancelled#1".to_owned(),
        };
        assert_eq!(agreement(&before, &after, &map, &site), Ok(2));
    }

    #[test]
    fn runs_that_never_differ_fail() {
        let before = [reach(1, "task:t", "src/x.rs:1")];
        let site = Site {
            at: "src/x.rs:1".to_owned(),
        };
        assert!(agreement(&before, &before.clone(), &HunkMap::default(), &site).is_err());
    }
}

mod deviations {
    use std::collections::BTreeSet;

    use zakura_sim::sim_acceptance::deviations::{Evan, DEVIATIONS};

    /// The plan's narrowings, each listed once, and none counts as signed
    /// off without every sign-off it needs.
    #[test]
    fn each_narrowing_is_listed_once_with_its_sign_offs() {
        let ids: BTreeSet<&str> = DEVIATIONS.iter().map(|d| d.id).collect();
        assert_eq!(ids.len(), DEVIATIONS.len(), "an ID is listed twice");
        for id in [
            "decision-2",
            "decision-3",
            "decision-7",
            "decision-8",
            "s4.6-sim-cfg-scope",
            "rule-9-release-and-archive",
        ] {
            assert!(ids.contains(id), "{id}");
        }
        for deviation in DEVIATIONS {
            assert!(
                !deviation.spec.is_empty() && !deviation.claim.is_empty(),
                "{}",
                deviation.id
            );
            if deviation.evan == Evan::Pending {
                assert!(!deviation.is_signed_off(), "{}", deviation.id);
            }
            if deviation.approval.is_none() {
                assert!(!deviation.is_signed_off(), "{}", deviation.id);
            }
        }
    }
}

mod manifest {
    use std::sync::OnceLock;

    use zakura_sim::{
        harness::header_acceptance::{tool::committed_required, Manifest},
        seed,
        sim_acceptance::manifest::{derive, problems, rows, Derived, Expected},
        suite::{SuiteFile, SUITE_FILE},
    };

    /// The derived families at this checkout, read once.
    fn derived() -> &'static Derived {
        static DERIVED: OnceLock<Derived> = OnceLock::new();
        DERIVED.get_or_init(|| derive(&seed::repo_root()).unwrap())
    }

    fn read(path: &str) -> String {
        std::fs::read_to_string(seed::repo_root().join(path)).unwrap_or_default()
    }

    /// The rows' fixed content is pinned: unchanged, it matches the pin,
    /// and changing one field of one row moves it.
    #[test]
    fn a_changed_row_field_fails_the_pin() {
        use zakura_sim::sim_acceptance::manifest::{
            rows, rows_digest, rows_digest_of, Derived, ROWS_SHA256,
        };
        assert_eq!(rows_digest(), ROWS_SHA256);
        let mut changed = rows(&Derived::default());
        changed[0].never_excluded = !changed[0].never_excluded;
        assert_ne!(rows_digest_of(&changed), ROWS_SHA256);
        let mut fewer = rows(&Derived::default());
        let row = fewer.iter_mut().find(|row| row.expected.len() > 1).unwrap();
        row.expected.pop();
        assert_ne!(rows_digest_of(&fewer), ROWS_SHA256);
    }

    #[test]
    fn the_committed_manifest_has_no_problems() {
        let derived = derived();
        assert!(!derived.seeds.is_empty() && !derived.end_records.is_empty());
        assert_eq!(
            derived.race_schedules.len(),
            6,
            "{:?}",
            derived.race_schedules
        );
        assert!(!derived.table_properties.is_empty());
        assert_eq!(problems(&rows(derived), derived), Vec::<String>::new());
    }

    /// Every result names something that exists at this checkout.
    #[test]
    fn every_named_result_exists() {
        let suite: SuiteFile = toml::from_str(&read(SUITE_FILE)).unwrap();
        let required = committed_required(&seed::repo_root(), &Manifest::committed())
            .expect("the committed inventories read, as hdr-14's own inventory test checks");
        let adapter = read("qa/fuzz/sim/src/fuzz/adapter.rs");
        for row in rows(derived()) {
            for expected in &row.expected {
                let ok = match expected {
                    Expected::Test(t) => read(&format!("crates/{}/{}", t.package, t.file))
                        .contains(&format!("fn {}(", t.test)),
                    Expected::UnitTest { package, path } => {
                        // `a::b::tests::name` is in `src/a/b.rs`.
                        let segments: Vec<&str> = path.split("::").collect();
                        let (name, module) = (
                            segments[segments.len() - 1],
                            &segments[..segments.len() - 2],
                        );
                        read(&format!("crates/{package}/src/{}.rs", module.join("/")))
                            .contains(&format!("fn {name}("))
                    }
                    Expected::FuzzTest(path) => {
                        adapter.contains(&format!("fn {}(", path.rsplit("::").next().unwrap()))
                    }
                    Expected::Lane(id) => suite.lane.iter().any(|lane| lane.id == *id),
                    Expected::HeaderKey(key) => required.contains_key(key),
                    _ => true,
                };
                assert!(ok, "{}: {expected}", row.id);
            }
        }
    }

    /// A row that loses one derived member, or a requirement that loses its
    /// row, is a problem, so the evaluator refuses the manifest.
    #[test]
    fn a_manifest_missing_a_derived_member_or_a_row_is_refused() {
        let derived = derived();
        let mut short = rows(derived);
        let saved = short
            .iter_mut()
            .find(|r| r.id == "corpus/saved-plan-suite")
            .unwrap();
        saved.expected.pop();
        assert_eq!(problems(&short, derived).len(), 1);
        let mut missing = rows(derived);
        missing.retain(|r| r.id != "S3.4");
        assert_eq!(problems(&missing, derived), ["requirement S3.4 has no row"]);
        let mut extra_seed = derived.clone();
        extra_seed
            .seeds
            .push("crates/zakura-sim/tests/fixtures/new.bin".to_owned());
        assert_eq!(problems(&rows(derived), &extra_seed).len(), 1);
    }
}
