//! E1's generator guard: every harness's default scenario and each of its
//! scenario families, a correct node cut at its harness's budget, drains to
//! a dry end, never to its bound.
#![cfg(zakura_sim)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};

use zakura_sim::{
    harness::HARNESSES, HarnessFailure, NullSink, Outcome, PropertyOutcome, RunReport,
};

/// A property a run must end as `expect` says: `holds`, `applicable` or
/// `violated`.
#[derive(Clone, Debug)]
struct Expected {
    property: String,
    expect: String,
}

/// The scenario families the sweep skips, as `harness/family`: each run
/// takes minutes, too long for the sweep.
const SLOW_FAMILIES: [&str; 3] = [
    "block-sync/storage/stuck-floor",
    "block-sync/storage/above-window",
    "block-sync/storage/decoded-input-stall",
];

/// What's wrong with a run's end, if anything. `report` is `None` when the
/// harness couldn't run the input at all. The sweep runs a correct node, so
/// a run ends with no harness failure, and with no violation but of a
/// property `witnesses` expects violated. A budget end's drain ran dry, and
/// each witness's property ends as it expects.
fn misend(report: Option<&RunReport>, witnesses: &[Expected]) -> Option<String> {
    let Some(report) = report else {
        return Some("the harness couldn't run it".to_owned());
    };
    let end = &report.end;
    if let Some(failure) = &end.harness {
        return Some(format!("a harness failure: {failure}"));
    }
    let outcome = |property: &str| {
        report
            .properties
            .iter()
            .find(|report| report.property == property)
            .map(|report| &report.outcome)
    };
    if end.outcome == Outcome::Budget && end.drain.is_none_or(|drain| drain.exhausted) {
        return Some(format!("a budget end without a dry drain: {:?}", end.drain));
    }
    let expected = |property: &str| {
        witnesses
            .iter()
            .any(|w| w.expect == "violated" && w.property == property)
    };
    // Every violated report counts, not just the stopping violation: a Draft
    // violation doesn't stop the run, and a dry end concludes open
    // obligations without setting `end.violation`.
    let stopping = end.violation.iter().map(|v| v.property.as_str());
    let reported = report
        .properties
        .iter()
        .filter(|report| matches!(report.outcome, PropertyOutcome::Violated { .. }))
        .map(|report| report.property.as_str());
    if let Some(property) = stopping.chain(reported).find(|p| !expected(p)) {
        return Some(format!("a violation of {property}"));
    }
    witnesses.iter().find_map(|witness| {
        let ended = outcome(&witness.property);
        let fits = matches!(
            (witness.expect.as_str(), ended),
            ("holds" | "applicable", Some(PropertyOutcome::Held { .. }))
                | ("violated", Some(PropertyOutcome::Violated { .. }))
        );
        (!fits).then(|| {
            format!(
                "{} expected {}, got {ended:?}",
                witness.property, witness.expect
            )
        })
    })
}

/// Each harness's inputs for the sweep, named: the empty input, which runs
/// the default scenario under the empty Plan, then each scenario family's
/// tag under the empty Plan. Machines have no drain, and a harness that
/// plants its bugs must violate its properties, so neither runs.
fn sweep() -> Vec<(String, &'static zakura_sim::harness::HarnessEntry, Vec<u8>)> {
    let mut inputs = Vec::new();
    for entry in HARNESSES.iter().filter(|entry| !entry.machine && !entry.planted) {
        inputs.push((format!("{} default", entry.name), entry, Vec::new()));
        for family in (entry.families)() {
            let name = format!("{}/{}", entry.name, family.name);
            if !SLOW_FAMILIES.contains(&name.as_str()) {
                inputs.push((name, entry, family.tag));
            }
        }
    }
    inputs
}

/// Every harness's default scenario and scenario families ([`sweep`]), run
/// as a campaign would, end with no violation ([`misend`]): once the budget
/// cuts a run, the drain finishes what the live phase started and runs dry
/// within the harness's drain bound. An unbounded generator that never
/// registers ([`zakura_sim::sim::Generators`]) keeps a task ready, so its
/// run hits the bound and fails here, naming the task. The inputs run on a
/// few threads, each run on its own.
#[test]
fn every_saved_plan_drains_dry() {
    let inputs = sweep();
    assert!(!inputs.is_empty(), "the sweep has inputs");
    let next = AtomicUsize::new(0);
    let misended = Mutex::new(Vec::new());
    let threads = thread::available_parallelism().map_or(1, |n| n.get().min(8));
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                while let Some((name, entry, input)) =
                    inputs.get(next.fetch_add(1, Ordering::Relaxed))
                {
                    let run = (entry.run_bytes)(input, &mut NullSink, None);
                    if let Some(why) = misend(run.report(), &[]) {
                        misended
                            .lock()
                            .expect("no thread panics holding it")
                            .push(format!("{name}: {why}"));
                    }
                }
            });
        }
    });
    let misended = misended.into_inner().expect("every thread finished");
    assert!(misended.is_empty(), "runs that misended: {misended:#?}");
}

/// Terra 2, round 4: a saved run that fails early, that the harness can't
/// run, or that ends at its budget without a dry drain misends, as does
/// one whose witness doesn't end as its sidecar expects or that reports a
/// violation no witness expects. A dry drain whose witness holds doesn't.
#[test]
fn misend_rejects_every_unexpected_end() {
    use zakura_sim::{end::Drain, PropertyReport, RunEnd};
    let report = |outcome, drain: Option<Drain>, harness, held: bool| RunReport {
        end: RunEnd {
            outcome,
            steps: 3,
            violation: None,
            drain,
            harness,
            rest: None,
        },
        properties: vec![PropertyReport::new(
            "p",
            if held {
                PropertyOutcome::Held { applicable: 1 }
            } else {
                PropertyOutcome::Violated { witness: vec![3] }
            },
        )],
        cleanup: Default::default(),
        log: Default::default(),
        picks: Vec::new(),
        resolved: Default::default(),
    };
    let dry = Some(Drain {
        steps: 1,
        exhausted: false,
    });
    let holds = [Expected {
        property: "p".to_owned(),
        expect: "holds".to_owned(),
    }];
    let stranded = HarnessFailure::SourceEvidence {
        source: "source 0".to_owned(),
        detail: "stranded".to_owned(),
    };
    assert_eq!(
        misend(Some(&report(Outcome::Budget, dry, None, true)), &holds),
        None
    );
    for (bad, why) in [
        (
            Some(report(Outcome::Harness, None, Some(stranded), true)),
            "a harness failure",
        ),
        (None, "no report"),
        (
            Some(report(Outcome::Budget, None, None, true)),
            "a budget end with no drain",
        ),
        (
            Some(report(Outcome::Budget, dry, None, false)),
            "a witness that broke",
        ),
        (
            Some(report(Outcome::Violation, None, None, false)),
            "a violation",
        ),
    ] {
        assert!(misend(bad.as_ref(), &holds).is_some(), "{why}");
    }
    // Sol and Terra, round 5: a dry budget end whose witness holds still
    // misends when an unlisted property, a Draft one or one concluded at
    // the dry end, reports a violation.
    let mut other = report(Outcome::Budget, dry, None, true);
    other.properties.push(PropertyReport::new(
        "q",
        PropertyOutcome::Violated { witness: vec![2] },
    ));
    assert!(
        misend(Some(&other), &holds).is_some(),
        "an unlisted violation"
    );
    let expects_q = [
        holds[0].clone(),
        Expected {
            property: "q".to_owned(),
            expect: "violated".to_owned(),
        },
    ];
    assert_eq!(misend(Some(&other), &expects_q), None);
}
