//! E1's generator guard: every committed seed, a correct node cut at its
//! harness's budget, drains to a dry end, never to its bound.
#![cfg(zakura_sim)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};

use zakura_sim::{
    harness,
    seed::{self, SeedWitness},
    HarnessFailure, NullSink, Outcome, PropertyOutcome, RunReport,
};

/// What's wrong with a saved run's end, if anything. `report` is `None`
/// when the harness couldn't run the seed at all. A saved seed runs a
/// correct node, so it ends with no harness failure, and with no
/// violation but of a property a witness expects violated. A budget end's
/// drain ran dry, and each witness's property ends as its sidecar expects.
fn misend(report: Option<&RunReport>, witnesses: &[SeedWitness]) -> Option<String> {
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
    match end.outcome {
        Outcome::Budget if end.drain.is_none_or(|drain| drain.exhausted) => {
            return Some(format!("a budget end without a dry drain: {:?}", end.drain));
        }
        Outcome::Violation => {
            let property = end.violation.as_ref().map(|v| v.property.as_str());
            let expected = witnesses
                .iter()
                .any(|w| w.expect == "violated" && Some(w.property.as_str()) == property);
            if !expected {
                return Some(format!("a violation of {property:?}"));
            }
        }
        _ => {}
    }
    witnesses.iter().find_map(|witness| {
        let ended = outcome(&witness.property);
        let fits = match (witness.expect.as_str(), ended) {
            ("holds" | "applicable", Some(PropertyOutcome::Held { .. })) => true,
            ("violated", Some(PropertyOutcome::Violated { .. })) => true,
            _ => false,
        };
        (!fits).then(|| {
            format!(
                "{} expected {}, got {ended:?}",
                witness.property, witness.expect
            )
        })
    })
}

/// Every saved Plan and seed under `tests/fixtures`, run by its harness as
/// a campaign would, ends as its sidecar expects ([`misend`]): once the
/// budget cuts it, the drain finishes what the live phase started and runs
/// dry within the harness's drain bound. An unbounded generator that never
/// registers ([`zakura_sim::sim::Generators`]) keeps a task ready, so its
/// run hits the bound and fails here, naming the task. Machines have no
/// drain. The seeds run on a few threads, each run on its own.
#[test]
fn every_saved_plan_drains_dry() {
    let fixtures = seed::repo_root().join("crates/zakura-sim/tests/fixtures");
    let loaded = seed::load(&[fixtures], None).expect("the fixture directory lists");
    let seeds: Vec<_> = loaded
        .admitted
        .iter()
        .map(|seed| {
            let entry =
                harness::find(&seed.sidecar.harness).expect("admitted seeds' harnesses exist");
            (seed, entry)
        })
        .filter(|(_, entry)| !entry.machine)
        .collect();
    assert!(!seeds.is_empty(), "the fixtures hold seeds");
    let next = AtomicUsize::new(0);
    let misended = Mutex::new(Vec::new());
    let threads = thread::available_parallelism().map_or(1, |n| n.get().min(8));
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                while let Some((seed, entry)) = seeds.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let run = (entry.run_bytes)(&seed.input, &mut NullSink, None);
                    if let Some(why) = misend(run.report(), &seed.sidecar.witness) {
                        misended
                            .lock()
                            .expect("no thread panics holding it")
                            .push(format!("{}: {why}", seed.path.display()));
                    }
                }
            });
        }
    });
    let misended = misended.into_inner().expect("every thread finished");
    assert!(
        misended.is_empty(),
        "saved runs that misended: {misended:#?}"
    );
}

/// Terra 2, round 4: a saved run that fails early, that the harness can't
/// run, or that ends at its budget without a dry drain misends, as does
/// one whose witness doesn't end as its sidecar expects. A dry drain whose
/// witness holds doesn't.
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
    let holds = [SeedWitness {
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
}
