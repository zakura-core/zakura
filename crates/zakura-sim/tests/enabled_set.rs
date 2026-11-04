//! M1.1: every pick was enabled, enabled sets have no duplicates, and a
//! shared identity is a harness failure. The model test is in `fake.rs`.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, collections::BTreeSet, time::Duration};

use support::{config, inputs, run};
use zakura_rt::{task, time};
use zakura_sim::{
    check,
    fake::{FakeNet, FakePeer, FakeStorage},
    HarnessFailure, NoChecks, Observation, Outcome, Plan, Sim,
};

/// Tasks that send datagrams, submit storage, and sleep, with a peer
/// acting alongside.
fn busy(sim: &mut Sim) {
    let net = FakeNet::new(Duration::from_millis(1), Duration::from_millis(4), 2);
    let storage = FakeStorage::new("store", Duration::from_millis(3));
    sim.add_source(Box::new(net.clone()));
    sim.add_source(Box::new(storage.clone()));
    sim.add_source(Box::new(FakePeer::new("p1", &["hello", "ping", "ping"], 1)));
    for label in ["a", "b"] {
        let (net, storage) = (net.clone(), storage.clone());
        sim.scheduler().spawn_root(label, async move {
            for _ in 0..2 {
                net.send("node->p1");
                let _ = storage.submit().await;
                time::sleep(Duration::from_millis(2)).await;
                task::yield_now().await;
            }
        });
    }
}

/// The fake event a pick of a fake action must log, as `(source, event)`,
/// or `None` for a task or a time advance.
fn expected_event(pick: &str) -> Option<(&'static str, String)> {
    let (category, key) = pick.split_once(':')?;
    match category {
        "deliver" | "drop" => Some(("net", format!("{category} {key}"))),
        "storage" => Some(("storage", key.to_owned())),
        "peer" => Some(("peer", key.to_owned())),
        _ => None,
    }
}

#[test]
fn every_pick_runs_its_own_action() {
    // Seen across all plans: a datagram picked past the first one enabled,
    // a drop, and each storage outcome.
    let mut later_datagram = false;
    let mut seen = BTreeSet::new();
    for plan in inputs(2_000, 48) {
        let problem = RefCell::new(None);
        let mut checker = check::after_step(|cx| {
            let shown: Vec<String> = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
            let pick = cx.pick.to_string();
            let events: Vec<(&str, &str)> = cx
                .log
                .of_step(cx.step)
                .filter_map(|obs| match obs {
                    Observation::Fake { source, event } => Some((source.as_str(), event.as_str())),
                    _ => None,
                })
                .collect();
            let expected = expected_event(&pick);
            let wanted: Vec<(&str, &str)> = expected
                .iter()
                .map(|(source, event)| (*source, event.as_str()))
                .collect();
            if !cx.enabled.windows(2).all(|w| w[0].identity < w[1].identity) {
                *problem.borrow_mut() = Some(format!("unsorted or duplicated: {shown:?}"));
            } else if events != wanted {
                *problem.borrow_mut() = Some(format!("{pick} logged {events:?}"));
            }
            let datagrams: Vec<&String> =
                shown.iter().filter(|s| s.starts_with("deliver:")).collect();
            if datagrams.len() > 1 && pick.starts_with("deliver:") && *datagrams[0] != pick {
                later_datagram = true;
            }
            if let Some(kind) = ["drop:", "storage:store:"]
                .iter()
                .find(|kind| pick.starts_with(*kind))
            {
                let outcome = pick.rsplit(':').next().unwrap_or_default();
                seen.insert(if *kind == "drop:" { "drop" } else { outcome }.to_owned());
            }
            None
        });
        let report = run(config(2, 5_000), plan, busy, &mut checker);
        assert_eq!(problem.take(), None);
        assert!(
            matches!(report.end.outcome, Outcome::Idle { .. }),
            "{:?}",
            report.end
        );
    }
    assert!(later_datagram, "a pick decoded a datagram past the first");
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        ["drop", "failure", "success"]
    );
}

#[test]
fn duplicate_identity_is_harness() {
    let report = run(
        config(2, 100),
        Plan::from_fuzz(&[]),
        |sim| {
            for _ in 0..2 {
                sim.add_source(Box::new(FakePeer::new("p1", &["hello"], 1)));
            }
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(
        report.end.harness,
        Some(HarnessFailure::DuplicateIdentity {
            identity: "peer:p1:hello".into()
        })
    );
}

#[test]
#[should_panic(expected = "per_instant")]
fn a_peer_with_no_budget_is_rejected() {
    let _ = FakePeer::new("p1", &["hello"], 0);
}

#[test]
#[should_panic(expected = "min")]
fn a_net_whose_window_closes_before_it_opens_is_rejected() {
    let _ = FakeNet::new(Duration::from_millis(10), Duration::from_millis(5), 0);
}
