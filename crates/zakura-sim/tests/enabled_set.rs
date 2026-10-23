//! M1.1: every pick was enabled, enabled sets have no duplicates, and a
//! shared identity is a harness failure. The model test is in `fake.rs`.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, time::Duration};

use support::{config, inputs, run};
use zakura_rt::{task, time};
use zakura_sim::{
    check,
    fake::{FakeNet, FakePeer, FakeStorage},
    HarnessFailure, NoChecks, Outcome, Plan, Sim,
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

#[test]
fn every_pick_was_enabled() {
    for plan in inputs(2_000, 48) {
        let problem = RefCell::new(None);
        let mut checker = check::after_step(|cx| {
            let shown: Vec<String> = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
            if !cx.enabled.windows(2).all(|w| w[0].identity < w[1].identity) {
                *problem.borrow_mut() = Some(format!("unsorted or duplicated: {shown:?}"));
            } else if !shown.contains(&cx.pick.to_string()) {
                *problem.borrow_mut() = Some(format!("{} not in {shown:?}", cx.pick));
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
