//! M1.4: once input runs out, round-robin can't starve a task. A spinner that
//! waits on a flag always lets the setter run.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use support::{config, inputs, run};
use zakura_rt::sync::atomic::{AtomicBool, Ordering};
use zakura_sim::{IdleStatus, NoChecks, Outcome, Plan, PlanInput};

/// Runs the spin-wait fixture under `plan` and checks it finishes.
fn spin_wait(plan: PlanInput) {
    let report = run(
        config(2, 1_000),
        plan,
        |sim| {
            let flag = Arc::new(AtomicBool::new(false));
            let seen = flag.clone();
            sim.scheduler().spawn_root("spinner", async move {
                // Each load is a preemption point.
                while !seen.load(Ordering::Acquire) {}
            });
            sim.scheduler().spawn_root("setter", async move {
                flag.store(true, Ordering::Release);
            });
        },
        &mut NoChecks,
    );
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        report.picks
    );
}

#[test]
fn spin_wait_setter_runs() {
    for len in 0..=8 {
        for plan in inputs(64, len) {
            spin_wait(plan);
        }
    }
    for byte in 0..=u8::MAX {
        spin_wait(Plan::from_fuzz(&[byte]));
    }
}
