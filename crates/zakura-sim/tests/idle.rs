//! M1.3: a run with nothing enabled ends with the scheduler's reason, and the
//! four reasons stay distinct.
#![cfg(zakura_sim)]

mod support;

use std::{future::pending, sync::Arc};

use support::{config, inputs, run};
use zakura_rt::{sync::Mutex, task};
use zakura_scheduler::hooks;
use zakura_sim::{IdleStatus, NoChecks, Outcome, Plan, PlanInput, Sim};

/// The idle status a run under `plan` with `workers` ends with.
fn idle(workers: u16, plan: PlanInput, setup: impl FnOnce(&mut Sim)) -> IdleStatus {
    let report = run(config(workers, 10_000), plan, setup, &mut NoChecks);
    match report.end.outcome {
        Outcome::Idle { status } => status,
        other => panic!("expected an idle end, got {other:?}"),
    }
}

#[test]
fn idle_outcomes_distinct() {
    let empty = || Plan::from_fuzz(&[]);
    assert_eq!(
        idle(2, empty(), |sim| {
            sim.scheduler().spawn_root("done", async {});
        }),
        IdleStatus::Quiescent
    );
    assert_eq!(
        idle(2, empty(), |sim| {
            sim.scheduler().spawn_root("stuck", pending::<()>());
        }),
        IdleStatus::Deadlock
    );
    assert_eq!(
        idle(2, empty(), |sim| {
            sim.scheduler().spawn_root("external", async {
                hooks::set_external_wait(true);
                pending::<()>().await
            });
        }),
        IdleStatus::WaitingOnExternal
    );

    // One worker: the holder yields while holding the lock, the waiter takes
    // the worker and parks on the lock, and the holder can't get it back.
    let starve = |sim: &mut Sim| {
        let lock = Arc::new(Mutex::new(()));
        let held = lock.clone();
        sim.scheduler().spawn_root("holder", async move {
            #[allow(clippy::await_holding_lock)]
            {
                let _guard = held.lock().unwrap();
                task::yield_now().await;
            }
        });
        sim.scheduler().spawn_root("waiter", async move {
            drop(lock.lock().unwrap());
        });
    };
    let statuses: Vec<IdleStatus> = inputs(200, 16)
        .into_iter()
        .map(|plan| idle(1, plan, starve))
        .collect();
    assert!(
        statuses.contains(&IdleStatus::WorkerStarved),
        "{statuses:?}"
    );
    assert!(statuses.contains(&IdleStatus::Quiescent), "{statuses:?}");
}
