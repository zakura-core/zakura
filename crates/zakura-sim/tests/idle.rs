//! M1.3: a run with nothing enabled ends with the scheduler's reason, and the
//! four reasons stay distinct.
#![cfg(zakura_sim)]

mod support;

use std::{cell::Cell, future::pending, sync::Arc, time::Duration};

use support::{config, inputs, run};
use zakura_rt::{sync::Mutex, task};
use zakura_scheduler::hooks;
use zakura_scheduler::Status;
use zakura_sim::{
    check, fake::FakeStorage, observe::Exit, IdleStatus, NoChecks, Observation, Outcome, Plan,
    PlanInput, Sim,
};

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

#[test]
fn storage_completion_releases_apparent_deadlock() {
    for plan in inputs(100, 16) {
        // Set when the scheduler alone looks deadlocked while a storage
        // completion is enabled.
        let apparent = Cell::new(false);
        let mut checker = check::after_step(|cx| {
            if cx.status == Status::Deadlock {
                apparent.set(true);
            }
            None
        });
        let report = run(
            config(2, 1_000),
            plan,
            |sim| {
                let storage = FakeStorage::new("store", Duration::from_secs(1));
                sim.add_source(Box::new(storage.clone()));
                sim.scheduler().spawn_root("waiter", async move {
                    let _ = storage.submit().await;
                });
            },
            &mut checker,
        );
        assert!(
            apparent.get(),
            "the waiter's poll left the scheduler deadlocked"
        );
        assert_eq!(
            report.end.outcome,
            Outcome::Idle {
                status: IdleStatus::Quiescent
            },
            "the completion ran and the waiter finished"
        );
        assert!(report
            .picks
            .iter()
            .any(|p| p.identity.starts_with("storage:store:1:")));
        // Quiescence counts a panicked task as finished, so check the exit.
        let exit = report.log.entries().iter().find_map(|e| match &e.obs {
            Observation::TaskExited { task, exit } if task == "waiter.1" => Some(*exit),
            _ => None,
        });
        assert_eq!(exit, Some(Exit::Completed));
    }
}
