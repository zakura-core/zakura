//! M1.5: a drain cut off at its bound is a harness failure, the drain
//! admits nothing external, and a violation ends the run at once.
#![cfg(zakura_sim)]

mod support;

use std::{cell::Cell, rc::Rc};

use support::{config, picks, run};
use zakura_rt::task;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::{self, FnChecker, Phase},
    sim::NO_LIVELOCK,
    ActionSource, Boundary, HarnessFailure, Identity, NoChecks, Observation, ObservationLog,
    Outcome, Plan, PropertyOutcome, PropertyReport, RunConfig, RunEnd, Violation,
};

/// A task that yields `n` times, then finishes.
async fn yields(n: u32) {
    for _ in 0..n {
        task::yield_now().await;
    }
}

/// A checker for a liveness claim, "`done.1` finishes". It reports the claim
/// held if the task exited, and violated otherwise.
fn finishes() -> impl check::Checker {
    FnChecker::new(
        |_: &check::StepCtx<'_>| None,
        |_: &RunEnd, log: &ObservationLog| {
            let exited = log.entries().iter().any(
                |e| matches!(&e.obs, Observation::TaskExited { task, .. } if task == "done.1"),
            );
            let outcome = match exited {
                true => PropertyOutcome::Held { applicable: 1 },
                false => PropertyOutcome::Violated { witness: vec![] },
            };
            vec![PropertyReport::new("test/done-finishes", outcome)]
        },
    )
}

/// #1840 rule 2: a drain cut off at its bound with a task still spinning,
/// and nothing open, is a harness failure naming the task, never a budget
/// end whose claims read as held.
#[test]
fn a_drain_cut_off_while_a_task_spins_is_drain_bound() {
    let config = RunConfig {
        drain_bound: 20,
        ..config(2, 10)
    };
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("done", yields(1_000));
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    let drain = report.end.drain.unwrap();
    assert!(drain.exhausted && drain.steps == 20);
    assert_eq!(report.end.steps, 30);
    assert!(
        matches!(
            &report.end.harness,
            Some(HarnessFailure::DrainBound { open }) if open.starts_with("task:done.1:")
        ),
        "{:?}",
        report.end.harness
    );

    // A closure checker's end-only violation is an end finding, so the
    // harness failure drops it rather than blame the task it couldn't judge.
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("done", yields(1_000));
        },
        &mut finishes(),
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(
        report.properties[0].outcome,
        PropertyOutcome::not_applicable(zakura_sim::NaReason::HarnessFailed)
    );

    // A task that finishes during the drain discharges the claim.
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("done", yields(15));
        },
        &mut finishes(),
    );
    assert_eq!(report.end.outcome, Outcome::Budget);
    assert!(!report.end.drain.unwrap().exhausted);
    assert_eq!(
        report.properties[0].outcome,
        PropertyOutcome::Held { applicable: 1 }
    );
}

/// A source whose one action is always enabled, counting how often it ran.
struct Always(Rc<Cell<u32>>);

impl ActionSource for Always {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        out.push((Identity::peer("p1", "ping"), 0));
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        self.0.set(self.0.get() + 1);
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut zakura_sim::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

#[test]
fn drain_admits_no_external() {
    let pings = Rc::new(Cell::new(0));
    let source = Always(pings.clone());
    let report = run(
        config(2, 10),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.add_source(Box::new(source));
            sim.scheduler().spawn_root("worker", yields(40));
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget);
    assert!(!report.end.drain.unwrap().exhausted, "the worker finished");
    let live_pings = report
        .picks
        .iter()
        .filter(|p| p.phase == Phase::Live && p.identity == "peer:p1:ping")
        .count();
    assert!(live_pings > 0, "the source ran while live");
    // usize always fits in u64 on supported targets.
    assert_eq!(u64::from(pings.get()), live_pings as u64);
    for pick in report.picks.iter().filter(|p| p.phase == Phase::Drain) {
        assert!(pick.identity.starts_with("task:"), "{}", pick.identity);
        assert_eq!(pick.options, 1, "the drain offers only the ready task");
    }
}

#[test]
fn violation_stops_at_once() {
    let mut checker = check::after_step(|cx| {
        (cx.step == 5).then(|| Violation {
            property: "test/stop".into(),
            step: cx.step,
            witness: vec![2, 5],
            expected: "fewer steps".into(),
            actual: "step 5".into(),
        })
    });
    let report = run(
        config(2, 1_000),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("a", yields(100));
            sim.scheduler().spawn_root("b", yields(100));
        },
        &mut checker,
    );
    assert_eq!(report.end.outcome, Outcome::Violation);
    assert_eq!(report.end.steps, 5);
    assert_eq!(report.picks.len(), 5);
    assert_eq!(report.end.violation.unwrap().step, 5);
    assert!(report.log.entries().iter().all(|e| e.step <= 5));
}

/// A closure checker's step violation is a step finding, so a harness
/// failure found after it, here a report naming a step past the run's end,
/// keeps it `Violated`, whatever the end closure reports for the property:
/// a bare `Violated`, one with an end finding of its own (Terra 5, round
/// 2), or `HarnessFailed` itself.
#[test]
fn a_closure_step_violation_survives_a_late_harness_failure() {
    let end_finding = PropertyReport {
        violations: vec![Violation {
            property: "test/stop".into(),
            step: 5,
            witness: vec![5],
            expected: "an end".into(),
            actual: "the end".into(),
        }],
        end_found: 1,
        ..PropertyReport::new("test/stop", PropertyOutcome::Violated { witness: vec![5] })
    };
    // The end closure reports the step's own violation again, as an end
    // finding (Sol 2 and Terra 2, round 3).
    let same_at_end = PropertyReport {
        violations: vec![Violation {
            property: "test/stop".into(),
            step: 5,
            witness: vec![2, 5],
            expected: "fewer steps".into(),
            actual: "step 5".into(),
        }],
        end_found: 1,
        ..PropertyReport::new(
            "test/stop",
            PropertyOutcome::Violated {
                witness: vec![2, 5],
            },
        )
    };
    for stop in [
        PropertyReport::new(
            "test/stop",
            PropertyOutcome::Violated {
                witness: vec![2, 5],
            },
        ),
        end_finding,
        same_at_end,
        PropertyReport::new(
            "test/stop",
            PropertyOutcome::not_applicable(zakura_sim::NaReason::HarnessFailed),
        ),
    ] {
        let mut checker = FnChecker::new(
            |cx: &check::StepCtx<'_>| {
                (cx.step == 5).then(|| Violation {
                    property: "test/stop".into(),
                    step: cx.step,
                    witness: vec![2, 5],
                    expected: "fewer steps".into(),
                    actual: "step 5".into(),
                })
            },
            move |_: &RunEnd, _: &ObservationLog| {
                let late = PropertyReport {
                    holds_since: Some(1_000),
                    ..PropertyReport::new("test/late", PropertyOutcome::Held { applicable: 1 })
                };
                vec![stop.clone(), late]
            },
        );
        let report = run(
            config(2, 1_000),
            Plan::from_fuzz(&[]),
            |sim| {
                sim.scheduler().spawn_root("a", yields(100));
            },
            &mut checker,
        );
        assert_eq!(report.end.outcome, Outcome::Harness);
        assert!(matches!(
            report.end.harness,
            Some(HarnessFailure::HoldsSinceOutsideRun { .. })
        ));
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Violated {
                witness: vec![2, 5]
            }
        );
    }
}

/// Terra 1, round 4: a report that retains a step violation says so, at an
/// ordinary violation end too, whatever the end closure reported: `Held`,
/// or `Violated` with an end finding's witness. The outcome's witness is
/// the first retained violation's.
#[test]
fn a_closure_report_with_a_step_violation_is_violated_with_its_witness() {
    let end_only = PropertyReport {
        violations: vec![Violation {
            property: "test/stop".into(),
            step: 5,
            witness: vec![5],
            expected: "an end".into(),
            actual: "the end".into(),
        }],
        end_found: 1,
        ..PropertyReport::new("test/stop", PropertyOutcome::Violated { witness: vec![5] })
    };
    for stop in [
        PropertyReport::new("test/stop", PropertyOutcome::Held { applicable: 1 }),
        end_only,
    ] {
        let mut checker = FnChecker::new(
            |cx: &check::StepCtx<'_>| {
                (cx.step == 5).then(|| Violation {
                    property: "test/stop".into(),
                    step: cx.step,
                    witness: vec![2, 5],
                    expected: "fewer steps".into(),
                    actual: "step 5".into(),
                })
            },
            move |_: &RunEnd, _: &ObservationLog| vec![stop.clone()],
        );
        let report = run(
            config(2, 1_000),
            Plan::from_fuzz(&[]),
            |sim| {
                sim.scheduler().spawn_root("a", yields(100));
            },
            &mut checker,
        );
        assert_eq!(report.end.outcome, Outcome::Violation);
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Violated {
                witness: vec![2, 5]
            }
        );
    }
}

/// Terra, round 9: a step violation survives a late harness failure even
/// when the end closure reports nothing for its property, and the merged
/// step findings keep the order they were found in, so an earlier step
/// finding the end closure carries stays first and gives the witness.
#[test]
fn a_closure_merge_keeps_every_step_finding_in_order() {
    let step = |at: u64, witness: Vec<u64>| Violation {
        property: "test/stop".into(),
        step: at,
        witness,
        expected: "fewer steps".into(),
        actual: format!("step {at}"),
    };
    let late = || PropertyReport {
        holds_since: Some(1_000),
        ..PropertyReport::new("test/late", PropertyOutcome::Held { applicable: 1 })
    };
    let earlier = PropertyReport {
        violations: vec![step(2, vec![2])],
        ..PropertyReport::new("test/stop", PropertyOutcome::Violated { witness: vec![2] })
    };
    for (reports, witness) in [(vec![late()], vec![2, 5]), (vec![earlier, late()], vec![2])] {
        let mut checker = FnChecker::new(
            move |cx: &check::StepCtx<'_>| (cx.step == 5).then(|| step(5, vec![2, 5])),
            move |_: &RunEnd, _: &ObservationLog| reports.clone(),
        );
        let report = run(
            config(2, 1_000),
            Plan::from_fuzz(&[]),
            |sim| {
                sim.scheduler().spawn_root("a", yields(100));
            },
            &mut checker,
        );
        assert_eq!(report.end.outcome, Outcome::Harness);
        let stop = report
            .properties
            .iter()
            .find(|p| p.property == "test/stop")
            .expect("the step violation keeps a report");
        assert_eq!(stop.outcome, PropertyOutcome::Violated { witness });
        let steps: Vec<u64> = stop.violations.iter().map(|v| v.step).collect();
        assert!(steps.windows(2).all(|w| w[0] <= w[1]), "{steps:?}");
        assert_eq!(stop.end_found, 0);
    }
}

/// Sol 2, round 2: a report's end finding that names a step outside the
/// run is dropped as a harness failure, and the step finding before it
/// stays, so the trim's count of end findings follows what's left.
#[test]
fn a_dropped_end_finding_leaves_the_step_finding() {
    let finding = |step: u64, witness: Vec<u64>| Violation {
        property: "test/two".into(),
        step,
        witness,
        expected: "nothing".into(),
        actual: "something".into(),
    };
    let mut checker = FnChecker::new(
        |_: &check::StepCtx<'_>| None,
        move |_: &RunEnd, _: &ObservationLog| {
            vec![PropertyReport {
                violations: vec![finding(2, vec![2]), finding(99_999, vec![99_999])],
                end_found: 1,
                ..PropertyReport::new("test/two", PropertyOutcome::Violated { witness: vec![2] })
            }]
        },
    );
    let report = run(
        config(2, 1_000),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("a", yields(5));
        },
        &mut checker,
    );
    assert!(matches!(
        report.end.harness,
        Some(HarnessFailure::EvidenceOutsideRun { step: 99_999, .. })
    ));
    assert_eq!(
        report.properties[0].outcome,
        PropertyOutcome::Violated { witness: vec![2] }
    );
}

#[test]
fn livelock_limit_violates() {
    let config = RunConfig {
        max_steps_per_instant: 50,
        enforce_no_livelock: true,
        ..config(2, 1_000)
    };
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("spin", yields(u32::MAX));
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Violation);
    assert_eq!(picks(&report).len(), 51);
    let violation = report.end.violation.unwrap();
    assert_eq!(violation.property, NO_LIVELOCK);
    assert_eq!(violation.step, 51);
}

#[test]
fn drain_that_finishes_on_its_bound_is_not_exhausted() {
    let config = RunConfig {
        drain_bound: 1,
        ..config(2, 0)
    };
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("done", async {});
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Budget);
    let drain = report.end.drain.unwrap();
    assert_eq!(drain.steps, 1);
    assert!(!drain.exhausted, "no task was ready when the drain stopped");
}

/// A config that enforces the livelock limit at 50 steps per instant.
fn livelock_config() -> RunConfig {
    RunConfig {
        max_steps_per_instant: 50,
        enforce_no_livelock: true,
        ..config(2, 1_000)
    }
}

#[test]
fn livelock_counts_a_source_acting_while_tasks_wait() {
    let report = run(
        livelock_config(),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.add_source(Box::new(Always(Rc::default())));
            sim.scheduler()
                .spawn_root("waiter", std::future::pending::<()>());
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert_eq!(report.end.violation.unwrap().property, NO_LIVELOCK);
}

#[test]
fn livelock_counts_advances_that_leave_the_clock_still() {
    let report = run(
        livelock_config(),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("clamped", async {
                use std::future::Future;
                loop {
                    let mut timer =
                        std::pin::pin!(zakura_rt::time::sleep(std::time::Duration::from_secs(1)));
                    std::future::poll_fn(|cx| {
                        let _ = timer.as_mut().poll(cx);
                        std::task::Poll::Ready(())
                    })
                    .await;
                    // Behind the clock, so only an advance to now fires it.
                    let past = zakura_rt::time::Instant::now() - std::time::Duration::from_secs(1);
                    timer.as_mut().reset(past);
                    let mut woken = false;
                    std::future::poll_fn(|_| {
                        if woken {
                            std::task::Poll::Ready(())
                        } else {
                            woken = true;
                            std::task::Poll::Pending
                        }
                    })
                    .await;
                }
            });
        },
        &mut NoChecks,
    );
    let advances = picks(&report)
        .iter()
        .filter(|p| p.starts_with("advance-time:"))
        .count();
    assert!(
        advances > 1,
        "the clock stayed at 0 across {advances} advances"
    );
    assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    assert_eq!(report.end.violation.unwrap().property, NO_LIVELOCK);
}

#[test]
fn harness_text_in_records_has_no_debug_output() {
    let script = zakura_sim::PlanInput::Script(Plan {
        choices: vec![zakura_sim::PlanChoice::Draw(zakura_sim::HexU64(42))],
    });
    let (_, lines) = support::recorded(
        config(2, 100),
        script,
        |sim| {
            sim.scheduler().spawn_root("a", async {});
            sim.scheduler().spawn_root("b", async {});
        },
        &mut NoChecks,
    );
    let Ok(zakura_sim::Record::End(end)) = serde_json::from_str(lines.last().unwrap()) else {
        panic!("the last record is end");
    };
    assert_eq!(
        end.harness.as_deref(),
        Some("the run asked for a pick, but the script has draw 0x2a")
    );
}

#[test]
fn livelock_on_a_sync_spin_still_shuts_down() {
    // Runs on its own thread, so a hang fails the test instead of the suite.
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let report = run(
            livelock_config(),
            Plan::from_fuzz(&[]),
            |sim| {
                let flag = std::sync::Arc::new(zakura_rt::sync::atomic::AtomicBool::new(false));
                sim.scheduler().spawn_root("spinner", async move {
                    // Nothing ever stores, and every load is a point.
                    while !flag.load(zakura_rt::sync::atomic::Ordering::Acquire) {}
                });
            },
            &mut NoChecks,
        );
        let _ = done.send((
            report.end.violation.map(|v| v.property),
            report.cleanup.leaked,
        ));
    });
    let (property, leaked) = finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the run reported after shutdown");
    assert_eq!(property.as_deref(), Some(NO_LIVELOCK));
    assert_eq!(leaked, 1, "shutdown leaked the spinner");
}

#[test]
fn finishing_the_last_task_is_not_a_livelock() {
    let config = RunConfig {
        max_steps_per_instant: 1,
        enforce_no_livelock: true,
        ..config(2, 100)
    };
    let report = run(
        config,
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("once", async {
                // One point, so the task takes two steps at one instant.
                let flag = zakura_rt::sync::atomic::AtomicBool::new(false);
                let _ = flag.load(zakura_rt::sync::atomic::Ordering::Acquire);
            });
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.steps, 2);
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: zakura_sim::IdleStatus::Quiescent
        },
        "{:?}",
        report.end
    );
}

#[test]
fn the_end_record_counts_a_leaked_task() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (_, lines) = support::recorded(
            livelock_config(),
            Plan::from_fuzz(&[]),
            |sim| {
                let flag = std::sync::Arc::new(zakura_rt::sync::atomic::AtomicBool::new(false));
                sim.scheduler().spawn_root("spinner", async move {
                    while !flag.load(zakura_rt::sync::atomic::Ordering::Acquire) {}
                });
            },
            &mut NoChecks,
        );
        let _ = done.send(lines);
    });
    let lines = finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the run reported after shutdown");
    let Ok(zakura_sim::Record::End(end)) = serde_json::from_str(lines.last().unwrap()) else {
        panic!("the last record is end");
    };
    assert_eq!(end.cleanup.leaked, 1);
}

/// Sets its flag when dropped, as a leaked task's guard would have.
struct SetsOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for SetsOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A source with no actions whose drop waits for its flag.
struct WaitsOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl ActionSource for WaitsOnDrop {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {}

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut zakura_sim::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

impl Drop for WaitsOnDrop {
    fn drop(&mut self) {
        while !self.0.load(std::sync::atomic::Ordering::SeqCst) {
            std::hint::spin_loop();
        }
    }
}

/// Builds a sim whose only task spins forever holding a guard that would
/// set the flag a source's drop waits for, and runs `end` on it on its own
/// thread. Returns what `end` returns, or panics after a minute.
fn abandoned_sim_with_a_waiting_source<T: Send + 'static>(
    end: impl FnOnce(zakura_sim::Sim) -> T + Send + 'static,
) -> T {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut sim = zakura_sim::Sim::new(livelock_config(), Plan::from_fuzz(&[]));
        sim.add_source(Box::new(WaitsOnDrop(flag.clone())));
        let spinner = zakura_rt::sync::atomic::AtomicBool::new(false);
        let guard = SetsOnDrop(flag);
        let task = sim.scheduler().spawn_root("spinner", async move {
            let _guard = guard;
            while !spinner.load(zakura_rt::sync::atomic::Ordering::Acquire) {}
        });
        // Mid-poll, so shutdown's drain runs it until the cleanup bound.
        sim.scheduler().run(task.task());
        let _ = done.send(end(sim));
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the sim ended")
}

#[test]
fn an_abandoned_run_forgets_its_sources() {
    let leaked = abandoned_sim_with_a_waiting_source(|sim| {
        sim.run(&mut NoChecks, &mut zakura_sim::MemorySink::default())
            .cleanup
            .leaked
    });
    assert_eq!(leaked, 1);
}

#[test]
fn dropping_a_sim_that_abandons_forgets_its_sources() {
    abandoned_sim_with_a_waiting_source(drop);
}

/// A waker that panics when woken.
struct PanicsOnWake;

impl std::task::Wake for PanicsOnWake {
    fn wake(self: std::sync::Arc<Self>) {
        panic!("this waker panics");
    }
}

#[test]
fn a_sim_whose_shutdown_panics_forgets_its_sources() {
    use std::future::Future;
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        // Signals when the thread ends, even by a panic.
        struct Done(std::sync::mpsc::Sender<()>);
        impl Drop for Done {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let _done = Done(done);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut sim = zakura_sim::Sim::new(livelock_config(), Plan::from_fuzz(&[]));
        sim.add_source(Box::new(WaitsOnDrop(flag.clone())));
        // A ends first in shutdown, and its joiner's waker panics.
        let mut a = sim
            .scheduler()
            .spawn_root("a", std::future::pending::<()>());
        // B's guard would set the flag, but the panic leaves B suspended.
        let guard = SetsOnDrop(flag);
        let b = sim.scheduler().spawn_root("b", async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
        sim.scheduler().run(a.task());
        sim.scheduler().run(b.task());
        let waker = std::task::Waker::from(std::sync::Arc::new(PanicsOnWake));
        assert!(std::pin::Pin::new(&mut a)
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending());
        drop(sim);
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the sim was dropped without waiting on its source");
}

#[test]
fn a_deadlock_at_the_last_live_step_is_idle() {
    let report = run(
        config(1, 1),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler()
                .spawn_root("stuck", std::future::pending::<()>());
        },
        &mut NoChecks,
    );
    assert_eq!(report.end.steps, 1);
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: zakura_sim::IdleStatus::Deadlock
        }
    );
    assert_eq!(report.end.drain, None);
}
