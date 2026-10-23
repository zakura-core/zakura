//! M1.5: a budget leaves an open liveness claim inconclusive, the drain
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
    ActionSource, Boundary, Identity, NoChecks, Observation, ObservationLog, Outcome, Plan,
    PropertyOutcome, PropertyReport, RunConfig, RunEnd, Violation,
};

/// A task that yields `n` times, then finishes.
async fn yields(n: u32) {
    for _ in 0..n {
        task::yield_now().await;
    }
}

/// A checker for a liveness claim, "`done.1` finishes". It reports the claim
/// held if the task exited, and inconclusive if the run ended at its budget
/// first.
fn finishes() -> impl check::Checker {
    FnChecker::new(
        |_: &check::StepCtx<'_>| None,
        |end: &RunEnd, log: &ObservationLog| {
            let exited = log.entries().iter().any(
                |e| matches!(&e.obs, Observation::TaskExited { task, .. } if task == "done.1"),
            );
            let outcome = match (exited, end.outcome) {
                (true, _) => PropertyOutcome::Held { applicable: 1 },
                (false, Outcome::Budget) => PropertyOutcome::Inconclusive {
                    reason: "open at the budget".into(),
                },
                (false, _) => PropertyOutcome::Violated { witness: vec![] },
            };
            vec![PropertyReport {
                property: "test/done-finishes".into(),
                outcome,
            }]
        },
    )
}

#[test]
fn budget_open_obligation_inconclusive() {
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
        &mut finishes(),
    );
    assert_eq!(report.end.outcome, Outcome::Budget);
    let drain = report.end.drain.unwrap();
    assert!(drain.exhausted && drain.steps == 20);
    assert_eq!(report.end.steps, 30);
    assert!(matches!(
        report.properties[0].outcome,
        PropertyOutcome::Inconclusive { .. }
    ));

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
            report.cleanup.unwound,
        ));
    });
    let (property, unwound) = finished
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the run reported after shutdown");
    assert_eq!(property.as_deref(), Some(NO_LIVELOCK));
    assert_eq!(unwound, 1, "shutdown force-unwound the spinner");
}
