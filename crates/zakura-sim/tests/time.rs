//! M1.2: time never advances while a task can run, never skips the next
//! boundary, and keeps the choices at one instant.
#![cfg(zakura_sim)]

mod support;

use std::{cell::Cell, collections::BTreeSet, future::Future, rc::Rc, time::Duration};

use zakura_sim::Sim;

use support::{advances, config, inputs, picks, run};
use zakura_rt::{task, time};
use zakura_scheduler::{SimInstant, Status};
use zakura_sim::{
    check, fake::FakeNet, ActionSource, Boundary, Identity, NoChecks, Observation, ObservationLog,
    Outcome, Violation,
};

const MS: u128 = 1_000_000;

#[test]
fn no_advance_while_ready() {
    for plan in inputs(300, 32) {
        let before = Cell::new(Status::Running);
        let mut checker = check::after_step(|cx| {
            let advanced = cx.pick.to_string().starts_with("advance-time:");
            let violation = (advanced && before.get() == Status::Running).then(|| Violation {
                property: "test/no-advance-while-ready".into(),
                step: cx.step,
                witness: vec![cx.step],
                expected: "no ready task".into(),
                actual: "a task was ready".into(),
            });
            before.set(cx.status);
            violation
        });
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                for (label, ms) in [("a", 3), ("b", 1)] {
                    sim.scheduler().spawn_root(label, async move {
                        for _ in 0..3 {
                            time::sleep(Duration::from_millis(ms)).await;
                            task::yield_now().await;
                        }
                    });
                }
            },
            &mut checker,
        );
        assert_eq!(report.end.violation, None);
        assert!(matches!(report.end.outcome, Outcome::Idle { .. }));
    }
}

#[test]
fn advances_to_earliest_boundary() {
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                for (label, ms) in [("ten", 10), ("five", 5), ("seven", 7)] {
                    sim.scheduler().spawn_root(label, async move {
                        time::sleep(Duration::from_millis(ms)).await;
                    });
                }
            },
            &mut NoChecks,
        );
        assert_eq!(advances(&report), [5 * MS, 7 * MS, 10 * MS]);
    }
}

#[test]
fn timer_cancels_later_timer() {
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                sim.scheduler().spawn_root("root", async {
                    let later = task::spawn(time::sleep(Duration::from_millis(10)));
                    time::sleep(Duration::from_millis(5)).await;
                    later.abort();
                });
            },
            &mut NoChecks,
        );
        assert_eq!(advances(&report), [5 * MS], "the 10 ms timer never fires");
        assert_eq!(report.cleanup.timers_left, 0);
    }
}

#[test]
fn same_instant_keeps_choices() {
    let mut first_after_advance = BTreeSet::new();
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                for label in ["a", "b"] {
                    sim.scheduler().spawn_root(label, async {
                        time::sleep(Duration::from_millis(5)).await;
                    });
                }
            },
            &mut NoChecks,
        );
        let at = report
            .picks
            .iter()
            .position(|p| p.identity.starts_with("advance-time:"))
            .expect("the run advances to 5 ms");
        let next = &report.picks[at + 1];
        assert_eq!(next.options, 2, "both timers fired at one instant");
        first_after_advance.insert(next.identity.split(':').nth(1).unwrap().to_owned());
    }
    assert_eq!(
        first_after_advance,
        BTreeSet::from(["a.1".to_owned(), "b.1".to_owned()]),
        "the plan picks either task first"
    );
}

/// A source with one action and a deadline at its start, until it acts.
struct DueNow {
    acted: Rc<Cell<bool>>,
}

impl ActionSource for DueNow {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.acted.get() {
            out.push((Identity::peer("p1", "answer"), 0));
        }
    }

    fn boundaries(&self, _: SimInstant, out: &mut Vec<Boundary>) {
        if !self.acted.get() {
            out.push(Boundary::Deadline(SimInstant::ZERO));
        }
    }

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        self.acted.set(true);
        log.push(Observation::Fake {
            source: "due-now".into(),
            event: "answered".into(),
        });
    }
}

#[test]
fn deadline_blocks_time() {
    for plan in inputs(200, 32) {
        let acted = Rc::new(Cell::new(false));
        let source = DueNow {
            acted: acted.clone(),
        };
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                sim.add_source(Box::new(source));
                sim.scheduler().spawn_root("sleeper", async {
                    time::sleep(Duration::from_millis(1)).await;
                });
            },
            &mut NoChecks,
        );
        let picks = picks(&report);
        let answered = picks.iter().position(|p| *p == "peer:p1:answer").unwrap();
        let advanced = picks
            .iter()
            .position(|p| p.starts_with("advance-time:"))
            .unwrap();
        assert!(answered < advanced, "{picks:?}");
    }
}

#[test]
fn no_repeated_zero_progress_advance() {
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| {
                sim.scheduler().spawn_root("root", async {
                    let mut early = std::pin::pin!(time::sleep(Duration::from_secs(30)));
                    std::future::poll_fn(|cx| {
                        let _ = early.as_mut().poll(cx);
                        std::task::Poll::Ready(())
                    })
                    .await;
                    time::sleep(Duration::from_secs(20)).await;
                    // Moves the timer behind the clock, so it's clamped to now
                    // and only an advance to now fires it.
                    early
                        .as_mut()
                        .reset(time::Instant::now() - Duration::from_secs(15));
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
                });
            },
            &mut NoChecks,
        );
        let advances = advances(&report);
        assert_eq!(
            advances,
            [20_000 * MS, 20_000 * MS],
            "one advance to 20 s, then one to now for the clamped timer"
        );
        assert!(matches!(report.end.outcome, Outcome::Idle { .. }));
        let zero_progress: Vec<_> = picks(&report)
            .windows(2)
            .filter(|w| w[0] == w[1] && w[0].starts_with("advance-time:"))
            .map(|w| w[0].to_owned())
            .collect();
        assert!(zero_progress.is_empty(), "{zero_progress:?}");
    }
}

/// A network that delivers 5 to 10 ms after sending and never drops, with
/// one task that sends `sends` datagrams and then sleeps `sleep_ms`.
fn sender(sim: &mut Sim, sends: usize, sleep_ms: u64) {
    let net = FakeNet::new(Duration::from_millis(5), Duration::from_millis(10), 0);
    sim.add_source(Box::new(net.clone()));
    sim.scheduler().spawn_root("sender", async move {
        for _ in 0..sends {
            net.send("node->p1");
        }
        time::sleep(Duration::from_millis(sleep_ms)).await;
    });
}

/// The virtual time, in nanoseconds, when each pick ran.
fn pick_times(report: &zakura_sim::RunReport) -> Vec<(u128, String)> {
    let mut now = 0;
    picks(report)
        .into_iter()
        .map(|p| {
            if let Some(ns) = p.strip_prefix("advance-time:") {
                now = ns.parse().unwrap();
            }
            (now, p.to_owned())
        })
        .collect()
}

#[test]
fn datagram_window_opens_at_boundary() {
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| sender(sim, 1, 7),
            &mut NoChecks,
        );
        assert_eq!(
            advances(&report)[0],
            5 * MS,
            "time stops where the window opens"
        );
        for (at, pick) in pick_times(&report) {
            if pick.starts_with("deliver:") {
                assert!((5 * MS..=10 * MS).contains(&at), "{pick} at {at}");
            }
        }
    }
}

#[test]
fn delivery_deadline_blocks_time() {
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| sender(sim, 1, 20),
            &mut NoChecks,
        );
        let times = pick_times(&report);
        let delivered = times
            .iter()
            .position(|(_, p)| p.starts_with("deliver:"))
            .expect("the datagram is delivered");
        assert!(times[delivered].0 <= 10 * MS);
        assert!(
            times[..delivered].iter().all(|(at, _)| *at <= 10 * MS),
            "time passed the deadline with the datagram in flight: {times:?}"
        );
        assert_eq!(advances(&report).last(), Some(&(20 * MS)));
    }
}

#[test]
fn same_instant_datagrams_keep_choices() {
    let mut first = BTreeSet::new();
    for plan in inputs(200, 32) {
        let report = run(
            config(2, 10_000),
            plan,
            |sim| sender(sim, 2, 1),
            &mut NoChecks,
        );
        let deliveries: Vec<String> = picks(&report)
            .into_iter()
            .filter(|p| p.starts_with("deliver:"))
            .map(str::to_owned)
            .collect();
        assert_eq!(deliveries.len(), 2);
        first.insert(deliveries[0].clone());
    }
    assert_eq!(first.len(), 2, "the plan delivers either datagram first");
}

#[test]
fn peer_waits_for_its_opening_across_an_unrelated_advance() {
    let report = run(
        config(2, 10_000),
        zakura_sim::Plan::from_fuzz(&[]),
        |sim| {
            // One action per instant, so the second waits for 1 s.
            sim.add_source(Box::new(zakura_sim::fake::FakePeer::new(
                "p1",
                &["first", "second"],
                1,
            )));
            sim.scheduler().spawn_root("timer", async {
                time::sleep(Duration::from_millis(5)).await;
                time::sleep(Duration::from_secs(2)).await;
            });
        },
        &mut NoChecks,
    );
    let times = pick_times(&report);
    let (at, _) = times
        .iter()
        .find(|(_, p)| p == "peer:p1:second")
        .expect("the peer takes its second action");
    assert!(
        *at >= 1_000 * MS,
        "the second action ran at {at} ns: {times:?}"
    );
}
