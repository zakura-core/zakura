//! The six planted races from `scheduler.md`'s fixture corpus, ported onto
//! `zakura-rt` primitives so they run through [`Sim`] (S4.4).
//!
//! `zakura-scheduler/tests/fixtures.rs` drives the same races on the bare
//! scheduler. Each port keeps the race's shape: the same tasks, the same
//! shared state, and the same preemption points around it. Harness-only
//! state, such as a "timed out" flag, is a plain `Cell`, so it adds no
//! points.
//!
//! Each fixture's scenario picks a [`Variant`]. Besides the planted race and
//! its fix, the variants rebuild `sim.md` Appendix C's change matrix: an
//! unrelated task spawned first, and an extra point at the start of each
//! racing task.

use std::{
    cell::Cell,
    future::{poll_fn, Future},
    pin::pin,
    rc::Rc,
    sync::PoisonError,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_rt::{
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        CancellationToken, Mutex, Notify,
    },
    time::sleep,
};
use zakura_scheduler::{hooks, Op, Point, SchedulerConfig, Status, TaskExit};

use super::Subsystem;
use crate::{
    property::{HasView, Status as PropertyStatus},
    trace::meta::PropertyRef,
    Checker, ObservationLog, PropertyOutcome, PropertyReport, RunConfig, RunEnd, Sim, StepCtx,
    Violation,
};

/// Which version of a fixture runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Variant {
    /// The planted race.
    Base,
    /// The race fixed. It must never fail.
    Fixed,
    /// The planted race, with an unrelated task spawned before the
    /// fixture's tasks.
    UnrelatedTask,
    /// The planted race, with a point of a kind the task doesn't otherwise
    /// reach at the start of each racing task.
    ExtraPointNewKind,
    /// The planted race, with a point of a kind the task already reaches at
    /// the start of each racing task.
    ExtraPointExistingKind,
}

impl Variant {
    /// Every variant, in [`Subsystem::draw`] order.
    pub const ALL: [Variant; 5] = [
        Variant::Base,
        Variant::Fixed,
        Variant::UnrelatedTask,
        Variant::ExtraPointNewKind,
        Variant::ExtraPointExistingKind,
    ];

    /// Reaches the variant's extra point, if it has one. `new` names a kind
    /// the calling task doesn't otherwise reach, and `existing` one it does.
    #[track_caller]
    fn extra_point(self, new: Op, existing: Op) {
        match self {
            Variant::ExtraPointNewKind => hooks::preempt(Point::here(new)),
            Variant::ExtraPointExistingKind => hooks::preempt(Point::here(existing)),
            Variant::Base | Variant::Fixed | Variant::UnrelatedTask => {}
        }
    }
}

/// A race fixture's scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Which version runs.
    pub variant: Variant,
    /// Simulated workers, 1 or 2. One worker hides races inside a poll.
    pub workers: u16,
}

/// One planted race on `zakura-rt` primitives.
pub trait Fixture: Sized + 'static {
    /// The harness's name, which is also its property's ID.
    const NAME: &'static str;

    /// What the property expects, for the violation's text.
    const EXPECTED: &'static str;

    /// The fixture's shared state, as the check reads it.
    type View;

    /// Spawns the fixture's tasks as root tasks.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self;

    /// The shared state now.
    fn view(&self) -> Self::View;

    /// What went wrong, if `view` shows the race after a step that leaves
    /// the scheduler `status`, with no task running.
    fn failure(view: &Self::View, status: Status) -> Option<String>;
}

/// A [`Fixture`] as a harness.
pub struct Race<F>(F);

impl<F: Fixture> HasView for Race<F> {
    type View = F::View;

    /// The fixture's shared state.
    fn view(&self) -> F::View {
        self.0.view()
    }
}

impl<F: Fixture> Subsystem for Race<F> {
    const NAME: &'static str = F::NAME;
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = Scenario;

    /// The scenario's workers, small stacks, and a budget far past any
    /// fixture's end.
    fn config(scenario: &Scenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: scenario.workers,
                // Fixture tasks are tiny, and a small stack keeps the pool
                // cheap.
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 1_000,
            drain_bound: 1_000,
            ..RunConfig::default()
        }
    }

    /// Spawns an unrelated task first for [`Variant::UnrelatedTask`], then
    /// the fixture.
    fn build(sim: &mut Sim, scenario: &Scenario) -> Self {
        if scenario.variant == Variant::UnrelatedTask {
            sim.scheduler().spawn_root("unrelated", async {
                sleep(Duration::from_millis(3)).await;
            });
        }
        Race(F::spawn(sim, scenario.variant))
    }

    /// A [`RaceChecker`] on the fixture's view.
    fn checker(this: &Rc<Self>, _: &Scenario) -> Box<dyn Checker> {
        Box::new(RaceChecker {
            this: Rc::clone(this),
            applicable: 0,
        })
    }

    /// The fixture's one property, active.
    fn properties(_: &Scenario) -> Vec<PropertyRef> {
        vec![PropertyRef {
            id: F::NAME.to_owned(),
            status: PropertyStatus::Active,
        }]
    }

    /// Byte 0 picks the variant from [`Variant::ALL`], and byte 1's low bit
    /// picks 1 or 2 workers.
    fn draw(prefix: &[u8]) -> Scenario {
        let byte = |i: usize| prefix.get(i).copied().unwrap_or_default();
        Scenario {
            variant: Variant::ALL[usize::from(byte(0)) % Variant::ALL.len()],
            workers: 1 + u16::from(byte(1) & 1),
        }
    }
}

/// Checks a fixture's property after each step that leaves no task running.
pub struct RaceChecker<F> {
    this: Rc<Race<F>>,
    /// Steps where the check applied.
    applicable: u32,
}

impl<F: Fixture> Checker for RaceChecker<F> {
    /// Fails the run once the fixture's view shows the race.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        if cx.status == Status::Running {
            return None;
        }
        self.applicable = self.applicable.saturating_add(1);
        let actual = F::failure(&self.this.view(), cx.status)?;
        Some(Violation {
            property: F::NAME.to_owned(),
            step: cx.step,
            witness: vec![cx.step],
            expected: F::EXPECTED.to_owned(),
            actual,
        })
    }

    /// Reports the property: violated, held at every applicable step, or
    /// never applicable.
    fn at_end(&mut self, end: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        let outcome = match &end.violation {
            Some(violation) if violation.property == F::NAME => PropertyOutcome::Violated {
                witness: violation.witness.clone(),
            },
            _ if self.applicable > 0 => PropertyOutcome::Held {
                applicable: self.applicable,
            },
            _ => PropertyOutcome::NotApplicable,
        };
        vec![PropertyReport {
            property: F::NAME.to_owned(),
            outcome,
            holds_since: None,
        }]
    }
}

/// Two tasks load and then store one counter. The fix adds in one atomic
/// step.
pub struct LostUpdate {
    counter: Rc<AtomicU64>,
    done: Rc<Cell<u32>>,
}

impl Fixture for LostUpdate {
    const NAME: &'static str = "race/lost-update";
    const EXPECTED: &'static str = "both increments land";
    /// The counter, and how many incrementers finished.
    type View = (u64, u32);

    /// Spawns `inc_a` and `inc_b`.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let counter = Rc::new(AtomicU64::new(0));
        let done = Rc::new(Cell::new(0));
        for label in ["inc_a", "inc_b"] {
            let (counter, done) = (Rc::clone(&counter), Rc::clone(&done));
            sim.scheduler().spawn_root(label, async move {
                variant.extra_point(Op::Yield, Op::AtomicLoad);
                if variant == Variant::Fixed {
                    counter.fetch_add(1, SeqCst);
                } else {
                    let v = counter.load(SeqCst);
                    counter.store(v + 1, SeqCst);
                }
                done.set(done.get() + 1);
            });
        }
        LostUpdate { counter, done }
    }

    /// Reads the counter. Outside a task its point is a no-op.
    fn view(&self) -> (u64, u32) {
        (self.counter.load(SeqCst), self.done.get())
    }

    /// The counter is short once both incrementers finish.
    fn failure(&(counter, done): &(u64, u32), _: Status) -> Option<String> {
        (done == 2 && counter != 2).then(|| format!("counter = {counter}, want 2"))
    }
}

/// Check capacity under one lock, then reserve under a second lock. The fix
/// checks and reserves under one.
pub struct SnapshotReserve {
    /// The capacity as the last holder left it. A `Cell` written under the
    /// lock, so the check never takes the lock.
    left: Rc<Cell<i64>>,
}

impl Fixture for SnapshotReserve {
    const NAME: &'static str = "race/snapshot-reserve";
    const EXPECTED: &'static str = "no more reservations than capacity";
    /// The capacity left.
    type View = i64;

    /// Spawns `res_a` and `res_b` on a capacity of 1.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let avail = Rc::new(Mutex::new(1i64));
        let left = Rc::new(Cell::new(1));
        for label in ["res_a", "res_b"] {
            let (avail, left) = (Rc::clone(&avail), Rc::clone(&left));
            sim.scheduler().spawn_root(label, async move {
                variant.extra_point(Op::Yield, Op::Lock);
                let lock = || avail.lock().expect("no holder panics");
                if variant == Variant::Fixed {
                    let mut avail = lock();
                    if *avail >= 1 {
                        *avail -= 1;
                        left.set(*avail);
                    }
                } else {
                    let ok = *lock() >= 1;
                    if ok {
                        let mut avail = lock();
                        *avail -= 1;
                        left.set(*avail);
                    }
                }
            });
        }
        SnapshotReserve { left }
    }

    /// The capacity left.
    fn view(&self) -> i64 {
        self.left.get()
    }

    /// The capacity went negative.
    fn failure(&left: &i64, _: Status) -> Option<String> {
        (left < 0).then(|| format!("over-committed: available = {left}"))
    }
}

/// Discovery's `wait_complete` shape: check a flag, then create
/// `notified()`. The fix registers before checking.
pub struct MissedNotify {
    timed_out: Rc<Cell<bool>>,
}

impl Fixture for MissedNotify {
    const NAME: &'static str = "race/missed-notify";
    const EXPECTED: &'static str = "the waiter wakes on the notification";
    /// True once the waiter timed out.
    type View = bool;

    /// Spawns `waiter` and `marker`.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let flag = Rc::new(AtomicU64::new(0));
        let notify = Rc::new(Notify::new());
        let timed_out = Rc::new(Cell::new(false));
        let (f, n, t) = (Rc::clone(&flag), Rc::clone(&notify), Rc::clone(&timed_out));
        sim.scheduler().spawn_root("waiter", async move {
            variant.extra_point(Op::Yield, Op::AtomicLoad);
            let late = if variant == Variant::Fixed {
                let mut notified = pin!(n.notified());
                notified.as_mut().enable();
                if f.load(SeqCst) == 1 {
                    return;
                }
                times_out(notified).await
            } else {
                if f.load(SeqCst) == 1 {
                    return;
                }
                times_out(n.notified()).await
            };
            t.set(late);
        });
        sim.scheduler().spawn_root("marker", async move {
            variant.extra_point(Op::Yield, Op::AtomicStore);
            flag.store(1, SeqCst);
            notify.notify_waiters();
        });
        MissedNotify { timed_out }
    }

    /// Whether the waiter timed out.
    fn view(&self) -> bool {
        self.timed_out.get()
    }

    /// The waiter timed out.
    fn failure(&timed_out: &bool, _: Status) -> Option<String> {
        timed_out.then(|| "the waiter missed the notification".to_owned())
    }
}

/// True if `notified` loses to a 2 s timeout. The select is biased, so a
/// notification that already happened wins.
async fn times_out(notified: impl Future<Output = ()>) -> bool {
    zakura_rt::select! {
        biased;
        () = notified => false,
        () = sleep(Duration::from_secs(2)) => true,
    }
}

/// A biased select polls the timer before the cancel branch, so it can pick
/// the timeout after the cancel already happened. The fix polls the cancel
/// first.
pub struct TimerCancel {
    bad: Rc<Cell<bool>>,
}

impl Fixture for TimerCancel {
    const NAME: &'static str = "race/timer-cancel";
    const EXPECTED: &'static str = "no timeout after the cancel";
    /// True once the timer chose its timeout after the cancel.
    type View = bool;

    /// Spawns `timer` and `closer`, both due at 10 ms.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let token = Rc::new(CancellationToken::new());
        // Set in the cancel's own step, with no point, as the bare
        // scheduler's `peek` reads the token.
        let cancelled = Rc::new(Cell::new(false));
        let bad = Rc::new(Cell::new(false));
        let (tk, c, b) = (Rc::clone(&token), Rc::clone(&cancelled), Rc::clone(&bad));
        sim.scheduler().spawn_root("timer", async move {
            variant.extra_point(Op::Yield, Op::Sleep);
            let tick = sleep(Duration::from_millis(10));
            let mut select = pin!(async {
                if variant == Variant::Fixed {
                    zakura_rt::select! {
                        biased;
                        () = tk.cancelled() => false,
                        () = tick => true,
                    }
                } else {
                    zakura_rt::select! {
                        biased;
                        () = tick => true,
                        () = tk.cancelled() => false,
                    }
                }
            });
            // A `zakura-rt` sleep's poll is a point, so a cancel can land
            // inside the select's poll, after the cancel branch said no. The
            // bare scheduler's fixture has no point there. Reading the token
            // as the deciding poll starts keeps the claim it checks: the
            // timeout won although the cancel came first.
            let cancelled_first = Cell::new(false);
            let timed_out = poll_fn(|cx| {
                cancelled_first.set(c.get());
                select.as_mut().poll(cx)
            })
            .await;
            if timed_out && cancelled_first.get() {
                b.set(true);
            }
        });
        sim.scheduler().spawn_root("closer", async move {
            variant.extra_point(Op::Yield, Op::Sleep);
            sleep(Duration::from_millis(10)).await;
            token.cancel();
            cancelled.set(true);
        });
        TimerCancel { bad }
    }

    /// Whether the timer timed out after the cancel.
    fn view(&self) -> bool {
        self.bad.get()
    }

    /// The timer timed out after the cancel.
    fn failure(&bad: &bool, _: Status) -> Option<String> {
        bad.then(|| "timeout chosen after cancel".to_owned())
    }
}

/// Opposite lock order. The fix takes both locks in one order.
pub struct AbBa;

impl Fixture for AbBa {
    const NAME: &'static str = "race/ab-ba";
    const EXPECTED: &'static str = "no lock cycle";
    /// Nothing. The scheduler's status shows the deadlock.
    type View = ();

    /// Spawns `ab` and `ba`. Shutdown unwinds a lock cycle, which poisons
    /// the locks, so the tasks take a poisoned lock as it is.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let a = Rc::new(Mutex::new(()));
        let b = Rc::new(Mutex::new(()));
        let (a2, b2) = (Rc::clone(&a), Rc::clone(&b));
        sim.scheduler().spawn_root("ab", async move {
            variant.extra_point(Op::Yield, Op::Lock);
            let _x = a.lock().unwrap_or_else(PoisonError::into_inner);
            let _y = b.lock().unwrap_or_else(PoisonError::into_inner);
        });
        sim.scheduler().spawn_root("ba", async move {
            variant.extra_point(Op::Yield, Op::Lock);
            let (first, second) = if variant == Variant::Fixed {
                (&a2, &b2)
            } else {
                (&b2, &a2)
            };
            let _x = first.lock().unwrap_or_else(PoisonError::into_inner);
            let _y = second.lock().unwrap_or_else(PoisonError::into_inner);
        });
        AbBa
    }

    /// Nothing.
    fn view(&self) {}

    /// The scheduler found the lock cycle.
    fn failure(_: &(), status: Status) -> Option<String> {
        (status == Status::Deadlock).then(|| "deadlock".to_owned())
    }
}

/// Abort a task, then assume its resources are gone. The fix awaits the
/// handle first, which makes that true.
pub struct AbortWithoutJoin {
    bad: Rc<Cell<bool>>,
}

/// Sets its flag when dropped, as a resource that releases on drop would.
struct Resource(Rc<Cell<bool>>);

impl Drop for Resource {
    /// Marks the resource released.
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl Fixture for AbortWithoutJoin {
    const NAME: &'static str = "race/abort-without-join";
    const EXPECTED: &'static str = "an aborted task's resource is released";
    /// True once the aborter found the resource still held.
    type View = bool;

    /// Spawns `worker`, which holds a resource for 100 ms, and `aborter`,
    /// which aborts it after 1 ms.
    fn spawn(sim: &mut Sim, variant: Variant) -> Self {
        let released = Rc::new(Cell::new(false));
        let bad = Rc::new(Cell::new(false));
        let r = Rc::clone(&released);
        let worker = sim.scheduler().spawn_root("worker", async move {
            variant.extra_point(Op::AtomicLoad, Op::Sleep);
            let _resource = Resource(r);
            sleep(Duration::from_millis(100)).await;
        });
        let b = Rc::clone(&bad);
        sim.scheduler().spawn_root("aborter", async move {
            variant.extra_point(Op::AtomicLoad, Op::Sleep);
            sleep(Duration::from_millis(1)).await;
            worker.abort();
            if variant == Variant::Fixed {
                assert_eq!(worker.await, TaskExit::Cancelled);
            }
            hooks::preempt(Point::here(Op::Yield));
            if !released.get() {
                b.set(true);
            }
        });
        AbortWithoutJoin { bad }
    }

    /// Whether the aborter found the resource held.
    fn view(&self) -> bool {
        self.bad.get()
    }

    /// The aborter found the resource held.
    fn failure(&bad: &bool, _: Status) -> Option<String> {
        bad.then(|| "the aborted task still held its resource".to_owned())
    }
}
