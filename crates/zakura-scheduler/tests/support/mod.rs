//! Test-only stand-ins for the `zakura-rt` sim primitives, and a random-walk
//! driver standing in for `zakura-sim`.

#![allow(dead_code)]

use std::{
    cell::{Cell, RefCell, RefMut},
    future::{poll_fn, Future},
    pin::{pin, Pin},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use zakura_scheduler::{
    hooks, CleanupReport, ObjectId, Op, Point, RunOutcome, Scheduler, SchedulerConfig, SimInstant,
    Status, TaskId, TimerId,
};

/// A fixture's property check. Returns a description of the violation.
pub type Check = Box<dyn FnOnce(&RunResult) -> Option<String>>;

/// What one driven run did.
pub struct RunResult {
    pub status: Status,
    pub steps: usize,
    /// Each pick as `task:kind#n`, or `advance-time`.
    pub picks: Vec<String>,
    pub panics: Vec<String>,
    pub report: CleanupReport,
}

/// A seeded random walk over the options at each step.
struct Walk(u64);

impl Walk {
    /// Picks an index below `n` (SplitMix64).
    fn pick(&mut self, n: usize) -> usize {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z % n as u64) as usize // the remainder is below n, which is a usize
    }
}

const MAX_STEPS: usize = 10_000;

/// Runs one fixture with `workers` workers, picking with `seed`.
///
/// Time advances only when no task is ready. The check runs after shutdown,
/// so it can see the cleanup report.
pub fn run_fixture(
    workers: u16,
    seed: u64,
    setup: impl FnOnce(&mut Scheduler) -> Check,
) -> (RunResult, Option<String>) {
    let config = SchedulerConfig {
        workers,
        // Fixture tasks are tiny, and a small stack keeps the pool cheap.
        stack_size: 64 * 1024,
        ..SchedulerConfig::default()
    };
    let mut sched = Scheduler::new(config, SimInstant::ZERO);
    let check = setup(&mut sched);
    let mut walk = Walk(seed);
    let mut picks = Vec::new();
    let mut panics = Vec::new();
    let status = loop {
        if picks.len() >= MAX_STEPS {
            panic!("fixture didn't finish in {MAX_STEPS} steps");
        }
        match sched.status() {
            Status::Running => {
                let ready = sched.ready();
                let t = ready[walk.pick(ready.len())];
                picks.push(format!("{}:{}", sched.task_name(t.task), t.at));
                if let RunOutcome::Panicked(msg) = sched.run(t.task) {
                    panics.push(msg);
                }
            }
            Status::WaitingOnTime => {
                let next = sched.next_deadline().expect("WaitingOnTime has a timer");
                sched.advance_to(next);
                picks.push("advance-time".into());
            }
            other => break other,
        }
    };
    let report = sched.shutdown();
    let result = RunResult {
        status,
        steps: picks.len(),
        picks,
        panics,
        report,
    };
    let violation = check(&result);
    (result, violation)
}

/// The first seed below `seeds` whose run violates the fixture's check.
pub fn first_violation(
    workers: u16,
    seeds: u64,
    setup: impl Fn(&mut Scheduler) -> Check,
) -> Option<u64> {
    (0..seeds).find(|&seed| run_fixture(workers, seed, &setup).1.is_some())
}

/// A coordination atomic: a preemption point before each operation.
#[derive(Default)]
pub struct Atomic(Cell<u64>);

impl Atomic {
    #[track_caller]
    pub fn load(&self) -> u64 {
        hooks::preempt(Point::here(Op::AtomicLoad));
        self.0.get()
    }

    #[track_caller]
    pub fn store(&self, v: u64) {
        hooks::preempt(Point::here(Op::AtomicStore));
        self.0.set(v)
    }

    /// Reads without a preemption point, for checks.
    pub fn peek(&self) -> u64 {
        self.0.get()
    }
}

/// A std-style mutex whose contended `lock` parks the task.
pub struct Mutex<T> {
    obj: ObjectId,
    holder: Cell<Option<Option<TaskId>>>,
    val: RefCell<T>,
}

pub struct Guard<'a, T> {
    mutex: &'a Mutex<T>,
    val: RefMut<'a, T>,
}

impl<T> Mutex<T> {
    pub fn new(v: T) -> Self {
        Mutex {
            obj: hooks::new_object().expect("fixtures create mutexes inside a run"),
            holder: Cell::new(None),
            val: RefCell::new(v),
        }
    }

    #[track_caller]
    pub fn lock(&self) -> Guard<'_, T> {
        hooks::preempt(Point::here(Op::Lock));
        while let Some(holder) = self.holder.get() {
            assert!(
                hooks::park(self.obj, holder),
                "contended fixture Mutex outside a task"
            );
        }
        self.holder.set(Some(hooks::current_task()));
        Guard {
            mutex: self,
            val: self.val.borrow_mut(),
        }
    }
}

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.val
    }
}

impl<T> std::ops::DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.val
    }
}

impl<T> Drop for Guard<'_, T> {
    /// A release is not a preemption point.
    fn drop(&mut self) {
        self.mutex.holder.set(None);
        hooks::unpark_all(self.mutex.obj);
    }
}

/// Tokio's `Notify`, with the `notified()` point at creation.
#[derive(Default)]
pub struct Notify(tokio::sync::Notify);

impl Notify {
    /// The point goes before creation, because tokio reads the notify state
    /// when the future is created (see `runtime.md`).
    #[track_caller]
    pub fn notified(&self) -> Pin<Box<tokio::sync::futures::Notified<'_>>> {
        hooks::preempt(Point::here(Op::Notified));
        Box::pin(self.0.notified())
    }

    #[track_caller]
    pub fn notify_waiters(&self) {
        hooks::preempt(Point::here(Op::Notify));
        self.0.notify_waiters()
    }
}

/// A cancellation flag with a preemption point before each operation.
#[derive(Default)]
pub struct Token {
    cancelled: Cell<bool>,
    notify: tokio::sync::Notify,
}

impl Token {
    #[track_caller]
    pub fn cancel(&self) {
        hooks::preempt(Point::here(Op::Cancel));
        self.cancelled.set(true);
        self.notify.notify_waiters();
    }

    /// Reads the flag without a preemption point, for checks.
    pub fn peek(&self) -> bool {
        self.cancelled.get()
    }

    /// Waits for `cancel`. The point is at the first poll.
    pub async fn cancelled(&self) {
        let mut first = true;
        poll_fn(|_| {
            if first {
                first = false;
                hooks::preempt(Point::here(Op::Cancelled));
            }
            Poll::Ready(())
        })
        .await;
        loop {
            let mut wait = pin!(self.notify.notified());
            wait.as_mut().enable();
            if self.cancelled.get() {
                return;
            }
            wait.await;
        }
    }
}

/// A virtual-time sleep of `ms` milliseconds. Registration is a point.
pub fn sleep(ms: u64) -> Sleep {
    let now = hooks::now().expect("fixtures sleep inside a run");
    Sleep {
        deadline: now
            .checked_add(Duration::from_millis(ms))
            .expect("fixture deadlines are small"),
        timer: None,
    }
}

pub struct Sleep {
    deadline: SimInstant,
    timer: Option<TimerId>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let now = || hooks::now().expect("fixtures sleep inside a run");
        if now() >= self.deadline {
            return Poll::Ready(());
        }
        match self.timer.take() {
            Some(timer) => hooks::cancel_timer(timer),
            None => hooks::preempt(Point::here(Op::Sleep)),
        }
        if now() >= self.deadline {
            return Poll::Ready(());
        }
        self.timer = Some(hooks::register_timer(self.deadline, cx.waker()));
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            hooks::cancel_timer(timer);
        }
    }
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// `select! { biased; a, b }`, with the point before polling the branches.
pub async fn biased<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let mut a = pin!(a);
    let mut b = pin!(b);
    poll_fn(|cx| {
        hooks::preempt(Point::here(Op::Select));
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

/// Marks a future as waiting on something outside the scheduler.
pub async fn external<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    poll_fn(|cx| match f.as_mut().poll(cx) {
        Poll::Ready(v) => {
            hooks::set_external_wait(false);
            Poll::Ready(v)
        }
        Poll::Pending => {
            hooks::set_external_wait(true);
            Poll::Pending
        }
    })
    .await
}

/// Joins two futures, polling only the child whose own waker fired, like
/// `FuturesUnordered`.
pub async fn selective_join(a: impl Future<Output = ()>, b: impl Future<Output = ()>) {
    struct Child {
        woken: AtomicBool,
        parent: StdMutex<Option<Waker>>,
    }
    impl Wake for Child {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref()
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.woken.store(true, Ordering::SeqCst);
            if let Some(w) = self.parent.lock().expect("never poisoned").as_ref() {
                w.wake_by_ref();
            }
        }
    }
    let child = || {
        Arc::new(Child {
            woken: AtomicBool::new(true),
            parent: StdMutex::new(None),
        })
    };
    let (ca, cb) = (child(), child());
    let (wa, wb) = (Waker::from(ca.clone()), Waker::from(cb.clone()));
    let (mut a, mut b) = (pin!(a), pin!(b));
    let (mut a_done, mut b_done) = (false, false);
    poll_fn(|cx| {
        for c in [&ca, &cb] {
            *c.parent.lock().expect("never poisoned") = Some(cx.waker().clone());
        }
        if !a_done && ca.woken.swap(false, Ordering::SeqCst) {
            a_done = a.as_mut().poll(&mut Context::from_waker(&wa)).is_ready();
        }
        if !b_done && cb.woken.swap(false, Ordering::SeqCst) {
            b_done = b.as_mut().poll(&mut Context::from_waker(&wb)).is_ready();
        }
        if a_done && b_done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Shares a value between setup code, tasks, and the check.
pub fn shared<T>(v: T) -> Rc<T> {
    Rc::new(v)
}
