//! Time, simulator build.
//!
//! Inside a run, [`Instant::now`] reads the executor's virtual clock and
//! timers register with the executor, so time only moves when the scheduler
//! moves it. Outside a run, everything reads tokio's clock and timers.
//! Whether a [`Sleep`] is simulated is fixed when it's created.

use std::{
    fmt,
    future::{Future, IntoFuture},
    ops::{Add, AddAssign, Sub, SubAssign},
    panic::Location,
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll, Waker},
    time::Duration,
};

pub use tokio::time::MissedTickBehavior;

use crate::sim::{self, Op, Point, RunId, TimerId};

/// Timer errors.
pub mod error {
    pub use tokio::time::error::Error;

    /// The error a [`Timeout`](super::Timeout) returns when its deadline
    /// passes first, as tokio's `Elapsed`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Elapsed(());

    impl Elapsed {
        /// The error for a deadline that passed.
        pub(super) fn new() -> Self {
            Elapsed(())
        }
    }

    impl std::fmt::Display for Elapsed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("deadline has elapsed")
        }
    }

    impl std::error::Error for Elapsed {}

    impl From<Elapsed> for std::io::Error {
        fn from(_: Elapsed) -> std::io::Error {
            std::io::ErrorKind::TimedOut.into()
        }
    }
}

/// The instant virtual time counts from. Fixed once per process, so virtual
/// instants compare and subtract like real ones.
fn base() -> tokio::time::Instant {
    static BASE: OnceLock<tokio::time::Instant> = OnceLock::new();
    *BASE.get_or_init(tokio::time::Instant::now)
}

/// About 30 years from `now`, which tokio also treats as never.
fn far_future(now: Instant) -> Instant {
    now + Duration::from_secs(86_400 * 365 * 30)
}

/// A monotonic instant, as tokio's `Instant`.
///
/// Inside a run, its value is the start of virtual time plus the executor's
/// clock. Compare it only with instants from the same run.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(tokio::time::Instant);

impl Instant {
    /// The current instant, virtual inside a run.
    pub fn now() -> Instant {
        match sim::executor() {
            Some(executor) => Instant(base() + executor.now()),
            None => Instant(tokio::time::Instant::now()),
        }
    }

    /// Converts a std instant.
    pub fn from_std(std: std::time::Instant) -> Instant {
        Instant(tokio::time::Instant::from_std(std))
    }

    /// Converts to a std instant.
    pub fn into_std(self) -> std::time::Instant {
        self.0.into_std()
    }

    /// Time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn duration_since(&self, earlier: Instant) -> Duration {
        self.0.duration_since(earlier.0)
    }

    /// Time from `earlier` to `self`, or `None` if `earlier` is later.
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        self.0.checked_duration_since(earlier.0)
    }

    /// Time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }

    /// Time since `self`, by the same clock [`Instant::now`] reads.
    pub fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }

    /// `self + duration`, or `None` on overflow.
    pub fn checked_add(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_add(duration).map(Instant)
    }

    /// `self - duration`, or `None` on overflow.
    pub fn checked_sub(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_sub(duration).map(Instant)
    }

    /// Virtual time at this instant, for registering a timer.
    fn since_base(self) -> Duration {
        self.0.saturating_duration_since(base())
    }
}

impl From<std::time::Instant> for Instant {
    fn from(std: std::time::Instant) -> Instant {
        Instant::from_std(std)
    }
}

impl From<Instant> for std::time::Instant {
    fn from(instant: Instant) -> std::time::Instant {
        instant.into_std()
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Instant {
        Instant(self.0 + rhs)
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, rhs: Duration) -> Instant {
        Instant(self.0 - rhs)
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, rhs: Instant) -> Duration {
        self.0 - rhs.0
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Waits until `duration` has passed, as tokio's `sleep`.
#[track_caller]
pub fn sleep(duration: Duration) -> Sleep {
    let now = Instant::now();
    let deadline = now.checked_add(duration).unwrap_or_else(|| far_future(now));
    sleep_until(deadline)
}

/// Waits until `deadline`, as tokio's `sleep_until`.
///
/// Inside a run, the sleep registers a virtual timer the first time it's
/// polled. Registration and expiry are both preemption points.
#[track_caller]
pub fn sleep_until(deadline: Instant) -> Sleep {
    let inner = match sim::run_id() {
        Some(run) => SleepInner::Sim(SimSleep {
            run,
            deadline,
            site: Location::caller(),
            polled: false,
            timer: None,
            waker: None,
        }),
        None => SleepInner::Tokio(Box::pin(tokio::time::sleep_until(deadline.0))),
    };
    Sleep { inner }
}

/// The future [`sleep`] and [`sleep_until`] return.
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    inner: SleepInner,
}

enum SleepInner {
    Tokio(Pin<Box<tokio::time::Sleep>>),
    Sim(SimSleep),
}

struct SimSleep {
    /// The run whose clock this sleep waits on.
    run: RunId,
    deadline: Instant,
    site: &'static Location<'static>,
    /// Whether the registration point has passed.
    polled: bool,
    timer: Option<TimerId>,
    /// The waker the timer holds, kept so `reset` can move the timer.
    waker: Option<Waker>,
}

impl SimSleep {
    /// Registers a timer for the current deadline with `waker`, replacing any
    /// earlier one.
    fn register(&mut self, waker: &Waker) {
        let Some(executor) = sim::executor_for(self.run) else {
            return;
        };
        if let Some(timer) = self.timer.take() {
            executor.cancel_timer(timer);
        }
        self.timer = Some(executor.register_timer(self.deadline.since_base(), waker));
        self.waker = Some(waker.clone());
    }

    /// Removes the registered timer, if any.
    fn cancel(&mut self) {
        if let Some(timer) = self.timer.take() {
            if let Some(executor) = sim::executor_for(self.run) {
                executor.cancel_timer(timer);
            }
        }
    }
}

impl Sleep {
    /// The instant this sleep completes at.
    pub fn deadline(&self) -> Instant {
        match &self.inner {
            SleepInner::Tokio(sleep) => Instant(sleep.deadline()),
            SleepInner::Sim(sleep) => sleep.deadline,
        }
    }

    /// True once the deadline has passed.
    pub fn is_elapsed(&self) -> bool {
        match &self.inner {
            SleepInner::Tokio(sleep) => sleep.is_elapsed(),
            SleepInner::Sim(sleep) => sleep.deadline <= Instant::now(),
        }
    }

    /// Moves the deadline, keeping any waker already registered.
    pub fn reset(self: Pin<&mut Self>, deadline: Instant) {
        match &mut self.get_mut().inner {
            SleepInner::Tokio(sleep) => sleep.as_mut().reset(deadline.0),
            SleepInner::Sim(sleep) => {
                sleep.deadline = deadline;
                match sleep.waker.take() {
                    Some(waker) => sleep.register(&waker),
                    None => sleep.cancel(),
                }
            }
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let sleep = match &mut self.get_mut().inner {
            SleepInner::Tokio(sleep) => return sleep.as_mut().poll(cx),
            SleepInner::Sim(sleep) => sleep,
        };
        let Some(executor) = sim::executor_for(sleep.run) else {
            // The run ended, and its clock with it.
            return Poll::Pending;
        };
        let first = !sleep.polled;
        sleep.polled = true;
        // The first poll's point covers registration, or expiry if the
        // deadline has already passed. A later poll that finds the deadline
        // passed is the expiry point.
        if first || sleep.deadline.since_base() <= executor.now() {
            sim::preempt(executor, Point::at(Op::Sleep, sleep.site));
        }
        if sleep.deadline.since_base() <= executor.now() {
            sleep.cancel();
            sleep.waker = None;
            return Poll::Ready(());
        }
        let current = sleep
            .waker
            .as_ref()
            .is_some_and(|w| w.will_wake(cx.waker()));
        if sleep.timer.is_none() || !current {
            sleep.register(cx.waker());
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let SleepInner::Sim(sleep) = &mut self.inner {
            sleep.cancel();
        }
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline())
            .finish_non_exhaustive()
    }
}

/// Requires `future` to complete within `duration`, as tokio's `timeout`.
#[track_caller]
pub fn timeout<F>(duration: Duration, future: F) -> Timeout<F::IntoFuture>
where
    F: IntoFuture,
{
    Timeout {
        value: future.into_future(),
        delay: sleep(duration),
    }
}

pin_project_lite::pin_project! {
    /// The future [`timeout`] returns.
    #[must_use = "futures do nothing unless polled"]
    #[derive(Debug)]
    pub struct Timeout<T> {
        #[pin]
        value: T,
        delay: Sleep,
    }
}

impl<T> Timeout<T> {
    /// The wrapped future.
    pub fn get_ref(&self) -> &T {
        &self.value
    }

    /// The wrapped future, mutably.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Unwraps the future, dropping the timer.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Future> Future for Timeout<T> {
    type Output = Result<T::Output, error::Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // Like tokio, the future gets the first chance to finish.
        let had_budget = tokio::task::coop::has_budget_remaining();
        if let Poll::Ready(value) = this.value.poll(cx) {
            return Poll::Ready(Ok(value));
        }
        // Like tokio, if the future used up the cooperative budget, the timer
        // is still checked, or a busy future would never time out. Only a
        // tokio-backed `Sleep` outside a run has a budget to run out of.
        let delay = Pin::new(this.delay);
        let elapsed = if had_budget && !tokio::task::coop::has_budget_remaining() {
            std::pin::pin!(tokio::task::coop::unconstrained(delay)).poll(cx)
        } else {
            delay.poll(cx)
        };
        elapsed.map(|()| Err(error::Elapsed::new()))
    }
}

/// Ticks every `period`, starting now, as tokio's `interval`.
///
/// # Panics
///
/// If `period` is zero.
#[track_caller]
pub fn interval(period: Duration) -> Interval {
    assert!(period > Duration::ZERO, "`period` must be non-zero.");
    Interval {
        delay: sleep_until(Instant::now()),
        period,
        missed_tick_behavior: MissedTickBehavior::default(),
    }
}

/// The ticker [`interval`] returns, with tokio's tick rules.
#[derive(Debug)]
pub struct Interval {
    delay: Sleep,
    period: Duration,
    missed_tick_behavior: MissedTickBehavior,
}

impl Interval {
    /// Waits for the next tick and returns when it was due.
    pub async fn tick(&mut self) -> Instant {
        std::future::poll_fn(|cx| self.poll_tick(cx)).await
    }

    /// Polls for the next tick.
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        if Pin::new(&mut self.delay).poll(cx).is_pending() {
            return Poll::Pending;
        }
        let due = self.delay.deadline();
        let now = Instant::now();
        // Tokio treats a tick up to 5 ms late as on time.
        let next = if now > due + Duration::from_millis(5) {
            self.next_after_missed(due, now)
        } else {
            due.checked_add(self.period)
                .unwrap_or_else(|| far_future(now))
        };
        Pin::new(&mut self.delay).reset(next);
        Poll::Ready(due)
    }

    /// The next deadline after the tick due at `due` ran late, at `now`.
    fn next_after_missed(&self, due: Instant, now: Instant) -> Instant {
        match self.missed_tick_behavior {
            MissedTickBehavior::Burst => due + self.period,
            MissedTickBehavior::Delay => now + self.period,
            MissedTickBehavior::Skip => {
                let late = (now - due).as_nanos() % self.period.as_nanos();
                let late = u64::try_from(late)
                    .expect("the remainder is below `period`, which fits in u64 nanoseconds");
                now + self.period - Duration::from_nanos(late)
            }
        }
    }

    /// Restarts the interval so the next tick is one period from now.
    pub fn reset(&mut self) {
        self.reset_at(Instant::now() + self.period);
    }

    /// Restarts the interval so the next tick is now.
    pub fn reset_immediately(&mut self) {
        self.reset_at(Instant::now());
    }

    /// Restarts the interval so the next tick is `after` from now.
    pub fn reset_after(&mut self, after: Duration) {
        self.reset_at(Instant::now() + after);
    }

    /// Restarts the interval so the next tick is at `deadline`.
    pub fn reset_at(&mut self, deadline: Instant) {
        Pin::new(&mut self.delay).reset(deadline);
    }

    /// What happens when ticks are missed.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    /// Sets what happens when ticks are missed.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// The time between ticks.
    pub fn period(&self) -> Duration {
        self.period
    }
}
