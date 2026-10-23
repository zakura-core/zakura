//! Consecutive runs on one thread share nothing (S3.6). This binary holds a
//! single test, so the counting allocator sees only its own thread's heap.

#[allow(unused_imports)]
mod support;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use support::{shared, sleep, Mutex};
use zakura_scheduler::{
    hooks, stack_pool, CleanupReport, DrawKind, Op, Point, Scheduler, SchedulerConfig, SimInstant,
    Status,
};

thread_local! {
    /// Heap bytes this thread allocated and hasn't freed.
    static LIVE: Cell<isize> = const { Cell::new(0) };
}

/// Adds `bytes` to this thread's live count. Skipped once the thread-local
/// is gone, during thread teardown.
fn count(bytes: isize) {
    let _ = LIVE.try_with(|live| live.set(live.get() + bytes));
}

/// The system allocator, counting live bytes per thread.
struct Counting;

/// A layout's size as a signed count. Layout sizes never exceed `isize::MAX`.
fn size(layout: Layout) -> isize {
    layout.size() as isize // Layout guarantees size <= isize::MAX
}

#[allow(unsafe_code)]
// SAFETY: every method forwards to `System` with the caller's arguments, so
// it upholds `GlobalAlloc`'s contract exactly as `System` does. The counter
// is a const thread-local `Cell`, which never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged (see the impl).
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(size(layout));
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged (see the impl).
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(size(layout));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged (see the impl).
        unsafe { System.dealloc(ptr, layout) };
        count(-size(layout));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged (see the impl).
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            count(new_size as isize - size(layout)); // new_size fits a Layout
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// A preemption point with no shared state behind it.
#[track_caller]
fn point() {
    hooks::preempt(Point::here(Op::Yield));
}

/// What each kind of run sets up. It returns the shared values the run's
/// tasks hold, so the test can check every task let go of them.
type Setup = fn(&mut Scheduler) -> Vec<Rc<dyn std::any::Any>>;

/// Tasks that all finish: a sleep, a contended lock, and a child spawn.
fn completes(s: &mut Scheduler) -> Vec<Rc<dyn std::any::Any>> {
    let m = shared(Mutex::new(0u32));
    for label in ["a", "b"] {
        let m = m.clone();
        s.spawn_root(label, async move {
            sleep(5).await;
            *m.lock() += 1;
            hooks::spawn("child", async { point() }).await;
        });
    }
    vec![m]
}

/// A task that panics next to one that finishes.
fn panics(s: &mut Scheduler) -> Vec<Rc<dyn std::any::Any>> {
    let flag = shared(Cell::new(false));
    let f = flag.clone();
    s.spawn_root("boom", async move {
        let _f = f;
        sleep(1).await;
        panic!("planted panic");
    });
    let f = flag.clone();
    s.spawn_root("survivor", async move { f.set(true) });
    vec![flag]
}

/// An abort joined by its aborter, and idle tasks that shutdown cancels with
/// their timers still registered.
fn cancels(s: &mut Scheduler) -> Vec<Rc<dyn std::any::Any>> {
    let held = shared(RefCell::new(Vec::<u8>::with_capacity(64)));
    let h = held.clone();
    let victim = s.spawn_root("victim", async move {
        let _h = h;
        sleep(100).await;
    });
    s.spawn_root("aborter", async move {
        victim.abort();
        victim.await;
    });
    let h = held.clone();
    s.spawn_root("idle", async move {
        let _h = h;
        sleep(1_000).await;
    });
    vec![held]
}

/// An AB/BA pair that ends parked, so shutdown force-unwinds one of them.
fn unwinds(s: &mut Scheduler) -> Vec<Rc<dyn std::any::Any>> {
    let a = shared(Mutex::new(()));
    let b = shared(Mutex::new(()));
    let (a2, b2) = (a.clone(), b.clone());
    s.spawn_root("ab", async move {
        let _x = a.lock();
        point();
        let _y = b.lock();
    });
    s.spawn_root("ba", async move {
        let _x = b2.lock();
        point();
        let _y = a2.lock();
    });
    vec![]
}

/// Runs one run of `setup` with a fixed schedule, and returns its report and
/// how many draws reached `draws`.
fn one_run(setup: Setup, set_draws: bool, draws: &Rc<Cell<u32>>) -> CleanupReport {
    let config = SchedulerConfig {
        stack_size: 64 * 1024,
        ..SchedulerConfig::default()
    };
    let mut s = Scheduler::new(config, SimInstant::ZERO);
    // Nothing from an earlier run is visible.
    assert_eq!(s.now(), SimInstant::ZERO);
    assert_eq!(s.next_deadline(), None);
    assert_eq!(s.status(), Status::Quiescent);
    assert_eq!(
        hooks::draw(DrawKind::Rng, std::panic::Location::caller(), 0),
        0,
        "an earlier run's draw source answered"
    );
    if set_draws {
        let d = draws.clone();
        s.set_draws(move |_| {
            d.set(d.get() + 1);
            7
        });
        assert_eq!(
            hooks::draw(DrawKind::Rng, std::panic::Location::caller(), 0),
            7
        );
    }
    let held = setup(&mut s);
    let first = s.ready()[0].task;
    assert_eq!(first.index(), 0, "task ids start over");
    assert!(s.task_name(first).ends_with(".1"), "{}", s.task_name(first));
    // Round-robin over the ready tasks, advancing time when none is ready.
    for step in 0.. {
        match s.status() {
            Status::Running => {
                let ready = s.ready();
                s.run(ready[step % ready.len()].task);
            }
            Status::WaitingOnTime
                if s.now() < SimInstant::from_start(Duration::from_millis(50)) =>
            {
                let d = s.next_deadline().expect("a timer is pending");
                s.advance_to(d);
            }
            _ => break,
        }
    }
    let report = s.shutdown();
    for value in held {
        assert_eq!(Rc::strong_count(&value), 1, "a task kept a shared value");
    }
    report
}

#[test]
fn consecutive_runs_share_nothing_and_reuse_stacks() {
    // The planted panic's message would grow the harness's output buffer.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if info.payload().downcast_ref::<&str>() != Some(&"planted panic") {
            default_hook(info);
        }
    }));
    let draws = Rc::new(Cell::new(0));
    let kinds: [(&str, Setup); 4] = [
        ("completes", completes),
        ("panics", panics),
        ("cancels", cancels),
        ("unwinds", unwinds),
    ];
    // Interleave the kinds, so each one starts after a different kind.
    let mut pool = None;
    let mut baseline = None;
    for round in 0..50 {
        for (i, (kind, setup)) in kinds.iter().enumerate() {
            let before = LIVE.with(Cell::get);
            let report = one_run(*setup, round == 0 && i == 0, &draws);
            let after = LIVE.with(Cell::get);
            assert_eq!(report.unwind_failures, 0, "{kind}");
            if *kind == "unwinds" {
                assert_eq!(report.unwound, 1, "{kind}");
            }
            if round == 0 {
                continue;
            }
            // No new stacks, and every one is back in the pool: the pool's
            // memory is retained capacity, not a leak.
            assert_eq!(
                *pool.get_or_insert(stack_pool()),
                stack_pool(),
                "round {round}, {kind}: the pool neither grew nor lost a stack"
            );
            assert_eq!(after, before, "round {round}, {kind}: heap left behind");
            assert_eq!(
                *baseline.get_or_insert(after),
                after,
                "round {round}, {kind}"
            );
        }
    }
    assert_eq!(
        draws.get(),
        1,
        "only the run that set the source drew from it"
    );
    let pool = pool.expect("later rounds ran");
    assert!(pool.idle > 0);
    assert_eq!(
        pool.allocated - pool.freed,
        pool.idle as u64,
        "no stack leaked"
    ); // idle fits u64
}
