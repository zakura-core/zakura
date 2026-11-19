//! `fuzz_one` itself, on test harnesses (sim-7.2, M7.5): each failure class
//! becomes its own panic with what it saved, a task's panic unwinds into a
//! violation, and repeated inputs leave no run state on the worker.

use std::{
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::OnceLock,
};

use zakura_scheduler::{stack_pool, SchedulerConfig};
use zakura_sim::{
    harness::{entry, HarnessEntry, Subsystem, NO_TASK_PANIC},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    trace::ARTIFACTS_VAR,
    RunConfig, Sim,
};

use super::fuzz_one;

/// The artifacts directory every test here shares, set once before any
/// run reads it, so tests on parallel threads agree on it.
fn artifacts() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("zakura-sim-fuzz-adapter-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Set before the first run, and never changed after.
        std::env::set_var(ARTIFACTS_VAR, &dir);
        dir
    })
}

/// The message `fuzz_one(entry, input)` panics with, or `None` if it
/// returns.
fn panic_of(entry: &'static HarnessEntry, input: &[u8]) -> Option<String> {
    artifacts();
    let result = panic::catch_unwind(AssertUnwindSafe(|| fuzz_one(entry, input)));
    result.err().map(|payload| {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default()
    })
}

/// One worker, small stacks, short limits.
fn limits() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 1,
            stack_size: 64 * 1024,
            ..SchedulerConfig::default()
        },
        budget: 50,
        drain_bound: 10,
        ..RunConfig::default()
    }
}

/// Implements a harness with no view and no scenario, whose `build` is
/// `$build` and whose properties are `$properties`.
macro_rules! test_harness {
    ($harness:ident, $name:literal, $build:expr, $properties:expr) => {
        struct $harness;

        impl HasView for $harness {
            type View = ();

            /// Nothing: the checks read observations, if any.
            fn view(&self) {}
        }

        impl Subsystem for $harness {
            const NAME: &'static str = $name;
            const GENERATOR: u32 = 1;
            const SCENARIO_BYTES: usize = 0;
            type Scenario = ();
            type Prepared = ();

            /// Short limits.
            fn config((): &()) -> RunConfig {
                limits()
            }

            /// Nothing to set up.
            fn prepare((): &()) {}

            /// Spawns the harness's tasks.
            fn build(sim: &mut Sim, (): &(), (): ()) -> Self {
                let build: fn(&mut Sim) = $build;
                build(sim);
                $harness
            }

            /// The harness's properties.
            fn properties() -> Vec<Property<Self>> {
                $properties
            }

            /// The one scenario.
            fn draw(_: &[u8]) {}
        }
    };
}

const ALWAYS_FAILS: &str = "test/adapter/always-fails";

/// Fails at the first step it checks.
fn fails<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    cx.violated("never checked", "checked")
}

test_harness!(
    Failing,
    "test/adapter/failing",
    |sim| {
        sim.scheduler().spawn_root("task", async {});
    },
    vec![Property {
        id: PropertyId::new(ALWAYS_FAILS),
        claim: "never holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Active,
        check: fails::<Failing>,
        witness: None,
    }]
);

test_harness!(
    Panicking,
    "test/adapter/panicking",
    |sim| {
        sim.scheduler()
            .spawn_root("task", async { panic!("a planted task panic") });
    },
    Vec::new()
);

test_harness!(
    BadName,
    "test/adapter/bad-name",
    |sim| {
        sim.scheduler().spawn_root("bad:name", async {});
    },
    Vec::new()
);

test_harness!(
    Clean,
    "test/adapter/clean",
    |sim| {
        sim.scheduler().spawn_root("task", async {
            zakura_rt::task::yield_now().await;
        });
    },
    Vec::new()
);

static FAILING: HarnessEntry = entry::<Failing>();
static PANICKING: HarnessEntry = entry::<Panicking>();
static BAD_NAME: HarnessEntry = entry::<BadName>();
static CLEAN: HarnessEntry = entry::<Clean>();

/// The files under `dir`, recursively.
fn files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}

#[test]
fn a_property_failure_saves_its_input_and_trace_and_panics_with_its_id() {
    let message = panic_of(&FAILING, b"input").expect("an Active violation panics");
    assert!(
        message.starts_with(&format!("{ALWAYS_FAILS}: trace at ")),
        "{message}"
    );
    let saved = artifacts().join("inputs/active").join(ALWAYS_FAILS);
    assert!(
        !files(&saved).is_empty(),
        "the input is saved under {}",
        saved.display()
    );
    let trace = message
        .split("trace at ")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .expect("the message names the trace");
    assert!(Path::new(trace).join("trace.jsonl").is_file(), "{trace}");
}

#[test]
fn a_task_panic_becomes_no_task_panic() {
    let message = panic_of(&PANICKING, b"").expect("a task panic is an Active violation");
    assert!(
        message.starts_with(&format!("{NO_TASK_PANIC}: ")),
        "{message}"
    );
}

#[test]
fn a_harness_failure_panics_with_harness() {
    let message = panic_of(&BAD_NAME, b"").expect("a harness failure panics");
    assert!(message.starts_with("harness: "), "{message}");
}

#[test]
fn a_clean_run_returns() {
    assert_eq!(panic_of(&CLEAN, b""), None);
}

std::thread_local! {
    /// Set on a thread when the hook [`unwind_task_panics_lets_a_task_panic_unwind`]
    /// installs runs there.
    static REPLACED_HOOK_RAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// libFuzzer installs a panic hook that aborts. `unwind_task_panics`
/// replaces whatever hook is installed, so a task's panic unwinds into a
/// `sim/no-task-panic` violation, and the replaced hook never runs. The
/// stand-in hook only marks the thread it runs on, so a parallel test's
/// panic can't trip this one.
#[test]
fn unwind_task_panics_lets_a_task_panic_unwind() {
    panic::set_hook(Box::new(|_| REPLACED_HOOK_RAN.with(|ran| ran.set(true))));
    super::unwind_task_panics();
    let message = panic_of(&PANICKING, b"").expect("the task panic unwound");
    assert!(
        message.starts_with(&format!("{NO_TASK_PANIC}: ")),
        "{message}"
    );
    // The task panicked on this test's worker thread, so read the mark there.
    let ran = super::WORKER.with(|worker| {
        worker
            .borrow_mut()
            .explore_with(&CLEAN, b"", None, |_, _, _| {
                REPLACED_HOOK_RAN.with(std::cell::Cell::get)
            })
    });
    assert!(!ran, "the replaced hook ran for the task's panic");
}

/// Spins once dropped, on a value no one sets, so the shutdown that drops
/// it gets stuck and abandons the run (as `zakura-sim`'s explore tests do).
struct SpinOnDrop;

impl Drop for SpinOnDrop {
    /// Spins on a value no one sets. Every load is a scheduling point.
    fn drop(&mut self) {
        let flag = zakura_rt::sync::atomic::AtomicU64::new(0);
        while flag.load(zakura_rt::sync::atomic::Ordering::SeqCst) == 0 {}
    }
}

test_harness!(
    Abandoning,
    "test/adapter/abandoning",
    |sim| {
        sim.scheduler().spawn_root("spinner", async {
            let _spin = SpinOnDrop;
            std::future::pending::<()>().await;
        });
    },
    Vec::new()
);

static ABANDONING: HarnessEntry = entry::<Abandoning>();

/// Runs `input` on `entry` on this thread's fuzz worker, as `fuzz_one`
/// does, and returns the coroutine stacks the worker's thread still held
/// once the run ended: allocated, less freed and idle. The stack pool is
/// per thread, so it's sampled inside the job, on the worker's thread.
fn held_after(entry: &'static HarnessEntry, input: &[u8]) -> u64 {
    super::WORKER.with(|worker| {
        worker
            .borrow_mut()
            .explore_with(entry, input, None, |_, _, _| {
                let pool = stack_pool();
                pool.allocated - pool.freed - pool.idle as u64
            })
    })
}

/// After a warm-up, a hundred runs of one input leave the worker's thread
/// holding nothing: no run state carries over between inputs. A control
/// whose run leaks its task shows the sample would see it.
#[test]
fn repeated_inputs_leave_no_run_state() {
    held_after(&CLEAN, b"");
    for n in 0..100 {
        assert_eq!(held_after(&CLEAN, b""), 0, "run {n}");
    }
    assert!(
        held_after(&ABANDONING, b"") > 0,
        "a leaked task's stack shows"
    );
}
