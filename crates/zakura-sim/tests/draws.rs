//! S4.1 and S4.2: every runtime draw reaches the run's `Plan` once, replays,
//! and can happen while a task runs. Also M1.6's order of draws in one step.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc};

use rand_10::Rng;
use support::{config, inputs, recorded};
use tokio_stream::StreamExt;
use zakura_rt::{collections::HashMap, stream::StreamMap};
use zakura_sim::{
    record::{DrawRecord, Record, StepRecord},
    HexU64, NoChecks, Outcome, Plan, PlanInput, Sim,
};

/// The draws in `lines`: the `init` record's, then each step's, in order.
fn draws(lines: &[String]) -> Vec<DrawRecord> {
    lines
        .iter()
        .flat_map(|line| match serde_json::from_str::<Record>(line).unwrap() {
            Record::Init(init) => init.draws,
            Record::Step(step) => step.draws,
            Record::End(_) => Vec::new(),
        })
        .collect()
}

/// The step records in `lines`.
fn steps(lines: &[String]) -> Vec<StepRecord> {
    lines
        .iter()
        .filter_map(|line| match serde_json::from_str::<Record>(line).unwrap() {
            Record::Step(step) => Some(step),
            Record::Init(_) | Record::End(_) => None,
        })
        .collect()
}

/// One task that takes one draw of each kind: an `rng()` value, a 3-branch
/// unbiased `select!`, a 3-stream `StreamMap`, and a hashed map. Each value
/// it saw goes into `seen`.
fn one_of_each(sim: &mut Sim, seen: Rc<RefCell<Vec<u64>>>) {
    sim.scheduler().spawn_root("root", async move {
        seen.borrow_mut().push(zakura_rt::rand::rng().next_u64());
        let branch = zakura_rt::select! {
            () = std::future::ready(()) => 0,
            () = std::future::ready(()) => 1,
            () = std::future::ready(()) => 2,
        };
        seen.borrow_mut().push(branch);
        let mut map = StreamMap::new();
        for key in 0..3u64 {
            map.insert(key, tokio_stream::iter([key]));
        }
        let (key, _) = map.next().await.expect("every stream has an item");
        seen.borrow_mut().push(key);
        let hashed: HashMap<u64, ()> = (0..8).map(|k| (k, ())).collect();
        seen.borrow_mut().extend(hashed.keys().copied());
    });
}

/// Runs [`one_of_each`] under `plan`. Returns the plan it resolved, its
/// records, and what it saw.
fn run_one_of_each(plan: PlanInput) -> (Plan, Vec<String>, Vec<u64>) {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let out = seen.clone();
    let (report, lines) = recorded(
        config(2, 1_000),
        plan,
        move |sim| one_of_each(sim, out),
        &mut NoChecks,
    );
    assert!(
        matches!(report.end.outcome, Outcome::Idle { .. }),
        "{:?}",
        report.end
    );
    (report.resolved, lines, seen.take())
}

#[test]
fn each_draw_reaches_plan_once() {
    for plan in inputs(50, 64) {
        let (_, lines, _) = run_one_of_each(plan);
        let shown: Vec<(String, HexU64)> = draws(&lines)
            .into_iter()
            .map(|d| (d.draw, d.bound))
            .collect();
        assert_eq!(
            shown,
            [
                ("root.1:rng#1".to_owned(), HexU64(0)),
                ("root.1:select#1".to_owned(), HexU64(3)),
                ("root.1:stream-map#1".to_owned(), HexU64(3)),
                ("root.1:hash-seed#1".to_owned(), HexU64(0)),
            ]
        );
    }
}

#[test]
fn replayed_draws_reproduce_values() {
    let mut values = std::collections::BTreeSet::new();
    for plan in inputs(50, 64) {
        let (resolved, lines, seen) = run_one_of_each(plan.clone());
        for record in draws(&lines) {
            assert!(record.bound.0 == 0 || record.value.0 < record.bound.0);
            values.insert(record.value);
        }
        let (_, again, seen_again) = run_one_of_each(plan);
        assert_eq!((again, seen_again), (lines.clone(), seen.clone()));
        let (_, scripted, scripted_seen) = run_one_of_each(PlanInput::Script(resolved));
        assert_eq!(draws(&scripted), draws(&lines));
        assert_eq!(scripted_seen, seen);
    }
    assert!(values.len() > 4, "the bytes change the answers");
}

#[test]
fn setup_draws_go_to_init() {
    let (_, lines) = recorded(
        config(2, 1_000),
        Plan::from_fuzz(&[7; 16]),
        |_| {
            // Setup code, such as a constructor, drawing before any step.
            let _ = zakura_rt::rand::rng().next_u64();
        },
        &mut NoChecks,
    );
    let Record::Init(init) = serde_json::from_str::<Record>(&lines[0]).unwrap() else {
        panic!("the first record is init: {}", lines[0]);
    };
    assert_eq!(init.draws.len(), 1);
    assert_eq!(init.draws[0].draw, "setup:rng#1");
    assert_eq!(init.draws[0].value, HexU64(0x0707_0707_0707_0707));
}

#[test]
fn draw_inside_run_no_borrow_failure() {
    for plan in inputs(1_000, 32) {
        let (_, lines, _) = run_one_of_each(plan);
        assert_eq!(draws(&lines).len(), 4);
    }
}

#[test]
fn draws_keep_order_within_step() {
    let (_, lines) = recorded(
        config(2, 1_000),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("root", async {
                // Neither takes a preemption point, so both draw in one step.
                let _ = zakura_rt::rand::rng().next_u64();
                let _: HashMap<u8, u8> = HashMap::default();
            });
        },
        &mut NoChecks,
    );
    let with_draws: Vec<Vec<String>> = steps(&lines)
        .into_iter()
        .filter(|s| !s.draws.is_empty())
        .map(|s| s.draws.into_iter().map(|d| d.draw).collect())
        .collect();
    assert_eq!(with_draws, [["root.1:rng#1", "root.1:hash-seed#1"]]);
}

#[test]
fn a_checker_draw_is_a_harness_failure() {
    let mut checker = zakura_sim::check::after_step(|_| {
        // The run's first hashed map draws its hash seed.
        let _: HashMap<u8, u8> = HashMap::default();
        None
    });
    let (report, lines) = recorded(
        config(2, 100),
        Plan::from_fuzz(&[]),
        |sim| {
            sim.scheduler().spawn_root("root", async {});
        },
        &mut checker,
    );
    assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert_eq!(
        report.end.harness,
        Some(zakura_sim::HarnessFailure::DrawOutsideStep)
    );
    assert!(
        draws(&lines).is_empty(),
        "the checker's draw took no plan input"
    );
}

#[test]
fn cleanup_draws_rotate_so_a_select_loop_exits() {
    // Runs on its own thread, so a hang fails the test instead of the suite.
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let config = zakura_sim::RunConfig {
            drain_bound: 0,
            ..config(2, 1)
        };
        let exited = Rc::new(std::cell::Cell::new(false));
        let exited_in_task = exited.clone();
        let (report, _) = recorded(
            config,
            Plan::from_fuzz(&[]),
            |sim| {
                sim.scheduler().spawn_root("looper", async move {
                    loop {
                        zakura_rt::select! {
                            () = std::future::ready(()) => continue,
                            () = std::future::ready(()) => break,
                        }
                    }
                    exited_in_task.set(true);
                });
            },
            &mut NoChecks,
        );
        let _ = done.send((
            report.end.outcome,
            exited.get(),
            report.cleanup.unwound,
            report.cleanup.leaked,
        ));
    });
    let (outcome, exited, unwound, leaked) = finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("shutdown finished the select loop");
    assert_eq!(outcome, Outcome::Budget);
    // The cleanup point bound would also end a stuck loop, by unwinding it.
    assert!(exited, "the loop took its exit branch during cleanup");
    assert_eq!(
        (unwound, leaked),
        (0, 0),
        "nothing was force-unwound or leaked"
    );
}

/// A sink that builds the run's first hashed map when it gets `init`.
struct DrawingSink(zakura_sim::MemorySink);

impl zakura_sim::RecordSink for DrawingSink {
    fn record(&mut self, record: Record) {
        if matches!(record, Record::Init(_)) {
            let _: HashMap<u8, u8> = HashMap::default();
        }
        self.0.record(record);
    }
}

#[test]
fn a_sink_draw_is_a_harness_failure() {
    for spawn_task in [false, true] {
        let mut sim = Sim::new(config(2, 100), Plan::from_fuzz(&[7; 16]));
        if spawn_task {
            sim.scheduler().spawn_root("root", async {});
        }
        let mut sink = DrawingSink(zakura_sim::MemorySink::default());
        let report = sim.run(&mut NoChecks, &mut sink);
        assert_eq!(
            report.end.harness,
            Some(zakura_sim::HarnessFailure::DrawOutsideStep),
            "with a task: {spawn_task}"
        );
        assert!(
            draws(&sink.0.lines()).is_empty(),
            "the sink's draw took no plan input"
        );
    }
}

#[test]
fn an_at_end_draw_is_a_harness_failure() {
    let mut checker = zakura_sim::check::FnChecker::new(
        |_: &zakura_sim::StepCtx<'_>| None,
        |_: &zakura_sim::RunEnd, _: &zakura_sim::ObservationLog| {
            // The run's first hashed map draws its hash seed.
            let _: HashMap<u8, u8> = HashMap::default();
            Vec::new()
        },
    );
    let (report, lines) = recorded(
        config(2, 100),
        Plan::from_fuzz(&[7; 16]),
        |_| {},
        &mut checker,
    );
    assert_eq!(
        report.end.harness,
        Some(zakura_sim::HarnessFailure::DrawOutsideStep),
        "{:?}",
        report.end
    );
    assert!(draws(&lines).is_empty(), "the draw took no plan input");
}
