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
