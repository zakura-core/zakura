//! The evidence hub's boundary reads on `header-sync/wire` (M6a-7a): the
//! chunk budget's counters, read at every step boundary with no scheduling
//! point. These only build with `RUSTFLAGS="--cfg zakura_sim"`, in a
//! separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

use std::{fs, path::PathBuf, time::Instant};

use zakura_sim::{
    harness::{
        header_node::reads::without_reads,
        header_sync::{HeaderWire, WireScenario},
        run_prepared,
        wire_plans::SavedPlan,
        HarnessError, Subsystem,
    },
    observe::{ComponentReading, ComponentValue},
    MemorySink, Observation, Plan, RunReport,
};

/// Saved Plan `plan`'s committed input.
fn committed(plan: SavedPlan) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-wire-{}.bin", plan.name()));
    fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// Runs a committed input as `zakura-sim record --input` does. Returns the
/// report and the trace's lines.
fn replay(input: &[u8]) -> (RunReport, Vec<String>) {
    let (prefix, plan) = input.split_at(HeaderWire::SCENARIO_BYTES);
    let scenario = HeaderWire::draw(prefix);
    run(&scenario, plan)
}

/// Runs `scenario` under the fuzz Plan `plan`.
fn run(scenario: &WireScenario, plan: &[u8]) -> (RunReport, Vec<String>) {
    let mut sink = MemorySink::default();
    let ports = HeaderWire::prepare(scenario);
    let report =
        match run_prepared::<HeaderWire>(scenario, ports, Plan::from_fuzz(plan), &mut sink, None) {
            Ok(report) => report,
            Err(HarnessError::Run { report, .. }) => *report,
            Err(error) => panic!("{error}"),
        };
    (report, sink.lines())
}

/// A trace's recorded choices: every line but each step's `obs` digest,
/// which covers the logged reads.
fn choices(lines: &[String]) -> Vec<serde_json::Value> {
    lines
        .iter()
        .map(|line| {
            let mut record: serde_json::Value =
                serde_json::from_str(line).expect("trace lines are JSON");
            if let Some(step) = record.as_object_mut().filter(|r| r.contains_key("step")) {
                step.remove("obs");
            }
            record
        })
        .filter(|record| record.get("end").is_none())
        .collect()
}

/// Every task stop the run logged, with its step, op, object and site.
fn stops(report: &RunReport) -> Vec<(u64, &Observation)> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.obs.ran_task().is_some())
        .map(|entry| (entry.step, &entry.obs))
        .collect()
}

/// The budget's logged reads, with their steps.
fn budget_reads(report: &RunReport) -> Vec<(u64, &ComponentReading)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ComponentRead {
                component, read, ..
            } if component == "Budget" => Some((entry.step, read)),
            _ => None,
        })
        .collect()
}

/// The budget read's owned count, if it's current.
fn owned(read: &ComponentReading) -> Option<u64> {
    match read {
        ComponentReading::Current(ComponentValue::Budget { owned, .. }) => Some(*owned),
        _ => None,
    }
}

/// The budget read's reserved and owned counts together, if it's current.
fn claimed(read: &ComponentReading) -> Option<u64> {
    match read {
        ComponentReading::Current(ComponentValue::Budget {
            reserved, owned, ..
        }) => Some(reserved + owned),
        _ => None,
    }
}

/// Every saved Plan takes the same picks, draws and task stops with the
/// budget's boundary reads as without them, and its end record is the
/// same. The runs with reads first show the reads ran: the budget reads
/// current, with headers owned, and in transition while the reactor
/// holds its lock.
///
/// This fails if a read reaches a scheduling point the scheduler records,
/// or changes what any task does.
#[test]
fn budget_reads_add_no_scheduling_point() {
    let (mut owning, mut held) = (false, false);
    for plan in SavedPlan::ALL {
        let input = committed(plan);
        let (read, read_lines) = replay(&input);
        let (unread, unread_lines) = without_reads(|| replay(&input));
        let reads = budget_reads(&read);
        assert!(!reads.is_empty(), "{}: the budget is read", plan.name());
        assert!(budget_reads(&unread).is_empty(), "{}", plan.name());
        owning |= reads.iter().any(|(_, read)| owned(read) > Some(0));
        held |= reads
            .iter()
            .any(|(_, read)| matches!(read, ComponentReading::Held { .. }));
        assert_eq!(read.end, unread.end, "{}", plan.name());
        assert_eq!(
            choices(&read_lines),
            choices(&unread_lines),
            "{}",
            plan.name()
        );
        assert_eq!(
            read_lines.last(),
            unread_lines.last(),
            "{}: the end record",
            plan.name()
        );
        assert_eq!(stops(&read), stops(&unread), "{}", plan.name());
    }
    assert!(owning, "some read shows owned headers");
    assert!(held, "some read shows the budget held");
}

/// The step at whose boundary each of `report`'s task stops was logged,
/// with the stop.
fn stop_at(report: &RunReport, step: u64) -> Option<&Observation> {
    stops(report)
        .into_iter()
        .find(|(at, _)| *at == step)
        .map(|(_, stop)| stop)
}

/// A boundary where the reactor stopped at `release_owned`'s or
/// `release_reserved`'s watch send (`peer_work.rs`), with the budget's lock
/// held and its count already lowered, reads the budget held without
/// blocking, never current. The next read is current with the lowered
/// count, and the run reaches both releases. The honest sync reaches
/// it, and its schedule is the same without reads
/// ([`budget_reads_add_no_scheduling_point`]).
///
/// This fails if the read takes the lock through a sim `try_lock`, which
/// reaches a point, or reports the held state as current.
#[test]
fn a_parked_holder_reads_in_transition() {
    let (report, _) = replay(&committed(SavedPlan::HonestSync));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let reads = budget_reads(&report);
    let (mut owned_released, mut reserved_released) = (false, false);
    for (index, (step, read)) in reads.iter().enumerate() {
        let ComponentReading::Held { holder } = read else {
            continue;
        };
        let stop = stop_at(&report, *step);
        let Some(Observation::TaskReached { task, at, site, .. }) = stop else {
            panic!("a held budget at step {step} with the reactor stopped at {stop:?}");
        };
        assert!(
            task.starts_with("header_sync_reactor") && at.starts_with("watch#"),
            "{task} {at}"
        );
        assert!(
            site.as_deref()
                .is_some_and(|site| site.contains("header_sync/scheduler/peer_work.rs")),
            "{site:?}"
        );
        assert!(holder.starts_with("task "), "{holder}");
        let Some(before) = reads[..index]
            .iter()
            .rev()
            .find_map(|(_, read)| Some((owned(read)?, claimed(read)?)))
        else {
            panic!("a release at step {step} before any current read");
        };
        let (_, next) = reads.get(index + 1).expect("the budget is read again");
        let after = (
            owned(next).expect("the next read after the release is current"),
            claimed(next).expect("it's current"),
        );
        assert!(
            after.1 < before.1,
            "the release lowered the claim from {before:?} to {after:?}"
        );
        owned_released |= after.0 < before.0;
        reserved_released |= after.0 == before.0;
    }
    assert!(
        owned_released && reserved_released,
        "the honest sync releases owned and reserved headers: {owned_released} \
         {reserved_released}"
    );
}

/// Prints each saved Plan's wall time with boundary reads on and off,
/// three runs each, alternating, and the cost per step. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test --release -p zakura-sim --test
/// header_reads -- --ignored --nocapture boundary_read_cost`.
#[test]
#[ignore = "a timing bench, not a check"]
#[allow(clippy::print_stdout, reason = "the bench's report is its output")]
fn boundary_read_cost() {
    const RUNS: u32 = 3;
    let (mut on_total, mut off_total, mut steps_total) = (0.0, 0.0, 0_u64);
    for plan in SavedPlan::ALL {
        let input = committed(plan);
        let (mut on, mut off, mut steps) = (0.0, 0.0, 0);
        for _ in 0..RUNS {
            let start = Instant::now();
            let (report, _) = replay(&input);
            on += start.elapsed().as_secs_f64();
            steps = report.end.steps;
            let start = Instant::now();
            without_reads(|| replay(&input));
            off += start.elapsed().as_secs_f64();
        }
        let (on, off) = (on / f64::from(RUNS), off / f64::from(RUNS));
        println!(
            "{:<28} steps {steps:>6}  on {on:>8.3}s  off {off:>8.3}s  {:>+6.1}%  {:>+7.2} us/step",
            plan.name(),
            100.0 * (on - off) / off,
            1e6 * (on - off) / steps as f64, // fewer than 2^53 steps
        );
        on_total += on;
        off_total += off;
        steps_total += steps;
    }
    println!(
        "{:<28} steps {steps_total:>6}  on {on_total:>8.3}s  off {off_total:>8.3}s  {:>+6.1}%  {:>+7.2} us/step",
        "all",
        100.0 * (on_total - off_total) / off_total,
        1e6 * (on_total - off_total) / steps_total as f64, // fewer than 2^53 steps
    );
}
