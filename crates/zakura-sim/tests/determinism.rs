//! M1.6: equal scenario, `Plan` and build give equal records, in this process
//! or a fresh one.
#![cfg(zakura_sim)]

mod support;

use std::{process::Command, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};
use support::{config, inputs, recorded};
use zakura_rt::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
    task, time,
};
use zakura_sim::{NoChecks, PlanInput, Record, Sim};

/// Three tasks that share a lock and a counter, sleep and yield, so the
/// schedule shows in the observations.
fn workload(sim: &mut Sim) {
    let lock = Arc::new(Mutex::new(0u32));
    let hits = Arc::new(AtomicU32::new(0));
    for (label, ms) in [("a", 1), ("b", 2), ("c", 3)] {
        let (lock, hits) = (lock.clone(), hits.clone());
        sim.scheduler().spawn_root(label, async move {
            for _ in 0..3 {
                *lock.lock().unwrap() += 1;
                hits.fetch_add(1, Ordering::AcqRel);
                time::sleep(Duration::from_millis(ms)).await;
                task::yield_now().await;
            }
        });
    }
}

/// The workload's records under `plan`.
fn records(plan: PlanInput) -> Vec<String> {
    recorded(config(2, 10_000), plan, workload, &mut NoChecks).1
}

#[test]
fn same_plan_same_records() {
    for plan in inputs(100, 64) {
        assert_eq!(records(plan.clone()), records(plan));
    }
}

#[test]
fn same_after_unrelated_run() {
    let mut plans = inputs(40, 64).into_iter();
    let a = plans.next().unwrap();
    let first = records(a.clone());
    let mut differed = false;
    for b in plans {
        differed |= records(b) != first;
        assert_eq!(records(a.clone()), first, "A, then B, then A again");
    }
    assert!(
        differed,
        "some other plan changes the run, so the check means something"
    );
}

#[test]
fn resolved_plan_replays() {
    for plan in inputs(100, 64) {
        let (report, from_bytes) = recorded(config(2, 10_000), plan, workload, &mut NoChecks);
        let (_, scripted) = recorded(
            config(2, 10_000),
            PlanInput::Script(report.resolved),
            workload,
            &mut NoChecks,
        );
        assert_eq!(picks_and_digests(&scripted), picks_and_digests(&from_bytes));
    }
}

/// Each step's pick and `obs` digest, and the `end` record. A script's picks
/// carry no byte offsets, so whole step records differ.
fn picks_and_digests(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .map(|line| match serde_json::from_str::<Record>(line).unwrap() {
            Record::Step(step) => format!("{} {:?}", step.pick, step.obs),
            Record::Init(_) | Record::End(_) => line.clone(),
        })
        .collect()
}

/// Set when this test binary runs as the fresh-process child.
const CHILD: &str = "ZAKURA_SIM_DETERMINISM_CHILD";

/// A digest over the records of a fixed set of runs.
fn fingerprint() -> String {
    let mut hasher = Sha256::new();
    for plan in inputs(20, 64) {
        for line in records(plan) {
            hasher.update(line.as_bytes());
            hasher.update(b"\n");
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Marks the fingerprint in the child's output. libtest may print its own
/// `test ... ` prefix on the same line, as it does with one test thread.
const MARKER: &str = "zakura-sim-fingerprint=";

/// Prints the fingerprint when run as the child, and does nothing otherwise.
#[test]
fn fresh_process_child() {
    if std::env::var_os(CHILD).is_some() {
        #[allow(
            clippy::print_stdout,
            reason = "the parent test reads the child's stdout"
        )]
        {
            println!("\n{MARKER}{}", fingerprint());
        }
    }
}

/// The fingerprint a fresh run of this test binary prints, with
/// `RUST_TEST_THREADS` set to `threads`, or unset.
fn child_fingerprint(threads: Option<&str>) -> String {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", "fresh_process_child", "--nocapture"])
        .env(CHILD, "1");
    match threads {
        Some(threads) => child.env("RUST_TEST_THREADS", threads),
        None => child.env_remove("RUST_TEST_THREADS"),
    };
    let output = child.output().expect("the test binary reruns itself");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let at = stdout
        .find(MARKER)
        .unwrap_or_else(|| panic!("the child printed no fingerprint: {stdout}"))
        + MARKER.len();
    stdout[at..]
        .chars()
        .take_while(char::is_ascii_hexdigit)
        .collect()
}

#[test]
fn same_records_in_a_fresh_process() {
    let expected = fingerprint();
    assert_eq!(expected.len(), 64);
    for threads in [None, Some("1"), Some("4")] {
        assert_eq!(
            child_fingerprint(threads),
            expected,
            "RUST_TEST_THREADS={threads:?}"
        );
    }
}
