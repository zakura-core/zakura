//! The `zakura-sim` binary's `record` subcommand: what it writes reads back
//! as a complete trace.
#![cfg(zakura_sim)]

use std::process::{Command, Output};

use tempfile::TempDir;
use zakura_sim::trace::{TraceReader, INPUT_FILE};

/// Runs the binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

#[test]
fn record_an_input() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input");
    // Discovery's scenario prefix is empty, so these are all Plan bytes.
    std::fs::write(&input, [0, 1, 7, 3, 9, 1, 4, 2]).unwrap();
    let out = dir.path().join("trace");
    let output = zakura_sim(&[
        "record",
        "--sim",
        "discovery/wait-complete",
        "--input",
        input.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let trace = TraceReader::open(&out).unwrap();
    assert_eq!(trace.meta.sim, "discovery/wait-complete");
    assert_eq!(trace.meta.scenario, serde_json::json!({}));
    assert_eq!(
        std::fs::read(out.join(INPUT_FILE)).unwrap(),
        std::fs::read(&input).unwrap()
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.starts_with(&format!("trace {} in ", trace.id)),
        "{stdout}"
    );
}

#[test]
fn record_a_plan_against_a_scenario() {
    let dir = TempDir::new().unwrap();
    let plan = dir.path().join("plan");
    std::fs::write(&plan, [3, 1, 4, 1, 5]).unwrap();
    let out = dir.path().join("trace");
    let output = zakura_sim(&[
        "record",
        "--sim",
        "discovery/wait-complete",
        "--scenario",
        "{}",
        "--plan",
        plan.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let trace = TraceReader::open(&out).unwrap();
    assert_eq!(trace.meta.sim, "discovery/wait-complete");
    assert_eq!(trace.meta.input, None);
    assert!(!out.join(INPUT_FILE).exists());
}

#[test]
fn usage_errors_exit_2() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("trace");
    for args in [
        vec![],
        vec!["replay"],
        vec![
            "record",
            "--sim",
            "no/such-harness",
            "--input",
            "x",
            "--out",
            "y",
        ],
        vec![
            "record",
            "--sim",
            "header-sync/smoke",
            "--out",
            out.to_str().unwrap(),
        ],
        vec![
            "record",
            "--sim",
            "header-sync/smoke",
            "--sim",
            "header-sync/smoke",
        ],
        vec![
            "record",
            "--sim",
            "header-sync/smoke",
            "--scenario",
            "not json",
            "--plan",
            "x",
            "--out",
            out.to_str().unwrap(),
        ],
    ] {
        let output = zakura_sim(&args);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
    }
    assert!(!out.exists(), "no usage error writes a trace");
}

#[test]
fn a_scenario_that_does_not_fit_fails() {
    let dir = TempDir::new().unwrap();
    let plan = dir.path().join("plan");
    std::fs::write(&plan, []).unwrap();
    let out = dir.path().join("trace");
    let output = zakura_sim(&[
        "record",
        "--sim",
        "header-sync/smoke",
        "--scenario",
        r#"{"peers":2}"#,
        "--plan",
        plan.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(!out.exists());
}
