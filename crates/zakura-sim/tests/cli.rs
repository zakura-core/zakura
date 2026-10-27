//! The `zakura-sim` binary: what `record` writes reads back as a complete
//! trace, and `replay` reruns it Exact in a fresh process.
#![cfg(zakura_sim)]

use std::{
    path::Path,
    process::{Command, Output},
};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zakura_sim::{
    harness::HARNESSES,
    trace::{TraceReader, INPUT_FILE, OBSERVATIONS_FILE, TRACE_FILE},
};

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

/// Records `harness` on the full fuzz input `input` into `dir`'s `trace`.
fn record(dir: &Path, harness: &str, input: &[u8]) -> std::path::PathBuf {
    let file = dir.join("input");
    std::fs::write(&file, input).unwrap();
    let out = dir.join("trace");
    let output = zakura_sim(&[
        "record",
        "--sim",
        harness,
        "--input",
        file.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{harness}: {output:?}");
    out
}

/// Runs `replay` on `trace` with `flags`.
fn replay(trace: &Path, flags: &[&str]) -> Output {
    let mut args = vec!["replay", trace.to_str().unwrap()];
    args.extend(flags);
    zakura_sim(&args)
}

#[test]
fn every_harness_replays_byte_for_byte_in_a_fresh_process() {
    // Empty input is all defaults. The others pick scenario variants, two
    // workers, and byte-chosen picks and draws.
    let inputs: [&[u8]; 4] = [
        &[],
        &[0, 1, 3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7],
        &[2, 1, 2, 7, 1, 8, 2, 8, 1, 8, 2, 8, 4, 5, 9, 0],
        &[4, 0, 255, 0, 128, 7, 64, 3, 32, 1, 16, 9, 8, 2, 4, 6],
    ];
    for harness in HARNESSES {
        for input in inputs {
            let dir = TempDir::new().unwrap();
            let trace = record(dir.path(), harness.name, input);
            let output = replay(&trace, &[]);
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success()
                    && stdout.contains("every pick, draw and observation matched"),
                "{} on {input:?}: {output:?}",
                harness.name
            );
        }
    }
}

/// Rewrites line `line` of `trace`'s `trace.jsonl` through `edit`.
fn edit_line(trace: &Path, line: usize, edit: impl FnOnce(&mut Value)) {
    let path = trace.join(TRACE_FILE);
    let mut lines: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    edit(&mut lines[line]);
    let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(path, text).unwrap();
}

#[test]
fn replay_exit_codes() {
    let dir = TempDir::new().unwrap();
    let trace = record(dir.path(), "discovery/wait-complete", &[0, 1, 7, 3, 9, 1]);

    // A dirty build with another executable is never equivalent, and the
    // flag never makes a replay a success.
    let mismatched = TempDir::new().unwrap();
    let copy = record(
        mismatched.path(),
        "discovery/wait-complete",
        &[0, 1, 7, 3, 9, 1],
    );
    edit_line(&copy, 0, |meta| {
        meta["meta"]["build"]["binary"] = json!("0".repeat(64));
        meta["meta"]["build"]["dirty"] = json!(true);
    });
    assert_eq!(replay(&copy, &[]).status.code(), Some(1));
    let output = replay(&copy, &["--allow-build-mismatch"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("never a successful replay"));

    // The flag alone fails a replay, on the recording executable too.
    let output = replay(&trace, &["--allow-build-mismatch"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");

    // A prefix that matches exits 0, and says it isn't complete.
    let path = trace.join(TRACE_FILE);
    let text = std::fs::read_to_string(&path).unwrap();
    let cut: String = text.lines().take(6).map(|l| format!("{l}\n")).collect();
    std::fs::write(&path, cut).unwrap();
    assert_eq!(
        replay(&trace, &[]).status.code(),
        Some(1),
        "a prefix isn't complete"
    );
    let output = replay(&trace, &["--prefix"]);
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("not a complete run"));
    let output = replay(&trace, &["--prefix", "--allow-build-mismatch"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");

    // A changed pick diverges.
    edit_line(&trace, 3, |step| step["pick"] = json!("task:gone.1:poll#1"));
    let output = replay(&trace, &["--prefix"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("diverged"));

    assert_eq!(
        replay(&dir.path().join("missing"), &[]).status.code(),
        Some(1)
    );
    for flags in [&["--bogus"][..], &["extra"], &["--prefix", "--prefix"]] {
        assert_eq!(replay(&trace, flags).status.code(), Some(2), "{flags:?}");
    }
}

#[test]
fn observe_exports_typed_observations_in_a_fresh_process() {
    let dir = TempDir::new().unwrap();
    let trace = record(dir.path(), "discovery/wait-complete", &[0, 1, 7, 3, 9, 1]);
    let output = replay(&trace, &["--observe"]);
    assert!(output.status.success(), "{output:?}");
    let steps = TraceReader::open(&trace).unwrap().steps;
    let log = std::fs::read_to_string(trace.join(OBSERVATIONS_FILE)).unwrap();
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.len(), steps.len() + 1, "steps 0 to the last");
    let mut tags = Vec::new();
    for (n, line) in lines.iter().enumerate() {
        let parsed: Value = serde_json::from_str(line).unwrap();
        assert_eq!(parsed["step"], json!(n));
        for obs in parsed["obs"].as_array().unwrap() {
            tags.push(
                obs["obs"]
                    .as_str()
                    .expect("each observation names its kind")
                    .to_owned(),
            );
        }
        // Step 0 has no digest, and every other line hashes, byte for byte,
        // to its step's.
        if n > 0 {
            let obs = line
                .strip_prefix(&format!(r#"{{"step":{n},"obs":"#))
                .and_then(|rest| rest.strip_suffix('}'))
                .unwrap();
            let digest: String = Sha256::digest(obs.as_bytes())[..8]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(
                steps[n - 1].obs.as_deref(),
                Some(digest.as_str()),
                "step {n}"
            );
        }
    }
    for tag in [
        "task-spawned",
        "task-reached",
        "task-waiting",
        "task-exited",
    ] {
        assert!(tags.iter().any(|t| t == tag), "{tag} in {tags:?}");
    }
}
