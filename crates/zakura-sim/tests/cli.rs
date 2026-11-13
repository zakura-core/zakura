//! The `zakura-sim` binary: what `record` writes reads back as a complete
//! trace, and `replay` reruns it Exact in a fresh process.
#![cfg(zakura_sim)]

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zakura_sim::{
    harness::HARNESSES,
    trace::{TraceReader, INPUT_FILE, MAX_INPUT_BYTES, OBSERVATIONS_FILE, TRACE_FILE},
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

/// `versions` prints the versions that fix what a harness's fuzz input
/// means, one `KEY=VALUE` line each.
#[test]
fn versions_print_key_value_lines() {
    let output = zakura_sim(&["versions", "--sim", "block-sync/storage"]);
    assert!(output.status.success(), "{output:?}");
    let entry = zakura_sim::harness::find("block-sync/storage").unwrap();
    let expected: String = zakura_sim::seed::versions(entry)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}\n"))
        .collect();
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
    assert!(expected.starts_with("harness=block-sync/storage\ngenerator="));
    let output = zakura_sim(&["versions", "--sim", "no/such-harness"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
}

/// `seeds --sim` writes each admitted seed's full fuzz input to `--out`,
/// named by its SHA-256, and a JSON report that gives paths in the
/// repository relative to its root.
#[test]
fn seeds_fill_a_corpus() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("corpus");
    let report = dir.path().join("report.json");
    let output = zakura_sim(&[
        "seeds",
        "--sim",
        "discovery/wait-complete",
        "--out",
        out.to_str().unwrap(),
        "--report",
        report.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let input = std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/discovery-wait-complete.bin"),
    )
    .unwrap();
    let digest: String = Sha256::digest(&input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let corpus: Vec<_> = std::fs::read_dir(&out).unwrap().collect();
    assert_eq!(corpus.len(), 1);
    assert_eq!(std::fs::read(out.join(&digest)).unwrap(), input);
    let report: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(report["seeds"][0]["status"], "admitted");
    assert_eq!(
        report["seeds"][0]["file"],
        "crates/zakura-sim/tests/fixtures/discovery-wait-complete.bin"
    );
    assert_eq!(report["fixtures"][0], "crates/zakura-sim/tests/fixtures");
    assert_eq!(report["seeds"][0]["input_sha256"], digest);
    assert_eq!(
        report["seeds"][0]["sidecar"]["harness"],
        "discovery/wait-complete"
    );
    assert!(report["other_harnesses"].as_u64().unwrap() > 0);
}

/// `seeds` exits 1 when a required seed fails, and writes a failed
/// optional seed to `--quarantine` with its reason.
#[test]
fn seeds_reject_and_quarantine() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let sidecar = std::fs::read_to_string(fixtures.join("discovery-wait-complete.toml")).unwrap();
    let input = std::fs::read(fixtures.join("discovery-wait-complete.bin")).unwrap();
    let dir = TempDir::new().unwrap();
    let seeds = dir.path().join("seeds");
    std::fs::create_dir(&seeds).unwrap();
    std::fs::write(seeds.join("old.bin"), &input).unwrap();
    std::fs::write(
        seeds.join("old.toml"),
        sidecar.replacen("generator = 1", "generator = 0\nrequired = false", 1),
    )
    .unwrap();
    let quarantine = dir.path().join("quarantine");
    let args = |extra: &[&'static str]| {
        let mut args = vec![
            "seeds".to_owned(),
            "--sim".to_owned(),
            "discovery/wait-complete".to_owned(),
            "--fixtures".to_owned(),
            seeds.to_str().unwrap().to_owned(),
            "--quarantine".to_owned(),
            quarantine.to_str().unwrap().to_owned(),
        ];
        args.extend(extra.iter().map(|arg| (*arg).to_owned()));
        args
    };
    let run = |args: Vec<String>| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .unwrap()
    };
    let output = run(args(&[]));
    assert!(output.status.success(), "{output:?}");
    let [record] = <[_; 1]>::try_from(quarantined(&quarantine)).expect("one record");
    assert_eq!(record.0, input);
    assert!(record.2.contains("built for generator 0"), "{}", record.2);
    std::fs::write(
        seeds.join("old.toml"),
        sidecar.replacen("generator = 1", "generator = 0", 1),
    )
    .unwrap();
    let output = run(args(&[]));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("rejected "), "{stdout}");
    assert!(
        stdout.contains("0 admitted, 0 quarantined, 1 rejected"),
        "{stdout}"
    );
}

/// An optional seed quarantined for a stale generator, before `seeds`
/// reads its file, keeps its sidecar and reason in `--quarantine`. A file
/// over `MAX_INPUT_BYTES` isn't copied, and the reason names the limit.
#[test]
fn quarantine_skips_an_oversized_file() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let sidecar = std::fs::read_to_string(fixtures.join("discovery-wait-complete.toml"))
        .unwrap()
        .replacen("generator = 1", "generator = 0\nrequired = false", 1);
    let dir = TempDir::new().unwrap();
    let seeds = dir.path().join("seeds");
    std::fs::create_dir(&seeds).unwrap();
    // A sparse file, so the test writes no data.
    std::fs::File::create(seeds.join("big.bin"))
        .unwrap()
        .set_len(MAX_INPUT_BYTES + 1)
        .unwrap();
    std::fs::write(seeds.join("big.toml"), &sidecar).unwrap();
    let quarantine = dir.path().join("quarantine");
    let output = zakura_sim(&[
        "seeds",
        "--sim",
        "discovery/wait-complete",
        "--fixtures",
        seeds.to_str().unwrap(),
        "--quarantine",
        quarantine.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&quarantine)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    let [reason, toml] = <[_; 2]>::try_from(files).expect("a sidecar and a reason, no .bin");
    assert_eq!(reason.extension().unwrap(), "reason");
    assert_eq!(toml, reason.with_extension("toml"));
    assert_eq!(std::fs::read_to_string(&toml).unwrap(), sidecar);
    let reason = std::fs::read_to_string(&reason).unwrap();
    assert!(reason.contains("built for generator 0"), "{reason}");
    assert!(
        reason.contains(&format!(
            "is {} bytes, over the {MAX_INPUT_BYTES}-byte MAX_INPUT_BYTES limit",
            MAX_INPUT_BYTES + 1
        )),
        "{reason}"
    );
}

/// Each `.bin` file in the quarantine directory `dir`, with its sidecar and
/// its reason, sorted by sidecar.
fn quarantined(dir: &Path) -> Vec<(Vec<u8>, String, String)> {
    let mut records: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "bin"))
        .map(|path| {
            (
                std::fs::read(&path).unwrap(),
                std::fs::read_to_string(path.with_extension("toml")).unwrap(),
                std::fs::read_to_string(path.with_extension("reason")).unwrap(),
            )
        })
        .collect();
    records.sort_by(|a, b| a.1.cmp(&b.1));
    records
}

/// Two quarantined `plan` seeds with the same Plan bytes and different
/// prefixes keep separate records, each with its own sidecar.
#[test]
fn quarantine_keeps_each_sidecar() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let input = std::fs::read(fixtures.join("block-sync-stale-beside-current.bin")).unwrap();
    let plan = &input[2..];
    let sidecar = std::fs::read_to_string(fixtures.join("block-sync-stale-beside-current.toml"))
        .unwrap()
        .replace(
            &zakura_sim::seed::sha256(&input),
            &zakura_sim::seed::sha256(plan),
        );
    let dir = TempDir::new().unwrap();
    let seeds = dir.path().join("seeds");
    std::fs::create_dir(&seeds).unwrap();
    let sidecars: Vec<String> = ["ff06", "ff0c"]
        .into_iter()
        .map(|prefix| {
            sidecar.replacen(
                "generator = 2",
                &format!("generator = 0\nrequired = false\nkind = \"plan\"\nprefix = \"{prefix}\""),
                1,
            )
        })
        .collect();
    for (stem, sidecar) in ["a", "b"].into_iter().zip(&sidecars) {
        std::fs::write(seeds.join(format!("{stem}.bin")), plan).unwrap();
        std::fs::write(seeds.join(format!("{stem}.toml")), sidecar).unwrap();
    }
    let quarantine = dir.path().join("quarantine");
    let output = zakura_sim(&[
        "seeds",
        "--sim",
        "block-sync/storage",
        "--fixtures",
        seeds.to_str().unwrap(),
        "--quarantine",
        quarantine.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{output:?}");
    let records = quarantined(&quarantine);
    assert_eq!(
        records
            .iter()
            .map(|(bytes, sidecar, _)| (bytes.as_slice(), sidecar))
            .collect::<Vec<_>>(),
        vec![(plan, &sidecars[0]), (plan, &sidecars[1])]
    );
}
