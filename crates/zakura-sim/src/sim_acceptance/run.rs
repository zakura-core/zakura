//! What `cargo xtask sim-acceptance` runs, and how its artifacts become the
//! sim-7 record (the sim-7 plan, "sim-7.4's tool"). The xtask only runs the
//! plan's tasks and keeps their raw output under `<artifacts>/<index>/`.
//! Every judgement is made here, from those files, so the tool's runner
//! never decides a verdict.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    deviations::DEVIATIONS,
    evaluate::{evaluate, header_refusal, Evaluation, HeaderRecordFacts, RowSpec, Verdict},
    header::{verify, Published},
    judge::{owned_run_passes, Registered, RunExpect},
    manifest::{derive, problems, rows, Expected},
};
use crate::{
    check::PropertyOutcome,
    harness::header_acceptance::tool::{Containment, Record, RunPlan},
    property::Status,
    trace::TraceReader,
};

/// One thing the xtask runs. Its artifacts go under the task's index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Task {
    /// One test by its exact name, through hdr-14's test runner, which
    /// writes `test.json`. A unit test's `file` is its library's `src/lib.rs`.
    Test {
        /// The expected result it produces.
        expected: String,
        /// The package.
        package: String,
        /// The test's file, relative to the package.
        file: String,
        /// The test's name, or a unit test's path.
        test: String,
    },
    /// One test in the `qa/fuzz/sim` workspace's library, with
    /// `--no-default-features`, which writes `test.json`.
    FuzzTest {
        /// The expected result it produces.
        expected: String,
        /// The test's path.
        test: String,
    },
    /// One `qa/suite.toml` lane's command, which writes `lane.json`.
    Lane {
        /// The expected result it produces.
        expected: String,
        /// The lane's ID.
        id: String,
        /// The lane's whole command line, as `qa/suite.toml`'s build
        /// prefixes it.
        command: String,
        /// The test targets it must run, as `<package>/<target>`.
        required: Vec<String>,
    },
    /// One committed seed's full input recorded in the release sim build,
    /// then replayed fresh, which writes `trace/` and `replay.json`.
    Seed {
        /// The expected result it produces.
        expected: String,
        /// The seed's harness.
        harness: String,
        /// Its full input's SHA-256, its file name in the run's corpus.
        input: String,
    },
    /// One seed witness recorded with `--witness`, which writes `trace/`
    /// with its `witness.json`.
    SeedWitness {
        /// The expected result it produces.
        expected: String,
        /// The seed's harness.
        harness: String,
        /// Its full input's SHA-256.
        input: String,
        /// The witnessed property.
        property: String,
        /// The expectation, as the sidecar spells it.
        expect: String,
    },
    /// One S4.6 command, which writes `command.json`.
    Command {
        /// The expected result it produces.
        expected: String,
        /// Its arguments after `cargo`.
        args: Vec<String>,
    },
    /// One planted case's verdict, through `cargo xtask planted`, which
    /// writes `command.json`.
    Planted {
        /// The expected result it produces.
        expected: String,
        /// The case's property.
        property: String,
    },
    /// One measured recording, which writes `trace/` and `measure.json`.
    Measure {
        /// The expected result it produces.
        expected: String,
        /// The harness.
        harness: String,
    },
}

impl Task {
    /// The expected result this task produces.
    pub fn expected(&self) -> &str {
        match self {
            Task::Test { expected, .. }
            | Task::FuzzTest { expected, .. }
            | Task::Lane { expected, .. }
            | Task::Seed { expected, .. }
            | Task::SeedWitness { expected, .. }
            | Task::Command { expected, .. }
            | Task::Planted { expected, .. }
            | Task::Measure { expected, .. } => expected,
        }
    }
}

/// What the tool runs, and what it can't yet, with the manifest's
/// problems.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sim7Plan {
    /// The tasks, in order. A task's artifacts go under its index.
    pub tasks: Vec<Task>,
    /// Expected results with no task yet, and why. Each blocks.
    pub unsupported: Vec<(String, String)>,
    /// Every way the manifest doesn't match the code at S. Any refuses the
    /// record.
    pub problems: Vec<String>,
}

/// The three S4.6 commands, by ID, as `cargo` arguments.
pub fn command_args(id: &str) -> Option<Vec<String>> {
    let args: &[&str] = match id {
        "cargo-tree-excludes-simulator" => &["tree", "--locked", "-p", "zakurad", "-e", "normal"],
        "check-workspace-normal" => &["check", "--workspace", "--locked"],
        "check-sim-packages" => &[
            "check",
            "--locked",
            "-p",
            "zakura-rt",
            "-p",
            "zakura-scheduler",
            "-p",
            "zakura-sim",
            "-p",
            "zakura-storage",
            "-p",
            "zakura-quic",
            "--all-targets",
        ],
        _ => return None,
    };
    Some(args.iter().map(|arg| (*arg).to_owned()).collect())
}

/// The plan at `repo`: one task per expected result the tool can run, in
/// manifest order, each listed once even when several rows share it.
pub fn plan(repo: &Path) -> Sim7Plan {
    let derived = match derive(repo) {
        Ok(derived) => derived,
        Err(problems) => {
            return Sim7Plan {
                problems,
                ..Sim7Plan::default()
            }
        }
    };
    let manifest = crate::suite::manifest(repo).ok();
    let seed_inputs: BTreeMap<String, (String, String)> = manifest
        .as_ref()
        .map(|m| {
            m.seed_report
                .seeds
                .iter()
                .filter_map(|row| {
                    Some((
                        row.file.display().to_string(),
                        (
                            row.sidecar.as_ref()?.harness.clone(),
                            row.input_sha256.clone()?,
                        ),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let mut plan = Sim7Plan {
        problems: problems(&rows(&derived), &derived),
        ..Sim7Plan::default()
    };
    let mut seen = BTreeSet::new();
    for row in rows(&derived) {
        for expected in row.expected {
            let id = expected.to_string();
            if !seen.insert(id.clone()) {
                continue;
            }
            let task = match &expected {
                Expected::Test(t) => Some(Task::Test {
                    expected: id.clone(),
                    package: t.package.to_owned(),
                    file: format!("tests/{}.rs", t.file),
                    test: t.name.to_owned(),
                }),
                Expected::UnitTest { package, path } => Some(Task::Test {
                    expected: id.clone(),
                    package: (*package).to_owned(),
                    file: "src/lib.rs".to_owned(),
                    test: (*path).to_owned(),
                }),
                Expected::FuzzTest(test) => Some(Task::FuzzTest {
                    expected: id.clone(),
                    test: (*test).to_owned(),
                }),
                Expected::Lane(lane) => lane_task(repo, &id, lane),
                Expected::Seed { file } => {
                    seed_inputs.get(file).map(|(harness, input)| Task::Seed {
                        expected: id.clone(),
                        harness: harness.clone(),
                        input: input.clone(),
                    })
                }
                Expected::SeedWitness(witness) => manifest.as_ref().and_then(|m| {
                    let row = m.seed_witnesses.iter().find(|w| &w.id == witness)?;
                    let (_, input) = seed_inputs.get(&row.seed)?;
                    Some(Task::SeedWitness {
                        expected: id.clone(),
                        harness: row.harness.clone(),
                        input: input.clone(),
                        property: row.property.clone(),
                        expect: row.witness.clone(),
                    })
                }),
                Expected::Command(command) => command_args(command).map(|args| Task::Command {
                    expected: id.clone(),
                    args,
                }),
                Expected::Planted(property) => Some(Task::Planted {
                    expected: id.clone(),
                    property: (*property).to_owned(),
                }),
                Expected::Measure(harness) => Some(Task::Measure {
                    expected: id.clone(),
                    harness: (*harness).to_owned(),
                }),
                // Judged from the header record and from seed tasks.
                Expected::HeaderKey(_) | Expected::EndRecord { .. } => None,
                Expected::RaceSchedule(_)
                | Expected::Target(_)
                | Expected::Minimize(_)
                | Expected::Agreement(_) => {
                    plan.unsupported
                        .push((id.clone(), "the tool can't run this result yet".to_owned()));
                    None
                }
            };
            match task {
                Some(task) => plan.tasks.push(task),
                None if matches!(
                    expected,
                    Expected::HeaderKey(_)
                        | Expected::EndRecord { .. }
                        | Expected::RaceSchedule(_)
                        | Expected::Target(_)
                        | Expected::Minimize(_)
                        | Expected::Agreement(_)
                ) => {}
                None => plan
                    .unsupported
                    .push((id, "no task could be built for it at S".to_owned())),
            }
        }
    }
    plan
}

/// `lane`'s task, from `qa/suite.toml` at `repo`.
fn lane_task(repo: &Path, expected: &str, lane: &str) -> Option<Task> {
    let text = fs::read_to_string(repo.join(crate::suite::SUITE_FILE)).ok()?;
    let suite: crate::suite::SuiteFile = toml::from_str(&text).ok()?;
    let row = suite.lane.iter().find(|row| row.id == lane)?;
    let (subcommand, args) = row.args.split_once(' ').unwrap_or((&row.args, ""));
    let package = args
        .split_whitespace()
        .skip_while(|word| *word != "-p")
        .nth(1)
        .unwrap_or_default();
    Some(Task::Lane {
        expected: expected.to_owned(),
        id: lane.to_owned(),
        command: row.build.command(subcommand, args),
        required: row
            .test_targets()
            .into_iter()
            .map(|target| format!("{package}/{target}"))
            .collect(),
    })
}

/// The sim-7 record.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sim7Record {
    /// The artifacts directory, relative to the run directory, as the
    /// shared runner's record publication expects.
    pub artifacts: String,
    /// The subject commit S.
    pub subject: String,
    /// S's source digest.
    pub subject_source: String,
    /// Why hdr-14's record can't be consumed, if it can't.
    pub header_refusal: Option<String>,
    /// The manifest's problems at S.
    pub problems: Vec<String>,
    /// Expected results the tool doesn't run yet.
    pub unsupported: Vec<(String, String)>,
    /// Every row's verdict.
    pub evaluation: Evaluation,
    /// Every narrowing of the spec, and whether it's signed off.
    pub deviations: Vec<(String, bool)>,
    /// No row blocked, no manifest problem, and the header record consumed.
    pub accepted: bool,
    /// Accepted, nothing excluded, and every deviation signed off.
    pub fully_accepted: bool,
}

/// Where the header record and its run are, as the evaluator reads them.
pub struct HeaderRun<'a> {
    /// The header run's `--out`.
    pub out: &'a Path,
}

/// Judges the plan's artifacts and returns the record: each task's
/// verdict from its files, each header key's from hdr-14's record once
/// it's bound to its run, and each unsupported result blocked.
pub fn record(
    repo: &Path,
    plan: &Sim7Plan,
    artifacts: &Path,
    header: &HeaderRun<'_>,
    subject: &str,
    subject_source: &str,
) -> Sim7Record {
    let mut verdicts: BTreeMap<String, Verdict> = BTreeMap::new();
    let mut applicable = 0u64;
    let mut seed_traces: BTreeMap<String, std::path::PathBuf> = BTreeMap::new();
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        let verdict = judge_task(task, &dir, subject_source, &mut applicable);
        if let Task::Seed { input, .. } = task {
            seed_traces.insert(input.clone(), dir.join("trace"));
        }
        verdicts.insert(task.expected().to_owned(), verdict);
    }
    for (id, why) in &plan.unsupported {
        verdicts.insert(id.clone(), Verdict::Blocked { why: why.clone() });
    }
    let derived = derive(repo).unwrap_or_default();
    let rows = rows(&derived);
    // End records: the seed's own trace must end exactly as committed.
    let seed_report = crate::suite::manifest(repo).map(|m| m.seed_report).ok();
    for row in &rows {
        for expected in &row.expected {
            if let Expected::EndRecord { file } = expected {
                let input = seed_report.as_ref().and_then(|report| {
                    let seed = Path::new(file).with_extension("bin");
                    report
                        .seeds
                        .iter()
                        .find(|row| row.file == seed)
                        .and_then(|row| row.input_sha256.clone())
                });
                let verdict = match input.and_then(|input| seed_traces.get(&input).cloned()) {
                    Some(trace) => end_record_matches(repo, file, &trace),
                    None => Verdict::Blocked {
                        why: "no seed run produced this end record's trace".to_owned(),
                    },
                };
                verdicts.insert(expected.to_string(), verdict);
            }
        }
    }
    let (refusal, header_record) = header_record(repo, header, subject, subject_source);
    for row in &rows {
        for expected in &row.expected {
            if let Expected::HeaderKey(key) = expected {
                let verdict = match (&refusal, &header_record) {
                    (None, Some(record)) => header_key_verdict(record, key),
                    (Some(why), _) => Verdict::Blocked { why: why.clone() },
                    (None, None) => Verdict::Blocked {
                        why: "no header record".to_owned(),
                    },
                };
                verdicts.insert(expected.to_string(), verdict);
            }
        }
    }
    let specs: Vec<RowSpec> = rows
        .iter()
        .map(|row| RowSpec {
            id: row.id.clone(),
            results: row.expected.iter().map(ToString::to_string).collect(),
            never_excluded: row.never_excluded,
        })
        .collect();
    let evaluation = evaluate(&specs, &verdicts, applicable);
    let deviations: Vec<(String, bool)> = DEVIATIONS
        .iter()
        .map(|d| (d.id.to_owned(), d.is_signed_off()))
        .collect();
    let accepted = evaluation.accepted && plan.problems.is_empty() && refusal.is_none();
    let fully_accepted =
        accepted && evaluation.fully_accepted && deviations.iter().all(|(_, signed)| *signed);
    Sim7Record {
        artifacts: artifacts
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
        subject: subject.to_owned(),
        subject_source: subject_source.to_owned(),
        header_refusal: refusal,
        problems: plan.problems.clone(),
        unsupported: plan.unsupported.clone(),
        evaluation,
        deviations,
        accepted,
        fully_accepted,
    }
}

/// One task's verdict from its artifacts in `dir`.
fn judge_task(task: &Task, dir: &Path, source: &str, applicable: &mut u64) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    let json = |name: &str| -> Option<Value> {
        serde_json::from_str(&fs::read_to_string(dir.join(name)).ok()?).ok()
    };
    match task {
        Task::Test { test, .. } | Task::FuzzTest { test, .. } => {
            let Some(result) = json("test.json") else {
                return blocked("the test left no test.json".to_owned());
            };
            exactly_one_passed(&result, test)
        }
        Task::Lane { required, .. } => match json("lane.json") {
            Some(lane) => lane_passed(&lane, required),
            None => blocked("the lane left no lane.json".to_owned()),
        },
        Task::Seed { .. } => {
            let replayed = json("replay.json")
                .and_then(|r| r.get("exited_ok").and_then(Value::as_bool))
                .unwrap_or(false);
            match judge_trace(&dir.join("trace"), source, &RunExpect::Passes, applicable) {
                Verdict::Verified if !replayed => {
                    blocked("the seed's trace didn't replay exactly in a fresh process".to_owned())
                }
                verdict => verdict,
            }
        }
        Task::SeedWitness {
            property, expect, ..
        } => {
            let passed = json("trace/witness.json")
                .and_then(|w| w.get("passed").and_then(Value::as_bool))
                .unwrap_or(false);
            if !passed {
                return blocked(format!("the witness of {property} didn't pass"));
            }
            let also: Vec<String> = json("trace/witness.json")
                .and_then(|w| serde_json::from_value(w.get("also_violates")?.clone()).ok())
                .unwrap_or_default();
            let run_expect = match expect.as_str() {
                "violated" | "violated-set" => RunExpect::Violates {
                    property: property.clone(),
                    also,
                },
                _ => RunExpect::Passes,
            };
            judge_trace(&dir.join("trace"), source, &run_expect, &mut 0)
        }
        Task::Command { args, .. } => match json("command.json") {
            Some(command) => command_passed(&command, args),
            None => blocked("the command left no command.json".to_owned()),
        },
        Task::Planted { property, .. } => match json("command.json") {
            Some(command) if command.get("exited_ok").and_then(Value::as_bool) == Some(true) => {
                Verdict::Verified
            }
            _ => blocked(format!("`cargo xtask planted {property}` didn't pass")),
        },
        Task::Measure { .. } => match json("measure.json") {
            Some(measure) if measure.get("steps").and_then(Value::as_u64).is_some() => {
                judge_trace(&dir.join("trace"), source, &RunExpect::Passes, &mut 0)
            }
            _ => blocked("the recording left no measure.json".to_owned()),
        },
    }
}

/// A test result passes only if its binary ran, exactly one test ran, and
/// it was `test` and passed.
fn exactly_one_passed(result: &Value, test: &str) -> Verdict {
    let ran: Vec<(String, String)> = result
        .get("tests")
        .and_then(|tests| serde_json::from_value(tests.clone()).ok())
        .unwrap_or_default();
    let exited = result.get("exited_ok").and_then(Value::as_bool) == Some(true);
    match &ran[..] {
        [(name, outcome)] if exited && name == test && outcome == "ok" => Verdict::Verified,
        _ => Verdict::Blocked {
            why: format!("expected {test} alone to run and pass, but ran {ran:?}"),
        },
    }
}

/// A lane passes when its command exited 0, every required target ran at
/// least one test that passed, no test of any target failed, and no test a
/// target's binary lists (`--list`, less its ignored ones) went unrun
/// (Rule 11).
fn lane_passed(lane: &Value, required: &[String]) -> Verdict {
    let exited = lane.get("exited_ok").and_then(Value::as_bool) == Some(true);
    let unrun: Vec<String> = lane
        .get("unrun")
        .and_then(|u| serde_json::from_value(u.clone()).ok())
        .unwrap_or_default();
    let targets: BTreeMap<String, Vec<(String, String)>> = lane
        .get("targets")
        .and_then(|t| serde_json::from_value(t.clone()).ok())
        .unwrap_or_default();
    if !exited {
        return Verdict::Blocked {
            why: "the lane's command failed".to_owned(),
        };
    }
    if let Some(test) = unrun.first() {
        return Verdict::Blocked {
            why: format!("{test} is listed but didn't run and pass"),
        };
    }
    for target in required {
        let ran = targets.get(target).map_or(0, |tests| {
            tests.iter().filter(|(_, outcome)| outcome == "ok").count()
        });
        if ran == 0 {
            return Verdict::Blocked {
                why: format!("required target {target} ran no test"),
            };
        }
    }
    for (target, tests) in &targets {
        if let Some((name, outcome)) = tests.iter().find(|(_, outcome)| outcome == "FAILED") {
            return Verdict::Blocked {
                why: format!("{target}: {name} {outcome}"),
            };
        }
    }
    Verdict::Verified
}

/// An S4.6 command passes when it exited 0, and the dependency-graph one
/// also when its output shows the `zakurad` root and none of the
/// simulator crates.
fn command_passed(command: &Value, args: &[String]) -> Verdict {
    let exited = command.get("exited_ok").and_then(Value::as_bool) == Some(true);
    let output = command
        .get("log")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !exited {
        return Verdict::Blocked {
            why: format!("`cargo {}` failed", args.join(" ")),
        };
    }
    if args.first().map(String::as_str) == Some("tree") {
        if !output.lines().any(|line| line.starts_with("zakurad ")) {
            return Verdict::Blocked {
                why: "`cargo tree`'s output has no zakurad root".to_owned(),
            };
        }
        if let Some(denied) = ["zakura-scheduler ", "zakura-sim ", "corosensei "]
            .iter()
            .find(|name| output.contains(*name))
        {
            return Verdict::Blocked {
                why: format!("the production graph has {}", denied.trim()),
            };
        }
    }
    Verdict::Verified
}

/// Judges the trace in `dir` with `owned_run_passes`, taking its
/// registered properties and statuses from its own meta, and requiring
/// its build to embed `source`. A passing run's applicable counts are
/// added to `applicable`.
fn judge_trace(dir: &Path, source: &str, expect: &RunExpect, applicable: &mut u64) -> Verdict {
    let trace = match TraceReader::open(dir) {
        Ok(trace) => trace,
        Err(error) => {
            return Verdict::Blocked {
                why: format!("the trace doesn't read: {error}"),
            }
        }
    };
    if trace.meta.build.source != source {
        return Verdict::Blocked {
            why: format!(
                "the trace's build has source {}, not S's {source}",
                trace.meta.build.source
            ),
        };
    }
    let Some(end) = &trace.end else {
        return Verdict::Blocked {
            why: "the trace has no end record".to_owned(),
        };
    };
    let registered = Registered {
        ids: trace.meta.properties.iter().map(|p| p.id.clone()).collect(),
        tolerated: trace
            .meta
            .properties
            .iter()
            .filter(|p| matches!(p.status, Status::Known { .. } | Status::Draft))
            .map(|p| p.id.clone())
            .collect(),
    };
    match owned_run_passes(end, &registered, expect) {
        Ok(()) => {
            *applicable += end
                .properties
                .iter()
                .map(|report| match report.outcome {
                    PropertyOutcome::Held { applicable } => u64::from(applicable),
                    _ => 0,
                })
                .sum::<u64>();
            Verdict::Verified
        }
        Err(blocked) => Verdict::Blocked {
            why: blocked.to_string(),
        },
    }
}

/// Whether the trace in `trace` ends exactly as the committed `file` says.
fn end_record_matches(repo: &Path, file: &str, trace: &Path) -> Verdict {
    let committed = fs::read_to_string(repo.join(file)).unwrap_or_default();
    let recorded = fs::read_to_string(trace.join(crate::trace::TRACE_FILE))
        .ok()
        .and_then(|text| text.lines().last().map(str::to_owned))
        .unwrap_or_default();
    if !committed.trim().is_empty() && committed.trim() == recorded.trim() {
        Verdict::Verified
    } else {
        Verdict::Blocked {
            why: "the seed's run didn't reproduce its committed end record".to_owned(),
        }
    }
}

/// hdr-14's published record at `header.out`, and why it can't be
/// consumed at `subject`, if it can't: its facts, then whether `verify`
/// binds it to its run's plan and artifacts.
fn header_record(
    repo: &Path,
    header: &HeaderRun<'_>,
    subject: &str,
    subject_source: &str,
) -> (Option<String>, Option<Record>) {
    let read =
        |path: &Path| fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()));
    let record: Record = match read(&header.out.join("record.json"))
        .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
    {
        Ok(record) => record,
        Err(why) => return (Some(format!("the header record doesn't read: {why}")), None),
    };
    let artifacts = header.out.join(&record.artifacts);
    let run_dir = artifacts.parent().unwrap_or(header.out).to_owned();
    let run_id = run_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let verified = read(&run_dir.join("plan.json"))
        .and_then(|text| serde_json::from_str::<RunPlan>(&text).map_err(|e| e.to_string()))
        .and_then(|plan| {
            verify(
                repo,
                &Published {
                    plan: &plan,
                    artifacts: &artifacts,
                    out: header.out,
                    run_id: &run_id,
                    subject_source,
                    record: &record,
                },
            )
        });
    let facts = HeaderRecordFacts {
        subject: record.subject.clone(),
        subreaper: record.containment == Containment::Subreaper,
        accepted: record.accepted,
        stray: record.stray.clone(),
        verified,
    };
    (header_refusal(&facts, subject), Some(record))
}

/// `key`'s verdict in an accepted header record.
fn header_key_verdict(record: &Record, key: &crate::harness::header_acceptance::Key) -> Verdict {
    if record.verified.iter().any(|entry| &entry.key == key) {
        Verdict::Verified
    } else if let Some(entry) = record.excluded.iter().find(|entry| &entry.key == key) {
        Verdict::Excluded {
            approval: entry.why.clone().unwrap_or_default(),
        }
    } else {
        Verdict::Blocked {
            why: "the header record doesn't verify this key".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{command_passed, exactly_one_passed, lane_passed, Verdict};

    fn blocked(verdict: &Verdict) -> bool {
        matches!(verdict, Verdict::Blocked { .. })
    }

    /// A test row passes only when its one test ran alone and passed.
    #[test]
    fn a_test_row_needs_exactly_its_test() {
        let one = json!({ "exited_ok": true, "tests": [["t", "ok"]] });
        assert_eq!(exactly_one_passed(&one, "t"), Verdict::Verified);
        let two = json!({ "exited_ok": true, "tests": [["t", "ok"], ["u", "ok"]] });
        assert!(blocked(&exactly_one_passed(&two, "t")));
        let failed = json!({ "exited_ok": false, "tests": [["t", "FAILED"]] });
        assert!(blocked(&exactly_one_passed(&failed, "t")));
        let other = json!({ "exited_ok": true, "tests": [["u", "ok"]] });
        assert!(blocked(&exactly_one_passed(&other, "t")));
    }

    /// A lane blocks on a failed command, a required target that ran no
    /// test, or any failed test.
    #[test]
    fn a_lane_needs_every_required_target_and_no_failure() {
        let required = ["zakura-sim/draws".to_owned()];
        let good = json!({ "exited_ok": true, "targets": { "zakura-sim/draws": [["a", "ok"]] } });
        assert_eq!(lane_passed(&good, &required), Verdict::Verified);
        let skipped = json!({ "exited_ok": true, "targets": { "zakura-sim/lib": [["a", "ok"]] } });
        assert!(blocked(&lane_passed(&skipped, &required)));
        let failed = json!({ "exited_ok": true, "targets": {
            "zakura-sim/draws": [["a", "ok"]], "zakura-sim/lib": [["b", "FAILED"]] } });
        assert!(blocked(&lane_passed(&failed, &required)));
        let exited =
            json!({ "exited_ok": false, "targets": { "zakura-sim/draws": [["a", "ok"]] } });
        assert!(blocked(&lane_passed(&exited, &required)));
        let ignored = json!({ "exited_ok": true, "targets": {
            "zakura-sim/draws": [["a", "ok"], ["regenerate", "ignored"]] } });
        assert_eq!(lane_passed(&ignored, &required), Verdict::Verified);
        let skipped_one = json!({ "exited_ok": true, "unrun": ["zakura-sim/draws::b"],
            "targets": { "zakura-sim/draws": [["a", "ok"]] } });
        assert!(
            blocked(&lane_passed(&skipped_one, &required)),
            "a --skip of one listed test blocks"
        );
    }

    /// `cargo tree` passes only on exit 0, with the zakurad root, and no
    /// simulator crate in the graph.
    #[test]
    fn the_dependency_graph_row_reads_cargo_tree_itself() {
        let tree = ["tree".to_owned()];
        let clean =
            json!({ "exited_ok": true, "log": "zakurad v5.0.0 (/repo)\n├── zakura-chain v5\n" });
        assert_eq!(command_passed(&clean, &tree), Verdict::Verified);
        let failed = json!({ "exited_ok": false, "log": "" });
        assert!(blocked(&command_passed(&failed, &tree)));
        let empty = json!({ "exited_ok": true, "log": "" });
        assert!(
            blocked(&command_passed(&empty, &tree)),
            "no root means it checked nothing"
        );
        let sim = json!({ "exited_ok": true, "log": "zakurad v5.0.0\n└── zakura-sim v0.0.0\n" });
        assert!(blocked(&command_passed(&sim, &tree)));
    }
}
