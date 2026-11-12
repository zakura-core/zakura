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
    header::{consumed_cleanup, verify, Published},
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
        /// The test targets it must run, as `<package>/<target>`, with
        /// `lib` for a library's unit tests.
        required: Vec<String>,
        /// The targets it selects that must list no test, as a `sim-package`
        /// row's `lib = false` says.
        empty: Vec<String>,
        /// The ignored tests `qa/suite.toml` marks `run = true` in its
        /// targets, as `<package>/<target>::<test>`, each of which must run.
        ignored_run: Vec<String>,
    },
    /// One committed seed's full input recorded in the release sim build,
    /// then replayed fresh, which writes `trace/` and `replay.json`.
    Seed {
        /// The expected result it produces.
        expected: String,
        /// The seed's file, relative to the repository's root, which its
        /// committed end record, if any, sits beside.
        file: String,
        /// The seed's harness.
        harness: String,
        /// Its full input's SHA-256, its file name in the run's corpus.
        input: String,
    },
    /// One seed witness recorded with `--witness`, then its trace replayed
    /// fresh, which writes `trace/` with its `witness.json`, and
    /// `replay.json`.
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
        /// Whether it runs in the simulator build, `--cfg zakura_sim`.
        sim: bool,
    },
    /// One measured recording, which writes `trace/` and `measure.json`.
    Measure {
        /// The expected result it produces.
        expected: String,
        /// The harness.
        harness: String,
        /// The full input's SHA-256 in the run's corpus, the harness's first
        /// committed seed, or `None` for empty input when it has none.
        input: Option<String>,
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

/// An S4.6 command, by ID, as `cargo` arguments: the dependency graph, the
/// normal workspace check, or one `sim-package` row's check in the
/// simulator build, selecting exactly what `qa/suite.toml` names for it.
pub fn command_args(repo: &Path, id: &str) -> Option<Vec<String>> {
    if let Some(name) = id.strip_prefix("check-sim-package/") {
        let text = fs::read_to_string(repo.join(crate::suite::SUITE_FILE)).ok()?;
        let suite: crate::suite::SuiteFile = toml::from_str(&text).ok()?;
        let row = suite.sim_package.iter().find(|row| row.name == name)?;
        let mut args = vec![
            "check".to_owned(),
            "--locked".to_owned(),
            "-p".to_owned(),
            name.to_owned(),
        ];
        if row.lib {
            args.push("--lib".to_owned());
        }
        match &row.tests {
            Some(tests) => {
                for test in tests {
                    args.extend(["--test".to_owned(), test.clone()]);
                }
            }
            None => args.push("--tests".to_owned()),
        }
        return Some(args);
    }
    let args: &[&str] = match id {
        // As the lint step reads it: the node's package is `zakura`.
        "cargo-tree-excludes-simulator" => &[
            "tree",
            "--locked",
            "-p",
            "zakura",
            "-e",
            "normal,build",
            "--target",
            "all",
            "--prefix",
            "none",
        ],
        "check-workspace-normal" => &["check", "--workspace", "--locked"],
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
                        file: file.clone(),
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
                Expected::Command(command) => {
                    command_args(repo, command).map(|args| Task::Command {
                        expected: id.clone(),
                        args,
                        sim: command.starts_with("check-sim-package/"),
                    })
                }
                Expected::SelfTest(path) => Some(Task::Test {
                    expected: id.clone(),
                    package: "zakura-sim".to_owned(),
                    file: "tests/sim_acceptance.rs".to_owned(),
                    test: path.clone(),
                }),
                Expected::Measure(harness) => Some(Task::Measure {
                    expected: id.clone(),
                    harness: (*harness).to_owned(),
                    input: seed_inputs
                        .values()
                        .find(|(seed_harness, _)| seed_harness == harness)
                        .map(|(_, input)| input.clone()),
                }),
                // Judged from the header record and from seed tasks.
                Expected::HeaderKey(_) | Expected::EndRecord { .. } => None,
                // The planted runner's verdict alone isn't evidence (Rule 11):
                // it blocks until each of its runs is kept and judged.
                Expected::Planted(_)
                | Expected::RaceSchedule(_)
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
                        | Expected::Planted(_)
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

/// `lane`'s task, from `qa/suite.toml` at `repo`. Its required targets
/// come from the packages, not from what the command happens to select: a
/// lane naming `--test` targets requires those and its packages' libraries,
/// and one that doesn't requires every top-level `tests/*.rs` of each
/// package and its library. A library a `sim-package` row marks `lib =
/// false` is expected empty, so it isn't required (Rule 11).
fn lane_task(repo: &Path, expected: &str, lane: &str) -> Option<Task> {
    let text = fs::read_to_string(repo.join(crate::suite::SUITE_FILE)).ok()?;
    let suite: crate::suite::SuiteFile = toml::from_str(&text).ok()?;
    let row = suite.lane.iter().find(|row| row.id == lane)?;
    let (subcommand, args) = row.args.split_once(' ').unwrap_or((&row.args, ""));
    let words: Vec<&str> = args.split_whitespace().collect();
    let packages: Vec<&str> = words
        .windows(2)
        .filter(|pair| pair[0] == "-p")
        .map(|pair| pair[1])
        .collect();
    let named = row.test_targets();
    let mut required = Vec::new();
    let mut empty = Vec::new();
    for package in &packages {
        let empty_lib = suite
            .sim_package
            .iter()
            .any(|p| p.name == *package && !p.lib);
        if empty_lib {
            empty.push(format!("{package}/lib"));
        } else {
            required.push(format!("{package}/lib"));
        }
        if named.is_empty() {
            let tests = repo.join("crates").join(package).join("tests");
            let mut stems: Vec<String> = fs::read_dir(&tests)
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    (path.extension()? == "rs")
                        .then(|| path.file_stem()?.to_str().map(str::to_owned))?
                })
                .collect();
            stems.sort();
            required.extend(stems.into_iter().map(|stem| format!("{package}/{stem}")));
        }
    }
    if let [package] = packages[..] {
        required.extend(named.iter().map(|target| format!("{package}/{target}")));
    }
    // `run = true` ignored tests in the lane's targets must run too.
    let ignored_run = suite
        .ignored
        .iter()
        .filter(|row| row.run)
        .filter_map(|row| {
            let path = Path::new(&row.file);
            let package = path.iter().nth(1)?.to_str()?;
            let stem = path.file_stem()?.to_str()?;
            let target = format!("{package}/{stem}");
            required
                .contains(&target)
                .then(|| format!("{target}::{}", row.test))
        })
        .collect();
    Some(Task::Lane {
        expected: expected.to_owned(),
        id: lane.to_owned(),
        command: row.build.command(subcommand, args),
        required,
        empty,
        ignored_run,
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
    /// How the run contained its children: `subreaper` or `best-effort`.
    /// Only a subreaper run is an acceptance record.
    pub containment: String,
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
    /// No row blocked, no manifest problem, the plan S's own, the run
    /// contained under a subreaper, and the header record consumed.
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
    containment: &str,
) -> Sim7Record {
    let mut problems = plan.problems.clone();
    // The plan must be S's own: a supplied plan that relabels a task or
    // drops an unsupported result would otherwise be judged as given.
    if &self::plan(repo) != plan {
        problems.push("the supplied plan isn't the plan the manifest at S makes".to_owned());
    }
    let bound = Bound {
        source: subject_source,
        release: artifacts
            .parent()
            .and_then(|run| fs::read_to_string(run.join("bin/index.json")).ok())
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|index| index.get("release")?.as_str().map(str::to_owned)),
    };
    let mut verdicts: BTreeMap<String, Verdict> = BTreeMap::new();
    let mut applicable = 0u64;
    let mut seed_traces: BTreeMap<String, std::path::PathBuf> = BTreeMap::new();
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        let recorded = fs::read_to_string(dir.join("task.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<Task>(&text).ok());
        let verdict = if recorded.as_ref() == Some(task) {
            judge_task(task, &dir, &bound, &mut applicable)
        } else {
            Verdict::Blocked {
                why: "the artifact directory's task.json isn't this task".to_owned(),
            }
        };
        if let Task::Seed { file, .. } = task {
            seed_traces.insert(file.clone(), dir.join("trace"));
        }
        verdicts.insert(task.expected().to_owned(), verdict);
    }
    for (id, why) in &plan.unsupported {
        verdicts.insert(id.clone(), Verdict::Blocked { why: why.clone() });
    }
    let derived = derive(repo).unwrap_or_default();
    let rows = rows(&derived);
    // End records: the seed's own trace must end exactly as committed. A
    // seed's file is its end record's, `name.end.json`, as `name.bin`.
    for row in &rows {
        for expected in &row.expected {
            if let Expected::EndRecord { file } = expected {
                let seed = file
                    .strip_suffix(".end.json")
                    .map(|stem| format!("{stem}.bin"));
                let verdict = match seed.and_then(|seed| seed_traces.get(&seed).cloned()) {
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
    let accepted = evaluation.accepted
        && problems.is_empty()
        && refusal.is_none()
        && containment == "subreaper";
    let fully_accepted =
        accepted && evaluation.fully_accepted && deviations.iter().all(|(_, signed)| *signed);
    Sim7Record {
        artifacts: artifacts
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
        subject: subject.to_owned(),
        subject_source: subject_source.to_owned(),
        containment: containment.to_owned(),
        header_refusal: refusal,
        problems,
        unsupported: plan.unsupported.clone(),
        evaluation,
        deviations,
        accepted,
        fully_accepted,
    }
}

/// What every judged artifact must be bound to.
struct Bound<'a> {
    /// S's source digest.
    source: &'a str,
    /// The SHA-256 of the release `zakura-sim` the run kept, which every
    /// seed, witness and measured trace must have been recorded by.
    release: Option<String>,
}

/// One task's verdict from its artifacts in `dir`.
fn judge_task(task: &Task, dir: &Path, bound: &Bound<'_>, applicable: &mut u64) -> Verdict {
    let source = bound.source;
    let blocked = |why: String| Verdict::Blocked { why };
    let json = |name: &str| -> Option<Value> {
        serde_json::from_str(&fs::read_to_string(dir.join(name)).ok()?).ok()
    };
    match task {
        Task::Test {
            package,
            file,
            test,
            ..
        } => {
            let Some(result) = json("test.json") else {
                return blocked("the test left no test.json".to_owned());
            };
            let same = result.get("package").and_then(Value::as_str) == Some(package)
                && result.get("file").and_then(Value::as_str) == Some(file);
            if !same {
                return blocked(format!("test.json isn't {package} {file}'s"));
            }
            if result.get("checkout_source").and_then(Value::as_str) != Some(source) {
                return blocked("the test didn't run from this run's checkout of S".to_owned());
            }
            // zakura-sim's binaries embed S's source digest, so theirs must
            // match. The others carry none, and are the run's own build.
            if package == "zakura-sim"
                && result.get("binary_source").and_then(Value::as_str) != Some(source)
            {
                return blocked("the test binary doesn't embed S's source digest".to_owned());
            }
            exactly_one_passed(&result, test)
        }
        Task::FuzzTest { test, .. } => match json("test.json") {
            Some(result) => exactly_one_passed(&result, test),
            None => blocked("the test left no test.json".to_owned()),
        },
        Task::Lane {
            required,
            empty,
            ignored_run,
            ..
        } => match json("lane.json") {
            Some(lane) => lane_passed(&lane, required, empty, ignored_run),
            None => blocked("the lane left no lane.json".to_owned()),
        },
        Task::Seed { harness, input, .. } => {
            if !exited(json("record.json")) {
                return blocked("the seed's recording didn't exit 0".to_owned());
            }
            if !exited(json("replay.json")) {
                return blocked(
                    "the seed's trace didn't replay exactly in a fresh process".to_owned(),
                );
            }
            judge_trace(
                &dir.join("trace"),
                bound,
                (harness, Some(input)),
                &RunExpect::Passes,
                applicable,
            )
        }
        Task::SeedWitness {
            harness,
            input,
            property,
            expect,
            ..
        } => {
            if !exited(json("record.json")) {
                return blocked("the witness recording didn't exit 0".to_owned());
            }
            if !exited(json("replay.json")) {
                return blocked(
                    "the witness trace didn't replay exactly in a fresh process".to_owned(),
                );
            }
            let Some(witness) = json("trace/witness.json") else {
                return blocked("the witness left no witness.json".to_owned());
            };
            let same = witness.get("property").and_then(Value::as_str) == Some(property)
                && witness.get("expect").and_then(Value::as_str) == Some(expect);
            if !same {
                return blocked(format!(
                    "witness.json isn't the witness of {property}, {expect}"
                ));
            }
            if witness.get("passed").and_then(Value::as_bool) != Some(true) {
                return blocked(format!("the witness of {property} didn't pass"));
            }
            let also: Vec<String> = witness
                .get("also_violates")
                .and_then(|also| serde_json::from_value(also.clone()).ok())
                .unwrap_or_default();
            let run_expect = match expect.as_str() {
                "violated" | "violated-set" => RunExpect::Violates {
                    property: property.clone(),
                    also,
                },
                _ => RunExpect::Passes,
            };
            judge_trace(
                &dir.join("trace"),
                bound,
                (harness, Some(input)),
                &run_expect,
                &mut 0,
            )
        }
        Task::Command { args, .. } => match json("command.json") {
            Some(command) => command_passed(&command, args),
            None => blocked("the command left no command.json".to_owned()),
        },
        Task::Measure { harness, input, .. } => {
            let Some(measure) = json("measure.json") else {
                return blocked("the recording left no measure.json".to_owned());
            };
            if !exited(json("record.json")) {
                return blocked("the measured recording didn't exit 0".to_owned());
            }
            let trace = dir.join("trace");
            let ended = TraceReader::open(&trace)
                .ok()
                .and_then(|trace| trace.end.map(|end| end.steps));
            let bytes = fs::metadata(trace.join(crate::trace::TRACE_FILE))
                .ok()
                .map(|meta| meta.len());
            let whole = ["wall_ns", "steps_per_sec", "stack_pool"]
                .iter()
                .all(|field| measure.get(*field).is_some());
            if measure.get("steps").and_then(Value::as_u64) != ended
                || measure.get("trace_bytes").and_then(Value::as_u64) != bytes
                || !whole
            {
                return blocked("measure.json doesn't match its trace".to_owned());
            }
            judge_trace(
                &trace,
                bound,
                (harness, input.as_deref()),
                &RunExpect::Passes,
                &mut 0,
            )
        }
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

/// What a lane's runner records: its command's outcome, every test each
/// target ran with its outcome, every non-ignored test each target's binary
/// listed before the run, and each target whose listing failed.
#[derive(Debug, Default, Deserialize)]
struct LaneRun {
    #[serde(default)]
    exited_ok: bool,
    #[serde(default)]
    targets: BTreeMap<String, Vec<(String, String)>>,
    #[serde(default)]
    listed: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    list_failures: Vec<String>,
}

/// A lane passes when its command exited 0, its binaries listed cleanly
/// before it ran, every required target was built and every test it lists
/// ran and passed, every expected-empty target listed none, every `run =
/// true` ignored test ran and passed, and no test failed (Rule 11).
fn lane_passed(
    lane: &Value,
    required: &[String],
    empty: &[String],
    ignored_run: &[String],
) -> Verdict {
    let blocked = |why: String| Verdict::Blocked { why };
    let run: LaneRun = serde_json::from_value(lane.clone()).unwrap_or_default();
    if !run.exited_ok {
        return blocked("the lane's command failed".to_owned());
    }
    if let Some(target) = run.list_failures.first() {
        return blocked(format!("{target}'s binary didn't list its tests"));
    }
    let passed = |target: &str, name: &str| {
        run.targets.get(target).is_some_and(|tests| {
            tests
                .iter()
                .any(|(n, outcome)| n == name && outcome == "ok")
        })
    };
    for target in required {
        let Some(listed) = run.listed.get(target) else {
            return blocked(format!("required target {target} wasn't built"));
        };
        if listed.is_empty() {
            return blocked(format!("required target {target} lists no test"));
        }
    }
    for (target, listed) in &run.listed {
        if let Some(name) = listed.iter().find(|name| !passed(target, name)) {
            return blocked(format!(
                "{target}::{name} is listed but didn't run and pass"
            ));
        }
    }
    for target in empty {
        if run
            .listed
            .get(target)
            .is_some_and(|listed| !listed.is_empty())
        {
            return blocked(format!("{target} is expected empty but lists tests"));
        }
    }
    for test in ignored_run {
        let (target, name) = test.split_once("::").unwrap_or((test, ""));
        if !passed(target, name) {
            return blocked(format!(
                "{test} is marked `run = true` but didn't run and pass"
            ));
        }
    }
    for (target, tests) in &run.targets {
        if let Some((name, outcome)) = tests.iter().find(|(_, outcome)| outcome == "FAILED") {
            return blocked(format!("{target}: {name} {outcome}"));
        }
    }
    Verdict::Verified
}

/// An S4.6 command passes when it exited 0, and the dependency-graph one
/// also when its output shows the node's `zakura` root and none of the
/// simulator crates the lint step denies.
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
        if !output.lines().any(|line| line.starts_with("zakura v")) {
            return Verdict::Blocked {
                why: "`cargo tree`'s output has no zakura root".to_owned(),
            };
        }
        if let Some(denied) = [
            "zakura-scheduler ",
            "zakura-sim ",
            "zakura-storage ",
            "corosensei ",
        ]
        .iter()
        .find(|name| output.lines().any(|line| line.starts_with(*name)))
        {
            return Verdict::Blocked {
                why: format!("the production graph has {}", denied.trim()),
            };
        }
    }
    Verdict::Verified
}

/// Whether a child's `<name>.json` says it exited 0.
fn exited(child: Option<Value>) -> bool {
    child.and_then(|c| c.get("exited_ok").and_then(Value::as_bool)) == Some(true)
}

/// Judges the trace in `dir` with `owned_run_passes`, taking the
/// registered properties and their statuses from the harness registry at
/// S, not from the trace. It must be a run of `binding`'s harness, on its
/// full input's SHA-256 where one is given, recorded by the release
/// executable the run kept, from a build that embeds S's source. A passing
/// run's applicable counts are added to `applicable`.
fn judge_trace(
    dir: &Path,
    bound: &Bound<'_>,
    binding: (&str, Option<&str>),
    expect: &RunExpect,
    applicable: &mut u64,
) -> Verdict {
    let trace = match TraceReader::open(dir) {
        Ok(trace) => trace,
        Err(error) => {
            return Verdict::Blocked {
                why: format!("the trace doesn't read: {error}"),
            }
        }
    };
    let (harness, input) = binding;
    if trace.meta.sim != harness {
        return Verdict::Blocked {
            why: format!("the trace is a run of {}, not {harness}", trace.meta.sim),
        };
    }
    if let Some(input) = input {
        if trace.meta.input.as_ref().map(|i| i.sha256.as_str()) != Some(input) {
            return Verdict::Blocked {
                why: format!("the trace isn't a run of the input {input}"),
            };
        }
    }
    let source = bound.source;
    if bound.release.as_deref() != Some(trace.meta.build.binary.as_str()) {
        return Verdict::Blocked {
            why: format!(
                "the trace was recorded by {}, not the run's release zakura-sim",
                trace.meta.build.binary
            ),
        };
    }
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
    let Ok(entry) = crate::harness::find(harness) else {
        return Verdict::Blocked {
            why: format!("{harness} isn't a harness at S"),
        };
    };
    let properties = (entry.properties)();
    let registered = Registered {
        ids: properties.iter().map(|p| p.id.clone()).collect(),
        tolerated: properties
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
        })
        .and_then(|results| consumed_cleanup(&results, &artifacts));
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

    /// A lane blocks on a failed command or listing, a required target
    /// that wasn't built or lists nothing, a listed test that didn't run and
    /// pass (a `--skip`), a nonempty expected-empty target, an unrun
    /// `run = true` ignored test, or any failure.
    #[test]
    fn a_lane_needs_its_whole_inventory() {
        let required = ["zakura-sim/draws".to_owned()];
        let empty = ["zakura-rt/lib".to_owned()];
        let none: [String; 0] = [];
        let lane = |extra: serde_json::Value| {
            let mut lane = json!({
                "exited_ok": true,
                "listed": { "zakura-sim/draws": ["a"], "zakura-rt/lib": [] },
                "targets": { "zakura-sim/draws": [["a", "ok"], ["slow", "ignored"]] },
                "list_failures": [],
            });
            for (key, value) in extra.as_object().unwrap() {
                lane[key] = value.clone();
            }
            lane
        };
        let judge = |lane: &serde_json::Value, ignored_run: &[String]| {
            lane_passed(lane, &required, &empty, ignored_run)
        };
        assert_eq!(judge(&lane(json!({})), &none), Verdict::Verified);
        assert!(blocked(&judge(&lane(json!({ "exited_ok": false })), &none)));
        assert!(blocked(&judge(
            &lane(json!({ "list_failures": ["zakura-sim/draws"] })),
            &none
        )));
        assert!(blocked(&judge(
            &lane(json!({ "listed": { "zakura-rt/lib": [] } })),
            &none
        )));
        let skipped =
            lane(json!({ "listed": { "zakura-sim/draws": ["a", "b"], "zakura-rt/lib": [] } }));
        assert!(
            blocked(&judge(&skipped, &none)),
            "a --skip of one listed test blocks"
        );
        let nonempty =
            lane(json!({ "listed": { "zakura-sim/draws": ["a"], "zakura-rt/lib": ["x"] } }));
        assert!(blocked(&judge(&nonempty, &none)));
        assert!(blocked(&judge(
            &lane(json!({})),
            &["zakura-sim/draws::slow".to_owned()]
        )));
        let failed =
            lane(json!({ "targets": { "zakura-sim/draws": [["a", "ok"], ["c", "FAILED"]] } }));
        assert!(blocked(&judge(&failed, &none)));
    }

    /// `cargo tree` passes only on exit 0, with the zakura root, and no
    /// simulator crate in the graph.
    #[test]
    fn the_dependency_graph_row_reads_cargo_tree_itself() {
        let tree = ["tree".to_owned()];
        let clean =
            json!({ "exited_ok": true, "log": "zakura v5.0.0 (/repo)\nzakura-chain v5.0.0\n" });
        assert_eq!(command_passed(&clean, &tree), Verdict::Verified);
        let failed = json!({ "exited_ok": false, "log": "" });
        assert!(blocked(&command_passed(&failed, &tree)));
        let empty = json!({ "exited_ok": true, "log": "" });
        assert!(
            blocked(&command_passed(&empty, &tree)),
            "no root means it checked nothing"
        );
        let sim = json!({ "exited_ok": true, "log": "zakura v5.0.0\nzakura-sim v0.0.0\n" });
        assert!(blocked(&command_passed(&sim, &tree)));
    }
}
