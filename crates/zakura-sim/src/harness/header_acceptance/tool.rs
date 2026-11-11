//! The acceptance tool's simulator side (hdr-14.7a, item 9).
//!
//! `cargo xtask header-acceptance` can't link this crate, which builds only
//! under `--cfg zakura_sim`, so it drives the `header-acceptance` binary in
//! three steps around the children it runs itself:
//!
//! 1. [`plan`] turns the manifest into [`Task`]s: the tests to execute by
//!    name, the planted cases to rebuild and record, and the integrity
//!    mutations to run. Every other expected result is listed with why the
//!    tool can't produce it yet, and stays blocked.
//! 2. xtask runs each task with a deadline and writes its artifacts under
//!    `<artifacts>/<task index>/`, as [`TEST_FILE`], recorded traces and
//!    [`PROVENANCE_FILE`].
//! 3. [`assemble`] reads those artifacts into the evaluator's
//!    [`ResultFile`], and [`record`] runs the evaluator and writes the
//!    record's three exports.
//!
//! The adapters here, from a trace's end record to an [`Execution`], stand
//! in for the full-report sidecars C8 adds. They read only what the trace
//! keeps.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    contracts::RunBy,
    evaluate::{
        evaluate, Actions, Actual, End, Evaluation, ExecMeta, Execution, ObligationState, Report,
        ResultFile, ResultRecord, Subject, TestOutcome, TestRan,
    },
    required::{required_keys, Inventories, GATE_OBLIGATIONS},
    rows::HEADER_HARNESSES,
    CaseRef, CaseRun, Expected, ExpectedId, Gate, Key, Manifest, RunId, COMPOSITION_CHECKS,
    NARROWING, REQUIRED_CONTRACTS,
};
use crate::harness::coverage::{Bypass, COVERAGE};

/// A test task's artifact: how the exact-name filter ran.
pub const TEST_FILE: &str = "test.json";
/// A planted case's artifact: whether the patched binary embeds its own
/// checkout's source digest (C5).
pub const PROVENANCE_FILE: &str = "provenance.json";
/// A child that hit its deadline leaves this file in its directory.
pub const KILLED_FILE: &str = "killed";
/// The artifacts directory's own artifact: the run that wrote it, and the
/// subject it ran at ([`RunBinding`]).
pub const RUN_FILE: &str = "run.json";
/// Each task directory's own artifact: the task the runner ran there, as
/// [`task_binding`] gives it.
pub const TASK_FILE: &str = "task.json";

/// What a task directory's [`TASK_FILE`] must hold: its index in the plan
/// and the plan's task itself, so a directory is read only as the task the
/// runner wrote it for, never by its position alone.
pub fn task_binding(index: usize, task: &Task) -> serde_json::Value {
    serde_json::json!({ "index": index, "task": task })
}

/// What an artifacts directory says of the run that wrote it, at its root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBinding {
    /// The run's own ID, which it chose fresh when it started and created
    /// the directory, so another attempt's directory never matches.
    pub run_id: String,
    /// The subject commit S the run ran at.
    pub subject: String,
    /// S's source digest.
    pub subject_source: String,
    /// How the run contained its children's process trees.
    pub containment: Containment,
}

/// How a run kept each child's descendants from outliving it (Adam's
/// containment rule, 2026-11-09). Only a [`Containment::Subreaper`] run is
/// an acceptance record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Containment {
    /// On Linux, the runner is every orphaned descendant's subreaper, so it
    /// can kill and reap each one before the next task starts.
    Subreaper,
    /// Elsewhere the runner samples the process table, which a descendant
    /// can slip between.
    BestEffort,
}

impl std::str::FromStr for Containment {
    type Err = String;

    /// Parses the runner's `--containment` value, refusing any other.
    fn from_str(value: &str) -> Result<Self, String> {
        match value {
            "subreaper" => Ok(Self::Subreaper),
            "best-effort" => Ok(Self::BestEffort),
            other => Err(format!("unknown containment {other:?}")),
        }
    }
}
/// A planted case's artifact: its fuzz-target confirmation, from the
/// planted runner's `--targets` run.
pub const TARGET_FILE: &str = "target.json";
/// A planted case's artifact: the planted runner's own verdict on it, which
/// covers its campaign as well as its two traces.
pub const VERDICT_FILE: &str = "verdict.json";

/// One thing the acceptance tool runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Task {
    /// A test, by its exact name.
    Test {
        /// The expected result it produces.
        expected: ExpectedId,
        /// Its package.
        package: String,
        /// Its file, relative to the package.
        file: String,
        /// Its name.
        test: String,
    },
    /// A planted case, rebuilt with its patch in its own target directory.
    Case {
        /// The expected result it's for. A node case's `Draft` run needs C9
        /// (hdr-14.7b), so its result can't be whole yet: the plan lists it
        /// unsupported, and its runs are only linked as evidence.
        expected: ExpectedId,
        /// The case's property directory.
        property: String,
        /// The case's name.
        name: String,
        /// The harness its input runs on.
        harness: String,
        /// Whether that harness is a machine.
        machine: bool,
        /// What the case runs: its committed input or its typed scenario.
        input: CaseInput,
        /// The patch, relative to the repository root.
        patch: String,
    },
    /// A native integrity mutation: its history's test unpatched, patched
    /// and restored.
    Integrity {
        /// The expected result it produces.
        expected: ExpectedId,
        /// The mutation's directory name.
        patch: String,
        /// The patch, relative to the repository root.
        patch_file: String,
        /// The history's harness.
        harness: String,
        /// The history's case.
        case: String,
        /// The history's test's package.
        package: String,
        /// Its file.
        file: String,
        /// Its name.
        test: String,
        /// The diagnostic the patched run must fail with.
        diagnostic: String,
    },
}

/// What [`plan`] gives: the tasks, and every expected result the tool can't
/// produce yet, with why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPlan {
    /// The tasks, in order. A task's artifacts go under its index.
    pub tasks: Vec<Task>,
    /// Expected results with no task, and why.
    pub unsupported: Vec<(ExpectedId, String)>,
}

/// What a planted case runs, as its `.toml` names it and the planted runner
/// reads it. Paths are relative to the repository root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum CaseInput {
    /// A committed fuzz input.
    Fuzz {
        /// The input.
        input: String,
    },
    /// A typed scenario and its Plan bytes, for a scenario the harness's
    /// grammar can't encode.
    Typed {
        /// The scenario, as the harness's JSON.
        scenario: String,
        /// The Plan bytes.
        plan: String,
    },
}

/// What a planted case's `.toml` runs: its `input`, or its `[typed]`
/// table's scenario and Plan. A case names exactly one.
fn case_input(repo: &Path, case: CaseRef) -> Result<CaseInput, String> {
    let file = repo
        .join("qa/planted")
        .join(case.property)
        .join(format!("{}.toml", case.name));
    let text = fs::read_to_string(&file).map_err(|error| format!("{}: {error}", file.display()))?;
    let table: toml::Table =
        toml::from_str(&text).map_err(|error| format!("{}: {error}", file.display()))?;
    let path = |table: &toml::Table, key: &str| {
        table
            .get(key)
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("{}: no {key}", file.display()))
    };
    match (table.get("input"), table.get("typed")) {
        (Some(_), None) => Ok(CaseInput::Fuzz {
            input: path(&table, "input")?,
        }),
        (None, Some(typed)) => {
            let typed = typed
                .as_table()
                .ok_or_else(|| format!("{}: `typed` isn't a table", file.display()))?;
            Ok(CaseInput::Typed {
                scenario: path(typed, "scenario")?,
                plan: path(typed, "plan")?,
            })
        }
        _ => Err(format!(
            "{}: a case names either `input` or `[typed]`",
            file.display()
        )),
    }
}

/// Turns `manifest`'s expected results into tasks. `Holds`,
/// `StaysNotApplicable` and `Replays` need a history recorded from its
/// bound scenario and input (C18) with its full report (C8), so they wait
/// for hdr-14.7b. A node `Caught`'s baseline and patched runs are tasks, but
/// its `Draft` run needs C9's override, so its expected result has none.
pub fn plan(manifest: &Manifest, repo: &Path) -> Result<RunPlan, String> {
    let mut tasks = Vec::new();
    let mut unsupported = Vec::new();
    let mut seen = BTreeSet::new();
    for result in &manifest.expected {
        let Some(id) = result.expected.id() else {
            continue;
        };
        if !seen.insert(id.clone()) {
            continue;
        }
        match result.expected {
            Expected::Passes { test } => tasks.push(Task::Test {
                expected: id,
                package: test.package.to_owned(),
                file: test.file.to_owned(),
                test: test.test.to_owned(),
            }),
            Expected::Caught { case, harness, .. } => {
                tasks.push(case_task(repo, case, harness, false, id.clone())?);
                unsupported.push((
                    id,
                    "its Draft run needs C9's override in record and replay (E5, hdr-14.7b)"
                        .to_owned(),
                ));
            }
            Expected::MachineCaught { case, harness } => {
                let task = case_task(repo, case, harness, true, id.clone())?;
                // Binding a typed case's trace to its scenario and Plan is
                // C18's (E10), so its runs are linked but give no result.
                if matches!(
                    task,
                    Task::Case {
                        input: CaseInput::Typed { .. },
                        ..
                    }
                ) {
                    unsupported.push((
                        id,
                        "binding a typed case's trace to its scenario and Plan needs C18 \
                         (E10, hdr-14.7b)"
                            .to_owned(),
                    ));
                }
                tasks.push(task);
            }
            Expected::IntegrityCaught {
                patch,
                history,
                diagnostic,
                ..
            } => tasks.push(Task::Integrity {
                expected: id,
                patch: patch.to_owned(),
                patch_file: native_patch(repo, patch)?,
                harness: history.harness.to_owned(),
                case: history.case.to_owned(),
                package: history.witness.package.to_owned(),
                file: history.witness.file.to_owned(),
                test: history.witness.test.to_owned(),
                diagnostic: diagnostic.to_owned(),
            }),
            Expected::Holds { .. } | Expected::StaysNotApplicable { .. } => unsupported.push((
                id,
                "recording a history from its bound scenario and input needs C18 and its full \
                 report needs C8 (hdr-14.7b)"
                    .to_owned(),
            )),
            Expected::Replays { .. } => unsupported.push((
                id,
                "a replay compares full reports and store snapshots, which need C8 (hdr-14.7b)"
                    .to_owned(),
            )),
            Expected::Open { .. } => {}
        }
    }
    Ok(RunPlan { tasks, unsupported })
}

/// A planted case's task.
fn case_task(
    repo: &Path,
    case: CaseRef,
    harness: &str,
    machine: bool,
    expected: ExpectedId,
) -> Result<Task, String> {
    Ok(Task::Case {
        expected,
        property: case.property.to_owned(),
        name: case.name.to_owned(),
        harness: harness.to_owned(),
        machine,
        input: case_input(repo, case)?,
        patch: format!("qa/planted/{}/{}.patch", case.property, case.name),
    })
}

/// The one patch file in native mutation `patch`'s directory, relative to
/// the repository root.
fn native_patch(repo: &Path, patch: &str) -> Result<String, String> {
    let dir = format!("qa/native-mutations/header-sync/{patch}");
    let patches: Vec<String> = fs::read_dir(repo.join(&dir))
        .map_err(|error| format!("{dir}: {error}"))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with(".patch"))
        .collect();
    match patches.as_slice() {
        [one] => Ok(format!("{dir}/{one}")),
        _ => Err(format!("{dir} holds {} patches, not one", patches.len())),
    }
}

/// A test task's artifact, as xtask writes it from libtest's output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestRun {
    /// The package xtask ran the filter in.
    pub package: String,
    /// The test file whose binary xtask ran.
    pub file: String,
    /// Each test libtest reported, as its name and `ok`, `FAILED` or
    /// `ignored`.
    pub tests: Vec<(String, String)>,
    /// Whether the child ended at its deadline.
    pub killed: bool,
    /// Whether the child exited 0. A test it reported `ok` passed only if
    /// it did.
    pub exited_ok: bool,
    /// The child's combined output, for an integrity diagnostic.
    pub log: String,
    /// The source digest of the checkout the test ran in, if the test
    /// binary embeds it, or the marker [`NOT_EMBEDDED`]. `None` if the tool
    /// couldn't find the binary.
    pub binary_source: Option<String>,
    /// The source digest of the checkout the test ran in: S's for an
    /// ordinary test, the patched checkout's for an integrity mutation's
    /// patched run. `None` if it couldn't be read.
    pub checkout_source: Option<String>,
}

/// What [`TestRun::binary_source`] holds when the binary doesn't embed the
/// subject's source digest.
pub const NOT_EMBEDDED: &str = "not-embedded";

/// A planted case's provenance artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceCheck {
    /// The patched checkout's source digest.
    pub source: String,
    /// The SHA-256 of the patch the rebuild applied, which must be the
    /// case's committed patch at S.
    pub patch_sha256: String,
    /// Why the binary isn't the checkout's own, or `None` if it is.
    pub refused: Option<String>,
}

/// A planted case's verdict artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictCheck {
    /// The case's property directory.
    pub property: String,
    /// The case's name.
    pub name: String,
    /// Why the planted runner didn't report the case caught, or `None` if
    /// it did.
    pub refused: Option<String>,
}

/// A planted case's target-confirmation artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetCheck {
    /// The case's property directory.
    pub property: String,
    /// The case's name.
    pub name: String,
    /// The fuzz target the case runs on, or `None` if none runs its
    /// harness.
    pub target: Option<String>,
    /// Why the case wasn't confirmed through its target, or `None` if it
    /// was or has none.
    pub refused: Option<String>,
}

/// A test result from its artifact: every test libtest reported, in the
/// package and file xtask ran. A killed child fails every test it ran, a
/// child that didn't exit 0 fails every test it reported `ok`, and an
/// unreadable artifact is a result that ran nothing.
fn test_actual(dir: &Path) -> Actual {
    let Some(run) = read_json::<TestRun>(&dir.join(TEST_FILE)) else {
        return Actual::Test {
            ran: Vec::new(),
            binary_source: None,
        };
    };
    let ran = run
        .tests
        .iter()
        .map(|(test, result)| TestRan {
            package: run.package.clone(),
            file: run.file.clone(),
            test: test.clone(),
            outcome: match result.as_str() {
                _ if run.killed => TestOutcome::Failed,
                "ok" if run.exited_ok => TestOutcome::Passed,
                "ignored" => TestOutcome::Skipped,
                _ => TestOutcome::Failed,
            },
        })
        .collect();
    Actual::Test {
        ran,
        binary_source: run.binary_source,
    }
}

/// The SHA-256 of `path`'s bytes in hex, or `missing` if it can't be read,
/// which names the one execution an artifact records.
fn digest(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    match fs::read(path) {
        Ok(bytes) => Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        Err(_) => "missing".to_owned(),
    }
}

/// `path` as `T`, or `None` if it's missing or malformed.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

/// A run's declared actions, by its harness and how it ended. A machine's
/// driver runs its whole input and stops only at its first model mismatch
/// (Rule 17's machine histories end as their driver does), so a completed
/// machine run carried out its input, a stopped one a prefix of it, and any
/// other end none it can show. A node
/// history's actions must each be shown to reach its documented effect,
/// not just picked, and that evidence comes with the history runs in
/// hdr-14.7b, so a node run is `Incomplete` until then.
fn actions(harness: &str, end: &End) -> Actions {
    let machine = crate::harness::find(harness).is_ok_and(|entry| entry.machine);
    match (machine, end) {
        (true, End::Completed) => Actions::Complete,
        (true, End::Violation { .. }) => Actions::Prefix,
        _ => Actions::Incomplete,
    }
}

/// The source digest a trace's binary recorded in its meta (`build.source`),
/// or `None` if the trace has none or it's `unknown`.
fn trace_source(dir: &Path) -> Option<String> {
    let text = fs::read_to_string(dir.join("trace.jsonl")).ok()?;
    let first: Value = serde_json::from_str(text.lines().next()?).ok()?;
    let source = first.get("meta")?.get("build")?.get("source")?.as_str()?;
    (!source.is_empty() && source != "unknown").then(|| source.to_owned())
}

/// The outcome name of a report in a trace's end record: a string, or an
/// object keyed by its name.
fn outcome_name(outcome: &Value) -> Option<&str> {
    outcome
        .as_str()
        .or_else(|| outcome.as_object()?.keys().next().map(String::as_str))
}

/// One property report from a trace's end record.
fn report(outcome: &Value) -> Option<Report> {
    Some(match outcome_name(outcome)? {
        "held" => Report::Held {
            applicable: outcome.get("held")?.get("applicable")?.as_u64()?,
        },
        // The trace's typed reason, so an unknown one is a malformed record.
        "not-applicable" => Report::NotApplicable {
            reason: serde_json::from_value::<crate::NaReason>(
                outcome.get("not-applicable")?.get("reason")?.clone(),
            )
            .ok()?
            .name()
            .into_owned(),
        },
        "violated" => Report::Violated,
        _ => return None,
    })
}

/// The execution `run` recorded in `dir`: its trace's end record and
/// picks, and its witness result. A missing trace, a killed child or a
/// malformed end record is a run that didn't happen. A completed end comes
/// from the trace's `idle`, `drain` or `budget` outcome until C1's
/// completion replaces them, and every result still depends on C1.
fn execution(dir: &Path, run: RunId, harness: &str, run_by: RunBy) -> Execution {
    let trace = digest(&dir.join("trace.jsonl"));
    let failed = |why: String| Execution {
        trace: trace.clone(),
        run: run.clone(),
        meta: ExecMeta {
            harness: harness.to_owned(),
            draft_override: Vec::new(),
            run_by,
            input: None,
        },
        end: End::Failed { why },
        reports: BTreeMap::new(),
        trigger: false,
        leaked: 0,
        actions: Actions::Incomplete,
    };
    if dir.join(KILLED_FILE).exists() {
        let mut killed = failed(String::new());
        killed.end = End::Killed;
        return killed;
    }
    // The simulator's own reader checks the whole trace: its versions,
    // every line's shape and order, its step numbers and that its last
    // line is a written `end`. A trace it refuses is no execution.
    if let Err(error) = crate::trace::TraceReader::open(dir) {
        return failed(format!("{}: {error}", dir.display()));
    }
    let Ok(text) = fs::read_to_string(dir.join("trace.jsonl")) else {
        return failed(format!("{}: no trace", dir.display()));
    };
    let lines: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let (Some(meta), Some(end)) = (
        lines.first().and_then(|line| line.get("meta")),
        lines.last().and_then(|line| line.get("end")),
    ) else {
        return failed(format!("{}: no meta or end record", dir.display()));
    };
    let recorded = meta.get("sim").and_then(Value::as_str).unwrap_or_default();
    let mut reports = BTreeMap::new();
    for property in end
        .get("properties")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(id), Some(report)) = (
            property.get("property").and_then(Value::as_str),
            property.get("outcome").and_then(report),
        ) else {
            return failed(format!("{}: an unreadable property report", dir.display()));
        };
        reports.insert(id.to_owned(), report);
    }
    let end_kind = match end.get("outcome").and_then(Value::as_str) {
        Some("violation") => End::Violation {
            property: end
                .get("violation")
                .and_then(|v| v.get("property"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        },
        Some("harness") => End::HarnessFailure {
            diagnostic: end.get("harness").map(Value::to_string).unwrap_or_default(),
        },
        Some("idle" | "drain" | "budget") => End::Completed,
        other => End::Failed {
            why: format!("the trace ended {other:?}"),
        },
    };
    let actions = actions(recorded, &end_kind);
    let witness: Option<Value> = read_json(&dir.join("witness.json"));
    Execution {
        trace,
        run,
        meta: ExecMeta {
            harness: recorded.to_owned(),
            draft_override: Vec::new(),
            run_by,
            input: None,
        },
        end: end_kind,
        reports,
        trigger: witness
            .as_ref()
            .and_then(|w| w.get("passed"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        leaked: end
            .get("cleanup")
            .and_then(|c| c.get("leaked"))
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX),
        actions,
    }
}

/// Whether `source` is a digest at all: not missing, empty, `unknown` or
/// [`NOT_EMBEDDED`], none of which names a source, so two of them never
/// match.
fn known(source: Option<&str>) -> bool {
    source.is_some_and(|source| !source.is_empty() && source != "unknown" && source != NOT_EMBEDDED)
}

/// Whether `log` holds the harness's own report of `diagnostic`. A harness
/// failure ends `run_prepared` with [`HarnessError::Run`], and the witness
/// panics with its `Display` form, `the run hit a harness failure:
/// <source>'s evidence failed: <diagnostic>: <detail>`
/// ([`HarnessFailure::SourceEvidence`](crate::end::HarnessFailure::SourceEvidence)),
/// whose detail leads with the failure's name. A panic that only mentions
/// the name isn't one. C8's structured end record replaces this in
/// hdr-14.7b.
///
/// [`HarnessError::Run`]: crate::harness::HarnessError::Run
fn reports_diagnostic(log: &str, diagnostic: &str) -> bool {
    let detail = format!("'s evidence failed: {diagnostic}: ");
    log.lines().any(|line| {
        line.split_once("the run hit a harness failure: ")
            .is_some_and(|(_, rest)| rest.find(&detail).is_some_and(|at| at > 0))
    })
}

/// An integrity mutation's history run from its test artifact: a passing
/// test is a completed run, a failing one whose output reports
/// `diagnostic` ([`reports_diagnostic`]) is that harness failure, and
/// anything else didn't happen. The artifact must be the history's own
/// test. A test reports
/// no property outcomes, so a clean run's whole-report check fails until
/// the tool records the history itself with C8's report (hdr-14.7b).
fn integrity_execution(
    dir: &Path,
    run: RunId,
    input: &RunId,
    harness: &str,
    diagnostic: &str,
    subject_source: &str,
) -> Execution {
    let patched = matches!(
        run,
        RunId::Case {
            run: CaseRun::Patched,
            ..
        }
    );
    let RunId::History {
        package,
        file,
        test: name,
        ..
    } = input
    else {
        unreachable!("an integrity run's input is a history");
    };
    let end = match read_json::<TestRun>(&dir.join(TEST_FILE)) {
        Some(test) if test.killed => End::Killed,
        // The artifact must be the history's own witness test, by package,
        // file and name, or it's not that history's run.
        Some(test) if test.package != *package || test.file != *file => End::Failed {
            why: format!(
                "it ran {}/{}, not {package}/{file}",
                test.package, test.file
            ),
        },
        // Its binary must embed the checkout it ran in, which is S for the
        // unpatched and restored runs and not S for the patched one.
        Some(test)
            if !known(test.binary_source.as_deref())
                || test.binary_source != test.checkout_source
                || patched == (test.checkout_source.as_deref() == Some(subject_source)) =>
        {
            End::Failed {
                why: format!(
                    "its binary embeds {:?} and its checkout is {:?}",
                    test.binary_source, test.checkout_source
                ),
            }
        }
        Some(test) => match test.tests.as_slice() {
            [(ran, _)] if ran != name => End::Failed {
                why: format!("it ran {ran}, not {name}"),
            },
            [(_, result)] if result == "ok" && test.exited_ok => End::Completed,
            [(_, result)] if result == "FAILED" && reports_diagnostic(&test.log, diagnostic) => {
                End::HarnessFailure {
                    diagnostic: diagnostic.to_owned(),
                }
            }
            tests => End::Failed {
                why: format!("the filter ran {tests:?}"),
            },
        },
        None => End::Failed {
            why: format!("{}: no test artifact", dir.display()),
        },
    };
    Execution {
        trace: digest(&dir.join(TEST_FILE)),
        run,
        meta: ExecMeta {
            harness: harness.to_owned(),
            draft_override: Vec::new(),
            run_by: RunBy::Tool,
            // The witness test runs every input of its loop, so which of
            // its histories `input` is isn't shown yet: that's C18's
            // witness-test history reconciliation (hdr-14.7b). Until then
            // the run claims no history, and the result blocks.
            input: None,
        },
        end,
        reports: BTreeMap::new(),
        trigger: false,
        leaked: 0,
        actions: Actions::Complete,
    }
}

/// Why `artifacts` and `run_plan` aren't the run `run` names, if they
/// aren't. Artifacts from another run, or another subject, are no run of S,
/// and a plan other than the one S's manifest gives can name another input
/// or patch for a case, so either gives no result and everything blocks.
fn binding_refusal(
    repo: &Path,
    run_plan: &RunPlan,
    artifacts: &Path,
    run: &RunBinding,
) -> Option<String> {
    let binding = read_json::<RunBinding>(&artifacts.join(RUN_FILE));
    if binding.as_ref() != Some(run) {
        return Some(format!(
            "{} doesn't name this run ({}) at its subject",
            RUN_FILE, run.run_id
        ));
    }
    if self::plan(&Manifest::committed(), repo).ok().as_ref() != Some(run_plan) {
        return Some("the plan isn't the one S's manifest gives".into());
    }
    None
}

/// The result file for `run_plan`'s tasks from their artifacts under
/// `artifacts`, for the run `run` names at its subject, with the committed
/// inputs and fuzz-target mapping read from `repo`. The directory's
/// [`RUN_FILE`] must be `run`'s and the plan the one S's manifest gives. A node case's runs are read, and its provenance applied,
/// but give no result until its `Draft` run exists.
pub fn assemble(repo: &Path, run_plan: &RunPlan, artifacts: &Path, run: &RunBinding) -> ResultFile {
    let (subject, subject_source) = (run.subject.as_str(), run.subject_source.as_str());
    let plan = run_plan;
    let targets = fuzz_targets(repo);
    let mut results = Vec::new();
    if binding_refusal(repo, run_plan, artifacts, run).is_some() {
        return ResultFile {
            subject: subject.to_owned(),
            subject_source: subject_source.to_owned(),
            results,
        };
    }
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        // A directory the runner didn't write for this task gives it no
        // result, so it blocks.
        if read_json::<serde_json::Value>(&dir.join(TASK_FILE)) != Some(task_binding(index, task)) {
            continue;
        }
        match task {
            Task::Test { expected, .. } => results.push(ResultRecord {
                expected: expected.clone(),
                commit: subject.to_owned(),
                actual: test_actual(&dir),
            }),
            Task::Case {
                expected,
                property,
                name,
                harness,
                input: CaseInput::Fuzz { input },
                machine: true,
                patch,
                ..
            } => {
                let case = |run| RunId::Case {
                    property: property.clone(),
                    name: name.clone(),
                    run,
                };
                // The planted runner runs a case's baseline and patched
                // runs (C4), and xtask copies its traces here.
                let mut baseline = execution(
                    &dir.join("baseline"),
                    case(CaseRun::Baseline),
                    harness,
                    RunBy::Planted,
                );
                let mut patched = execution(
                    &dir.join("patched"),
                    case(CaseRun::Patched),
                    harness,
                    RunBy::Planted,
                );
                credit_sources(&dir, subject_source, &mut baseline, &mut patched);
                if let Some(why) = patch_refusal(repo, patch, &dir) {
                    fail(&mut patched, why);
                }
                // Each trace must have run the case's committed input, and
                // each witness file must judge the case's property with its
                // run's expectation.
                for (run, execution, expect) in [
                    ("baseline", &mut baseline, "holds"),
                    ("patched", &mut patched, "violated"),
                ] {
                    if let Some(why) = input_refusal(repo, input, &dir.join(run)) {
                        fail(execution, why);
                    }
                    if !witness_judges(&dir.join(run), property, expect) {
                        execution.trigger = false;
                        fail(
                            execution,
                            format!("its witness file doesn't judge {property} expecting {expect}"),
                        );
                    }
                }
                // The runner's own verdict covers what the traces don't, its
                // campaign, and item 9 runs `--targets` for a case with a
                // fuzz target, so its catch needs both, through the target
                // the committed mapping gives its harness.
                let target = target_of(&targets, harness);
                if let Some(why) = case_refusal(&dir, property, name, target.as_deref()) {
                    fail(&mut patched, why);
                }
                results.push(ResultRecord {
                    expected: expected.clone(),
                    commit: subject.to_owned(),
                    actual: Actual::MachineCaught { baseline, patched },
                });
            }
            // A node case's result needs its Draft run, and a typed
            // machine case's needs C18's binding, so neither gives one yet.
            Task::Case { .. } => {}
            Task::Integrity {
                expected,
                patch,
                harness,
                case,
                package,
                file,
                test,
                diagnostic,
                ..
            } => {
                let run = |which| RunId::native(patch, which);
                // Each run executes the history's own witness test, so each
                // names that history as its input.
                let input = RunId::History {
                    harness: harness.clone(),
                    package: package.clone(),
                    file: file.clone(),
                    test: test.clone(),
                    case: case.clone(),
                };
                results.push(ResultRecord {
                    expected: expected.clone(),
                    commit: subject.to_owned(),
                    actual: Actual::IntegrityCaught {
                        unpatched: integrity_execution(
                            &dir.join("unpatched"),
                            run(CaseRun::Unpatched),
                            &input,
                            harness,
                            diagnostic,
                            subject_source,
                        ),
                        patched: integrity_execution(
                            &dir.join("patched"),
                            run(CaseRun::Patched),
                            &input,
                            harness,
                            diagnostic,
                            subject_source,
                        ),
                        restored: integrity_execution(
                            &dir.join("restored"),
                            run(CaseRun::Restored),
                            &input,
                            harness,
                            diagnostic,
                            subject_source,
                        ),
                    },
                });
            }
        }
    }
    ResultFile {
        subject: subject.to_owned(),
        subject_source: subject_source.to_owned(),
        results,
    }
}

/// Fails each of a case's runs whose trace doesn't come from the binary
/// it's credited to: the baseline from S itself (`subject_source`), the
/// patched run from the rebuilt patched checkout its provenance check
/// measured, which must also have passed. A trace with no known source
/// fails. A killed run stays killed.
fn credit_sources(
    dir: &Path,
    subject_source: &str,
    baseline: &mut Execution,
    patched: &mut Execution,
) {
    if trace_source(&dir.join("baseline")).as_deref() != Some(subject_source) {
        fail(
            baseline,
            "its trace's build source isn't the subject's".to_owned(),
        );
    }
    let measured = read_json::<ProvenanceCheck>(&dir.join(PROVENANCE_FILE));
    if let Some(why) = provenance_refusal(dir) {
        fail(patched, why);
    } else if trace_source(&dir.join("patched")).is_none()
        || measured.map(|check| check.source) != trace_source(&dir.join("patched"))
    {
        fail(
            patched,
            "its trace's build source isn't the measured patched checkout's".to_owned(),
        );
    }
}

/// Why the planted runner didn't credit case `name` of `property`: it
/// didn't report it caught, or it wasn't confirmed through `target`, the
/// fuzz target its harness maps to, if any, or either check is missing,
/// names another case or names another target.
fn case_refusal(dir: &Path, property: &str, name: &str, target: Option<&str>) -> Option<String> {
    let other =
        |p: &str, n: &str| (p != property || n != name).then(|| format!("it judges {p}/{n}"));
    let verdict = match read_json::<VerdictCheck>(&dir.join(VERDICT_FILE)) {
        Some(check) => other(&check.property, &check.name).or(check.refused),
        None => Some("no planted verdict".to_owned()),
    };
    verdict.or_else(|| match read_json::<TargetCheck>(&dir.join(TARGET_FILE)) {
        Some(check) => other(&check.property, &check.name)
            .or_else(|| {
                (check.target.as_deref() != target).then(|| {
                    format!(
                        "its target confirmation names {:?}, and its harness maps to {target:?}",
                        check.target
                    )
                })
            })
            .or(check.refused),
        None => Some("no target confirmation".to_owned()),
    })
}

/// Each fuzz target and the harness it runs, from the fuzz package's
/// `[package.metadata.zakura-sim.targets]` table under `repo`, as the
/// planted runner reads it. Empty if it can't be read, so a case with a
/// target then can't be confirmed.
fn fuzz_targets(repo: &Path) -> BTreeMap<String, String> {
    let Ok(text) = fs::read_to_string(repo.join("qa/fuzz/sim/Cargo.toml")) else {
        return BTreeMap::new();
    };
    let Ok(manifest) = toml::from_str::<toml::Table>(&text) else {
        return BTreeMap::new();
    };
    manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("zakura-sim"))
        .and_then(|z| z.get("targets"))
        .and_then(toml::Value::as_table)
        .map(|targets| {
            targets
                .iter()
                .filter_map(|(target, harness)| {
                    Some((target.clone(), harness.as_str()?.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The fuzz target that runs `harness` in `targets`, if one does.
fn target_of(targets: &BTreeMap<String, String>, harness: &str) -> Option<String> {
    targets
        .iter()
        .find(|(_, runs)| runs.as_str() == harness)
        .map(|(target, _)| target.clone())
}

/// Why case `dir`'s provenance check isn't for its committed patch at
/// `patch` (a path under `repo`): its recorded SHA-256 is another patch's,
/// or either can't be read.
fn patch_refusal(repo: &Path, patch: &str, dir: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    let Ok(bytes) = fs::read(repo.join(patch)) else {
        return Some(format!("the committed patch {patch} can't be read"));
    };
    let committed: String = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    match read_json::<ProvenanceCheck>(&dir.join(PROVENANCE_FILE)) {
        Some(check) if check.patch_sha256 == committed => None,
        Some(check) => Some(format!(
            "its rebuild applied a patch with SHA-256 {}, and {patch} is {committed}",
            check.patch_sha256
        )),
        None => Some("no provenance check".to_owned()),
    }
}

/// Why the trace in `dir` didn't run the committed input at `input` (a
/// path under `repo`): its meta's `input` names another SHA-256 or length,
/// or either can't be read.
fn input_refusal(repo: &Path, input: &str, dir: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    let Ok(bytes) = fs::read(repo.join(input)) else {
        return Some(format!("the committed input {input} can't be read"));
    };
    let committed: String = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let recorded = fs::read_to_string(dir.join("trace.jsonl"))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(text.lines().next()?).ok())
        .and_then(|first| first.get("meta")?.get("input").cloned());
    let matches = recorded.as_ref().is_some_and(|input| {
        input.get("sha256").and_then(Value::as_str) == Some(committed.as_str())
            && input.get("len").and_then(Value::as_u64) == u64::try_from(bytes.len()).ok()
    });
    (!matches).then(|| format!("its trace ran {recorded:?}, not the committed input {input}"))
}

/// Whether `dir`'s `witness.json` judges `property` expecting `expect`
/// (`holds` or `violated`). A witness file for another property or role
/// isn't this run's trigger evidence.
fn witness_judges(dir: &Path, property: &str, expect: &str) -> bool {
    read_json::<Value>(&dir.join("witness.json")).is_some_and(|witness| {
        witness.get("property").and_then(Value::as_str) == Some(property)
            && witness.get("expect").and_then(Value::as_str) == Some(expect)
    })
}

/// Fails `run` for `why`, unless it was killed, which stays its end.
fn fail(run: &mut Execution, why: String) {
    if run.end != End::Killed {
        run.end = End::Failed { why };
    }
}

/// Why a case's patched binary isn't its own checkout's (C5), if it isn't
/// or its check is missing.
fn provenance_refusal(dir: &Path) -> Option<String> {
    match read_json::<ProvenanceCheck>(&dir.join(PROVENANCE_FILE)) {
        Some(check) => check.refused,
        None => Some("no provenance check".to_owned()),
    }
}

/// Every planted case under `repo`'s `qa/planted` that is header sync's
/// ([`is_header_case`]), as `(property, name)`. The property is the patch's
/// directory relative to `qa/planted`, at any depth, as the planted runner
/// reads it.
pub fn planted_cases(repo: &Path) -> Vec<(String, String)> {
    let registered = super::registered();
    let ids: BTreeSet<&str> = registered
        .iter()
        .map(|registration| registration.property.as_ref())
        .collect();
    planted_cases_in(&repo.join("qa/planted"), &|property, name| {
        is_header_case(repo, &ids, property, name)
    })
}

/// Whether planted case `name` of `property` is header sync's: any case
/// under the `header-sync` or `header` families, whatever harness it names,
/// so validation still checks it; or a case of a registered header ID
/// (`ids`) that runs on a header harness. An ID that other harnesses
/// register too, such as `session/honest-never-punished`, holds their cases
/// as well, so a case there is left out only when it names a known
/// harness that isn't a header one. One whose harness can't be read, or
/// names no known harness, is kept, so it fails validation rather than
/// disappearing.
fn is_header_case(repo: &Path, ids: &BTreeSet<&str>, property: &str, name: &str) -> bool {
    if property.starts_with("header-sync/") || property.starts_with("header/") {
        return true;
    }
    if !ids.contains(property) {
        return false;
    }
    super::planted_harness(repo, property, name).map_or(true, |harness| {
        HEADER_HARNESSES.contains(&harness.as_str())
            || !crate::harness::HARNESSES
                .iter()
                .any(|entry| entry.name == harness)
    })
}

/// Every planted case under `root` that `header` keeps, given its property
/// and name, as `(property, name)`, sorted. An unreadable directory holds
/// none.
fn planted_cases_in(root: &Path, header: &dyn Fn(&str, &str) -> bool) -> Vec<(String, String)> {
    /// Collects every `.patch` under `dir`.
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for path in entries.filter_map(Result::ok).map(|entry| entry.path()) {
            if path.is_dir() {
                walk(root, &path, out);
            } else if path.extension().is_some_and(|ext| ext == "patch") {
                let property = path
                    .parent()
                    .and_then(|parent| parent.strip_prefix(root).ok())
                    .and_then(Path::to_str);
                let name = path.file_stem().and_then(|stem| stem.to_str());
                if let (Some(property), Some(name)) = (property, name) {
                    out.push((property.to_owned(), name.to_owned()));
                }
            }
        }
    }
    let mut cases = Vec::new();
    walk(root, root, &mut cases);
    cases.retain(|(property, name)| header(property, name));
    cases.sort();
    cases
}

/// Every native-mutation directory under `repo`'s
/// `qa/native-mutations/header-sync/`.
pub fn native_dirs(repo: &Path) -> Vec<String> {
    let mut dirs: Vec<String> = fs::read_dir(repo.join("qa/native-mutations/header-sync"))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter_map(|path| path.file_name()?.to_str().map(str::to_owned))
        .collect();
    dirs.sort();
    dirs
}

/// Every `(file, test, property)` `COVERAGE` names for a header ID.
pub fn coverage_tests() -> Vec<(String, String, String)> {
    let ids: BTreeSet<String> = super::registered()
        .into_iter()
        .map(|registration| registration.property)
        .collect();
    let mut tests = BTreeSet::new();
    for row in COVERAGE.iter().filter(|row| ids.contains(row.property)) {
        let witness = row.passing.witness();
        tests.insert((
            witness.file.to_owned(),
            witness.test.to_owned(),
            row.property.to_owned(),
        ));
        if let Bypass::Test(bypass) = row.bypass {
            tests.insert((
                bypass.file.to_owned(),
                bypass.test.to_owned(),
                row.property.to_owned(),
            ));
        }
    }
    tests.into_iter().collect()
}

/// The keys `manifest` must cover, from `repo`'s directories and the
/// registry.
pub fn committed_required(repo: &Path, manifest: &Manifest) -> BTreeMap<Key, BTreeSet<Gate>> {
    let registered = super::registered();
    let needs = super::needs();
    let planted = planted_cases(repo);
    let native = native_dirs(repo);
    let coverage = coverage_tests();
    let planted_harnesses: BTreeMap<(String, String), String> = planted
        .iter()
        .filter_map(|(property, name)| {
            let harness = super::planted_harness(repo, property, name).ok()?;
            Some(((property.clone(), name.clone()), harness))
        })
        .collect();
    required_keys(&Inventories {
        registered: &registered,
        needs: &needs,
        planted: &planted,
        planted_harnesses: &planted_harnesses,
        native: &native,
        coverage_tests: &coverage,
        contracts: &super::contract_table(),
        required_contracts: REQUIRED_CONTRACTS,
        narrowing: NARROWING,
        requirements: GATE_OBLIGATIONS,
        composition: COMPOSITION_CHECKS,
        reasons: super::reasons::NA_REASONS,
        expected: &manifest.expected,
        histories: &super::rows::history_targets(),
        machine: super::rows::is_machine,
    })
}

/// Where one obligation's evidence is.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Links {
    /// The harness its runs ran on.
    pub harness: Option<String>,
    /// The committed input, or a typed case's scenario.
    pub input: Option<String>,
    /// A typed case's Plan bytes.
    pub plan: Option<String>,
    /// The property whose trigger history it judged.
    pub trigger: Option<String>,
    /// The history its runs executed, for an integrity mutation.
    pub history: Option<RunId>,
    /// The result in `results.json` that judges it, or `None` if the tool
    /// produced none.
    pub result: Option<ExpectedId>,
    /// Its artifacts, relative to the artifacts directory.
    pub artifacts: Vec<String>,
}

/// How a record written to `out` names its `artifacts` directory: relative
/// to `out` when it's under it, so the two move together, and absolute
/// otherwise. Relative arguments are taken from this process's directory
/// first, so neither form depends on where a reader runs.
pub fn record_artifacts(artifacts: &Path, out: &Path) -> String {
    let absolute = |path: &Path| {
        if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir().map_or_else(|_| path.to_owned(), |dir| dir.join(path))
        }
    };
    let (artifacts, out) = (absolute(artifacts), absolute(out));
    match artifacts.strip_prefix(&out) {
        Ok(relative) => relative.display().to_string(),
        Err(_) => artifacts.display().to_string(),
    }
}

/// One contract's row in the record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractExport {
    /// Its ID, as its `Debug` name, such as `C7`.
    pub id: String,
    /// Its owner.
    pub owner: String,
    /// Its state, as its `Debug` form: `Satisfied`, `Blocked` with what it
    /// waits for, or `Excluded` with its reason and approval record.
    pub state: String,
}

/// A standing `Open` a key's result is, with its reason, owner and
/// approval record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenExport {
    /// What's left out and why.
    pub reason: String,
    /// Who owns it.
    pub owner: String,
    /// The approval, as its `Debug` form, such as `Adam { record: "..." }`.
    pub approval: String,
}

/// One obligation in the record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordEntry {
    /// The obligation key.
    pub key: Key,
    /// Why it's excluded or blocked, or `None` if verified.
    pub why: Option<String>,
    /// Its evidence.
    pub links: Links,
    /// The standing `Open` its expected result is, if it's one.
    pub open: Option<OpenExport>,
}

/// The acceptance record: its subject commit, the evaluator's three
/// exports by obligation key, each gate's verdict and the strays.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    /// The subject commit S.
    pub subject: String,
    /// How the run contained its children (see [`Containment`]).
    pub containment: Containment,
    /// The artifacts directory every link's `artifacts` path is relative
    /// to: relative to the record's own directory when it's under it, as the
    /// `header-acceptance` binary writes it, so the two move together.
    pub artifacts: String,
    /// The contracts table as judged, so an exclusion that names a contract
    /// resolves to its reason and approval record here.
    pub contracts: Vec<ContractExport>,
    /// Verified obligations.
    pub verified: Vec<RecordEntry>,
    /// Excluded obligations, each with its exclusion.
    pub excluded: Vec<RecordEntry>,
    /// Blocked obligations, each with what blocks it.
    pub blocked: Vec<RecordEntry>,
    /// Each gate's verdict.
    pub gates: BTreeMap<Gate, bool>,
    /// What fails acceptance outside any key.
    pub stray: Vec<String>,
    /// Whether the blocked list and the strays are empty.
    pub accepted: bool,
    /// Whether the excluded list is empty too.
    pub fully_accepted: bool,
}

/// Each key's evidence links, from `plan`'s tasks, of only the tasks whose
/// directories are `bound` to them ([`TASK_FILE`]).
fn links(
    manifest: &Manifest,
    plan: &RunPlan,
    bound: impl Fn(usize, &Task) -> bool,
) -> BTreeMap<Key, Links> {
    let mut by_id: BTreeMap<ExpectedId, Links> = BTreeMap::new();
    for (index, task) in plan.tasks.iter().enumerate() {
        // A directory that isn't this task's links nothing.
        if !bound(index, task) {
            continue;
        }
        let (id, links) = match task {
            Task::Test { expected, .. } => (
                expected.clone(),
                Links {
                    result: Some(expected.clone()),
                    artifacts: vec![format!("{index}/{TEST_FILE}")],
                    ..Links::default()
                },
            ),
            Task::Case {
                expected,
                property,
                harness,
                input,
                machine,
                ..
            } => (
                expected.clone(),
                Links {
                    harness: Some(harness.clone()),
                    input: Some(match input {
                        CaseInput::Fuzz { input } => input.clone(),
                        CaseInput::Typed { scenario, .. } => scenario.clone(),
                    }),
                    plan: match input {
                        CaseInput::Typed { plan, .. } => Some(plan.clone()),
                        CaseInput::Fuzz { .. } => None,
                    },
                    trigger: Some(property.clone()),
                    // A node case's result needs its Draft run, and a typed
                    // case's C18's binding, so the tool writes none, but
                    // its runs stay linked.
                    result: (*machine && matches!(input, CaseInput::Fuzz { .. }))
                        .then(|| expected.clone()),
                    artifacts: vec![
                        format!("{index}/baseline/trace.jsonl"),
                        format!("{index}/baseline/witness.json"),
                        format!("{index}/patched/trace.jsonl"),
                        format!("{index}/patched/witness.json"),
                        format!("{index}/{PROVENANCE_FILE}"),
                        format!("{index}/{VERDICT_FILE}"),
                        format!("{index}/{TARGET_FILE}"),
                    ],
                    ..Links::default()
                },
            ),
            Task::Integrity {
                expected,
                harness,
                case,
                package,
                file,
                test,
                ..
            } => (
                expected.clone(),
                Links {
                    harness: Some(harness.clone()),
                    history: Some(RunId::History {
                        harness: harness.clone(),
                        package: package.clone(),
                        file: file.clone(),
                        test: test.clone(),
                        case: case.clone(),
                    }),
                    result: Some(expected.clone()),
                    artifacts: ["unpatched", "patched", "restored"]
                        .iter()
                        .map(|run| format!("{index}/{run}/{TEST_FILE}"))
                        .collect(),
                    ..Links::default()
                },
            ),
        };
        by_id.insert(id, links);
    }
    // A history or replay the tool can't produce yet still links what it's
    // of: a history's harness and identity, and a planted run's replay the
    // trace its case task copied.
    let cases: BTreeMap<(&str, &str), (usize, &Task)> = plan
        .tasks
        .iter()
        .enumerate()
        .filter_map(|(index, task)| match task {
            Task::Case { property, name, .. } if bound(index, task) => {
                Some(((property.as_str(), name.as_str()), (index, task)))
            }
            _ => None,
        })
        .collect();
    for result in &manifest.expected {
        let Some(id) = result.expected.id() else {
            continue;
        };
        let fallback = match &result.expected {
            Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
                Links {
                    harness: Some(history.harness.to_owned()),
                    history: Some(RunId::from(*history)),
                    ..Links::default()
                }
            }
            Expected::Replays { run } => replay_links(run, &cases),
            _ => continue,
        };
        by_id.entry(id).or_insert(fallback);
    }
    let mut links = BTreeMap::new();
    for result in &manifest.expected {
        let found = result
            .expected
            .id()
            .and_then(|id| by_id.get(&id).cloned())
            .unwrap_or_default();
        for key in &result.keys {
            links.insert(key.clone(), found.clone());
        }
    }
    links
}

/// What a replay of `run` links before the tool replays anything: a
/// history's harness and identity, or a planted run's harness, input and
/// trace, as its case's task (`cases`, by property and name) planned and
/// copied it. A `Draft` run has no trace yet.
fn replay_links(run: &RunId, cases: &BTreeMap<(&str, &str), (usize, &Task)>) -> Links {
    match run {
        RunId::History { harness, .. } => Links {
            harness: Some(harness.clone()),
            history: Some(run.clone()),
            ..Links::default()
        },
        RunId::Case {
            property,
            name,
            run: role,
        } => {
            let Some((index, Task::Case { harness, input, .. })) =
                cases.get(&(property.as_str(), name.as_str()))
            else {
                return Links {
                    history: Some(run.clone()),
                    trigger: Some(property.clone()),
                    ..Links::default()
                };
            };
            let dir = match role {
                CaseRun::Baseline => Some("baseline"),
                CaseRun::Patched => Some("patched"),
                _ => None,
            };
            let (input, plan) = match input {
                CaseInput::Fuzz { input } => (Some(input.clone()), None),
                CaseInput::Typed { scenario, plan } => (Some(scenario.clone()), Some(plan.clone())),
            };
            Links {
                harness: Some(harness.clone()),
                input,
                plan,
                history: Some(run.clone()),
                trigger: Some(property.clone()),
                artifacts: dir
                    .map(|dir| {
                        vec![
                            format!("{index}/{dir}/trace.jsonl"),
                            format!("{index}/{dir}/witness.json"),
                        ]
                    })
                    .unwrap_or_default(),
                ..Links::default()
            }
        }
    }
}

/// The record of `evaluation` for the run `run`, naming `artifacts`. Each
/// key's evidence links come from `plan`, relative to `artifacts`, when the
/// artifacts are that plan's run (`refusal` is `None`). Otherwise the record
/// links nothing and names the refusal as a stray. A run without
/// [`Containment::Subreaper`] is never accepted (see [`Containment`]).
pub fn record(
    manifest: &Manifest,
    plan: &RunPlan,
    artifacts: &Path,
    run: &RunBinding,
    refusal: Option<String>,
    evaluation: &Evaluation,
) -> Record {
    let links = match refusal {
        None => links(manifest, plan, |index, task| {
            read_json::<serde_json::Value>(&artifacts.join(index.to_string()).join(TASK_FILE))
                == Some(task_binding(index, task))
        }),
        Some(_) => BTreeMap::new(),
    };
    let open = |key: &Key| {
        manifest
            .expected
            .iter()
            .find_map(|result| match result.expected {
                Expected::Open {
                    reason,
                    owner,
                    approval,
                    until: None,
                } if result.keys.contains(key) => Some(OpenExport {
                    reason: reason.to_owned(),
                    owner: owner.to_owned(),
                    approval: format!("{approval:?}"),
                }),
                _ => None,
            })
    };
    let entry = |key: &Key, why: Option<String>| RecordEntry {
        key: key.clone(),
        why,
        links: links.get(key).cloned().unwrap_or_default(),
        open: open(key),
    };
    let mut stray = evaluation.stray.clone();
    stray.extend(refusal);
    if run.containment != Containment::Subreaper {
        stray.push(
            "the run's containment was best-effort; only a Linux run under a subreaper is an \
             acceptance record"
                .into(),
        );
    }
    let mut record = Record {
        subject: run.subject.clone(),
        containment: run.containment,
        artifacts: artifacts.display().to_string(),
        contracts: manifest
            .contracts
            .iter()
            .map(|contract| ContractExport {
                id: format!("{:?}", contract.id),
                owner: contract.owner.to_owned(),
                state: format!("{:?}", contract.state),
            })
            .collect(),
        verified: Vec::new(),
        excluded: Vec::new(),
        blocked: Vec::new(),
        gates: evaluation.gates.clone(),
        accepted: evaluation.accepted() && stray.is_empty(),
        fully_accepted: evaluation.fully_accepted() && stray.is_empty(),
        stray,
    };
    for (key, state) in &evaluation.keys {
        match state {
            ObligationState::Verified => record.verified.push(entry(key, None)),
            ObligationState::Excluded { why } => {
                record.excluded.push(entry(key, Some(why.clone())))
            }
            ObligationState::Blocked { why } => record.blocked.push(entry(key, Some(why.clone()))),
        }
    }
    record
}

/// Assembles `plan`'s artifacts, evaluates them against the committed
/// manifest, and returns the result file and the record, failed if the
/// committed inventories don't match their pin ([`evaluate_against`]).
pub fn evaluate_artifacts(
    repo: &Path,
    plan: &RunPlan,
    artifacts: &Path,
    run: &RunBinding,
) -> (ResultFile, Record) {
    evaluate_against(
        repo,
        plan,
        artifacts,
        run,
        super::inventory::INVENTORY_FINGERPRINT,
    )
}

/// [`evaluate_artifacts`] with the inventories checked against `pin`
/// ([`super::inventory`]): on a mismatch the evaluation is invalidated, so
/// the record lists the mismatch as a stray and fails every gate. The
/// record's only path runs through here, so deleting a list entry or a
/// static test can't remove the check.
fn evaluate_against(
    repo: &Path,
    plan: &RunPlan,
    artifacts: &Path,
    run: &RunBinding,
    pin: &str,
) -> (ResultFile, Record) {
    let (subject, subject_source) = (run.subject.as_str(), run.subject_source.as_str());
    let manifest = Manifest::committed();
    let required = committed_required(repo, &manifest);
    let results = assemble(repo, plan, artifacts, run);
    let mut evaluation = evaluate(
        &manifest,
        &required,
        &results,
        Subject {
            commit: subject,
            source: subject_source,
        },
    );
    evaluation.invalidate(super::inventory::errors_against(
        &manifest,
        &required,
        &super::inventory::Lists::committed(),
        pin,
    ));
    let refusal = binding_refusal(repo, plan, artifacts, run);
    let record = record(&manifest, plan, artifacts, run, refusal, &evaluation);
    (results, record)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A history identity for the fixtures.
    fn history_run() -> RunId {
        RunId::History {
            harness: "header-sync/storage".into(),
            package: "zakura-sim".into(),
            file: "tests/header_node.rs".into(),
            test: "a_test".into(),
            case: "plan 0".into(),
        }
    }

    /// A machine harness, and a node one.
    const MACHINE: &str = "header/reactor-policy";
    const NODE: &str = "header-sync/storage";

    /// Records a real trace of the machine harness on its committed input
    /// into `dir`, its meta's build source set to `source`, with a witness
    /// that passed.
    fn record_into(dir: &Path, source: &str) {
        let input = fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/header-reactor-policy.bin"),
        )
        .expect("the committed input is readable");
        let entry = crate::harness::find(MACHINE).expect("the machine harness is registered");
        crate::harness::record(entry, crate::harness::RecordInput::Input(&input), dir)
            .expect("the machine records its committed input");
        rewrite(dir, |lines| {
            lines[0]["meta"]["build"]["source"] = Value::String(source.to_owned());
        });
        fs::write(dir.join("witness.json"), r#"{"passed": true}"#)
            .expect("the temp dir is writable");
    }

    /// Rewrites `dir`'s trace through `edit` on its parsed lines.
    fn rewrite(dir: &Path, edit: impl FnOnce(&mut Vec<Value>)) {
        let path = dir.join("trace.jsonl");
        let text = fs::read_to_string(&path).expect("the trace is readable");
        let mut lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is JSON"))
            .collect();
        edit(&mut lines);
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        fs::write(&path, text).expect("the temp dir is writable");
    }

    /// [`record_into`] a fresh directory, built from `s`.
    fn recorded() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        record_into(dir.path(), "s");
        dir
    }

    /// A machine run's actions follow its driver: completed is its whole
    /// input, stopped at a violation a prefix, anything else none. A node
    /// run's actions need effect evidence the tool doesn't have yet, so
    /// they're incomplete however it ends or whatever it picked.
    #[test]
    fn a_run_s_actions_follow_its_harness() {
        assert!(crate::harness::find(MACHINE).is_ok_and(|entry| entry.machine));
        assert!(crate::harness::find(NODE).is_ok_and(|entry| !entry.machine));
        let violation = End::Violation {
            property: "t/a".into(),
        };
        assert_eq!(actions(MACHINE, &End::Completed), Actions::Complete);
        assert_eq!(actions(MACHINE, &violation), Actions::Prefix);
        assert_eq!(actions(MACHINE, &End::Killed), Actions::Incomplete);
        assert_eq!(actions(NODE, &End::Completed), Actions::Incomplete);
        assert_eq!(actions(NODE, &violation), Actions::Incomplete);
        assert_eq!(
            actions("no/such-harness", &End::Completed),
            Actions::Incomplete
        );
    }

    /// An execution is read from a real machine trace: its reports, end,
    /// actions, leak count and digest. A machine trace, input and no
    /// control list, is a whole completed run, as a machine baseline must
    /// be. A trace the simulator's reader refuses (a malformed middle line,
    /// an end with no newline), a killed child or a missing trace is a run
    /// that didn't complete.
    #[test]
    fn an_execution_is_read_from_its_trace() {
        let dir = recorded();
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert_eq!(run.end, End::Completed, "{run:?}");
        assert_eq!(run.actions, Actions::Complete);
        assert_eq!(run.leaked, 0);
        assert_eq!(run.meta.harness, MACHINE);
        assert!(run.trigger);
        assert_eq!(run.trace, digest(&dir.path().join("trace.jsonl")));
        assert!(usable_digest(&run.trace));
        assert!(matches!(
            run.reports.get(MACHINE),
            Some(Report::Held { .. })
        ));
        assert_eq!(run.meta.input, None);
        assert_eq!(trace_source(dir.path()).as_deref(), Some("s"));

        // A malformed middle line, or an end cut off before its newline.
        let malformed = recorded();
        let path = malformed.path().join("trace.jsonl");
        let text = fs::read_to_string(&path).expect("the trace is readable");
        let (first, rest) = text.split_once('\n').expect("the trace has lines");
        fs::write(&path, format!("{first}\nnot json\n{rest}")).expect("the temp dir is writable");
        let run = execution(malformed.path(), history_run(), MACHINE, RunBy::Tool);
        assert!(matches!(run.end, End::Failed { .. }), "{:?}", run.end);
        let cut = recorded();
        let path = cut.path().join("trace.jsonl");
        let text = fs::read_to_string(&path).expect("the trace is readable");
        fs::write(&path, text.trim_end()).expect("the temp dir is writable");
        let run = execution(cut.path(), history_run(), MACHINE, RunBy::Tool);
        assert!(matches!(run.end, End::Failed { .. }), "{:?}", run.end);

        fs::write(dir.path().join(KILLED_FILE), "").expect("the temp dir is writable");
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert_eq!(run.end, End::Killed);
        let empty = tempfile::tempdir().expect("a temp dir can be created");
        let run = execution(empty.path(), history_run(), MACHINE, RunBy::Tool);
        assert!(matches!(run.end, End::Failed { .. }));
        assert_eq!(run.trace, "missing");
    }

    /// A case's baseline must be built from the subject's source, and its
    /// patched run from the source its provenance check measured, which
    /// must have passed. Each one-field change fails only its own run, and
    /// a killed run stays killed.
    #[test]
    fn each_case_trace_comes_from_its_credited_binary() {
        let credit = |baseline_source: &str,
                      patched_source: &str,
                      check: Option<ProvenanceCheck>,
                      killed: bool|
         -> (End, End) {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            for (run, source) in [("baseline", baseline_source), ("patched", patched_source)] {
                let run = dir.path().join(run);
                fs::create_dir_all(&run).expect("the temp dir is writable");
                record_into(&run, source);
                if killed {
                    fs::write(run.join(KILLED_FILE), "").expect("the temp dir is writable");
                }
            }
            if let Some(check) = check {
                fs::write(
                    dir.path().join(PROVENANCE_FILE),
                    serde_json::to_string(&check).expect("a check serializes"),
                )
                .expect("the temp dir is writable");
            }
            let read = |run: &str| {
                execution(
                    &dir.path().join(run),
                    history_run(),
                    MACHINE,
                    RunBy::Planted,
                )
            };
            let (mut baseline, mut patched) = (read("baseline"), read("patched"));
            credit_sources(dir.path(), "subject", &mut baseline, &mut patched);
            (baseline.end, patched.end)
        };
        let ok = |(baseline, patched): (End, End)| {
            (baseline == End::Completed, patched == End::Completed)
        };
        let passed = |source: &str| {
            Some(ProvenanceCheck {
                source: source.into(),
                patch_sha256: String::new(),
                refused: None,
            })
        };
        assert_eq!(
            ok(credit("subject", "patched", passed("patched"), false)),
            (true, true)
        );
        // The baseline from another source, or an unknown one.
        assert_eq!(
            ok(credit("other", "patched", passed("patched"), false)),
            (false, true)
        );
        assert_eq!(
            ok(credit("unknown", "patched", passed("patched"), false)),
            (false, true)
        );
        // The patched trace from a source other than the measured one.
        assert_eq!(
            ok(credit("subject", "other", passed("patched"), false)),
            (true, false)
        );
        assert_eq!(
            ok(credit("subject", "unknown", passed("unknown"), false)),
            (true, false)
        );
        // A refused or missing provenance check.
        let refused = Some(ProvenanceCheck {
            source: "patched".into(),
            patch_sha256: String::new(),
            refused: Some("it embeds S".into()),
        });
        assert_eq!(
            ok(credit("subject", "patched", refused, false)),
            (true, false)
        );
        assert_eq!(ok(credit("subject", "patched", None, false)), (true, false));
        // A killed planted invocation stays killed through every check.
        assert_eq!(
            credit("other", "other", None, true),
            (End::Killed, End::Killed)
        );
    }

    /// A case is credited only when the planted runner reported it caught
    /// and, if it has a fuzz target, confirmed it there, each check naming
    /// this case. Each check refused, missing or naming another case on its
    /// own is a reason to fail its patched run.
    #[test]
    fn a_case_needs_its_runner_verdict_and_target_confirmation() {
        type Check<'a> = Option<(&'a str, &'a str, Option<&'a str>)>;
        let read = |verdict: Check, target: Check, target_name: Option<&str>| {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            if let Some((property, name, refused)) = verdict {
                let check = VerdictCheck {
                    property: property.into(),
                    name: name.into(),
                    refused: refused.map(str::to_owned),
                };
                fs::write(
                    dir.path().join(VERDICT_FILE),
                    serde_json::to_string(&check).expect("a check serializes"),
                )
                .expect("the temp dir is writable");
            }
            if let Some((property, name, refused)) = target {
                let check = TargetCheck {
                    property: property.into(),
                    name: name.into(),
                    target: target_name.map(str::to_owned),
                    refused: refused.map(str::to_owned),
                };
                fs::write(
                    dir.path().join(TARGET_FILE),
                    serde_json::to_string(&check).expect("a check serializes"),
                )
                .expect("the temp dir is writable");
            }
            case_refusal(dir.path(), "p", "a", Some("fuzz_a"))
        };
        let mine = Some(("p", "a", None));
        assert_eq!(read(mine, mine, Some("fuzz_a")), None);
        // Confirmed through no target, or another one, while the harness
        // maps to `fuzz_a`.
        assert!(read(mine, mine, None).is_some());
        assert!(read(mine, mine, Some("fuzz_b")).is_some());
        // The runner's verdict refused, or missing.
        assert!(read(
            Some(("p", "a", Some("the campaign failed"))),
            mine,
            Some("fuzz_a")
        )
        .is_some());
        assert!(read(None, mine, Some("fuzz_a")).is_some());
        // The target confirmation refused, or missing.
        assert!(read(mine, Some(("p", "a", Some("it held"))), Some("fuzz_a")).is_some());
        assert!(read(mine, None, Some("fuzz_a")).is_some());
        // Either check for another case, one field off.
        for other in [("q", "a", None), ("p", "a-b", None)] {
            assert!(
                read(Some(other), mine, Some("fuzz_a")).is_some(),
                "{other:?}"
            );
            assert!(
                read(mine, Some(other), Some("fuzz_a")).is_some(),
                "{other:?}"
            );
        }
        // A harness no target runs needs a check naming none.
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        for (file, value) in [
            (
                VERDICT_FILE,
                serde_json::json!({"property": "p", "name": "a", "refused": null}),
            ),
            (
                TARGET_FILE,
                serde_json::json!({"property": "p", "name": "a", "target": null, "refused": null}),
            ),
        ] {
            fs::write(dir.path().join(file), value.to_string()).expect("the temp dir is writable");
        }
        assert_eq!(case_refusal(dir.path(), "p", "a", None), None);
        assert!(case_refusal(dir.path(), "p", "a", Some("fuzz_a")).is_some());
    }

    /// The committed fuzz-target mapping is read from the fuzz package, as
    /// the planted runner reads it.
    #[test]
    fn a_harness_s_fuzz_target_is_the_committed_one() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let targets = fuzz_targets(&repo);
        assert_eq!(
            target_of(&targets, MACHINE).as_deref(),
            Some("header_reactor_policy")
        );
        assert_eq!(target_of(&targets, "header-sync/wire"), None);
        assert!(fuzz_targets(Path::new("/no/such/repo")).is_empty());
    }

    /// A machine trace counts for a case only if it ran the case's committed
    /// input, by SHA-256 and length, and its witness file judges the case's
    /// property with the run's expectation.
    #[test]
    fn a_case_trace_ran_its_committed_input_and_witness() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dir = recorded();
        let own = "crates/zakura-sim/tests/fixtures/header-reactor-policy.bin";
        let other = "crates/zakura-sim/tests/fixtures/header-publisher-policy.bin";
        assert_eq!(input_refusal(&repo, own, dir.path()), None);
        assert!(input_refusal(&repo, other, dir.path()).is_some());
        assert!(input_refusal(&repo, "no/such/input.bin", dir.path()).is_some());
        // Different bytes of the same length, so only the digest tells them
        // apart.
        let mut bytes = fs::read(repo.join(own)).expect("the committed input is readable");
        bytes[0] ^= 1;
        let altered = tempfile::tempdir().expect("a temp dir can be created");
        fs::write(altered.path().join("in.bin"), &bytes).expect("the temp dir is writable");
        assert!(input_refusal(altered.path(), "in.bin", dir.path()).is_some());
        let witness = |property: &str, expect: &str| {
            fs::write(
                dir.path().join("witness.json"),
                serde_json::json!({"passed": true, "property": property, "expect": expect, "error": null})
                    .to_string(),
            )
            .expect("the temp dir is writable");
        };
        witness(MACHINE, "violated");
        assert!(witness_judges(dir.path(), MACHINE, "violated"));
        assert!(!witness_judges(dir.path(), MACHINE, "holds"));
        assert!(!witness_judges(
            dir.path(),
            "header/publisher-policy",
            "violated"
        ));
        fs::remove_file(dir.path().join("witness.json")).expect("the file exists");
        assert!(!witness_judges(dir.path(), MACHINE, "violated"));
    }

    /// A typed planted case plans beside a fuzz-input one: both get tasks,
    /// the typed one's result is listed unsupported until C18 binds its
    /// scenario and Plan, and its links name both. A case naming both forms,
    /// or neither, is refused.
    #[test]
    fn a_typed_case_is_planned_beside_a_fuzz_case() {
        let repo = tempfile::tempdir().expect("a temp dir can be created");
        let dir = repo.path().join("qa/planted/header/reactor-policy");
        fs::create_dir_all(&dir).expect("the temp dir is writable");
        for (name, toml) in [
            ("fuzz", "input = \"fixtures/in.bin\"\n"),
            (
                "typed",
                "[typed]\nharness = \"header/reactor-policy\"\nscenario = \"s.json\"\nplan = \"p.bin\"\n",
            ),
            ("both", "input = \"in.bin\"\n[typed]\nharness = \"h\"\nscenario = \"s\"\nplan = \"p\"\n"),
        ] {
            fs::write(dir.join(format!("{name}.toml")), toml).expect("the temp dir is writable");
        }
        let caught = |name: &'static str| super::super::ExpectedResult {
            keys: Vec::new(),
            gates: Vec::new(),
            expected: Expected::MachineCaught {
                case: CaseRef {
                    property: MACHINE,
                    name,
                },
                harness: MACHINE,
            },
        };
        let manifest = Manifest {
            expected: vec![caught("fuzz"), caught("typed")],
            ..Manifest::committed()
        };
        let planned = plan(&manifest, repo.path()).expect("both cases plan");
        let inputs: Vec<&CaseInput> = planned
            .tasks
            .iter()
            .filter_map(|task| match task {
                Task::Case { input, .. } => Some(input),
                _ => None,
            })
            .collect();
        assert_eq!(
            inputs,
            [
                &CaseInput::Fuzz {
                    input: "fixtures/in.bin".into()
                },
                &CaseInput::Typed {
                    scenario: "s.json".into(),
                    plan: "p.bin".into()
                },
            ]
        );
        let typed_id = caught("typed").expected.id();
        assert!(planned
            .unsupported
            .iter()
            .any(|(id, _)| Some(id) == typed_id.as_ref()));
        let fuzz_id = caught("fuzz").expected.id();
        assert!(!planned
            .unsupported
            .iter()
            .any(|(id, _)| Some(id) == fuzz_id.as_ref()));
        let both = Manifest {
            expected: vec![caught("both")],
            ..Manifest::committed()
        };
        assert!(plan(&both, repo.path()).is_err());
    }

    /// An artifacts directory counts only for the run of S it names: one
    /// with no `run.json`, or another subject's, gives no result, so every
    /// obligation blocks. A case's provenance check must be for its
    /// committed patch.
    #[test]
    fn artifacts_count_only_for_their_own_run_and_patch() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let artifacts = tempfile::tempdir().expect("a temp dir can be created");
        let run = RunBinding {
            run_id: "run-1".into(),
            subject: "s".into(),
            subject_source: "source".into(),
            containment: Containment::Subreaper,
        };
        let results = |dir: &Path, plan: &RunPlan| assemble(&repo, plan, dir, &run).results.len();
        // A refused directory's record links none of its artifacts and says
        // why it was refused.
        let linked = |dir: &Path, plan: &RunPlan| {
            let (_, record) = evaluate_artifacts(&repo, plan, dir, &run);
            let linked = [&record.verified, &record.excluded, &record.blocked]
                .into_iter()
                .flatten()
                .any(|entry| entry.links != Links::default());
            (linked, record.stray)
        };
        let refused = |dir: &Path, plan: &RunPlan, why: &str| {
            let (linked, stray) = linked(dir, plan);
            assert!(!linked, "a refused run links nothing");
            assert!(stray.iter().any(|stray| stray.contains(why)), "{stray:?}");
        };
        assert_eq!(results(artifacts.path(), &plan), 0);
        refused(artifacts.path(), &plan, RUN_FILE);
        let bind = |id: &str, subject: &str, source: &str| {
            let binding = RunBinding {
                run_id: id.into(),
                subject: subject.into(),
                subject_source: source.into(),
                ..run.clone()
            };
            fs::write(
                artifacts.path().join(RUN_FILE),
                serde_json::to_string(&binding).expect("a binding serializes"),
            )
            .expect("the temp dir is writable");
        };
        bind("run-1", "another commit", "source");
        assert_eq!(results(artifacts.path(), &plan), 0);
        bind("run-1", "s", "another source");
        assert_eq!(results(artifacts.path(), &plan), 0);
        // Another attempt at S.
        bind("run-0", "s", "source");
        assert_eq!(results(artifacts.path(), &plan), 0);
        refused(artifacts.path(), &plan, RUN_FILE);
        // The same run, subject and source, but run.json says the run's
        // containment was best-effort.
        fs::write(
            artifacts.path().join(RUN_FILE),
            serde_json::to_string(&RunBinding {
                containment: Containment::BestEffort,
                ..run.clone()
            })
            .expect("a binding serializes"),
        )
        .expect("the temp dir is writable");
        assert_eq!(results(artifacts.path(), &plan), 0);
        refused(artifacts.path(), &plan, RUN_FILE);
        assert!(
            !evaluate_artifacts(&repo, &plan, artifacts.path(), &run)
                .1
                .accepted
        );
        bind("run-1", "s", "source");
        // Without its task file, no task directory is read.
        assert_eq!(results(artifacts.path(), &plan), 0);
        let write_task = |index: usize, task: &Task| {
            let dir = artifacts.path().join(index.to_string());
            fs::create_dir_all(&dir).expect("the temp dir is writable");
            fs::write(dir.join(TASK_FILE), task_binding(index, task).to_string())
                .expect("the temp dir is writable");
        };
        for (index, task) in plan.tasks.iter().enumerate() {
            write_task(index, task);
        }
        let all = results(artifacts.path(), &plan);
        assert!(all > 0);
        // Two test tasks' directories swapped: each names the other task,
        // so neither gives a result.
        let tests: Vec<usize> = plan
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| matches!(task, Task::Test { .. }))
            .map(|(index, _)| index)
            .take(2)
            .collect();
        let [a, b] = tests[..] else {
            panic!("the plan has two test tasks");
        };
        write_task(a, &plan.tasks[b]);
        write_task(b, &plan.tasks[a]);
        assert_eq!(results(artifacts.path(), &plan), all - 2);
        // Nor does the record link either directory.
        let links_into = |index: usize| {
            let (_, record) = evaluate_artifacts(&repo, &plan, artifacts.path(), &run);
            let prefix = format!("{index}/");
            let linked = [&record.verified, &record.excluded, &record.blocked]
                .into_iter()
                .flatten()
                .any(|entry| {
                    entry
                        .links
                        .artifacts
                        .iter()
                        .any(|path| path.starts_with(&prefix))
                });
            linked
        };
        assert!(!links_into(a) && !links_into(b));
        // The right task at the wrong index.
        fs::write(
            artifacts.path().join(a.to_string()).join(TASK_FILE),
            task_binding(b, &plan.tasks[a]).to_string(),
        )
        .expect("the temp dir is writable");
        assert_eq!(results(artifacts.path(), &plan), all - 2);
        write_task(a, &plan.tasks[a]);
        write_task(b, &plan.tasks[b]);
        assert_eq!(results(artifacts.path(), &plan), all);
        assert!(links_into(a) && links_into(b));
        let (linked_now, stray) = linked(artifacts.path(), &plan);
        assert!(linked_now && stray.is_empty(), "{stray:?}");
        // A plan other than the one S's manifest gives: one case's input
        // changed.
        let mut stale = plan.clone();
        let input = stale
            .tasks
            .iter_mut()
            .find_map(|task| match task {
                Task::Case { input, .. } => Some(input),
                _ => None,
            })
            .expect("the plan has a case");
        *input = CaseInput::Fuzz {
            input: "an/older/input.bin".into(),
        };
        assert_eq!(results(artifacts.path(), &stale), 0);
        refused(artifacts.path(), &stale, "the plan");

        let patch = "qa/planted/header/reactor-policy/locator-call-settles-without-removal.patch";
        let case = tempfile::tempdir().expect("a temp dir can be created");
        let check = |sha: String| {
            let check = ProvenanceCheck {
                source: "patched".into(),
                patch_sha256: sha,
                refused: None,
            };
            fs::write(
                case.path().join(PROVENANCE_FILE),
                serde_json::to_string(&check).expect("a check serializes"),
            )
            .expect("the temp dir is writable");
        };
        use sha2::{Digest, Sha256};
        let committed: String =
            Sha256::digest(fs::read(repo.join(patch)).expect("the patch exists"))
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
        check(committed);
        assert_eq!(patch_refusal(&repo, patch, case.path()), None);
        check("00".repeat(32));
        assert!(patch_refusal(&repo, patch, case.path()).is_some());
        fs::remove_file(case.path().join(PROVENANCE_FILE)).expect("the file exists");
        assert!(patch_refusal(&repo, patch, case.path()).is_some());
    }

    /// A record names artifacts under its own directory relative to it, and
    /// any others absolutely, relative arguments taken from this process's
    /// directory.
    #[test]
    fn a_record_names_its_artifacts_where_a_reader_finds_them() {
        let here = std::env::current_dir().expect("the process has a directory");
        assert_eq!(
            record_artifacts(Path::new("/r/out/artifacts"), Path::new("/r/out")),
            "artifacts"
        );
        assert_eq!(
            record_artifacts(Path::new("out/artifacts"), Path::new("out")),
            "artifacts"
        );
        assert_eq!(
            record_artifacts(Path::new("runs/a"), Path::new("qa/record")),
            here.join("runs/a").display().to_string()
        );
        assert_eq!(
            record_artifacts(Path::new("/abs/runs/a"), Path::new("qa/record")),
            "/abs/runs/a"
        );
        assert_eq!(
            record_artifacts(&here.join("out/artifacts"), Path::new("out")),
            "artifacts"
        );
    }

    /// The runtime planted inventory reads nested properties at any depth,
    /// keeps the header ones, and skips the rest.
    #[test]
    fn the_planted_inventory_reads_nested_properties() {
        let root = tempfile::tempdir().expect("a temp dir can be created");
        for (dir, name) in [
            ("header-sync/s5v9/status/reserved-flags-closes", "flags"),
            ("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
            ("session/honest-never-punished", "punished"),
            ("block-sync/retained-byte-bound", "wide"),
        ] {
            fs::create_dir_all(root.path().join(dir)).expect("the temp dir is writable");
            fs::write(root.path().join(dir).join(format!("{name}.patch")), "")
                .expect("the temp dir is writable");
        }
        let cases = planted_cases_in(root.path(), &|property, _| {
            property.starts_with("header-sync/") || property == "session/honest-never-punished"
        });
        let owned = |property: &str, name: &str| (property.to_owned(), name.to_owned());
        assert_eq!(
            cases,
            vec![
                owned("header-sync/s5v9/headers/reserved-flags-closes", "flags"),
                owned("header-sync/s5v9/status/reserved-flags-closes", "flags"),
                owned("session/honest-never-punished", "punished"),
            ]
        );
    }

    /// The record fails when the inventories don't match the pin: it lists
    /// the mismatch as a stray, fails every gate, and isn't accepted. With
    /// the committed pin, the inventories add nothing to it.
    #[test]
    fn the_record_fails_on_an_inventory_mismatch() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let artifacts = tempfile::tempdir().expect("a temp dir can be created");
        let run = RunBinding {
            run_id: "run-1".into(),
            subject: "s".into(),
            subject_source: "source".into(),
            containment: Containment::Subreaper,
        };
        let changed = |record: &Record| {
            record
                .stray
                .iter()
                .any(|stray| stray.contains("inventories changed"))
        };
        let (_, committed) = evaluate_artifacts(&repo, &plan, artifacts.path(), &run);
        assert!(!changed(&committed));
        let (_, moved) = evaluate_against(&repo, &plan, artifacts.path(), &run, "another pin");
        assert!(changed(&moved));
        assert!(!moved.accepted);
        assert!(moved.gates.values().all(|verdict| !verdict));
    }

    /// A shared header ID's planted case counts for header acceptance only
    /// when it runs on a header harness, or when its harness can't be read
    /// or is no known one, by its typed table or its fuzz input's sidecar.
    /// A header-family case counts whatever its harness says.
    #[test]
    fn a_shared_id_s_case_counts_only_on_a_header_harness() {
        const SHARED: &str = "session/honest-never-punished";
        let registered = super::super::registered();
        assert!(
            registered.iter().any(|r| r.property == SHARED),
            "{SHARED} is a registered header ID"
        );
        let repo = tempfile::tempdir().expect("a temp dir can be created");
        let write = |path: &str, text: &str| {
            let path = repo.path().join(path);
            fs::create_dir_all(path.parent().expect("the path has a parent"))
                .expect("the temp dir is writable");
            fs::write(path, text).expect("the temp dir is writable");
        };
        let typed = |harness: &str| {
            format!("[typed]\nharness = \"{harness}\"\nscenario = \"s\"\nplan = \"p\"\n")
        };
        let fuzz = |input: &str| format!("input = \"{input}\"\n");
        let cases: [(&str, &str, Option<String>, bool); 9] = [
            (
                SHARED,
                "typed-header",
                Some(typed("header-sync/storage")),
                true,
            ),
            (
                SHARED,
                "typed-block",
                Some(typed("block-sync/storage")),
                false,
            ),
            (
                SHARED,
                "typed-typo",
                Some(typed("header-sync/no-such-harness")),
                true,
            ),
            (
                SHARED,
                "fuzz-header",
                Some(fuzz("fixtures/header.bin")),
                true,
            ),
            (
                SHARED,
                "fuzz-discovery",
                Some(fuzz("fixtures/discovery.bin")),
                false,
            ),
            (SHARED, "fuzz-typo", Some(fuzz("fixtures/typo.bin")), true),
            (SHARED, "unreadable", None, true),
            (
                "header-sync/x/y",
                "typed-block",
                Some(typed("block-sync/storage")),
                true,
            ),
            (
                "block-sync/retained-byte-bound",
                "typed-block",
                Some(typed("block-sync/storage")),
                false,
            ),
        ];
        write(
            "fixtures/header.toml",
            "harness = \"header/publisher-policy\"\n",
        );
        write("fixtures/discovery.toml", "harness = \"discovery/wire\"\n");
        write(
            "fixtures/typo.toml",
            "harness = \"discovery/no-such-harness\"\n",
        );
        for (property, name, toml, _) in &cases {
            write(&format!("qa/planted/{property}/{name}.patch"), "");
            if let Some(toml) = toml {
                write(&format!("qa/planted/{property}/{name}.toml"), toml);
            }
        }
        let mut kept: Vec<(String, String)> = cases
            .iter()
            .filter(|(.., kept)| *kept)
            .map(|(property, name, ..)| ((*property).to_owned(), (*name).to_owned()))
            .collect();
        kept.sort();
        assert_eq!(planted_cases(repo.path()), kept);
    }

    /// Every task's keys link its evidence: a node case, whose result the
    /// tool can't produce yet, keeps its runs' links with no result, and an
    /// integrity mutation links the history it ran.
    #[test]
    fn every_task_links_its_evidence() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let links = links(&manifest, &plan, |_, _| true);
        let mut node = false;
        let mut integrity = false;
        for (index, task) in plan.tasks.iter().enumerate() {
            let expected = match task {
                Task::Test { expected, .. }
                | Task::Case { expected, .. }
                | Task::Integrity { expected, .. } => expected,
            };
            let key = manifest
                .expected
                .iter()
                .find(|result| result.expected.id().as_ref() == Some(expected))
                .and_then(|result| result.keys.first())
                .expect("a task's identity has a key");
            let found = &links[key];
            assert!(
                found
                    .artifacts
                    .iter()
                    .all(|artifact| artifact.starts_with(&format!("{index}/"))),
                "{found:?}"
            );
            match task {
                Task::Case { machine: false, .. } => {
                    node = true;
                    assert_eq!(found.result, None);
                    assert!(found
                        .artifacts
                        .contains(&format!("{index}/patched/trace.jsonl")));
                    assert!(found
                        .artifacts
                        .contains(&format!("{index}/patched/witness.json")));
                }
                Task::Integrity { .. } => {
                    integrity = true;
                    assert!(matches!(found.history, Some(RunId::History { .. })));
                    assert_eq!(found.result.as_ref(), Some(expected));
                }
                _ => assert_eq!(found.result.as_ref(), Some(expected)),
            }
        }
        assert!(
            node && integrity,
            "the plan has a node case and an integrity mutation"
        );
    }

    /// A digest the evaluator would accept as naming an execution.
    fn usable_digest(trace: &str) -> bool {
        trace.len() == 64 && trace.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// A test artifact's results: a killed child fails every test it ran,
    /// and an unreadable artifact ran nothing.
    #[test]
    fn a_test_artifact_is_read_by_name() {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        let write = |killed: bool, exited_ok: bool| {
            let run = TestRun {
                package: "zakura-sim".into(),
                file: "tests/t.rs".into(),
                tests: vec![("a".into(), "ok".into()), ("b".into(), "ignored".into())],
                killed,
                exited_ok,
                log: String::new(),
                binary_source: Some("source".into()),
                checkout_source: Some("source".into()),
            };
            fs::write(
                dir.path().join(TEST_FILE),
                serde_json::to_string(&run).expect("a test run serializes"),
            )
            .expect("the temp dir is writable");
        };
        let outcomes = |actual: Actual| match actual {
            Actual::Test { ran, .. } => ran.into_iter().map(|r| r.outcome).collect::<Vec<_>>(),
            _ => unreachable!(),
        };
        write(false, true);
        assert_eq!(
            outcomes(test_actual(dir.path())),
            [TestOutcome::Passed, TestOutcome::Skipped]
        );
        // `ok` and then a nonzero exit or a signal isn't a pass.
        write(false, false);
        assert_eq!(
            outcomes(test_actual(dir.path())),
            [TestOutcome::Failed, TestOutcome::Skipped]
        );
        write(true, false);
        assert_eq!(
            outcomes(test_actual(dir.path())),
            [TestOutcome::Failed, TestOutcome::Failed]
        );
        let empty = tempfile::tempdir().expect("a temp dir can be created");
        assert!(outcomes(test_actual(empty.path())).is_empty());
    }

    /// Each integrity run's end is read from its test artifact: passing
    /// with a clean exit is completed, failing with the witness's own report
    /// of the diagnostic is that harness failure, and anything else didn't
    /// happen. An artifact one field off the history's test, in its package,
    /// file or name, isn't that run, and its binary must embed the checkout
    /// it ran in, S for the unpatched and restored runs and not S for the
    /// patched one. No run claims its history yet (C18, hdr-14.7b).
    #[test]
    fn an_integrity_run_is_read_from_its_artifact() {
        let input = history_run();
        let read =
            |run: CaseRun, test: (&str, &str, &str), result: &str, log: &str, source: &str| {
                let dir = tempfile::tempdir().expect("a temp dir can be created");
                let (package, file, name) = test;
                let artifact = TestRun {
                    package: package.into(),
                    file: file.into(),
                    tests: vec![(name.into(), result.into())],
                    killed: false,
                    exited_ok: result == "ok",
                    log: log.into(),
                    binary_source: Some(source.into()),
                    checkout_source: Some(source.into()),
                };
                fs::write(
                    dir.path().join(TEST_FILE),
                    serde_json::to_string(&artifact).expect("a test run serializes"),
                )
                .expect("the temp dir is writable");
                integrity_execution(
                    dir.path(),
                    RunId::native("raw-budget-lease", run),
                    &input,
                    NODE,
                    "OwnedMismatch",
                    "subject",
                )
            };
        // The witness's `run` panics with `{error}` on `run_prepared`'s
        // `HarnessError::Run`, whose `Display` is this line.
        let reported = |diagnostic: &str| {
            let failure = crate::end::HarnessFailure::SourceEvidence {
                source: "storage:header".into(),
                detail: format!(
                    "{diagnostic}: at a quiet boundary the header budget holds 3 owned"
                ),
            };
            format!(
                "thread 'a_test' panicked at tests/header_node_capacity.rs:52:34:\n\
                 the run hit a harness failure: {failure}\n"
            )
        };
        let own = ("zakura-sim", "tests/header_node.rs", "a_test");
        let patched = read(
            CaseRun::Patched,
            own,
            "FAILED",
            &reported("OwnedMismatch"),
            "patched",
        );
        assert_eq!(patched.meta.input, None);
        assert_eq!(
            patched.end,
            End::HarnessFailure {
                diagnostic: "OwnedMismatch".into()
            }
        );
        for run in [CaseRun::Unpatched, CaseRun::Restored] {
            assert_eq!(read(run, own, "ok", "", "subject").end, End::Completed);
        }
        let failed = |end: End| matches!(end, End::Failed { .. });
        // `ok` with a nonzero exit isn't completed.
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        let artifact = TestRun {
            package: own.0.into(),
            file: own.1.into(),
            tests: vec![(own.2.into(), "ok".into())],
            killed: false,
            exited_ok: false,
            log: String::new(),
            binary_source: Some("subject".into()),
            checkout_source: Some("subject".into()),
        };
        fs::write(
            dir.path().join(TEST_FILE),
            serde_json::to_string(&artifact).expect("a test run serializes"),
        )
        .expect("the temp dir is writable");
        let run = RunId::native("raw-budget-lease", CaseRun::Unpatched);
        assert!(failed(
            integrity_execution(dir.path(), run, &input, NODE, "OwnedMismatch", "subject").end
        ));
        // Another diagnostic, or a panic that only mentions this one.
        assert!(failed(
            read(
                CaseRun::Patched,
                own,
                "FAILED",
                &reported("ReservedMismatch"),
                "patched"
            )
            .end
        ));
        for mention in [
            "thread 'a_test' panicked: expected OwnedMismatch",
            "the run hit a harness failure: 's evidence failed: OwnedMismatch: x",
            "left: Some(SourceEvidence { source: \"storage:header\", detail: \"OwnedMismatch: x\" })",
            "storage:header's evidence failed: OwnedMismatch: at a quiet boundary",
        ] {
            assert!(
                failed(read(CaseRun::Patched, own, "FAILED", mention, "patched").end),
                "{mention}"
            );
        }
        // Matching markers that name no source aren't a match.
        for marker in ["unknown", "", NOT_EMBEDDED] {
            assert!(
                failed(
                    read(
                        CaseRun::Patched,
                        own,
                        "FAILED",
                        &reported("OwnedMismatch"),
                        marker
                    )
                    .end
                ),
                "{marker:?}"
            );
        }
        // The patched run from S's checkout, or a clean run from another.
        assert!(failed(
            read(
                CaseRun::Patched,
                own,
                "FAILED",
                &reported("OwnedMismatch"),
                "subject"
            )
            .end
        ));
        assert!(failed(
            read(CaseRun::Restored, own, "ok", "", "patched").end
        ));
        // One field off the history's test.
        for test in [
            ("xtask", own.1, own.2),
            (own.0, "tests/header_node_capacity.rs", own.2),
            (own.0, own.1, "another_test"),
        ] {
            assert!(
                failed(read(CaseRun::Unpatched, test, "ok", "", "subject").end),
                "{test:?}"
            );
            assert!(
                failed(
                    read(
                        CaseRun::Patched,
                        test,
                        "FAILED",
                        &reported("OwnedMismatch"),
                        "patched"
                    )
                    .end
                ),
                "{test:?}"
            );
        }
    }

    /// A planted run's replay links its case task's copy of that run's
    /// trace, by its property, name and role: another case's task or another
    /// role isn't it, and a `Draft` run has none yet.
    #[test]
    fn a_replay_links_its_run_s_trace() {
        let task = |name: &str, input: CaseInput| Task::Case {
            expected: ExpectedId::Passes {
                package: "p".into(),
                file: name.into(),
                test: name.into(),
            },
            property: "p".into(),
            name: name.into(),
            harness: MACHINE.into(),
            machine: true,
            input,
            patch: format!("qa/planted/p/{name}.patch"),
        };
        let (a, b) = (
            task(
                "a",
                CaseInput::Fuzz {
                    input: "a.bin".into(),
                },
            ),
            task(
                "a-b",
                CaseInput::Typed {
                    scenario: "s.json".into(),
                    plan: "p.bin".into(),
                },
            ),
        );
        let cases = BTreeMap::from([(("p", "a"), (3, &a)), (("p", "a-b"), (4, &b))]);
        let run = |name: &str, role| RunId::Case {
            property: "p".into(),
            name: name.into(),
            run: role,
        };
        let links = replay_links(&run("a", CaseRun::Patched), &cases);
        assert_eq!(
            links.artifacts,
            ["3/patched/trace.jsonl", "3/patched/witness.json"]
        );
        assert_eq!(links.history, Some(run("a", CaseRun::Patched)));
        assert_eq!(links.harness.as_deref(), Some(MACHINE));
        assert_eq!(links.input.as_deref(), Some("a.bin"));
        assert_eq!(links.plan, None);
        let typed = replay_links(&run("a-b", CaseRun::Patched), &cases);
        assert_eq!(typed.input.as_deref(), Some("s.json"));
        assert_eq!(typed.plan.as_deref(), Some("p.bin"));
        assert_eq!(
            replay_links(&run("a-b", CaseRun::Baseline), &cases).artifacts,
            ["4/baseline/trace.jsonl", "4/baseline/witness.json"]
        );
        assert!(replay_links(&run("a", CaseRun::Draft), &cases)
            .artifacts
            .is_empty());
        assert!(replay_links(&run("c", CaseRun::Patched), &cases)
            .artifacts
            .is_empty());
        let history = history_run();
        assert_eq!(
            replay_links(&history, &cases).harness.as_deref(),
            Some("header-sync/storage")
        );
    }

    /// The record exports what an exclusion rests on: a standing `Open`'s
    /// reason, owner and approval on its key's entry, and the contracts
    /// table with each contract's state.
    #[test]
    fn the_record_exports_exclusions() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let standing = manifest
            .expected
            .iter()
            .find(|result| {
                matches!(
                    result.expected,
                    Expected::Open {
                        until: None,
                        approval: super::super::Approval::Adam { .. },
                        ..
                    }
                )
            })
            .expect("the manifest has an approved standing Open");
        let key = standing.keys[0].clone();
        let evaluation = Evaluation {
            keys: BTreeMap::from([(
                key.clone(),
                ObligationState::Excluded {
                    why: "an approved standing Open".into(),
                },
            )]),
            gates: BTreeMap::new(),
            stray: Vec::new(),
        };
        let run = RunBinding {
            run_id: "run-1".into(),
            subject: "s".into(),
            subject_source: "source".into(),
            containment: Containment::Subreaper,
        };
        let record = |run: &RunBinding| {
            record(
                &manifest,
                &plan,
                Path::new("/runs/a"),
                run,
                None,
                &evaluation,
            )
        };
        // A best-effort run is never an acceptance record, even with nothing
        // blocked, and says why.
        let best_effort = record(&RunBinding {
            containment: Containment::BestEffort,
            ..run.clone()
        });
        assert!(!best_effort.accepted && !best_effort.fully_accepted);
        assert!(best_effort
            .stray
            .iter()
            .any(|stray| stray.contains("best-effort")));
        assert!("best-effort".parse::<Containment>().is_ok());
        assert!("none".parse::<Containment>().is_err());
        let record = record(&run);
        assert!(record.accepted && record.stray.is_empty());
        assert_eq!(record.containment, Containment::Subreaper);
        assert_eq!(record.artifacts, "/runs/a");
        let entry = &record.excluded[0];
        assert_eq!(entry.key, key);
        let Expected::Open {
            reason, approval, ..
        } = standing.expected
        else {
            unreachable!("found as an Open");
        };
        let open = entry.open.as_ref().expect("its standing Open is exported");
        assert_eq!(open.reason, reason);
        assert_eq!(open.approval, format!("{approval:?}"));
        assert_eq!(record.contracts.len(), manifest.contracts.len());
        assert!(record
            .contracts
            .iter()
            .all(|contract| !contract.state.is_empty()));
    }
}
