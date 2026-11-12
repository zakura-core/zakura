//! The acceptance tool's simulator side (hdr-14.7a and hdr-14.7b, item 9).
//!
//! `cargo xtask header-acceptance` can't link this crate, which builds only
//! under `--cfg zakura_sim`, so it drives the `header-acceptance` binary in
//! three steps around the children it runs itself:
//!
//! 1. [`plan`] turns the manifest into [`Task`]s: the tests to execute by
//!    name, the histories to declare and record, the planted cases to
//!    rebuild and record, and the integrity mutations to run. Every other
//!    expected result is listed with why the tool can't produce it, and
//!    stays blocked.
//! 2. xtask runs each task with a deadline and writes its artifacts under
//!    `<artifacts>/<task index>/`, as the contract below gives them.
//! 3. [`assemble`] reads those artifacts into the evaluator's
//!    [`ResultFile`], and [`record`] runs the evaluator and writes the
//!    record's three exports.
//!
//! # The runner's artifacts
//!
//! Every path below is under the task's directory `<i>`, which holds its
//! [`TASK_FILE`] first: the run's ID, as the root's [`RUN_FILE`] names it,
//! the task's index and the task itself ([`task_binding`]). A directory
//! whose task file names another run, such as one kept from an earlier
//! attempt, isn't read. A file a child didn't write is missing, and a child
//! killed at its deadline leaves [`KILLED_FILE`] in its directory. A
//! missing or killed artifact gives its execution no result, so it blocks.
//!
//! A *recorded run* is a directory `zakura-sim record --out` wrote:
//! `trace.jsonl`, `full-report.json` ([`FULL_REPORT_FILE`], C8), and
//! `witness.json` when it was recorded with `--witness`. The witness file
//! names what it judged: `trace`, the SHA-256 of that `trace.jsonl`,
//! `source`, the source digest its build embeds, and `draft`, the run's
//! `Draft` override, beside its `property`, `expect`, `also_violates`,
//! `passed` and `error`. The full report names its `trace` and `source`
//! the same way. A witness file or full report whose `trace`, `source` or
//! `draft` isn't the trace's beside it is no witness or report of that run.
//! Every artifact below is bound to what its reader expects by the one
//! check, [`binding::check`], which each artifact kind's declared fields go
//! through (Rule 25). Its *fresh replay*, where the contract asks for
//! one, is the directory [`REPLAY_DIR`] inside it, written by a separate
//! `zakura-sim replay <run> --report <run>/replay/full-report.json` with the
//! same binary that recorded the run: the replay's own `full-report.json`,
//! which names the replayed trace and that binary's source as the
//! recording's does, and [`REPLAY_FILE`], a [`ReplayRun`] holding how that
//! child ended and its output.
//!
//! * `test`: [`TEST_FILE`], a [`TestRun`] of the exact-name filter.
//! * `history`: the witness test run as for `test`, with
//!   [`HISTORIES_VAR`](super::history::HISTORIES_VAR) set to `<i>/declared`,
//!   so `<i>/declared` holds what it declared ([`super::history`]). Then
//!   `<i>/run`, the declaration whose case is the task's `case`, recorded by
//!   S's `zakura-sim record` with [`record_args`]: `--sim <harness>
//!   --scenario <scenario>`, where `<scenario>` is the JSON text its
//!   `scenario.json` holds, passed as one argument (the CLI parses
//!   `--scenario`'s value as JSON, not as a path), then `--plan <path of its
//!   plan.bin>` or `--script <path of its script.json>`, with `--witness
//!   <property> --expect <expect>` when the task names `expect`, and its
//!   fresh replay.
//! * `case`: the planted runner's `baseline` and `patched` recorded runs,
//!   copied whole, with the fresh replays it made of them (C4). The
//!   baseline is recorded with `--witness <property> --expect holds`, and
//!   the patched run with the task's `patched` witness, `--expect
//!   <expect>` and, if its `beside` isn't empty, `--also-violates <beside,
//!   comma-separated>`: `violated` beside the case's `also_violates`, or
//!   `violated-set` with a nested case's exact `set`. With the runs come
//!   [`PROVENANCE_FILE`], a [`ProvenanceCheck`] naming the `base` commit
//!   the rebuild's checkout was made from, which must be S, the SHA-256 of
//!   the patch it applied (`patch_sha256`), which must be the task's
//!   `patch` at S, and the `source` it measured there, [`VERDICT_FILE`], a
//!   [`VerdictCheck`]
//!   naming the `baseline` and `patched` traces it judged by SHA-256, and
//!   [`TARGET_FILE`], a [`TargetCheck`] naming the `source` of the patched
//!   build it ran, which must be the provenance check's, and, with a
//!   target, the SHA-256 of the committed input it ran (`input_sha256`) and
//!   of its own patched run's trace (`trace`), copied whole to
//!   [`CONFIRMATION_DIR`], whose meta must name that source and input and
//!   the case's `harness`, the one the target maps to.
//!   A node case's task names `draft`, its set: then `<i>/draft` is
//!   recorded by the case's own rebuilt binary with `zakura-sim record
//!   --sim <harness> <its input> --draft <set, comma-separated>` (C9),
//!   where its input is `--input <input>`, `--scenario <scenario> --plan
//!   <plan>` or `--scenario <scenario> --script <script>` as [`CaseInput`]
//!   names it, `--scenario` again taking the scenario file's JSON text and
//!   the others paths, with its fresh replay by the same binary. A case whose task
//!   names `companions` gets [`COMPANIONS_FILE`] in `<i>/draft` from the
//!   same rebuilt checkout's `header-acceptance companions --trace <i>/draft
//!   --out <i>/draft/companions.json`, a [`CompanionsRun`], whose `trace`
//!   and `source` must be that `Draft` trace's digest and build source as
//!   for a witness file.
//! * `integrity`: for each of `unpatched`, `patched` and `restored`, the
//!   history's witness test as for `history` in `<i>/<run>`, with
//!   [`TEST_FILE`] and `declared`, and `<i>/<run>/history`, the declaration
//!   named by the task's `case`, recorded by that checkout's own
//!   `zakura-sim` with no witness. `<i>/patched` also holds
//!   [`PATCHED_BUILD_FILE`], a [`PatchedBuild`] the runner measures once it
//!   has applied the patch: `base`, the commit its checkout was made from,
//!   which must be S, `patch_sha256`, the SHA-256 of the patch it applied,
//!   which must be the task's `patch_file` at S, and `source`, the patched
//!   checkout's source digest, which the patched run's test binary and
//!   checkout and its `history` recording must all name.
//!
//! Every recorded run's report and store snapshot come from its
//! `full-report.json`, never from the trace's `end` record, which keeps no
//! violation and no store, and the two must agree on every outcome. That
//! holds for an `integrity` run's `history` recording too, which owes no
//! fresh replay.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    binding::{self, known, Expect, Field},
    contracts::RunBy,
    evaluate::{
        evaluate, Actions, Actual, Companion, End, Evaluation, ExecMeta, Execution,
        ObligationState, Report, ResultFile, ResultRecord, Subject, TestOutcome, TestRan,
    },
    history::{declared, DeclaredHistory, Ran, PLAN_FILE, RAN_FILE, SCENARIO_FILE, SCRIPT_FILE},
    refs::TriggerRef,
    required::{required_keys, Inventories, GATE_OBLIGATIONS},
    rows::HEADER_HARNESSES,
    CaseRef, CaseRun, Expected, ExpectedId, Gate, Key, Manifest, RunId, COMPOSITION_CHECKS,
    NARROWING, REQUIRED_CONTRACTS,
};
use crate::{
    harness::coverage::{Bypass, COVERAGE},
    record::{FullReport, OutcomeKind},
    trace::{meta::PlanRef, Trace},
    PropertyOutcome,
};

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

/// What a task directory's [`TASK_FILE`] must hold: the ID of the run that
/// wrote it (`run_id`), its index in the plan and the plan's task itself,
/// so a directory is read only as the task this run's runner wrote it for,
/// never by its position alone, and never as another attempt's.
pub fn task_binding(run_id: &str, index: usize, task: &Task) -> serde_json::Value {
    serde_json::json!({ "run_id": run_id, "index": index, "task": task })
}

/// Whether `artifacts`' directory for `task`, at `index`, is the one the
/// run `run_id` wrote for it ([`task_binding`], [`binding::TASK`]).
fn task_bound(artifacts: &Path, run_id: &str, index: usize, task: &Task) -> bool {
    let expect = Expect::new()
        .with(Field::RunId, run_id)
        .with(Field::TaskIndex, index)
        .with(Field::Task, task);
    let path = artifacts.join(index.to_string()).join(TASK_FILE);
    binding::read(&binding::TASK, &path, &expect).is_ok()
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
/// A recorded run's fresh replay, the directory inside it.
pub const REPLAY_DIR: &str = "replay";
/// A fresh replay's own artifact: how its child ended ([`ReplayRun`]).
pub const REPLAY_FILE: &str = "replay.json";
/// A node case's `Draft` run's companions, judged by the case's own build
/// ([`CompanionsRun`]).
pub const COMPANIONS_FILE: &str = "companions.json";
pub use crate::record::FULL_REPORT_FILE;

/// What a fresh replay's [`REPLAY_FILE`] holds, as the runner writes it
/// from its child.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRun {
    /// Whether the child exited 0, which `zakura-sim replay` does only for a
    /// verified replay.
    pub exited_ok: bool,
    /// Whether it ended at its deadline.
    pub killed: bool,
    /// Its combined output, which names the trace it replayed.
    pub log: String,
}

/// What a `Draft` run's [`COMPANIONS_FILE`] holds: each companion's verdict
/// on that trace, from `header-acceptance companions` built from the case's
/// own patched checkout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompanionsRun {
    /// The digest of the trace it judged.
    pub trace: String,
    /// The source digest the judging binary embeds.
    pub source: String,
    /// Each companion, in its declared order.
    pub companions: Vec<CompanionVerdict>,
}

/// One companion's verdict in a [`CompanionsRun`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompanionVerdict {
    /// Its declared name.
    pub name: String,
    /// Whether it held.
    pub held: bool,
    /// Why not, if it didn't.
    pub why: Option<String>,
}

/// A planted case's artifact: its fuzz-target confirmation, from the
/// planted runner's `--targets` run.
pub const TARGET_FILE: &str = "target.json";
/// A planted case's artifact: the planted runner's own verdict on it, which
/// covers its campaign as well as its two traces.
pub const VERDICT_FILE: &str = "verdict.json";
/// A planted case's artifact: the patched run its `--targets` confirmation
/// recorded, copied whole, which its [`TARGET_FILE`] names.
pub const CONFIRMATION_DIR: &str = "confirmation";

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
    /// A history, declared by its witness test and recorded from that
    /// declaration (C18).
    History {
        /// The expected result it produces.
        expected: ExpectedId,
        /// The harness it runs on.
        harness: String,
        /// The property whose witness the recording runs.
        property: String,
        /// `--expect`'s value for that witness: `holds` for a `Holds`, and
        /// `not-applicable` for a `StaysNotApplicable`, whose target stays
        /// `NotApplicable` while its trigger history is reached. `None`
        /// records with no witness, which shows no trigger.
        expect: Option<String>,
        /// Its witness test's package.
        package: String,
        /// Its witness test's file.
        file: String,
        /// Its witness test's name.
        test: String,
        /// Its case within that test.
        case: String,
    },
    /// A planted case, rebuilt with its patch in its own target directory.
    Case {
        /// The expected result it's for.
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
        /// What its patched runs' witness expects, as its `.toml` names it.
        patched: PatchedWitness,
        /// The patch, relative to the repository root.
        patch: String,
        /// A node case's set, its `Draft` run's override (C9). `None` for a
        /// machine case, which has no `Draft` run.
        draft: Option<Vec<String>>,
        /// The companions judged on its `Draft` run, by name.
        companions: Vec<String>,
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
    /// A typed scenario and a scripted Plan, for a history no Plan bytes
    /// reach, such as one a test steers to.
    Scripted {
        /// The scenario, as the harness's JSON.
        scenario: String,
        /// The scripted Plan, as JSON.
        script: String,
    },
}

/// What a planted case's patched witness expects, as its `.toml` names it
/// and the planted runner passes it to `zakura-sim record`: the case's
/// property `violated` beside its `also_violates`, or, for a nested case
/// that names a `set`, `violated-set` with exactly that set (C6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchedWitness {
    /// `--expect`'s value: `violated` or `violated-set`.
    pub expect: String,
    /// `--also-violates`'s list, in the case file's order: its
    /// `also_violates` under `violated`, its `set` under `violated-set`.
    pub beside: Vec<String>,
}

/// What a planted case's `.toml` says its patched witness expects
/// ([`PatchedWitness`]). A case naming both `set` and `also_violates`, or
/// either as anything but a list of IDs, is refused, as the planted runner
/// refuses it. A `draft_set` case's patched runs break its property alone,
/// so they expect `violated` beside nothing: its set is judged on its
/// `Draft` run ([`super::Found::AtStepSetOnDraft`]).
fn patched_witness(repo: &Path, case: CaseRef) -> Result<PatchedWitness, String> {
    let file = repo
        .join("qa/planted")
        .join(case.property)
        .join(format!("{}.toml", case.name));
    let text = fs::read_to_string(&file).map_err(|error| format!("{}: {error}", file.display()))?;
    let table: toml::Table =
        toml::from_str(&text).map_err(|error| format!("{}: {error}", file.display()))?;
    let ids = |key: &str| -> Result<Vec<String>, String> {
        let Some(value) = table.get(key) else {
            return Ok(Vec::new());
        };
        value
            .as_array()
            .and_then(|ids| {
                ids.iter()
                    .map(|id| id.as_str().map(str::to_owned))
                    .collect::<Option<Vec<String>>>()
            })
            .ok_or_else(|| format!("{}: `{key}` isn't a list of IDs", file.display()))
    };
    match (ids("set")?, ids("also_violates")?) {
        (set, also) if !set.is_empty() && !also.is_empty() => Err(format!(
            "{}: a case names either `set` or `also_violates`, not both",
            file.display()
        )),
        (set, _) if !set.is_empty() => Ok(PatchedWitness {
            expect: "violated-set".to_owned(),
            beside: set,
        }),
        (_, also) => Ok(PatchedWitness {
            expect: "violated".to_owned(),
            beside: also,
        }),
    }
}

impl CaseInput {
    /// Its committed input, or a typed case's scenario.
    fn input(&self) -> &str {
        match self {
            CaseInput::Fuzz { input } => input,
            CaseInput::Typed { scenario, .. } | CaseInput::Scripted { scenario, .. } => scenario,
        }
    }

    /// A typed case's Plan file: its Plan bytes or its script.
    fn plan(&self) -> Option<&str> {
        match self {
            CaseInput::Fuzz { .. } => None,
            CaseInput::Typed { plan, .. } => Some(plan),
            CaseInput::Scripted { script, .. } => Some(script),
        }
    }
}

/// What a planted case's `.toml` runs: its `input`, or its `[typed]`
/// table's scenario and Plan, its `plan` bytes or its `script`. A case
/// names exactly one input, and a typed one exactly one Plan.
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
            let scenario = path(typed, "scenario")?;
            match (typed.get("plan"), typed.get("script")) {
                (Some(_), None) => Ok(CaseInput::Typed {
                    scenario,
                    plan: path(typed, "plan")?,
                }),
                (None, Some(_)) => Ok(CaseInput::Scripted {
                    scenario,
                    script: path(typed, "script")?,
                }),
                _ => Err(format!(
                    "{}: a typed case names either `plan` or `script`",
                    file.display()
                )),
            }
        }
        _ => Err(format!(
            "{}: a case names either `input` or `[typed]`",
            file.display()
        )),
    }
}

/// Turns `manifest`'s expected results into tasks: a `Holds` or
/// `StaysNotApplicable` becomes a history task, a planted case a case task
/// with its `Draft` run if it's a node one, and a `Replays` is read from
/// the task that records its run. A replay of a run no task records is
/// listed unsupported.
pub fn plan(manifest: &Manifest, repo: &Path) -> Result<RunPlan, String> {
    let mut tasks = Vec::new();
    let mut unsupported = Vec::new();
    let mut seen = BTreeSet::new();
    let mut replays = Vec::new();
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
            Expected::Holds {
                property, history, ..
            } => tasks.push(history_task(id, property, history, Some("holds"))),
            // A target that stays NotApplicable records its trigger history
            // under the `not-applicable` expectation.
            Expected::StaysNotApplicable {
                property, history, ..
            } => tasks.push(history_task(id, property, history, Some("not-applicable"))),
            Expected::Caught {
                case,
                harness,
                set,
                companions,
                ..
            } => {
                let mut task = case_task(repo, case, harness, false, id)?;
                if let Task::Case {
                    draft,
                    companions: named,
                    ..
                } = &mut task
                {
                    *draft = Some(set.iter().map(|id| (*id).to_owned()).collect());
                    *named = companions.iter().map(|name| (*name).to_owned()).collect();
                }
                tasks.push(task);
            }
            Expected::MachineCaught { case, harness } => {
                tasks.push(case_task(repo, case, harness, true, id)?);
            }
            // A harness-caught case has no Draft run: its patched run can't
            // complete.
            Expected::HarnessCaught { case, harness, .. } => {
                tasks.push(case_task(repo, case, harness, false, id)?);
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
            Expected::Replays { ref run } => replays.push((id, run.clone())),
            Expected::Open { .. } => {}
        }
    }
    for (id, run) in replays {
        if recorded_at(&tasks, &run).is_none() {
            unsupported.push((id, format!("no task records {run:?}")));
        }
    }
    Ok(RunPlan { tasks, unsupported })
}

/// A history's task, recording its witness of `property` with `expect`.
fn history_task(
    expected: ExpectedId,
    property: &str,
    history: super::HistoryRef,
    expect: Option<&str>,
) -> Task {
    Task::History {
        expected,
        harness: history.harness.to_owned(),
        property: property.to_owned(),
        expect: expect.map(str::to_owned),
        package: history.witness.package.to_owned(),
        file: history.witness.file.to_owned(),
        test: history.witness.test.to_owned(),
        case: history.case.to_owned(),
    }
}

/// Where in `tasks` execution `run` is recorded: its task's index and the
/// recorded run's directory within it, `run` for a history and the role's
/// name for a planted case's run. `None` if no task records it, as for a
/// native mutation's run, whose witness test isn't a recorded run.
pub fn recorded_at(tasks: &[Task], run: &RunId) -> Option<(usize, &'static str)> {
    tasks.iter().enumerate().find_map(|(index, task)| {
        let dir = match (task, run) {
            (Task::History { expected, .. }, RunId::History { .. })
                if history_of(expected) == Some(run) =>
            {
                "run"
            }
            (
                Task::Case {
                    property,
                    name,
                    draft,
                    ..
                },
                RunId::Case {
                    property: p,
                    name: n,
                    run: role,
                },
            ) if property == p && name == n => match role {
                CaseRun::Baseline => "baseline",
                CaseRun::Patched => "patched",
                CaseRun::Draft if draft.is_some() => "draft",
                _ => return None,
            },
            _ => return None,
        };
        Some((index, dir))
    })
}

/// The history a `Holds` or `StaysNotApplicable` identity names.
fn history_of(expected: &ExpectedId) -> Option<&RunId> {
    match expected {
        ExpectedId::Holds { run, .. } | ExpectedId::StaysNotApplicable { run, .. } => Some(run),
        _ => None,
    }
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
        patched: patched_witness(repo, case)?,
        patch: format!("qa/planted/{}/{}.patch", case.property, case.name),
        draft: None,
        companions: Vec::new(),
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

/// An integrity mutation's patched run's artifact: the build the runner
/// measured in its patched checkout ([`PatchedBuild`]).
pub const PATCHED_BUILD_FILE: &str = "build.json";

/// What [`PATCHED_BUILD_FILE`] holds, as the runner measures it once it has
/// applied the mutation's patch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchedBuild {
    /// The commit the patched checkout was made from, which must be S.
    pub base: String,
    /// The SHA-256 of the patch it applied, which must be the mutation's
    /// committed patch at S.
    pub patch_sha256: String,
    /// The patched checkout's source digest, which its test binary, its
    /// checkout and its recorded history must all name.
    pub source: String,
}

/// A planted case's provenance artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceCheck {
    /// The commit the patched checkout was made from, which must be S.
    pub base: String,
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
    /// The SHA-256 of the baseline `trace.jsonl` the verdict judged, which
    /// must be the case's `baseline` run's.
    pub baseline: String,
    /// The SHA-256 of the patched `trace.jsonl` it judged, which must be
    /// the case's `patched` run's.
    pub patched: String,
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
    /// The source digest of the patched build the confirmation ran, which
    /// must be the one the case's provenance check measured.
    pub source: String,
    /// The SHA-256 of the input the confirmation ran on its target, which
    /// must be the case's committed input, or `None` with no target.
    pub input_sha256: Option<String>,
    /// The SHA-256 of the confirmation run's `trace.jsonl`, which must be
    /// the one in [`CONFIRMATION_DIR`], or `None` with no target.
    pub trace: Option<String>,
    /// Why the case wasn't confirmed through its target, or `None` if it
    /// was or has none.
    pub refused: Option<String>,
}

/// A test result from its artifact: every test libtest reported, in the
/// package and file xtask ran, with the sources of its binary and of the
/// checkout it ran in, both of which the evaluator requires to be S's. A
/// killed child fails every test it ran, a child that didn't exit 0 fails
/// every test it reported `ok`, and an unreadable artifact is a result that
/// ran nothing.
fn test_actual(dir: &Path) -> Actual {
    let Some(run) = read_json::<TestRun>(&dir.join(TEST_FILE)) else {
        return Actual::Test {
            ran: Vec::new(),
            binary_source: None,
            checkout_source: None,
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
        checkout_source: run.checkout_source,
    }
}

/// The SHA-256 of `path`'s bytes in hex, or `missing` if it can't be read,
/// which names the one execution an artifact records.
fn digest(path: &Path) -> String {
    file_digest(path).unwrap_or_else(|| "missing".to_owned())
}

/// The SHA-256 of `path`'s bytes in hex, if it can be read.
fn file_digest(path: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    let bytes = fs::read(path).ok()?;
    Some(
        Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    )
}

/// `path` as `T`, or `None` if it's missing or malformed.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

/// `dir`'s `witness.json`, if it judged `dir`'s own trace, build and
/// `Draft` override ([`binding::WITNESS`]), or why not.
fn bound_witness(dir: &Path) -> Result<Value, String> {
    Expect::of_trace(dir)
        .and_then(|expect| binding::read(&binding::WITNESS, &dir.join("witness.json"), &expect))
        .map_err(|why| format!("its witness file isn't its trace's: {why}"))
}

/// A run's declared actions, from its recorded `trace` and `full` report
/// and how it ended. A machine's driver runs its whole input and stops only
/// at its first model mismatch (Rule 17's machine histories end as their
/// driver does), so a completed machine run carried out its input, a
/// stopped one a prefix of it, and any other end none it can show. A node
/// run's actions are derived from its bound scenario and judged on its
/// trace and stores ([`super::actions`]); `witnessed` says whether a
/// witness judgement of this trace passed. A harness neither knows is
/// `Incomplete`, so its result blocks.
fn actions(trace: &Trace, full: &FullReport, end: &End, witnessed: bool) -> Actions {
    let harness = trace.meta.sim.as_str();
    match crate::harness::find(harness).map(|entry| entry.machine) {
        Ok(true) => match end {
            End::Completed => Actions::Complete,
            End::Violation { .. } => Actions::Prefix,
            _ => Actions::Incomplete,
        },
        Ok(false) => match super::actions::declared(harness, &trace.meta.scenario) {
            Ok(declared) => super::actions::judge(&declared, trace, full, end, witnessed),
            Err(_) => Actions::Incomplete,
        },
        Err(_) => Actions::Incomplete,
    }
}

/// The full report (C8) in `at`'s [`FULL_REPORT_FILE`], read as the trace
/// reader reads its files (a regular file within
/// [`MAX_REPORT_BYTES`](crate::trace::MAX_REPORT_BYTES)), if it reports the
/// recorded run in `dir` ([`binding::FULL_REPORT`]). `at` is `dir` for the
/// recording's own report and its [`REPLAY_DIR`] for its fresh replay's.
fn bound_report(dir: &Path, at: &Path) -> Result<FullReport, String> {
    let path = at.join(FULL_REPORT_FILE);
    let value: Value = match crate::trace::read::read_bounded(&path, crate::trace::MAX_REPORT_BYTES)
    {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes)
            .map_err(|error| format!("{}: {error}", path.display()))?,
        Ok(None) => return Err(format!("{}: missing", path.display())),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    if let Err(why) = Expect::of_trace(dir)
        .and_then(|expect| binding::check(&binding::FULL_REPORT, &value, &expect))
    {
        return Err(format!(
            "{}: it isn't a report of {}'s trace: {why}",
            path.display(),
            dir.display()
        ));
    }
    serde_json::from_value(value).map_err(|error| format!("{}: {error}", path.display()))
}

/// The recorded run in `dir`: its trace, which the simulator's own reader
/// checks whole (its versions, every line's shape and order, its step
/// numbers and that its last line is a written `end`), and its full
/// report, which must report that trace ([`bound_report`]) and whose
/// outcomes must be the `end` record's, property by property.
fn recorded(dir: &Path) -> Result<(Trace, FullReport), String> {
    let trace = crate::trace::TraceReader::open(dir)
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    let end = trace
        .end
        .as_ref()
        .ok_or_else(|| format!("{}: no end record", dir.display()))?;
    let report = bound_report(dir, dir)?;
    let ended: Vec<(&str, &PropertyOutcome)> = end
        .properties
        .iter()
        .map(|found| (found.property.as_str(), &found.outcome))
        .collect();
    let reported: Vec<(&str, &PropertyOutcome)> = report
        .properties
        .iter()
        .map(|found| (found.property.as_str(), &found.outcome))
        .collect();
    if ended != reported {
        return Err(format!(
            "{}: its full report's outcomes aren't its end record's",
            dir.display()
        ));
    }
    Ok((trace, report))
}

/// One property's report, from its outcome in a full report.
fn report(outcome: &PropertyOutcome) -> Report {
    match outcome {
        PropertyOutcome::Held { applicable } => Report::Held {
            applicable: u64::from(*applicable),
        },
        PropertyOutcome::NotApplicable { reason } => Report::NotApplicable {
            reason: reason.name().into_owned(),
        },
        PropertyOutcome::Violated { .. } => Report::Violated,
    }
}

/// The execution `run` recorded in `dir`: its trace's end and meta, its
/// full report's outcomes (C8), its `Draft` override from `meta.draft`
/// (C9), and its witness result. A missing or refused trace or report, or
/// a killed child, is a run that didn't happen. A completed end is the
/// trace's `idle` or `budget` outcome, which C1's completion decides, and
/// every result still depends on C1.
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
    let (recorded, full) = match recorded(dir) {
        Ok(read) => read,
        Err(why) => return failed(why),
    };
    let end = recorded
        .end
        .as_ref()
        .expect("`recorded` refuses a trace with no end");
    let end_kind = match end.outcome {
        OutcomeKind::Violation => End::Violation {
            property: end
                .violation
                .as_ref()
                .map(|violation| violation.property.clone())
                .unwrap_or_default(),
        },
        OutcomeKind::Harness => End::HarnessFailure {
            diagnostic: end.harness.clone().unwrap_or_default(),
        },
        OutcomeKind::Idle | OutcomeKind::Budget => End::Completed,
    };
    // Only a witness of this trace shows its trigger.
    let witnessed = bound_witness(dir)
        .ok()
        .and_then(|witness| witness.get("passed")?.as_bool())
        .unwrap_or(false);
    let actions = actions(&recorded, &full, &end_kind, witnessed);
    Execution {
        trace,
        run,
        meta: ExecMeta {
            harness: recorded.meta.sim.clone(),
            draft_override: recorded.meta.draft.clone(),
            run_by,
            input: None,
        },
        end: end_kind,
        reports: full
            .properties
            .iter()
            .map(|found| (found.property.clone(), report(&found.outcome)))
            .collect(),
        trigger: witnessed,
        // A usize count always fits in a u64 on supported targets.
        leaked: end.cleanup.leaked as u64,
        actions,
    }
}

/// The execution `run` recorded in `dir`, by `run_by` on `harness`, as
/// [`assemble`] reads every recorded run before its role's own checks.
pub fn read_execution(dir: &Path, run: RunId, harness: &str, run_by: RunBy) -> Execution {
    execution(dir, run, harness, run_by)
}

/// The fresh replay (C3, C8) of the recorded run `run` in `dir`, from its
/// [`REPLAY_DIR`]: it's exact only if its child exited 0, wasn't killed
/// and said it replayed this trace with every pick, draw and observation
/// matched ([`binding::REPLAY_LOG`]). Its full report, which must report `dir`'s trace
/// ([`bound_report`]), is compared with the recording's, outcomes and
/// violations apart from the store snapshot, so a changed stored fact fails
/// it even when every report is the same. Its meta is the recording's: the
/// replay reruns the meta's harness with its `Draft` override.
fn replay_actual(dir: &Path, run: RunId, run_by: RunBy) -> Actual {
    let of = digest(&dir.join("trace.jsonl"));
    let replay = dir.join(REPLAY_DIR);
    let ran = read_json::<ReplayRun>(&replay.join(REPLAY_FILE));
    let recorded = recorded(dir).ok();
    let meta = ExecMeta {
        harness: recorded
            .as_ref()
            .map(|(trace, _)| trace.meta.sim.clone())
            .unwrap_or_default(),
        draft_override: recorded
            .as_ref()
            .map(|(trace, _)| trace.meta.draft.clone())
            .unwrap_or_default(),
        run_by,
        input: None,
    };
    let expect = Expect::new().with_read(
        Field::Trace,
        file_digest(&dir.join("trace.jsonl")),
        "the recorded trace can't be read",
    );
    let exact = ran.as_ref().is_some_and(|ran| {
        ran.exited_ok
            && !ran.killed
            && binding::replay_claims(&ran.log)
                .iter()
                .any(|claim| binding::check(&binding::REPLAY_LOG, claim, &expect).is_ok())
    });
    let (reports_equal, stores_equal) = match (
        recorded.as_ref().map(|(_, report)| report),
        bound_report(dir, &replay),
    ) {
        (Some(original), Ok(replayed)) => (
            original.properties == replayed.properties,
            original.snapshot == replayed.snapshot,
        ),
        _ => (false, false),
    };
    Actual::Replay {
        run,
        of,
        meta,
        exact,
        reports_equal,
        stores_equal,
        fresh: ran.is_some(),
    }
}

/// Whether `failure`, a harness failure's `Display` form, is the source
/// evidence failure `diagnostic`: `<source>'s evidence failed:
/// <diagnostic>: <detail>`
/// ([`HarnessFailure::SourceEvidence`](crate::end::HarnessFailure::SourceEvidence)),
/// whose detail leads with the failure's name. A text that only mentions
/// the name isn't one.
fn fails_with(failure: &str, diagnostic: &str) -> bool {
    let detail = format!("'s evidence failed: {diagnostic}: ");
    failure.find(&detail).is_some_and(|at| at > 0)
}

/// Whether `log` holds the harness's own report of `diagnostic`. A harness
/// failure ends `run_prepared` with [`HarnessError::Run`], and the witness
/// panics with its `Display` form, `the run hit a harness failure:
/// <failure>`, where the failure is [`fails_with`]'s.
///
/// [`HarnessError::Run`]: crate::harness::HarnessError::Run
fn reports_diagnostic(log: &str, diagnostic: &str) -> bool {
    log.lines().any(|line| {
        line.split_once("the run hit a harness failure: ")
            .is_some_and(|(_, rest)| fails_with(rest, diagnostic))
    })
}

/// An integrity mutation's history run in `dir`. Its end comes from its
/// test artifact: a passing test is a completed run, a failing one whose
/// output reports `diagnostic` ([`reports_diagnostic`]) is that harness
/// failure, and anything else didn't happen. The artifact must be the
/// history's own test, from a binary embedding the checkout it ran in.
///
/// The run claims `input` as its history only if the test declared it
/// exactly once ([`declaration`]), and its reports come from that
/// declaration recorded in `dir/history` by the same checkout (C18, C8),
/// whose trace must bind to the declaration and end as the test did: a
/// harness failure reporting `diagnostic` for a failing test, completed for
/// a passing one. A test reports no property outcomes, so without that
/// recording the run claims no history and its whole-report check fails.
/// Its `Draft` override and declared actions are that recording's too, so
/// a history recorded with an override, or whose actions didn't all run,
/// is judged as it ran, and without the recording its actions are
/// incomplete. Its trigger history is the test's own assertions, which
/// passed only if it did.
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
        case,
        ..
    } = input
    else {
        unreachable!("an integrity run's input is a history");
    };
    let artifact = read_json::<TestRun>(&dir.join(TEST_FILE));
    let mut end = match &artifact {
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
    let trigger = end == End::Completed;
    let mut claimed = None;
    let mut reports = BTreeMap::new();
    let mut leaked = 0;
    let mut draft_override = Vec::new();
    let mut actions = Actions::Incomplete;
    if matches!(end, End::Completed | End::HarnessFailure { .. }) {
        let history = dir.join("history");
        let bound = declaration(&dir.join("declared"), harness, name, case).and_then(|at| {
            match declared_refusal(harness, &at, &history) {
                Some(why) => Err(why),
                None => Ok(()),
            }
        });
        let checkout = artifact
            .as_ref()
            .and_then(|test| test.checkout_source.clone());
        let recorded = execution(&history, run.clone(), harness, RunBy::Tool);
        let agrees = match (&end, &recorded.end) {
            (End::Completed, End::Completed) => Ok(()),
            (
                End::HarnessFailure { .. },
                End::HarnessFailure {
                    diagnostic: failure,
                },
            ) if fails_with(failure, diagnostic) => Ok(()),
            (_, other) => Err(format!("its recorded history ended {other:?}")),
        };
        let expect =
            Expect::new().with_read(Field::Source, checkout, "its checkout wasn't measured");
        let from_checkout = binding::check_trace(&history, &expect).map_err(|why| {
            format!("its recorded history isn't from the checkout its test ran in: {why}")
        });
        match bound.and(agrees).and(from_checkout) {
            Ok(()) => {
                claimed = Some(input.clone());
                reports = recorded.reports;
                leaked = recorded.leaked;
                draft_override = recorded.meta.draft_override;
                actions = recorded.actions;
            }
            Err(why) if patched => end = End::Failed { why },
            Err(_) => {}
        }
    }
    Execution {
        trace: digest(&dir.join(TEST_FILE)),
        run,
        meta: ExecMeta {
            harness: harness.to_owned(),
            draft_override,
            run_by: RunBy::Tool,
            input: claimed,
        },
        end,
        reports,
        trigger,
        leaked,
        actions,
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
/// [`RUN_FILE`] must be `run`'s and the plan the one S's manifest gives.
/// Each `Replays` is read from the task directory that records its run.
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
    let bound = |index: usize, task: &Task| task_bound(artifacts, &run.run_id, index, task);
    for (index, task) in plan.tasks.iter().enumerate() {
        let dir = artifacts.join(index.to_string());
        // A directory the runner didn't write for this task gives it no
        // result, so it blocks.
        if !bound(index, task) {
            continue;
        }
        let actual = match task {
            Task::Test { .. } => test_actual(&dir),
            Task::History {
                expected,
                harness,
                property,
                expect,
                package,
                file,
                test,
                case,
            } => Actual::Run(history_execution(
                &dir,
                history_of(expected)
                    .expect("a history task's result names its history")
                    .clone(),
                harness,
                (property, expect.as_deref()),
                (package, file, test, case),
                subject_source,
            )),
            Task::Case {
                expected,
                property,
                name,
                harness,
                machine,
                input,
                patched: witness,
                patch,
                draft,
                companions,
            } => {
                let case = |run| RunId::Case {
                    property: property.clone(),
                    name: name.clone(),
                    run,
                };
                let checks = CaseChecks {
                    repo,
                    dir: &dir,
                    property,
                    harness,
                    input,
                    subject_source,
                };
                // The planted runner runs a case's baseline and patched
                // runs (C4), and xtask copies its traces here.
                let mut baseline =
                    checks.planted(case(CaseRun::Baseline), "baseline", "holds", &[]);
                let mut patched = checks.planted(
                    case(CaseRun::Patched),
                    "patched",
                    &witness.expect,
                    &witness.beside,
                );
                credit_sources(&dir, subject_source, &mut baseline, &mut patched);
                if let Some(why) = rebuild_refusal(repo, patch, subject, &dir) {
                    fail(&mut patched, why);
                }
                // The runner's own verdict covers what the traces don't, its
                // campaign, and item 9 runs `--targets` for a case with a
                // fuzz target, so its catch needs both, through the target
                // the committed mapping gives its harness.
                if let Some(why) =
                    mapped_case_refusal(&targets, repo, &dir, (property, name), harness, input)
                {
                    fail(&mut patched, why);
                }
                match (machine, draft) {
                    (true, _) => Actual::MachineCaught { baseline, patched },
                    (false, None) if matches!(expected, ExpectedId::HarnessCaught(_)) => {
                        Actual::HarnessCaught { baseline, patched }
                    }
                    (false, Some(_)) => {
                        let draft = checks.draft(case(CaseRun::Draft));
                        let companions = companions_of(&dir, &draft, companions);
                        Actual::Caught {
                            baseline,
                            patched,
                            draft,
                            companions,
                        }
                    }
                    (false, None) => continue,
                }
            }
            Task::Integrity {
                patch,
                patch_file,
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
                let read = |name: &str, which| {
                    integrity_execution(
                        &dir.join(name),
                        run(which),
                        &input,
                        harness,
                        diagnostic,
                        subject_source,
                    )
                };
                let mut patched = read("patched", CaseRun::Patched);
                if let Some(why) =
                    patched_build_refusal(repo, patch_file, subject, &dir.join("patched"))
                {
                    fail(&mut patched, why);
                }
                Actual::IntegrityCaught {
                    unpatched: read("unpatched", CaseRun::Unpatched),
                    patched,
                    restored: read("restored", CaseRun::Restored),
                }
            }
        };
        results.push(ResultRecord {
            expected: task_expected(task).clone(),
            commit: subject.to_owned(),
            actual,
        });
    }
    // Each replay comes from the recorded run's own directory, by whoever
    // ran that run: the planted runner replays its own (C4).
    let mut seen = BTreeSet::new();
    for result in &Manifest::committed().expected {
        let (Expected::Replays { run: replayed }, Some(id)) =
            (&result.expected, result.expected.id())
        else {
            continue;
        };
        if !seen.insert(id.clone()) {
            continue;
        }
        let Some((index, role)) = recorded_at(&plan.tasks, replayed) else {
            continue;
        };
        if !bound(index, &plan.tasks[index]) {
            continue;
        }
        let run_by = match role {
            "baseline" | "patched" => RunBy::Planted,
            _ => RunBy::Tool,
        };
        results.push(ResultRecord {
            expected: id,
            commit: subject.to_owned(),
            actual: replay_actual(
                &artifacts.join(index.to_string()).join(role),
                replayed.clone(),
                run_by,
            ),
        });
    }
    ResultFile {
        subject: subject.to_owned(),
        subject_source: subject_source.to_owned(),
        results,
    }
}

/// The expected result `task` produces.
fn task_expected(task: &Task) -> &ExpectedId {
    match task {
        Task::Test { expected, .. }
        | Task::History { expected, .. }
        | Task::Case { expected, .. }
        | Task::Integrity { expected, .. } => expected,
    }
}

/// What a planted case's runs are checked against.
struct CaseChecks<'a> {
    /// The repository at S.
    repo: &'a Path,
    /// The case task's directory.
    dir: &'a Path,
    /// The case's property directory.
    property: &'a str,
    /// The harness its input runs on.
    harness: &'a str,
    /// Its committed input.
    input: &'a CaseInput,
    /// S's source digest.
    subject_source: &'a str,
}

impl CaseChecks<'_> {
    /// The planted runner's run `name` of the case, as `run`: it must have
    /// run the case's committed input (C18), and its witness file must judge
    /// the case's property expecting `expect` beside exactly `beside`
    /// ([`witness_judges`]).
    fn planted(&self, run: RunId, name: &str, expect: &str, beside: &[String]) -> Execution {
        let dir = self.dir.join(name);
        let mut execution = execution(&dir, run, self.harness, RunBy::Planted);
        if let Some(why) = case_input_refusal(self.repo, self.harness, self.input, &dir) {
            fail(&mut execution, why);
        }
        if !witness_judges(&dir, self.property, expect, beside) {
            execution.trigger = false;
            fail(
                &mut execution,
                format!(
                    "its witness file doesn't judge {} expecting {expect} beside {beside:?}",
                    self.property
                ),
            );
        }
        execution
    }

    /// The case's `Draft` run, as `run`, which the tool records itself with
    /// the case's rebuilt binary (C9): it must have run the committed input,
    /// from the binary the provenance check measured, which must have
    /// passed. Its override is read from its meta, and the evaluator checks
    /// it's the case's set.
    fn draft(&self, run: RunId) -> Execution {
        let dir = self.dir.join("draft");
        let mut execution = execution(&dir, run, self.harness, RunBy::Tool);
        if let Some(why) = case_input_refusal(self.repo, self.harness, self.input, &dir) {
            fail(&mut execution, why);
        }
        let measured = measured_source(self.dir);
        if let Some(why) = provenance_refusal(self.dir) {
            fail(&mut execution, why);
        } else if let Err(why) = if measured.as_deref() == Some(self.subject_source) {
            Err("the measured patched checkout is S's own".to_owned())
        } else {
            binding::check_trace(&dir, &measured_expect(measured))
        } {
            fail(
                &mut execution,
                format!("its trace's build source isn't the measured patched checkout's: {why}"),
            );
        }
        execution
    }
}

/// The companions judged on `draft`, the case's `Draft` run in `dir`, from
/// its [`COMPANIONS_FILE`] ([`binding::COMPANIONS`]). They count only from
/// the case's own build, the source its provenance check measured, and
/// only for the `Draft` trace beside them, so a file from any other build
/// or for any other trace shows none. A case with no declared companions
/// reads none.
fn companions_of(dir: &Path, draft: &Execution, declared: &[String]) -> Vec<Companion> {
    if declared.is_empty() {
        return Vec::new();
    }
    let draft_dir = dir.join("draft");
    let run = Expect::of_trace(&draft_dir)
        .map(|expect| expect.with_read(Field::Source, measured_source(dir), "no provenance check"))
        .and_then(|expect| {
            binding::read(
                &binding::COMPANIONS,
                &draft_dir.join(COMPANIONS_FILE),
                &expect,
            )
        })
        .and_then(|artifact| {
            serde_json::from_value::<CompanionsRun>(artifact).map_err(|error| error.to_string())
        });
    let Ok(run) = run else {
        return Vec::new();
    };
    run.companions
        .into_iter()
        .map(|verdict| Companion {
            name: verdict.name,
            trace: run.trace.clone(),
            run: draft.run.clone(),
            held: verdict.held,
        })
        .collect()
}

/// A history task's run, `run`, in `dir`: its witness test (`test`, as
/// package, file, name and case) must have passed at S from a binary
/// embedding S, in a checkout of S, and declared the history once
/// ([`declaration`]); then `dir/run`, S's recording of that declaration,
/// must bind to it (C18) and come from S's build. Its trigger comes only
/// from a `witness.json` that judges the task's own `witness`, its property
/// and expectation, as a planted run's must ([`CaseChecks::planted`]): one
/// judging another property or expectation fails the run, and a task with
/// no expectation shows no trigger.
fn history_execution(
    dir: &Path,
    run: RunId,
    harness: &str,
    witness: (&str, Option<&str>),
    test: (&str, &str, &str, &str),
    subject_source: &str,
) -> Execution {
    let (package, file, name, case) = test;
    let recording = dir.join("run");
    let mut execution = execution(&recording, run, harness, RunBy::Tool);
    match witness {
        (property, Some(expect)) if !witness_judges(&recording, property, expect, &[]) => {
            execution.trigger = false;
            fail(
                &mut execution,
                format!("its witness file doesn't judge {property} expecting {expect}"),
            );
        }
        (_, None) => execution.trigger = false,
        _ => {}
    }
    let passed = read_json::<TestRun>(&dir.join(TEST_FILE)).is_some_and(|ran| {
        ran.package == package
            && ran.file == file
            && !ran.killed
            && ran.exited_ok
            && known(Some(subject_source))
            && ran.binary_source.as_deref() == Some(subject_source)
            && ran.checkout_source.as_deref() == Some(subject_source)
            && ran.tests == [(name.to_owned(), "ok".to_owned())]
    });
    if !passed {
        fail(
            &mut execution,
            format!("its witness test {package}/{file}::{name} didn't pass at S"),
        );
    }
    match declaration(&dir.join("declared"), harness, name, case) {
        Ok(at) => {
            if let Some(why) = declared_refusal(harness, &at, &recording) {
                fail(&mut execution, why);
            }
        }
        Err(why) => fail(&mut execution, why),
    }
    let errors = inventory_errors(
        &inventory_cases(package, file, name),
        &declared(&dir.join("declared")),
        name,
    );
    if !errors.is_empty() {
        fail(&mut execution, errors.join("; "));
    }
    if let Err(why) = binding::check_trace(
        &recording,
        &Expect::new().with(Field::Source, subject_source),
    ) {
        fail(
            &mut execution,
            format!("its trace's build source isn't the subject's: {why}"),
        );
    }
    execution
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
    if let Err(why) = binding::check_trace(
        &dir.join("baseline"),
        &Expect::new().with(Field::Source, subject_source),
    ) {
        fail(
            baseline,
            format!("its trace's build source isn't the subject's: {why}"),
        );
    }
    if let Some(why) = provenance_refusal(dir) {
        fail(patched, why);
    } else if let Err(why) =
        binding::check_trace(&dir.join("patched"), &measured_expect(measured_source(dir)))
    {
        fail(
            patched,
            format!("its trace's build source isn't the measured patched checkout's: {why}"),
        );
    }
}

/// The source case `dir`'s provenance check measured in its patched
/// checkout, if it has one.
fn measured_source(dir: &Path) -> Option<String> {
    read_json::<ProvenanceCheck>(&dir.join(PROVENANCE_FILE)).map(|check| check.source)
}

/// What a run of the measured patched checkout `measured` must name as its
/// build: that source, and none without a measurement.
fn measured_expect(measured: Option<String>) -> Expect {
    Expect::new().with_read(Field::Source, measured, "no provenance check measured it")
}

/// Why the planted runner didn't credit case `name` of `property` in `dir`:
/// it didn't report it caught, or it wasn't confirmed through `target`,
/// the fuzz target its `harness` maps to, if any, or either check is
/// missing, names another case or another target, or judged runs other
/// than this attempt's ([`verdict_refusal`], [`target_refusal`]).
fn case_refusal(
    repo: &Path,
    dir: &Path,
    (property, name): (&str, &str),
    (harness, target): (&str, Option<&str>),
    input: &CaseInput,
) -> Option<String> {
    verdict_refusal(dir, property, name)
        .or_else(|| target_refusal(repo, dir, (property, name), (harness, target), input))
}

/// Why `dir`'s [`VERDICT_FILE`] isn't the planted runner's catch of case
/// `name` of `property` on `dir`'s own runs: it's missing, refused, names
/// another case, or judged a baseline or patched trace other than the one
/// beside it, such as an older attempt's ([`binding::VERDICT`]).
fn verdict_refusal(dir: &Path, property: &str, name: &str) -> Option<String> {
    let trace = |run: &str| file_digest(&dir.join(run).join("trace.jsonl"));
    let expect = Expect::new()
        .with(Field::Property, property)
        .with(Field::Case, name)
        .with_read(
            Field::BaselineTrace,
            trace("baseline"),
            "the case has no baseline trace",
        )
        .with_read(
            Field::PatchedTrace,
            trace("patched"),
            "the case has no patched trace",
        );
    let check =
        binding::read(&binding::VERDICT, &dir.join(VERDICT_FILE), &expect).and_then(|artifact| {
            serde_json::from_value::<VerdictCheck>(artifact)
                .map_err(|error| format!("its planted verdict: {error}"))
        });
    match check {
        Ok(check) => check.refused,
        Err(why) => Some(why),
    }
}

/// Why `dir`'s [`TARGET_FILE`] isn't case `name` of `property`'s
/// confirmation through `target`: it's missing, refused, names another case
/// or target, or ran a build other than the one the case's provenance check
/// measured. With a target, it must also name the case's committed `input`
/// and its own run in [`CONFIRMATION_DIR`], whose trace must have run that
/// input on `harness`, the harness `target` maps to, from that build.
/// Without one, it names no input or run ([`binding::TARGET`]).
fn target_refusal(
    repo: &Path,
    dir: &Path,
    (property, name): (&str, &str),
    (harness, target): (&str, Option<&str>),
    input: &CaseInput,
) -> Option<String> {
    let run = dir.join(CONFIRMATION_DIR);
    let ran = match (target, input) {
        (None, _) => None,
        (Some(_), CaseInput::Fuzz { input }) => match committed_input(repo, input) {
            Ok(ran) => Some((input, ran)),
            Err(why) => return Some(why),
        },
        (Some(_), _) => {
            return Some("a typed case has no fuzz input for its target to run".to_owned())
        }
    };
    let expect = match &ran {
        None => Ok(Expect::new()
            .with(Field::Trace, Value::Null)
            .with(Field::InputSha256, Value::Null)),
        Some((_, ran)) => {
            Expect::of_trace(&run).map(|expect| expect.with(Field::InputSha256, ran.sha256.clone()))
        }
    }
    .map(|expect| {
        expect
            .with(Field::Property, property)
            .with(Field::Case, name)
            .with(Field::Target, target)
            .with_read(Field::Source, measured_source(dir), "no provenance check")
    });
    let check = expect
        .and_then(|expect| binding::read(&binding::TARGET, &dir.join(TARGET_FILE), &expect))
        .and_then(|artifact| {
            serde_json::from_value::<TargetCheck>(artifact)
                .map_err(|error| format!("its target confirmation: {error}"))
        });
    let check = match check {
        Ok(check) => check,
        Err(why) => return Some(format!("its target confirmation isn't this case's: {why}")),
    };
    if let Some(why) = check.refused {
        return Some(why);
    }
    // A run of another harness on the same build and input confirms
    // nothing through this target.
    let (input, ran) = ran?;
    binding::check_trace(&run, &ran.expect().with(Field::Harness, harness))
        .err()
        .map(|why| format!("its target confirmation's run didn't run {input} on {harness}: {why}"))
}

/// A committed fuzz input as a trace's meta names it.
struct CommittedInput {
    /// Its SHA-256 in hex.
    sha256: String,
    /// Its length.
    len: usize,
}

impl CommittedInput {
    /// What a trace that ran it must name ([`binding::TRACE_META`]).
    fn expect(&self) -> Expect {
        Expect::new()
            .with(Field::InputSha256, &self.sha256)
            .with(Field::InputLen, self.len)
    }
}

/// The committed fuzz input at `input`, a path under `repo`, or why it
/// can't be read.
fn committed_input(repo: &Path, input: &str) -> Result<CommittedInput, String> {
    use sha2::{Digest, Sha256};
    let bytes = fs::read(repo.join(input))
        .map_err(|_| format!("the committed input {input} can't be read"))?;
    Ok(CommittedInput {
        sha256: Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        len: bytes.len(),
    })
}

/// [`case_refusal`] for case `case`, `(property, name)`, of `input` on
/// `harness`, through the target `targets`, the committed mapping, gives
/// its harness. A mapping that can't be read refuses every case, so a
/// confirmation naming no target never stands in for one the mapping would
/// require.
fn mapped_case_refusal(
    targets: &Result<BTreeMap<String, String>, String>,
    repo: &Path,
    dir: &Path,
    case: (&str, &str),
    harness: &str,
    input: &CaseInput,
) -> Option<String> {
    match targets {
        Ok(targets) => case_refusal(
            repo,
            dir,
            case,
            (harness, target_of(targets, harness).as_deref()),
            input,
        ),
        Err(why) => Some(format!("the fuzz-target mapping can't be read: {why}")),
    }
}

/// Each fuzz target and the harness it runs, from the fuzz package's
/// `[package.metadata.zakura-sim.targets]` table under `repo`, as the
/// planted runner reads it, or why it can't be read: the manifest is
/// missing or isn't TOML, it has no such table, or a target maps to
/// something other than a harness name. A harness the table doesn't name
/// has no target, which differs from a mapping that can't be read, under
/// which no case's target can be confirmed.
fn fuzz_targets(repo: &Path) -> Result<BTreeMap<String, String>, String> {
    let path = repo.join("qa/fuzz/sim/Cargo.toml");
    let text = fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let manifest = toml::from_str::<toml::Table>(&text)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let targets = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("zakura-sim"))
        .and_then(|z| z.get("targets"))
        .and_then(toml::Value::as_table)
        .ok_or_else(|| {
            format!(
                "{} has no [package.metadata.zakura-sim.targets] table",
                path.display()
            )
        })?;
    targets
        .iter()
        .map(|(target, harness)| {
            harness
                .as_str()
                .map(|harness| (target.clone(), harness.to_owned()))
                .ok_or_else(|| {
                    format!(
                        "{}: {target} maps to {harness:?}, not a harness name",
                        path.display()
                    )
                })
        })
        .collect()
}

/// The fuzz target that runs `harness` in `targets`, if one does.
fn target_of(targets: &BTreeMap<String, String>, harness: &str) -> Option<String> {
    targets
        .iter()
        .find(|(_, runs)| runs.as_str() == harness)
        .map(|(target, _)| target.clone())
}

/// Why the integrity mutation's patched run in `dir` isn't S with its
/// committed patch `patch_file` (a path under `repo`) applied: its
/// [`PatchedBuild`] is missing, names a base other than `subject` or a
/// patch other than `patch_file`'s ([`binding::PATCHED_BUILD`]), or its
/// test binary, checkout or recorded history doesn't name the source it
/// measured. A previous patched directory for the same mutation, from
/// another source, then never counts.
fn patched_build_refusal(
    repo: &Path,
    patch_file: &str,
    subject: &str,
    dir: &Path,
) -> Option<String> {
    let expect = Expect::new().with(Field::Base, subject).with_read(
        Field::PatchSha256,
        file_digest(&repo.join(patch_file)),
        "its committed patch can't be read",
    );
    let build = binding::read(
        &binding::PATCHED_BUILD,
        &dir.join(PATCHED_BUILD_FILE),
        &expect,
    )
    .and_then(|artifact| {
        serde_json::from_value::<PatchedBuild>(artifact)
            .map_err(|error| format!("its patched build measurement: {error}"))
    });
    let build = match build {
        Ok(build) => build,
        Err(why) => {
            return Some(format!(
                "its patched checkout isn't S plus {patch_file}: {why}"
            ))
        }
    };
    let measured = Some(build.source.as_str());
    let test = read_json::<TestRun>(&dir.join(TEST_FILE));
    let ran = test.as_ref().is_some_and(|test| {
        test.binary_source.as_deref() == measured && test.checkout_source.as_deref() == measured
    });
    let recorded = binding::check_trace(
        &dir.join("history"),
        &Expect::new().with(Field::Source, &build.source),
    );
    if !known(measured) || !ran || recorded.is_err() {
        return Some(format!(
            "its test and recorded history aren't from the measured patched source {}",
            build.source
        ));
    }
    None
}

/// Why case `dir`'s provenance check isn't for S, `subject`, with its
/// committed patch at `patch` (a path under `repo`) applied: its checkout
/// was made from another commit, its recorded SHA-256 is another patch's,
/// or either can't be read ([`binding::PROVENANCE`]). An older checkout
/// with the same patch then never counts as S's catch.
fn rebuild_refusal(repo: &Path, patch: &str, subject: &str, dir: &Path) -> Option<String> {
    let Some(committed) = file_digest(&repo.join(patch)) else {
        return Some(format!("the committed patch {patch} can't be read"));
    };
    let expect = Expect::new()
        .with(Field::Base, subject)
        .with(Field::PatchSha256, committed);
    binding::read(&binding::PROVENANCE, &dir.join(PROVENANCE_FILE), &expect)
        .and_then(|artifact| {
            serde_json::from_value::<ProvenanceCheck>(artifact)
                .map_err(|error| format!("its provenance check: {error}"))
        })
        .err()
        .map(|why| format!("its rebuild isn't S plus {patch}: {why}"))
}

/// Why the trace in `dir` didn't run the committed fuzz input at `input` (a
/// path under `repo`): its meta's `input` names another SHA-256 or length,
/// or either can't be read ([`binding::TRACE_META`]).
fn input_refusal(repo: &Path, input: &str, dir: &Path) -> Option<String> {
    let committed = match committed_input(repo, input) {
        Ok(committed) => committed,
        Err(why) => return Some(why),
    };
    binding::check_trace(dir, &committed.expect())
        .err()
        .map(|why| format!("it didn't run the committed input {input}: {why}"))
}

/// A typed Plan file's reference as `meta.plan` names it: Plan bytes by
/// their own digest, a script by the Plan it parses to ([`PlanRef::script`]),
/// so a committed script binds however it's formatted.
fn plan_ref(path: &Path, script: bool) -> Result<PlanRef, String> {
    let bytes = fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if script {
        let plan: crate::Plan = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{} isn't a Plan: {error}", path.display()))?;
        Ok(PlanRef::script(&plan))
    } else {
        Ok(PlanRef::bytes(&bytes))
    }
}

/// Why the trace in `dir` isn't a run of `scenario_file`'s scenario under
/// the Plan in `plan_file` (C18): its harness isn't `harness`, its meta's
/// scenario isn't the one `harness` describes the file's scenario with, it
/// names a fuzz input, or its `meta.plan` isn't the file's Plan, bytes or
/// script as `script` says ([`binding::TRACE_META`]). Any file that can't
/// be read refuses it too.
fn typed_refusal(
    harness: &str,
    scenario_file: &Path,
    plan_file: &Path,
    script: bool,
    dir: &Path,
) -> Option<String> {
    let check = || -> Result<(), String> {
        let trace = crate::trace::TraceReader::open(dir)
            .map_err(|error| format!("{}: {error}", dir.display()))?;
        let text = fs::read_to_string(scenario_file)
            .map_err(|error| format!("{}: {error}", scenario_file.display()))?;
        let scenario: Value = serde_json::from_str(&text)
            .map_err(|error| format!("{}: {error}", scenario_file.display()))?;
        let entry = crate::harness::find(harness).map_err(|error| error.to_string())?;
        let described = (entry.meta)(&scenario).map_err(|error| error.to_string())?;
        let committed = plan_ref(plan_file, script)?;
        // A typed run names its Plan and no fuzz input.
        let expect = Expect::new()
            .with(Field::Harness, harness)
            .with(Field::Scenario, &described.scenario)
            .with(Field::InputSha256, Value::Null)
            .with(Field::Plan, &committed);
        let meta = serde_json::to_value(&trace.meta).map_err(|error| error.to_string())?;
        binding::check(&binding::TRACE_META, &meta, &expect).map_err(|why| {
            format!(
                "it isn't a run of {} under {}: {why}",
                scenario_file.display(),
                plan_file.display()
            )
        })
    };
    check().err()
}

/// Why the trace in `dir` didn't run `input`, the case's committed input
/// under `repo` on `harness`: its fuzz input by digest, or its typed
/// scenario and Plan bytes or script (C18).
fn case_input_refusal(repo: &Path, harness: &str, input: &CaseInput, dir: &Path) -> Option<String> {
    match input {
        CaseInput::Fuzz { input } => input_refusal(repo, input, dir),
        CaseInput::Typed { scenario, plan } => {
            typed_refusal(harness, &repo.join(scenario), &repo.join(plan), false, dir)
        }
        CaseInput::Scripted { scenario, script } => {
            typed_refusal(harness, &repo.join(scenario), &repo.join(script), true, dir)
        }
    }
}

/// The one declaration under `declared_dir` of `harness`'s history `case`
/// by `test` ([`super::history`]), or why there isn't exactly one. A
/// declaration by another test, or of the case on another harness, isn't
/// it.
fn declaration(
    declared_dir: &Path,
    harness: &str,
    test: &str,
    case: &str,
) -> Result<std::path::PathBuf, String> {
    let all = declared(declared_dir);
    let named: Vec<&(std::path::PathBuf, DeclaredHistory)> = all
        .iter()
        .filter(|(_, history)| history.case == case)
        .collect();
    match named.as_slice() {
        [(dir, history)] if history.harness == harness && libtest_names(&history.test, test) => {
            Ok(dir.clone())
        }
        [(_, history)] => Err(format!(
            "its witness test declared {case} as {}'s on {}, not {test}'s on {harness}",
            history.test, history.harness
        )),
        [] => Err(format!(
            "its witness test declared no history named {case} among {} declarations",
            all.len()
        )),
        _ => Err(format!(
            "its witness test declared {case} {} times",
            named.len()
        )),
    }
}

/// Every counted history the committed inventory
/// ([`counted_histories`](super::rows::counted_histories)) names for the
/// witness test `test` in `package`'s `file`, as its harness and case.
pub fn inventory_cases(package: &str, file: &str, test: &str) -> BTreeSet<(String, String)> {
    super::rows::counted_histories()
        .into_iter()
        .filter(|history| {
            (
                history.witness.package,
                history.witness.file,
                history.witness.test,
            ) == (package, file, test)
        })
        .map(|history| (history.harness.to_owned(), history.case.to_owned()))
        .collect()
}

/// Why the declarations a counted witness test made under
/// `ZAKURA_SIM_HISTORIES`, `declared`, aren't exactly `inventory`, the
/// counted histories the committed inventory names for it
/// ([`inventory_cases`]): one it names that the test didn't declare, one
/// the test declared that it doesn't name or declared twice, or one
/// another test declared. Empty when they match, or when the inventory
/// names no history for the test, which then counts none.
pub fn inventory_errors(
    inventory: &BTreeSet<(String, String)>,
    declared: &[(std::path::PathBuf, DeclaredHistory)],
    test: &str,
) -> Vec<String> {
    if inventory.is_empty() {
        return Vec::new();
    }
    let mut errors = Vec::new();
    let mut seen = BTreeSet::new();
    for (_, history) in declared {
        let key = (history.harness.clone(), history.case.clone());
        if !libtest_names(&history.test, test) {
            errors.push(format!(
                "{} declared {} on {}, inside {test}'s run",
                history.test, history.case, history.harness
            ));
        } else if !inventory.contains(&key) {
            errors.push(format!(
                "{test} declared {} on {}, which the inventory doesn't count",
                history.case, history.harness
            ));
        } else if !seen.insert(key) {
            errors.push(format!(
                "{test} declared {} on {} twice",
                history.case, history.harness
            ));
        }
    }
    for (harness, case) in inventory.difference(&seen) {
        errors.push(format!(
            "{test} didn't declare its counted history {case} on {harness}"
        ));
    }
    errors
}

/// Whether libtest's thread name `thread` names the test `test`: the test
/// itself, or a path ending in it within its file's crate.
fn libtest_names(thread: &str, test: &str) -> bool {
    thread == test || thread.ends_with(&format!("::{test}"))
}

/// The declaration in `declaration`'s one Plan file and whether it's a
/// script, or why it names neither or both forms.
fn declared_plan(declaration: &Path) -> Result<(std::path::PathBuf, bool), String> {
    let (plan, script) = (declaration.join(PLAN_FILE), declaration.join(SCRIPT_FILE));
    match (plan.exists(), script.exists()) {
        (true, false) => Ok((plan, false)),
        (false, true) => Ok((script, true)),
        _ => Err("its declaration names neither or both Plan forms".to_owned()),
    }
}

/// The `zakura-sim record` arguments, before `--out` and any witness, that
/// record the declaration in `declaration` on `harness`, as the runner's
/// contract gives them: `--sim`, then `--scenario` with the JSON text its
/// [`SCENARIO_FILE`] holds as one argument, since the CLI parses that value
/// as JSON, and `--plan` or `--script` with its Plan file's path.
pub fn record_args(harness: &str, declaration: &Path) -> Result<Vec<String>, String> {
    let (file, script) = declared_plan(declaration)?;
    let scenario_file = declaration.join(SCENARIO_FILE);
    let scenario = fs::read_to_string(&scenario_file)
        .map_err(|error| format!("{}: {error}", scenario_file.display()))?;
    Ok(vec![
        "--sim".to_owned(),
        harness.to_owned(),
        "--scenario".to_owned(),
        scenario,
        if script { "--script" } else { "--plan" }.to_owned(),
        file.display().to_string(),
    ])
}

/// Why the trace in `dir` isn't the declaration in `declaration` recorded
/// on `harness`: its scenario and its Plan bytes or script, by digest
/// (C18), or its steps aren't the ones the declaring test's own run took,
/// its [`RAN_FILE`] ([`super::history::run_declared`], [`binding::RAN`]). A declaration with
/// no such file wasn't run where it was declared, so it names no history.
fn declared_refusal(harness: &str, declaration: &Path, dir: &Path) -> Option<String> {
    let (file, script) = match declared_plan(declaration) {
        Ok(found) => found,
        Err(why) => return Some(why),
    };
    typed_refusal(
        harness,
        &declaration.join(SCENARIO_FILE),
        &file,
        script,
        dir,
    )
    .or_else(|| {
        let Some(ran) = read_json::<Value>(&declaration.join(RAN_FILE)) else {
            return Some("its declaration has no steps from its test's own run".to_owned());
        };
        let recorded = Ran::of(&crate::trace::TraceReader::open(dir).ok()?.steps);
        let expect = Expect::new()
            .with(Field::Steps, recorded.steps)
            .with(Field::StepsDigest, &recorded.digest);
        binding::check(&binding::RAN, &ran, &expect)
            .err()
            .map(|why| format!("its recording took other steps than its test's own run: {why}"))
    })
}

/// Whether `dir`'s `witness.json` judged `dir`'s own trace
/// ([`bound_witness`]) and the trigger `property` expecting `expect`
/// (`holds`, `not-applicable`, `violated` or `violated-set`) with exactly
/// `beside` as its `also_violates`, in order ([`TriggerRef`]). A witness
/// file for another trace, property, role or set isn't this run's trigger
/// evidence.
fn witness_judges(dir: &Path, property: &str, expect: &str, beside: &[String]) -> bool {
    let want = TriggerRef {
        property: property.to_owned(),
        expect: expect.to_owned(),
        also_violates: beside.to_vec(),
    };
    bound_witness(dir).is_ok_and(|witness| trigger_of(&witness).as_ref() == Some(&want))
}

/// The trigger `witness`, a witness file, judged: its property, its
/// expectation and its `also_violates`, none when it names none.
fn trigger_of(witness: &Value) -> Option<TriggerRef> {
    let also_violates = match witness.get("also_violates") {
        None => Vec::new(),
        Some(also) => also
            .as_array()?
            .iter()
            .map(|id| id.as_str().map(str::to_owned))
            .collect::<Option<_>>()?,
    };
    Some(TriggerRef {
        property: witness.get("property")?.as_str()?.to_owned(),
        expect: witness.get("expect")?.as_str()?.to_owned(),
        also_violates,
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
    let harness_caught: BTreeSet<(String, String)> = planted
        .iter()
        .filter(|(property, name)| {
            super::planted_harness_caught(repo, property, name).unwrap_or(false)
        })
        .cloned()
        .collect();
    required_keys(&Inventories {
        registered: &registered,
        needs: &needs,
        planted: &planted,
        planted_harnesses: &planted_harnesses,
        harness_caught: &harness_caught,
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

/// Which history a run of [`a_a_and_a_b_a`] records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Which {
    /// The history under comparison.
    A,
    /// A different history, run between two of A's.
    B,
}

/// Gate 1's A/A and A/B/A comparison, in this one process: `record` writes
/// history A's run into a fresh directory, then A again, then a distinct
/// history B, then A once more, each a recorded run under `root`. A's
/// three runs must have the same trace and the same full report, every
/// outcome and violation and the stores' snapshot (C8), so neither a rerun
/// nor anything B leaves behind changes it. B must be another execution,
/// with a trace of its own. A run that can't be
/// read, such as one with no full report, is a missing comparison.
pub fn a_a_and_a_b_a(
    root: &Path,
    record: &mut dyn FnMut(Which, &Path) -> Result<(), String>,
) -> Result<(), String> {
    let mut read = |which: Which, name: &str| -> Result<(Trace, FullReport), String> {
        let dir = root.join(name);
        fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        record(which, &dir).map_err(|why| format!("{name}: {why}"))?;
        recorded(&dir).map_err(|why| format!("{name} is a missing comparison: {why}"))
    };
    let first = read(Which::A, "a")?;
    let again = read(Which::A, "a-again")?;
    let other = read(Which::B, "b")?;
    let after = read(Which::A, "a-after-b")?;
    let same = |(trace, report): &(Trace, FullReport)| (trace.id == first.0.id, *report == first.1);
    match same(&again) {
        (true, true) => {}
        (trace, report) => {
            return Err(format!(
                "A's second run differs: same trace {trace}, same full report {report}"
            ))
        }
    }
    // A trace holds every pick, draw and observation digest, so another
    // execution has another trace.
    if other.0.id == first.0.id {
        return Err("B resolves to A's execution".to_owned());
    }
    match same(&after) {
        (true, true) => Ok(()),
        (trace, report) => Err(format!(
            "A's run after B differs: same trace {trace}, same full report {report}"
        )),
    }
}

/// The fresh replay of the recorded run `run` in `dir`, by `run_by`, as
/// [`assemble`] reads a `Replays` result from the runner's artifacts.
pub fn read_replay(dir: &Path, run: RunId, run_by: RunBy) -> Actual {
    replay_actual(dir, run, run_by)
}

/// Where one obligation's evidence is.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Links {
    /// The harness its runs ran on.
    pub harness: Option<String>,
    /// The committed input, or a typed case's scenario.
    pub input: Option<String>,
    /// A typed case's Plan file: its Plan bytes or its script.
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

/// A header ID still `Draft`, as the record lists it: what promotes it,
/// and who.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DraftExport {
    /// The property.
    pub property: String,
    /// What must hold before it goes `Active`.
    pub condition: String,
    /// Who promotes it.
    pub owner: String,
}

/// Every header ID a harness in `registered` registers as `Draft`, once,
/// with its row's promotion, and an error for each such ID whose row in
/// `rows` is missing, isn't `Draft`, or names no promotion (item 9).
pub fn draft_list(
    registered: &[super::Registered],
    rows: &[super::Row],
) -> (Vec<DraftExport>, Vec<String>) {
    let drafts: BTreeSet<&str> = registered
        .iter()
        .filter(|registration| registration.status == crate::property::Status::Draft)
        .map(|registration| registration.property.as_str())
        .collect();
    let mut listed = Vec::new();
    let mut errors = Vec::new();
    for property in drafts {
        let row = rows.iter().find(|row| row.property == property);
        match row.and_then(|row| {
            (row.status == crate::property::Status::Draft)
                .then_some(row.promotion)
                .flatten()
        }) {
            Some(promotion) => listed.push(DraftExport {
                property: property.to_owned(),
                condition: promotion.condition.to_owned(),
                owner: promotion.owner.to_owned(),
            }),
            None => errors.push(format!(
                "{property} is registered Draft with no Draft row saying what promotes it"
            )),
        }
    }
    (listed, errors)
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
    /// Every header ID still `Draft`, with what promotes it and who. A
    /// `Draft` with no row saying so is a stray.
    pub drafts: Vec<DraftExport>,
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
            Task::History {
                expected,
                harness,
                property,
                ..
            } => (
                expected.clone(),
                Links {
                    harness: Some(harness.clone()),
                    history: history_of(expected).cloned(),
                    trigger: Some(property.clone()),
                    result: Some(expected.clone()),
                    artifacts: vec![
                        format!("{index}/{TEST_FILE}"),
                        format!("{index}/declared"),
                        format!("{index}/run/trace.jsonl"),
                        format!("{index}/run/{FULL_REPORT_FILE}"),
                        format!("{index}/run/witness.json"),
                    ],
                    ..Links::default()
                },
            ),
            Task::Case {
                expected,
                property,
                harness,
                input,
                draft,
                ..
            } => (
                expected.clone(),
                Links {
                    harness: Some(harness.clone()),
                    input: Some(input.input().to_owned()),
                    plan: input.plan().map(str::to_owned),
                    trigger: Some(property.clone()),
                    result: Some(expected.clone()),
                    artifacts: {
                        let mut artifacts = Vec::new();
                        for run in ["baseline", "patched"]
                            .into_iter()
                            .chain(draft.as_ref().map(|_| "draft"))
                        {
                            artifacts.push(format!("{index}/{run}/trace.jsonl"));
                            artifacts.push(format!("{index}/{run}/{FULL_REPORT_FILE}"));
                            if run != "draft" {
                                artifacts.push(format!("{index}/{run}/witness.json"));
                            }
                        }
                        if draft.is_some() {
                            artifacts.push(format!("{index}/draft/{COMPANIONS_FILE}"));
                        }
                        artifacts.extend([
                            format!("{index}/{PROVENANCE_FILE}"),
                            format!("{index}/{VERDICT_FILE}"),
                            format!("{index}/{TARGET_FILE}"),
                            format!("{index}/{CONFIRMATION_DIR}/trace.jsonl"),
                        ]);
                        artifacts
                    },
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
                        .flat_map(|run| {
                            [
                                format!("{index}/{run}/{TEST_FILE}"),
                                format!("{index}/{run}/declared"),
                                format!("{index}/{run}/history/trace.jsonl"),
                                format!("{index}/{run}/history/{FULL_REPORT_FILE}"),
                            ]
                        })
                        .chain([format!("{index}/patched/{PATCHED_BUILD_FILE}")])
                        .collect(),
                    ..Links::default()
                },
            ),
        };
        by_id.insert(id, links);
    }
    // A replay links the recorded run it replayed and its fresh replay, in
    // the task that records that run, and a replay of a run no task records
    // links what it's of.
    for result in &manifest.expected {
        let (Expected::Replays { run }, Some(id)) = (&result.expected, result.expected.id()) else {
            continue;
        };
        let links = replay_links(run, &plan.tasks, &bound);
        by_id.entry(id).or_insert(links);
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

/// What a replay of `run` links: the recorded run in the task of `tasks`
/// that records it, if that task's directory is `bound` to it, with the
/// replay's own artifacts, and its case's or history's harness, input and
/// trigger. A run no bound task records links only its identity.
fn replay_links(run: &RunId, tasks: &[Task], bound: &impl Fn(usize, &Task) -> bool) -> Links {
    let Some((index, dir)) =
        recorded_at(tasks, run).filter(|(index, _)| bound(*index, &tasks[*index]))
    else {
        return Links {
            history: Some(run.clone()),
            ..Links::default()
        };
    };
    let (harness, input, plan, trigger) = match &tasks[index] {
        Task::Case {
            harness,
            input,
            property,
            ..
        } => (
            harness.clone(),
            Some(input.input().to_owned()),
            input.plan().map(str::to_owned),
            property.clone(),
        ),
        Task::History {
            harness, property, ..
        } => (harness.clone(), None, None, property.clone()),
        _ => unreachable!("only a case or history task records a run"),
    };
    Links {
        harness: Some(harness),
        input,
        plan,
        history: Some(run.clone()),
        trigger: Some(trigger),
        result: Some(ExpectedId::Replays(run.clone())),
        artifacts: vec![
            format!("{index}/{dir}/trace.jsonl"),
            format!("{index}/{dir}/{FULL_REPORT_FILE}"),
            format!("{index}/{dir}/{REPLAY_DIR}/{REPLAY_FILE}"),
            format!("{index}/{dir}/{REPLAY_DIR}/{FULL_REPORT_FILE}"),
        ],
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
            task_bound(artifacts, &run.run_id, index, task)
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
    let (drafts, draft_errors) = draft_list(&super::registered(), &super::rows());
    stray.extend(draft_errors);
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
        drafts,
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

    /// The committed rows list `honest-progress` as the one `Draft`, owned
    /// by Evan. A `Draft` registration whose row is gone, isn't `Draft`, or
    /// names no promotion is an error instead.
    #[test]
    fn the_draft_list_names_each_draft_and_its_owner() {
        let registered = super::super::registered();
        let rows = super::super::rows();
        let (drafts, errors) = draft_list(&registered, &rows);
        assert_eq!(errors, Vec::<String>::new());
        assert_eq!(
            drafts
                .iter()
                .map(|draft| (draft.property.as_str(), draft.owner.as_str()))
                .collect::<Vec<_>>(),
            [("header-sync/honest-progress", "Evan")]
        );
        let at = rows
            .iter()
            .position(|row| row.property == "header-sync/honest-progress")
            .expect("honest-progress has a row");
        let mut unpromoted = rows.clone();
        unpromoted[at].promotion = None;
        let mut activated = rows.clone();
        activated[at].status = crate::property::Status::Active;
        let mut missing = rows.clone();
        missing.remove(at);
        for changed in [unpromoted, activated, missing] {
            let (drafts, errors) = draft_list(&registered, &changed);
            assert!(drafts.is_empty(), "{drafts:?}");
            assert_eq!(errors.len(), 1, "{errors:?}");
        }
    }

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
        write_witness(dir, serde_json::json!({ "passed": true }));
    }

    /// Writes `fields` as `dir`'s witness file, bound as `zakura-sim record`
    /// binds it to the trace it judged: that trace's digest, the source its
    /// meta records and its `Draft` override. Returns what it wrote.
    fn write_witness(dir: &Path, mut fields: Value) -> Value {
        let trace = crate::trace::TraceReader::open(dir).expect("the trace reads");
        fields["trace"] = Value::String(trace.id);
        fields["source"] = Value::String(trace.meta.build.source);
        fields["draft"] = serde_json::json!(trace.meta.draft);
        fs::write(dir.join("witness.json"), fields.to_string()).expect("the temp dir is writable");
        fields
    }

    /// Rewrites `dir`'s trace through `edit` on its parsed lines, and
    /// rebinds its full report, if it has one, to the rewritten trace as
    /// `zakura-sim record` binds it: that trace's digest and the source its
    /// meta records.
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
        let report = dir.join(FULL_REPORT_FILE);
        let Ok(text) = fs::read_to_string(&report) else {
            return;
        };
        let mut full: FullReport = serde_json::from_str(&text).expect("the report reads");
        let trace = crate::trace::TraceReader::open_prefix(dir).expect("the trace reads");
        full.trace = Some(trace.id);
        full.source = Some(trace.meta.build.source);
        fs::write(&report, full.to_line()).expect("the temp dir is writable");
    }

    /// [`record_into`] a fresh directory, built from `s`.
    fn recorded() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        record_into(dir.path(), "s");
        dir
    }

    /// A machine run's actions follow its driver: completed is its whole
    /// input, stopped at a violation a prefix, anything else none. A node
    /// run's actions come from its bound scenario: the committed
    /// close-and-relink history picks its four control steps in order and
    /// the `ends` store shows each one acted, so it's complete, and a run a
    /// violation stopped is judged as a prefix. Controls: a declared step
    /// that was never picked, one picked whose effect the store doesn't
    /// show, a picked peer action the scenario doesn't declare, and a later
    /// step the scenario holds that a hand list of the first four would
    /// leave out, ending before it's picked, each make the run incomplete.
    #[test]
    fn a_run_s_actions_follow_its_harness() {
        let violation = End::Violation {
            property: "t/a".into(),
        };
        let machine_dir = recorded();
        let machine = crate::trace::TraceReader::open(machine_dir.path()).expect("it reads");
        let machine_report = bound_report(machine_dir.path(), machine_dir.path())
            .unwrap_or_else(|_| FullReport::new(&[], None));
        let judged =
            |trace: &Trace, report: &FullReport, end: &End| actions(trace, report, end, false);
        assert_eq!(
            judged(&machine, &machine_report, &End::Completed),
            Actions::Complete
        );
        assert_eq!(
            judged(&machine, &machine_report, &violation),
            Actions::Prefix
        );
        assert_eq!(
            judged(&machine, &machine_report, &End::Killed),
            Actions::Incomplete
        );

        let trace = crate::trace::TraceReader::open(storage_recording()).expect("it reads");
        let report = bound_report(storage_recording(), storage_recording())
            .expect("the recording has its report");
        assert_eq!(judged(&trace, &report, &End::Completed), Actions::Complete);
        assert_eq!(judged(&trace, &report, &violation), Actions::Prefix);
        assert_eq!(judged(&trace, &report, &End::Killed), Actions::Incomplete);

        let last = "peer:mirror-1:relink";
        let mut unpicked = trace.clone();
        unpicked.steps.retain(|step| step.pick != last);
        assert_eq!(
            judged(&unpicked, &report, &End::Completed),
            Actions::Incomplete
        );

        let mut unshown = report.clone();
        for store in unshown
            .snapshot
            .as_mut()
            .and_then(Value::as_array_mut)
            .expect("a node run keeps its stores")
        {
            if store["store"] == "ends" {
                let text = store["snapshot"].as_str().expect("a Debug snapshot");
                let relinked =
                    "Environment { peer: \"mirror-1\", connection: Some(4), action: Relink }";
                assert!(text.contains(relinked), "{text}");
                store["snapshot"] = Value::String(text.replace(relinked, "Dropped"));
            }
        }
        assert_eq!(
            judged(&trace, &unshown, &End::Completed),
            Actions::Incomplete
        );

        let mut undeclared = trace.clone();
        let mut extra = undeclared.steps[0].clone();
        extra.pick = "peer:mirror-0:grow".to_owned();
        undeclared.steps.push(extra);
        assert_eq!(
            judged(&undeclared, &report, &End::Completed),
            Actions::Incomplete
        );

        let mut later = trace.clone();
        later.meta.scenario["controls"]
            .as_array_mut()
            .expect("the scenario lists its controls")
            .push(serde_json::json!({ "peer": "mirror-0", "action": "close" }));
        assert_eq!(
            judged(&later, &report, &End::Completed),
            Actions::Incomplete
        );
        let declared = super::super::actions::declared(NODE, &later.meta.scenario)
            .expect("the scenario derives");
        assert_eq!(declared.len(), 5);
        assert_eq!(
            super::super::actions::judge(&declared[..4], &later, &report, &End::Completed, false),
            Actions::Complete,
            "a hand list of the first four steps would have passed it"
        );
    }

    /// Each control step's effect is the fact its own execution recorded,
    /// on a link the `sessions` store shows opened, in its turn. The
    /// committed close-and-relink history's four steps each name their
    /// own fact, so it's complete. Controls: a churn of `mirror-1` put
    /// ahead of the script relinks it twice, and the script's later
    /// `Relink` of it records nothing, so the churn's spare relink can't
    /// stand for it. With the `Relink`'s own fact back, it's complete. A
    /// relink naming a connection never opened to its peer, or one an
    /// earlier relink named, and a close recorded after the next step's
    /// relink each leave the run incomplete.
    #[test]
    fn each_control_effect_joins_its_own_step() {
        let trace = crate::trace::TraceReader::open(storage_recording()).expect("it reads");
        let report = bound_report(storage_recording(), storage_recording())
            .expect("the recording has its report");
        let judged =
            |trace: &Trace, report: &FullReport| actions(trace, report, &End::Completed, false);
        assert_eq!(judged(&trace, &report), Actions::Complete);
        // `report` with `name`'s store text rewritten by `edit`.
        let edited = |report: &FullReport, name: &str, edit: &dyn Fn(&str) -> String| {
            let mut report = report.clone();
            for store in report
                .snapshot
                .as_mut()
                .and_then(Value::as_array_mut)
                .expect("a node run keeps its stores")
            {
                if store["store"] != name {
                    continue;
                }
                let text = if name == "sessions" {
                    &mut store["snapshot"]["facts"]
                } else {
                    &mut store["snapshot"]
                };
                let before = text.as_str().expect("a Debug snapshot").to_owned();
                let after = edit(&before);
                assert_ne!(before, after, "the edit applies to {name}");
                *text = Value::String(after);
            }
            report
        };
        let relink =
            "(21, Environment { peer: \"mirror-1\", connection: Some(4), action: Relink })";
        let causes = "causes: [(4, 0), (12, 1), (18, 2), (21, 3)]";

        // A churn of mirror-1 first, its pick ahead of the script's.
        let mut churned = trace.clone();
        churned.meta.scenario["controls"]
            .as_array_mut()
            .expect("the scenario lists its controls")
            .insert(
                0,
                serde_json::json!({ "peer": "mirror-1", "action": "churn" }),
            );
        let first = churned
            .steps
            .iter()
            .position(|step| step.pick.starts_with("peer:"))
            .expect("the history picks a control step");
        let mut pick = churned.steps[first].clone();
        pick.pick = "peer:mirror-1:churn".into();
        churned.steps.insert(first, pick);
        let churn = |relink_by: usize| {
            let report = edited(&report, "ends", &|text| {
                text.replace(
                    relink,
                    &format!(
                        "(5, Environment {{ peer: \"mirror-1\", connection: Some(9), action: Relink }}), {relink}"
                    ),
                )
                .replace(
                    causes,
                    &format!("causes: [(4, 1), (5, 0), (12, 2), (18, 3), (21, {relink_by})]"),
                )
            });
            edited(&report, "sessions", &|text| {
                text.replacen(
                    "LinkOpened",
                    "LinkOpened { peer: \"mirror-1\", connection: 9, session_id: 9, dialer: \"node\" }, LinkOpened",
                    1,
                )
            })
        };
        // The script's Relink is position 4, the churn 0.
        assert_eq!(judged(&churned, &churn(0)), Actions::Incomplete);
        assert_eq!(judged(&churned, &churn(4)), Actions::Complete);

        for connection in ["Some(9)", "Some(3)"] {
            let changed = edited(&report, "ends", &|text| {
                text.replace(relink, &relink.replace("Some(4)", connection))
            });
            assert_eq!(
                judged(&trace, &changed),
                Actions::Incomplete,
                "{connection}"
            );
        }
        let late = edited(&report, "ends", &|text| {
            text.replace("(18, Environment", "(30, Environment")
                .replace("(18, 2)", "(30, 2)")
        });
        assert_eq!(judged(&trace, &late), Actions::Incomplete);
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
        assert_eq!(
            binding::check_trace(dir.path(), &Expect::new().with(Field::Source, "s")),
            Ok(())
        );

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

    /// An execution's reports come from its full report (C8), which must
    /// agree with the trace's `end` record on every outcome: a missing
    /// report, or one whose outcome differs, is a run that didn't happen.
    /// Its `Draft` override is the trace's `meta.draft` (C9).
    #[test]
    fn an_execution_s_reports_come_from_its_full_report() {
        let dir = recorded();
        let path = dir.path().join(FULL_REPORT_FILE);
        let mut report = bound_report(dir.path(), dir.path()).expect("record wrote it");
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert_eq!(run.end, End::Completed, "{run:?}");
        assert_eq!(run.meta.draft_override, Vec::<String>::new());
        report.properties[0].outcome = PropertyOutcome::Violated { witness: vec![1] };
        fs::write(&path, report.to_line()).expect("the temp dir is writable");
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert!(matches!(run.end, End::Failed { .. }), "{:?}", run.end);
        fs::remove_file(&path).expect("it exists");
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert!(matches!(run.end, End::Failed { .. }), "{:?}", run.end);
        let dir = recorded();
        rewrite(dir.path(), |lines| {
            lines[0]["meta"]["draft"] = serde_json::json!(["header/reactor-policy"]);
        });
        let run = execution(dir.path(), history_run(), MACHINE, RunBy::Tool);
        assert_eq!(run.meta.draft_override, ["header/reactor-policy"]);
    }

    /// Writes `dir`'s fresh replay as the runner does: the replay's own full
    /// report from an in-process replay of the trace, and its child's end
    /// with `log`.
    fn replay_into(dir: &Path, exited_ok: bool, log: &str) {
        let trace = crate::trace::TraceReader::open(dir).expect("the trace reads");
        let replayed = crate::trace::replay(&trace, crate::trace::ReplayOptions::default())
            .expect("the trace replays");
        let replay = dir.join(REPLAY_DIR);
        fs::create_dir_all(&replay).expect("the temp dir is writable");
        // As the recording's own build, whose source the trace records,
        // binds its replay's report.
        let report = FullReport {
            source: Some(trace.meta.build.source.clone()),
            ..replayed.report.expect("a replay sends its report")
        };
        fs::write(replay.join(FULL_REPORT_FILE), report.to_line())
            .expect("the temp dir is writable");
        let ran = ReplayRun {
            exited_ok,
            killed: false,
            log: log.to_owned(),
        };
        fs::write(
            replay.join(REPLAY_FILE),
            serde_json::to_string(&ran).expect("it serializes"),
        )
        .expect("the temp dir is writable");
    }

    /// A replay is exact only when its child exited 0 naming this trace with
    /// every record matched, and its reports and store snapshot are equal
    /// only when its full report's outcomes and violations, and its
    /// snapshot, are the recording's. A changed stored fact with every
    /// report the same is unequal stores. No replay file is no fresh
    /// replay.
    #[test]
    fn a_replay_is_read_from_its_own_artifacts() {
        let dir = recorded();
        let of = digest(&dir.path().join("trace.jsonl"));
        let matched = format!(
            "replayed trace {of}: idle after 3 steps; every pick, draw and observation matched"
        );
        replay_into(dir.path(), true, &matched);
        let read = |dir: &Path| match replay_actual(dir, history_run(), RunBy::Tool) {
            Actual::Replay {
                exact,
                reports_equal,
                stores_equal,
                fresh,
                of: replayed,
                meta,
                ..
            } => {
                assert_eq!(replayed, of);
                assert_eq!(meta.harness, MACHINE);
                (exact, reports_equal, stores_equal, fresh)
            }
            other => unreachable!("{other:?}"),
        };
        assert_eq!(read(dir.path()), (true, true, true, true));
        // A child that failed, or named another trace.
        for (ok, log) in [
            (false, matched.clone()),
            (true, matched.replace(&of, &"0".repeat(64))),
            (true, String::new()),
        ] {
            let dir = recorded();
            replay_into(dir.path(), ok, &log);
            assert!(!read(dir.path()).0, "{log}");
        }
        // One stored fact changed in the replay's report, every outcome and
        // violation the same: only the stores differ.
        let replayed = dir.path().join(REPLAY_DIR).join(FULL_REPORT_FILE);
        let mut report: FullReport =
            serde_json::from_str(&fs::read_to_string(&replayed).expect("it exists"))
                .expect("it reads");
        report.snapshot = Some(serde_json::json!([{"store": "a", "snapshot": [1]}]));
        fs::write(&replayed, report.to_line()).expect("the temp dir is writable");
        assert_eq!(read(dir.path()), (true, true, false, true));
        let dir = recorded();
        assert_eq!(read(dir.path()), (false, false, false, false));
    }

    /// A typed case's trace binds to its committed scenario and Plan (C18):
    /// Plan bytes by their digest, a script by the Plan it parses to however
    /// it's formatted. Another scenario, other Plan bytes, another script,
    /// the other Plan form or another harness each refuse it.
    #[test]
    fn a_typed_trace_binds_to_its_scenario_and_plan() {
        let input = fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/header-reactor-policy.bin"),
        )
        .expect("the committed input is readable");
        let entry = crate::harness::find(MACHINE).expect("the machine harness is registered");
        let split = entry.scenario_bytes.min(input.len());
        let (scenario, plan) = (entry.scenario(&input), input[split..].to_vec());
        let repo = tempfile::tempdir().expect("a temp dir can be created");
        let put = |name: &str, bytes: &[u8]| {
            fs::write(repo.path().join(name), bytes).expect("the temp dir is writable")
        };
        put("scenario.json", scenario.to_string().as_bytes());
        put("plan.bin", &plan);
        let script = crate::Plan::default();
        put(
            "script.json",
            serde_json::to_string_pretty(&script)
                .expect("it serializes")
                .as_bytes(),
        );
        let typed = CaseInput::Typed {
            scenario: "scenario.json".into(),
            plan: "plan.bin".into(),
        };
        let scripted = CaseInput::Scripted {
            scenario: "scenario.json".into(),
            script: "script.json".into(),
        };
        let bytes_run = tempfile::tempdir().expect("a temp dir can be created");
        crate::harness::record(
            entry,
            crate::harness::RecordInput::Plan {
                scenario: &scenario,
                plan: &plan,
            },
            bytes_run.path(),
        )
        .expect("the Plan bytes record");
        let script_run = tempfile::tempdir().expect("a temp dir can be created");
        crate::harness::record(
            entry,
            crate::harness::RecordInput::Script {
                scenario: &scenario,
                plan: &script,
                derived_from: None,
            },
            script_run.path(),
        )
        .expect("the script records");
        let refused =
            |input: &CaseInput, dir: &Path| case_input_refusal(repo.path(), MACHINE, input, dir);
        assert_eq!(refused(&typed, bytes_run.path()), None);
        assert_eq!(refused(&scripted, script_run.path()), None);
        assert!(refused(&typed, script_run.path()).is_some());
        assert!(refused(&scripted, bytes_run.path()).is_some());
        assert!(case_input_refusal(
            repo.path(),
            "header/publisher-policy",
            &typed,
            bytes_run.path()
        )
        .is_some());
        let mut other = plan.clone();
        other.push(0);
        put("plan.bin", &other);
        assert!(refused(&typed, bytes_run.path()).is_some());
        put("plan.bin", &plan);
        // Another valid scenario of the harness, from another prefix.
        let changed = (0..=u8::MAX)
            .map(|byte| {
                let mut other = input.clone();
                other.iter_mut().take(split).for_each(|b| *b ^= byte);
                entry.scenario(&other)
            })
            .find(|other| *other != scenario)
            .expect("another prefix decodes another scenario");
        put("scenario.json", changed.to_string().as_bytes());
        assert!(refused(&typed, bytes_run.path()).is_some());
        put("scenario.json", scenario.to_string().as_bytes());
        put(
            "script.json",
            serde_json::to_string(&crate::Plan {
                choices: vec![crate::PlanChoice::Pick("x".into())],
            })
            .expect("it serializes")
            .as_bytes(),
        );
        assert!(refused(&scripted, script_run.path()).is_some());
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
                base: "s".into(),
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
            base: "s".into(),
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

    /// The machine case's committed input, as a task names it.
    const MACHINE_INPUT: &str = "crates/zakura-sim/tests/fixtures/header-reactor-policy.bin";

    /// A case directory as the runner leaves one for a machine case run at
    /// the subject built as `subject` and its patched checkout built as
    /// `patched`: its baseline and patched runs, its target confirmation's
    /// run in [`CONFIRMATION_DIR`], each recorded, and its provenance check.
    fn case_dir(subject: &str, patched: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        for (run, source) in [
            ("baseline", subject),
            ("patched", patched),
            (CONFIRMATION_DIR, patched),
        ] {
            record_into(&dir.path().join(run), source);
        }
        let check = ProvenanceCheck {
            base: "s".into(),
            source: patched.into(),
            patch_sha256: String::new(),
            refused: None,
        };
        fs::write(
            dir.path().join(PROVENANCE_FILE),
            serde_json::to_string(&check).expect("a check serializes"),
        )
        .expect("the temp dir is writable");
        dir
    }

    /// The verdict and target checks the runner writes for `case`'s own
    /// runs, confirming it through `target`, if any.
    fn case_checks(case: &Path, target: Option<&str>) -> (VerdictCheck, TargetCheck) {
        let trace = |run: &str| digest(&case.join(run).join("trace.jsonl"));
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let verdict = VerdictCheck {
            property: "p".into(),
            name: "a".into(),
            baseline: trace("baseline"),
            patched: trace("patched"),
            refused: None,
        };
        let confirmation = TargetCheck {
            property: "p".into(),
            name: "a".into(),
            target: target.map(str::to_owned),
            source: read_json::<ProvenanceCheck>(&case.join(PROVENANCE_FILE))
                .expect("the case has a provenance check")
                .source,
            input_sha256: target.map(|_| digest(&repo.join(MACHINE_INPUT))),
            trace: target.map(|_| trace(CONFIRMATION_DIR)),
            refused: None,
        };
        (verdict, confirmation)
    }

    /// Writes `verdict` and `target` into `case`.
    fn write_checks(case: &Path, verdict: &VerdictCheck, target: &TargetCheck) {
        for (file, check) in [
            (VERDICT_FILE, serde_json::to_value(verdict)),
            (TARGET_FILE, serde_json::to_value(target)),
        ] {
            fs::write(
                case.join(file),
                check.expect("a check serializes").to_string(),
            )
            .expect("the temp dir is writable");
        }
    }

    /// A case is credited only when the planted runner reported it caught
    /// and, if it has a fuzz target, confirmed it there, each check naming
    /// this case. Each check refused, missing or naming another case on its
    /// own is a reason to fail its patched run.
    #[test]
    fn a_case_needs_its_runner_verdict_and_target_confirmation() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let input = CaseInput::Fuzz {
            input: MACHINE_INPUT.into(),
        };
        let case = case_dir("s", "patched");
        let (mine, confirmed) = case_checks(case.path(), Some("fuzz_a"));
        let read = |verdict: Option<&VerdictCheck>, target: Option<&TargetCheck>| {
            for (file, check) in [
                (VERDICT_FILE, verdict.map(serde_json::to_value)),
                (TARGET_FILE, target.map(serde_json::to_value)),
            ] {
                let path = case.path().join(file);
                match check {
                    Some(check) => fs::write(&path, check.expect("a check serializes").to_string())
                        .expect("the temp dir is writable"),
                    None => {
                        let _ = fs::remove_file(&path);
                    }
                }
            }
            case_refusal(
                &repo,
                case.path(),
                ("p", "a"),
                (MACHINE, Some("fuzz_a")),
                &input,
            )
        };
        assert_eq!(read(Some(&mine), Some(&confirmed)), None);
        // Confirmed through no target, or another one, while the harness
        // maps to `fuzz_a`.
        for target in [None, Some("fuzz_b".to_owned())] {
            let other = TargetCheck {
                target,
                ..confirmed.clone()
            };
            assert!(read(Some(&mine), Some(&other)).is_some(), "{other:?}");
        }
        // The runner's verdict refused, or missing.
        let refused = VerdictCheck {
            refused: Some("the campaign failed".into()),
            ..mine.clone()
        };
        assert!(read(Some(&refused), Some(&confirmed)).is_some());
        assert!(read(None, Some(&confirmed)).is_some());
        // The target confirmation refused, or missing.
        let held = TargetCheck {
            refused: Some("it held".into()),
            ..confirmed.clone()
        };
        assert!(read(Some(&mine), Some(&held)).is_some());
        assert!(read(Some(&mine), None).is_some());
        // Either check for another case, one field off.
        for (property, name) in [("q", "a"), ("p", "a-b")] {
            let verdict = VerdictCheck {
                property: property.into(),
                name: name.into(),
                ..mine.clone()
            };
            let target = TargetCheck {
                property: property.into(),
                name: name.into(),
                ..confirmed.clone()
            };
            assert!(read(Some(&verdict), Some(&confirmed)).is_some(), "{name}");
            assert!(read(Some(&mine), Some(&target)).is_some(), "{name}");
        }
        // A harness no target runs needs a check naming none.
        let (verdict, untargeted) = case_checks(case.path(), None);
        write_checks(case.path(), &verdict, &untargeted);
        assert_eq!(
            case_refusal(&repo, case.path(), ("p", "a"), (MACHINE, None), &input),
            None
        );
        assert!(case_refusal(
            &repo,
            case.path(),
            ("p", "a"),
            (MACHINE, Some("fuzz_a")),
            &input
        )
        .is_some());
    }

    /// Sol P2: an older attempt's checks for the same case never count
    /// beside this attempt's runs. A verdict that judged an older baseline
    /// or patched trace is refused, and so is a target confirmation that
    /// ran an older patched build, another input, or names a run other
    /// than the one beside it, even when that run and its check agree, or
    /// one of another harness than its target maps to.
    #[test]
    fn an_older_attempt_s_case_checks_are_refused() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let input = CaseInput::Fuzz {
            input: MACHINE_INPUT.into(),
        };
        let current = case_dir("s", "patched");
        let older = case_dir("s-before", "patched-before");
        let (mine, confirmed) = case_checks(current.path(), Some("fuzz_a"));
        let (old_verdict, old_target) = case_checks(older.path(), Some("fuzz_a"));
        let refusal = |verdict: &VerdictCheck, target: &TargetCheck| {
            write_checks(current.path(), verdict, target);
            case_refusal(
                &repo,
                current.path(),
                ("p", "a"),
                (MACHINE, Some("fuzz_a")),
                &input,
            )
        };
        assert_eq!(refusal(&mine, &confirmed), None);
        let substitutes = [
            ("the older verdict", old_verdict.clone(), confirmed.clone()),
            (
                "a verdict on an older baseline",
                VerdictCheck {
                    baseline: old_verdict.baseline.clone(),
                    ..mine.clone()
                },
                confirmed.clone(),
            ),
            (
                "a verdict on an older patched run",
                VerdictCheck {
                    patched: old_verdict.patched.clone(),
                    ..mine.clone()
                },
                confirmed.clone(),
            ),
            (
                "the older target confirmation",
                mine.clone(),
                old_target.clone(),
            ),
            (
                "a confirmation from the older build",
                mine.clone(),
                TargetCheck {
                    source: old_target.source.clone(),
                    ..confirmed.clone()
                },
            ),
            (
                "a confirmation on another input",
                mine.clone(),
                TargetCheck {
                    input_sha256: Some("00".repeat(32)),
                    ..confirmed.clone()
                },
            ),
            (
                "a confirmation naming the older run",
                mine.clone(),
                TargetCheck {
                    trace: old_target.trace.clone(),
                    ..confirmed.clone()
                },
            ),
        ];
        for (substitute, verdict, target) in substitutes {
            assert!(refusal(&verdict, &target).is_some(), "{substitute}");
        }
        // A run of another harness on the same build and input, which the
        // confirmation names: only the trace's harness is off the one the
        // target maps to.
        let on_another_harness = TargetCheck {
            trace: Some({
                rewrite(&current.path().join(CONFIRMATION_DIR), |lines| {
                    lines[0]["meta"]["sim"] = Value::String(NODE.to_owned());
                });
                digest(&current.path().join(CONFIRMATION_DIR).join("trace.jsonl"))
            }),
            ..confirmed.clone()
        };
        let why = refusal(&mine, &on_another_harness).expect("another harness's run is refused");
        assert!(why.contains(&format!("ran {NODE}, not {MACHINE}")), "{why}");
        // The older attempt's confirmation run copied in with its own check,
        // which agree with each other, still isn't from the measured build.
        fs::remove_dir_all(current.path().join(CONFIRMATION_DIR)).expect("it exists");
        record_into(&current.path().join(CONFIRMATION_DIR), "patched-before");
        assert!(refusal(&mine, &old_target).is_some());
    }

    /// C5: a planted catch counts only from a rebuild of S itself with the
    /// case's committed patch. A rebuild from an older checkout with the
    /// same patch, whose patched run, confirmation and checks all name the
    /// source it measured, and whose baseline is S's, is refused for its
    /// base alone.
    #[test]
    fn a_planted_build_from_another_base_is_refused() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let patch = "qa/planted/header/reactor-policy/locator-call-settles-without-removal.patch";
        let input = CaseInput::Fuzz {
            input: MACHINE_INPUT.into(),
        };
        let case = case_dir("s", "patched");
        let (verdict, target) = case_checks(case.path(), Some("fuzz_a"));
        write_checks(case.path(), &verdict, &target);
        let rebuilt_from = |base: &str| {
            let check = ProvenanceCheck {
                base: base.into(),
                source: "patched".into(),
                patch_sha256: digest(&repo.join(patch)),
                refused: None,
            };
            fs::write(
                case.path().join(PROVENANCE_FILE),
                serde_json::to_string(&check).expect("a check serializes"),
            )
            .expect("the temp dir is writable");
            // Every measured-source join and case check holds.
            let read = |run: &str| {
                execution(
                    &case.path().join(run),
                    history_run(),
                    MACHINE,
                    RunBy::Planted,
                )
            };
            let (mut baseline, mut patched) = (read("baseline"), read("patched"));
            credit_sources(case.path(), "s", &mut baseline, &mut patched);
            assert_eq!(
                (baseline.end, patched.end),
                (End::Completed, End::Completed)
            );
            assert_eq!(
                case_refusal(
                    &repo,
                    case.path(),
                    ("p", "a"),
                    (MACHINE, Some("fuzz_a")),
                    &input
                ),
                None
            );
            rebuild_refusal(&repo, patch, "s", case.path())
        };
        assert_eq!(rebuilt_from("s"), None);
        let older = rebuilt_from("older");
        assert!(
            older
                .as_deref()
                .is_some_and(|why| why.contains("made from older")),
            "{older:?}"
        );
    }

    /// The committed fuzz-target mapping is read from the fuzz package, as
    /// the planted runner reads it.
    #[test]
    fn a_harness_s_fuzz_target_is_the_committed_one() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let targets = fuzz_targets(&repo).expect("the committed mapping reads");
        assert_eq!(
            target_of(&targets, MACHINE).as_deref(),
            Some("header_reactor_policy")
        );
        assert_eq!(target_of(&targets, "header-sync/wire"), None);
        // A mapping that's missing, isn't TOML, has no targets table or
        // maps a target to no harness name is an error, not an empty map.
        assert!(fuzz_targets(Path::new("/no/such/repo")).is_err());
        let malformed = tempfile::tempdir().expect("a temp dir can be created");
        let manifest = malformed.path().join("qa/fuzz/sim/Cargo.toml");
        fs::create_dir_all(manifest.parent().expect("it has a parent"))
            .expect("the temp dir is writable");
        for text in [
            "not = [toml",
            "[package]\nname = \"fuzz\"\n",
            "[package.metadata.zakura-sim.targets]\nheader_sync_storage = 1\n",
        ] {
            fs::write(&manifest, text).expect("the temp dir is writable");
            assert!(fuzz_targets(malformed.path()).is_err(), "{text}");
        }
        fs::write(
            &manifest,
            "[package.metadata.zakura-sim.targets]\nheader_sync_storage = \"header-sync/storage\"\n",
        )
        .expect("the temp dir is writable");
        let valid = fuzz_targets(malformed.path()).expect("a valid mapping reads");
        assert_eq!(target_of(&valid, "header-sync/wire"), None);

        // A storage case confirmed through no target: refused under the
        // committed mapping, which gives its harness one, and under one that
        // can't be read, and accepted only for a harness a valid mapping
        // gives none.
        let case = case_dir("s", "patched");
        let (verdict, untargeted) = case_checks(case.path(), None);
        write_checks(case.path(), &verdict, &untargeted);
        let input = CaseInput::Fuzz {
            input: MACHINE_INPUT.into(),
        };
        let refusal = |targets: &Result<BTreeMap<String, String>, String>, harness: &str| {
            mapped_case_refusal(targets, &repo, case.path(), ("p", "a"), harness, &input)
        };
        let storage = "header-sync/storage";
        let committed = Ok(targets);
        assert!(refusal(&committed, storage).is_some());
        let unreadable = fuzz_targets(Path::new("/no/such/repo"));
        assert!(refusal(&unreadable, storage).is_some());
        assert_eq!(refusal(&committed, "header-sync/wire"), None);
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
            write_witness(
                dir.path(),
                serde_json::json!({"passed": true, "property": property, "expect": expect, "error": null}),
            );
        };
        witness(MACHINE, "violated");
        assert!(witness_judges(dir.path(), MACHINE, "violated", &[]));
        assert!(!witness_judges(dir.path(), MACHINE, "holds", &[]));
        assert!(!witness_judges(
            dir.path(),
            "header/publisher-policy",
            "violated",
            &[]
        ));
        // A co-violation the case doesn't list isn't its witness.
        assert!(!witness_judges(
            dir.path(),
            MACHINE,
            "violated",
            &["p/b".to_owned()]
        ));
        fs::remove_file(dir.path().join("witness.json")).expect("the file exists");
        assert!(!witness_judges(dir.path(), MACHINE, "violated", &[]));
    }

    /// A planted case's patched witness is the one the planted runner
    /// records: the nested `target-page-refused` and `wedged-run-loop` cases
    /// name a `set`, so their tasks expect `violated-set` with exactly that
    /// set, a case naming `also_violates` expects `violated` beside it, and
    /// a `draft_set` case expects `violated` beside nothing. A patched run
    /// of the nested case whose witness judges its set is read with its
    /// trigger. The same witness under `violated`, with a member missing or
    /// with another one added, isn't its witness, and fails the run.
    #[test]
    fn a_nested_case_s_patched_witness_judges_its_exact_set() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&Manifest::committed(), &repo).expect("the committed manifest plans");
        let task = |property: &str, name: &str| {
            plan.tasks
                .iter()
                .find(|task| {
                    matches!(task, Task::Case { property: p, name: n, .. }
                        if p == property && n == name)
                })
                .unwrap_or_else(|| panic!("{property}/{name} has a task"))
        };
        let witness_of = |property: &str, name: &str| {
            let Task::Case { patched, .. } = task(property, name) else {
                unreachable!("found as a case");
            };
            patched.clone()
        };
        let flood = "header-sync/watch-observed-under-flood";
        let lifecycle = "header-sync/watch-observed-under-lifecycle-flood";
        assert_eq!(
            witness_of(flood, "watch-check-revert"),
            PatchedWitness {
                expect: "violated".into(),
                beside: vec![lifecycle.into()],
            }
        );
        assert_eq!(
            witness_of(flood, "wedged-run-loop"),
            PatchedWitness {
                expect: "violated-set".into(),
                beside: vec![lifecycle.into()],
            }
        );
        // A set judged on the Draft run leaves its patched runs the
        // property alone.
        assert_eq!(
            witness_of(
                "header-sync/s5v9/subscribe-headers/sender-within-credit",
                "page-past-credit"
            ),
            PatchedWitness {
                expect: "violated".into(),
                beside: Vec::new(),
            }
        );
        let Task::Case {
            property,
            harness,
            input,
            patched,
            ..
        } = task("session/honest-never-punished", "target-page-refused")
        else {
            unreachable!("found as a case");
        };
        let set = vec!["header-sync/s5v9/conformant-never-closes".to_owned()];
        assert_eq!(
            patched,
            &PatchedWitness {
                expect: "violated-set".into(),
                beside: set.clone(),
            }
        );
        let CaseInput::Fuzz { input: file } = input else {
            panic!("the nested case runs a committed fuzz input");
        };
        let case = tempfile::tempdir().expect("a temp dir can be created");
        let dir = case.path().join("patched");
        let bytes = fs::read(repo.join(file)).expect("the committed input is readable");
        let entry = crate::harness::find(harness).expect("the case's harness is registered");
        crate::harness::record(entry, crate::harness::RecordInput::Input(&bytes), &dir)
            .expect("the case's harness records its committed input");
        let checks = CaseChecks {
            repo: &repo,
            dir: case.path(),
            property,
            harness,
            input,
            subject_source: "s",
        };
        let read = |expect: &str, beside: &[String]| {
            write_witness(
                &dir,
                serde_json::json!({
                    "property": property,
                    "expect": expect,
                    "also_violates": beside,
                    "passed": true,
                    "error": null,
                }),
            );
            checks.planted(
                RunId::Case {
                    property: property.clone(),
                    name: "target-page-refused".into(),
                    run: CaseRun::Patched,
                },
                "patched",
                &patched.expect,
                &patched.beside,
            )
        };
        let caught = read("violated-set", &set);
        assert!(caught.trigger);
        assert!(
            !matches!(caught.end, End::Failed { .. }),
            "{:?}",
            caught.end
        );
        let mut wider = set.clone();
        wider.push("header-sync/honest-progress".into());
        for (expect, beside) in [
            ("violated", set.clone()),
            ("violated-set", Vec::new()),
            ("violated-set", wider),
        ] {
            let refused = read(expect, &beside);
            assert!(!refused.trigger, "{expect} {beside:?}");
            assert!(
                matches!(&refused.end, End::Failed { why } if why.contains("witness file")),
                "{expect} {beside:?}: {:?}",
                refused.end
            );
        }
    }

    /// A `Draft` run's companions count only from a file judging that
    /// run's own trace, by the build its provenance check measured: the
    /// same verdicts naming another trace's digest, or a build other than
    /// the one the trace records, show none.
    #[test]
    fn a_draft_run_s_companions_judge_its_own_trace() {
        let case = tempfile::tempdir().expect("a temp dir can be created");
        let dir = case.path().join("draft");
        record_into(&dir, "patched");
        fs::write(
            case.path().join(PROVENANCE_FILE),
            serde_json::to_string(&ProvenanceCheck {
                base: "s".into(),
                source: "patched".into(),
                patch_sha256: String::new(),
                refused: None,
            })
            .expect("a check serializes"),
        )
        .expect("the temp dir is writable");
        let draft = execution(&dir, history_run(), MACHINE, RunBy::Tool);
        let declared = ["c".to_owned()];
        let read = |trace: &str| {
            let _ = fs::remove_file(dir.join(COMPANIONS_FILE));
            let run = CompanionsRun {
                trace: trace.to_owned(),
                source: "patched".into(),
                companions: vec![CompanionVerdict {
                    name: "c".into(),
                    held: true,
                    why: None,
                }],
            };
            fs::write(
                dir.join(COMPANIONS_FILE),
                serde_json::to_string(&run).expect("a run serializes"),
            )
            .expect("the temp dir is writable");
            companions_of(case.path(), &draft, &declared)
        };
        assert_eq!(read(&draft.trace).len(), 1);
        assert!(read(&"0".repeat(64)).is_empty());
        // The measured build judged it, but the trace says another recorded
        // it.
        rewrite(&dir, |lines| {
            lines[0]["meta"]["build"]["source"] = Value::String("another".into());
        });
        let trace = crate::trace::TraceReader::open(&dir)
            .expect("the trace reads")
            .id;
        assert!(read(&trace).is_empty());
    }

    /// A typed planted case plans beside a fuzz-input one: each gets a task
    /// and a result, and its links name its scenario and Plan. A typed case's Plan is
    /// its bytes or its script. A case naming both forms, or neither, is
    /// refused, and so is a typed case naming both Plans.
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
            (
                "scripted",
                "[typed]\nharness = \"header/reactor-policy\"\nscenario = \"s.json\"\nscript = \"p.json\"\n",
            ),
            ("both", "input = \"in.bin\"\n[typed]\nharness = \"h\"\nscenario = \"s\"\nplan = \"p\"\n"),
            (
                "both-plans",
                "[typed]\nharness = \"h\"\nscenario = \"s\"\nplan = \"p\"\nscript = \"p.json\"\n",
            ),
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
            expected: vec![caught("fuzz"), caught("typed"), caught("scripted")],
            ..Manifest::committed()
        };
        let planned = plan(&manifest, repo.path()).expect("every case plans");
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
                &CaseInput::Scripted {
                    scenario: "s.json".into(),
                    script: "p.json".into()
                },
            ]
        );
        // C18 binds a typed case's trace to its scenario and Plan, so
        // every case gives a result.
        for name in ["fuzz", "typed", "scripted"] {
            let id = caught(name).expected.id();
            assert!(
                !planned
                    .unsupported
                    .iter()
                    .any(|(unsupported, _)| Some(unsupported) == id.as_ref()),
                "{name}"
            );
        }
        for refused in ["both", "both-plans"] {
            let both = Manifest {
                expected: vec![caught(refused)],
                ..Manifest::committed()
            };
            assert!(plan(&both, repo.path()).is_err(), "{refused}");
        }
    }

    /// Each history task records its target's witness: a `Holds` under
    /// `holds`, and a `StaysNotApplicable` under `not-applicable`, so its
    /// trigger history is recorded beside its target's `NotApplicable`.
    /// A `not-applicable` witness of the task's own property supplies its
    /// trigger, and one recorded under `holds` doesn't.
    #[test]
    fn a_stays_not_applicable_task_records_its_trigger() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let mut stays = 0;
        for task in &plan.tasks {
            if let Task::History {
                expected, expect, ..
            } = task
            {
                let wanted = match expected {
                    ExpectedId::StaysNotApplicable { .. } => {
                        stays += 1;
                        "not-applicable"
                    }
                    _ => "holds",
                };
                assert_eq!(expect.as_deref(), Some(wanted), "{expected:?}");
            }
        }
        assert_eq!(stays, 2);
        let read = |expect: &str| {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            let witness =
                serde_json::json!({ "property": "p/a", "expect": expect, "passed": true });
            history_task_dir(dir.path(), Some(witness));
            history_execution(
                dir.path(),
                history_run(),
                NODE,
                ("p/a", Some("not-applicable")),
                ("zakura-sim", "tests/header_node.rs", "test", "plan 0"),
                "s",
            )
        };
        assert!(read("not-applicable").trigger);
        assert!(!read("holds").trigger);
    }

    /// An artifacts directory counts only for the run of S it names: one
    /// with no `run.json`, or another subject's, gives no result, so every
    /// obligation blocks, and a task directory another attempt wrote gives
    /// its task none. A case's provenance check must be for its committed
    /// patch.
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
            fs::write(
                dir.join(TASK_FILE),
                task_binding("run-1", index, task).to_string(),
            )
            .expect("the temp dir is writable");
        };
        for (index, task) in plan.tasks.iter().enumerate() {
            write_task(index, task);
        }
        let all = results(artifacts.path(), &plan);
        assert!(all > 0);
        // A task directory kept from an earlier attempt at S, under this
        // run's `run.json`: only its run ID differs, and it gives no result
        // and links nothing.
        let kept = plan
            .tasks
            .iter()
            .position(|task| matches!(task, Task::Test { .. }))
            .expect("the plan has a test task");
        fs::write(
            artifacts.path().join(kept.to_string()).join(TASK_FILE),
            task_binding("run-0", kept, &plan.tasks[kept]).to_string(),
        )
        .expect("the temp dir is writable");
        assert_eq!(results(artifacts.path(), &plan), all - 1);
        let (_, record) = evaluate_artifacts(&repo, &plan, artifacts.path(), &run);
        let prefix = format!("{kept}/");
        assert!(![&record.verified, &record.excluded, &record.blocked]
            .into_iter()
            .flatten()
            .any(|entry| entry
                .links
                .artifacts
                .iter()
                .any(|path| path.starts_with(&prefix))));
        write_task(kept, &plan.tasks[kept]);
        assert_eq!(results(artifacts.path(), &plan), all);
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
            task_binding("run-1", b, &plan.tasks[a]).to_string(),
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
                base: "s".into(),
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
        assert_eq!(rebuild_refusal(&repo, patch, "s", case.path()), None);
        check("00".repeat(32));
        assert!(rebuild_refusal(&repo, patch, "s", case.path()).is_some());
        fs::remove_file(case.path().join(PROVENANCE_FILE)).expect("the file exists");
        assert!(rebuild_refusal(&repo, patch, "s", case.path()).is_some());
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

    /// Every task's keys link its evidence and its result: a node case its
    /// `Draft` run and companions, a history its declarations and
    /// recording, and an integrity mutation the history it ran.
    #[test]
    fn every_task_links_its_evidence() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let links = links(&manifest, &plan, |_, _| true);
        let (mut node, mut history, mut integrity) = (false, false, false);
        for (index, task) in plan.tasks.iter().enumerate() {
            let expected = task_expected(task);
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
            assert_eq!(found.result.as_ref(), Some(expected));
            match task {
                // A harness-caught case has no Draft run to link.
                Task::Case {
                    machine: false,
                    draft: None,
                    ..
                } => {
                    assert!(matches!(expected, ExpectedId::HarnessCaught(_)));
                    assert!(found
                        .artifacts
                        .contains(&format!("{index}/patched/trace.jsonl")));
                    assert!(!found.artifacts.iter().any(|a| a.contains("/draft/")));
                }
                Task::Case { machine: false, .. } => {
                    node = true;
                    for artifact in [
                        "patched/trace.jsonl",
                        "patched/witness.json",
                        "draft/trace.jsonl",
                        "draft/full-report.json",
                        "draft/companions.json",
                    ] {
                        assert!(
                            found.artifacts.contains(&format!("{index}/{artifact}")),
                            "{artifact}: {found:?}"
                        );
                    }
                }
                Task::History { .. } => {
                    history = true;
                    assert!(matches!(found.history, Some(RunId::History { .. })));
                    assert!(found
                        .artifacts
                        .contains(&format!("{index}/run/trace.jsonl")));
                }
                Task::Integrity { .. } => {
                    integrity = true;
                    assert!(matches!(found.history, Some(RunId::History { .. })));
                }
                _ => {}
            }
        }
        assert!(
            node && history && integrity,
            "the plan has a node case, a history and an integrity mutation"
        );
    }

    /// A digest the evaluator would accept as naming an execution.
    fn usable_digest(trace: &str) -> bool {
        trace.len() == 64 && trace.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// A test artifact's results: a killed child fails every test it ran,
    /// and an unreadable artifact ran nothing. Its binary's and checkout's
    /// sources are each carried as measured, even when they differ.
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
                checkout_source: Some("t".into()),
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
        // It carries both measured sources for the evaluator to judge.
        let Actual::Test {
            binary_source,
            checkout_source,
            ..
        } = test_actual(dir.path())
        else {
            unreachable!("a test artifact reads as a test result");
        };
        assert_eq!(
            (binary_source.as_deref(), checkout_source.as_deref()),
            (Some("source"), Some("t"))
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

    /// A storage history the integrity fixtures declare and record: the
    /// committed close-and-relink seed's scenario and Plan bytes.
    fn storage_history() -> (Value, Vec<u8>) {
        let input = fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/header-storage-close-relink.bin"),
        )
        .expect("the committed seed is readable");
        let entry = crate::harness::find(NODE).expect("the storage harness is registered");
        let split = entry.scenario_bytes.min(input.len());
        (entry.scenario(&input), input[split..].to_vec())
    }

    /// [`storage_history`] recorded once, by this build.
    fn storage_recording() -> &'static Path {
        static RECORDED: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        RECORDED
            .get_or_init(|| {
                let dir = tempfile::tempdir().expect("a temp dir can be created");
                let (scenario, plan) = storage_history();
                crate::harness::record(
                    crate::harness::find(NODE).expect("the storage harness is registered"),
                    crate::harness::RecordInput::Plan {
                        scenario: &scenario,
                        plan: &plan,
                    },
                    dir.path(),
                )
                .expect("the storage history records");
                dir
            })
            .path()
    }

    /// Writes into `dir` what the runner leaves for an integrity run whose
    /// test `test` declared [`storage_history`] as `case`: the declaration,
    /// and that history recorded by the build with source `source`, ending
    /// in the harness failure `failure` when one is given.
    fn declare_and_record(dir: &Path, test: &str, case: &str, source: &str, failure: Option<&str>) {
        let (scenario, plan) = storage_history();
        let declared = dir.join("declared");
        super::super::history::declare_into(
            &declared,
            NODE,
            case,
            &scenario,
            &crate::PlanInput::Bytes(plan),
        )
        .expect("the declaration writes");
        let identity = declared.join("0").join(super::super::history::HISTORY_FILE);
        let mut history: DeclaredHistory =
            serde_json::from_str(&fs::read_to_string(&identity).expect("it was written"))
                .expect("it reads back");
        history.test = format!("a::path::{test}");
        fs::write(
            &identity,
            serde_json::to_string(&history).expect("it serializes"),
        )
        .expect("the temp dir is writable");
        let recorded =
            crate::trace::TraceReader::open(storage_recording()).expect("the recording reads back");
        fs::write(
            declared.join("0").join(RAN_FILE),
            serde_json::to_string(&Ran::of(&recorded.steps)).expect("it serializes"),
        )
        .expect("the temp dir is writable");
        let recording = dir.join("history");
        fs::create_dir_all(&recording).expect("the temp dir is writable");
        for file in ["trace.jsonl", FULL_REPORT_FILE] {
            fs::copy(storage_recording().join(file), recording.join(file))
                .expect("the recording is copied");
        }
        rewrite(&recording, |lines| {
            lines[0]["meta"]["build"]["source"] = Value::String(source.to_owned());
            if let Some(failure) = failure {
                let end = &mut lines.last_mut().expect("the trace has an end")["end"];
                end["outcome"] = Value::String("harness".to_owned());
                end["harness"] = Value::String(failure.to_owned());
            }
        });
    }

    /// An integrity run's `Draft` override and declared actions are its
    /// recorded history's. A clean recording carries none and all of its
    /// actions. One recorded with an override carries it, so the
    /// evaluator's override check sees it, and one whose `ends` store
    /// doesn't show a control step's effect is incomplete, as is a run
    /// with no recording.
    #[test]
    fn an_integrity_run_carries_its_recording_s_override_and_actions() {
        let input = history_run();
        let read = |edit: &dyn Fn(&Path)| {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            declare_and_record(dir.path(), "a_test", "plan 0", "subject", None);
            edit(&dir.path().join("history"));
            let artifact = TestRun {
                package: "zakura-sim".into(),
                file: "tests/header_node.rs".into(),
                tests: vec![("a_test".into(), "ok".into())],
                killed: false,
                exited_ok: true,
                log: String::new(),
                binary_source: Some("subject".into()),
                checkout_source: Some("subject".into()),
            };
            fs::write(
                dir.path().join(TEST_FILE),
                serde_json::to_string(&artifact).expect("a test run serializes"),
            )
            .expect("the temp dir is writable");
            integrity_execution(
                dir.path(),
                RunId::native("raw-budget-lease", CaseRun::Unpatched),
                &input,
                NODE,
                "OwnedMismatch",
                "subject",
            )
        };
        let clean = read(&|_| {});
        assert_eq!(clean.meta.input.as_ref(), Some(&input), "{clean:?}");
        assert!(clean.meta.draft_override.is_empty());
        assert_eq!(clean.actions, Actions::Complete);
        let drafted = read(&|history| {
            rewrite(history, |lines| {
                lines[0]["meta"]["draft"] = serde_json::json!(["p/a"]);
            });
        });
        assert_eq!(drafted.meta.input.as_ref(), Some(&input), "{drafted:?}");
        assert_eq!(drafted.meta.draft_override, ["p/a"]);
        let unshown = read(&|history| {
            let path = history.join(FULL_REPORT_FILE);
            let text = fs::read_to_string(&path).expect("the report is readable");
            let relinked =
                "(21, Environment { peer: \\\"mirror-1\\\", connection: Some(4), action: Relink })";
            assert!(text.contains(relinked), "{text}");
            fs::write(&path, text.replace(relinked, "(21, Dropped)"))
                .expect("the temp dir is writable");
        });
        assert_eq!(unshown.meta.input.as_ref(), Some(&input), "{unshown:?}");
        assert_eq!(unshown.actions, Actions::Incomplete);
        let unrecorded = read(&|history| {
            fs::remove_dir_all(history).expect("the recording exists");
        });
        assert_eq!(unrecorded.actions, Actions::Incomplete);
    }

    /// A declaration binds to the run its test made through
    /// [`run_declared_in`](super::super::history::run_declared_in): the
    /// tool's recording of the declared scenario and Plan takes the steps the
    /// test's own run took. A test whose run used a Plan one byte off from
    /// the one it declared is refused, and so is a declaration no run
    /// followed.
    #[test]
    fn a_declaration_binds_to_the_run_its_test_made() {
        use super::super::history::run_declared_in;
        let entry = crate::harness::find(NODE).expect("the storage harness is registered");
        let (scenario, plan) = storage_history();
        let mut off = plan.clone();
        off[0] = off[0].wrapping_add(1);
        let declared = |ran: Option<Vec<u8>>| {
            let root = tempfile::tempdir().expect("a temp dir can be created");
            let declared = plan.clone();
            run_declared_in(
                Some(root.path()),
                NODE,
                "plan 0",
                &scenario,
                crate::PlanInput::Bytes(declared),
                |scenario, declared, sink| {
                    let Some(ran) = ran else {
                        return;
                    };
                    let input = match ran.as_slice() == plan.as_slice() {
                        true => declared,
                        false => crate::PlanInput::Bytes(ran),
                    };
                    (entry.run)(scenario, input, sink).expect("the history runs");
                },
            );
            let out = root.path().join("run");
            crate::harness::record(
                entry,
                crate::harness::RecordInput::Plan {
                    scenario: &scenario,
                    plan: &plan,
                },
                &out,
            )
            .expect("the declaration records");
            declared_refusal(NODE, &root.path().join("0"), &out)
        };
        assert_eq!(declared(Some(plan.clone())), None);
        let one_off = declared(Some(off));
        assert!(
            one_off
                .as_ref()
                .is_some_and(|why| why.contains("other steps")),
            "{one_off:?}"
        );
        let unrun = declared(None);
        assert!(
            unrun.as_ref().is_some_and(|why| why.contains("no steps")),
            "{unrun:?}"
        );
    }

    /// Writes into `dir` what the runner leaves for a history task whose
    /// test `test` declared [`storage_history`] as `plan 0`: the test's
    /// passing artifact from a binary embedding `s`, its declaration, and
    /// S's recording of it in `dir/run` with `witness` as its witness file.
    fn history_task_dir(dir: &Path, witness: Option<Value>) {
        declare_and_record(dir, "test", "plan 0", "s", None);
        fs::rename(dir.join("history"), dir.join("run")).expect("the temp dir is writable");
        let ran = TestRun {
            package: "zakura-sim".into(),
            file: "tests/header_node.rs".into(),
            tests: vec![("test".into(), "ok".into())],
            killed: false,
            exited_ok: true,
            log: String::new(),
            binary_source: Some("s".into()),
            checkout_source: Some("s".into()),
        };
        fs::write(
            dir.join(TEST_FILE),
            serde_json::to_string(&ran).expect("a test run serializes"),
        )
        .expect("the temp dir is writable");
        if let Some(witness) = witness {
            write_witness(&dir.join("run"), witness);
        }
    }

    /// C5, for a standalone witness history: its run counts only when the
    /// binary that recorded its trace and the one its witness test ran in
    /// both embed the subject's source digest, and that test ran in a
    /// checkout of S. A trace recorded by a binary built from other source,
    /// or one embedding the `unknown` marker, a witness test from a stale
    /// binary or one that embeds no digest, and one that ran in another
    /// checkout or one whose checkout wasn't measured, each fail the run,
    /// naming why. An unknown digest matches nothing, not even an unknown
    /// subject's.
    #[test]
    fn a_history_from_a_stale_binary_is_refused() {
        type Measured<'a> = Option<Option<&'a str>>;
        let read =
            |subject: &str, trace: Option<&str>, binary: Option<&str>, checkout: Measured| {
                let dir = tempfile::tempdir().expect("a temp dir can be created");
                history_task_dir(dir.path(), None);
                if let Some(source) = trace {
                    rewrite(&dir.path().join("run"), |lines| {
                        lines[0]["meta"]["build"]["source"] = Value::String(source.to_owned());
                    });
                }
                if let Some(source) = binary {
                    let path = dir.path().join(TEST_FILE);
                    let mut ran: TestRun =
                        serde_json::from_str(&fs::read_to_string(&path).expect("it was written"))
                            .expect("it reads back");
                    ran.binary_source = Some(source.to_owned());
                    fs::write(&path, serde_json::to_string(&ran).expect("it serializes"))
                        .expect("the temp dir is writable");
                }
                if let Some(source) = checkout {
                    let path = dir.path().join(TEST_FILE);
                    let mut ran: TestRun =
                        serde_json::from_str(&fs::read_to_string(&path).expect("it was written"))
                            .expect("it reads back");
                    ran.checkout_source = source.map(str::to_owned);
                    fs::write(&path, serde_json::to_string(&ran).expect("it serializes"))
                        .expect("the temp dir is writable");
                }
                history_execution(
                    dir.path(),
                    history_run(),
                    NODE,
                    ("p/a", None),
                    ("zakura-sim", "tests/header_node.rs", "test", "plan 0"),
                    subject,
                )
                .end
            };
        let fresh = read("s", None, None, None);
        assert!(!matches!(fresh, End::Failed { .. }), "{fresh:?}");
        let refused = |end: End, why: &str| {
            assert!(
                matches!(&end, End::Failed { why: found } if found.contains(why)),
                "{why}: {end:?}"
            );
        };
        let stale_trace = "its trace's build source isn't the subject's";
        let stale_test = "its witness test zakura-sim/tests/header_node.rs::test didn't pass at S";
        refused(read("s", Some("stale"), None, None), stale_trace);
        refused(read("s", Some("unknown"), None, None), stale_trace);
        refused(read("s", None, Some("stale"), None), stale_test);
        refused(read("s", None, Some(NOT_EMBEDDED), None), stale_test);
        // A binary of S run in a checkout that has since moved, or whose
        // checkout wasn't measured: only `checkout_source` differs.
        refused(read("s", None, None, Some(Some("t"))), stale_test);
        refused(read("s", None, None, Some(None)), stale_test);
        // Unknown on both sides, the subject's and every binary's.
        refused(
            read("unknown", Some("unknown"), Some("unknown"), None),
            stale_trace,
        );
    }

    /// A counted witness test's declarations must be exactly the counted
    /// histories the committed inventory names for it. The storage
    /// witness's 32 plans declared once each pass. One missing, one extra,
    /// one declared twice and one declared by another test each fail, and a
    /// test the inventory names nothing for isn't checked.
    #[test]
    fn a_counted_test_declares_exactly_its_inventory() {
        let (file, test) = (
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        );
        let inventory = inventory_cases("zakura-sim", file, test);
        assert_eq!(inventory.len(), 32, "{inventory:?}");
        let declare = |cases: &[(&str, &str)]| -> Vec<(std::path::PathBuf, DeclaredHistory)> {
            cases
                .iter()
                .map(|(by, case)| {
                    (
                        std::path::PathBuf::new(),
                        DeclaredHistory {
                            test: format!("a::{by}"),
                            harness: NODE.to_owned(),
                            case: (*case).to_owned(),
                        },
                    )
                })
                .collect()
        };
        let all: Vec<(&str, &str)> = inventory
            .iter()
            .map(|(_, case)| (test, case.as_str()))
            .collect();
        assert_eq!(
            inventory_errors(&inventory, &declare(&all), test),
            Vec::<String>::new()
        );
        let errors =
            |cases: Vec<(&str, &str)>| inventory_errors(&inventory, &declare(&cases), test);
        let missing = errors(all[1..].to_vec());
        assert!(
            missing.len() == 1 && missing[0].contains("didn't declare"),
            "{missing:?}"
        );
        let mut extra = all.clone();
        extra.push((test, "plan 32 of inputs(32, 256)"));
        let extra = errors(extra);
        assert!(
            extra.len() == 1 && extra[0].contains("doesn't count"),
            "{extra:?}"
        );
        let mut twice = all.clone();
        twice.push(all[0]);
        let twice = errors(twice);
        assert!(twice.len() == 1 && twice[0].contains("twice"), "{twice:?}");
        let mut other = all.clone();
        other[0].0 = "another_test";
        let other = errors(other);
        assert!(
            other.iter().any(|error| error.contains("another_test")),
            "{other:?}"
        );
        assert!(inventory_cases("zakura-sim", file, "not_counted").is_empty());
        assert_eq!(
            inventory_errors(&BTreeSet::new(), &declare(&all), "not_counted"),
            Vec::<String>::new()
        );
    }

    /// A history's trigger comes only from a witness file judging the
    /// task's own property and expectation on the task's own trace. One a
    /// single field off, its property or its expectation, fails the run and
    /// shows no trigger, and a task with no expectation shows none from a
    /// stray passing file. A witness of the same property and expectation
    /// from another run, or one naming another build or `Draft` override,
    /// or no trace at all, fails the run too.
    #[test]
    fn a_history_s_trigger_comes_from_its_own_witness() {
        let read = |witness: Value, expect: Option<&str>| {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            history_task_dir(dir.path(), Some(witness));
            history_execution(
                dir.path(),
                history_run(),
                NODE,
                ("p/a", expect),
                ("zakura-sim", "tests/header_node.rs", "test", "plan 0"),
                "s",
            )
        };
        let judged = |property: &str, expect: &str| serde_json::json!({ "property": property, "expect": expect, "passed": true });
        let own = read(judged("p/a", "holds"), Some("holds"));
        assert!(own.trigger, "{:?}", own.end);
        assert!(!matches!(own.end, End::Failed { .. }), "{:?}", own.end);
        for other in [judged("p/b", "holds"), judged("p/a", "applicable")] {
            let run = read(other.clone(), Some("holds"));
            assert!(!run.trigger, "{other}");
            assert!(
                matches!(&run.end, End::Failed { why } if why.contains("doesn't judge p/a")),
                "{other}: {:?}",
                run.end
            );
        }
        let unexpected = read(judged("p/a", "holds"), None);
        assert!(!unexpected.trigger);
        assert!(!matches!(unexpected.end, End::Failed { .. }));
        // A passing witness of p/a under `holds`, bound to another run's
        // trace, copied beside this one.
        let other = recorded();
        let copied = write_witness(other.path(), judged("p/a", "holds"));
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        history_task_dir(dir.path(), None);
        let rebound = |witness: &Value| {
            fs::write(dir.path().join("run/witness.json"), witness.to_string())
                .expect("the temp dir is writable");
            history_execution(
                dir.path(),
                history_run(),
                NODE,
                ("p/a", Some("holds")),
                ("zakura-sim", "tests/header_node.rs", "test", "plan 0"),
                "s",
            )
        };
        let own = write_witness(&dir.path().join("run"), judged("p/a", "holds"));
        assert!(rebound(&own).trigger);
        assert_ne!(copied["trace"], own["trace"]);
        let mut unbound = own.clone();
        unbound.as_object_mut().expect("an object").remove("trace");
        let mut built = own.clone();
        built["source"] = Value::String("another build".into());
        let mut drafted = own.clone();
        drafted["draft"] = serde_json::json!(["p/a"]);
        for stale in [copied, unbound, built, drafted] {
            let run = rebound(&stale);
            assert!(!run.trigger, "{stale}");
            assert!(
                matches!(run.end, End::Failed { .. }),
                "{stale}: {:?}",
                run.end
            );
        }
    }

    /// Each integrity run's end is read from its test artifact: passing
    /// with a clean exit is completed, failing with the witness's own report
    /// of the diagnostic is that harness failure, and anything else didn't
    /// happen. An artifact one field off the history's test, in its package,
    /// file or name, isn't that run, and its binary must embed the checkout
    /// it ran in, S for the unpatched and restored runs and not S for the
    /// patched one. Each run claims its history, and takes its reports, only
    /// from the test's own declaration of it recorded by the same checkout,
    /// ending as the test did (C18, C8).
    #[test]
    fn an_integrity_run_is_read_from_its_artifact() {
        let input = history_run();
        let read =
            |run: CaseRun, test: (&str, &str, &str), result: &str, log: &str, source: &str| {
                let dir = tempfile::tempdir().expect("a temp dir can be created");
                let failure = (result == "FAILED").then_some(
                    "storage:header's evidence failed: OwnedMismatch: at a quiet boundary",
                );
                declare_and_record(dir.path(), "a_test", "plan 0", source, failure);
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
        assert_eq!(patched.meta.input.as_ref(), Some(&input));
        assert_eq!(
            patched.end,
            End::HarnessFailure {
                diagnostic: "OwnedMismatch".into()
            }
        );
        for run in [CaseRun::Unpatched, CaseRun::Restored] {
            let clean = read(run, own, "ok", "", "subject");
            assert_eq!(clean.end, End::Completed);
            assert_eq!(clean.meta.input.as_ref(), Some(&input));
            assert!(clean.trigger && !clean.reports.is_empty(), "{clean:?}");
        }
        // Without its declaration and recording, or with a declaration of
        // another case, by another test, or a recording from another
        // checkout or of another Plan, a run claims no history, and the
        // patched one didn't happen.
        let reread = |dir: &Path, run: CaseRun| {
            integrity_execution(
                dir,
                RunId::native("raw-budget-lease", run),
                &input,
                NODE,
                "OwnedMismatch",
                "subject",
            )
        };
        type Defect<'a> = (&'a str, &'a dyn Fn(&Path));
        let defects: [Defect; 5] = [
            ("nothing", &|dir| {
                fs::remove_dir_all(dir.join("declared")).expect("it exists");
                fs::remove_dir_all(dir.join("history")).expect("it exists");
            }),
            ("another case", &|dir| {
                let path = dir
                    .join("declared/0")
                    .join(super::super::history::HISTORY_FILE);
                let text = fs::read_to_string(&path).expect("it exists");
                fs::write(&path, text.replace("\"plan 0\"", "\"plan 1\""))
                    .expect("the temp dir is writable");
            }),
            ("another test", &|dir| {
                let path = dir
                    .join("declared/0")
                    .join(super::super::history::HISTORY_FILE);
                let text = fs::read_to_string(&path).expect("it exists");
                fs::write(&path, text.replace("::a_test", "::another_test"))
                    .expect("the temp dir is writable");
            }),
            ("another checkout", &|dir| {
                rewrite(&dir.join("history"), |lines| {
                    lines[0]["meta"]["build"]["source"] = Value::String("elsewhere".to_owned());
                });
            }),
            ("another Plan", &|dir| {
                fs::write(dir.join("declared/0").join(PLAN_FILE), [7u8; 3])
                    .expect("the temp dir is writable");
            }),
        ];
        for (defect, apply) in defects {
            for (run, result, log, source) in [
                (CaseRun::Unpatched, "ok", String::new(), "subject"),
                (
                    CaseRun::Patched,
                    "FAILED",
                    reported("OwnedMismatch"),
                    "patched",
                ),
            ] {
                let dir = tempfile::tempdir().expect("a temp dir can be created");
                let failure = (result == "FAILED").then_some(
                    "storage:header's evidence failed: OwnedMismatch: at a quiet boundary",
                );
                declare_and_record(dir.path(), "a_test", "plan 0", source, failure);
                let artifact = TestRun {
                    package: own.0.into(),
                    file: own.1.into(),
                    tests: vec![(own.2.into(), result.into())],
                    killed: false,
                    exited_ok: result == "ok",
                    log: log.clone(),
                    binary_source: Some(source.into()),
                    checkout_source: Some(source.into()),
                };
                fs::write(
                    dir.path().join(TEST_FILE),
                    serde_json::to_string(&artifact).expect("a test run serializes"),
                )
                .expect("the temp dir is writable");
                assert_eq!(reread(dir.path(), run).meta.input.as_ref(), Some(&input));
                apply(dir.path());
                let read = reread(dir.path(), run);
                assert_eq!(read.meta.input, None, "{defect}");
                if run == CaseRun::Patched {
                    assert!(matches!(read.end, End::Failed { .. }), "{defect}");
                }
            }
        }
        // A patched recording that ended clean, or failed with another
        // diagnostic, isn't the test's harness failure.
        for failure in [
            None,
            Some("storage:header's evidence failed: ReservedMismatch: x"),
        ] {
            let dir = tempfile::tempdir().expect("a temp dir can be created");
            declare_and_record(dir.path(), "a_test", "plan 0", "patched", failure);
            let artifact = TestRun {
                package: own.0.into(),
                file: own.1.into(),
                tests: vec![(own.2.into(), "FAILED".into())],
                killed: false,
                exited_ok: false,
                log: reported("OwnedMismatch"),
                binary_source: Some("patched".into()),
                checkout_source: Some("patched".into()),
            };
            fs::write(
                dir.path().join(TEST_FILE),
                serde_json::to_string(&artifact).expect("a test run serializes"),
            )
            .expect("the temp dir is writable");
            let read = reread(dir.path(), CaseRun::Patched);
            assert!(matches!(read.end, End::Failed { .. }), "{failure:?}");
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

    /// C5: an integrity catch counts only from a patched build of S with
    /// the mutation's committed patch, as the runner measured it, whose test
    /// binary, checkout and recorded history all come from that measured
    /// source. A previous patched directory for the same mutation, whose
    /// source differs from S but isn't the measured one, is refused, as is
    /// a patched run with no measurement or one of another base or patch.
    #[test]
    fn a_stale_integrity_build_is_refused() {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let plan = plan(&manifest, &repo).expect("the committed manifest plans");
        let run = RunBinding {
            run_id: "run-1".into(),
            subject: "s".into(),
            subject_source: "subject".into(),
            containment: Containment::Subreaper,
        };
        let (index, task) = plan
            .tasks
            .iter()
            .enumerate()
            .find(|(_, task)| matches!(task, Task::Integrity { .. }))
            .expect("the plan has an integrity mutation");
        let Task::Integrity {
            expected,
            patch_file,
            package,
            file,
            case,
            test,
            diagnostic,
            ..
        } = task
        else {
            unreachable!("it was found as one");
        };
        let committed = digest(&repo.join(patch_file));
        // The runner's three runs, the patched one from a checkout of
        // source `ran`, beside the patched build `build` it measured.
        let patched = |build: Option<PatchedBuild>, ran: &str| {
            let artifacts = tempfile::tempdir().expect("a temp dir can be created");
            fs::write(
                artifacts.path().join(RUN_FILE),
                serde_json::to_string(&run).expect("a binding serializes"),
            )
            .expect("the temp dir is writable");
            let dir = artifacts.path().join(index.to_string());
            fs::create_dir_all(&dir).expect("the temp dir is writable");
            fs::write(
                dir.join(TASK_FILE),
                task_binding(&run.run_id, index, task).to_string(),
            )
            .expect("the temp dir is writable");
            for (name, source, result) in [
                ("unpatched", "subject", "ok"),
                ("patched", ran, "FAILED"),
                ("restored", "subject", "ok"),
            ] {
                let at = dir.join(name);
                let failed = result == "FAILED";
                let failure =
                    format!("storage:header's evidence failed: {diagnostic}: at a quiet boundary");
                declare_and_record(&at, test, case, source, failed.then_some(&failure));
                let artifact = TestRun {
                    package: package.clone(),
                    file: file.clone(),
                    tests: vec![(test.clone(), result.into())],
                    killed: false,
                    exited_ok: !failed,
                    log: if failed {
                        format!("the run hit a harness failure: {failure}\n")
                    } else {
                        String::new()
                    },
                    binary_source: Some(source.into()),
                    checkout_source: Some(source.into()),
                };
                fs::write(
                    at.join(TEST_FILE),
                    serde_json::to_string(&artifact).expect("a test run serializes"),
                )
                .expect("the temp dir is writable");
            }
            if let Some(build) = build {
                fs::write(
                    dir.join("patched").join(PATCHED_BUILD_FILE),
                    serde_json::to_string(&build).expect("a build serializes"),
                )
                .expect("the temp dir is writable");
            }
            let results = assemble(&repo, &plan, artifacts.path(), &run).results;
            let record = results
                .into_iter()
                .find(|record| record.expected == *expected)
                .expect("the integrity task gives a result");
            let Actual::IntegrityCaught {
                unpatched,
                patched,
                restored,
            } = record.actual
            else {
                panic!("an integrity task gives an integrity result");
            };
            assert_eq!(
                (unpatched.end, restored.end),
                (End::Completed, End::Completed)
            );
            patched.end
        };
        let measured = |base: &str, patch_sha256: &str, source: &str| PatchedBuild {
            base: base.into(),
            patch_sha256: patch_sha256.into(),
            source: source.into(),
        };
        assert_eq!(
            patched(Some(measured("s", &committed, "patched")), "patched"),
            End::HarnessFailure {
                diagnostic: diagnostic.clone()
            }
        );
        for (defect, build, ran) in [
            (
                "a previous patched directory",
                Some(measured("s", &committed, "patched")),
                "patched-before",
            ),
            ("no measurement", None, "patched"),
            (
                "another base",
                Some(measured("older", &committed, "patched")),
                "patched",
            ),
            (
                "another patch",
                Some(measured("s", &"00".repeat(32), "patched")),
                "patched",
            ),
        ] {
            assert!(
                matches!(patched(build, ran), End::Failed { .. }),
                "{defect}"
            );
        }
    }

    /// A replay links the recorded run it replays and its fresh replay, in
    /// the task that records that run, by its property, name and role:
    /// another case's task, a role the task doesn't record, or a task whose
    /// directory isn't bound to it links only the run's identity.
    #[test]
    fn a_replay_links_its_run_s_trace() {
        let task = |name: &str, input: CaseInput, draft: bool| Task::Case {
            expected: ExpectedId::Passes {
                package: "p".into(),
                file: name.into(),
                test: name.into(),
            },
            property: "p".into(),
            name: name.into(),
            harness: MACHINE.into(),
            machine: !draft,
            input,
            patched: PatchedWitness {
                expect: "violated".into(),
                beside: Vec::new(),
            },
            patch: format!("qa/planted/p/{name}.patch"),
            draft: draft.then(|| vec!["p".to_owned()]),
            companions: Vec::new(),
        };
        let tasks = [
            task(
                "a",
                CaseInput::Fuzz {
                    input: "a.bin".into(),
                },
                false,
            ),
            task(
                "a-b",
                CaseInput::Typed {
                    scenario: "s.json".into(),
                    plan: "p.bin".into(),
                },
                true,
            ),
            task(
                "s",
                CaseInput::Scripted {
                    scenario: "s.json".into(),
                    script: "p.json".into(),
                },
                false,
            ),
        ];
        let all = |_: usize, _: &Task| true;
        let run = |name: &str, role| RunId::Case {
            property: "p".into(),
            name: name.into(),
            run: role,
        };
        let links = replay_links(&run("a", CaseRun::Patched), &tasks, &all);
        assert_eq!(
            links.artifacts,
            [
                "0/patched/trace.jsonl",
                "0/patched/full-report.json",
                "0/patched/replay/replay.json",
                "0/patched/replay/full-report.json"
            ]
        );
        assert_eq!(links.history, Some(run("a", CaseRun::Patched)));
        assert_eq!(links.harness.as_deref(), Some(MACHINE));
        assert_eq!(links.input.as_deref(), Some("a.bin"));
        assert_eq!(links.plan, None);
        let typed = replay_links(&run("a-b", CaseRun::Patched), &tasks, &all);
        assert_eq!(typed.input.as_deref(), Some("s.json"));
        assert_eq!(typed.plan.as_deref(), Some("p.bin"));
        let scripted = replay_links(&run("s", CaseRun::Patched), &tasks, &all);
        assert_eq!(scripted.plan.as_deref(), Some("p.json"));
        assert_eq!(
            replay_links(&run("a-b", CaseRun::Draft), &tasks, &all).artifacts[0],
            "1/draft/trace.jsonl"
        );
        // A machine case records no Draft run, another case isn't it, and
        // an unbound directory links nothing.
        for unlinked in [
            replay_links(&run("a", CaseRun::Draft), &tasks, &all),
            replay_links(&run("c", CaseRun::Patched), &tasks, &all),
            replay_links(&run("a", CaseRun::Patched), &tasks, &|_, _| false),
        ] {
            assert!(unlinked.artifacts.is_empty() && unlinked.result.is_none());
        }
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
