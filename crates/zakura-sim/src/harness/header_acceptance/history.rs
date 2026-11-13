//! Witness-test history reconciliation (C18, hdr-14.7b).
//!
//! A manifest history is named by its witness test and a case within it,
//! such as `plan 0 of inputs(16, 256)`. A witness test builds its scenario
//! and Plan in code and may run many, so the test's name alone doesn't say
//! which run is which. Each counted witness test runs each history through
//! [`run_declared`], which declares the exact scenario and Plan and then
//! hands those same values, with a sink, to the test's own run. When the
//! acceptance runner sets [`HISTORIES_VAR`], every declaration is written
//! under that directory, stamped with the run, task and role the runner
//! names in [`HISTORIES_FOR_VAR`], with [`RAN_FILE`], the steps the test's run took,
//! so the tool can record the declared history itself with `zakura-sim
//! record`, bind the recorded trace to it by digest, and require the
//! recording to take the steps the test's own run took. Without the
//! variable, [`run_declared`] only runs.
//!
//! The directory holds one subdirectory per declaration, numbered from 0 in
//! the order the test made them. Each holds [`HISTORY_FILE`], a
//! [`DeclaredHistory`], which the tool reads only as its own run's
//! ([`declared`], [`binding::DECLARATION`]), the scenario as [`SCENARIO_FILE`], the Plan as
//! either [`PLAN_FILE`], Plan bytes for `zakura-sim record --plan`, or
//! [`SCRIPT_FILE`], a scripted Plan for `--script`, and, once the run
//! ended, [`RAN_FILE`].

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::binding::{self, Expect};
use crate::{record::StepRecord, PlanInput, Record, RecordSink};

/// The variable naming the directory [`run_declared`] writes under.
pub const HISTORIES_VAR: &str = "ZAKURA_SIM_HISTORIES";
/// The variable naming, as a [`DeclaredFor`] in JSON, the run, task and
/// role the runner ran the declaring test for, which every declaration
/// under [`HISTORIES_VAR`] carries.
pub const HISTORIES_FOR_VAR: &str = "ZAKURA_SIM_HISTORIES_FOR";
/// Each declaration's identity, a [`DeclaredHistory`].
pub const HISTORY_FILE: &str = "history.json";
/// Each declaration's scenario, as the harness's JSON.
pub const SCENARIO_FILE: &str = "scenario.json";
/// A declaration's Plan bytes.
pub const PLAN_FILE: &str = "plan.bin";
/// A declaration's scripted Plan, as JSON.
pub const SCRIPT_FILE: &str = "script.json";
/// The steps the declaring test's own run took, a [`Ran`], written when
/// that run's `end` record arrives.
pub const RAN_FILE: &str = "ran.json";

/// The run, task and role a declaring test ran for, as the runner names
/// them in [`HISTORIES_FOR_VAR`] and its other artifacts carry them
/// ([`binding::TEST`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredFor {
    /// The acceptance run's ID.
    pub run_id: String,
    /// The task's index in its plan.
    pub index: usize,
    /// The test's directory in its task's, empty for the task's own.
    pub role: String,
}

/// What one declaration's [`HISTORY_FILE`] holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredHistory {
    /// The acceptance run's ID, from [`HISTORIES_FOR_VAR`].
    pub run_id: String,
    /// The task's index in its plan, from [`HISTORIES_FOR_VAR`].
    pub index: usize,
    /// The test's role in its task, from [`HISTORIES_FOR_VAR`].
    pub role: String,
    /// The test that declared it, as libtest names its thread: the test's
    /// path within its file's crate.
    pub test: String,
    /// The harness it runs on.
    pub harness: String,
    /// Its case, as the manifest's [`HistoryRef::case`](super::HistoryRef::case)
    /// names it.
    pub case: String,
    /// The SHA-256 of the [`SCENARIO_FILE`] it wrote.
    pub scenario_sha256: String,
    /// The SHA-256 of the [`PLAN_FILE`] or [`SCRIPT_FILE`] it wrote.
    pub plan_sha256: String,
}

/// What [`RAN_FILE`] holds: how many steps a run took and the digest of
/// each step's pick, observation digest and typed machine evidence, in
/// order. A machine step has no observation digest and its pick names only
/// the machine and operation number, so its evidence is what tells two
/// same-length machine runs apart. A step's input offset and draws are left
/// out, since a full fuzz input and its split scenario and Plan bytes place
/// the same picks at different offsets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ran {
    /// The steps the run took.
    pub steps: u64,
    /// The SHA-256, in hex, of each step's `pick`, its `obs` and its
    /// `machine` evidence as JSON, tab separated, one line per step.
    pub digest: String,
}

impl Ran {
    /// The [`Ran`] of `steps`, a run's step records in order.
    pub fn of<'a>(steps: impl IntoIterator<Item = &'a StepRecord>) -> Ran {
        let mut hasher = Sha256::new();
        let mut count = 0u64;
        for step in steps {
            hasher.update(step_line(step));
            count += 1;
        }
        Ran {
            steps: count,
            digest: hex(hasher),
        }
    }
}

/// One step's line in a [`Ran`] digest. Machine evidence is plain structs
/// and enums, so its JSON is the same wherever it's written.
fn step_line(step: &StepRecord) -> String {
    let machine = step
        .machine
        .as_deref()
        .map(|machine| serde_json::to_string(machine).expect("machine evidence serializes"))
        .unwrap_or_default();
    format!(
        "{}\t{}\t{machine}\n",
        step.pick,
        step.obs.as_deref().unwrap_or("")
    )
}

/// `hasher`'s digest in lowercase hex.
fn hex(hasher: Sha256) -> String {
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Runs the calling witness test's history `case` on `harness`: declares
/// `plan` on `scenario` when [`HISTORIES_VAR`] names a directory, for the
/// run, task and role [`HISTORIES_FOR_VAR`] names, then calls
/// `run` with those same two values and a sink the run must record into.
/// The sink keeps the steps the run took and writes them as the
/// declaration's [`RAN_FILE`] at the run's `end`, so a test that declared
/// one history and ran another is refused by the tool, which requires its
/// recording of the declaration to take those steps. A Plan that's neither
/// bytes nor a script names no committed input, so it can't be declared,
/// and neither can a write that fails, a declaration with no readable
/// [`HISTORIES_FOR_VAR`] to name its run, and a sink that sees a second run
/// can't say which was declared: each panics, which fails the test, so a
/// run never claims a history it didn't declare.
pub fn run_declared<Sc: Serialize, R>(
    harness: &str,
    case: &str,
    scenario: &Sc,
    plan: PlanInput,
    run: impl FnOnce(&Sc, PlanInput, &mut dyn RecordSink) -> R,
) -> R {
    let root = std::env::var_os(HISTORIES_VAR).map(PathBuf::from);
    let declared_for = root.as_ref().map(|root| {
        let named = std::env::var(HISTORIES_FOR_VAR).unwrap_or_else(|error| {
            panic!(
                "declaring under {} needs {HISTORIES_FOR_VAR}: {error}",
                root.display()
            )
        });
        serde_json::from_str::<DeclaredFor>(&named)
            .unwrap_or_else(|error| panic!("{HISTORIES_FOR_VAR} {named:?}: {error}"))
    });
    let at = root.as_deref().zip(declared_for.as_ref());
    run_declared_in(at, harness, case, scenario, plan, run)
}

/// [`run_declared`], declaring under `root`'s directory for its run, task
/// and role rather than under the ones [`HISTORIES_VAR`] and
/// [`HISTORIES_FOR_VAR`] name, and only running when it's `None`.
pub fn run_declared_in<Sc: Serialize, R>(
    root: Option<(&Path, &DeclaredFor)>,
    harness: &str,
    case: &str,
    scenario: &Sc,
    plan: PlanInput,
    run: impl FnOnce(&Sc, PlanInput, &mut dyn RecordSink) -> R,
) -> R {
    let dir = root.map(|(root, declared_for)| {
        declare_into(root, declared_for, harness, case, scenario, &plan).unwrap_or_else(|error| {
            panic!("declaring history {case} under {}: {error}", root.display())
        })
    });
    let mut sink = RanSink {
        dir,
        hasher: Sha256::new(),
        steps: 0,
        ended: false,
    };
    run(scenario, plan, &mut sink)
}

/// The sink [`run_declared`] hands a declared run: it keeps the run's steps
/// and writes them to its declaration's [`RAN_FILE`] at the `end` record.
/// With no declaration, it keeps nothing.
struct RanSink {
    /// The declaration's directory, or `None` when nothing is declared.
    dir: Option<PathBuf>,
    hasher: Sha256,
    steps: u64,
    ended: bool,
}

impl RecordSink for RanSink {
    /// Folds a step into the digest, and writes [`RAN_FILE`] at the `end`.
    fn record(&mut self, record: Record) {
        let Some(dir) = &self.dir else {
            return;
        };
        match record {
            Record::Step(step) => {
                self.hasher.update(step_line(&step));
                self.steps += 1;
            }
            Record::End(_) => {
                assert!(
                    !self.ended,
                    "a declared history is one run, and {} saw a second",
                    dir.display()
                );
                self.ended = true;
                let ran = Ran {
                    steps: self.steps,
                    digest: hex(self.hasher.clone()),
                };
                fs::write(
                    dir.join(RAN_FILE),
                    serde_json::to_string(&ran).expect("a Ran serializes"),
                )
                .unwrap_or_else(|error| panic!("{}: {error}", dir.display()));
            }
            Record::Meta(_) | Record::Init(_) => {}
        }
    }

    /// Steps only when there's a declaration to write them for.
    fn wants_steps(&self) -> bool {
        self.dir.is_some()
    }
}

/// Writes one declaration for `declared_for`'s run, task and role, as
/// [`run_declared`] does, into the next free numbered directory under
/// `root`, creating `root`, and returns that directory. A directory is
/// taken with `create_dir`, so two tests declaring at once never share one,
/// and the numbers run from 0 with no gap.
pub fn declare_into(
    root: &Path,
    declared_for: &DeclaredFor,
    harness: &str,
    case: &str,
    scenario: &impl Serialize,
    plan: &PlanInput,
) -> Result<PathBuf, String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let dir = (0u64..)
        .map(|index| root.join(index.to_string()))
        .find(|dir| fs::create_dir(dir).is_ok())
        .expect("some index is free");
    let put = |name: &str, bytes: &[u8]| {
        fs::write(dir.join(name), bytes).map_err(|error| format!("{name}: {error}"))
    };
    let json = |value: serde_json::Result<String>| value.map_err(|error| error.to_string());
    let digest = |bytes: &[u8]| {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex(hasher)
    };
    let plan_sha256 = match plan {
        PlanInput::Bytes(bytes) => {
            put(PLAN_FILE, bytes)?;
            digest(bytes)
        }
        PlanInput::Script(script) => {
            let text = json(serde_json::to_string(script))?;
            put(SCRIPT_FILE, text.as_bytes())?;
            digest(text.as_bytes())
        }
        PlanInput::Trace(_) | PlanInput::Skip(_) => {
            return Err("only Plan bytes or a script name a committed input".to_owned())
        }
    };
    let scenario = json(serde_json::to_string(scenario))?;
    put(SCENARIO_FILE, scenario.as_bytes())?;
    let identity = DeclaredHistory {
        scenario_sha256: digest(scenario.as_bytes()),
        plan_sha256,
        run_id: declared_for.run_id.clone(),
        index: declared_for.index,
        role: declared_for.role.clone(),
        test: std::thread::current().name().unwrap_or_default().to_owned(),
        harness: harness.to_owned(),
        case: case.to_owned(),
    };
    put(
        HISTORY_FILE,
        json(serde_json::to_string(&identity))?.as_bytes(),
    )?;
    Ok(dir)
}

/// One declaration a witness test made for the run, task and role its
/// reader expects, read only by [`declared`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declaration {
    dir: PathBuf,
    history: DeclaredHistory,
    scenario: String,
}

impl Declaration {
    /// Its directory, which holds its Plan file and [`RAN_FILE`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Its [`HISTORY_FILE`].
    pub fn history(&self) -> &DeclaredHistory {
        &self.history
    }

    /// Its scenario's JSON text, as [`SCENARIO_FILE`] holds it.
    pub fn scenario(&self) -> &str {
        &self.scenario
    }
}

/// Every declaration under `root`, in the order they were made, each bound
/// to the run, task and role `expect` names ([`binding::read_declaration`]),
/// or why one isn't: a declaration another run, task or role made, or one
/// that can't be read, refuses them all, so no stale or broken directory
/// beside them names a history. No `root` is no declaration.
pub fn declared(root: &Path, expect: &Expect) -> Result<Vec<Declaration>, String> {
    (0u64..)
        .map(|index| root.join(index.to_string()))
        .take_while(|dir| dir.is_dir())
        .map(|dir| {
            let (history, scenario) = binding::read_declaration(&dir, expect)
                .map_err(|why| format!("declaration {}: {why}", dir.display()))?;
            let history = serde_json::from_value(history)
                .map_err(|error| format!("declaration {}: {error}", dir.display()))?;
            Ok(Declaration {
                dir,
                history,
                scenario,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run, task and role the fixtures declare for.
    fn task() -> DeclaredFor {
        DeclaredFor {
            run_id: "r".into(),
            index: 3,
            role: "patched".into(),
        }
    }

    /// What a reader of [`task`]'s declarations expects them to carry.
    fn expect(at: &DeclaredFor) -> Expect {
        Expect::new()
            .with(binding::Field::RunId, &at.run_id)
            .with(binding::Field::TaskIndex, at.index)
            .with(binding::Field::Role, &at.role)
    }

    /// Declarations read back in the order they were made, each with its
    /// scenario and its one Plan form, named by the declaring test's
    /// thread and stamped with the run, task and role it was made for. A
    /// malformed declaration refuses them all.
    #[test]
    fn declarations_read_back_in_order() {
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let scenario = serde_json::json!({ "peers": 2 });
        declare_into(
            root.path(),
            &task(),
            "h",
            "a",
            &scenario,
            &PlanInput::Bytes(vec![1, 2]),
        )
        .unwrap();
        let script = crate::Plan::default();
        declare_into(
            root.path(),
            &task(),
            "h",
            "b",
            &scenario,
            &PlanInput::Script(script.clone()),
        )
        .unwrap();
        let found = declared(root.path(), &expect(&task())).unwrap();
        let cases: Vec<&str> = found.iter().map(|d| d.history().case.as_str()).collect();
        assert_eq!(cases, ["a", "b"]);
        let first = &found[0];
        assert!(first
            .history()
            .test
            .ends_with("declarations_read_back_in_order"));
        assert_eq!(
            (
                first.history().run_id.as_str(),
                first.history().index,
                first.history().role.as_str()
            ),
            ("r", 3, "patched")
        );
        assert_eq!(fs::read(first.dir().join(PLAN_FILE)).unwrap(), [1, 2]);
        assert!(!first.dir().join(SCRIPT_FILE).exists());
        let read: serde_json::Value = serde_json::from_str(first.scenario()).unwrap();
        assert_eq!(read, scenario);
        let second: crate::Plan =
            serde_json::from_slice(&fs::read(found[1].dir().join(SCRIPT_FILE)).unwrap()).unwrap();
        assert_eq!(second, script);
        assert!(declared(&root.path().join("missing"), &expect(&task()))
            .unwrap()
            .is_empty());
        fs::write(found[1].dir().join(HISTORY_FILE), "not json").unwrap();
        assert!(declared(root.path(), &expect(&task())).is_err());
    }

    /// A declaration counts only for the run, task and role it was made
    /// for: its reader's expectation with any one of them moved refuses it
    /// (Rule 25's one-key-off controls), and so does one declaration
    /// another attempt left among this one's, otherwise whole.
    #[test]
    fn a_declaration_counts_only_for_its_own_run() {
        use super::super::refs::one_key_off::assert_one_key_off;
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let scenario = serde_json::json!({ "peers": 2 });
        let declare = |at: &DeclaredFor, case: &str| {
            declare_into(
                root.path(),
                at,
                "h",
                case,
                &scenario,
                &PlanInput::Bytes(vec![1]),
            )
            .unwrap()
        };
        declare(&task(), "a");
        let keys = assert_one_key_off(&task(), |at| declared(root.path(), &expect(at)).is_ok());
        assert_eq!(keys, 3);
        let earlier = DeclaredFor {
            run_id: "an earlier attempt".into(),
            ..task()
        };
        declare(&earlier, "b");
        let refused = declared(root.path(), &expect(&task()));
        assert!(
            refused
                .as_ref()
                .is_err_and(|why| why.contains("names the run an earlier attempt")),
            "{refused:?}"
        );
    }

    /// Two machine runs with the same picks and step count but a different
    /// operation have different [`Ran`]s, so one can't stand for the other.
    #[test]
    fn machine_operations_change_the_ran_digest() {
        use crate::{
            plan::By,
            record::machine::{FixtureStep, MachineStep},
        };
        let step = |input: u8| StepRecord {
            step: 1,
            pick: "machine:fixture:1".into(),
            key: None,
            options: 1,
            by: By::Forced,
            offset: None,
            draws: Vec::new(),
            obs: None,
            machine: Some(Box::new(MachineStep::Fixture(FixtureStep {
                input: vec![input],
                failure: None,
            }))),
        };
        let first = Ran::of(&[step(0)]);
        assert_eq!(first, Ran::of(&[step(0)]));
        let other = Ran::of(&[step(1)]);
        assert_eq!(first.steps, other.steps);
        assert_ne!(first, other);
    }
}
