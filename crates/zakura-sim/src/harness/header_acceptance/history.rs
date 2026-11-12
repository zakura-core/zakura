//! Witness-test history reconciliation (C18, hdr-14.7b).
//!
//! A manifest history is named by its witness test and a case within it,
//! such as `plan 0 of inputs(16, 256)`. A witness test builds its scenario
//! and Plan in code and may run many, so the test's name alone doesn't say
//! which run is which. Each counted witness test runs each history through
//! [`run_declared`], which declares the exact scenario and Plan and then
//! hands those same values, with a sink, to the test's own run. When the
//! acceptance runner sets [`HISTORIES_VAR`], every declaration is written
//! under that directory, with [`RAN_FILE`], the steps the test's run took,
//! so the tool can record the declared history itself with `zakura-sim
//! record`, bind the recorded trace to it by digest, and require the
//! recording to take the steps the test's own run took. Without the
//! variable, [`run_declared`] only runs.
//!
//! The directory holds one subdirectory per declaration, numbered from 0 in
//! the order the test made them. Each holds [`HISTORY_FILE`], a
//! [`DeclaredHistory`], the scenario as [`SCENARIO_FILE`], the Plan as
//! either [`PLAN_FILE`], Plan bytes for `zakura-sim record --plan`, or
//! [`SCRIPT_FILE`], a scripted Plan for `--script`, and, once the run
//! ended, [`RAN_FILE`].

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{record::StepRecord, PlanInput, Record, RecordSink};

/// The variable naming the directory [`run_declared`] writes under.
pub const HISTORIES_VAR: &str = "ZAKURA_SIM_HISTORIES";
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

/// What one declaration's [`HISTORY_FILE`] holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredHistory {
    /// The test that declared it, as libtest names its thread: the test's
    /// path within its file's crate.
    pub test: String,
    /// The harness it runs on.
    pub harness: String,
    /// Its case, as the manifest's [`HistoryRef::case`](super::HistoryRef::case)
    /// names it.
    pub case: String,
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
/// `plan` on `scenario` when [`HISTORIES_VAR`] names a directory, then calls
/// `run` with those same two values and a sink the run must record into.
/// The sink keeps the steps the run took and writes them as the
/// declaration's [`RAN_FILE`] at the run's `end`, so a test that declared
/// one history and ran another is refused by the tool, which requires its
/// recording of the declaration to take those steps. A Plan that's neither
/// bytes nor a script names no committed input, so it can't be declared,
/// and neither can a write that fails, and a sink that sees a second run
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
    run_declared_in(root.as_deref(), harness, case, scenario, plan, run)
}

/// [`run_declared`], declaring under `root` rather than under the directory
/// [`HISTORIES_VAR`] names, and only running when it's `None`.
pub fn run_declared_in<Sc: Serialize, R>(
    root: Option<&Path>,
    harness: &str,
    case: &str,
    scenario: &Sc,
    plan: PlanInput,
    run: impl FnOnce(&Sc, PlanInput, &mut dyn RecordSink) -> R,
) -> R {
    let dir = root.map(|root| {
        declare_into(root, harness, case, scenario, &plan).unwrap_or_else(|error| {
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

/// Writes one declaration, as [`run_declared`] does, into the next free
/// numbered directory under `root`, creating `root`, and returns that
/// directory. A directory is taken with `create_dir`, so two tests
/// declaring at once never share one.
pub fn declare_into(
    root: &Path,
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
    match plan {
        PlanInput::Bytes(bytes) => put(PLAN_FILE, bytes)?,
        PlanInput::Script(script) => {
            put(SCRIPT_FILE, json(serde_json::to_string(script))?.as_bytes())?
        }
        PlanInput::Trace(_) | PlanInput::Skip(_) => {
            return Err("only Plan bytes or a script name a committed input".to_owned())
        }
    }
    put(
        SCENARIO_FILE,
        json(serde_json::to_string(scenario))?.as_bytes(),
    )?;
    let identity = DeclaredHistory {
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

/// Every declaration under `root` with its directory, in the order they
/// were made. An unreadable or malformed one is left out, so it never
/// names a history.
pub fn declared(root: &Path) -> Vec<(PathBuf, DeclaredHistory)> {
    let mut found: Vec<(u64, PathBuf, DeclaredHistory)> = fs::read_dir(root)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let index = entry.file_name().to_str()?.parse::<u64>().ok()?;
            let dir = entry.path();
            let text = fs::read_to_string(dir.join(HISTORY_FILE)).ok()?;
            Some((index, dir, serde_json::from_str(&text).ok()?))
        })
        .collect();
    found.sort_by_key(|(index, ..)| *index);
    found
        .into_iter()
        .map(|(_, dir, history)| (dir, history))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Declarations read back in the order they were made, each with its
    /// scenario and its one Plan form, named by the declaring test's
    /// thread. A Plan that names no committed input isn't declared, and a
    /// malformed declaration names nothing.
    #[test]
    fn declarations_read_back_in_order() {
        let root = tempfile::tempdir().expect("a temp dir can be created");
        let scenario = serde_json::json!({ "peers": 2 });
        declare_into(
            root.path(),
            "h",
            "a",
            &scenario,
            &PlanInput::Bytes(vec![1, 2]),
        )
        .unwrap();
        let script = crate::Plan::default();
        declare_into(
            root.path(),
            "h",
            "b",
            &scenario,
            &PlanInput::Script(script.clone()),
        )
        .unwrap();
        let found = declared(root.path());
        let cases: Vec<&str> = found.iter().map(|(_, h)| h.case.as_str()).collect();
        assert_eq!(cases, ["a", "b"]);
        let (first, history) = &found[0];
        assert!(history.test.ends_with("declarations_read_back_in_order"));
        assert_eq!(fs::read(first.join(PLAN_FILE)).unwrap(), [1, 2]);
        assert!(!first.join(SCRIPT_FILE).exists());
        let read: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(first.join(SCENARIO_FILE)).unwrap()).unwrap();
        assert_eq!(read, scenario);
        let second: crate::Plan =
            serde_json::from_slice(&fs::read(found[1].0.join(SCRIPT_FILE)).unwrap()).unwrap();
        assert_eq!(second, script);
        fs::write(found[1].0.join(HISTORY_FILE), "not json").unwrap();
        assert_eq!(declared(root.path()).len(), 1);
        assert!(declared(&root.path().join("missing")).is_empty());
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
