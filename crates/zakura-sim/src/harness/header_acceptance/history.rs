//! Witness-test history reconciliation (C18, hdr-14.7b).
//!
//! A manifest history is named by its witness test and a case within it,
//! such as `plan 0 of inputs(16, 256)`. A witness test builds its scenario
//! and Plan in code and may run many, so the test's name alone doesn't say
//! which run is which. Each counted witness test calls [`declare`] with the
//! exact scenario and Plan it's about to run. When the acceptance runner
//! sets [`HISTORIES_VAR`], every declaration is written under that
//! directory, so the tool can record the declared history itself with
//! `zakura-sim record` and bind the recorded trace to it by digest. Without
//! the variable, [`declare`] does nothing.
//!
//! The directory holds one subdirectory per declaration, numbered from 0 in
//! the order the test made them. Each holds [`HISTORY_FILE`], a
//! [`DeclaredHistory`], the scenario as [`SCENARIO_FILE`], and the Plan as
//! either [`PLAN_FILE`], Plan bytes for `zakura-sim record --plan`, or
//! [`SCRIPT_FILE`], a scripted Plan for `--script`.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::PlanInput;

/// The variable naming the directory [`declare`] writes under.
pub const HISTORIES_VAR: &str = "ZAKURA_SIM_HISTORIES";
/// Each declaration's identity, a [`DeclaredHistory`].
pub const HISTORY_FILE: &str = "history.json";
/// Each declaration's scenario, as the harness's JSON.
pub const SCENARIO_FILE: &str = "scenario.json";
/// A declaration's Plan bytes.
pub const PLAN_FILE: &str = "plan.bin";
/// A declaration's scripted Plan, as JSON.
pub const SCRIPT_FILE: &str = "script.json";

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

/// Declares that the calling witness test is about to run `plan` on
/// `scenario` as `harness`'s history `case`, when [`HISTORIES_VAR`] names a
/// directory. A Plan that's neither bytes nor a script names no committed
/// input, so it can't be declared, and neither can a write that fails:
/// either panics, which fails the test, so a run never claims a history it
/// didn't declare.
pub fn declare(harness: &str, case: &str, scenario: &impl Serialize, plan: &PlanInput) {
    let Some(root) = std::env::var_os(HISTORIES_VAR).map(PathBuf::from) else {
        return;
    };
    declare_into(&root, harness, case, scenario, plan).unwrap_or_else(|error| {
        panic!("declaring history {case} under {}: {error}", root.display())
    });
}

/// Writes one declaration, as [`declare`] does, into the next free numbered
/// directory under `root`, creating `root`. A directory is taken with
/// `create_dir`, so two tests declaring at once never share one.
pub fn declare_into(
    root: &Path,
    harness: &str,
    case: &str,
    scenario: &impl Serialize,
    plan: &PlanInput,
) -> Result<(), String> {
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
    )
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
}
