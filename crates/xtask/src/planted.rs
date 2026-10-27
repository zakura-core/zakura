//! `cargo xtask planted [<property-id>]`: checks that each retained bypass
//! patch still makes its property fail (properties.md, Planted bugs and
//! evidence).
//!
//! A planted case is `qa/planted/<property-id>/<name>.patch`, a production
//! change that bypasses the property's guard, with `<name>.toml` beside it
//! naming the committed fuzz input that must find the bug. The input's own
//! sidecar names its harness. For each case the runner:
//!
//! 1. checks out `HEAD` in a scratch worktree under `target/planted/`; every
//!    case builds in `target/planted/target`, apart from the checkout's own
//!    target directory;
//! 2. records the input unpatched, which must hold the property with no
//!    violation of any property and pass its witness test expecting
//!    `holds`;
//! 3. applies the patch and records the input again, which must violate
//!    exactly the case's property and pass its witness test expecting
//!    `violated`;
//! 4. removes the worktree, keeping both traces as evidence.
//!
//! A witness test passes only if the run reached the property's trigger
//! history. `zakura-sim record --witness` judges it with the rule the
//! simulator's own witness tests use, and writes the result beside the
//! trace.
//!
//! A patch that doesn't apply, a build that fails, a harness failure, a
//! violation of another property, a patch the input doesn't catch, and a
//! catch that misses the trigger history are each reported as their own
//! failure, never as a catch.

use std::{
    env,
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{run_command, sim::SIM_RUSTFLAGS, BoxError};

/// Where the planted cases live, relative to the repository root.
const PLANTED_DIR: &str = "qa/planted";

/// One retained bypass patch.
#[derive(Clone, Debug)]
struct Case {
    /// The property the patch must violate.
    property: String,
    /// The patch.
    patch: PathBuf,
    /// The committed fuzz input that must find it, relative to the
    /// repository root.
    input: PathBuf,
}

/// A case's `<name>.toml`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseFile {
    /// The committed fuzz input, relative to the repository root.
    input: PathBuf,
}

/// A committed fuzz input's sidecar, as `zakura-sim`'s tests check it.
#[derive(Deserialize)]
struct Sidecar {
    harness: String,
    sha256: String,
}

/// The `witness.json` that `zakura-sim record --witness` writes beside the
/// trace.
#[derive(Deserialize)]
struct WitnessFile {
    passed: bool,
    error: Option<String>,
}

/// What one run of a case's input showed.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Run {
    /// The trace's `end` record: how the run ended, the violated property if
    /// any, and the case's property's outcome.
    Ended {
        /// The violation that stopped the run, if one did.
        violation: Option<String>,
        /// The first property other than the case's whose report is
        /// `violated`, for a run no violation stopped.
        other_violated: Option<String>,
        /// The case's property's outcome, such as `held`, or `None` if the
        /// run doesn't report it.
        outcome: Option<String>,
        /// The harness failure, for a run that ended at one.
        harness: Option<String>,
        /// Why the witness test failed, or `None` if it passed.
        witness: Option<String>,
    },
    /// The build or the `zakura-sim` command failed before writing a trace.
    Failed(String),
}

/// How a case ended.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Verdict {
    /// The unpatched run holds the property, and the patched run violates
    /// exactly it.
    Caught,
    /// The unpatched run doesn't hold the property cleanly.
    BaselineFails(String),
    /// The patch doesn't apply to `HEAD`.
    PatchDoesntApply(String),
    /// The patched build or run failed before it wrote a trace.
    Failed(String),
    /// The patched run ended at a harness failure.
    Harness(String),
    /// The patched run violated another property.
    Unrelated(String),
    /// The patched run didn't violate the property.
    NotCaught(String),
    /// The patched run violated the property, but its witness test failed,
    /// as when the run missed the trigger history.
    WitnessFails(String),
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Caught => f.write_str("caught"),
            Verdict::BaselineFails(why) => write!(f, "the unpatched baseline fails: {why}"),
            Verdict::PatchDoesntApply(why) => write!(f, "the patch doesn't apply: {why}"),
            Verdict::Failed(why) => write!(f, "the patched run failed: {why}"),
            Verdict::Harness(why) => write!(f, "the patched run hit a harness failure: {why}"),
            Verdict::Unrelated(property) => {
                write!(f, "the patched run violated {property} instead")
            }
            Verdict::NotCaught(outcome) => {
                write!(f, "the patched run didn't violate it: {outcome}")
            }
            Verdict::WitnessFails(why) => {
                write!(f, "the patched run's witness test fails: {why}")
            }
        }
    }
}

/// Runs every planted case, or only `property`'s, and fails unless each is
/// caught.
pub(super) fn run(repo_root: &Path, property: Option<&str>) -> Result<(), BoxError> {
    let cases: Vec<Case> = cases(repo_root)?
        .into_iter()
        .filter(|case| property.is_none_or(|p| case.property == p))
        .collect();
    if cases.is_empty() {
        return Err(match property {
            Some(property) => format!("no planted case for {property}").into(),
            None => format!("no planted cases under {PLANTED_DIR}").into(),
        });
    }
    let work = repo_root.join("target").join("planted");
    let mut failed = 0;
    for (index, case) in cases.iter().enumerate() {
        let scratch = work.join(format!("case-{index}"));
        let verdict = run_case(repo_root, &scratch, &work.join("target"), case)?;
        println!("{} {}: {verdict}", case.property, case.patch.display());
        if verdict != Verdict::Caught {
            failed += 1;
        }
    }
    if failed > 0 {
        return Err(format!("{failed} of {} planted cases failed", cases.len()).into());
    }
    Ok(())
}

/// Every case under `repo_root`'s [`PLANTED_DIR`], in path order.
fn cases(repo_root: &Path) -> Result<Vec<Case>, BoxError> {
    let root = repo_root.join(PLANTED_DIR);
    let mut patches = Vec::new();
    find_patches(&root, &mut patches)?;
    patches.sort();
    patches
        .into_iter()
        .map(|patch| {
            let property = patch
                .parent()
                .and_then(|dir| dir.strip_prefix(&root).ok())
                .and_then(Path::to_str)
                .ok_or_else(|| format!("{} isn't under a property", patch.display()))?
                .to_owned();
            let case_file = patch.with_extension("toml");
            let CaseFile { input } = toml::from_str(
                &fs::read_to_string(&case_file)
                    .map_err(|error| format!("{}: {error}", case_file.display()))?,
            )
            .map_err(|error| format!("{}: {error}", case_file.display()))?;
            Ok(Case {
                property,
                patch,
                input,
            })
        })
        .collect()
}

/// Appends every `.patch` file under `dir` to `patches`.
fn find_patches(dir: &Path, patches: &mut Vec<PathBuf>) -> Result<(), BoxError> {
    for entry in fs::read_dir(dir).map_err(|error| format!("{}: {error}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            find_patches(&path, patches)?;
        } else if path.extension().is_some_and(|e| e == "patch") {
            patches.push(path);
        }
    }
    Ok(())
}

/// Runs `case` in a scratch worktree under `scratch`, building in `target`,
/// and removes the worktree after. The traces stay under `scratch`.
fn run_case(
    repo_root: &Path,
    scratch: &Path,
    target: &Path,
    case: &Case,
) -> Result<Verdict, BoxError> {
    let worktree = scratch.join("worktree");
    if worktree.exists() {
        run_command(
            git(repo_root)
                .args(["worktree", "remove", "--force"])
                .arg(&worktree),
        )?;
    }
    fs::create_dir_all(scratch)?;
    run_command(
        git(repo_root)
            .args(["worktree", "add", "--detach"])
            .arg(&worktree)
            .arg("HEAD"),
    )?;
    let verdict = check_case(&worktree, scratch, target, case);
    run_command(
        git(repo_root)
            .args(["worktree", "remove", "--force"])
            .arg(&worktree),
    )?;
    verdict
}

/// Runs the baseline and the patched input in `worktree`, building in
/// `target` and writing their traces under `scratch`.
fn check_case(
    worktree: &Path,
    scratch: &Path,
    target: &Path,
    case: &Case,
) -> Result<Verdict, BoxError> {
    let harness = harness_of(worktree, &case.input)?;
    let baseline = record(
        worktree,
        target,
        &harness,
        case,
        "holds",
        &scratch.join("baseline"),
    )?;
    if let Some(why) = baseline_failure(&case.property, &baseline) {
        return Ok(Verdict::BaselineFails(why));
    }
    if let Err(why) = apply(worktree, &case.patch) {
        return Ok(Verdict::PatchDoesntApply(why));
    }
    let patched = record(
        worktree,
        target,
        &harness,
        case,
        "violated",
        &scratch.join("patched"),
    )?;
    Ok(classify(&case.property, &patched))
}

/// The harness `input`'s sidecar names, once the sidecar's digest matches
/// the input.
fn harness_of(worktree: &Path, input: &Path) -> Result<String, BoxError> {
    let input = worktree.join(input);
    let sidecar = input.with_extension("toml");
    let text =
        fs::read_to_string(&sidecar).map_err(|error| format!("{}: {error}", sidecar.display()))?;
    let Sidecar { harness, sha256 } =
        toml::from_str(&text).map_err(|error| format!("{}: {error}", sidecar.display()))?;
    let bytes = fs::read(&input).map_err(|error| format!("{}: {error}", input.display()))?;
    let digest: String = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if digest != sha256 {
        return Err(format!(
            "{}: the sidecar's sha256 isn't the input's",
            sidecar.display()
        )
        .into());
    }
    Ok(harness)
}

/// Applies `patch` to `worktree`, or says why it doesn't apply.
fn apply(worktree: &Path, patch: &Path) -> Result<(), String> {
    let output = git(worktree)
        .arg("apply")
        .arg(patch)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

/// Records `case`'s input on `harness` in `worktree`'s simulator build as a
/// witness test of the case's property that expects `expect`, writing the
/// trace to `out`, and reads its `end` record and the test's result.
fn record(
    worktree: &Path,
    target: &Path,
    harness: &str,
    case: &Case,
    expect: &str,
    out: &Path,
) -> Result<Run, BoxError> {
    if out.exists() {
        fs::remove_dir_all(out)?;
    }
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let status = Command::new(cargo)
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", target)
        .current_dir(worktree)
        .args([
            "run",
            "--locked",
            "-p",
            "zakura-sim",
            "--bin",
            "zakura-sim",
            "--",
        ])
        .args(["record", "--sim", harness, "--input"])
        .arg(worktree.join(&case.input))
        .arg("--out")
        .arg(out)
        .args(["--witness", &case.property, "--expect", expect])
        .status()?;
    // A failed witness test exits 1 too, but only after writing its result.
    let Ok(witness) = fs::read_to_string(out.join("witness.json")) else {
        return Ok(Run::Failed(format!(
            "`zakura-sim record` exited with {status}"
        )));
    };
    let witness: WitnessFile = serde_json::from_str(&witness)?;
    let trace = fs::read_to_string(out.join("trace.jsonl"))?;
    let end = trace.lines().next_back().ok_or("the trace is empty")?;
    let mut run = end_of(&case.property, end)?;
    if let Run::Ended {
        witness: failed, ..
    } = &mut run
    {
        *failed = (!witness.passed).then(|| {
            witness
                .error
                .unwrap_or_else(|| "no reason given".to_owned())
        });
    }
    Ok(run)
}

/// Reads `line`, a trace's `end` record, for `property`.
fn end_of(property: &str, line: &str) -> Result<Run, BoxError> {
    let line: Value = serde_json::from_str(line)?;
    let end = line
        .get("end")
        .ok_or("the trace's last line isn't its end")?;
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    let properties = end
        .get("properties")
        .and_then(Value::as_array)
        .ok_or("the end record has no properties")?;
    // An outcome is a string such as `"held"`, or an object keyed by its
    // name, such as `{"violated":{...}}`.
    let outcome_name = |report: &Value| {
        let outcome = report.get("outcome")?;
        outcome
            .as_str()
            .or_else(|| outcome.as_object()?.keys().next().map(String::as_str))
            .map(str::to_owned)
    };
    let id = |report: &Value| text(report.get("property")).unwrap_or_default();
    Ok(Run::Ended {
        violation: text(end.get("violation").and_then(|v| v.get("property"))),
        other_violated: properties
            .iter()
            .filter(|report| id(report) != property)
            .find(|report| outcome_name(report).as_deref() == Some("violated"))
            .map(id),
        outcome: properties
            .iter()
            .find(|report| id(report) == property)
            .and_then(outcome_name),
        harness: text(end.get("harness")),
        witness: None,
    })
}

/// Why `baseline` doesn't hold `property` cleanly, or `None` if it does.
fn baseline_failure(property: &str, baseline: &Run) -> Option<String> {
    match baseline {
        Run::Failed(why) => Some(why.clone()),
        Run::Ended {
            harness: Some(failure),
            ..
        } => Some(format!("harness failure: {failure}")),
        Run::Ended {
            violation: Some(violated),
            ..
        }
        | Run::Ended {
            other_violated: Some(violated),
            ..
        } => Some(format!("{violated} is violated")),
        Run::Ended { outcome, .. } if outcome.as_deref() != Some("held") => Some(format!(
            "{property} is {}",
            outcome.as_deref().unwrap_or("not reported")
        )),
        Run::Ended {
            witness: Some(why), ..
        } => Some(format!("its witness test fails: {why}")),
        Run::Ended { .. } => None,
    }
}

/// What the patched run shows about `property`.
fn classify(property: &str, patched: &Run) -> Verdict {
    match patched {
        Run::Failed(why) => Verdict::Failed(why.clone()),
        Run::Ended {
            harness: Some(failure),
            ..
        } => Verdict::Harness(failure.clone()),
        Run::Ended {
            violation: Some(violated),
            ..
        } if violated != property => Verdict::Unrelated(violated.clone()),
        Run::Ended {
            other_violated: Some(violated),
            ..
        } => Verdict::Unrelated(violated.clone()),
        Run::Ended {
            violation: None,
            outcome,
            ..
        } if outcome.as_deref() != Some("violated") => Verdict::NotCaught(
            outcome
                .clone()
                .unwrap_or_else(|| format!("{property} is not reported")),
        ),
        Run::Ended {
            witness: Some(why), ..
        } => Verdict::WitnessFails(why.clone()),
        Run::Ended { .. } => Verdict::Caught,
    }
}

/// `git` in `dir`.
fn git(dir: &Path) -> Command {
    let mut command = Command::new("git");
    command.current_dir(dir);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "discovery/wait-complete-wakes";

    /// An `end` line with `violation`, and `reports` as (property, outcome
    /// JSON) pairs.
    fn end(violation: Option<&str>, reports: &[(&str, &str)]) -> String {
        let properties: Vec<String> = reports
            .iter()
            .map(|(property, outcome)| {
                format!(r#"{{"property":"{property}","outcome":{outcome}}}"#)
            })
            .collect();
        let violation = violation.map_or_else(String::new, |property| {
            format!(r#""violation":{{"property":"{property}","step":3,"witness":[3],"expected":"","actual":""}},"#)
        });
        format!(
            r#"{{"end":{{"outcome":"budget",{violation}"steps":9,"drain":null,"cleanup":{{}},"properties":[{}]}}}}"#,
            properties.join(",")
        )
    }

    const HELD: &str = r#"{"held":{"applicable":1}}"#;
    const VIOLATED: &str = r#"{"violated":{"witness":[3]}}"#;

    fn run(violation: Option<&str>, reports: &[(&str, &str)]) -> Run {
        end_of(ID, &end(violation, reports)).expect("the line is an end record")
    }

    #[test]
    fn planted_passes_only_on_its_own_id() {
        assert_eq!(
            classify(ID, &run(Some(ID), &[(ID, VIOLATED)])),
            Verdict::Caught
        );
        // A violation that didn't stop the run, as for a Known property.
        assert_eq!(classify(ID, &run(None, &[(ID, VIOLATED)])), Verdict::Caught);
        assert_eq!(
            classify(
                ID,
                &run(
                    Some("sim/no-task-panic"),
                    &[(ID, HELD), ("sim/no-task-panic", VIOLATED)]
                )
            ),
            Verdict::Unrelated("sim/no-task-panic".to_owned())
        );
        assert_eq!(
            classify(ID, &run(None, &[(ID, VIOLATED), ("other/draft", VIOLATED)])),
            Verdict::Unrelated("other/draft".to_owned())
        );
        assert_eq!(
            classify(ID, &run(None, &[(ID, HELD)])),
            Verdict::NotCaught("held".to_owned())
        );
        assert_eq!(
            classify(ID, &run(None, &[])),
            Verdict::NotCaught(format!("{ID} is not reported"))
        );
        assert_eq!(
            classify(ID, &Run::Failed("build".to_owned())),
            Verdict::Failed("build".to_owned())
        );
    }

    /// A run's witness test result, as `record` reads it from
    /// `witness.json`.
    fn judged(run: Run, why: &str) -> Run {
        match run {
            Run::Ended {
                violation,
                other_violated,
                outcome,
                harness,
                ..
            } => Run::Ended {
                violation,
                other_violated,
                outcome,
                harness,
                witness: Some(why.to_owned()),
            },
            failed => failed,
        }
    }

    /// A violation that misses the trigger history isn't a catch, and a
    /// baseline that misses it doesn't pass.
    #[test]
    fn planted_requires_the_trigger_history() {
        let lost = "witness test lost its trigger history";
        assert_eq!(
            classify(ID, &judged(run(Some(ID), &[(ID, VIOLATED)]), lost)),
            Verdict::WitnessFails(lost.to_owned())
        );
        assert_eq!(
            baseline_failure(ID, &judged(run(None, &[(ID, HELD)]), lost)),
            Some(format!("its witness test fails: {lost}"))
        );
        // The end record's own failures still name themselves first.
        assert_eq!(
            classify(ID, &judged(run(None, &[(ID, HELD)]), lost)),
            Verdict::NotCaught("held".to_owned())
        );
    }

    #[test]
    fn planted_requires_passing_baseline() {
        assert_eq!(baseline_failure(ID, &run(None, &[(ID, HELD)])), None);
        assert_eq!(
            baseline_failure(ID, &run(Some(ID), &[(ID, VIOLATED)])),
            Some(format!("{ID} is violated"))
        );
        assert_eq!(
            baseline_failure(ID, &run(None, &[(ID, HELD), ("other/known", VIOLATED)])),
            Some("other/known is violated".to_owned())
        );
        assert_eq!(
            baseline_failure(ID, &run(None, &[(ID, r#""not-applicable""#)])),
            Some(format!("{ID} is not-applicable"))
        );
        assert!(baseline_failure(ID, &Run::Failed("build".to_owned())).is_some());
    }

    /// A patch that doesn't apply says why, and leaves the checkout as it
    /// was.
    #[test]
    fn planted_patch_must_apply() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let repo = dir.path();
        let git_ok = |args: &[&str]| {
            assert!(git(repo).args(args).status().expect("git runs").success());
        };
        git_ok(&["init", "-q"]);
        fs::write(repo.join("a.rs"), "fn a() {}\n").unwrap();
        let patch = repo.join("p.patch");
        fs::write(
            &patch,
            "--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn b() {}\n+fn c() {}\n",
        )
        .unwrap();
        let why = apply(repo, &patch).expect_err("the patch's context isn't in a.rs");
        assert!(why.contains("a.rs"), "{why}");
        assert_eq!(
            fs::read_to_string(repo.join("a.rs")).unwrap(),
            "fn a() {}\n"
        );

        fs::write(
            &patch,
            "--- a/a.rs\n+++ b/a.rs\n@@ -1 +1 @@\n-fn a() {}\n+fn c() {}\n",
        )
        .unwrap();
        apply(repo, &patch).expect("the patch applies");
        assert_eq!(
            fs::read_to_string(repo.join("a.rs")).unwrap(),
            "fn c() {}\n"
        );
    }

    /// The committed cases parse, and each names an input with a sidecar.
    #[test]
    fn committed_cases_name_their_inputs() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let cases = cases(&repo_root).expect("the planted cases parse");
        assert!(cases.iter().any(|case| case.property == ID));
        for case in &cases {
            harness_of(&repo_root, &case.input)
                .unwrap_or_else(|error| panic!("{}: {error}", case.patch.display()));
        }
    }
}
