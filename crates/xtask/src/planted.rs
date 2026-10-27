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
//!    violation of any property;
//! 3. applies the patch and records the input again, which must violate
//!    exactly the case's property;
//! 4. runs a short campaign on the patched build: [`CAMPAIGN_INPUTS`] fuzz
//!    inputs, drawn from a seed that the property's ID fixes, each of which
//!    must hold or violate only the case's property;
//! 5. removes the worktree, keeping every trace as evidence.
//!
//! A patch that doesn't apply, a build that fails, a harness failure, a
//! violation of another property, and a patch the input doesn't catch are
//! each reported as their own failure, never as a catch. The campaign
//! reports how many of its inputs caught the patch. It fails the case only
//! on a run that fails, ends at a harness failure, or violates another
//! property, so the gate stays the committed input.
//!
//! `--changed-since <rev>` runs only the cases a change since `rev` can
//! affect: those whose patch, case file, input, patched files or listed
//! checks changed, or every case if this runner changed.

use std::{
    env,
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;
use serde_json::Value;

use crate::{run_command, sim::SIM_RUSTFLAGS, BoxError};

/// Where the planted cases live, relative to the repository root.
const PLANTED_DIR: &str = "qa/planted";

/// This runner, relative to the repository root. A change to it affects
/// every case.
const RUNNER: &str = "crates/xtask/src/planted.rs";

/// Fuzz inputs the short campaign records after the committed input.
const CAMPAIGN_INPUTS: usize = 32;

/// The campaign's longest input, in bytes.
const CAMPAIGN_INPUT_BYTES: usize = 64;

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
    /// Other paths whose change can break the catch, relative to the
    /// repository root.
    checks: Vec<PathBuf>,
}

/// A case's `<name>.toml`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseFile {
    /// The committed fuzz input, relative to the repository root.
    input: PathBuf,
    /// Files or directories, relative to the repository root, whose change
    /// can break the catch besides the files the patch changes: the
    /// property's check, its harness, and its trigger history.
    #[serde(default)]
    checks: Vec<PathBuf>,
}

/// A committed fuzz input's sidecar, as `zakura-sim`'s tests check it.
#[derive(Deserialize)]
struct Sidecar {
    harness: String,
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
    },
    /// The build or the `zakura-sim` command failed before writing a trace.
    Failed(String),
}

/// What a case's short campaign found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Campaign {
    /// Inputs recorded.
    inputs: usize,
    /// Inputs whose run violated the case's property.
    caught: usize,
}

/// How a case ended.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Verdict {
    /// The unpatched run holds the property, and the patched run violates
    /// exactly it. `None` before the campaign runs.
    Caught(Option<Campaign>),
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
    /// A campaign input's patched run failed, ended at a harness failure,
    /// or violated another property.
    CampaignFails {
        /// The input.
        input: PathBuf,
        /// Its run's verdict.
        verdict: Box<Verdict>,
    },
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Caught(None) => f.write_str("caught"),
            Verdict::Caught(Some(Campaign { inputs, caught })) => write!(
                f,
                "caught; the campaign caught it on {caught} of {inputs} inputs"
            ),
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
            Verdict::CampaignFails { input, verdict } => {
                write!(f, "campaign input {}: {verdict}", input.display())
            }
        }
    }
}

/// Runs every planted case, or only `property`'s, and fails unless each is
/// caught. With `changed_since`, it runs only the cases a change since that
/// revision affects, and passes if none is.
pub(super) fn run(
    repo_root: &Path,
    property: Option<&str>,
    changed_since: Option<&str>,
) -> Result<(), BoxError> {
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
    let cases = match changed_since {
        Some(rev) => {
            let changed = changed_since_rev(repo_root, rev)?;
            let mut affected = Vec::new();
            for case in cases {
                if is_affected(&watched(repo_root, &case)?, &changed) {
                    affected.push(case);
                }
            }
            if affected.is_empty() {
                println!("no planted case is affected by the changes since {rev}");
                return Ok(());
            }
            affected
        }
        None => cases,
    };
    let work = repo_root.join("target").join("planted");
    let mut failed = 0;
    for (index, case) in cases.iter().enumerate() {
        let scratch = work.join(format!("case-{index}"));
        let verdict = run_case(repo_root, &scratch, &work.join("target"), case)?;
        println!("{} {}: {verdict}", case.property, case.patch.display());
        if !matches!(verdict, Verdict::Caught(_)) {
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
            let CaseFile { input, checks } = toml::from_str(
                &fs::read_to_string(&case_file)
                    .map_err(|error| format!("{}: {error}", case_file.display()))?,
            )
            .map_err(|error| format!("{}: {error}", case_file.display()))?;
            Ok(Case {
                property,
                patch,
                input,
                checks,
            })
        })
        .collect()
}

/// The paths `git diff` reports changed between `rev`'s merge base with
/// `HEAD` and `HEAD`, relative to the repository root.
fn changed_since_rev(repo_root: &Path, rev: &str) -> Result<Vec<PathBuf>, BoxError> {
    let output = git(repo_root)
        .args(["diff", "--name-only", &format!("{rev}...HEAD")])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "`git diff {rev}...HEAD` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .map(PathBuf::from)
        .collect())
}

/// Every path whose change can break `case`'s catch, relative to
/// `repo_root`: the patch and its case file, the input and its sidecar, the
/// files the patch changes, the case's checks, and this runner.
fn watched(repo_root: &Path, case: &Case) -> Result<Vec<PathBuf>, BoxError> {
    let relative = |path: &Path| {
        path.strip_prefix(repo_root)
            .map(Path::to_path_buf)
            .map_err(|_| format!("{} isn't under the repository", path.display()))
    };
    let mut watched = vec![
        relative(&case.patch)?,
        relative(&case.patch.with_extension("toml"))?,
        case.input.clone(),
        case.input.with_extension("toml"),
        PathBuf::from(RUNNER),
    ];
    watched.extend(patched_files(repo_root, &case.patch)?);
    watched.extend(case.checks.iter().cloned());
    Ok(watched)
}

/// The files `patch` changes, as `git apply --numstat` lists them without
/// applying it.
fn patched_files(repo_root: &Path, patch: &Path) -> Result<Vec<PathBuf>, BoxError> {
    let output = git(repo_root)
        .args(["apply", "--numstat"])
        .arg(patch)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "{}: {}",
            patch.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    // Each line is `<added>\t<deleted>\t<path>`.
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .filter_map(|line| line.splitn(3, '\t').nth(2))
        .map(PathBuf::from)
        .collect())
}

/// True if a path in `changed` is a watched path, or lies in a watched
/// directory.
fn is_affected(watched: &[PathBuf], changed: &[PathBuf]) -> bool {
    changed
        .iter()
        .any(|path| watched.iter().any(|w| path.starts_with(w)))
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
    let input = worktree.join(&case.input);
    let baseline = record(
        cargo_run(worktree, target),
        &harness,
        &input,
        &case.property,
        &scratch.join("baseline"),
    )?;
    if let Some(why) = baseline_failure(&case.property, &baseline) {
        return Ok(Verdict::BaselineFails(why));
    }
    if let Err(why) = apply(worktree, &case.patch) {
        return Ok(Verdict::PatchDoesntApply(why));
    }
    let patched = record(
        cargo_run(worktree, target),
        &harness,
        &input,
        &case.property,
        &scratch.join("patched"),
    )?;
    match classify(&case.property, &patched) {
        Verdict::Caught(None) => campaign(target, &harness, case, &scratch.join("campaign")),
        verdict => Ok(verdict),
    }
}

/// Records [`CAMPAIGN_INPUTS`] fuzz inputs on `harness` with the patched
/// build in `target`, which recording the committed input built. Each input
/// and its trace go under `dir`.
fn campaign(target: &Path, harness: &str, case: &Case, dir: &Path) -> Result<Verdict, BoxError> {
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;
    let binary = target.join("debug").join("zakura-sim");
    let mut caught = 0;
    for (index, bytes) in campaign_inputs(&case.property).into_iter().enumerate() {
        let input = dir.join(format!("input-{index}.bin"));
        fs::write(&input, bytes)?;
        let run = record(
            Command::new(&binary),
            harness,
            &input,
            &case.property,
            &dir.join(format!("run-{index}")),
        )?;
        match classify(&case.property, &run) {
            Verdict::Caught(_) => caught += 1,
            Verdict::NotCaught(_) => {}
            verdict => {
                return Ok(Verdict::CampaignFails {
                    input,
                    verdict: Box::new(verdict),
                })
            }
        }
    }
    Ok(Verdict::Caught(Some(Campaign {
        inputs: CAMPAIGN_INPUTS,
        caught,
    })))
}

/// The campaign's inputs for `property`: [`CAMPAIGN_INPUTS`] byte strings of
/// up to [`CAMPAIGN_INPUT_BYTES`], from a SplitMix64 stream that the
/// property's ID seeds, so a case's campaign is the same on every run and
/// doesn't move when other cases are added.
fn campaign_inputs(property: &str) -> Vec<Vec<u8>> {
    // FNV-1a.
    let mut state = property
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let mut next = move || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    (0..CAMPAIGN_INPUTS)
        .map(|_| {
            // The remainder is below CAMPAIGN_INPUT_BYTES + 1, so it fits.
            let len = (next() % (CAMPAIGN_INPUT_BYTES as u64 + 1)) as usize;
            // Each byte is the low 8 bits of a draw.
            (0..len).map(|_| next() as u8).collect()
        })
        .collect()
}

/// The harness `input`'s sidecar names.
fn harness_of(worktree: &Path, input: &Path) -> Result<String, BoxError> {
    let sidecar = worktree.join(input).with_extension("toml");
    let text =
        fs::read_to_string(&sidecar).map_err(|error| format!("{}: {error}", sidecar.display()))?;
    let Sidecar { harness } =
        toml::from_str(&text).map_err(|error| format!("{}: {error}", sidecar.display()))?;
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

/// `zakura-sim`, built and run from `worktree`'s simulator build in
/// `target`.
fn cargo_run(worktree: &Path, target: &Path) -> Command {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(cargo);
    command
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
        ]);
    command
}

/// Records `input` on `harness` with `zakura_sim`, a `zakura-sim` command,
/// writing the trace to `out`, and reads its `end` record for `property`.
fn record(
    mut zakura_sim: Command,
    harness: &str,
    input: &Path,
    property: &str,
    out: &Path,
) -> Result<Run, BoxError> {
    if out.exists() {
        fs::remove_dir_all(out)?;
    }
    let status = zakura_sim
        .args(["record", "--sim", harness, "--input"])
        .arg(input)
        .arg("--out")
        .arg(out)
        .status()?;
    if !status.success() {
        return Ok(Run::Failed(format!(
            "`zakura-sim record` exited with {status}"
        )));
    }
    let trace = fs::read_to_string(out.join("trace.jsonl"))?;
    let end = trace.lines().next_back().ok_or("the trace is empty")?;
    end_of(property, end)
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
        Run::Ended { outcome, .. } if outcome.as_deref() == Some("held") => None,
        Run::Ended { outcome, .. } => Some(format!(
            "{property} is {}",
            outcome.as_deref().unwrap_or("not reported")
        )),
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
            violation: Some(_), ..
        } => Verdict::Caught(None),
        Run::Ended { outcome, .. } if outcome.as_deref() == Some("violated") => {
            Verdict::Caught(None)
        }
        Run::Ended { outcome, .. } => Verdict::NotCaught(
            outcome
                .clone()
                .unwrap_or_else(|| format!("{property} is not reported")),
        ),
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
            Verdict::Caught(None)
        );
        // A violation that didn't stop the run, as for a Known property.
        assert_eq!(
            classify(ID, &run(None, &[(ID, VIOLATED)])),
            Verdict::Caught(None)
        );
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

    /// A case's campaign inputs are fixed by its property's ID alone, stay
    /// within the length bound, and differ between properties.
    #[test]
    fn campaign_inputs_follow_the_property() {
        let inputs = campaign_inputs(ID);
        assert_eq!(inputs.len(), CAMPAIGN_INPUTS);
        assert!(inputs.iter().all(|i| i.len() <= CAMPAIGN_INPUT_BYTES));
        assert!(inputs.iter().any(|i| !i.is_empty()));
        assert_eq!(inputs, campaign_inputs(ID));
        assert_ne!(inputs, campaign_inputs("race/lost-update"));
    }

    /// A campaign run that violates another property fails the case and
    /// names the input; one that holds or catches the patch doesn't.
    #[test]
    fn campaign_failures_name_their_input() {
        let verdict = Verdict::CampaignFails {
            input: PathBuf::from("campaign/input-3.bin"),
            verdict: Box::new(classify(ID, &run(Some("sim/no-task-panic"), &[(ID, HELD)]))),
        };
        assert_eq!(
            verdict.to_string(),
            "campaign input campaign/input-3.bin: the patched run violated sim/no-task-panic instead"
        );
        assert_eq!(
            Verdict::Caught(Some(Campaign {
                inputs: 32,
                caught: 5
            }))
            .to_string(),
            "caught; the campaign caught it on 5 of 32 inputs"
        );
    }

    /// A case is affected by a change to its patch's files, its checks, its
    /// own files, or the runner, and not by an unrelated change.
    #[test]
    fn planted_runs_only_affected_cases() {
        let watched = [
            PathBuf::from("crates/x/src/lib.rs"),
            PathBuf::from("crates/zakura-sim/src/domain"),
            PathBuf::from(RUNNER),
        ];
        let affected = |path: &str| is_affected(&watched, &[PathBuf::from(path)]);
        assert!(affected("crates/x/src/lib.rs"));
        assert!(affected("crates/zakura-sim/src/domain/discovery.rs"));
        assert!(affected(RUNNER));
        assert!(!affected("crates/x/src/other.rs"));
        assert!(!affected("crates/zakura-sim/src/domainx.rs"));
        assert!(!is_affected(&watched, &[]));
    }

    /// Each committed case watches its patch's production files and its
    /// listed checks, which all exist.
    #[test]
    fn committed_cases_watch_existing_files() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        for case in cases(&repo_root).expect("the planted cases parse") {
            let watched = watched(&repo_root, &case).expect("the case's files exist");
            for path in &watched {
                assert!(
                    repo_root.join(path).exists(),
                    "{}: {} doesn't exist",
                    case.patch.display(),
                    path.display()
                );
            }
            assert!(!case.checks.is_empty(), "{}", case.patch.display());
            assert!(
                watched
                    .iter()
                    .any(|path| path.starts_with("crates/zakura-network")),
                "{} watches the production file it patches",
                case.patch.display()
            );
        }
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
