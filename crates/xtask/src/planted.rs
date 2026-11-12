//! `cargo xtask planted [<property-id>]`: checks that each retained bypass
//! patch still makes its property fail (properties.md, Planted bugs and
//! evidence).
//!
//! A planted case is `qa/planted/<property-id>/<name>.patch`, a production
//! change that bypasses the property's guard, with `<name>.toml` beside it
//! naming what must find the bug: either the committed fuzz input, whose
//! own sidecar names its harness, or a typed case, a `[typed]` table naming
//! the harness, a committed scenario as that harness's JSON and the Plan it
//! runs under: either Plan bytes (`plan`) or a scripted Plan as JSON
//! (`script`), never both. A typed case is for a scenario the harness's
//! grammar can't encode, such as one with a control step or more mirrors
//! than the grammar takes, and a script is for a history no Plan bytes
//! reach, such as one a test steers to.
//!
//! The runner checks out `HEAD` once, in a scratch worktree under
//! `target/planted/`, and builds the unpatched simulator there once. Every
//! recording build uses the workspace's `planted` profile in
//! `target/planted/target`, apart from the checkout's own target directory. Then for each case it:
//!
//! 1. records the input with the unpatched build, which must hold the
//!    property with no violation of any property and pass its witness test
//!    expecting `holds`, and replays that trace in a fresh `zakura-sim
//!    replay` of the same build, which must match Exact;
//! 2. applies the patch, which must not change, delete or rename the
//!    case's witness files (its input and sidecar, or its scenario and
//!    Plan file), so both runs read the same committed witness. It then
//!    rebuilds and records the input again, which must violate the case's
//!    property and no property outside the case's `also_violates`, and pass
//!    its witness test expecting `violated` beside those;
//! 3. runs a short campaign on the patched build: [`CAMPAIGN_INPUTS`] fuzz
//!    inputs, drawn from a seed that the property's ID fixes, each of which
//!    must hold or violate only the case's property, and a violation must
//!    pass the witness test. A case file's `also_violates` names properties
//!    the patch's bug breaks too. Any run may violate them beside the case's
//!    property, and a campaign run may violate one of them instead;
//! 4. replays the patched trace in a fresh `zakura-sim replay` of the
//!    patched build, which must match Exact. Each fresh replay writes its
//!    full report and how its child ended under the trace's `replay/`,
//!    which the acceptance tool reads (C4);
//! 5. resets the worktree to `HEAD`, so the next patched build recompiles
//!    only what the two patches touch.
//!
//! It then removes the worktree, keeping every trace as evidence.
//!
//! A witness test passes only if the run reached the property's trigger
//! history. `zakura-sim record --witness` judges it with the rule the
//! simulator's own witness tests use, and writes the result beside the
//! trace.
//!
//! A patch that doesn't apply or changes a witness file, a build that
//! fails, a harness failure, a violation of another property, a patch the
//! input doesn't catch, and a catch that misses the trigger history are
//! each reported as their own failure, never as a catch. The campaign reports how many of its inputs
//! caught the patch, and how the others' runs ended. It fails the case only
//! on a run that fails, ends at a harness failure, violates another
//! property not in `also_violates`, doesn't report the case's property, or
//! violates it without its trigger history, so the gate stays the committed
//! input, which must violate the case's property.
//!
//! A typed case may name `missing_evidence` instead, a harness-caught case
//! (Adam's ruling on C19): its patched runs count only when they end at a
//! missing-evidence harness failure of the case's property whose detail
//! says that text, violating nothing. Every other harness failure is still
//! a failure.
//!
//! `--targets` runs only the cases whose committed fuzz input runs on a harness
//! that a simulator fuzz target runs, as the fuzz package's
//! `[package.metadata.zakura-sim.targets]` table maps them, and prints
//! each target's cases, or that it has none. After step 3 it also confirms
//! each patched input through its target in fresh processes, as
//! `cargo xtask sim-minimize` confirms an original: the patched target must
//! crash with the case's property alone and name the harness,
//! `zakura-sim confirm` must find the property's trigger history, and a
//! fresh `zakura-sim replay` must match the trace Exact. It builds
//! `zakura-sim` and the target in `target/planted/confirm-builds/`, outside
//! the scratch worktree, so later cases and runs reuse the builds, and both
//! binaries must embed the patched checkout's source digest. A check that
//! fails is its own failure, never a catch.
//!
//! `--shard <index>/<count>` runs only every `count`th case in path order,
//! starting at position `index`. It picks from the cases that the property
//! and `--targets` keep, and `--changed-since` filters its pick. The
//! `count` shards of one count together run every case once. A shard that
//! holds no case fails the run, so a run that passes ran at least one case.
//! A case's shard follows from its position, so adding or removing a case
//! moves every later case to another shard: across that change, a case can
//! run in two consecutive shard runs, or wait up to `2 * count - 1` shard
//! runs. The nightly job runs two shards a night, and a remote host runs
//! the whole catalogue (qa/fuzz/sim's README, Long runs).
//!
//! `--changed-since <rev>` runs only the cases a change since `rev` can
//! affect: those whose patch, case file, input, patched files or listed
//! checks changed, or every case if the shared machinery changed: this
//! runner, the simulator that records and judges each run, its scheduler
//! and runtime, or the build configuration. A renamed file counts as a
//! change to its old and its new path.
//!
//! `cargo xtask planted pilot (sensitivity|discovery)` runs the mutation
//! pilot's paired experiments on the reviewed catalogue's cases; see
//! [`pilot`].

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fmt, fs,
    path::{self, Component, Path, PathBuf},
    process::Command,
    sync::atomic::AtomicBool,
    time::Duration,
};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    run_command,
    sim::SIM_RUSTFLAGS,
    sim_minimize::{self, Sanitizer, Status},
    BoxError,
};

mod mutation_loop;
mod outcome;
mod pilot;

#[allow(dead_code)]
#[path = "../../zakura-sim/build/provenance.rs"]
pub(super) mod provenance;

/// Where the cases live and the property each bypasses, shared with the
/// simulator's checks of the catalogue.
#[path = "../../zakura-sim/src/planted.rs"]
mod planted_layout;

pub(super) use mutation_loop::run as mutation_loop;
use outcome::EndOutcome;
pub(super) use pilot::run as pilot;
use planted_layout::PLANTED_DIR;

/// What every case runs on, relative to the repository root: xtask, which
/// holds this runner and the flags and arguments it runs with, the
/// simulator that records each run and judges it with the property engine,
/// the scheduler and runtime it runs on, and the build configuration. A
/// change to any of them affects every case.
const SHARED: &[&str] = &[
    "crates/xtask",
    "crates/zakura-sim",
    "crates/zakura-scheduler",
    "crates/zakura-rt",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".cargo",
];

/// The cargo profile every case builds with: optimized, with the dev
/// profile's debug assertions and overflow checks (the workspace's
/// `Cargo.toml`).
pub(super) const PROFILE: &str = "planted";

/// Fuzz inputs the short campaign records after the committed input.
const CAMPAIGN_INPUTS: usize = 32;

/// The campaign's longest input, in bytes.
const CAMPAIGN_INPUT_BYTES: usize = 64;

/// The longest one child of a confirmation through a fuzz target may run.
const TARGET_CHILD: Duration = Duration::from_secs(300);

/// The longest one `zakura-sim` run of a case may take: a baseline, patched
/// or campaign recording, or a fresh replay. A run past it is killed and
/// never counts as a catch (C4).
const RUN_DEADLINE: Duration = Duration::from_secs(900);

/// The interrupt flag of a case's runs, which nothing sets: only the pilot
/// interrupts its runs.
static NEVER_INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// The directory under the planted work directory where a confirmation
/// through a fuzz target builds `zakura-sim` and the target. It sits
/// outside the scratch worktree, which each run removes, so later cases,
/// later runs and CI's cache of `target/` reuse the builds.
const CONFIRM_BUILDS: &str = "confirm-builds";

/// One part of the catalogue: the cases at positions `index`,
/// `index + count`, `index + 2 * count` and so on, in path order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Shard {
    index: usize,
    count: usize,
}

impl Shard {
    /// Reads `<index>/<count>`, where `index` is below `count`.
    pub(super) fn parse(text: &str) -> Result<Shard, String> {
        let usage = || format!("expected `<index>/<count>` with index below count, not `{text}`");
        let (index, count) = text.split_once('/').ok_or_else(usage)?;
        let index: usize = index.parse().map_err(|_| usage())?;
        let count: usize = count.parse().map_err(|_| usage())?;
        if index >= count {
            return Err(usage());
        }
        Ok(Shard { index, count })
    }

    /// The cases of `cases` in this shard, in their order.
    fn select<T>(self, cases: Vec<T>) -> Vec<T> {
        cases
            .into_iter()
            .skip(self.index)
            .step_by(self.count)
            .collect()
    }
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.index, self.count)
    }
}

/// One retained bypass patch.
#[derive(Clone, Debug)]
struct Case {
    /// The property the patch must violate.
    property: String,
    /// The patch.
    patch: PathBuf,
    /// What must find it.
    witness: Witness,
    /// Other paths whose change can break the catch, relative to the
    /// repository root.
    checks: Vec<PathBuf>,
    /// Other properties the patch's bug breaks, which any run may violate
    /// beside the case's property, and a campaign run instead of it.
    also_violates: Vec<String>,
    /// A nested case's exact set: the broader properties its patch can't
    /// break the case's property without, each of which the committed
    /// input must violate too, and nothing else (C4). Empty for a case that
    /// breaks its property alone.
    set: Vec<String>,
    /// A harness-caught case's named diagnostic: what the missing evidence
    /// its patched runs must end at says ([`classify_missing_evidence`]).
    /// `None` for a case caught by a violation.
    missing_evidence: Option<String>,
    /// The simulator fuzz target that runs the input's harness, with
    /// `--targets`.
    target: Option<String>,
}

/// What a case runs, with paths relative to the repository root.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Witness {
    /// A committed fuzz input, whose sidecar names its harness.
    Input(PathBuf),
    /// A committed scenario, as `harness`'s JSON, run under the Plan in
    /// `plan`.
    Typed {
        /// The harness.
        harness: String,
        /// The scenario file.
        scenario: PathBuf,
        /// The Plan file.
        plan: PlanFile,
    },
}

/// A typed case's committed Plan, by its file's path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PlanFile {
    /// Plan bytes, which `zakura-sim record --plan` reads.
    Bytes(PathBuf),
    /// A scripted Plan as JSON, which `zakura-sim record --script` reads.
    Script(PathBuf),
}

impl PlanFile {
    /// The file's path.
    fn path(&self) -> &Path {
        match self {
            PlanFile::Bytes(path) | PlanFile::Script(path) => path,
        }
    }

    /// The same kind of Plan at `root` joined with its path.
    fn under(&self, root: &Path) -> PlanFile {
        match self {
            PlanFile::Bytes(path) => PlanFile::Bytes(root.join(path)),
            PlanFile::Script(path) => PlanFile::Script(root.join(path)),
        }
    }

    /// The `zakura-sim record` flag that reads it.
    fn flag(&self) -> &'static str {
        match self {
            PlanFile::Bytes(_) => "--plan",
            PlanFile::Script(_) => "--script",
        }
    }
}

impl Witness {
    /// The harness it runs on, read from the input's sidecar under
    /// `root` for a fuzz input.
    fn harness(&self, root: &Path) -> Result<String, BoxError> {
        match self {
            Witness::Input(input) => harness_of(root, input),
            Witness::Typed { harness, .. } => Ok(harness.clone()),
        }
    }

    /// Its committed files: the input and its sidecar, or the scenario and
    /// the Plan file.
    fn files(&self) -> Vec<PathBuf> {
        match self {
            Witness::Input(input) => vec![input.clone(), input.with_extension("toml")],
            Witness::Typed { scenario, plan, .. } => {
                vec![scenario.clone(), plan.path().to_path_buf()]
            }
        }
    }

    /// Its files under `root`, as [`record`] runs them.
    fn located(&self, root: &Path) -> Located {
        match self {
            Witness::Input(input) => Located::Input(root.join(input)),
            Witness::Typed { scenario, plan, .. } => Located::Typed {
                scenario: root.join(scenario),
                plan: plan.under(root),
            },
        }
    }
}

/// What [`record`] runs, by its files' paths.
#[derive(Clone, Debug)]
pub(super) enum Located {
    /// A full fuzz input.
    Input(PathBuf),
    /// A scenario file and its Plan file.
    Typed {
        /// The scenario file.
        scenario: PathBuf,
        /// The Plan file.
        plan: PlanFile,
    },
}

/// A typed case's `[typed]` table.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypedFile {
    /// The harness the scenario runs on.
    harness: String,
    /// The scenario, as the harness's JSON, relative to the repository
    /// root.
    scenario: PathBuf,
    /// The Plan bytes, relative to the repository root. A typed case names
    /// this or `script`, never both.
    plan: Option<PathBuf>,
    /// A scripted Plan as JSON, relative to the repository root, for a
    /// history no Plan bytes reach.
    script: Option<PathBuf>,
}

impl TypedFile {
    /// The table's one Plan file, or why it doesn't name exactly one.
    fn plan_file(&self) -> Result<PlanFile, String> {
        match (&self.plan, &self.script) {
            (Some(plan), None) => Ok(PlanFile::Bytes(plan.clone())),
            (None, Some(script)) => Ok(PlanFile::Script(script.clone())),
            (plan, _) => Err(format!(
                "a typed case names either `plan` or `script`, and this names {}",
                if plan.is_some() { "both" } else { "neither" }
            )),
        }
    }
}

/// A case's `<name>.toml`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseFile {
    /// The committed fuzz input, relative to the repository root. A case
    /// names this or `typed`, never both.
    input: Option<PathBuf>,
    /// The typed case, for a scenario the harness's grammar can't encode.
    typed: Option<TypedFile>,
    /// Files or directories, relative to the repository root, whose change
    /// can break the catch besides the files the patch changes and
    /// [`SHARED`], such as the rest of the patched module.
    #[serde(default)]
    checks: Vec<PathBuf>,
    /// Other properties the patched bug breaks, by ID, as when a leaked
    /// charge also takes the byte budget past its bound. Any run may violate
    /// them beside the case's property, and a campaign input's run instead
    /// of it. The committed input must still violate the case's property.
    #[serde(default)]
    also_violates: Vec<String>,
    /// A nested case's exact set, by ID: the broader properties the patched
    /// bug can't break the case's property without, such as
    /// `conformant-never-closes` beside a `cadence-never-closes` catch. Every
    /// run of the committed input and the campaign must violate the case's
    /// property and each of them, and nothing else, to count as a catch. A
    /// case names `set` or `also_violates`, not both.
    #[serde(default)]
    set: Vec<String>,
    /// A harness-caught case's named diagnostic (Adam's ruling on C19,
    /// 2026-11-12): its patched run must end at a missing-evidence harness
    /// failure of the case's property whose detail says this, such as
    /// `with a boundary unread`, violating nothing. Only a typed case that
    /// names neither `set` nor `also_violates` may name it.
    missing_evidence: Option<String>,
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
        /// Every property other than the case's whose report is
        /// `violated`, in report order.
        others_violated: Vec<String>,
        /// The case's property's outcome, or `None` if the run doesn't
        /// report it.
        outcome: Option<EndOutcome>,
        /// The harness failure, for a run that ended at one.
        harness: Option<String>,
        /// Why the witness test failed, or `None` if it passed.
        witness: Option<String>,
    },
    /// The build or the `zakura-sim` command failed before writing a trace.
    Failed(String),
    /// The pilot's run passed its deadline or died from a signal, whatever
    /// files it wrote. The pilot counts it as a harness failure, never as a
    /// result of the code under test.
    Killed(String),
}

/// What a case's short campaign found.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Campaign {
    /// Inputs recorded.
    inputs: usize,
    /// Inputs whose run violated the case's property.
    caught: usize,
    /// How many of the other inputs' runs ended with each outcome of the
    /// case's property, such as `held` or `not-applicable/never-applied`.
    missed: BTreeMap<String, usize>,
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
    /// The patch changes, deletes or renames these witness files of the
    /// case.
    PatchesWitness(Vec<PathBuf>),
    /// The patched build or run failed before it wrote a trace.
    Failed(String),
    /// The patched binary doesn't embed the patched checkout's source
    /// digest, so it may be another case's or an older build.
    StaleBinary(String),
    /// The patched run ended at a harness failure.
    Harness(String),
    /// The patched run violated another property.
    Unrelated(String),
    /// The patched run reported the property, with an outcome other than
    /// violated.
    NotCaught(String),
    /// The patched run didn't report the property.
    Unreported,
    /// The patched run violated the property, but its witness test failed,
    /// as when the run missed the trigger history.
    WitnessFails(String),
    /// A typed case's patched trace didn't replay Exact in a fresh process.
    ReplayMisses(String),
    /// The patched input didn't confirm through the fuzz target that runs
    /// its harness.
    TargetMisses {
        /// The target.
        target: String,
        /// The confirmation's status and reason.
        why: String,
    },
    /// A campaign input's patched run failed, ended at a harness failure,
    /// violated another property, didn't report the case's property, or
    /// violated it without its trigger history.
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
            Verdict::Caught(Some(Campaign {
                inputs,
                caught,
                missed,
            })) => {
                write!(
                    f,
                    "caught; the campaign caught it on {caught} of {inputs} inputs"
                )?;
                for (index, (outcome, count)) in missed.iter().enumerate() {
                    let lead = if index == 0 { "; the others: " } else { ", " };
                    write!(f, "{lead}{count} {outcome}")?;
                }
                Ok(())
            }
            Verdict::BaselineFails(why) => write!(f, "the unpatched baseline fails: {why}"),
            Verdict::PatchDoesntApply(why) => write!(f, "the patch doesn't apply: {why}"),
            Verdict::PatchesWitness(files) => {
                f.write_str("the patch changes the case's witness files")?;
                for (index, file) in files.iter().enumerate() {
                    let lead = if index == 0 { ": " } else { ", " };
                    write!(f, "{lead}{}", file.display())?;
                }
                Ok(())
            }
            Verdict::Failed(why) => write!(f, "the patched run failed: {why}"),
            Verdict::StaleBinary(why) => {
                write!(f, "the patched binary isn't the patched checkout's: {why}")
            }
            Verdict::Harness(why) => write!(f, "the patched run hit a harness failure: {why}"),
            Verdict::Unrelated(property) => {
                write!(f, "the patched run violated {property} instead")
            }
            Verdict::NotCaught(outcome) => {
                write!(f, "the patched run didn't violate it: {outcome}")
            }
            Verdict::Unreported => f.write_str("the patched run didn't report the property"),
            Verdict::WitnessFails(why) => {
                write!(f, "the patched run's witness test fails: {why}")
            }
            Verdict::ReplayMisses(why) => {
                write!(f, "the patched trace doesn't replay Exact: {why}")
            }
            Verdict::TargetMisses { target, why } => {
                write!(
                    f,
                    "the patched input doesn't confirm through {target}: {why}"
                )
            }
            Verdict::CampaignFails { input, verdict } => {
                write!(f, "campaign input {}: {verdict}", input.display())
            }
        }
    }
}

/// Runs every planted case, or only `property`'s, and fails unless each is
/// caught. With `shard`, it runs only that shard's cases, and fails if
/// the shard has none. With `changed_since`, it runs only the cases a
/// change since that revision affects, and passes if none is. With
/// `targets`, it runs only the cases a simulator fuzz target runs, and
/// confirms each through its target.
pub(super) fn run(
    repo_root: &Path,
    property: Option<&str>,
    shard: Option<Shard>,
    changed_since: Option<&str>,
    targets: bool,
) -> Result<(), BoxError> {
    let mut cases: Vec<Case> = cases(repo_root)?
        .into_iter()
        .filter(|case| property.is_none_or(|p| case.property == p))
        .collect();
    if targets {
        let harnesses = sim_minimize::fuzz_targets(repo_root)?;
        let mut on_targets = Vec::new();
        for mut case in cases {
            // A typed case has no fuzz input for a target to run.
            if let Witness::Input(input) = &case.witness {
                case.target = target_of(&harnesses, &harness_of(repo_root, input)?);
            }
            if case.target.is_some() {
                on_targets.push(case);
            }
        }
        cases = on_targets;
        for (target, harness) in &harnesses {
            let Some(harness) = harness else { continue };
            let names: Vec<String> = cases
                .iter()
                .filter(|case| case.target.as_ref() == Some(target))
                .map(|case| format!("{} {}", case.property, case.patch.display()))
                .collect();
            match names.as_slice() {
                [] => println!("{target} ({harness}): no retained production bypass"),
                names => println!("{target} ({harness}): {}", names.join(", ")),
            }
        }
    }
    if cases.is_empty() {
        return Err(match (property, targets) {
            (Some(property), true) => {
                format!("no planted case for {property} runs on a fuzz target").into()
            }
            (Some(property), false) => format!("no planted case for {property}").into(),
            (None, true) => "no planted case runs on a fuzz target".into(),
            (None, false) => format!("no planted cases under {PLANTED_DIR}").into(),
        });
    }
    // The shard picks from the catalogue before `changed_since` does, so a
    // case's shard doesn't move with the changes.
    let total = cases.len();
    let cases = match shard {
        Some(shard) => {
            let picked = shard.select(cases);
            if picked.is_empty() {
                return Err(
                    format!("shard {shard} holds none of the {total} planted cases").into(),
                );
            }
            picked
        }
        None => cases,
    };
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
    let worktree = work.join("worktree");
    remove_worktree(repo_root, &worktree)?;
    fs::create_dir_all(&work)?;
    run_command(
        git(repo_root)
            .args(["worktree", "add", "--detach"])
            .arg(&worktree)
            .arg("HEAD"),
    )?;
    let failed = run_cases(&worktree, &work, &cases);
    remove_worktree(repo_root, &worktree)?;
    let failed = failed?;
    if failed > 0 {
        return Err(format!("{failed} of {} planted cases failed", cases.len()).into());
    }
    Ok(())
}

/// The index [`run`] gives case `name` of `property` when it runs that
/// property without `--targets`: its position among the property's cases
/// in path order, which names its `case-<index>` directory.
pub(super) fn case_index(repo_root: &Path, property: &str, name: &str) -> Result<usize, BoxError> {
    cases(repo_root)?
        .into_iter()
        .filter(|case| case.property == property)
        .position(|case| case.patch.file_stem().and_then(|stem| stem.to_str()) == Some(name))
        .ok_or_else(|| format!("{property}/{name} has no planted case").into())
}

/// The index [`run`] gives case `name` of `property` when it runs that
/// property with `--targets`: its position among the property's cases that
/// a fuzz target runs, as [`run`] keeps them, which names its `case-<index>`
/// directory there. `None` if no fuzz target runs the case.
pub(super) fn target_case_index(
    repo_root: &Path,
    property: &str,
    name: &str,
) -> Result<Option<usize>, BoxError> {
    let harnesses = sim_minimize::fuzz_targets(repo_root)?;
    let mut on_targets = Vec::new();
    for case in cases(repo_root)?
        .into_iter()
        .filter(|case| case.property == property)
    {
        if let Witness::Input(input) = &case.witness {
            if target_of(&harnesses, &harness_of(repo_root, input)?).is_some() {
                on_targets.push(case);
            }
        }
    }
    Ok(on_targets
        .iter()
        .position(|case| case.patch.file_stem().and_then(|stem| stem.to_str()) == Some(name)))
}

/// Every case under `repo_root`'s [`PLANTED_DIR`], in path order.
fn cases(repo_root: &Path) -> Result<Vec<Case>, BoxError> {
    let root = repo_root.join(PLANTED_DIR);
    planted_layout::files(&root, "patch")?
        .into_iter()
        .map(|patch| {
            let property = planted_layout::property_id(&root, &patch)
                .ok_or_else(|| format!("{} isn't under a property", patch.display()))?;
            let case_file = patch.with_extension("toml");
            let CaseFile {
                input,
                typed,
                checks,
                also_violates,
                set,
                missing_evidence,
            } = toml::from_str(
                &fs::read_to_string(&case_file)
                    .map_err(|error| format!("{}: {error}", case_file.display()))?,
            )
            .map_err(|error| format!("{}: {error}", case_file.display()))?;
            let witness = match (input, typed) {
                (Some(input), None) => Witness::Input(input),
                (None, Some(typed)) => Witness::Typed {
                    plan: typed
                        .plan_file()
                        .map_err(|error| format!("{}: {error}", case_file.display()))?,
                    harness: typed.harness,
                    scenario: typed.scenario,
                },
                (input, _) => {
                    return Err(format!(
                        "{}: a case names either `input` or `[typed]`, and this names {}",
                        case_file.display(),
                        if input.is_some() { "both" } else { "neither" }
                    )
                    .into())
                }
            };
            let files = witness.files();
            for path in files.iter().chain(&checks) {
                if !is_plain_relative(path) {
                    return Err(format!(
                        "{}: {} isn't a plain path relative to the repository root",
                        case_file.display(),
                        path.display()
                    )
                    .into());
                }
            }
            if !set.is_empty() && !also_violates.is_empty() {
                return Err(format!(
                    "{}: a case names either `set` or `also_violates`, not both",
                    case_file.display()
                )
                .into());
            }
            // No fuzz target confirms a harness failure, and a nested or
            // co-violating case is caught by its violations.
            let harness_caught_alone = matches!(witness, Witness::Typed { .. })
                && set.is_empty()
                && also_violates.is_empty();
            if missing_evidence.is_some() && !harness_caught_alone {
                return Err(format!(
                    "{}: a harness-caught case is typed and names no `set` or `also_violates`",
                    case_file.display()
                )
                .into());
            }
            Ok(Case {
                property,
                patch,
                witness,
                checks,
                also_violates,
                set,
                missing_evidence,
                target: None,
            })
        })
        .collect()
}

/// True if `path` is non-empty and made only of ordinary names, so that it
/// compares with `git diff`'s repository-relative paths component by
/// component.
fn is_plain_relative(path: &Path) -> bool {
    path.components().next().is_some()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// The paths `git diff` reports changed between `rev`'s merge base with
/// `HEAD` and `HEAD`, relative to the repository root. `rev` must name a
/// commit. Without rename detection, a renamed file shows up as its deleted
/// old path and its added new one, so a case watching the old path sees the
/// rename.
fn changed_since_rev(repo_root: &Path, rev: &str) -> Result<Vec<PathBuf>, BoxError> {
    // Resolving `rev` first keeps git from reading it as an option, a
    // pathspec or a range.
    let resolved = git(repo_root)
        .args([
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &format!("{rev}^{{commit}}"),
        ])
        .output()?;
    if !resolved.status.success() {
        return Err(format!("`{rev}` doesn't name a commit").into());
    }
    let base = String::from_utf8(resolved.stdout)?.trim().to_owned();
    let output = git(repo_root)
        .args([
            "diff",
            "--name-only",
            "--no-renames",
            "-z",
            &format!("{base}...HEAD"),
            "--",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "`git diff {base}...HEAD` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    // `-z` ends each path with a NUL and leaves it unquoted.
    Ok(String::from_utf8(output.stdout)?
        .split_terminator('\0')
        .map(PathBuf::from)
        .collect())
}

/// Every path whose change can break `case`'s catch, relative to
/// `repo_root`: the patch and its case file, the witness's files, the files
/// the patch changes, the case's checks, and [`SHARED`].
fn watched(repo_root: &Path, case: &Case) -> Result<Vec<PathBuf>, BoxError> {
    let relative = |path: &Path| {
        path.strip_prefix(repo_root)
            .map(Path::to_path_buf)
            .map_err(|_| format!("{} isn't under the repository", path.display()))
    };
    let mut watched = vec![
        relative(&case.patch)?,
        relative(&case.patch.with_extension("toml"))?,
    ];
    watched.extend(case.witness.files());
    watched.extend(SHARED.iter().map(PathBuf::from));
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

/// Where confirmations through a fuzz target build, in the planted work
/// directory `work`: [`CONFIRM_BUILDS`], beside the scratch worktree rather
/// than in it.
fn confirm_builds(work: &Path) -> PathBuf {
    work.join(CONFIRM_BUILDS)
}

/// Removes the scratch worktree at `worktree`, if there is one.
fn remove_worktree(repo_root: &Path, worktree: &Path) -> Result<(), BoxError> {
    if worktree.exists() {
        run_command(
            git(repo_root)
                .args(["worktree", "remove", "--force"])
                .arg(worktree),
        )?;
    }
    Ok(())
}

/// Builds the unpatched simulator in `worktree` once, then runs each case
/// there, writing its traces under `work`, and resets the worktree after
/// each. Returns how many cases failed.
fn run_cases(worktree: &Path, work: &Path, cases: &[Case]) -> Result<usize, BoxError> {
    let target = work.join("target");
    run_command(&mut cargo_sim(worktree, &target))?;
    // The patched builds reuse the target directory, so the baseline keeps
    // its own copy of the binary.
    let baseline = work.join("baseline-zakura-sim");
    fs::copy(target.join(PROFILE).join("zakura-sim"), &baseline)?;
    // Every catch is judged against this baseline, so it must be the clean
    // checkout's own build, never one an earlier run left behind.
    if let Err(why) = built_from(&baseline, worktree)? {
        return Err(format!(
            "the baseline simulator isn't {}'s: {why}",
            worktree.display()
        )
        .into());
    }
    let mut failed = 0;
    for (index, case) in cases.iter().enumerate() {
        let scratch = work.join(format!("case-{index}"));
        fs::create_dir_all(&scratch)?;
        let builds = confirm_builds(work);
        let verdict = check_case(worktree, &scratch, &target, &builds, &baseline, case);
        run_command(git(worktree).args(["reset", "--hard", "--quiet", "HEAD"]))?;
        run_command(git(worktree).args(["clean", "-fdq"]))?;
        let verdict = verdict?;
        println!("{} {}: {verdict}", case.property, case.patch.display());
        if !matches!(verdict, Verdict::Caught(_)) {
            failed += 1;
        }
    }
    Ok(failed)
}

/// Runs the input on the unpatched `baseline` binary, then patches
/// `worktree` and runs it again, building in `target` and writing both
/// traces under `scratch`. A confirmation through a fuzz target builds
/// under `builds`.
fn check_case(
    worktree: &Path,
    scratch: &Path,
    target: &Path,
    builds: &Path,
    baseline: &Path,
    case: &Case,
) -> Result<Verdict, BoxError> {
    let harness = case.witness.harness(worktree)?;
    let input = case.witness.located(worktree);
    let baseline_binary = baseline;
    let baseline = record(
        in_phase(Command::new(baseline), "baseline"),
        &harness,
        &input,
        &case.property,
        Expect::Holds,
        &scratch.join("baseline"),
        Some((RUN_DEADLINE, &NEVER_INTERRUPTED)),
    )?;
    if let Some(why) = baseline_failure(&case.property, &baseline) {
        return Ok(Verdict::BaselineFails(why));
    }
    // The baseline replays fresh with its own binary too (C4), so the
    // acceptance tool reads both of the case's runs with their replays.
    if let Some(why) = replay_misses(
        baseline_binary,
        &scratch.join("baseline"),
        RUN_DEADLINE,
        "replay-baseline",
    )? {
        return Ok(Verdict::BaselineFails(format!(
            "its trace doesn't replay Exact: {why}"
        )));
    }
    if let Some(verdict) = apply_case_patch(worktree, case)? {
        return Ok(verdict);
    }
    if let Err(error) = run_command(&mut cargo_sim(worktree, target)) {
        return Ok(Verdict::Failed(format!(
            "the patched build failed: {error}"
        )));
    }
    let patched = target.join(PROFILE).join("zakura-sim");
    // The patched builds reuse the target directory, so a catch counts only
    // from a binary built from this case's patched checkout (C5).
    if let Err(why) = built_from(&patched, worktree)? {
        return Ok(Verdict::StaleBinary(why));
    }
    let verdict = judge_patched(&patched, &harness, &input, case, scratch, RUN_DEADLINE)?;
    match (&verdict, &case.target) {
        (Verdict::Caught(_), Some(fuzz)) => {
            Ok(through_target(worktree, builds, scratch, case, fuzz, &harness)?.unwrap_or(verdict))
        }
        _ => Ok(verdict),
    }
}

/// Applies `case`'s patch to `worktree`, a checkout of `HEAD`. `Some` is
/// the verdict of a case that can't go on: the patch doesn't apply, or it
/// changes, deletes or renames one of the case's witness files. The
/// patched runs read those files after the patch while the baseline read
/// the committed ones, so a catch might come from the changed witness
/// rather than the code change.
fn apply_case_patch(worktree: &Path, case: &Case) -> Result<Option<Verdict>, BoxError> {
    if let Err(why) = apply(worktree, &case.patch) {
        return Ok(Some(Verdict::PatchDoesntApply(why)));
    }
    // `--no-renames` lists a renamed file's old path, not only its new one.
    let output = git(worktree)
        .args(["diff", "--name-only", "--no-renames", "-z", "HEAD", "--"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "`git diff` failed in {}: {}",
            worktree.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let changed = String::from_utf8(output.stdout)?;
    let changed: Vec<&Path> = changed.split_terminator('\0').map(Path::new).collect();
    let touched: Vec<PathBuf> = case
        .witness
        .files()
        .into_iter()
        .filter(|file| changed.contains(&file.as_path()))
        .collect();
    Ok((!touched.is_empty()).then_some(Verdict::PatchesWitness(touched)))
}

/// Records `input` on `harness` with the patched `zakura_sim` binary into
/// `scratch`'s `patched`, and judges it: the patched run's verdict, then,
/// for a catch, the campaign's, then the patched trace's fresh replay's.
/// Each run gets `deadline`.
fn judge_patched(
    zakura_sim: &Path,
    harness: &str,
    input: &Located,
    case: &Case,
    scratch: &Path,
    deadline: Duration,
) -> Result<Verdict, BoxError> {
    let trace = scratch.join("patched");
    let patched = record(
        in_phase(Command::new(zakura_sim), "patched"),
        harness,
        input,
        &case.property,
        case.expect(),
        &trace,
        Some((deadline, &NEVER_INTERRUPTED)),
    )?;
    let verdict = match case.classify(&patched) {
        Verdict::Caught(None) => campaign(
            zakura_sim,
            harness,
            case,
            &scratch.join("campaign"),
            deadline,
        )?,
        verdict => return Ok(verdict),
    };
    fresh_replay(verdict, || {
        replay_misses(zakura_sim, &trace, deadline, "replay-patched")
    })
}

impl Case {
    /// What every patched run of the case expects: its property violated,
    /// beside `also_violates`, or with exactly its `set`.
    fn expect(&self) -> Expect<'_> {
        if self.set.is_empty() {
            Expect::Violated(&self.also_violates)
        } else {
            Expect::ViolatedSet(&self.set)
        }
    }

    /// The properties a confirmation through a fuzz target may find
    /// violated beside the case's: its `also_violates`, or its nested `set`.
    /// The committed input's recording has already shown each member of the
    /// set violated, exactly ([`Case::expect`]), so the confirmation checks
    /// that the target selects the case's property and allows the members
    /// beside it, as it allows co-violations.
    fn beside(&self) -> Vec<String> {
        self.also_violates
            .iter()
            .chain(&self.set)
            .cloned()
            .collect()
    }

    /// What `patched`, one of the case's patched runs, shows about it:
    /// [`classify_missing_evidence`] for a harness-caught case, and
    /// [`classify_set`] for any other.
    fn classify(&self, patched: &Run) -> Verdict {
        match &self.missing_evidence {
            Some(detail) => classify_missing_evidence(&self.property, detail, patched),
            None => classify_set(&self.property, &self.also_violates, &self.set, patched),
        }
    }
}

/// The fuzz target that runs `harness`, from the fuzz package's targets
/// and the harnesses they run, `harnesses`. The first by name, if several
/// do.
fn target_of(harnesses: &BTreeMap<String, Option<String>>, harness: &str) -> Option<String> {
    harnesses
        .iter()
        .find(|(_, runs)| runs.as_deref() == Some(harness))
        .map(|(target, _)| target.clone())
}

/// Confirms `case`'s committed input in the patched `worktree` through the
/// fuzz target `target`, which runs `harness`, in fresh processes, with
/// [`sim_minimize::confirm`]: the target must crash with the case's
/// property, beside no other violation but the case's `also_violates`, and
/// name `harness`, `zakura-sim confirm` must find the property's trigger
/// history, and a fresh replay must match Exact. Builds
/// under `builds`, which outlives the worktree, and writes under `scratch`.
///
/// The builds are reused, so both pinned binaries must also name the
/// patched source: each embeds the source digest that `zakura-sim`'s build
/// script computed, and the digest must be the patched worktree's
/// ([`names_source`]). `None` if every check passed.
fn through_target(
    worktree: &Path,
    builds: &Path,
    scratch: &Path,
    case: &Case,
    target: &str,
    harness: &str,
) -> Result<Option<Verdict>, BoxError> {
    let Witness::Input(input) = &case.witness else {
        unreachable!("`run` gives a typed case no target");
    };
    let out = path::absolute(scratch.join("target"))?;
    if out.exists() {
        fs::remove_dir_all(&out)?;
    }
    let request = sim_minimize::Request {
        target: target.to_owned(),
        harness: harness.to_owned(),
        input: path::absolute(worktree.join(input))?,
        property: case.property.clone(),
        also_violates: case.beside(),
        out: out.clone(),
        budget: sim_minimize::Budget {
            runs: 1,
            wall: TARGET_CHILD,
            child: TARGET_CHILD,
        },
        sanitizer: Sanitizer::Address,
    };
    let report = sim_minimize::confirm(
        worktree,
        &path::absolute(builds)?,
        &request,
        &AtomicBool::new(false),
    )?;
    let misses = |why: String| {
        Ok(Some(Verdict::TargetMisses {
            target: target.to_owned(),
            why: format!(
                "{why}; see {}",
                out.join(sim_minimize::REPORT_FILE).display()
            ),
        }))
    };
    if report.status != Status::Confirmed {
        return misses(format!(
            "{}: {}",
            report.status,
            report.reason.as_deref().unwrap_or("no reason given")
        ));
    }
    let source = provenance::Provenance::read(worktree, provenance::SOURCE_PATHS).source;
    let bin = sim_minimize::bin_dir(&out);
    for binary in [bin.join("zakura-sim"), bin.join(target)] {
        if let Err(why) = names_source(&fs::read(&binary)?, &source) {
            return misses(format!("{}: {why}", binary.display()));
        }
    }
    println!(
        "{} {}: confirmed through {target}; see {}",
        case.property,
        case.patch.display(),
        out.join(sim_minimize::REPORT_FILE).display()
    );
    Ok(None)
}

/// Whether the binary at `binary` was built from `checkout` as it stands:
/// it embeds the checkout's source digest ([`names_source`]). An unknown
/// digest, which git couldn't compute, never matches.
fn built_from(binary: &Path, checkout: &Path) -> Result<Result<(), String>, BoxError> {
    let source = provenance::Provenance::read(checkout, provenance::SOURCE_PATHS).source;
    Ok(names_source(&fs::read(binary)?, &source))
}

/// Checks that `binary` embeds `source`, the patched checkout's source
/// digest as `zakura-sim`'s build script computes it, so it was built from
/// that source and not reused from an earlier one. A digest git couldn't
/// compute names no source.
pub(crate) fn names_source(binary: &[u8], source: &str) -> Result<(), String> {
    if source == provenance::UNKNOWN {
        return Err("git couldn't compute the patched checkout's source digest".to_owned());
    }
    if binary
        .windows(source.len())
        .any(|window| window == source.as_bytes())
    {
        Ok(())
    } else {
        Err(format!(
            "the build doesn't embed the patched checkout's source digest {source}"
        ))
    }
}

/// Records [`CAMPAIGN_INPUTS`] fuzz inputs on `harness` with the patched
/// `binary`, which [`check_case`] built. Each input and its trace go under
/// `dir`.
fn campaign(
    binary: &Path,
    harness: &str,
    case: &Case,
    dir: &Path,
    deadline: Duration,
) -> Result<Verdict, BoxError> {
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;
    let mut caught = 0;
    let mut missed = BTreeMap::new();
    for (index, bytes) in campaign_inputs(&case.property).into_iter().enumerate() {
        let input = dir.join(format!("input-{index}.bin"));
        fs::write(&input, bytes)?;
        let run = record(
            in_phase(Command::new(binary), "campaign"),
            harness,
            &Located::Input(input.clone()),
            &case.property,
            case.expect(),
            &dir.join(format!("run-{index}")),
            Some((deadline, &NEVER_INTERRUPTED)),
        )?;
        match case.classify(&run) {
            Verdict::Caught(_) => caught += 1,
            Verdict::NotCaught(outcome) => *missed.entry(outcome).or_default() += 1,
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
        missed,
    })))
}

/// The campaign's inputs for `property`: [`CAMPAIGN_INPUTS`] byte strings of
/// up to [`CAMPAIGN_INPUT_BYTES`], from a SplitMix64 stream that the
/// property's ID seeds, so a case's campaign is the same on every run and
/// doesn't move when other cases are added.
pub(crate) fn campaign_inputs(property: &str) -> Vec<Vec<u8>> {
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

/// `cargo build` for `worktree`'s `zakura-sim` binary, with [`PROFILE`] and
/// the simulator's flags, in `target`.
///
/// A bypass patch can leave the code it bypasses unused, so the build
/// reports warnings but doesn't fail on them, even where the environment
/// denies them, as CI's Rust setup does with `CARGO_BUILD_WARNINGS=deny`.
pub(super) fn cargo_sim(worktree: &Path, target: &Path) -> Command {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(cargo);
    command
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_BUILD_WARNINGS", "warn")
        .current_dir(worktree)
        .arg("build")
        .args([
            "--locked",
            "--profile",
            PROFILE,
            "-p",
            "zakura-sim",
            "--bin",
            "zakura-sim",
        ]);
    command
}

/// What a witness test that [`record`] runs expects of the case's property.
#[derive(Clone, Copy, Debug)]
enum Expect<'a> {
    /// The property applies and holds, and no property is violated.
    Holds,
    /// The property is violated, and no other property is, except the
    /// listed ones, the case's `also_violates`.
    Violated(&'a [String]),
    /// The property and every listed one, the case's `set`, are violated,
    /// and no other property is.
    ViolatedSet(&'a [String]),
}

impl Expect<'_> {
    /// `zakura-sim record`'s `--expect` flag and value, then its
    /// `--also-violates` flag and value if the list isn't empty.
    fn args(self) -> Vec<String> {
        match self {
            Expect::Holds => vec!["--expect".to_owned(), "holds".to_owned()],
            Expect::Violated(also_violates) => {
                let mut args = vec!["--expect".to_owned(), "violated".to_owned()];
                if !also_violates.is_empty() {
                    args.extend(["--also-violates".to_owned(), also_violates.join(",")]);
                }
                args
            }
            Expect::ViolatedSet(set) => vec![
                "--expect".to_owned(),
                "violated-set".to_owned(),
                "--also-violates".to_owned(),
                set.join(","),
            ],
        }
    }
}

/// `command` with its runs' acceptance phase set to `phase`, so sim-7's
/// acceptance tool can tell the baseline, patched, campaign and replay runs
/// apart. It changes nothing unless the tool also names a records
/// directory.
fn in_phase(mut command: Command, phase: &str) -> Command {
    command.env("ZAKURA_SIM_ACCEPTANCE_PHASE", phase);
    command
}

/// Records `input`, a fuzz input or a typed scenario and its Plan bytes, on
/// `harness` with `zakura_sim`, a `zakura-sim` command, as a witness test of
/// `property` that expects `expect`, writing the trace to `out`, and reads
/// its `end` record and the test's result.
///
/// With a `deadline` and its interrupt flag, the command's output goes to
/// `<out>.log`. A run past the deadline, or one that a signal killed, is
/// [`Run::Killed`], whatever files it already wrote. A set flag kills the
/// run and stops the pilot. A run whose witness passed but that didn't
/// exit 0 didn't finish either, so it's [`Run::Failed`]. A failed witness
/// exits nonzero anyway, and keeps its reason.
fn record(
    mut zakura_sim: Command,
    harness: &str,
    input: &Located,
    property: &str,
    expect: Expect<'_>,
    out: &Path,
    deadline: Option<(Duration, &AtomicBool)>,
) -> Result<Run, BoxError> {
    if out.exists() {
        fs::remove_dir_all(out)?;
    }
    zakura_sim
        .args(["record", "--sim", harness])
        .args(input_args(input)?)
        .arg("--out")
        .arg(out)
        .args(["--witness", property])
        .args(expect.args());
    let (ended, exited_ok) = match deadline {
        None => {
            let status = zakura_sim.status()?;
            (format!("exited with {status}"), status.success())
        }
        Some((deadline, interrupt)) => {
            let log = out.with_extension("log");
            let end = sim_minimize::run_child(&mut zakura_sim, &log, Some(deadline), interrupt)?;
            let ended = format!("{end}; see {}", log.display());
            match end {
                sim_minimize::ChildEnd::Interrupted => return Err(pilot::INTERRUPTED_ERROR.into()),
                // Its trace and witness result may look whole, but the
                // run didn't finish.
                sim_minimize::ChildEnd::TimedOut | sim_minimize::ChildEnd::Signaled(_) => {
                    return Ok(Run::Killed(format!("`zakura-sim record` {ended}")))
                }
                sim_minimize::ChildEnd::Exited(code) => (ended, code == 0),
            }
        }
    };
    // A failed witness test exits 1 too, but only after writing its result.
    let Ok(witness) = fs::read_to_string(out.join("witness.json")) else {
        return Ok(Run::Failed(format!("`zakura-sim record` {ended}")));
    };
    let witness: WitnessFile = serde_json::from_str(&witness)?;
    if witness.passed && !exited_ok {
        return Ok(Run::Failed(format!(
            "`zakura-sim record` {ended} after writing a passing witness result"
        )));
    }
    let trace = match fs::read_to_string(out.join("trace.jsonl")) {
        Ok(trace) => trace,
        Err(error) => {
            return Ok(Run::Failed(format!(
                "`zakura-sim record` {ended} and wrote its witness result, but its trace is unreadable: {error}"
            )))
        }
    };
    let end = trace.lines().next_back().ok_or("the trace is empty")?;
    let mut run = end_of(property, end)?;
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

/// `zakura-sim record`'s arguments for `input`: `--input` and the file, or
/// `--scenario` with the scenario file's text, then `--plan` with the Plan
/// bytes file or `--script` with the scripted Plan's file.
fn input_args(input: &Located) -> Result<Vec<OsString>, BoxError> {
    Ok(match input {
        Located::Input(path) => vec!["--input".into(), path.into()],
        Located::Typed { scenario, plan } => vec![
            "--scenario".into(),
            fs::read_to_string(scenario)
                .map_err(|error| format!("{}: {error}", scenario.display()))?
                .into(),
            plan.flag().into(),
            plan.path().into(),
        ],
    })
}

/// `verdict`, unless it's a catch and `replay`, the fresh replay of the
/// patched run's trace, says why it missed: then [`Verdict::ReplayMisses`].
/// Every catch replays, a fuzz input's and a typed case's alike (C4).
fn fresh_replay(
    verdict: Verdict,
    replay: impl FnOnce() -> Result<Option<String>, BoxError>,
) -> Result<Verdict, BoxError> {
    if !matches!(verdict, Verdict::Caught(_)) {
        return Ok(verdict);
    }
    Ok(match replay()? {
        Some(why) => Verdict::ReplayMisses(why),
        None => verdict,
    })
}

/// A recorded run's fresh replay, the directory inside its trace directory,
/// as the acceptance tool's reader (`tool::REPLAY_DIR`) names it.
pub(super) const REPLAY_DIR: &str = "replay";
/// A fresh replay's full report inside [`REPLAY_DIR`]
/// (`tool::FULL_REPORT_FILE`).
const REPLAY_REPORT: &str = "full-report.json";
/// How a fresh replay's child ended, inside [`REPLAY_DIR`]
/// (`tool::REPLAY_FILE`, a `ReplayRun`).
const REPLAY_FILE: &str = "replay.json";

/// Replays the trace in `trace` with `zakura_sim`, the binary that recorded
/// it, in a fresh process, and says why it didn't match Exact, or `None` if
/// it did. The replay writes its full report to `<trace>/replay/`, beside
/// `replay.json`, its child's end and output, which the acceptance tool
/// reads as the run's fresh replay (C4, C8). A replay past `deadline` is
/// killed and misses. `phase` names it to sim-7's acceptance tool
/// ([`in_phase`]) as the replay of the baseline or of the patched run.
fn replay_misses(
    zakura_sim: &Path,
    trace: &Path,
    deadline: Duration,
    phase: &str,
) -> Result<Option<String>, BoxError> {
    let dir = trace.join(REPLAY_DIR);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let log = dir.join("replay.log");
    let mut replay = in_phase(Command::new(zakura_sim), phase);
    replay
        .arg("replay")
        .arg(trace)
        .arg("--report")
        .arg(dir.join(REPLAY_REPORT));
    let end = sim_minimize::run_child(&mut replay, &log, Some(deadline), &NEVER_INTERRUPTED)?;
    let output = fs::read_to_string(&log).unwrap_or_default();
    fs::write(
        dir.join(REPLAY_FILE),
        serde_json::json!({
            "exited_ok": end == sim_minimize::ChildEnd::Exited(0),
            "killed": matches!(
                end,
                sim_minimize::ChildEnd::TimedOut
                    | sim_minimize::ChildEnd::Signaled(_)
                    | sim_minimize::ChildEnd::Interrupted
            ),
            "log": output,
        })
        .to_string(),
    )?;
    Ok(match end {
        sim_minimize::ChildEnd::Exited(0) => None,
        end => Some(format!("`zakura-sim replay` {end}: {}", output.trim())),
    })
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
    // Each report's outcome, typed, so an unknown outcome or reason fails
    // the read.
    let reports = properties
        .iter()
        .map(|report| {
            let outcome = report
                .get("outcome")
                .ok_or("an end record's report has no outcome")?;
            Ok((
                text(report.get("property")).unwrap_or_default(),
                EndOutcome::parse(outcome)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Run::Ended {
        violation: text(end.get("violation").and_then(|v| v.get("property"))),
        others_violated: reports
            .iter()
            .filter(|(id, outcome)| id != property && matches!(outcome, EndOutcome::Violated {}))
            .map(|(id, _)| id.clone())
            .collect(),
        outcome: reports
            .into_iter()
            .find(|(id, _)| id == property)
            .map(|(_, outcome)| outcome),
        harness: text(end.get("harness")),
        witness: None,
    })
}

/// Why `baseline` doesn't hold `property` cleanly, or `None` if it does.
fn baseline_failure(property: &str, baseline: &Run) -> Option<String> {
    match baseline {
        Run::Failed(why) | Run::Killed(why) => Some(why.clone()),
        Run::Ended {
            harness: Some(failure),
            ..
        } => Some(format!("harness failure: {failure}")),
        Run::Ended {
            violation: Some(violated),
            ..
        } => Some(format!("{violated} is violated")),
        Run::Ended {
            others_violated, ..
        } if !others_violated.is_empty() => Some(format!("{} is violated", others_violated[0])),
        Run::Ended { outcome, .. } if !matches!(outcome, Some(EndOutcome::Held { .. })) => {
            Some(format!(
                "{property} is {}",
                outcome
                    .as_ref()
                    .map_or_else(|| "not reported".to_owned(), ToString::to_string)
            ))
        }
        Run::Ended {
            witness: Some(why), ..
        } => Some(format!("its witness test fails: {why}")),
        Run::Ended { .. } => None,
    }
}

/// What the patched run shows about `property`. A violation of a property
/// in `also_violates`, which the patched bug breaks too, is expected beside
/// `property`'s: it neither counts as the catch nor fails the case. Any
/// other violated property makes the verdict [`Verdict::Unrelated`]. The
/// committed input and every campaign input are judged by this one rule.
fn classify(property: &str, also_violates: &[String], patched: &Run) -> Verdict {
    classify_set(property, also_violates, &[], patched)
}

/// [`classify`] for a case with a nested `set`, which every member of must
/// violate beside `property` for a catch. A member is expected, as an
/// `also_violates` property is, and a run that violates `property` without
/// every member isn't a catch.
fn classify_set(
    property: &str,
    also_violates: &[String],
    set: &[String],
    patched: &Run,
) -> Verdict {
    let (violation, others_violated, outcome, witness) = match patched {
        Run::Failed(why) | Run::Killed(why) => return Verdict::Failed(why.clone()),
        Run::Ended {
            harness: Some(failure),
            ..
        } => return Verdict::Harness(failure.clone()),
        Run::Ended {
            violation,
            others_violated,
            outcome,
            witness,
            ..
        } => (violation, others_violated, outcome, witness),
    };
    let violated = || violation.iter().chain(others_violated);
    let expected = |id: &String| id == property || also_violates.contains(id) || set.contains(id);
    if let Some(unrelated) = violated().find(|id| !expected(id)) {
        return Verdict::Unrelated(unrelated.clone());
    }
    if violation.as_deref() != Some(property) && !matches!(outcome, Some(EndOutcome::Violated {})) {
        // A listed property's violation alone misses the patch.
        return match violated().find(|id| *id != property) {
            Some(listed) => Verdict::NotCaught(format!("violated {listed}")),
            None => outcome.as_ref().map_or(Verdict::Unreported, |outcome| {
                Verdict::NotCaught(outcome.to_string())
            }),
        };
    }
    if let Some(missing) = set
        .iter()
        .find(|member| !violated().any(|id| id == *member))
    {
        return Verdict::NotCaught(format!("violated, but not {missing} of its set"));
    }
    match witness {
        Some(why) => Verdict::WitnessFails(why.clone()),
        None => Verdict::Caught(None),
    }
}

/// What `patched` shows about a harness-caught case of `property` whose
/// named diagnostic is `detail`. It's a catch only if the run ended at
/// `property`'s missing evidence, `<property> lacks evidence after step
/// <n>: <detail>` as the simulator writes it, whose detail says `detail`,
/// and violated nothing. Any other harness failure is still
/// [`Verdict::Harness`], so no other failure ever counts. The witness test
/// fails at any harness failure, which can't show a trigger history, so its
/// result doesn't decide this.
fn classify_missing_evidence(property: &str, detail: &str, patched: &Run) -> Verdict {
    let (violation, others_violated, outcome, harness) = match patched {
        Run::Failed(why) | Run::Killed(why) => return Verdict::Failed(why.clone()),
        Run::Ended {
            violation,
            others_violated,
            outcome,
            harness,
            ..
        } => (violation, others_violated, outcome, harness),
    };
    if let Some(violated) = violation.iter().chain(others_violated).next() {
        return Verdict::Unrelated(violated.clone());
    }
    if matches!(outcome, Some(EndOutcome::Violated {})) {
        return Verdict::NotCaught(format!("violated {property} at no harness failure"));
    }
    match harness {
        Some(failure) if names_missing_evidence(property, detail, failure) => Verdict::Caught(None),
        Some(failure) => Verdict::Harness(failure.clone()),
        None => outcome.as_ref().map_or(Verdict::Unreported, |outcome| {
            Verdict::NotCaught(outcome.to_string())
        }),
    }
}

/// Whether `failure` is `property`'s missing evidence whose detail says
/// `detail`.
fn names_missing_evidence(property: &str, detail: &str, failure: &str) -> bool {
    failure
        .strip_prefix(property)
        .and_then(|rest| rest.strip_prefix(" lacks evidence after step "))
        .and_then(|rest| rest.split_once(": "))
        .is_some_and(|(step, shown)| {
            !step.is_empty() && step.bytes().all(|b| b.is_ascii_digit()) && shown.contains(detail)
        })
}

/// `git` in `dir`.
pub(super) fn git(dir: &Path) -> Command {
    let mut command = Command::new("git");
    command.current_dir(dir);
    command
}

/// Runs `script` until it starts. A child that another test's thread forks
/// while a script is open for writing holds it open until that child execs,
/// and running the script meanwhile fails with ETXTBSY. Once it starts, no
/// process holds it open for writing.
#[cfg(all(test, unix))]
pub(super) fn wait_until_executable(script: &Path) {
    while let Err(error) = Command::new(script)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        assert_eq!(error.raw_os_error(), Some(libc::ETXTBSY), "{error}");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
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
            classify(ID, &[], &run(Some(ID), &[(ID, VIOLATED)])),
            Verdict::Caught(None)
        );
        // A violation that didn't stop the run, as for a Known property.
        assert_eq!(
            classify(ID, &[], &run(None, &[(ID, VIOLATED)])),
            Verdict::Caught(None)
        );
        assert_eq!(
            classify(
                ID,
                &[],
                &run(
                    Some("sim/no-task-panic"),
                    &[(ID, HELD), ("sim/no-task-panic", VIOLATED)]
                )
            ),
            Verdict::Unrelated("sim/no-task-panic".to_owned())
        );
        assert_eq!(
            classify(
                ID,
                &[],
                &run(None, &[(ID, VIOLATED), ("other/draft", VIOLATED)])
            ),
            Verdict::Unrelated("other/draft".to_owned())
        );
        assert_eq!(
            classify(ID, &[], &run(None, &[(ID, HELD)])),
            Verdict::NotCaught("held".to_owned())
        );
        assert_eq!(classify(ID, &[], &run(None, &[])), Verdict::Unreported);
        assert_eq!(
            classify(ID, &[], &Run::Failed("build".to_owned())),
            Verdict::Failed("build".to_owned())
        );
    }

    /// The witness test's flags carry the case's `also_violates` only for a
    /// violated expectation, and only when the list isn't empty.
    #[test]
    fn witness_flags_carry_the_co_violations() {
        let also = ["a/one".to_owned(), "b/two".to_owned()];
        assert_eq!(Expect::Holds.args(), ["--expect", "holds"]);
        assert_eq!(Expect::Violated(&[]).args(), ["--expect", "violated"]);
        assert_eq!(
            Expect::Violated(&also).args(),
            ["--expect", "violated", "--also-violates", "a/one,b/two"]
        );
    }

    /// A property in the case's `also_violates` may be violated beside the
    /// case's own, on the committed input as on a campaign input. Only the
    /// case's own violation catches the patch, and an unlisted violation
    /// still fails it.
    #[test]
    fn listed_co_violations_are_expected() {
        let also = ["other/conformant".to_owned()];
        assert_eq!(
            classify(
                ID,
                &also,
                &run(None, &[(ID, VIOLATED), ("other/conformant", VIOLATED)])
            ),
            Verdict::Caught(None)
        );
        // The case's own violation stopped the run, as for an `Active` one.
        assert_eq!(
            classify(
                ID,
                &also,
                &run(Some(ID), &[(ID, VIOLATED), ("other/conformant", VIOLATED)])
            ),
            Verdict::Caught(None)
        );
        assert_eq!(
            classify(
                ID,
                &also,
                &run(
                    None,
                    &[
                        (ID, VIOLATED),
                        ("other/conformant", VIOLATED),
                        ("other/draft", VIOLATED)
                    ]
                )
            ),
            Verdict::Unrelated("other/draft".to_owned())
        );
        assert_eq!(
            classify(
                ID,
                &also,
                &run(None, &[(ID, HELD), ("other/conformant", VIOLATED)])
            ),
            Verdict::NotCaught("violated other/conformant".to_owned())
        );
        assert_eq!(
            classify(
                ID,
                &also,
                &run(
                    Some("other/conformant"),
                    &[(ID, HELD), ("other/conformant", VIOLATED)]
                )
            ),
            Verdict::NotCaught("violated other/conformant".to_owned())
        );
        // Without the listing, the co-violation is unrelated.
        assert_eq!(
            classify(
                ID,
                &[],
                &run(None, &[(ID, VIOLATED), ("other/conformant", VIOLATED)])
            ),
            Verdict::Unrelated("other/conformant".to_owned())
        );
    }

    /// A run's witness test result, as `record` reads it from
    /// `witness.json`.
    fn judged(run: Run, why: &str) -> Run {
        match run {
            Run::Ended {
                violation,
                others_violated,
                outcome,
                harness,
                ..
            } => Run::Ended {
                violation,
                others_violated,
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
            classify(ID, &[], &judged(run(Some(ID), &[(ID, VIOLATED)]), lost)),
            Verdict::WitnessFails(lost.to_owned())
        );
        assert_eq!(
            baseline_failure(ID, &judged(run(None, &[(ID, HELD)]), lost)),
            Some(format!("its witness test fails: {lost}"))
        );
        // The end record's own failures still name themselves first.
        assert_eq!(
            classify(ID, &[], &judged(run(None, &[(ID, HELD)]), lost)),
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
            baseline_failure(
                ID,
                &run(
                    None,
                    &[(ID, r#"{"not-applicable":{"reason":"never-applied"}}"#)]
                )
            ),
            Some(format!("{ID} is not-applicable/never-applied"))
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

    /// A campaign run that violates another property or doesn't report the
    /// case's fails the case and names the input. One that holds or catches
    /// the patch doesn't, and the verdict counts the others' outcomes.
    #[test]
    fn campaign_failures_name_their_input() {
        let verdict = Verdict::CampaignFails {
            input: PathBuf::from("campaign/input-3.bin"),
            verdict: Box::new(classify(
                ID,
                &[],
                &run(Some("sim/no-task-panic"), &[(ID, HELD)]),
            )),
        };
        assert_eq!(
            verdict.to_string(),
            "campaign input campaign/input-3.bin: the patched run violated sim/no-task-panic instead"
        );
        let verdict = Verdict::CampaignFails {
            input: PathBuf::from("campaign/input-4.bin"),
            verdict: Box::new(classify(ID, &[], &run(None, &[]))),
        };
        assert_eq!(
            verdict.to_string(),
            "campaign input campaign/input-4.bin: the patched run didn't report the property"
        );
        assert_eq!(
            Verdict::Caught(Some(Campaign {
                inputs: 32,
                caught: 5,
                missed: BTreeMap::from([
                    ("held".to_owned(), 25),
                    ("not-applicable/never-applied".to_owned(), 2)
                ]),
            }))
            .to_string(),
            "caught; the campaign caught it on 5 of 32 inputs; the others: 25 held, 2 \
             not-applicable/never-applied"
        );
    }

    /// The shards of one count split the cases between them, each case in
    /// exactly one, and keep path order.
    #[test]
    fn shards_split_the_cases() {
        let cases: Vec<usize> = (0..10).collect();
        let shards: Vec<Vec<usize>> = (0..3)
            .map(|index| Shard { index, count: 3 }.select(cases.clone()))
            .collect();
        assert_eq!(shards, [vec![0, 3, 6, 9], vec![1, 4, 7], vec![2, 5, 8]]);
        assert_eq!(Shard { index: 0, count: 1 }.select(cases.clone()), cases);
        assert!(Shard { index: 4, count: 7 }
            .select(vec![0, 1, 2])
            .is_empty());
    }

    /// Every simulator build warns rather than fails on warnings, so a
    /// patch that leaves code unused still builds where the environment
    /// sets `CARGO_BUILD_WARNINGS=deny`.
    #[test]
    fn simulator_builds_override_denied_warnings() {
        let build = cargo_sim(Path::new("worktree"), Path::new("target"));
        let warnings = build
            .get_envs()
            .find(|(name, _)| *name == "CARGO_BUILD_WARNINGS")
            .and_then(|(_, value)| value);
        assert_eq!(warnings, Some(std::ffi::OsStr::new("warn")));
        assert_eq!(build.get_args().next(), Some(std::ffi::OsStr::new("build")));
    }

    #[test]
    fn a_shard_parses_only_an_index_below_its_count() {
        assert_eq!(Shard::parse("2/7"), Ok(Shard { index: 2, count: 7 }));
        assert_eq!(Shard::parse("0/1"), Ok(Shard { index: 0, count: 1 }));
        for text in ["7/7", "0/0", "1", "a/2", "1/b", "-1/2", "1/2/3", ""] {
            assert!(Shard::parse(text).is_err(), "{text}");
        }
    }

    /// A case is affected by a change to its patch's files, its checks, its
    /// own files, or the runner, and not by an unrelated change.
    #[test]
    fn planted_runs_only_affected_cases() {
        let watched = [
            PathBuf::from("crates/x/src/lib.rs"),
            PathBuf::from("crates/zakura-sim/src/domain"),
            PathBuf::from(SHARED[0]),
        ];
        let affected = |path: &str| is_affected(&watched, &[PathBuf::from(path)]);
        assert!(affected("crates/x/src/lib.rs"));
        assert!(affected("crates/zakura-sim/src/domain/discovery.rs"));
        assert!(affected("crates/xtask/src/planted.rs"));
        assert!(!affected("crates/x/src/other.rs"));
        assert!(!affected("crates/zakura-sim/src/domainx.rs"));
        assert!(!is_affected(&watched, &[]));
    }

    /// A change to the shared machinery affects the committed case, though
    /// it touches none of the case's own files.
    #[test]
    fn shared_changes_affect_the_committed_case() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let cases = cases(&repo_root).expect("the planted cases parse");
        let case = cases
            .iter()
            .find(|case| case.property.starts_with("discovery/"))
            .expect("a committed discovery case");
        let watched = watched(&repo_root, case).expect("the case's files exist");
        for path in [
            "crates/xtask/src/sim.rs",
            "crates/xtask/src/main.rs",
            "crates/zakura-sim/src/property/registry.rs",
            "crates/zakura-sim/src/sim.rs",
            "crates/zakura-scheduler/src/lib.rs",
            "crates/zakura-rt/src/sim/sync/notify.rs",
            "Cargo.lock",
            ".cargo/config.toml",
        ] {
            assert!(is_affected(&watched, &[PathBuf::from(path)]), "{path}");
        }
        // The harness also drives the transport under discovery, which the
        // case's checks cover.
        for path in [
            "crates/zakura-network/src/zakura/transport/io.rs",
            "crates/zakura-network/src/zakura/wire_codec/message.rs",
        ] {
            assert!(is_affected(&watched, &[PathBuf::from(path)]), "{path}");
        }
    }

    /// With `--targets`, a case runs through a fuzz target that runs its
    /// input's harness, such as the discovery wait case through
    /// `discovery_wait_complete`. Checking each case keeps the test valid as
    /// cases and targets are added.
    #[test]
    fn committed_cases_run_through_the_target_of_their_harness() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let harnesses = sim_minimize::fuzz_targets(&repo_root).expect("cargo metadata reads it");
        let mut on_targets = BTreeMap::new();
        for case in cases(&repo_root).expect("the planted cases parse") {
            let Witness::Input(input) = &case.witness else {
                continue;
            };
            let harness = harness_of(&repo_root, input).expect("the input has a sidecar");
            if let Some(target) = target_of(&harnesses, &harness) {
                assert_eq!(
                    harnesses.get(&target),
                    Some(&Some(harness)),
                    "{}: its target runs its harness",
                    case.property
                );
                on_targets.insert(case.property, target);
            }
        }
        assert_eq!(
            on_targets
                .get("discovery/wait-complete-wakes")
                .map(String::as_str),
            Some("discovery_wait_complete")
        );
    }

    /// A harness maps to the first target by name that runs it, and a
    /// target that runs no harness maps nothing.
    #[test]
    fn a_harness_maps_to_the_first_target_that_runs_it() {
        let harnesses = BTreeMap::from([
            ("a_probe".to_owned(), None),
            ("b_target".to_owned(), Some("h/one".to_owned())),
            ("c_target".to_owned(), Some("h/one".to_owned())),
            ("d_target".to_owned(), Some("h/two".to_owned())),
        ]);
        assert_eq!(target_of(&harnesses, "h/one").as_deref(), Some("b_target"));
        assert_eq!(target_of(&harnesses, "h/two").as_deref(), Some("d_target"));
        assert_eq!(target_of(&harnesses, "h/three"), None);
    }

    /// The builds a confirmation through a fuzz target makes outlive the
    /// scratch worktree, which every run removes, so the next case, the
    /// next run and CI's cache of `target/` reuse them.
    #[test]
    fn confirm_builds_outlive_the_worktree() {
        let repo = tempfile::tempdir().expect("a temporary repository");
        let root = repo.path();
        fs::write(root.join("file"), "file").expect("the file is written");
        git_ok(root, &["init", "-q"]);
        git_ok(root, &["add", "."]);
        git_ok(root, &["commit", "-q", "-m", "base"]);
        let work = root.join("target").join("planted");
        let worktree = work.join("worktree");
        git_ok(
            root,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                "target/planted/worktree",
            ],
        );
        let built = confirm_builds(&work)
            .join("sim")
            .join("debug")
            .join("zakura-sim");
        fs::create_dir_all(built.parent().expect("a parent")).expect("the directory is created");
        fs::write(&built, "binary").expect("the build is written");

        remove_worktree(root, &worktree).expect("the worktree is removed");
        assert!(!worktree.exists());
        assert_eq!(fs::read(&built).expect("the build is kept"), b"binary");
    }

    /// C5: a binary counts only for the checkout it embeds the digest of.
    /// A patch that changes the checkout makes the earlier build stale, and
    /// a checkout whose digest git can't compute matches no binary, not even
    /// one that embeds the `unknown` marker.
    #[test]
    fn a_binary_must_be_its_checkouts_own() {
        let repo = tempfile::tempdir().expect("a temporary repository");
        let root = repo.path();
        fs::create_dir_all(root.join("crates")).unwrap();
        fs::write(root.join("crates/lib.rs"), "fn a() {}\n").unwrap();
        git_ok(root, &["init", "-q"]);
        git_ok(root, &["add", "."]);
        git_ok(root, &["commit", "-qm", "source"]);
        let source = provenance::Provenance::read(root, provenance::SOURCE_PATHS).source;
        let bin_dir = tempfile::tempdir().expect("a binary directory");
        let binary = bin_dir.path().join("zakura-sim");
        fs::write(
            &binary,
            [b"head".as_slice(), source.as_bytes(), b"tail"].concat(),
        )
        .unwrap();
        assert_eq!(built_from(&binary, root).unwrap(), Ok(()));
        // A patch changes the checkout's digest, so the unpatched build is
        // stale for it.
        fs::write(root.join("crates/lib.rs"), "fn b() {}\n").unwrap();
        assert!(built_from(&binary, root).unwrap().is_err());
        // Outside a checkout the digest is unknown, which matches nothing,
        // not even a binary that embeds the marker.
        let elsewhere = tempfile::tempdir().expect("a directory outside git");
        fs::write(&binary, provenance::UNKNOWN).unwrap();
        assert!(built_from(&binary, elsewhere.path()).unwrap().is_err());
    }

    /// A confirmation's binaries must embed the patched checkout's source
    /// digest, so a reused build of other source doesn't pass.
    #[test]
    fn a_build_names_its_source() {
        let source = "ab".repeat(32);
        let binary = [b"head".as_slice(), source.as_bytes(), b"tail"].concat();
        assert_eq!(names_source(&binary, &source), Ok(()));
        let other = "cd".repeat(32);
        assert!(names_source(&binary, &other).unwrap_err().contains(&other));
        assert!(names_source(provenance::UNKNOWN.as_bytes(), provenance::UNKNOWN).is_err());
    }

    /// Runs `git` in `dir` with a fixed identity, and expects it to pass.
    fn git_ok(dir: &Path, args: &[&str]) {
        let status = git(dir)
            .args(["-c", "user.name=planted", "-c", "user.email=planted@test"])
            .args(args)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {args:?}");
    }

    /// A case's patch may change only code: one that changes, deletes or
    /// renames a witness file, typed or fuzz input, stops the case before
    /// anything runs on the patched checkout.
    #[test]
    fn a_patch_may_not_change_the_witness() {
        let repo = tempfile::tempdir().expect("a temporary repository");
        let patches = tempfile::tempdir().expect("a patch directory");
        let root = repo.path();
        for (path, text) in [
            ("src/lib.rs", "fn a() {}\n"),
            ("s.json", "{}\n"),
            ("p.bin", "plan\n"),
            ("p.json", "{}\n"),
            ("a.bin", "input\n"),
            ("a.toml", "harness = \"h\"\n"),
        ] {
            fs::create_dir_all(root.join(path).parent().expect("a parent"))
                .expect("the directory is created");
            fs::write(root.join(path), text).expect("the file is written");
        }
        git_ok(root, &["init", "-q"]);
        git_ok(root, &["add", "."]);
        git_ok(root, &["commit", "-qm", "witnesses"]);
        // Makes a patch from `edit`'s changes, then resets the checkout.
        let patch = |name: &str, edit: &dyn Fn()| {
            edit();
            git_ok(root, &["add", "-A"]);
            let diff = git(root)
                .args(["diff", "--cached", "-M", "--binary"])
                .output()
                .expect("git diff runs");
            assert!(diff.status.success());
            git_ok(root, &["reset", "-q", "--hard", "HEAD"]);
            let path = patches.path().join(name);
            fs::write(&path, diff.stdout).expect("the patch is written");
            path
        };
        let write = |path: &'static str| move || fs::write(root.join(path), "changed\n").unwrap();
        let code = patch("code.patch", &write("src/lib.rs"));
        let plan = patch("plan.patch", &|| {
            write("src/lib.rs")();
            write("p.bin")();
        });
        let script = patch("script.patch", &|| {
            write("src/lib.rs")();
            write("p.json")();
        });
        let moved = patch("moved.patch", &|| {
            fs::rename(root.join("s.json"), root.join("t.json")).unwrap()
        });
        let sidecar = patch("sidecar.patch", &write("a.toml"));
        let input = patch("input.patch", &|| {
            fs::remove_file(root.join("a.bin")).unwrap()
        });
        let typed = Case {
            property: ID.to_owned(),
            patch: code.clone(),
            witness: Witness::Typed {
                harness: "h".to_owned(),
                scenario: "s.json".into(),
                plan: PlanFile::Bytes("p.bin".into()),
            },
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: Vec::new(),
            missing_evidence: None,
            target: None,
        };
        let scripted = Case {
            witness: Witness::Typed {
                harness: "h".to_owned(),
                scenario: "s.json".into(),
                plan: PlanFile::Script("p.json".into()),
            },
            ..typed.clone()
        };
        let fuzz = Case {
            witness: Witness::Input("a.bin".into()),
            ..typed.clone()
        };
        let judge = |case: &Case, patch: &Path| {
            let case = Case {
                patch: patch.to_owned(),
                ..case.clone()
            };
            let verdict = apply_case_patch(root, &case).expect("git runs");
            git_ok(root, &["reset", "-q", "--hard", "HEAD"]);
            git_ok(root, &["clean", "-fdq"]);
            verdict
        };
        let touches = |files: &[&str]| {
            Some(Verdict::PatchesWitness(
                files.iter().map(PathBuf::from).collect(),
            ))
        };
        assert_eq!(judge(&typed, &code), None, "a code patch goes on");
        assert_eq!(judge(&fuzz, &code), None, "a code patch goes on");
        assert_eq!(judge(&typed, &plan), touches(&["p.bin"]));
        assert_eq!(judge(&scripted, &code), None, "a code patch goes on");
        assert_eq!(judge(&scripted, &script), touches(&["p.json"]));
        assert_eq!(
            judge(&scripted, &plan),
            None,
            "another case's Plan bytes aren't this witness"
        );
        assert_eq!(judge(&typed, &moved), touches(&["s.json"]));
        assert_eq!(judge(&scripted, &moved), touches(&["s.json"]));
        assert_eq!(judge(&fuzz, &sidecar), touches(&["a.toml"]));
        assert_eq!(judge(&fuzz, &input), touches(&["a.bin"]));
        assert_eq!(
            judge(&typed, &input),
            None,
            "another case's input isn't this witness"
        );
    }

    /// A renamed file or directory reports its old paths and its new ones,
    /// so a case watching the old path sees the move.
    #[test]
    fn renames_report_the_old_path() {
        let repo = tempfile::tempdir().expect("a temporary repository");
        let root = repo.path();
        for (path, text) in [("a/service.rs", "service"), ("b/one.rs", "one")] {
            fs::create_dir_all(root.join(path).parent().expect("a parent"))
                .expect("the directory is created");
            fs::write(root.join(path), text).expect("the file is written");
        }
        git_ok(root, &["init", "-q"]);
        git_ok(root, &["add", "."]);
        git_ok(root, &["commit", "-q", "-m", "base"]);
        git_ok(root, &["mv", "a/service.rs", "a/renamed_service.rs"]);
        git_ok(root, &["mv", "b", "c"]);
        git_ok(root, &["commit", "-q", "-m", "move"]);

        let mut changed = changed_since_rev(root, "HEAD^1").expect("the diff runs");
        changed.sort();
        assert_eq!(
            changed,
            [
                "a/renamed_service.rs",
                "a/service.rs",
                "b/one.rs",
                "c/one.rs"
            ]
            .map(PathBuf::from)
        );
        assert!(is_affected(&[PathBuf::from("a/service.rs")], &changed));
        assert!(is_affected(&[PathBuf::from("b")], &changed));
    }

    /// Writes a case for each `(property, name)` under `root`'s
    /// [`PLANTED_DIR`]: a patch that changes `src/<name>.rs`, and a case file
    /// naming `qa/inputs/<name>.bin`.
    fn write_cases(root: &Path, cases: &[(&str, &str)]) {
        for (property, name) in cases {
            let dir = root.join(PLANTED_DIR).join(property);
            fs::create_dir_all(&dir).expect("the property directory is created");
            fs::write(
                dir.join(format!("{name}.patch")),
                format!("--- a/src/{name}.rs\n+++ b/src/{name}.rs\n@@ -1 +1 @@\n-fn a() {{}}\n+fn b() {{}}\n"),
            )
            .expect("the patch is written");
            fs::write(
                dir.join(format!("{name}.toml")),
                format!("input = \"qa/inputs/{name}.bin\"\n"),
            )
            .expect("the case file is written");
        }
    }

    /// A shard that holds no case fails the run, so a passing run always
    /// ran a case.
    #[test]
    fn an_empty_shard_fails() {
        let repo = tempfile::tempdir().expect("a temporary repository");
        write_cases(repo.path(), &[("a", "one"), ("a", "two")]);
        let error = super::run(
            repo.path(),
            None,
            Some(Shard { index: 2, count: 7 }),
            None,
            false,
        )
        .expect_err("shard 2/7 of two cases is empty");
        assert!(
            error.to_string().contains("shard 2/7 holds none of the 2"),
            "{error}"
        );
    }

    /// A revision git can't resolve fails the run instead of selecting no
    /// case.
    #[test]
    fn an_unknown_revision_fails() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let error = changed_since_rev(&repo_root, "no-such-revision")
            .expect_err("the revision doesn't exist");
        assert!(error.to_string().contains("no-such-revision"), "{error}");
    }

    /// An empty revision, an option, a path and a range each fail instead
    /// of reaching `git diff` as something other than a commit.
    #[test]
    fn a_revision_that_isnt_a_commit_fails() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        for rev in ["", "--output=/dev/null", "crates", "HEAD^1..HEAD", "-z"] {
            let error =
                changed_since_rev(&repo_root, rev).expect_err("the revision doesn't name a commit");
            assert!(
                error.to_string().contains("doesn't name a commit"),
                "{rev}: {error}"
            );
        }
    }

    /// Case paths must be plain repository-relative names, so they compare
    /// with `git diff`'s paths.
    #[test]
    fn case_paths_must_be_plain() {
        for path in ["a/b", "a", "crates/zakura-network/src/zakura/discovery"] {
            assert!(is_plain_relative(Path::new(path)), "{path}");
        }
        for path in [
            "",
            "/a",
            "./a",
            "a/../a",
            "crates/zakura-network/src/zakura/discovery/../discovery",
        ] {
            assert!(!is_plain_relative(Path::new(path)), "{path}");
        }
    }

    /// Each committed case patches only production code, and watches its
    /// patch's files and its listed checks, which all exist.
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
            let patched = patched_files(&repo_root, &case.patch).expect("the patch parses");
            assert!(
                !patched.is_empty(),
                "{} changes no file",
                case.patch.display()
            );
            for path in &patched {
                assert!(
                    path.starts_with("crates")
                        && !path.starts_with("crates/zakura-sim")
                        && !path.starts_with("crates/xtask")
                        && !path.components().any(|part| part.as_os_str() == "tests"),
                    "{} patches {}, which isn't production code",
                    case.patch.display(),
                    path.display()
                );
            }
        }
    }

    /// The committed cases parse, and each names an input with a sidecar,
    /// or a typed case's files.
    #[test]
    fn committed_cases_name_their_inputs() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let cases = cases(&repo_root).expect("the planted cases parse");
        assert!(cases.iter().any(|case| case.property == ID));
        for case in &cases {
            case.witness
                .harness(&repo_root)
                .unwrap_or_else(|error| panic!("{}: {error}", case.patch.display()));
            for file in case.witness.files() {
                assert!(
                    repo_root.join(&file).is_file(),
                    "{}: {} is committed",
                    case.patch.display(),
                    file.display()
                );
            }
        }
    }

    /// Writes `qa/planted/<ID>/case.patch` and `case.toml` with `toml` under
    /// a fresh repository root, and reads its cases.
    fn parse_case(toml: &str) -> Result<Vec<Case>, BoxError> {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let property = dir.path().join(PLANTED_DIR).join(ID);
        fs::create_dir_all(&property).unwrap();
        fs::write(property.join("case.patch"), "").unwrap();
        fs::write(property.join("case.toml"), toml).unwrap();
        cases(dir.path())
    }

    /// A case file names a fuzz input or a typed case, never both and never
    /// neither, and a typed case's files are plain relative paths.
    #[test]
    fn a_case_names_an_input_or_a_typed_case() {
        let input = parse_case(r#"input = "fixtures/a.bin""#).expect("an input case parses");
        assert_eq!(input[0].witness, Witness::Input("fixtures/a.bin".into()));
        let typed = r#"
            [typed]
            harness = "header-sync/storage"
            scenario = "qa/planted/x/case.scenario.json"
            plan = "qa/planted/x/case.plan.bin"
        "#;
        let typed = parse_case(typed).expect("a typed case parses");
        assert_eq!(
            typed[0].witness,
            Witness::Typed {
                harness: "header-sync/storage".to_owned(),
                scenario: "qa/planted/x/case.scenario.json".into(),
                plan: PlanFile::Bytes("qa/planted/x/case.plan.bin".into()),
            }
        );
        let scripted = r#"
            [typed]
            harness = "header-sync/wire"
            scenario = "qa/planted/x/case.scenario.json"
            script = "qa/planted/x/case.script.json"
        "#;
        let scripted = parse_case(scripted).expect("a scripted case parses");
        assert_eq!(
            scripted[0].witness,
            Witness::Typed {
                harness: "header-sync/wire".to_owned(),
                scenario: "qa/planted/x/case.scenario.json".into(),
                plan: PlanFile::Script("qa/planted/x/case.script.json".into()),
            }
        );
        let both = r#"
            input = "fixtures/a.bin"
            [typed]
            harness = "header-sync/storage"
            scenario = "s.json"
            plan = "p.bin"
        "#;
        let error = parse_case(both).expect_err("both is refused").to_string();
        assert!(error.contains("names both"), "{error}");
        let error = parse_case("checks = []")
            .expect_err("neither is refused")
            .to_string();
        assert!(error.contains("names neither"), "{error}");
        let outside = r#"
            [typed]
            harness = "header-sync/storage"
            scenario = "../s.json"
            plan = "p.bin"
        "#;
        let error = parse_case(outside)
            .expect_err("a path outside is refused")
            .to_string();
        assert!(error.contains("isn't a plain path"), "{error}");
        let missing = r#"
            [typed]
            harness = "header-sync/storage"
            scenario = "s.json"
        "#;
        let error = parse_case(missing)
            .expect_err("a typed case needs its Plan")
            .to_string();
        assert!(error.contains("`plan` or `script`"), "{error}");
        assert!(error.contains("names neither"), "{error}");
        let both = r#"
            [typed]
            harness = "header-sync/wire"
            scenario = "s.json"
            plan = "p.bin"
            script = "p.json"
        "#;
        let error = parse_case(both)
            .expect_err("Plan bytes and a script are refused")
            .to_string();
        assert!(error.contains("`plan` or `script`"), "{error}");
        assert!(error.contains("names both"), "{error}");
        let outside = r#"
            [typed]
            harness = "header-sync/wire"
            scenario = "s.json"
            script = "/p.json"
        "#;
        let error = parse_case(outside)
            .expect_err("a script outside is refused")
            .to_string();
        assert!(error.contains("isn't a plain path"), "{error}");
    }

    /// A catch, a fuzz input's or a typed case's, passes only if its
    /// fresh replay matches (C4). A miss is its own verdict, and an
    /// uncaught case never replays.
    #[test]
    fn every_catch_needs_its_fresh_replay() {
        let caught = || Verdict::Caught(None);
        assert_eq!(fresh_replay(caught(), || Ok(None)).unwrap(), caught());
        assert_eq!(
            fresh_replay(caught(), || Ok(Some("differs at step 3".to_owned()))).unwrap(),
            Verdict::ReplayMisses("differs at step 3".to_owned())
        );
        let unreachable = || -> Result<Option<String>, BoxError> { panic!("only a catch replays") };
        assert_eq!(
            fresh_replay(Verdict::Unreported, unreachable).unwrap(),
            Verdict::Unreported
        );
    }

    /// The runner's patched judgment replays a caught case's own patched
    /// trace, a typed case's and a fuzz input's alike: with a `zakura-sim`
    /// whose runs violate the case's property, a replay that matches keeps
    /// the catch, and one that doesn't makes it [`Verdict::ReplayMisses`].
    #[cfg(unix)]
    #[test]
    fn the_runner_replays_every_caught_case() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let end = end(Some(ID), &[(ID, VIOLATED)]);
        // `record` writes a violating trace and a passed witness under
        // `--out`. `replay` exits with `$code` for the case's own patched
        // trace, and 9 for any other path.
        let fake = |name: &str, code: u8| {
            let path = dir.path().join(name);
            let script = format!(
                "#!/bin/sh\n\
                 [ $# -gt 0 ] || exit 0\n\
                 if [ \"$1\" = replay ]; then\n\
                 case \"$2\" in */case/patched) exit {code} ;; *) exit 9 ;; esac\n\
                 fi\n\
                 out=; prev=\n\
                 for arg in \"$@\"; do [ \"$prev\" = --out ] && out=$arg; prev=$arg; done\n\
                 mkdir -p \"$out\"\n\
                 printf '%s\\n' '{end}' > \"$out/trace.jsonl\"\n\
                 printf '%s\\n' '{{\"passed\":true,\"error\":null}}' > \"$out/witness.json\"\n"
            );
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            wait_until_executable(&path);
            path
        };
        let matches = fake("matches", 0);
        let misses = fake("misses", 1);
        let scratch = dir.path().join("case");
        fs::create_dir_all(&scratch).unwrap();
        fs::write(dir.path().join("s.json"), "{}").unwrap();
        let typed = Case {
            property: ID.to_owned(),
            patch: dir.path().join("case.patch"),
            witness: Witness::Typed {
                harness: "header-sync/storage".to_owned(),
                scenario: "s.json".into(),
                plan: PlanFile::Bytes("p.bin".into()),
            },
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: Vec::new(),
            missing_evidence: None,
            target: None,
        };
        let judge = |binary: &Path, case: &Case| {
            judge_patched(
                binary,
                "header-sync/storage",
                &case.witness.located(dir.path()),
                case,
                &scratch,
                RUN_DEADLINE,
            )
            .expect("the fake binary runs")
        };
        assert!(
            matches!(judge(&matches, &typed), Verdict::Caught(Some(_))),
            "a matching replay keeps the catch"
        );
        assert!(
            matches!(judge(&misses, &typed), Verdict::ReplayMisses(_)),
            "a failed replay of the patched trace isn't a catch"
        );
        let input = Case {
            witness: Witness::Input("a.bin".into()),
            ..typed.clone()
        };
        assert!(
            matches!(judge(&misses, &input), Verdict::ReplayMisses(_)),
            "a fuzz-input case replays too"
        );
        assert!(
            matches!(judge(&matches, &input), Verdict::Caught(Some(_))),
            "a fuzz-input case's matching replay keeps the catch"
        );
    }

    /// C4: a recording that writes a whole trace and a passing witness and
    /// then exits nonzero, as a panic after its witness would, didn't
    /// finish. Its patched run is never a catch and its baseline never
    /// passes, with a deadline or without. The same recording exiting 0 is
    /// a catch and a passing baseline, and a failing witness that exits 1,
    /// as `zakura-sim record`'s does, keeps its reason.
    #[cfg(unix)]
    #[test]
    fn a_recording_that_exits_nonzero_never_counts() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        // `record` writes `end` and `witness` under `--out` and exits
        // `code`. `replay` matches.
        let fake = |name: &str, end: &str, witness: &str, code: u8| {
            let path = dir.path().join(name);
            let script = format!(
                "#!/bin/sh\n\
                 [ $# -gt 0 ] || exit 0\n\
                 [ \"$1\" = replay ] && exit 0\n\
                 out=; prev=\n\
                 for arg in \"$@\"; do [ \"$prev\" = --out ] && out=$arg; prev=$arg; done\n\
                 mkdir -p \"$out\"\n\
                 printf '%s\\n' '{end}' > \"$out/trace.jsonl\"\n\
                 printf '%s\\n' '{witness}' > \"$out/witness.json\"\n\
                 exit {code}\n"
            );
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            wait_until_executable(&path);
            path
        };
        let passed = r#"{"passed":true,"error":null}"#;
        let violated = end(Some(ID), &[(ID, VIOLATED)]);
        let held = end(None, &[(ID, HELD)]);
        let case = Case {
            property: ID.to_owned(),
            patch: dir.path().join("case.patch"),
            witness: Witness::Input("a.bin".into()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: Vec::new(),
            missing_evidence: None,
            target: None,
        };
        let scratch = dir.path().join("case");
        fs::create_dir_all(&scratch).unwrap();
        let input = case.witness.located(dir.path());
        let patched = |binary: &Path| {
            judge_patched(
                binary,
                "header-sync/storage",
                &input,
                &case,
                &scratch,
                RUN_DEADLINE,
            )
            .expect("the fake binary runs")
        };
        let baseline = |binary: &Path, deadline| {
            record(
                Command::new(binary),
                "header-sync/storage",
                &input,
                ID,
                Expect::Holds,
                &scratch.join("baseline"),
                deadline,
            )
            .expect("the fake binary runs")
        };
        let deadline = Some((RUN_DEADLINE, &NEVER_INTERRUPTED));

        let panics = fake("caught-then-panics", &violated, passed, 101);
        assert!(
            matches!(patched(&panics), Verdict::Failed(_)),
            "{:?}",
            patched(&panics)
        );
        let panics = fake("holds-then-panics", &held, passed, 101);
        for deadline in [deadline, None] {
            let run = baseline(&panics, deadline);
            assert!(
                matches!(&run, Run::Failed(why) if why.contains("passing witness")),
                "{run:?}"
            );
            assert!(baseline_failure(ID, &run).is_some());
        }

        let caught = fake("caught", &violated, passed, 0);
        assert!(matches!(patched(&caught), Verdict::Caught(_)));
        let holds = fake("holds", &held, passed, 0);
        assert_eq!(baseline_failure(ID, &baseline(&holds, deadline)), None);

        let failed = fake("fails", &held, r#"{"passed":false,"error":"boom"}"#, 1);
        let run = baseline(&failed, deadline);
        assert!(
            matches!(&run, Run::Ended { witness: Some(why), .. } if why == "boom"),
            "{run:?}"
        );
    }

    /// [`replay_misses`] reads the replay's exit status: a `zakura-sim`
    /// that exits 0 matched, and one that exits 1 says why it didn't.
    #[cfg(unix)]
    #[test]
    fn a_replay_miss_reads_the_exit_status() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let fake = |name: &str, body: &str| {
            let path = dir.path().join(name);
            fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            wait_until_executable(&path);
            path
        };
        let matched = fake("matched", "test \"$1\" = replay || exit 2");
        let missed = fake("missed", "echo 'pick 3 differs' >&2; exit 1");
        assert_eq!(
            replay_misses(&matched, dir.path(), RUN_DEADLINE, "replay-patched").unwrap(),
            None
        );
        let why = replay_misses(&missed, dir.path(), RUN_DEADLINE, "replay-patched")
            .unwrap()
            .expect("a failed replay says why");
        assert!(why.contains("pick 3 differs"), "{why}");
        // A replay that hangs is killed at its deadline, and misses.
        // `wait_until_executable` runs it with no arguments.
        let hangs = fake("hangs", "[ $# -gt 0 ] || exit 0\nsleep 60");
        let why = replay_misses(
            &hangs,
            dir.path(),
            Duration::from_millis(200),
            "replay-patched",
        )
        .unwrap()
        .expect("a hung replay misses");
        assert!(why.contains("ran past its deadline"), "{why}");
    }

    /// C4: every run of a case has a deadline. A patched recording that
    /// hangs is killed at it, ends `Killed`, and never counts as a catch.
    #[cfg(unix)]
    #[test]
    fn a_hung_patched_run_is_killed_and_never_caught() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let hangs = dir.path().join("hangs");
        // `wait_until_executable` runs it with no arguments.
        fs::write(&hangs, "#!/bin/sh\n[ $# -gt 0 ] || exit 0\nsleep 60\n").unwrap();
        fs::set_permissions(&hangs, fs::Permissions::from_mode(0o755)).unwrap();
        wait_until_executable(&hangs);
        let case = Case {
            property: ID.to_owned(),
            patch: dir.path().join("case.patch"),
            witness: Witness::Input("a.bin".into()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: Vec::new(),
            missing_evidence: None,
            target: None,
        };
        let scratch = dir.path().join("case");
        fs::create_dir_all(&scratch).unwrap();
        let started = std::time::Instant::now();
        let run = record(
            Command::new(&hangs),
            "header-sync/storage",
            &case.witness.located(dir.path()),
            ID,
            case.expect(),
            &scratch.join("patched"),
            Some((Duration::from_millis(200), &NEVER_INTERRUPTED)),
        )
        .expect("the fake binary starts");
        assert!(matches!(run, Run::Killed(_)), "{run:?}");
        assert!(started.elapsed() < Duration::from_secs(30));
        assert!(
            matches!(case.classify(&run), Verdict::Failed(_)),
            "a killed run isn't a catch"
        );
        let verdict = judge_patched(
            &hangs,
            "header-sync/storage",
            &case.witness.located(dir.path()),
            &case,
            &scratch,
            Duration::from_millis(200),
        )
        .expect("the fake binary starts");
        assert!(matches!(verdict, Verdict::Failed(_)), "{verdict:?}");
    }

    /// C4: a baseline and a campaign run have deadlines too. A baseline
    /// recording that hangs is killed at its deadline, ends `Killed`, and
    /// fails the case's baseline. A campaign whose first run hangs ends
    /// there as a failed campaign, never a catch.
    #[cfg(unix)]
    #[test]
    fn a_hung_baseline_or_campaign_run_is_killed_and_never_passes() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let hangs = dir.path().join("hangs");
        // `wait_until_executable` runs it with no arguments.
        fs::write(&hangs, "#!/bin/sh\n[ $# -gt 0 ] || exit 0\nsleep 60\n").unwrap();
        fs::set_permissions(&hangs, fs::Permissions::from_mode(0o755)).unwrap();
        wait_until_executable(&hangs);
        let case = Case {
            property: ID.to_owned(),
            patch: dir.path().join("case.patch"),
            witness: Witness::Input("a.bin".into()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: Vec::new(),
            missing_evidence: None,
            target: None,
        };
        let scratch = dir.path().join("case");
        fs::create_dir_all(&scratch).unwrap();
        let deadline = Duration::from_millis(200);
        let started = std::time::Instant::now();
        let baseline = record(
            Command::new(&hangs),
            "header-sync/storage",
            &case.witness.located(dir.path()),
            ID,
            Expect::Holds,
            &scratch.join("baseline"),
            Some((deadline, &NEVER_INTERRUPTED)),
        )
        .expect("the fake binary starts");
        assert!(matches!(baseline, Run::Killed(_)), "{baseline:?}");
        assert!(
            baseline_failure(ID, &baseline).is_some(),
            "a killed baseline fails the case"
        );
        let verdict = campaign(
            &hangs,
            "header-sync/storage",
            &case,
            &scratch.join("campaign"),
            deadline,
        )
        .expect("the fake binary starts");
        assert!(
            matches!(&verdict, Verdict::CampaignFails { verdict, .. }
                if matches!(**verdict, Verdict::Failed(_))),
            "{verdict:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(30));
    }

    /// C4: a nested case's exact set. Its committed input is a catch only
    /// with its property and every member of its set violated. A missing
    /// member isn't a catch, and a property outside the set is unrelated.
    /// Its runs ask the witness judge for the same set.
    /// A harness-caught case counts only its own property's missing
    /// evidence with its named detail, violating nothing, and any other
    /// harness failure stays one (Adam's ruling on C19). Only a typed case
    /// with no set or co-violations may name the detail.
    #[test]
    fn a_harness_caught_case_needs_its_named_missing_evidence() {
        const DETAIL: &str = "with a boundary unread";
        let ended = |harness: Option<&str>, others: &[&str]| Run::Ended {
            violation: None,
            others_violated: others.iter().map(|id| (*id).to_owned()).collect(),
            outcome: None,
            harness: harness.map(str::to_owned),
            witness: Some("the run ended at a harness failure".to_owned()),
        };
        let missing =
            format!("{ID} lacks evidence after step 940: the run ran dry after step 940 {DETAIL}");
        assert_eq!(
            classify_missing_evidence(ID, DETAIL, &ended(Some(&missing), &[])),
            Verdict::Caught(None)
        );
        for failure in [
            format!("other/id lacks evidence after step 940: {DETAIL}"),
            format!("{ID} lacks evidence after step 940: a gap"),
            format!("{ID} lacks evidence after step : {DETAIL}"),
            "source 3's evidence failed: PeerActionFailed".to_owned(),
        ] {
            assert_eq!(
                classify_missing_evidence(ID, DETAIL, &ended(Some(&failure), &[])),
                Verdict::Harness(failure.clone())
            );
        }
        assert_eq!(
            classify_missing_evidence(ID, DETAIL, &ended(Some(&missing), &["sim/no-task-panic"])),
            Verdict::Unrelated("sim/no-task-panic".to_owned())
        );
        assert_eq!(
            classify_missing_evidence(ID, DETAIL, &ended(None, &[])),
            Verdict::Unreported
        );
        let typed = r#"
            missing_evidence = "with a boundary unread"
            [typed]
            harness = "header-sync/storage"
            scenario = "qa/planted/x/case.scenario.json"
            plan = "qa/planted/x/case.plan.bin"
        "#;
        let cases = parse_case(typed).expect("a typed harness-caught case parses");
        assert_eq!(cases[0].missing_evidence.as_deref(), Some(DETAIL));
        assert_eq!(
            cases[0].classify(&ended(Some(&missing), &[])),
            Verdict::Caught(None)
        );
        for refused in [
            "input = \"a.bin\"\nmissing_evidence = \"x\"".to_owned(),
            typed.replace("[typed]", "set = [\"x/broader\"]\n[typed]"),
            typed.replace("[typed]", "also_violates = [\"x/other\"]\n[typed]"),
        ] {
            let error = parse_case(&refused)
                .expect_err("only a typed case alone is harness-caught")
                .to_string();
            assert!(error.contains("harness-caught"), "{error}");
        }
    }

    #[test]
    fn a_nested_case_needs_exactly_its_set() {
        const BROADER: &str = "header-sync/s5v9/conformant-never-closes";
        let set = [BROADER.to_owned()];
        let judged = |others: &[&str]| {
            let mut reports = vec![(ID, VIOLATED)];
            reports.extend(others.iter().map(|id| (*id, VIOLATED)));
            classify_set(ID, &[], &set, &run(Some(ID), &reports))
        };
        assert_eq!(judged(&[BROADER]), Verdict::Caught(None));
        assert!(
            matches!(judged(&[]), Verdict::NotCaught(why) if why.contains(BROADER)),
            "a missing member isn't a catch"
        );
        assert_eq!(
            judged(&[BROADER, "test/other"]),
            Verdict::Unrelated("test/other".to_owned())
        );
        assert_eq!(
            Expect::ViolatedSet(&set).args(),
            ["--expect", "violated-set", "--also-violates", BROADER]
        );
        // A confirmation through the case's fuzz target keeps the set.
        let nested = Case {
            property: ID.to_owned(),
            patch: PathBuf::from("case.patch"),
            witness: Witness::Input("a.bin".into()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            set: set.to_vec(),
            missing_evidence: None,
            target: Some("header_sync".to_owned()),
        };
        assert_eq!(nested.beside(), set);
        let both = r#"
            input = "a.bin"
            set = ["x/broader"]
            also_violates = ["x/other"]
        "#;
        let error = parse_case(both)
            .expect_err("a case names a set or co-violations")
            .to_string();
        assert!(error.contains("not both"), "{error}");
    }

    /// A typed case watches its scenario and Plan file, and runs with
    /// `--scenario` holding the scenario's text and `--plan` or `--script`
    /// naming its Plan file.
    #[test]
    fn a_typed_case_watches_and_runs_its_files() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let witness = Witness::Typed {
            harness: "header-sync/storage".to_owned(),
            scenario: "s.json".into(),
            plan: PlanFile::Bytes("p.bin".into()),
        };
        assert_eq!(
            witness.files(),
            vec![PathBuf::from("s.json"), PathBuf::from("p.bin")]
        );
        assert_eq!(
            witness.harness(dir.path()).expect("a typed case names it"),
            "header-sync/storage"
        );
        fs::write(dir.path().join("s.json"), r#"{"mirrors":2}"#).unwrap();
        let args = input_args(&witness.located(dir.path())).expect("the scenario reads");
        assert_eq!(
            args,
            vec![
                OsString::from("--scenario"),
                OsString::from(r#"{"mirrors":2}"#),
                OsString::from("--plan"),
                dir.path().join("p.bin").into_os_string(),
            ]
        );
        let scripted = Witness::Typed {
            harness: "header-sync/wire".to_owned(),
            scenario: "s.json".into(),
            plan: PlanFile::Script("p.json".into()),
        };
        assert_eq!(
            scripted.files(),
            vec![PathBuf::from("s.json"), PathBuf::from("p.json")]
        );
        assert_eq!(
            input_args(&scripted.located(dir.path())).expect("the scenario reads"),
            vec![
                OsString::from("--scenario"),
                OsString::from(r#"{"mirrors":2}"#),
                OsString::from("--script"),
                dir.path().join("p.json").into_os_string(),
            ]
        );
        let input = Witness::Input("a.bin".into());
        assert_eq!(
            input_args(&input.located(dir.path())).expect("an input needs no read"),
            vec![
                OsString::from("--input"),
                dir.path().join("a.bin").into_os_string()
            ]
        );
    }
}
