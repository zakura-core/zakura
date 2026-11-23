//! `cargo xtask planted [<property-id>]`: checks that each retained bypass
//! patch still makes its property fail (properties.md, Planted bugs and
//! evidence).
//!
//! A planted case is `qa/planted/<property-id>/<name>.patch` in the QA
//! store, whose local copy `ZAKURA_QA_STORE` names (`planted_layout`). It is
//! a production change that bypasses the property's guard, with
//! `<name>.toml` beside it naming what must find the bug, in one of three
//! ways:
//!
//! - a seed, by its name in the store's seeds, which names the seed's
//!   harness and holds its input;
//! - a typed case, a `[typed]` table naming the harness, a stored scenario
//!   as that harness's JSON and, optionally, the Plan it runs under: either
//!   Plan bytes (`plan`) or a scripted Plan as JSON (`script`), never both.
//!   A typed case with neither runs the empty Plan. A typed case is for a
//!   scenario the harness's grammar can't encode, such as one with a control
//!   step or more mirrors than the grammar takes, and a script is for a
//!   history no Plan bytes reach, such as one a test steers to;
//! - a search case, a `[search]` table naming the harness and a
//!   `zakura-sim generate` search on it: its `seed`, its `runs` and,
//!   optionally, its `draws` (`uniform` or `cycle`) and its `schedule`
//!   (`plan` or `pct`). The case commits no
//!   input. The runner runs the search on the patched build, and the input
//!   whose run stopped it is the case's witness. A search that runs out of
//!   inputs, or stops on anything but the case's property, misses the
//!   patch ([`Verdict::SearchMisses`]).
//!
//! A property that registers no trigger-history check, such as the step
//! loop's `sim/no-task-panic` and `sim/no-livelock`, has no trigger history
//! for a witness test to judge. `zakura-sim properties --untriggered` lists
//! them. For such a property the runner records the unpatched run as a
//! witness test that expects `applicable`, and the patched runs with no
//! witness test: each is judged from its trace's end alone. So a patch that
//! breaks the property for another reason than the case's bug still counts
//! as a catch.
//!
//! The runner checks out `HEAD` once, in a scratch worktree under
//! `target/planted/`, and builds the unpatched simulator there once. Every
//! recording build uses the workspace's `planted` profile in
//! `target/planted/target`, apart from the checkout's own target directory.
//! The unpatched build lists the store's seeds ([`seeds`]), and a case
//! naming a seed that the store lacks or the build rejects fails the run. Then for
//! each case it:
//!
//! 1. records the witness with the unpatched build, which must hold the
//!    property with no violation of any property and pass its witness test
//!    expecting `holds`. A seed's witness is the input the unpatched build
//!    writes from the store. A search case first applies the patch, builds
//!    it and runs its search (step 2's build), and its witness is the input
//!    the search found;
//! 2. applies the patch, which must not change, delete or rename the
//!    case's witness files, so both runs read the same witness. The witness
//!    files live in the store, outside the checkout the patch changes. It then rebuilds and
//!    records the witness again, which must violate the case's property and
//!    no property outside the case's `also_violates`, and pass its witness
//!    test expecting `violated` beside those;
//! 3. runs a short campaign on the patched build: [`CAMPAIGN_INPUTS`] fuzz
//!    inputs, drawn from a seed that the property's ID fixes, each of which
//!    must hold or violate only the case's property, and a violation must
//!    pass the witness test. A case file's `also_violates` names properties
//!    the patch's bug breaks too. Any run may violate them beside the case's
//!    property, and a campaign run may violate one of them instead;
//! 4. replays the patched trace in a fresh `zakura-sim replay`, which must
//!    match Exact;
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
//! witness doesn't catch, and a catch that misses the trigger history are
//! each reported as their own failure, never as a catch. The campaign reports how many of its inputs
//! caught the patch, and how the others' runs ended. It fails the case only
//! on a run that fails, ends at a harness failure, violates another
//! property not in `also_violates`, doesn't report the case's property, or
//! violates it without its trigger history, so the gate stays the case's
//! witness, which must violate the case's property.
//!
//! `--targets` runs only the seed cases whose seed runs on a harness that a
//! simulator fuzz target runs, as the fuzz package's
//! `[package.metadata.zakura-sim.targets]` table maps them, and prints
//! each target's cases, or that it has none. It builds and lists the
//! registry before it picks the cases. After step 3 it also confirms each
//! seed's input, which the unpatched build writes, through its target on
//! the patched checkout in fresh processes, as
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
//! affect: those whose patched files or listed checks changed, or every
//! case if the shared machinery changed: this runner, the simulator that
//! records and judges each run, its scheduler and runtime, or the build
//! configuration. A renamed file counts as a change to its old and its new
//! path. Git can't see a change to the store, so `--store-changed <file>`
//! names the store's changed paths, one per line, relative to its root: a
//! case whose patch, case file or witness files it lists runs too.
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

use crate::{
    run_command,
    sim::SIM_RUSTFLAGS,
    sim_minimize::{self, Sanitizer, Status},
    BoxError,
};

mod lane;
mod mutation_loop;
mod outcome;
mod pilot;
mod seeds;

#[allow(dead_code)]
#[path = "../../zakura-sim/build/provenance.rs"]
pub(super) mod provenance;

/// The checkout state a fuzz build's identity records, as `zakura-sim
/// campaign build` reads it.
#[path = "../../zakura-sim/src/campaign/checkout.rs"]
mod checkout;

/// Where the cases live and the property each bypasses, shared with the
/// simulator's checks of the catalogue.
#[path = "../../zakura-sim/src/planted.rs"]
mod planted_layout;

pub(super) use mutation_loop::{run as mutation_loop, triage};
use outcome::EndOutcome;
pub(super) use pilot::run as pilot;
pub(super) use planted_layout::require_store;
use planted_layout::PLANTED_DIR;
use seeds::Registry;

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

/// Fuzz inputs the short campaign records after the case's witness.
const CAMPAIGN_INPUTS: usize = 32;

/// The campaign's longest input, in bytes.
const CAMPAIGN_INPUT_BYTES: usize = 64;

/// The longest one child of a confirmation through a fuzz target may run.
const TARGET_CHILD: Duration = Duration::from_secs(300);

/// The longest a search case's `zakura-sim generate` may take.
const SEARCH_DEADLINE: Duration = Duration::from_secs(3_600);

/// The file `zakura-sim generate` writes its report to, in its output
/// directory: `zakura_sim::explore::generate::REPORT_FILE`.
const GENERATE_REPORT: &str = "generate.json";

/// The longest one `zakura-sim` run of a case may take: a baseline, patched
/// or campaign recording, or a fresh replay. A run past it is killed and
/// never counts as a catch.
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
    /// The simulator fuzz target that runs the seed's harness, with
    /// `--targets`.
    target: Option<String>,
    /// Why no fuzz target confirms the case, if none does.
    no_target: Option<String>,
    /// Whether the case's property registers no trigger-history check, as
    /// the unpatched build lists it. Its runs are then judged by their
    /// outcome alone ([`Case::expect`]).
    untriggered: bool,
}

/// What a case runs, with paths relative to the store's root.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Witness {
    /// A seed of the store, by name. The store names its harness and holds
    /// its input.
    Seed(String),
    /// A stored scenario, as `harness`'s JSON, run under the Plan in
    /// `plan`, or the empty Plan without one.
    Typed {
        /// The harness.
        harness: String,
        /// The scenario file.
        scenario: PathBuf,
        /// The Plan file, if any.
        plan: Option<PlanFile>,
    },
    /// The input that a search on the patched build stops at.
    Search(Search),
}

/// A case's `[search]` table: a `zakura-sim generate` search whose stopping
/// input is the case's witness.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Search {
    /// The harness it searches.
    harness: String,
    /// `generate`'s `--seed`.
    seed: u64,
    /// `generate`'s `--runs`: the most inputs it draws.
    runs: u64,
    /// `generate`'s `--draws`, `uniform` or `cycle`, or its default.
    draws: Option<String>,
    /// `generate`'s `--schedule`, `plan` or `pct`, or its default.
    schedule: Option<String>,
}

impl Search {
    /// Why the table can't run, if it can't: a value `generate` refuses.
    fn check(&self) -> Result<(), String> {
        if self.runs == 0 {
            return Err("a search draws at least one input, and `runs` is 0".to_owned());
        }
        if let Some(draws) = self
            .draws
            .as_deref()
            .filter(|d| !["uniform", "cycle"].contains(d))
        {
            return Err(format!("`draws` is uniform or cycle, not `{draws}`"));
        }
        if let Some(schedule) = self
            .schedule
            .as_deref()
            .filter(|s| !["plan", "pct"].contains(s))
        {
            return Err(format!("`schedule` is plan or pct, not `{schedule}`"));
        }
        Ok(())
    }

    /// `zakura-sim generate`'s arguments for the search, writing to `out`.
    fn args(&self, out: &Path) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec![
            "generate".into(),
            "--sim".into(),
            (&self.harness).into(),
            "--seed".into(),
            self.seed.to_string().into(),
            "--runs".into(),
            self.runs.to_string().into(),
            "--out".into(),
            out.into(),
        ];
        if let Some(draws) = &self.draws {
            args.extend(["--draws".into(), draws.into()]);
        }
        if let Some(schedule) = &self.schedule {
            args.extend(["--schedule".into(), schedule.into()]);
        }
        args
    }
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
    pub(super) fn path(&self) -> &Path {
        match self {
            PlanFile::Bytes(path) | PlanFile::Script(path) => path,
        }
    }

    /// The same kind of Plan at `root` joined with its path.
    pub(super) fn under(&self, root: &Path) -> PlanFile {
        match self {
            PlanFile::Bytes(path) => PlanFile::Bytes(root.join(path)),
            PlanFile::Script(path) => PlanFile::Script(root.join(path)),
        }
    }

    /// The `zakura-sim record` flag that reads it.
    pub(super) fn flag(&self) -> &'static str {
        match self {
            PlanFile::Bytes(_) => "--plan",
            PlanFile::Script(_) => "--script",
        }
    }
}

impl Witness {
    /// The harness it runs on, which `registry` names for a seed.
    fn harness(&self, registry: &Registry) -> Result<String, BoxError> {
        match self {
            Witness::Seed(name) => registry.harness(name),
            Witness::Typed { harness, .. } => Ok(harness.clone()),
            Witness::Search(search) => Ok(search.harness.clone()),
        }
    }

    /// The store's files it reads, relative to the store's root: the seed's
    /// input, or the scenario and the Plan file. A search reads none: it
    /// runs the simulator's generator, which [`SHARED`] holds.
    fn files(&self) -> Vec<PathBuf> {
        match self {
            Witness::Seed(name) => vec![seeds::input_path(name)],
            Witness::Typed { scenario, plan, .. } => std::iter::once(scenario.clone())
                .chain(plan.as_ref().map(|plan| plan.path().to_path_buf()))
                .collect(),
            Witness::Search(_) => Vec::new(),
        }
    }

    /// What [`record`] runs, with a typed case's files under `root`, the
    /// store's root, or `None` for a search case, whose input only its
    /// search finds.
    fn located(&self, root: &Path) -> Option<Located> {
        match self {
            Witness::Seed(name) => Some(Located::Seed(name.clone())),
            Witness::Typed { scenario, plan, .. } => Some(Located::Typed {
                scenario: root.join(scenario),
                plan: plan.as_ref().map(|plan| plan.under(root)),
            }),
            Witness::Search(_) => None,
        }
    }
}

/// What [`record`] runs, by its files' paths or its seed's name.
#[derive(Clone, Debug)]
pub(super) enum Located {
    /// A full fuzz input.
    Input(PathBuf),
    /// A seed of the registry, by name.
    Seed(String),
    /// A scenario file and its Plan file, if any.
    Typed {
        /// The scenario file.
        scenario: PathBuf,
        /// The Plan file, or `None` for the empty Plan.
        plan: Option<PlanFile>,
    },
}

/// A typed case's `[typed]` table.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypedFile {
    /// The harness the scenario runs on.
    harness: String,
    /// The scenario, as the harness's JSON, relative to the store's root.
    scenario: PathBuf,
    /// The Plan bytes, relative to the store's root. A typed case names
    /// this or `script`, never both, and runs the empty Plan without either.
    plan: Option<PathBuf>,
    /// A scripted Plan as JSON, relative to the store's root, for a
    /// history no Plan bytes reach.
    script: Option<PathBuf>,
}

impl TypedFile {
    /// The table's Plan file, `None` for the empty Plan, or an error if it
    /// names both Plan bytes and a script.
    pub(super) fn plan_file(&self) -> Result<Option<PlanFile>, String> {
        match (&self.plan, &self.script) {
            (Some(plan), None) => Ok(Some(PlanFile::Bytes(plan.clone()))),
            (None, Some(script)) => Ok(Some(PlanFile::Script(script.clone()))),
            (None, None) => Ok(None),
            (Some(_), Some(_)) => {
                Err("a typed case names either `plan` or `script`, and this names both".to_owned())
            }
        }
    }
}

/// A case's `<name>.toml`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseFile {
    /// The seed's name in the store's seeds.
    seed: Option<String>,
    /// The typed case, for a scenario the harness's grammar can't encode.
    typed: Option<TypedFile>,
    /// The search whose stopping input is the case's witness. A case names
    /// exactly one of `seed`, `typed` and `search`.
    pub(super) search: Option<Search>,
    /// Files or directories, relative to the repository root, whose change
    /// can break the catch besides the files the patch changes and
    /// [`SHARED`], such as the rest of the patched module.
    #[serde(default)]
    checks: Vec<PathBuf>,
    /// Other properties the patched bug breaks, by ID, as when a leaked
    /// charge also takes the byte budget past its bound. Any run may violate
    /// them beside the case's property, and a campaign input's run instead
    /// of it. The case's witness must still violate the case's property.
    #[serde(default)]
    also_violates: Vec<String>,
    /// Why `--targets` doesn't confirm the case through its fuzz target, if
    /// it doesn't, such as a patched run that takes longer than
    /// [`TARGET_CHILD`] under AddressSanitizer. `--targets` lists the case
    /// with this reason and runs the others. The case's other runs still
    /// show the catch.
    no_target: Option<String>,
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
    /// A search case's search found no input whose run violated the
    /// property: it ran out of inputs, failed, or stopped on another run.
    SearchMisses(String),
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
            Verdict::SearchMisses(why) => write!(f, "the search found no input: {why}"),
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

/// Runs every planted case of the QA store `store`, or only `property`'s,
/// and fails unless each is caught. With `shard`, it runs only that shard's cases, and fails if
/// the shard has none. With `changed_since`, it runs only the cases a
/// change since that revision affects, and passes if none is. With
/// `targets`, it runs only the cases a simulator fuzz target runs, and
/// confirms each through its target.
pub(super) fn run(
    repo_root: &Path,
    store: &Path,
    property: Option<&str>,
    shard: Option<Shard>,
    changed_since: Option<Changed<'_>>,
    targets: bool,
) -> Result<(), BoxError> {
    let cases: Vec<Case> = cases(store)?
        .into_iter()
        .filter(|case| property.is_none_or(|p| case.property == p))
        .collect();
    let work = repo_root.join("target").join("planted");
    let worktree = work.join("worktree");
    let select = Select {
        property,
        shard,
        changed_since,
        targets,
    };
    // Only `--targets` needs the unpatched build before it picks the cases.
    let mut baseline = None;
    let picked = pick(repo_root, store, &work, cases, &select, &mut baseline);
    let mut cases = match picked {
        Ok(Some(cases)) => cases,
        Ok(None) => return remove_worktree(repo_root, &worktree),
        Err(error) => {
            remove_worktree(repo_root, &worktree)?;
            return Err(error);
        }
    };
    let failed = (|| -> Result<usize, BoxError> {
        let baseline = match baseline {
            Some(baseline) => baseline,
            None => Baseline::build(repo_root, store, &work)?,
        };
        check_seeds(&baseline.registry, &cases)?;
        mark_untriggered(&baseline, &mut cases)?;
        run_cases(&worktree, &work, &baseline, &cases)
    })();
    remove_worktree(repo_root, &worktree)?;
    let failed = failed?;
    if failed > 0 {
        return Err(format!("{failed} of {} planted cases failed", cases.len()).into());
    }
    Ok(())
}

/// Which cases [`run`] runs: the flags that pick them.
struct Select<'a> {
    /// Only this property's cases.
    property: Option<&'a str>,
    /// Only this shard of them.
    shard: Option<Shard>,
    /// Only those a change since this revision, or in the store, affects.
    changed_since: Option<Changed<'a>>,
    /// Only those a simulator fuzz target runs.
    targets: bool,
}

/// The cases of `cases`, the property's, that `select` keeps, in path
/// order, or `None` if `select.changed_since` affects none. With
/// `select.targets` and a seed case, it builds `baseline` under `work` to
/// read each seed's harness, and gives each case the target that runs its
/// seed's harness. Fails on zero cases before the change filter.
fn pick(
    repo_root: &Path,
    store: &Path,
    work: &Path,
    mut cases: Vec<Case>,
    select: &Select<'_>,
    baseline: &mut Option<Baseline>,
) -> Result<Option<Vec<Case>>, BoxError> {
    if select.targets {
        let harnesses = sim_minimize::fuzz_targets(repo_root)?;
        if cases
            .iter()
            .any(|case| matches!(case.witness, Witness::Seed(_)))
        {
            let registry = &baseline
                .insert(Baseline::build(repo_root, store, work)?)
                .registry;
            assign_targets(&mut cases, &harnesses, registry)?;
        }
        let skipped;
        (cases, skipped) = through_targets(cases);
        for (case, why) in &skipped {
            println!(
                "{} {}: not confirmed through {}: {why}",
                case.property,
                case.patch.display(),
                case.target.as_deref().unwrap_or_default()
            );
        }
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
        return Err(match (select.property, select.targets) {
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
    let cases = match select.shard {
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
    let Some(since) = select.changed_since else {
        return Ok(Some(cases));
    };
    let changed = since.paths(repo_root)?;
    let mut affected = Vec::new();
    for case in cases {
        if is_affected(&watched(repo_root, store, &case)?, &changed) {
            affected.push(case);
        }
    }
    if affected.is_empty() {
        println!(
            "no planted case is affected by the changes since {}",
            since.rev
        );
        return Ok(None);
    }
    Ok(Some(affected))
}

/// The scratch worktree's unpatched build, the store's seeds it lists, and
/// the store.
struct Baseline {
    /// A copy of the unpatched `zakura-sim`, which the patched builds in the
    /// same target directory don't overwrite.
    binary: PathBuf,
    /// Every seed, as the unpatched build lists it.
    registry: Registry,
    /// The QA store's root, which holds the cases' witness files.
    store: PathBuf,
}

impl Baseline {
    /// Checks `HEAD` out in the scratch worktree under `work`, builds the
    /// unpatched simulator there in `work`'s target directory, copies the
    /// binary, and lists the seeds of `store`, the QA store, with it.
    fn build(repo_root: &Path, store: &Path, work: &Path) -> Result<Baseline, BoxError> {
        let worktree = work.join("worktree");
        remove_worktree(repo_root, &worktree)?;
        fs::create_dir_all(work)?;
        run_command(
            git(repo_root)
                .args(["worktree", "add", "--detach"])
                .arg(&worktree)
                .arg("HEAD"),
        )?;
        let target = work.join("target");
        run_command(&mut cargo_sim(&worktree, &target))?;
        let binary = work.join("baseline-zakura-sim");
        fs::copy(target.join(PROFILE).join("zakura-sim"), &binary)?;
        // Every catch is judged against this baseline, so it must be the
        // clean checkout's own build, never one an earlier run left behind.
        if let Err(why) = built_from(&binary, &worktree)? {
            return Err(format!(
                "the baseline simulator isn't {}'s: {why}",
                worktree.display()
            )
            .into());
        }
        let registry = Registry::read(&binary, &work.join("seeds"))?;
        Ok(Baseline {
            binary,
            registry,
            store: store.to_owned(),
        })
    }
}

/// Fails, naming each, if a case of `cases` names a seed that `registry`
/// lacks or rejects.
fn check_seeds(registry: &Registry, cases: &[Case]) -> Result<(), BoxError> {
    let failed: Vec<String> = cases
        .iter()
        .filter_map(|case| match &case.witness {
            Witness::Seed(name) => registry
                .seed(name)
                .err()
                .map(|error| format!("{} {}: {error}", case.property, case.patch.display())),
            Witness::Typed { .. } | Witness::Search(_) => None,
        })
        .collect();
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("cases name unusable seeds: {}", failed.join("; ")).into())
    }
}

/// Marks each case of `cases` whose property registers no trigger-history
/// check on its harness, as the unpatched build's
/// `zakura-sim properties --untriggered` lists them.
fn mark_untriggered(baseline: &Baseline, cases: &mut [Case]) -> Result<(), BoxError> {
    let mut listed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for case in cases {
        let harness = case.witness.harness(&baseline.registry)?;
        if !listed.contains_key(&harness) {
            let ids = untriggered(&baseline.binary, &harness)?;
            listed.insert(harness.clone(), ids);
        }
        case.untriggered = listed[&harness].contains(&case.property);
    }
    Ok(())
}

/// The properties of `harness` that register no trigger-history check, as
/// the `zakura_sim` binary lists them.
fn untriggered(zakura_sim: &Path, harness: &str) -> Result<Vec<String>, BoxError> {
    let output = Command::new(zakura_sim)
        .args(["properties", "--sim", harness, "--untriggered"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "`zakura-sim properties --sim {harness} --untriggered` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .map(str::to_owned)
        .collect())
}

/// Gives each seed case of `cases` the fuzz target that runs its seed's
/// harness, which `registry` names, from the fuzz package's targets and the
/// harnesses they run, `harnesses`. A typed case has no seed for a target
/// to run.
fn assign_targets(
    cases: &mut [Case],
    harnesses: &BTreeMap<String, Option<String>>,
    registry: &Registry,
) -> Result<(), BoxError> {
    for case in cases {
        if let Witness::Seed(name) = &case.witness {
            case.target = target_of(harnesses, &registry.harness(name)?);
        }
    }
    Ok(())
}

/// Splits `cases`, each with its fuzz target if one runs its seed, into
/// the cases `--targets` confirms through their target, and the cases with
/// a target whose `no_target` says why not, with that reason. It drops a
/// case with no target.
fn through_targets(cases: Vec<Case>) -> (Vec<Case>, Vec<(Case, String)>) {
    let mut confirmed = Vec::new();
    let mut skipped = Vec::new();
    for mut case in cases {
        match (&case.target, case.no_target.take()) {
            (Some(_), Some(why)) => skipped.push((case, why)),
            (Some(_), None) => confirmed.push(case),
            (None, _) => {}
        }
    }
    (confirmed, skipped)
}

/// Every case under the store `store`'s [`PLANTED_DIR`], in path order.
fn cases(store: &Path) -> Result<Vec<Case>, BoxError> {
    let root = store.join(PLANTED_DIR);
    planted_layout::files(&root, "patch")?
        .into_iter()
        .map(|patch| {
            let property = planted_layout::property_id(&root, &patch)
                .ok_or_else(|| format!("{} isn't under a property", patch.display()))?;
            let case_file = patch.with_extension("toml");
            let CaseFile {
                seed,
                typed,
                search,
                checks,
                also_violates,
                no_target,
            } = toml::from_str(
                &fs::read_to_string(&case_file)
                    .map_err(|error| format!("{}: {error}", case_file.display()))?,
            )
            .map_err(|error| format!("{}: {error}", case_file.display()))?;
            let witness = match (seed, typed, search) {
                (Some(seed), None, None) => Witness::Seed(seed),
                (None, Some(typed), None) => Witness::Typed {
                    plan: typed
                        .plan_file()
                        .map_err(|error| format!("{}: {error}", case_file.display()))?,
                    harness: typed.harness,
                    scenario: typed.scenario,
                },
                (None, None, Some(search)) => {
                    search
                        .check()
                        .map_err(|error| format!("{}: {error}", case_file.display()))?;
                    Witness::Search(search)
                }
                (seed, typed, search) => {
                    let named = [seed.is_some(), typed.is_some(), search.is_some()]
                        .into_iter()
                        .filter(|named| *named)
                        .count();
                    return Err(format!(
                        "{}: a case names one of `seed`, `[typed]` and `[search]`, and this names {named}",
                        case_file.display(),
                    )
                    .into());
                }
            };
            let files = witness.files();
            for path in files.iter().chain(&checks) {
                require_plain_relative(&case_file, path)?;
            }
            if no_target
                .as_deref()
                .is_some_and(|why| why.trim().is_empty())
            {
                return Err(format!("{}: `no_target` names no reason", case_file.display()).into());
            }
            Ok(Case {
                property,
                patch,
                witness,
                checks,
                also_violates,
                target: None,
                no_target,
                untriggered: false,
            })
        })
        .collect()
}

/// An error unless `path`, which `case_file` names, is plain and relative
/// ([`is_plain_relative`]), so that joining it to a root stays under that
/// root.
fn require_plain_relative(case_file: &Path, path: &Path) -> Result<(), BoxError> {
    if is_plain_relative(path) {
        Ok(())
    } else {
        Err(format!(
            "{}: {} isn't a plain path relative to its root",
            case_file.display(),
            path.display()
        )
        .into())
    }
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

/// What `--changed-since` and `--store-changed` name: a revision, and the
/// file that lists the store's changed paths, if any.
#[derive(Clone, Copy, Debug)]
pub(super) struct Changed<'a> {
    /// The revision whose changes count.
    pub(super) rev: &'a str,
    /// The file that lists the store's changed paths, one per line, relative
    /// to its root.
    pub(super) store: Option<&'a Path>,
}

impl Changed<'_> {
    /// The repository's paths `git diff` reports changed since `rev`
    /// ([`changed_since_rev`]), then the store's paths the store file lists.
    pub(super) fn paths(&self, repo_root: &Path) -> Result<Vec<PathBuf>, BoxError> {
        let mut changed = changed_since_rev(repo_root, self.rev)?;
        if let Some(file) = self.store {
            changed.extend(store_changes(file)?);
        }
        Ok(changed)
    }
}

/// The store's changed paths that `file` lists, one per line, relative to
/// the store's root. Blank lines don't count. Fails on a path that isn't
/// plain and relative.
pub(super) fn store_changes(file: &Path) -> Result<Vec<PathBuf>, BoxError> {
    let text = fs::read_to_string(file).map_err(|error| format!("{}: {error}", file.display()))?;
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let path = PathBuf::from(line);
            if is_plain_relative(&path) {
                Ok(path)
            } else {
                Err(format!("{}: `{line}` isn't a plain relative path", file.display()).into())
            }
        })
        .collect()
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

/// Every path whose change can break `case`'s catch: the patch and its
/// case file and the witness's files, relative to `store`, and the files
/// the patch changes, the case's checks and [`SHARED`], relative to
/// `repo_root`. The store's paths and the repository's don't overlap.
fn watched(repo_root: &Path, store: &Path, case: &Case) -> Result<Vec<PathBuf>, BoxError> {
    let relative = |path: &Path| {
        path.strip_prefix(store)
            .map(Path::to_path_buf)
            .map_err(|_| format!("{} isn't under the QA store", path.display()))
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

/// Runs each case in `worktree` against its unpatched `baseline`, writing
/// its traces under `work`, and resets the worktree after each. Returns
/// how many cases failed.
fn run_cases(
    worktree: &Path,
    work: &Path,
    baseline: &Baseline,
    cases: &[Case],
) -> Result<usize, BoxError> {
    let target = work.join("target");
    let mut failed = 0;
    for (index, case) in cases.iter().enumerate() {
        let scratch = work.join(format!("case-{index}"));
        fs::create_dir_all(&scratch)?;
        let builds = confirm_builds(work);
        let verdict = check_case(worktree, &scratch, &target, &builds, baseline, case);
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

/// Runs the witness on the unpatched `baseline` binary, then patches
/// `worktree` and runs it again, building in `target` and writing both
/// traces under `scratch`. A search case patches and builds first, and its
/// witness is the input its search on the patched build stops at. A
/// confirmation through a fuzz target builds under `builds`.
fn check_case(
    worktree: &Path,
    scratch: &Path,
    target: &Path,
    builds: &Path,
    baseline: &Baseline,
    case: &Case,
) -> Result<Verdict, BoxError> {
    let harness = case.witness.harness(&baseline.registry)?;
    let mut patched = None;
    let input = match &case.witness {
        // A seed's builder can run the code the patch changes, so a seed
        // built on the patched tree could be another input. Both runs take
        // the unpatched build's input.
        Witness::Seed(name) => Located::Input(seeds::write_input(
            &baseline.binary,
            baseline.registry.seed(name)?,
            &scratch.join("seed"),
        )?),
        Witness::Typed { .. } => case
            .witness
            .located(&baseline.store)
            .ok_or("a typed case has its files")?,
        Witness::Search(search) => {
            let binary = match build_patched(worktree, target, case)? {
                Ok(binary) => binary,
                Err(verdict) => return Ok(verdict),
            };
            let found = search_input(&binary, search, case, &scratch.join("search"))?;
            patched = Some(binary);
            match found {
                Ok(input) => Located::Input(input),
                Err(verdict) => return Ok(verdict),
            }
        }
    };
    let unpatched = record(
        Command::new(&baseline.binary),
        &harness,
        &input,
        &case.property,
        case.expect_unpatched(),
        &scratch.join("baseline"),
        Some((RUN_DEADLINE, &NEVER_INTERRUPTED)),
    )?;
    if let Some(why) = baseline_failure(&case.property, &unpatched) {
        return Ok(Verdict::BaselineFails(why));
    }
    let patched = match patched {
        Some(binary) => binary,
        None => match build_patched(worktree, target, case)? {
            Ok(binary) => binary,
            Err(verdict) => return Ok(verdict),
        },
    };
    let verdict = judge_patched(&patched, &harness, &input, case, scratch, RUN_DEADLINE)?;
    match (&verdict, &case.target) {
        // A confirmation through a target needs the trigger history.
        (Verdict::Caught(_), Some(fuzz)) if case.untriggered => Ok(Verdict::TargetMisses {
            target: fuzz.clone(),
            why: format!(
                "{} registers no trigger-history check, which `zakura-sim confirm` needs; \
                 give the case a `no_target` reason",
                case.property
            ),
        }),
        (Verdict::Caught(_), Some(fuzz)) => Ok(through_target(
            worktree, builds, scratch, case, fuzz, &harness, baseline,
        )?
        .unwrap_or(verdict)),
        _ => Ok(verdict),
    }
}

/// Applies `case`'s patch to `worktree` and builds it in `target`. Returns
/// the patched binary, or the verdict of a case that can't go on: the
/// patch doesn't apply or changes a witness file, the build fails, or the
/// binary isn't this checkout's.
fn build_patched(
    worktree: &Path,
    target: &Path,
    case: &Case,
) -> Result<Result<PathBuf, Verdict>, BoxError> {
    if let Some(verdict) = apply_case_patch(worktree, case)? {
        return Ok(Err(verdict));
    }
    if let Err(error) = run_command(&mut cargo_sim(worktree, target)) {
        return Ok(Err(Verdict::Failed(format!(
            "the patched build failed: {error}"
        ))));
    }
    let patched = target.join(PROFILE).join("zakura-sim");
    // The patched builds reuse the target directory, so a catch counts only
    // from a binary built from this case's patched checkout.
    if let Err(why) = built_from(&patched, worktree)? {
        return Ok(Err(Verdict::StaleBinary(why)));
    }
    Ok(Ok(patched))
}

/// Runs `search` with the patched `zakura_sim` binary into `out`, and
/// returns the input that stopped it, or [`Verdict::SearchMisses`] if it
/// ran out of inputs or stopped on anything but `case`'s property. The
/// search gets [`SEARCH_DEADLINE`].
fn search_input(
    zakura_sim: &Path,
    search: &Search,
    case: &Case,
    out: &Path,
) -> Result<Result<PathBuf, Verdict>, BoxError> {
    if out.exists() {
        fs::remove_dir_all(out)?;
    }
    let out = path::absolute(out)?;
    let log = out.with_extension("log");
    let mut generate = Command::new(zakura_sim);
    generate.args(search.args(&out));
    let end = sim_minimize::run_child(
        &mut generate,
        &log,
        Some(SEARCH_DEADLINE),
        &NEVER_INTERRUPTED,
    )?;
    let misses = |why: String| {
        Ok(Err(Verdict::SearchMisses(format!(
            "{why}; see {}",
            log.display()
        ))))
    };
    let report = match (&end, fs::read_to_string(out.join(GENERATE_REPORT))) {
        (sim_minimize::ChildEnd::Exited(0), Ok(report)) => report,
        (end, _) => return misses(format!("`zakura-sim generate` {end}")),
    };
    let report: Value = serde_json::from_str(&report)?;
    search_stop(&case.property, &report).map_or_else(misses, |input| Ok(Ok(input)))
}

/// The input that stopped the search whose `generate.json` is `report`, if
/// a violation of `property` stopped it, or why it missed.
fn search_stop(property: &str, report: &Value) -> Result<PathBuf, String> {
    let stop = &report["stop"];
    match stop["kind"].as_str() {
        Some("violation") if stop["property"].as_str() == Some(property) => report["input"]
            .as_str()
            .map(PathBuf::from)
            .ok_or_else(|| "the search stopped on a violation but saved no input".to_owned()),
        Some("violation") => Err(format!(
            "the search stopped on {} instead",
            stop["property"].as_str().unwrap_or("an unnamed property")
        )),
        Some("clean") => Err(format!(
            "none of the search's {} inputs violated the property",
            report["executed"]
        )),
        Some(kind) => Err(format!(
            "the search stopped on a run that ended {kind}: {stop}"
        )),
        None => Err(format!("the search report has no stop: {stop}")),
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
        .filter(|file| changed.iter().any(|path| path.starts_with(file)))
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
        Command::new(zakura_sim),
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
    fresh_replay(verdict, || replay_misses(zakura_sim, &trace, deadline))
}

impl Case {
    /// What every patched run of the case expects: its property violated,
    /// beside `also_violates`. An untriggered property's run has no witness
    /// test, and its end alone shows the violation.
    fn expect(&self) -> Expect<'_> {
        if self.untriggered {
            Expect::Unjudged
        } else {
            Expect::Violated(&self.also_violates)
        }
    }

    /// What the unpatched run expects: the property holds. An untriggered
    /// property's witness test can expect only `applicable`, and
    /// [`baseline_failure`] still rejects any violation.
    fn expect_unpatched(&self) -> Expect<'_> {
        if self.untriggered {
            Expect::Applicable
        } else {
            Expect::Holds
        }
    }

    /// What `patched`, one of the case's patched runs, shows about it
    /// ([`classify`]).
    fn classify(&self, patched: &Run) -> Verdict {
        classify(&self.property, &self.also_violates, patched)
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

/// Confirms `case`'s seed in the patched `worktree` through the fuzz target
/// `target`, which runs `harness`, in fresh processes, with
/// [`sim_minimize::confirm`]: the target must crash with the case's
/// property, beside no other violation but the case's `also_violates`, and
/// name `harness`, `zakura-sim confirm` must find the property's trigger
/// history, and a fresh replay must match Exact. The unpatched `baseline`
/// writes the seed's input under `scratch`. Builds under `builds`, which
/// outlives the worktree, and writes under `scratch`.
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
    baseline: &Baseline,
) -> Result<Option<Verdict>, BoxError> {
    let Witness::Seed(name) = &case.witness else {
        unreachable!("`run` gives a typed case no target");
    };
    let input = seeds::write_input(
        &baseline.binary,
        baseline.registry.seed(name)?,
        &scratch.join("seed"),
    )?;
    let out = path::absolute(scratch.join("target"))?;
    if out.exists() {
        fs::remove_dir_all(&out)?;
    }
    let request = sim_minimize::Request {
        target: target.to_owned(),
        harness: harness.to_owned(),
        input: path::absolute(input)?,
        property: case.property.clone(),
        also_violates: case.also_violates.clone(),
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
pub(super) fn names_source(binary: &[u8], source: &str) -> Result<(), String> {
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
            Command::new(binary),
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
    /// The property applies and holds, judged by its outcome alone: the
    /// unpatched run of a property with no trigger-history check.
    Applicable,
    /// No witness test: a patched run of a property with no trigger-history
    /// check, which the trace's end alone judges.
    Unjudged,
}

impl Expect<'_> {
    /// `zakura-sim record`'s `--expect` flag and value, then its
    /// `--also-violates` flag and value if the list isn't empty. Nothing
    /// for [`Expect::Unjudged`], which runs no witness test.
    fn args(self) -> Vec<String> {
        match self {
            Expect::Holds => vec!["--expect".to_owned(), "holds".to_owned()],
            Expect::Applicable => vec!["--expect".to_owned(), "applicable".to_owned()],
            Expect::Violated(also_violates) => {
                let mut args = vec!["--expect".to_owned(), "violated".to_owned()];
                if !also_violates.is_empty() {
                    args.extend(["--also-violates".to_owned(), also_violates.join(",")]);
                }
                args
            }
            Expect::Unjudged => Vec::new(),
        }
    }

    /// Whether the run is a witness test, which writes `witness.json`.
    fn judged(self) -> bool {
        !matches!(self, Expect::Unjudged)
    }
}

/// Records `input`, a fuzz input, a seed or a typed scenario and its Plan bytes, on
/// `harness` with `zakura_sim`, a `zakura-sim` command, as a witness test of
/// `property` that expects `expect`, writing the trace to `out`, and reads
/// its `end` record and the test's result. An [`Expect::Unjudged`] run is
/// no witness test, and its end record alone judges it.
///
/// With a `deadline` and its interrupt flag, the command's output goes to
/// `<out>.log`. A run past the deadline, or one that a signal killed, is
/// [`Run::Killed`], whatever files it already wrote. A set flag kills the
/// run and stops the pilot.
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
        .arg(out);
    if expect.judged() {
        zakura_sim.args(["--witness", property]);
    }
    zakura_sim.args(expect.args());
    let ended = match deadline {
        None => format!("exited with {}", zakura_sim.status()?),
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
                sim_minimize::ChildEnd::Exited(_) => ended,
            }
        }
    };
    // A failed witness test exits 1 too, but only after writing its result.
    let witness: Option<WitnessFile> = if expect.judged() {
        let Ok(witness) = fs::read_to_string(out.join("witness.json")) else {
            return Ok(Run::Failed(format!("`zakura-sim record` {ended}")));
        };
        Some(serde_json::from_str(&witness)?)
    } else {
        None
    };
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
    if let (
        Run::Ended {
            witness: failed, ..
        },
        Some(witness),
    ) = (&mut run, witness)
    {
        *failed = (!witness.passed).then(|| {
            witness
                .error
                .unwrap_or_else(|| "no reason given".to_owned())
        });
    }
    Ok(run)
}

/// `zakura-sim record`'s arguments for `input`: `--input` and the file,
/// `--seed` and the seed's name, or `--scenario` with the scenario file's
/// text and, if the case has a Plan file, `--plan` with the Plan bytes file
/// or `--script` with the scripted Plan's file.
fn input_args(input: &Located) -> Result<Vec<OsString>, BoxError> {
    Ok(match input {
        Located::Input(path) => vec!["--input".into(), path.into()],
        Located::Seed(name) => vec!["--seed".into(), name.into()],
        Located::Typed { scenario, plan } => {
            let mut args = vec![
                "--scenario".into(),
                fs::read_to_string(scenario)
                    .map_err(|error| format!("{}: {error}", scenario.display()))?
                    .into(),
            ];
            if let Some(plan) = plan {
                args.extend([plan.flag().into(), plan.path().into()]);
            }
            args
        }
    })
}

/// `verdict`, unless it's a catch and `replay`, the fresh replay of the
/// patched run's trace, says why it missed: then [`Verdict::ReplayMisses`].
/// Every catch replays, a seed case's and a typed case's alike.
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

/// Replays the trace in `trace` with `zakura_sim` in a fresh process, and
/// says why it didn't match Exact, or `None` if it did. The replay's output
/// goes to `<trace>.replay.log`, and a replay past `deadline` is killed and
/// misses.
fn replay_misses(
    zakura_sim: &Path,
    trace: &Path,
    deadline: Duration,
) -> Result<Option<String>, BoxError> {
    let log = trace.with_extension("replay.log");
    let mut replay = Command::new(zakura_sim);
    replay.arg("replay").arg(trace);
    let end = sim_minimize::run_child(&mut replay, &log, Some(deadline), &NEVER_INTERRUPTED)?;
    Ok(match end {
        sim_minimize::ChildEnd::Exited(0) => None,
        end => Some(format!(
            "`zakura-sim replay` {end}: {}",
            fs::read_to_string(&log).unwrap_or_default().trim()
        )),
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
/// case's witness and every campaign input are judged by this one rule.
fn classify(property: &str, also_violates: &[String], patched: &Run) -> Verdict {
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
    let expected = |id: &String| id == property || also_violates.contains(id);
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
    match witness {
        Some(why) => Verdict::WitnessFails(why.clone()),
        None => Verdict::Caught(None),
    }
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
fn wait_until_executable(script: &Path) {
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
    /// case's own, on the case's witness as on a campaign input. Only the
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

    /// A store holding one discovery case, `ID/case`, whose patch changes
    /// `crates/zakura-network/src/zakura/discovery/mod.rs` and whose checks
    /// name the transport and the wire codec, beside a typed case,
    /// `typed/case`, and the case's cases.
    fn discovery_store() -> (tempfile::TempDir, Vec<Case>) {
        let store = tempfile::tempdir().expect("a temporary store");
        let write = |path: &str, text: &str| {
            let path = store.path().join(path);
            fs::create_dir_all(path.parent().expect("a parent")).unwrap();
            fs::write(path, text).unwrap();
        };
        let file = "crates/zakura-network/src/zakura/discovery/mod.rs";
        let patch = format!("--- a/{file}\n+++ b/{file}\n@@ -1 +1 @@\n-a\n+b\n");
        write(&format!("{PLANTED_DIR}/{ID}/case.patch"), &patch);
        write(
            &format!("{PLANTED_DIR}/{ID}/case.toml"),
            "seed = \"discovery-wait-complete\"\nchecks = [\"crates/zakura-network/src/zakura/transport\", \"crates/zakura-network/src/zakura/wire_codec\"]\n",
        );
        write(&format!("{PLANTED_DIR}/typed/case.patch"), &patch);
        write(
            &format!("{PLANTED_DIR}/typed/case.toml"),
            "[typed]\nharness = \"discovery/wire\"\nscenario = \"fixtures/s.json\"\n",
        );
        let cases = cases(store.path()).expect("the stored cases parse");
        (store, cases)
    }

    /// A change to the shared machinery affects a stored case, though it
    /// touches none of the case's own files, and so does a change to the
    /// case's own files in the store.
    #[test]
    fn shared_changes_affect_a_stored_case() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let (store, cases) = discovery_store();
        let case = cases
            .iter()
            .find(|case| case.property == ID)
            .expect("the discovery case");
        let watched = watched(&repo_root, store.path(), case).expect("the case's files exist");
        for path in [
            "crates/xtask/src/sim.rs",
            "crates/xtask/src/main.rs",
            "crates/zakura-sim/src/property/registry.rs",
            "crates/zakura-sim/src/sim.rs",
            "crates/zakura-scheduler/src/lib.rs",
            "crates/zakura-rt/src/sim/sync/notify.rs",
            "Cargo.lock",
            ".cargo/config.toml",
            "crates/zakura-network/src/zakura/discovery/mod.rs",
            "crates/zakura-network/src/zakura/transport/io.rs",
            "crates/zakura-network/src/zakura/wire_codec/message.rs",
            "qa/planted/discovery/wait-complete-wakes/case.patch",
            "qa/planted/discovery/wait-complete-wakes/case.toml",
            "seeds/discovery-wait-complete.bin",
        ] {
            assert!(is_affected(&watched, &[PathBuf::from(path)]), "{path}");
        }
        assert!(!is_affected(&watched, &[PathBuf::from("seeds/other.bin")]));
    }

    /// `--store-changed` lists the store's changed paths, one per line, and
    /// refuses a path that isn't plain and relative.
    #[test]
    fn store_changes_read_plain_paths() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file = dir.path().join("changed.txt");
        fs::write(&file, "qa/planted/a/b.toml\n\nseeds/x.bin\n").unwrap();
        assert_eq!(
            store_changes(&file).expect("the list reads"),
            [
                PathBuf::from("qa/planted/a/b.toml"),
                PathBuf::from("seeds/x.bin")
            ]
        );
        fs::write(&file, "../escape\n").unwrap();
        assert!(store_changes(&file).is_err());
    }

    /// A registry row for the admitted seed `name` on `harness`.
    fn seed_row(name: &str, harness: &str) -> seeds::SeedRow {
        seeds::SeedRow {
            name: name.to_owned(),
            harness: harness.to_owned(),
            set: "corpus".to_owned(),
            status: "admitted".to_owned(),
            input_sha256: Some("ab".repeat(32)),
            reason: None,
        }
    }

    /// With `--targets`, a seed case runs through the fuzz target that runs
    /// its seed's harness, such as the discovery wait case's seed through
    /// `discovery_wait_complete`, and a typed case runs through none. A
    /// seed the registry lacks fails.
    #[test]
    fn stored_cases_run_through_the_target_of_their_harness() {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let harnesses = sim_minimize::fuzz_targets(&repo_root).expect("cargo metadata reads it");
        let (_store, mut cases) = discovery_store();
        let registry = Registry::from_rows([seed_row(
            "discovery-wait-complete",
            "discovery/wait-complete",
        )])
        .expect("one row");
        assign_targets(&mut cases, &harnesses, &registry).expect("the registry names the seed");
        for case in &cases {
            let expected = (case.property == ID).then(|| "discovery_wait_complete".to_owned());
            assert_eq!(case.target, expected, "{}", case.patch.display());
        }
        let mut unknown = vec![Case {
            witness: Witness::Seed("no-such-seed".to_owned()),
            ..cases[0].clone()
        }];
        let error = assign_targets(&mut unknown, &harnesses, &registry)
            .expect_err("the registry lacks the seed")
            .to_string();
        assert!(error.contains("no seed is named `no-such-seed`"), "{error}");
    }

    /// `--targets` confirms a case with a target through it, lists a case
    /// with a target and a `no_target` reason with that reason, and drops
    /// a case no target runs. Only a case with a target needs the reason.
    #[test]
    fn no_target_keeps_a_case_off_its_target() {
        let case = |name: &str, target: Option<&str>, no_target: Option<&str>| Case {
            property: format!("p/{name}"),
            patch: PathBuf::from(format!("qa/planted/p/{name}/case.patch")),
            witness: Witness::Seed("in".to_owned()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            target: target.map(str::to_owned),
            no_target: no_target.map(str::to_owned),
            untriggered: false,
        };
        let (confirmed, skipped) = through_targets(vec![
            case("runs", Some("t"), None),
            case("slow", Some("t"), Some("too slow")),
            case("untargeted", None, Some("unused")),
            case("plain", None, None),
        ]);
        let names = |cases: Vec<&Case>| -> Vec<String> {
            cases
                .into_iter()
                .map(|case| case.property.clone())
                .collect()
        };
        assert_eq!(names(confirmed.iter().collect()), ["p/runs"]);
        assert_eq!(
            names(skipped.iter().map(|(case, _)| case).collect()),
            ["p/slow"]
        );
        assert_eq!(skipped[0].1, "too slow");
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

    /// A binary counts only for the checkout it embeds the digest of.
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
    /// renames a witness file, a typed case's or the seed registry's, stops
    /// the case before anything runs on the patched checkout.
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
            ("seeds/a.bin", "fn seeds() {}\n"),
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
        let registry = patch("registry.patch", &write("seeds/a.bin"));
        let removed = patch("removed.patch", &|| {
            fs::remove_file(root.join("seeds/a.bin")).unwrap()
        });
        let typed = Case {
            property: ID.to_owned(),
            patch: code.clone(),
            witness: Witness::Typed {
                harness: "h".to_owned(),
                scenario: "s.json".into(),
                plan: Some(PlanFile::Bytes("p.bin".into())),
            },
            checks: Vec::new(),
            also_violates: Vec::new(),
            target: None,
            no_target: None,
            untriggered: false,
        };
        let scripted = Case {
            witness: Witness::Typed {
                harness: "h".to_owned(),
                scenario: "s.json".into(),
                plan: Some(PlanFile::Script("p.json".into())),
            },
            ..typed.clone()
        };
        let seed = Case {
            witness: Witness::Seed("a".to_owned()),
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
        assert_eq!(judge(&seed, &code), None, "a code patch goes on");
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
        assert_eq!(judge(&seed, &registry), touches(&["seeds/a.bin"]));
        assert_eq!(judge(&seed, &removed), touches(&["seeds/a.bin"]));
        assert_eq!(
            judge(&typed, &registry),
            None,
            "a seed's input isn't a typed case's witness"
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
    /// naming the seed `<name>`.
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
                format!("seed = \"{name}\"\n"),
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

    /// A case file names a seed or a typed case, never both and never
    /// neither, a typed case's files are plain relative paths, a typed case
    /// names Plan bytes or a script but not both, and a typed case without
    /// either runs the empty Plan.
    #[test]
    fn a_case_names_a_seed_or_a_typed_case() {
        let seed = parse_case(r#"seed = "a""#).expect("a seed case parses");
        assert_eq!(seed[0].witness, Witness::Seed("a".to_owned()));
        let error = parse_case(r#"input = "fixtures/a.bin""#)
            .expect_err("a fixture path is refused")
            .to_string();
        assert!(error.contains("unknown field `input`"), "{error}");
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
                plan: Some(PlanFile::Bytes("qa/planted/x/case.plan.bin".into())),
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
                plan: Some(PlanFile::Script("qa/planted/x/case.script.json".into())),
            }
        );
        let both = r#"
            seed = "a"
            [typed]
            harness = "header-sync/storage"
            scenario = "s.json"
            plan = "p.bin"
        "#;
        let error = parse_case(both).expect_err("both is refused").to_string();
        assert!(error.contains("this names 2"), "{error}");
        let error = parse_case("checks = []")
            .expect_err("neither is refused")
            .to_string();
        assert!(error.contains("this names 0"), "{error}");
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
        let empty_plan = r#"
            [typed]
            harness = "header-sync/storage"
            scenario = "s.json"
        "#;
        assert_eq!(
            parse_case(empty_plan).expect("a typed case without a Plan file parses")[0].witness,
            Witness::Typed {
                harness: "header-sync/storage".to_owned(),
                scenario: "s.json".into(),
                plan: None,
            }
        );
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

    /// A search case names its harness and its search, and no input. It
    /// names no other witness beside it, and only the draw policies and
    /// schedules `zakura-sim generate` takes.
    #[test]
    fn a_search_case_names_its_search() {
        let search = r#"
            [search]
            harness = "header-sync/storage"
            seed = 7
            runs = 200
            draws = "cycle"
        "#;
        let parsed = parse_case(search).expect("a search case parses");
        let Witness::Search(found) = &parsed[0].witness else {
            panic!("{:?} is a search", parsed[0].witness);
        };
        assert_eq!(parsed[0].witness.files(), Vec::<PathBuf>::new());
        assert!(parsed[0].witness.located(Path::new(".")).is_none());
        assert_eq!(
            found.args(Path::new("out")),
            [
                "generate",
                "--sim",
                "header-sync/storage",
                "--seed",
                "7",
                "--runs",
                "200",
                "--out",
                "out",
                "--draws",
                "cycle",
            ]
            .map(OsString::from)
        );
        let pct = r#"
            [search]
            harness = "sync/wire"
            seed = 1
            runs = 3
            schedule = "pct"
        "#;
        let Witness::Search(found) = &parse_case(pct).expect("a pct search parses")[0].witness
        else {
            panic!("a search");
        };
        assert_eq!(
            found.args(Path::new("o"))[7..],
            ["--out", "o", "--schedule", "pct"].map(OsString::from)
        );
        for (bad, why) in [
            (r#"draws = "every""#, "`draws` is uniform or cycle"),
            (r#"schedule = "random""#, "`schedule` is plan or pct"),
            ("runs = 0", "`runs` is 0"),
        ] {
            let mut table = format!("[search]\nharness = \"h\"\nseed = 1\n{bad}\n");
            if !bad.starts_with("runs") {
                table.push_str("runs = 5\n");
            }
            let error = parse_case(&table).expect_err(bad).to_string();
            assert!(error.contains(why), "{error}");
        }
        let both = r#"
            seed = "a"
            [search]
            harness = "h"
            seed = 1
            runs = 1
        "#;
        let error = parse_case(both)
            .expect_err("a seed beside a search")
            .to_string();
        assert!(error.contains("this names 2"), "{error}");
        let error = parse_case("[search]\nharness = \"h\"\nruns = 1\n")
            .expect_err("a search without a seed")
            .to_string();
        assert!(error.contains("missing field `seed`"), "{error}");
    }

    /// The search's report gives the input only when a violation of the
    /// case's own property stopped it. Any other stop misses the patch.
    #[test]
    fn a_search_stops_only_on_the_property() {
        let report = |stop: Value| {
            serde_json::json!({
                "stop": stop,
                "executed": 40,
                "input": "/s/abc.bin",
            })
        };
        assert_eq!(
            search_stop(
                ID,
                &report(serde_json::json!({"kind": "violation", "property": ID}))
            ),
            Ok(PathBuf::from("/s/abc.bin"))
        );
        let other = search_stop(
            ID,
            &report(serde_json::json!({"kind": "violation", "property": "sim/no-livelock"})),
        )
        .expect_err("another property misses");
        assert!(other.contains("sim/no-livelock instead"), "{other}");
        let clean = search_stop(ID, &report(serde_json::json!({"kind": "clean"})))
            .expect_err("a clean search misses");
        assert!(clean.contains("none of the search's 40 inputs"), "{clean}");
        let failed = search_stop(
            ID,
            &report(serde_json::json!({"kind": "harness-failure", "reason": "x"})),
        )
        .expect_err("a harness failure misses");
        assert!(failed.contains("ended harness-failure"), "{failed}");
        let unsaved = search_stop(
            ID,
            &serde_json::json!({"stop": {"kind": "violation", "property": ID}}),
        )
        .expect_err("a stop with no input misses");
        assert!(unsaved.contains("saved no input"), "{unsaved}");
    }

    /// A property with no trigger-history check records its unpatched run
    /// as a witness test that expects `applicable`, and its patched runs
    /// with no witness test. The end record alone judges a patched run: a
    /// violation of the property alone is a catch, and any other violation
    /// is unrelated, as for any case.
    #[cfg(unix)]
    #[test]
    fn an_untriggered_property_is_judged_by_its_outcome() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temporary directory");
        // `record` writes `$end` as its trace and its arguments beside it,
        // and a witness result only for a witness test.
        let fake = |name: &str, end: &str| {
            let path = dir.path().join(name);
            let script = format!(
                "#!/bin/sh\n\
                 [ \"$1\" = replay ] && exit 0\n\
                 out=; prev=; witness=\n\
                 for arg in \"$@\"; do [ \"$prev\" = --out ] && out=$arg; [ \"$arg\" = --witness ] && witness=1; prev=$arg; done\n\
                 mkdir -p \"$out\"\n\
                 printf '%s\\n' \"$*\" > \"$out/args\"\n\
                 printf '%s\\n' '{end}' > \"$out/trace.jsonl\"\n\
                 [ -n \"$witness\" ] && printf '%s\\n' '{{\"passed\":false,\"error\":\"judged\"}}' > \"$out/witness.json\"\n\
                 exit 0\n"
            );
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            wait_until_executable(&path);
            path
        };
        let case = Case {
            property: ID.to_owned(),
            patch: dir.path().join("case.patch"),
            witness: Witness::Seed("a".to_owned()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            target: None,
            no_target: None,
            untriggered: true,
        };
        assert!(matches!(case.expect_unpatched(), Expect::Applicable));
        assert!(matches!(case.expect(), Expect::Unjudged));
        let triggered = Case {
            untriggered: false,
            ..case.clone()
        };
        assert!(matches!(triggered.expect_unpatched(), Expect::Holds));
        assert!(matches!(triggered.expect(), Expect::Violated(_)));
        assert_eq!(Expect::Applicable.args(), ["--expect", "applicable"]);
        assert!(Expect::Unjudged.args().is_empty());

        let input = Located::Seed("a".to_owned());
        let violates = fake("violates", &end(Some(ID), &[(ID, VIOLATED)]));
        let out = dir.path().join("unjudged");
        let run = record(
            Command::new(&violates),
            "header-sync/storage",
            &input,
            ID,
            case.expect(),
            &out,
            None,
        )
        .expect("the fake binary runs");
        let args = fs::read_to_string(out.join("args")).unwrap();
        assert!(
            !args.contains("--witness") && !args.contains("--expect"),
            "{args}"
        );
        // No witness test ran, so its failure can't read into the run.
        assert!(matches!(&run, Run::Ended { witness: None, .. }), "{run:?}");
        assert_eq!(case.classify(&run), Verdict::Caught(None));
        let other = fake(
            "other",
            &end(
                Some("sim/no-livelock"),
                &[(ID, HELD), ("sim/no-livelock", VIOLATED)],
            ),
        );
        let run = record(
            Command::new(&other),
            "header-sync/storage",
            &input,
            ID,
            case.expect(),
            &dir.path().join("other-run"),
            None,
        )
        .expect("the fake binary runs");
        assert_eq!(
            case.classify(&run),
            Verdict::Unrelated("sim/no-livelock".to_owned())
        );
        // The unpatched run is a witness test, and its result counts.
        let baseline = dir.path().join("baseline");
        let run = record(
            Command::new(&other),
            "header-sync/storage",
            &input,
            ID,
            case.expect_unpatched(),
            &baseline,
            None,
        )
        .expect("the fake binary runs");
        let args = fs::read_to_string(baseline.join("args")).unwrap();
        assert!(
            args.contains(&format!("--witness {ID} --expect applicable")),
            "{args}"
        );
        assert!(baseline_failure(ID, &run).is_some());
    }

    /// A catch, a fuzz input's or a typed case's, passes only if its
    /// fresh replay matches. A miss is its own verdict, and an
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
    /// trace, a typed case's and a seed case's alike: with a `zakura-sim`
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
                plan: Some(PlanFile::Bytes("p.bin".into())),
            },
            checks: Vec::new(),
            also_violates: Vec::new(),
            target: None,
            no_target: None,
            untriggered: false,
        };
        let judge = |binary: &Path, case: &Case| {
            judge_patched(
                binary,
                "header-sync/storage",
                &case.witness.located(dir.path()).unwrap(),
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
        let seed = Case {
            witness: Witness::Seed("a".to_owned()),
            ..typed.clone()
        };
        assert!(
            matches!(judge(&misses, &seed), Verdict::ReplayMisses(_)),
            "a seed case replays too"
        );
        assert!(
            matches!(judge(&matches, &seed), Verdict::Caught(Some(_))),
            "a seed case's matching replay keeps the catch"
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
            replay_misses(&matched, dir.path(), RUN_DEADLINE).unwrap(),
            None
        );
        let why = replay_misses(&missed, dir.path(), RUN_DEADLINE)
            .unwrap()
            .expect("a failed replay says why");
        assert!(why.contains("pick 3 differs"), "{why}");
        // A replay that hangs is killed at its deadline, and misses.
        // `wait_until_executable` runs it with no arguments.
        let hangs = fake("hangs", "[ $# -gt 0 ] || exit 0\nsleep 60");
        let why = replay_misses(&hangs, dir.path(), Duration::from_millis(200))
            .unwrap()
            .expect("a hung replay misses");
        assert!(why.contains("ran past its deadline"), "{why}");
    }

    /// Every run of a case has a deadline. A patched recording that
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
            witness: Witness::Seed("a".to_owned()),
            checks: Vec::new(),
            also_violates: Vec::new(),
            target: None,
            no_target: None,
            untriggered: false,
        };
        let scratch = dir.path().join("case");
        fs::create_dir_all(&scratch).unwrap();
        let started = std::time::Instant::now();
        let run = record(
            Command::new(&hangs),
            "header-sync/storage",
            &case.witness.located(dir.path()).unwrap(),
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
            &case.witness.located(dir.path()).unwrap(),
            &case,
            &scratch,
            Duration::from_millis(200),
        )
        .expect("the fake binary starts");
        assert!(matches!(verdict, Verdict::Failed(_)), "{verdict:?}");
    }

    /// A typed case watches its scenario and Plan file, and runs with
    /// `--scenario` holding the scenario's text, and `--plan` or `--script`
    /// only if it has a Plan file. A seed case watches the registry and runs
    /// with `--seed`.
    #[test]
    fn a_typed_case_watches_and_runs_its_files() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let witness = Witness::Typed {
            harness: "header-sync/storage".to_owned(),
            scenario: "s.json".into(),
            plan: Some(PlanFile::Bytes("p.bin".into())),
        };
        assert_eq!(
            witness.files(),
            vec![PathBuf::from("s.json"), PathBuf::from("p.bin")]
        );
        assert_eq!(
            witness
                .harness(&Registry::default())
                .expect("a typed case names it"),
            "header-sync/storage"
        );
        fs::write(dir.path().join("s.json"), r#"{"mirrors":2}"#).unwrap();
        let args = input_args(&witness.located(dir.path()).unwrap()).expect("the scenario reads");
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
            plan: Some(PlanFile::Script("p.json".into())),
        };
        assert_eq!(
            scripted.files(),
            vec![PathBuf::from("s.json"), PathBuf::from("p.json")]
        );
        assert_eq!(
            input_args(&scripted.located(dir.path()).unwrap()).expect("the scenario reads"),
            vec![
                OsString::from("--scenario"),
                OsString::from(r#"{"mirrors":2}"#),
                OsString::from("--script"),
                dir.path().join("p.json").into_os_string(),
            ]
        );
        let empty_plan = Witness::Typed {
            harness: "header-sync/storage".to_owned(),
            scenario: "s.json".into(),
            plan: None,
        };
        assert_eq!(empty_plan.files(), vec![PathBuf::from("s.json")]);
        assert_eq!(
            input_args(&empty_plan.located(dir.path()).unwrap()).expect("the scenario reads"),
            vec![
                OsString::from("--scenario"),
                OsString::from(r#"{"mirrors":2}"#),
            ]
        );
        let seed = Witness::Seed("a".to_owned());
        assert_eq!(seed.files(), vec![PathBuf::from("seeds/a.bin")]);
        let registry = Registry::from_rows([seed_row("a", "discovery/wire")]).expect("one row");
        assert_eq!(seed.harness(&registry).unwrap(), "discovery/wire");
        assert_eq!(
            input_args(&seed.located(dir.path()).unwrap()).expect("a seed needs no read"),
            vec![OsString::from("--seed"), OsString::from("a")]
        );
        assert_eq!(
            input_args(&Located::Input(dir.path().join("a.bin"))).expect("an input needs no read"),
            vec![
                OsString::from("--input"),
                dir.path().join("a.bin").into_os_string()
            ]
        );
    }
}
