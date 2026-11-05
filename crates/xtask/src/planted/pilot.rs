//! `cargo xtask planted pilot (sensitivity|discovery)`: the mutation pilot's
//! paired runner (mutation plan, mut-3).
//!
//! Both experiments read the reviewed catalogue,
//! `qa/mutation-pilot/catalogue.json`, and check its pins before anything
//! runs: each selected case's patch, witness input and production files
//! must match the catalogue's hashes at the pinned revision. Each builds the
//! original and each mutant in its own worktree and target directory under
//! `--work`, applies the patch there, and requires that it change exactly
//! the case's declared production files. A case's worktree and build go
//! once its artifacts are written; a failure to remove them is recorded
//! beside its result. Results go to `--out`, which must be missing or
//! empty, so a run never overwrites an earlier result.
//!
//! `sensitivity` checks out the witness revision, `source_revision`, and
//! builds `zakura-sim` with the catalogue's witness build flags and
//! toolchain. For each case it records the witness on the original build,
//! which must hold the intended property with nonzero applicability, settle
//! quiescent with zero cleanup, and pair with the catalogue's build
//! identity. It then records the witness on the mutant, expecting the
//! intended violation with its trigger history. A violation must then
//! confirm in a fresh `zakura-sim confirm` process, and the confirmed trace
//! must replay Exact in another. It writes `sensitivity.json`.
//!
//! `discovery` checks out the fuzz revision, whose production source must
//! equal the witness revision's. For each target and seed it runs one
//! campaign on the original build, and for each case and seed one on the
//! mutant, with the catalogue's bounds, from the frozen empty corpus, each
//! in a fresh store. Paired campaigns differ only in their store, name and
//! build. A mutant finding of the intended property must confirm and replay
//! Exact in fresh processes. It writes `discovery.json`. A scored campaign
//! runs on a remote campaign host, never locally or in CI.
//!
//! Each result is exactly one class of the plan's table ([`Class`]). Each
//! report counts its results by class and fault class, and the
//! intended-detection rate's numerator and denominator.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::AtomicBool,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    apply, classify as planted_verdict, git, provenance, record, remove_worktree, Verdict,
};
use crate::{
    run_command,
    sim::SIM_RUSTFLAGS,
    sim_minimize::{run_child, ChildEnd},
    BoxError, UsageError,
};

mod classify;
#[cfg(test)]
mod tests;

use classify::{
    CampaignFacts, Class, Classified, DiscoveryRun, Recorded, SensitivityRun, Summary, TraceFacts,
};

/// The catalogue, relative to the repository root.
const CATALOGUE: &str = "qa/mutation-pilot/catalogue.json";

/// How long past its exploration budget a campaign's `start` may run, for
/// its startup, snapshot and end record, before the runner kills it.
const CAMPAIGN_GRACE: Duration = Duration::from_secs(300);

/// The file `zakura-sim confirm` writes into its `--out`.
const CONFIRMATION_FILE: &str = "confirmation.json";

/// The variable the fuzz target's filter reads. A discovery campaign runs
/// without it, so every property's violation counts.
const EXPECT_VAR: &str = "ZAKURA_SIM_EXPECT";

/// The sanitizer option variables a campaign's manifest records. A
/// discovery campaign runs without them.
const SANITIZER_VARS: &[&str] = &[
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "MSAN_OPTIONS",
    "TSAN_OPTIONS",
    "UBSAN_OPTIONS",
];

/// The witness trace's build fields a mutant shares with the original: all
/// but `dirty`, `source` and the binary's digest.
const WITNESS_FIELDS: &[&str] = &[
    "git",
    "lockfile",
    "rustc",
    "target",
    "cfg",
    "features",
    "opt_level",
    "sanitizer",
];

/// The fuzz build identity's fields a mutant shares with the original: all
/// but `dirty` and `source`.
const FUZZ_FIELDS: &[&str] = &[
    "target",
    "harness",
    "git",
    "lockfile",
    "fuzz_lockfile",
    "rustc",
    "cargo_fuzz",
    "libfuzzer_sys",
    "rustflags",
    "cargo_args",
];

/// Never set: the runner's children run to their end or their deadline.
static NO_INTERRUPT: AtomicBool = AtomicBool::new(false);

/// The parts of the catalogue the runner reads.
#[derive(Clone, Debug, Deserialize)]
struct Catalogue {
    /// The witness revision.
    source_revision: String,
    /// The witness build's identity, as its traces record it.
    baseline_build: Value,
    /// The fuzz builds' revision and identity.
    fuzz_build_inputs: FuzzInputs,
    /// The discovery bounds.
    discovery: Bounds,
    /// The confirmation bounds.
    confirmation: ConfirmationBounds,
    /// The reviewed cases.
    cases: Vec<Case>,
}

/// The catalogue's fuzz build pins.
#[derive(Clone, Debug, Deserialize)]
struct FuzzInputs {
    source_revision: String,
    source_sha256: String,
    lockfile_sha256: String,
    rustc: String,
    cargo_fuzz: String,
    libfuzzer_sys: String,
    rustflags: String,
    cargo_args: Vec<String>,
}

/// The catalogue's discovery bounds, shared by every paired campaign.
#[derive(Clone, Debug, Deserialize)]
struct Bounds {
    corpus: PathBuf,
    seeds: Vec<u32>,
    seconds_per_seed: u64,
    workers: usize,
    max_len: u64,
    input_timeout_seconds: u64,
    rss_limit_mb: u64,
    max_restarts: u64,
    abandon_limit: u64,
    feedback: String,
    sanitizer: String,
}

/// The catalogue's confirmation bounds.
#[derive(Clone, Debug, Deserialize)]
struct ConfirmationBounds {
    /// The longest one record, confirm or replay child may run.
    child_seconds: u64,
}

/// One reviewed case.
#[derive(Clone, Debug, Deserialize)]
struct Case {
    id: String,
    fault_class: String,
    patch: PathBuf,
    patch_sha256: String,
    /// Each production file the patch changes, with its hash before.
    production_files: BTreeMap<String, String>,
    property: String,
    harness: String,
    target: String,
    witness: PathBuf,
    witness_sha256: String,
    /// The review's argument that the mutation can't change behavior.
    /// Only review sets it; the runner never infers equivalence.
    #[serde(default)]
    equivalent: Option<String>,
}

/// What every report records about its run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Meta {
    /// `sensitivity` or `discovery`.
    experiment: String,
    /// The catalogue's SHA-256.
    catalogue_sha256: String,
    /// The revision the experiment built.
    built_revision: String,
    /// The runner's own commit.
    runner_revision: String,
    /// Cases the catalogue holds.
    reviewed_cases: usize,
}

/// A report: the run, its results, and their counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Report<T> {
    #[serde(flatten)]
    meta: Meta,
    cases: Vec<T>,
    summary: Summary,
}

/// One sensitivity case's result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SensitivityCase {
    id: String,
    fault_class: String,
    property: String,
    harness: String,
    result: Classified,
    /// The original build's witness trace.
    baseline: Option<TraceFacts>,
    /// The mutant's witness trace.
    mutant: Option<TraceFacts>,
    /// The case's artifacts, relative to `--out`.
    artifacts: PathBuf,
    /// Why the case's worktree or build couldn't be removed.
    cleanup: Option<String>,
}

/// One discovery case's results, one per seed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DiscoveryCase {
    id: String,
    fault_class: String,
    property: String,
    target: String,
    seeds: Vec<SeedResult>,
    /// Why the case's worktree or builds couldn't be removed.
    cleanup: Option<String>,
}

/// One seed's paired campaigns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SeedResult {
    seed: u32,
    result: Classified,
    baseline: Option<CampaignFacts>,
    mutant: Option<CampaignFacts>,
    /// The mutant campaign's store, relative to `--out`.
    store: Option<PathBuf>,
}

/// Runs `cargo xtask planted pilot <experiment> --out <dir> [--work <dir>]
/// [--only <case-id>]... [--seed <n>]...`.
pub(crate) fn run(
    repo_root: &Path,
    mut args: impl Iterator<Item = String>,
) -> Result<(), BoxError> {
    let experiment = args.next().ok_or(UsageError(
        "expected `sensitivity` or `discovery` after `planted pilot`",
    ))?;
    let (mut out, mut work, mut only, mut seeds) = (None, None, Vec::new(), Vec::new());
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--out" if out.is_none() => out = Some(PathBuf::from(value)),
            "--work" if work.is_none() => work = Some(PathBuf::from(value)),
            "--only" => only.push(value),
            "--seed" => seeds.push(value.parse::<u32>().map_err(|e| format!("--seed: {e}"))?),
            _ => return Err(format!("unknown or repeated flag `{flag}`").into()),
        }
    }
    let out = out.ok_or(UsageError("`planted pilot` needs --out <dir>"))?;
    let bytes = fs::read(repo_root.join(CATALOGUE))?;
    let catalogue: Catalogue = serde_json::from_slice(&bytes)?;
    let cases = select(&catalogue.cases, &only)?;
    let work = work.unwrap_or_else(|| {
        repo_root
            .join("target")
            .join("mutation-pilot")
            .join(&experiment)
    });
    let mut meta = Meta {
        experiment: experiment.clone(),
        catalogue_sha256: hex(&Sha256::digest(&bytes)),
        built_revision: String::new(),
        runner_revision: head(repo_root)?,
        reviewed_cases: catalogue.cases.len(),
    };
    let summary = match experiment.as_str() {
        "sensitivity" => {
            if !seeds.is_empty() {
                return Err(UsageError("--seed applies only to `discovery`").into());
            }
            fresh_dir(&out)?;
            meta.built_revision = catalogue.source_revision.clone();
            let rows = sensitivity(repo_root, &catalogue, &cases, &out, &work)?;
            write_report(&out, sensitivity_report(meta, rows)?)?
        }
        "discovery" => {
            let seeds = select_seeds(&catalogue.discovery.seeds, &seeds)?;
            fresh_dir(&out)?;
            meta.built_revision = catalogue.fuzz_build_inputs.source_revision.clone();
            let rows = discovery(repo_root, &catalogue, &cases, &seeds, &out, &work)?;
            write_report(&out, discovery_report(meta, rows)?)?
        }
        _ => {
            return Err(
                UsageError("expected `sensitivity` or `discovery` after `planted pilot`").into(),
            )
        }
    };
    println!(
        "{experiment}: {} of {} counted results are intended violations, over {} results; report in {}",
        summary.intended,
        summary.denominator,
        summary.results,
        out.join(format!("{experiment}.json")).display()
    );
    Ok(())
}

/// The cases `only` names, or every case without `only`. Fails on an ID
/// the catalogue doesn't hold, and on zero selected cases.
fn select<'a>(cases: &'a [Case], only: &[String]) -> Result<Vec<&'a Case>, String> {
    if let Some(unknown) = only
        .iter()
        .find(|id| !cases.iter().any(|case| &case.id == *id))
    {
        return Err(format!("--only {unknown} names no catalogue case"));
    }
    let selected: Vec<&Case> = cases
        .iter()
        .filter(|case| only.is_empty() || only.contains(&case.id))
        .collect();
    if selected.is_empty() {
        return Err("zero cases selected: the catalogue holds none".to_owned());
    }
    Ok(selected)
}

/// The seeds `only` names, or every catalogue seed without it. Fails on a
/// seed the catalogue doesn't allocate, and on zero seeds.
fn select_seeds(seeds: &[u32], only: &[u32]) -> Result<Vec<u32>, String> {
    if let Some(unknown) = only.iter().find(|seed| !seeds.contains(seed)) {
        return Err(format!("--seed {unknown} isn't a catalogue seed"));
    }
    let selected: Vec<u32> = seeds
        .iter()
        .copied()
        .filter(|seed| only.is_empty() || only.contains(seed))
        .collect();
    if selected.is_empty() {
        return Err("zero seeds selected: the catalogue allocates none".to_owned());
    }
    Ok(selected)
}

/// The sensitivity report over `cases`. Fails on zero cases.
fn sensitivity_report(
    meta: Meta,
    cases: Vec<SensitivityCase>,
) -> Result<Report<SensitivityCase>, String> {
    let summary = classify::summarize(
        cases
            .iter()
            .map(|case| (case.fault_class.as_str(), case.result.class)),
    )?;
    Ok(Report {
        meta,
        cases,
        summary,
    })
}

/// The discovery report over `cases`, counting each case's seeds. Fails on
/// zero results.
fn discovery_report(
    meta: Meta,
    cases: Vec<DiscoveryCase>,
) -> Result<Report<DiscoveryCase>, String> {
    let summary = classify::summarize(cases.iter().flat_map(|case| {
        case.seeds
            .iter()
            .map(|seed| (case.fault_class.as_str(), seed.result.class))
    }))?;
    Ok(Report {
        meta,
        cases,
        summary,
    })
}

/// Writes `report` to `<out>/<experiment>.json`, and returns its summary.
fn write_report<T: Serialize>(out: &Path, report: Report<T>) -> Result<Summary, BoxError> {
    let path = out.join(format!("{}.json", report.meta.experiment));
    fs::write(&path, serde_json::to_string_pretty(&report)? + "\n")?;
    Ok(report.summary)
}

/// Runs each case's sensitivity experiment, writing its artifacts under
/// `out` and building under `work`.
fn sensitivity(
    repo_root: &Path,
    catalogue: &Catalogue,
    cases: &[&Case],
    out: &Path,
    work: &Path,
) -> Result<Vec<SensitivityCase>, BoxError> {
    let toolchain = toolchain(&catalogue.baseline_build)?;
    let child = Duration::from_secs(catalogue.confirmation.child_seconds);
    let tree = work.join("baseline").join("worktree");
    add_worktree(repo_root, &tree, &catalogue.source_revision)?;
    let rows = (|| {
        check_pins(repo_root, &tree, cases)?;
        let built = build_sim(
            &tree,
            &work.join("baseline").join("target"),
            toolchain,
            &out.join("baseline-build.log"),
        )?;
        let mut rows = Vec::new();
        for case in cases {
            let row =
                sensitivity_case(repo_root, catalogue, case, &tree, &built, out, work, child)?;
            println!(
                "{}: {} — {}",
                row.id,
                class_name(row.result.class),
                row.result.reason
            );
            rows.push(row);
        }
        Ok::<_, BoxError>(rows)
    })();
    remove_worktree(repo_root, &tree)?;
    rows
}

/// Runs one case's sensitivity experiment with the original build `built`
/// in `baseline_tree`.
#[allow(clippy::too_many_arguments)]
fn sensitivity_case(
    repo_root: &Path,
    catalogue: &Catalogue,
    case: &Case,
    baseline_tree: &Path,
    built: &Result<PathBuf, String>,
    out: &Path,
    work: &Path,
    child: Duration,
) -> Result<SensitivityCase, BoxError> {
    let artifacts = Path::new("cases").join(&case.id);
    let dir = out.join(&artifacts);
    fs::create_dir_all(&dir)?;
    let mut run = SensitivityRun {
        equivalent: case.equivalent.clone(),
        ..SensitivityRun::default()
    };
    let mut cleanup = None;
    if run.equivalent.is_none() {
        run.baseline = Some(match built {
            Err(why) => Err(format!("the original build fails: {why}")),
            Ok(sim) => Ok(witness_run(
                sim,
                baseline_tree,
                case,
                catalogue,
                false,
                &dir.join("baseline"),
                child,
            )?),
        });
        if classify::sensitivity_baseline(&case.property, &run.baseline).is_none() {
            let tree = work.join("cases").join(&case.id).join("worktree");
            let target = work.join("cases").join(&case.id).join("target");
            add_worktree(repo_root, &tree, &catalogue.source_revision)?;
            let mutant = (|| {
                let sim = match prepare_patch(repo_root, &tree, case)? {
                    Err(why) => Err(why),
                    Ok(()) => build_sim(
                        &tree,
                        &target,
                        toolchain(&catalogue.baseline_build)?,
                        &dir.join("mutant-build.log"),
                    )?
                    .map_err(|why| format!("the mutant doesn't compile: {why}")),
                };
                run.patch = Some(sim.as_ref().map(|_| ()).map_err(Clone::clone));
                let Ok(sim) = sim else { return Ok(()) };
                let recorded = witness_run(
                    &sim,
                    &tree,
                    case,
                    catalogue,
                    true,
                    &dir.join("mutant"),
                    child,
                )?;
                if matches!(
                    planted_verdict(&case.property, &recorded.run),
                    Verdict::Caught(_)
                ) {
                    run.confirmation = Some(confirm_and_replay(
                        &sim,
                        &case.harness,
                        &tree.join(&case.witness),
                        &case.property,
                        &dir.join("confirmation"),
                        child,
                    )?);
                }
                run.mutant = Some(Ok(recorded));
                Ok::<_, BoxError>(())
            })();
            cleanup = clean(repo_root, &tree, &[&target]);
            mutant?;
        }
    }
    let result = classify::sensitivity(&case.property, &run);
    let trace = |recorded: &Option<Result<Recorded, String>>| {
        recorded
            .as_ref()
            .and_then(|recorded| recorded.as_ref().ok())
            .and_then(|recorded| recorded.trace.clone())
    };
    Ok(SensitivityCase {
        id: case.id.clone(),
        fault_class: case.fault_class.clone(),
        property: case.property.clone(),
        harness: case.harness.clone(),
        result,
        baseline: trace(&run.baseline),
        mutant: trace(&run.mutant),
        artifacts,
        cleanup,
    })
}

/// Records `case`'s witness with `sim` from `tree`, expecting the original
/// to hold and a mutant to violate, into `out`, and checks the trace's
/// build and input against the catalogue.
fn witness_run(
    sim: &Path,
    tree: &Path,
    case: &Case,
    catalogue: &Catalogue,
    mutated: bool,
    out: &Path,
    child: Duration,
) -> Result<Recorded, BoxError> {
    let expect = if mutated { "violated" } else { "holds" };
    let run = record(
        Command::new(sim),
        &case.harness,
        &tree.join(&case.witness),
        &case.property,
        expect,
        out,
        Some(child),
    )?;
    let trace = match fs::read_to_string(out.join("trace.jsonl")) {
        Ok(text) => Some(parse_trace(&text, &case.property)?),
        Err(_) => None,
    };
    let unpaired = trace.as_ref().and_then(|trace| {
        let mut why: Vec<String> = Vec::new();
        if trace.harness != case.harness {
            why.push(format!(
                "the trace ran {}, not {}",
                trace.harness, case.harness
            ));
        }
        if trace.input_sha256 != case.witness_sha256 {
            why.push("the trace's input isn't the catalogue's witness".to_owned());
        }
        why.extend(paired_identity(
            &catalogue.baseline_build,
            &trace.build,
            WITNESS_FIELDS,
            mutated,
        ));
        (!why.is_empty()).then(|| why.join("; "))
    });
    Ok(Recorded {
        run,
        trace,
        unpaired,
    })
}

/// Runs each case's discovery experiment on `seeds`, writing stores and
/// reports under `out` and building under `work`.
fn discovery(
    repo_root: &Path,
    catalogue: &Catalogue,
    cases: &[&Case],
    seeds: &[u32],
    out: &Path,
    work: &Path,
) -> Result<Vec<DiscoveryCase>, BoxError> {
    let fuzz = &catalogue.fuzz_build_inputs;
    let same_source = git(repo_root)
        .args([
            "diff",
            "--quiet",
            &catalogue.source_revision,
            &fuzz.source_revision,
            "--",
        ])
        .args(provenance::SOURCE_PATHS)
        .status()?;
    if !same_source.success() {
        return Err(format!(
            "the fuzz revision {} changes production source of the witness revision {}",
            fuzz.source_revision, catalogue.source_revision
        )
        .into());
    }
    let corpus = repo_root.join(&catalogue.discovery.corpus);
    check_empty_corpus(&corpus)?;
    let toolchain = toolchain(&catalogue.baseline_build)?;
    let child = Duration::from_secs(catalogue.confirmation.child_seconds);
    let tree = work.join("baseline").join("worktree");
    add_worktree(repo_root, &tree, &fuzz.source_revision)?;
    let rows = (|| {
        check_pins(repo_root, &tree, cases)?;
        let built = build_sim(
            &tree,
            &work.join("baseline").join("sim-target"),
            toolchain,
            &out.join("baseline-build.log"),
        )?;
        let mut baselines = BTreeMap::new();
        for case in cases.iter().filter(|case| case.equivalent.is_none()) {
            for seed in seeds {
                let key = (case.target.clone(), *seed);
                if baselines.contains_key(&key) {
                    continue;
                }
                let lane_dir = out
                    .join("baseline")
                    .join(&case.target)
                    .join(format!("seed-{seed}"));
                let facts = match &built {
                    Err(why) => Err(format!("the original simulator fails to build: {why}")),
                    Ok(sim) => campaign_lane(
                        sim,
                        &lane_dir,
                        &work.join("baseline").join("fuzz-target"),
                        case,
                        *seed,
                        catalogue,
                        &corpus,
                        false,
                    )?
                    .map_err(|error| error.to_string()),
                };
                baselines.insert(key, facts);
            }
        }
        let mut rows = Vec::new();
        for case in cases {
            let row = discovery_case(
                repo_root, catalogue, case, seeds, &baselines, &corpus, out, work, child,
            )?;
            for seed in &row.seeds {
                println!(
                    "{} seed {}: {} — {}",
                    row.id,
                    seed.seed,
                    class_name(seed.result.class),
                    seed.result.reason
                );
            }
            rows.push(row);
        }
        Ok::<_, BoxError>(rows)
    })();
    remove_worktree(repo_root, &tree)?;
    rows
}

/// Runs one case's mutant campaigns on `seeds`, beside the original's
/// `baselines` by target and seed.
#[allow(clippy::too_many_arguments)]
fn discovery_case(
    repo_root: &Path,
    catalogue: &Catalogue,
    case: &Case,
    seeds: &[u32],
    baselines: &BTreeMap<(String, u32), Result<CampaignFacts, String>>,
    corpus: &Path,
    out: &Path,
    work: &Path,
    child: Duration,
) -> Result<DiscoveryCase, BoxError> {
    let mut runs: Vec<(u32, DiscoveryRun, Option<PathBuf>)> = seeds
        .iter()
        .map(|seed| {
            let baseline = baselines.get(&(case.target.clone(), *seed)).cloned();
            let run = DiscoveryRun {
                equivalent: case.equivalent.clone(),
                baseline,
                ..DiscoveryRun::default()
            };
            (*seed, run, None)
        })
        .collect();
    let runnable = |run: &DiscoveryRun| {
        run.equivalent.is_none() && classify::discovery_baseline(&run.baseline).is_none()
    };
    let mut cleanup = None;
    if runs.iter().any(|(_, run, _)| runnable(run)) {
        let case_work = work.join("cases").join(&case.id);
        let (tree, sim_target, fuzz_target) = (
            case_work.join("worktree"),
            case_work.join("sim-target"),
            case_work.join("fuzz-target"),
        );
        add_worktree(
            repo_root,
            &tree,
            &catalogue.fuzz_build_inputs.source_revision,
        )?;
        let ran = (|| {
            let dir = out.join("cases").join(&case.id);
            fs::create_dir_all(&dir)?;
            let mut patch = match prepare_patch(repo_root, &tree, case)? {
                Err(why) => Err(why),
                Ok(()) => build_sim(
                    &tree,
                    &sim_target,
                    toolchain(&catalogue.baseline_build)?,
                    &dir.join("mutant-build.log"),
                )?
                .map_err(|why| format!("the mutant simulator doesn't compile: {why}")),
            };
            for (seed, run, store) in runs.iter_mut().filter(|(_, run, _)| runnable(run)) {
                let sim = match &patch {
                    Err(why) => {
                        run.patch = Some(Err(why.clone()));
                        continue;
                    }
                    Ok(sim) => sim.clone(),
                };
                let lane_dir = dir.join(format!("seed-{seed}"));
                match campaign_lane(
                    &sim,
                    &lane_dir,
                    &fuzz_target,
                    case,
                    *seed,
                    catalogue,
                    corpus,
                    true,
                )? {
                    Err(LaneError::Build(why)) => {
                        let why = format!("the mutant fuzz target doesn't compile: {why}");
                        run.patch = Some(Err(why.clone()));
                        patch = Err(why);
                    }
                    Err(LaneError::Run(why)) => {
                        run.patch = Some(Ok(()));
                        run.mutant = Some(Err(why));
                    }
                    Ok(facts) => {
                        run.patch = Some(Ok(()));
                        if let Some(input) = &facts.intended_input {
                            run.confirmation = Some(confirm_and_replay(
                                &sim,
                                &case.harness,
                                &lane_dir.join("store").join(input),
                                &case.property,
                                &lane_dir.join("confirmation"),
                                child,
                            )?);
                        }
                        run.mutant = Some(Ok(facts));
                        *store = lane_dir
                            .join("store")
                            .strip_prefix(out)
                            .ok()
                            .map(Path::to_path_buf);
                    }
                }
            }
            Ok::<_, BoxError>(())
        })();
        cleanup = clean(repo_root, &tree, &[&sim_target, &fuzz_target]);
        ran?;
    }
    let ok = |lane: &Option<Result<CampaignFacts, String>>| {
        lane.as_ref().and_then(|lane| lane.as_ref().ok()).cloned()
    };
    Ok(DiscoveryCase {
        id: case.id.clone(),
        fault_class: case.fault_class.clone(),
        property: case.property.clone(),
        target: case.target.clone(),
        seeds: runs
            .into_iter()
            .map(|(seed, run, store)| SeedResult {
                seed,
                result: classify::discovery(&case.property, &run),
                baseline: ok(&run.baseline),
                mutant: ok(&run.mutant),
                store,
            })
            .collect(),
        cleanup,
    })
}

/// Why a campaign lane has no facts.
#[derive(Debug)]
enum LaneError {
    /// The fuzz target didn't build.
    Build(String),
    /// The campaign didn't leave the records the runner reads.
    Run(String),
}

impl std::fmt::Display for LaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneError::Build(why) => write!(f, "the fuzz target fails to build: {why}"),
            LaneError::Run(why) => f.write_str(why),
        }
    }
}

/// Builds `case`'s fuzz target with `sim` in `fuzz_target`, and runs one
/// campaign on it with the catalogue's bounds and `seed`, from `corpus`, in
/// a fresh store under `dir`. Reads what the store's records show.
#[allow(clippy::too_many_arguments)]
fn campaign_lane(
    sim: &Path,
    dir: &Path,
    fuzz_target: &Path,
    case: &Case,
    seed: u32,
    catalogue: &Catalogue,
    corpus: &Path,
    mutated: bool,
) -> Result<Result<CampaignFacts, LaneError>, BoxError> {
    let store = dir.join("store");
    if store.exists() {
        return Err(format!("{} isn't a fresh store", store.display()).into());
    }
    fs::create_dir_all(dir)?;
    let bounds = &catalogue.discovery;
    let mut build = Command::new(sim);
    build
        .args(["campaign", "build", "--store"])
        .arg(&store)
        .args([
            "--target",
            &case.target,
            "--sanitizer",
            &bounds.sanitizer,
            "--target-dir",
        ])
        .arg(fuzz_target);
    let log = dir.join("build.log");
    match run_child(&mut build, &log, None, &NO_INTERRUPT)? {
        ChildEnd::Exited(0) => {}
        end => {
            return Ok(Err(LaneError::Build(format!(
                "`zakura-sim campaign build` {end}; see {}",
                log.display()
            ))))
        }
    }
    let Some(build_digest) = only_entry(&store.join("builds"))? else {
        return Ok(Err(LaneError::Run(
            "the store holds no single build".to_owned(),
        )));
    };
    let name =
        format!("{}-{}", if mutated { &case.id } else { &case.target }, seed).replace('/', "-");
    let mut start = Command::new(sim);
    start.args(start_args(
        bounds,
        &name,
        &case.harness,
        &build_digest,
        seed,
        corpus,
        &store,
    ));
    start.env_remove(EXPECT_VAR);
    for var in SANITIZER_VARS {
        start.env_remove(var);
    }
    let log = dir.join("start.log");
    let deadline = Duration::from_secs(bounds.seconds_per_seed) + CAMPAIGN_GRACE;
    // `start` exits 1 for any run that isn't complete, as a run a finding
    // stopped, so the store's records decide.
    if run_child(&mut start, &log, Some(deadline), &NO_INTERRUPT)? == ChildEnd::TimedOut {
        return Ok(Err(LaneError::Run(format!(
            "`zakura-sim campaign start` ran past {} s; see {}",
            deadline.as_secs(),
            log.display()
        ))));
    }
    let Some(manifest_file) = only_entry(&store.join("manifests"))? else {
        return Ok(Err(LaneError::Run(format!(
            "the store holds no single run manifest; see {}",
            log.display()
        ))));
    };
    let run = manifest_file.trim_end_matches(".json").to_owned();
    let manifest: Value =
        serde_json::from_slice(&fs::read(store.join("manifests").join(&manifest_file))?)?;
    let initial = manifest
        .get("initial_snapshot")
        .and_then(Value::as_str)
        .ok_or("the manifest names no initial snapshot")?;
    let snapshot: Value = serde_json::from_slice(&fs::read(
        store.join("snapshots").join(initial).join("snapshot.json"),
    )?)?;
    let initial_inputs = snapshot
        .get("inputs")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let end: Option<Value> = fs::read(store.join("runs").join(&run).join("end.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let evidence_file = dir.join("evidence.json");
    let mut evidence = Command::new(sim);
    evidence
        .args(["campaign", "evidence", "--store"])
        .arg(&store)
        .args(["--run", &run, "--out"])
        .arg(&evidence_file);
    let log = dir.join("evidence.log");
    let evidence = match run_child(&mut evidence, &log, None, &NO_INTERRUPT)? {
        ChildEnd::Exited(0) => serde_json::from_slice::<Value>(&fs::read(&evidence_file)?)?,
        end => {
            return Ok(Err(LaneError::Run(format!(
                "`zakura-sim campaign evidence` {end}; see {}",
                log.display()
            ))))
        }
    };
    let mut facts = campaign_facts(&case.property, end.as_ref(), &evidence, initial_inputs);
    let expected = expected_fuzz_identity(catalogue, case);
    let actual = manifest.pointer("/build/identity").unwrap_or(&Value::Null);
    facts.unpaired = paired_identity(&expected, actual, FUZZ_FIELDS, mutated);
    Ok(Ok(facts))
}

/// `campaign start`'s arguments for one lane: every bound and the seed come
/// from the catalogue, so paired campaigns differ only in `store`, `name`
/// and `build`.
fn start_args(
    bounds: &Bounds,
    name: &str,
    harness: &str,
    build: &str,
    seed: u32,
    corpus: &Path,
    store: &Path,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = ["campaign", "start", "--store"]
        .into_iter()
        .map(OsString::from)
        .collect();
    args.push(store.into());
    for (flag, value) in [
        ("--campaign", name.to_owned()),
        ("--sim", harness.to_owned()),
        ("--build", build.to_owned()),
        ("--seconds", bounds.seconds_per_seed.to_string()),
        ("--workers", bounds.workers.to_string()),
        ("--seed", seed.to_string()),
        ("--max-len", bounds.max_len.to_string()),
        ("--timeout", bounds.input_timeout_seconds.to_string()),
        ("--rss-limit-mb", bounds.rss_limit_mb.to_string()),
        ("--max-restarts", bounds.max_restarts.to_string()),
        ("--abandon-limit", bounds.abandon_limit.to_string()),
        ("--feedback", bounds.feedback.clone()),
    ] {
        args.push(flag.into());
        args.push(value.into());
    }
    args.push("--fixtures".into());
    args.push(corpus.into());
    args
}

/// The fuzz build identity the catalogue pins for `case`'s target, with
/// the field names a manifest's build identity uses.
fn expected_fuzz_identity(catalogue: &Catalogue, case: &Case) -> Value {
    let fuzz = &catalogue.fuzz_build_inputs;
    serde_json::json!({
        "target": case.target,
        "harness": case.harness,
        "git": fuzz.source_revision,
        "source": fuzz.source_sha256,
        "lockfile": catalogue.baseline_build.get("lockfile"),
        "fuzz_lockfile": fuzz.lockfile_sha256,
        "rustc": fuzz.rustc,
        "cargo_fuzz": fuzz.cargo_fuzz,
        "libfuzzer_sys": fuzz.libfuzzer_sys,
        "rustflags": fuzz.rustflags,
        "cargo_args": fuzz.cargo_args,
    })
}

/// Why `actual`, a build identity, doesn't pair with `expected`, the
/// original's: every one of `fields` must match. The original must also be
/// clean and match `expected`'s source digest, and a mutant must be dirty
/// with another source digest. `None` if it pairs.
fn paired_identity(
    expected: &Value,
    actual: &Value,
    fields: &[&str],
    mutated: bool,
) -> Option<String> {
    let mut differ: Vec<&str> = fields
        .iter()
        .copied()
        .filter(|field| expected.get(field) != actual.get(field))
        .collect();
    let same_source =
        expected.get("source").is_some() && expected.get("source") == actual.get("source");
    if same_source == mutated {
        differ.push("source");
    }
    if actual.get("dirty") != Some(&Value::Bool(mutated)) {
        differ.push("dirty");
    }
    (!differ.is_empty()).then(|| {
        format!(
            "the {} build doesn't pair with the catalogue's: {} differ",
            if mutated { "mutant" } else { "original" },
            differ.join(", ")
        )
    })
}

/// What a saved failure file is, from its path in the store.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Failure {
    /// An input saved as a violation of this property, under an
    /// `active`, `known` or `draft` bucket.
    Violation(String),
    /// libFuzzer's crash input, which a saved violation may explain.
    Crash,
    /// Any other failure: a harness failure, crash, abandoned run,
    /// nondeterministic rerun or run without an end that the target saved,
    /// and a libFuzzer timeout, OOM or leak.
    Other,
    /// A file that goes with a saved input, such as its trace.
    Companion,
}

/// What the store file at `path` is. A worker's target saves inputs as
/// `.../sim/inputs/<bucket>/[<property>/]<sha256>.bin`, and libFuzzer saves
/// `.../artifacts/<kind>-<sha1>`.
fn failure_kind(path: &str) -> Failure {
    let segments: Vec<&str> = path.split('/').collect();
    let inputs = segments
        .windows(2)
        .position(|pair| matches!(pair[0], "sim" | "compact-sim") && pair[1] == "inputs");
    if let Some(at) = inputs {
        return match &segments[at + 2..] {
            [bucket, property @ .., _input]
                if matches!(*bucket, "active" | "known" | "draft") && !property.is_empty() =>
            {
                Failure::Violation(property.join("/"))
            }
            _ => Failure::Other,
        };
    }
    let artifact = segments
        .iter()
        .rev()
        .nth(1)
        .is_some_and(|dir| matches!(*dir, "artifacts" | "compact-artifacts"));
    match segments.last() {
        Some(file) if artifact && file.starts_with("crash-") => Failure::Crash,
        Some(_) if artifact => Failure::Other,
        _ => Failure::Companion,
    }
}

/// What a campaign's end record `end` and `evidence` show about
/// `property`, for a run whose initial snapshot held `initial_inputs`.
fn campaign_facts(
    property: &str,
    end: Option<&Value>,
    evidence: &Value,
    initial_inputs: usize,
) -> CampaignFacts {
    let mut violated = BTreeSet::new();
    let (mut failures, mut crashes, mut intended_input) = (Vec::new(), Vec::new(), None);
    let records = end
        .and_then(|end| end.get("failures"))
        .and_then(Value::as_array);
    for path in records
        .into_iter()
        .flatten()
        .filter_map(|record| record.get("path").and_then(Value::as_str))
    {
        match failure_kind(path) {
            Failure::Violation(violation) => {
                if violation == property && intended_input.is_none() {
                    intended_input = Some(path.to_owned());
                }
                violated.insert(violation);
            }
            Failure::Crash => crashes.push(path.to_owned()),
            Failure::Other => failures.push(path.to_owned()),
            Failure::Companion => {}
        }
    }
    if violated.is_empty() {
        failures.extend(crashes);
    }
    let counts = |totals: &str, field: &str| {
        evidence
            .pointer(&format!("/{totals}/properties"))
            .and_then(|properties| properties.get(property))
            .and_then(|counts| counts.get(field))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    CampaignFacts {
        status: evidence
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("none")
            .to_owned(),
        initial_inputs,
        violated: violated.into_iter().collect(),
        failures,
        incomplete: evidence
            .get("incomplete")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(Value::to_string)
            .collect(),
        applied: evidence
            .get("unapplied")
            .and_then(Value::as_array)
            .is_some_and(|unapplied| !unapplied.iter().any(|id| id.as_str() == Some(property))),
        runs: evidence
            .pointer("/totals/runs")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        applicable_runs: counts("complete_totals", "held") + counts("complete_totals", "violated"),
        millis: end
            .and_then(|end| end.get("workers"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|worker| worker.get("millis").and_then(Value::as_u64))
            .sum(),
        intended_input,
        unpaired: None,
    }
}

/// Confirms `input`'s violation of `property` on `harness` with `sim` in a
/// fresh process, then replays the confirmed trace in another, writing
/// under `dir`. Each child gets `child`.
fn confirm_and_replay(
    sim: &Path,
    harness: &str,
    input: &Path,
    property: &str,
    dir: &Path,
    child: Duration,
) -> Result<Result<(), String>, BoxError> {
    let confirm_dir = dir.join("confirm");
    let mut confirm = Command::new(sim);
    confirm
        .args(["confirm", "--sim", harness, "--input"])
        .arg(input)
        .args(["--property", property, "--out"])
        .arg(&confirm_dir);
    let ended = run_child(
        &mut confirm,
        &dir.join("confirm.log"),
        Some(child),
        &NO_INTERRUPT,
    )?;
    let confirmation = fs::read(confirm_dir.join(CONFIRMATION_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let trace = match confirmed_trace(ended, confirmation.as_ref()) {
        Ok(trace) => confirm_dir.join(trace),
        Err(why) => return Ok(Err(why)),
    };
    let log = dir.join("replay.log");
    let mut replay = Command::new(sim);
    replay.arg("replay").arg(&trace);
    Ok(
        match run_child(&mut replay, &log, Some(child), &NO_INTERRUPT)? {
            ChildEnd::Exited(0) => Ok(()),
            end => Err(format!(
                "the confirmed trace doesn't replay Exact: `zakura-sim replay` {end}; see {}",
                log.display()
            )),
        },
    )
}

/// The confirmed trace's directory, relative to the confirmation's, from a
/// `zakura-sim confirm` that ended `ended` and wrote `confirmation`, or why
/// it didn't confirm.
fn confirmed_trace(ended: ChildEnd, confirmation: Option<&Value>) -> Result<PathBuf, String> {
    let Some(confirmation) = confirmation else {
        return Err(format!(
            "`zakura-sim confirm` {ended} without a confirmation"
        ));
    };
    let status = confirmation
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let detail = confirmation
        .get("detail")
        .and_then(Value::as_str)
        .map_or_else(String::new, |detail| format!(": {detail}"));
    match status {
        "confirmed" if ended == ChildEnd::Exited(0) => {}
        "witness-lost" | "witness-unsupported" => {
            return Err(format!(
                "missing witness: the fresh confirmation is {status}{detail}"
            ))
        }
        status => {
            return Err(format!(
                "the fresh confirmation is {status}{detail}; `zakura-sim confirm` {ended}"
            ))
        }
    }
    confirmation
        .get("trace")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| "the confirmation names no trace".to_owned())
}

/// Reads a trace's `meta` and `end` records for `property`.
fn parse_trace(text: &str, property: &str) -> Result<TraceFacts, BoxError> {
    let record = |line: Option<&str>, key: &str| -> Result<Value, BoxError> {
        let line: Value = serde_json::from_str(line.ok_or("the trace is empty")?)?;
        line.get(key)
            .cloned()
            .ok_or_else(|| format!("the trace has no {key} record where it should").into())
    };
    let meta = record(text.lines().next(), "meta")?;
    let end = record(text.lines().next_back(), "end")?;
    let text_at = |value: &Value, pointer: &str| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let applicable = end
        .get("properties")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|report| report.get("property").and_then(Value::as_str) == Some(property))
        .and_then(|report| report.pointer("/outcome/held/applicable"))
        .and_then(Value::as_u64);
    Ok(TraceFacts {
        build: meta.get("build").cloned().unwrap_or(Value::Null),
        harness: text_at(&meta, "/sim"),
        input_sha256: text_at(&meta, "/input/sha256"),
        steps: end.get("steps").and_then(Value::as_u64).unwrap_or(0),
        status: end.get("status").and_then(Value::as_str).map(str::to_owned),
        cleanup: end
            .get("cleanup")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(name, n)| (name.clone(), n.as_u64().unwrap_or(0)))
            .collect(),
        applicable,
    })
}

/// Applies `case`'s patch, from `repo_root`, in `tree`, and checks that it
/// changed exactly the case's declared production files. `Err` says why
/// the mutant isn't valid.
fn prepare_patch(
    repo_root: &Path,
    tree: &Path,
    case: &Case,
) -> Result<Result<(), String>, BoxError> {
    if let Err(why) = apply(tree, &repo_root.join(&case.patch)) {
        return Ok(Err(format!("the patch doesn't apply: {why}")));
    }
    let mut changed = BTreeSet::new();
    for args in [
        &["diff", "--name-only", "-z", "HEAD", "--"][..],
        &["ls-files", "--others", "--exclude-standard", "-z"][..],
    ] {
        let output = git(tree).args(args).output()?;
        if !output.status.success() {
            return Err(format!("`git {}` failed in {}", args.join(" "), tree.display()).into());
        }
        changed.extend(
            String::from_utf8(output.stdout)?
                .split_terminator('\0')
                .map(str::to_owned),
        );
    }
    Ok(match undeclared(&changed, &case.production_files) {
        Some(why) => Err(why),
        None => Ok(()),
    })
}

/// Why `changed` isn't exactly the `declared` production files, or `None`
/// if it is.
fn undeclared(changed: &BTreeSet<String>, declared: &BTreeMap<String, String>) -> Option<String> {
    let extra: Vec<&str> = changed
        .iter()
        .filter(|path| !declared.contains_key(*path))
        .map(String::as_str)
        .collect();
    let missing: Vec<&str> = declared
        .keys()
        .filter(|path| !changed.contains(*path))
        .map(String::as_str)
        .collect();
    match (extra.as_slice(), missing.as_slice()) {
        ([], []) => None,
        (extra, missing) => Some(format!(
            "the patch changes undeclared files [{}] and leaves declared files [{}]",
            extra.join(", "),
            missing.join(", ")
        )),
    }
}

/// Checks each case's frozen inputs: its patch in `repo_root`, and its
/// witness input and production files in `tree` before any patch, against
/// the catalogue's hashes.
fn check_pins(repo_root: &Path, tree: &Path, cases: &[&Case]) -> Result<(), BoxError> {
    let mut differ = Vec::new();
    for case in cases {
        let mut pins = vec![
            (repo_root.join(&case.patch), &case.patch_sha256),
            (tree.join(&case.witness), &case.witness_sha256),
        ];
        pins.extend(
            case.production_files
                .iter()
                .map(|(path, sha256)| (tree.join(path), sha256)),
        );
        for (path, expected) in pins {
            if file_sha256(&path).ok().as_ref() != Some(expected) {
                differ.push(format!("{}: {}", case.id, path.display()));
            }
        }
    }
    if differ.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "frozen inputs differ from the catalogue: {}",
            differ.join(", ")
        )
        .into())
    }
}

/// Fails unless `corpus` is a directory with no seed input or sidecar.
fn check_empty_corpus(corpus: &Path) -> Result<(), BoxError> {
    if !corpus.is_dir() {
        return Err(format!("the frozen corpus {} isn't a directory", corpus.display()).into());
    }
    for entry in fs::read_dir(corpus)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "bin" || extension == "toml")
        {
            return Err(format!("the frozen empty corpus holds {}", path.display()).into());
        }
    }
    Ok(())
}

/// The rustup toolchain the catalogue's witness build names in its `rustc`
/// version, such as `1.99.0`.
fn toolchain(build: &Value) -> Result<&str, String> {
    build
        .get("rustc")
        .and_then(Value::as_str)
        .and_then(|rustc| rustc.split_whitespace().nth(1))
        .ok_or_else(|| "the catalogue's witness build names no rustc version".to_owned())
}

/// Builds `tree`'s `zakura-sim` binary in `target` with `toolchain` and the
/// catalogue's witness build flags: the dev profile without debug
/// information or incremental compilation, and the simulator's flag. The
/// binary's path, or why the build failed.
fn build_sim(
    tree: &Path,
    target: &Path,
    toolchain: &str,
    log: &Path,
) -> Result<Result<PathBuf, String>, BoxError> {
    let mut cargo = Command::new("rustup");
    cargo
        .args(["run", toolchain, "cargo", "build", "--locked"])
        .args(["-p", "zakura-sim", "--bin", "zakura-sim"])
        .env("RUSTFLAGS", SIM_RUSTFLAGS)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .env("CARGO_INCREMENTAL", "0")
        .env_remove("RUSTC")
        .current_dir(tree);
    println!(
        "building zakura-sim in {}; log in {}",
        tree.display(),
        log.display()
    );
    Ok(match run_child(&mut cargo, log, None, &NO_INTERRUPT)? {
        ChildEnd::Exited(0) => Ok(target.join("debug").join("zakura-sim")),
        end => Err(format!("`cargo build` {end}; see {}", log.display())),
    })
}

/// Checks out `rev` in a new detached worktree at `path`, replacing any
/// earlier one there.
fn add_worktree(repo_root: &Path, path: &Path, rev: &str) -> Result<(), BoxError> {
    remove_worktree(repo_root, path)?;
    run_command(git(repo_root).args(["worktree", "prune"]))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    run_command(
        git(repo_root)
            .args(["worktree", "add", "--detach"])
            .arg(path)
            .arg(rev),
    )
}

/// Removes the worktree `tree` and the build directories `builds`, and
/// says what couldn't be removed.
fn clean(repo_root: &Path, tree: &Path, builds: &[&Path]) -> Option<String> {
    let mut failed = Vec::new();
    if let Err(error) = remove_worktree(repo_root, tree) {
        failed.push(format!("{}: {error}", tree.display()));
    }
    for build in builds {
        if build.exists() {
            if let Err(error) = fs::remove_dir_all(build) {
                failed.push(format!("{}: {error}", build.display()));
            }
        }
    }
    (!failed.is_empty()).then(|| failed.join("; "))
}

/// The name of the only entry in `dir`, or `None` if it holds none or
/// several.
fn only_entry(dir: &Path) -> Result<Option<String>, BoxError> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(None);
    };
    let names = entries
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<String>, std::io::Error>>()?;
    Ok(match names.as_slice() {
        [name] => Some(name.clone()),
        _ => None,
    })
}

/// Creates `dir`, or fails if it already holds anything: a run never
/// overwrites an earlier result.
fn fresh_dir(dir: &Path) -> Result<(), BoxError> {
    if dir.exists() && fs::read_dir(dir)?.next().is_some() {
        return Err(format!(
            "{} already holds results; a run never overwrites an earlier one",
            dir.display()
        )
        .into());
    }
    fs::create_dir_all(dir)?;
    Ok(())
}

/// `HEAD`'s commit in `repo_root`.
fn head(repo_root: &Path) -> Result<String, BoxError> {
    let output = git(repo_root).args(["rev-parse", "HEAD"]).output()?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// The lowercase hex SHA-256 of `path`'s contents.
fn file_sha256(path: &Path) -> Result<String, BoxError> {
    Ok(hex(&Sha256::digest(fs::read(path)?)))
}

/// `bytes` in lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `class`'s name, as the report writes it.
fn class_name(class: Class) -> String {
    serde_json::to_value(class)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}
