//! `cargo xtask planted pilot (sensitivity|discovery)`: the mutation pilot's
//! paired runner (mutation plan, mut-3).
//!
//! Both experiments read the reviewed catalogue,
//! `qa/mutation-pilot/catalogue.json`, and check its pins before anything
//! runs: each selected case's patch, witness input and production files
//! must match the catalogue's hashes at the pinned revision. Every pilot
//! input comes from the pinned revision's checkout, never from the tree the
//! pilot runs from: the patches, witnesses, production files and the empty
//! corpus. Only the catalogue itself comes from that tree. Each builds the
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
//! runs on a remote campaign host, never locally or in CI. A case whose
//! catalogue entry sets `discovery_excluded` runs no campaign. The report
//! lists it with that reason, outside its counts.
//!
//! Each result is exactly one class of the plan's table ([`Class`]). Each
//! report counts its results by class and fault class, and the
//! intended-detection rate's numerator and denominator.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    apply, classify as planted_verdict, git, provenance, record, remove_worktree, Expect, Located,
    Run, Verdict,
};
use crate::{
    run_command,
    sim::SIM_RUSTFLAGS,
    sim_minimize::{install_interrupt_handler, ChildEnd, INTERRUPTED},
    BoxError, UsageError,
};

mod classify;
#[cfg(test)]
mod tests;

use super::lane::{
    self, confirm_and_replay, run_lane_child, sim_build_failure, BuildFailure, CampaignBounds,
    CampaignFacts, Confirm, LaneError, LaneSpec,
};
#[cfg(test)]
use super::lane::{
    campaign_facts, confirmed_trace, failure_kind, fuzz_build_failure, start_args,
    stop_on_interrupt, ConfirmedTrace, Failure,
};
use classify::{
    Class, Classified, DiscoveryRun, NoCampaign, Recorded, SensitivityRun, Summary, TraceFacts,
};

/// The catalogue, relative to the repository root.
const CATALOGUE: &str = "qa/mutation-pilot/catalogue.json";

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

/// The error that stops the pilot after SIGINT, SIGTERM or SIGHUP.
pub(super) const INTERRUPTED_ERROR: &str =
    "interrupted: the running child's process group was killed";

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
    /// What each campaign runs with.
    #[serde(flatten)]
    campaign: CampaignBounds,
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
    /// The review's reason that discovery leaves this case out, such as a
    /// target whose property never applied in calibration. Only review sets
    /// it. Sensitivity still runs the case.
    #[serde(default)]
    discovery_excluded: Option<String>,
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
    /// Selected cases that the experiment left out, which the summary
    /// doesn't count.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    excluded: Vec<Excluded>,
    summary: Summary,
}

/// A selected case that discovery left out, and the review's reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Excluded {
    id: String,
    fault_class: String,
    reason: String,
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
    let Flags {
        out,
        work,
        only,
        seeds,
    } = parse_flags(args)?;
    // Each child runs in its own process group, which a terminal's signal
    // doesn't reach. The handler lets the running child's group be killed.
    install_interrupt_handler();
    let bytes = fs::read(repo_root.join(CATALOGUE))?;
    let catalogue: Catalogue = serde_json::from_slice(&bytes)?;
    check_exclusions(&catalogue.cases)?;
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
            check_interrupt(&INTERRUPTED)?;
            write_report(&out, sensitivity_report(meta, rows)?)?
        }
        "discovery" => {
            let seeds = select_seeds(&catalogue.discovery.seeds, &seeds)?;
            let (scored, excluded) = discovery_cases(&cases)?;
            fresh_dir(&out)?;
            meta.built_revision = catalogue.fuzz_build_inputs.source_revision.clone();
            let rows = discovery(repo_root, &catalogue, &scored, &seeds, &out, &work)?;
            check_interrupt(&INTERRUPTED)?;
            write_report(&out, discovery_report(meta, rows, excluded)?)?
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

/// The flags after the experiment's name.
#[derive(Debug, PartialEq, Eq)]
struct Flags {
    /// `--out`, absolute.
    out: PathBuf,
    /// `--work`, absolute, if given.
    work: Option<PathBuf>,
    /// Each `--only` case ID.
    only: Vec<String>,
    /// Each `--seed`.
    seeds: Vec<u32>,
}

/// Reads the flags in `args`. It makes `--out` and `--work` absolute
/// against the current directory, since the builds run cargo from their
/// worktree, and cargo would read a relative `CARGO_TARGET_DIR` there.
fn parse_flags(mut args: impl Iterator<Item = String>) -> Result<Flags, BoxError> {
    let (mut out, mut work, mut only, mut seeds) = (None, None, Vec::new(), Vec::new());
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--out" if out.is_none() => out = Some(std::path::absolute(value)?),
            "--work" if work.is_none() => work = Some(std::path::absolute(value)?),
            "--only" => only.push(value),
            "--seed" => seeds.push(value.parse::<u32>().map_err(|e| format!("--seed: {e}"))?),
            _ => return Err(format!("unknown or repeated flag `{flag}`").into()),
        }
    }
    Ok(Flags {
        out: out.ok_or(UsageError("`planted pilot` needs --out <dir>"))?,
        work,
        only,
        seeds,
    })
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

/// Fails if a case sets `discovery_excluded` without a reason, as
/// `scripts/check_mutation_pilot.py` does.
fn check_exclusions(cases: &[Case]) -> Result<(), String> {
    match cases.iter().find(|case| {
        case.discovery_excluded
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
    }) {
        Some(case) => Err(format!("{}: discovery exclusion needs a reason", case.id)),
        None => Ok(()),
    }
}

/// Splits the selected `cases` into those discovery runs and those whose
/// catalogue entry excludes them. Fails if it would run none.
fn discovery_cases<'a>(cases: &[&'a Case]) -> Result<(Vec<&'a Case>, Vec<Excluded>), String> {
    let (excluded, scored): (Vec<&Case>, Vec<&Case>) = cases
        .iter()
        .partition(|case| case.discovery_excluded.is_some());
    if scored.is_empty() {
        return Err("zero cases selected for discovery: each selected case is excluded".to_owned());
    }
    let excluded = excluded
        .into_iter()
        .map(|case| Excluded {
            id: case.id.clone(),
            fault_class: case.fault_class.clone(),
            reason: case.discovery_excluded.clone().unwrap_or_default(),
        })
        .collect();
    Ok((scored, excluded))
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
        excluded: Vec::new(),
        summary,
    })
}

/// The discovery report over `cases`, counting each case's seeds, and
/// listing the `excluded` cases outside the counts. Fails on zero results.
fn discovery_report(
    meta: Meta,
    cases: Vec<DiscoveryCase>,
    excluded: Vec<Excluded>,
) -> Result<Report<DiscoveryCase>, String> {
    let summary = classify::summarize(cases.iter().flat_map(|case| {
        case.seeds
            .iter()
            .map(|seed| (case.fault_class.as_str(), seed.result.class))
    }))?;
    Ok(Report {
        meta,
        cases,
        excluded,
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
        check_pins(&tree, &catalogue.source_revision, cases)?;
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
    built: &Result<PathBuf, BuildFailure>,
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
        match built {
            Err(BuildFailure::Broken(why)) => {
                run.broken_build = Some(format!("the original build broke: {why}"));
            }
            Err(BuildFailure::Rejected(why)) => {
                run.baseline = Some(Err(format!("the original doesn't compile: {why}")));
            }
            Ok(sim) => {
                run.baseline = Some(Ok(witness_run(
                    sim,
                    baseline_tree,
                    case,
                    catalogue,
                    false,
                    &dir.join("baseline"),
                    child,
                    &INTERRUPTED,
                )?));
            }
        }
        if classify::sensitivity_baseline(&case.property, &run.baseline).is_none() {
            let tree = work.join("cases").join(&case.id).join("worktree");
            let target = work.join("cases").join(&case.id).join("target");
            add_worktree(repo_root, &tree, &catalogue.source_revision)?;
            let mutant = (|| {
                if let Err(why) = prepare_patch(&tree, case)? {
                    run.patch = Some(Err(why));
                    return Ok(());
                }
                let built = build_sim(
                    &tree,
                    &target,
                    toolchain(&catalogue.baseline_build)?,
                    &dir.join("mutant-build.log"),
                )?;
                let sim = match built {
                    Err(BuildFailure::Rejected(why)) => {
                        run.patch = Some(Err(format!("the mutant doesn't compile: {why}")));
                        return Ok(());
                    }
                    Err(BuildFailure::Broken(why)) => {
                        run.broken_build = Some(format!("the mutant build broke: {why}"));
                        return Ok(());
                    }
                    Ok(sim) => sim,
                };
                run.patch = Some(Ok(()));
                let recorded = witness_run(
                    &sim,
                    &tree,
                    case,
                    catalogue,
                    true,
                    &dir.join("mutant"),
                    child,
                    &INTERRUPTED,
                )?;
                if matches!(
                    planted_verdict(&case.property, &[], &recorded.run),
                    Verdict::Caught(_)
                ) {
                    run.confirmation = Some(
                        confirm_and_replay(
                            &|| Command::new(&sim),
                            &case.harness,
                            &tree.join(&case.witness),
                            &case.property,
                            &[],
                            Confirm::Witness,
                            &dir.join("confirmation"),
                            child,
                        )?
                        .map(drop),
                    );
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
/// build and input against the catalogue. `interrupt` kills the run and
/// stops the pilot.
#[allow(clippy::too_many_arguments)]
fn witness_run(
    sim: &Path,
    tree: &Path,
    case: &Case,
    catalogue: &Catalogue,
    mutated: bool,
    out: &Path,
    child: Duration,
    interrupt: &AtomicBool,
) -> Result<Recorded, BoxError> {
    let expect = if mutated {
        Expect::Violated(&[])
    } else {
        Expect::Holds
    };
    let mut run = record(
        Command::new(sim),
        &case.harness,
        &Located::Input(tree.join(&case.witness)),
        &case.property,
        expect,
        out,
        Some((child, interrupt)),
    )?;
    let trace = match fs::read_to_string(out.join("trace.jsonl")) {
        Err(_) => None,
        Ok(text) => match (parse_trace(&text, &case.property), &mut run) {
            (Ok(trace), _) => Some(trace),
            // A run that crashed or passed its deadline can leave a trace
            // with no end record. Its failure classifies it.
            (Err(why), Run::Failed(failed) | Run::Killed(failed)) => {
                failed.push_str(&format!("; its trace is unreadable: {why}"));
                None
            }
            (Err(why), Run::Ended { .. }) => return Err(why),
        },
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
    let toolchain = toolchain(&catalogue.baseline_build)?;
    let child = Duration::from_secs(catalogue.confirmation.child_seconds);
    let tree = work.join("baseline").join("worktree");
    add_worktree(repo_root, &tree, &fuzz.source_revision)?;
    let rows = (|| {
        check_pins(&tree, &fuzz.source_revision, cases)?;
        let corpus = tree.join(&catalogue.discovery.corpus);
        check_empty_corpus(&corpus)?;
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
                    Err(failure) => Err(LaneError::Build(failure.of("the original simulator"))),
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
                    .map_err(|error| match error {
                        LaneError::Build(failure) => {
                            LaneError::Build(failure.of("the original fuzz target"))
                        }
                        error => error,
                    }),
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
    baselines: &BTreeMap<(String, u32), Result<CampaignFacts, LaneError>>,
    corpus: &Path,
    out: &Path,
    work: &Path,
    child: Duration,
) -> Result<DiscoveryCase, BoxError> {
    let mut runs: Vec<(u32, DiscoveryRun, Option<PathBuf>)> = seeds
        .iter()
        .map(|seed| {
            let mut run = DiscoveryRun {
                equivalent: case.equivalent.clone(),
                ..DiscoveryRun::default()
            };
            match baselines.get(&(case.target.clone(), *seed)) {
                None => {}
                Some(Ok(facts)) => run.baseline = Some(Ok(facts.clone())),
                Some(Err(LaneError::Build(BuildFailure::Broken(why)))) => {
                    run.broken_build = Some(why.clone());
                }
                Some(Err(LaneError::Killed(why))) => {
                    run.baseline = Some(Err(NoCampaign::Killed(why.clone())));
                }
                Some(Err(error)) => run.baseline = Some(Err(NoCampaign::Failed(error.to_string()))),
            }
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
            // The mutant simulator, or why no seed can run: an invalid
            // mutant, or a build that broke.
            let mut mutant = match prepare_patch(&tree, case)? {
                Err(why) => Err(BuildFailure::Rejected(why)),
                Ok(()) => build_sim(
                    &tree,
                    &sim_target,
                    toolchain(&catalogue.baseline_build)?,
                    &dir.join("mutant-build.log"),
                )?
                .map_err(|failure| failure.of("the mutant simulator")),
            };
            for (seed, run, store) in runs.iter_mut().filter(|(_, run, _)| runnable(run)) {
                let sim = match &mutant {
                    Err(BuildFailure::Rejected(why)) => {
                        run.patch = Some(Err(why.clone()));
                        continue;
                    }
                    Err(BuildFailure::Broken(why)) => {
                        run.broken_build = Some(why.clone());
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
                    Err(LaneError::Build(failure)) => {
                        fuzz_build_failed(run, &mut mutant, failure.of("the mutant fuzz target"));
                    }
                    Err(LaneError::Run(why)) => {
                        run.patch = Some(Ok(()));
                        run.mutant = Some(Err(NoCampaign::Failed(why)));
                    }
                    Err(LaneError::Killed(why)) => {
                        run.patch = Some(Ok(()));
                        run.mutant = Some(Err(NoCampaign::Killed(why)));
                    }
                    Ok(facts) => {
                        run.patch = Some(Ok(()));
                        if let Some(input) = &facts.intended_input {
                            run.confirmation = Some(
                                confirm_and_replay(
                                    &|| Command::new(&sim),
                                    &case.harness,
                                    &lane_dir.join("store").join(input),
                                    &case.property,
                                    &[],
                                    Confirm::Witness,
                                    &lane_dir.join("confirmation"),
                                    child,
                                )?
                                .map(drop),
                            );
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
    let ok = |lane: &Option<Result<CampaignFacts, NoCampaign>>| {
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

/// Records a mutant fuzz target's build `failure` in its seed's `run`. A
/// rejected build makes the mutant invalid for this seed and, through
/// `mutant`, for every later seed, since they build the same source. A
/// broken build fails only this seed, and the next seed builds again.
fn fuzz_build_failed(
    run: &mut DiscoveryRun,
    mutant: &mut Result<PathBuf, BuildFailure>,
    failure: BuildFailure,
) {
    match failure {
        BuildFailure::Rejected(why) => {
            run.patch = Some(Err(why.clone()));
            *mutant = Err(BuildFailure::Rejected(why));
        }
        BuildFailure::Broken(why) => run.broken_build = Some(why),
    }
}

/// Builds `case`'s fuzz target with `sim` in `fuzz_target`, and runs one
/// campaign on it with the catalogue's bounds and `seed`, from `corpus`, in
/// a fresh store under `dir` ([`lane::campaign_lane`]). Reads what the
/// store's records show, and whether the run's build pairs with the
/// catalogue's.
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
    let name =
        format!("{}-{}", if mutated { &case.id } else { &case.target }, seed).replace('/', "-");
    let spec = LaneSpec {
        target: &case.target,
        harness: &case.harness,
        name: &name,
        seed,
        corpus,
    };
    Ok(lane::campaign_lane(
        &|| Command::new(sim),
        dir,
        fuzz_target,
        &spec,
        &catalogue.discovery.campaign,
        lane::start_deadline(&catalogue.discovery.campaign),
    )?
    .map(|campaign| {
        let mut facts = campaign.facts(&case.property);
        let expected = expected_fuzz_identity(catalogue, case);
        facts.unpaired = paired_identity(&expected, &campaign.identity, FUZZ_FIELDS, mutated);
        facts
    }))
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

/// Applies `case`'s patch in `tree`, a checkout of the pinned revision,
/// from that checkout, and checks that it changed exactly the case's
/// declared production files. `Err` says why the mutant isn't valid.
fn prepare_patch(tree: &Path, case: &Case) -> Result<Result<(), String>, BoxError> {
    if let Err(why) = apply(tree, &tree.join(&case.patch)) {
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

/// Checks each case's frozen inputs, its patch, witness input and
/// production files, in `tree`, a checkout of the pinned `revision`, before
/// any patch, against the catalogue's hashes. The pilot reads every input
/// at the pin, never from the tree it runs from, so a later change there
/// leaves the run alone. A patch missing at the pin fails.
fn check_pins(tree: &Path, revision: &str, cases: &[&Case]) -> Result<(), BoxError> {
    let missing: Vec<String> = cases
        .iter()
        .filter(|case| !tree.join(&case.patch).is_file())
        .map(|case| format!("{}: {}", case.id, case.patch.display()))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "the pinned revision {revision} has no patch for {}",
            missing.join(", ")
        )
        .into());
    }
    let mut differ = Vec::new();
    for case in cases {
        let mut pins = vec![
            (tree.join(&case.patch), &case.patch_sha256),
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
/// binary's path, or why the build failed. Cargo writes its messages as
/// JSON, which [`sim_build_failure`] reads.
fn build_sim(
    tree: &Path,
    target: &Path,
    toolchain: &str,
    log: &Path,
) -> Result<Result<PathBuf, BuildFailure>, BoxError> {
    let mut cargo = Command::new("rustup");
    cargo
        .args(["run", toolchain, "cargo", "build", "--locked"])
        .arg("--message-format=json")
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
    Ok(match run_lane_child(&mut cargo, log, None)? {
        ChildEnd::Exited(0) => Ok(target.join("debug").join("zakura-sim")),
        end => {
            let why = format!("`cargo build` {end}; see {}", log.display());
            let text = fs::read(log).unwrap_or_default();
            Err(sim_build_failure(end, &String::from_utf8_lossy(&text), why))
        }
    })
}

/// The error that stops the pilot if `interrupt` is set. A signal from the
/// terminal also reaches the children that share the pilot's process
/// group, such as `git apply` and `git worktree remove`, and can end one as
/// an invalid mutant or a cleanup failure. So the pilot checks the flag
/// before it writes a report.
fn check_interrupt(interrupt: &AtomicBool) -> Result<(), BoxError> {
    if interrupt.load(Ordering::SeqCst) {
        return Err(INTERRUPTED_ERROR.into());
    }
    Ok(())
}

/// Checks out `rev` in a new detached worktree at `path`, replacing any
/// earlier one there.
pub(super) fn add_worktree(repo_root: &Path, path: &Path, rev: &str) -> Result<(), BoxError> {
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

/// Creates `dir`, or fails if it already holds anything: a run never
/// overwrites an earlier result.
pub(super) fn fresh_dir(dir: &Path) -> Result<(), BoxError> {
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
pub(super) fn head(repo_root: &Path) -> Result<String, BoxError> {
    let output = git(repo_root).args(["rev-parse", "HEAD"]).output()?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// The lowercase hex SHA-256 of `path`'s contents.
pub(super) fn file_sha256(path: &Path) -> Result<String, BoxError> {
    Ok(hex(&Sha256::digest(fs::read(path)?)))
}

/// `bytes` in lowercase hex.
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `class`'s name, as the report writes it.
fn class_name(class: Class) -> String {
    serde_json::to_value(class)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}
