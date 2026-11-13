//! `cargo xtask planted pilot rescore --from <dir> --out <dir>`: re-derives
//! a finished `discovery` or `feedback` run's report from the records the
//! run left under its `--out`, without running a campaign.
//!
//! A run keeps each campaign's lane directory under its `--out`: the
//! store, with the run's manifest, initial snapshot, end record and each
//! worker's attempts, and the evidence `campaign evidence` wrote
//! ([`CampaignRecords`]). Its report keeps each seed's class and reason,
//! which the build, patch and confirmation steps decided, and each
//! campaign's pairing verdict. `rescore` keeps those, reads each
//! campaign's facts again for its case's own property, checks that every
//! paired fuzz build shares one toolchain, and compares the arms and counts the
//! results again. It writes the report under the new `--out`, which must
//! be missing or empty, and leaves the run's own report alone.
//!
//! The report records the commit that re-derived it as `rescored_by`. A
//! run's facts never change its classes: the original campaign's
//! classification reads no property, and the mutant's facts were already
//! its case's.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::Value;

use super::{
    baseline_dir, classify::Classified, compare, discovery_report, fresh_dir, head, mutant_dir,
    report_file, toolchain_of, write_report, Arm, Campaign, CampaignFacts, CampaignRecords,
    DiscoveryCase, Excluded, Lane, Meta, SeedResult, Written,
};
use crate::BoxError;

/// The experiments whose reports `rescore` re-derives.
const RESCORED: &[&str] = &["discovery", "feedback"];

/// A discovery report as the run wrote it: only what `rescore` keeps.
#[derive(Debug, Deserialize)]
struct Saved {
    #[serde(flatten)]
    meta: Meta,
    cases: Vec<SavedCase>,
    #[serde(default)]
    excluded: Vec<Excluded>,
}

/// One case's row as the run wrote it.
#[derive(Debug, Deserialize)]
struct SavedCase {
    id: String,
    fault_class: String,
    property: String,
    target: String,
    seeds: Vec<SavedSeed>,
    cleanup: Option<String>,
}

/// One seed's row as the run wrote it.
#[derive(Debug, Deserialize)]
struct SavedSeed {
    seed: u32,
    arm: Arm,
    result: Classified,
    /// `None` if the original campaign left no facts.
    baseline: Option<SavedFacts>,
    /// `None` if the mutant campaign left no facts.
    mutant: Option<SavedFacts>,
    store: Option<PathBuf>,
}

/// What `rescore` keeps of a campaign's facts: its pairing verdict, which
/// needs the run's checkout to decide again.
#[derive(Debug, Deserialize)]
struct SavedFacts {
    unpaired: Option<String>,
}

/// Re-derives the report of the run whose `--out` is `from` into `out`.
/// `repo_root` names the commit that re-derives it. Fails if `from` holds
/// no report, or several, if a campaign's records are missing, and if the
/// fuzz builds don't share one toolchain.
pub(super) fn rescore(repo_root: &Path, from: &Path, out: &Path) -> Result<Written, BoxError> {
    let found: Vec<&str> = RESCORED
        .iter()
        .copied()
        .filter(|experiment| from.join(report_file(experiment)).is_file())
        .collect();
    let [experiment] = found[..] else {
        return Err(format!(
            "{} holds {} of discovery.json and feedback.json; rescore needs exactly one",
            from.display(),
            found.len()
        )
        .into());
    };
    let path = from.join(report_file(experiment));
    let saved: Saved = serde_json::from_slice(&fs::read(&path)?)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if saved.meta.experiment != experiment {
        return Err(format!(
            "{} names the experiment {}",
            path.display(),
            saved.meta.experiment
        )
        .into());
    }
    let mut toolchains = BTreeSet::new();
    let rows = saved
        .cases
        .into_iter()
        .map(|case| rescore_case(from, case, &mut toolchains))
        .collect::<Result<Vec<DiscoveryCase>, BoxError>>()?;
    let mut meta = saved.meta;
    meta.fuzz_toolchain = one_toolchain(meta.fuzz_toolchain.take(), toolchains)?;
    meta.rescored_by = Some(head(repo_root)?);
    let report = discovery_report(meta, rows, saved.excluded)?;
    fresh_dir(out)?;
    write_report(out, report)
}

/// `case`'s row, with each campaign's facts read again from the lane
/// directories under `from`. Adds each campaign's toolchain to
/// `toolchains`.
fn rescore_case(
    from: &Path,
    case: SavedCase,
    toolchains: &mut BTreeSet<String>,
) -> Result<DiscoveryCase, BoxError> {
    let mut seeds = Vec::new();
    for seed in case.seeds {
        let lane = Lane {
            seed: seed.seed,
            arm: seed.arm,
        };
        let mut facts = |dir: PathBuf, saved: Option<SavedFacts>| {
            saved
                .map(|saved| reread(&dir, &case.property, saved, toolchains))
                .transpose()
        };
        let baseline = facts(baseline_dir(from, &case.target, lane), seed.baseline)?;
        let mutant = facts(mutant_dir(from, &case.id, lane), seed.mutant)?;
        seeds.push(SeedResult {
            seed: seed.seed,
            arm: seed.arm,
            result: seed.result,
            baseline,
            mutant,
            store: seed.store,
        });
    }
    Ok(DiscoveryCase {
        id: case.id,
        fault_class: case.fault_class,
        property: case.property,
        target: case.target,
        arms: compare::compare(&seeds),
        seeds,
        cleanup: case.cleanup,
    })
}

/// The facts of the campaign in lane directory `dir` for `property`, with
/// the `saved` pairing verdict. Adds its build's toolchain to `toolchains`
/// if the build paired: an unpaired build is already a failure.
fn reread(
    dir: &Path,
    property: &str,
    saved: SavedFacts,
    toolchains: &mut BTreeSet<String>,
) -> Result<CampaignFacts, BoxError> {
    let records =
        CampaignRecords::read(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let identity = records
        .manifest
        .pointer("/build/identity")
        .unwrap_or(&Value::Null);
    if saved.unpaired.is_none() {
        toolchains.insert(Value::Object(toolchain_of(identity)).to_string());
    }
    Ok(Campaign {
        records,
        unpaired: saved.unpaired,
    }
    .facts(property))
}

/// The one toolchain every paired build shares, as `toolchains` holds
/// them, which must be the run's `recorded` one if it recorded one. The
/// `recorded` one if no paired build left records.
fn one_toolchain(
    recorded: Option<Value>,
    toolchains: BTreeSet<String>,
) -> Result<Option<Value>, BoxError> {
    let mut toolchains = toolchains.into_iter();
    let Some(first) = toolchains.next() else {
        return Ok(recorded);
    };
    if let Some(other) = toolchains.next() {
        return Err(format!(
            "the run's fuzz builds don't share one toolchain: {first} and {other}"
        )
        .into());
    }
    let first: Value = serde_json::from_str(&first)?;
    match recorded {
        Some(recorded) if recorded != first => Err(format!(
            "the run recorded the fuzz toolchain {recorded}, but its builds name {first}"
        )
        .into()),
        _ => Ok(Some(first)),
    }
}
