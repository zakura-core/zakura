//! The feedback comparison's case list (fuzz-14),
//! `qa/mutation-pilot/feedback.toml`.
//!
//! The list freezes the comparison's bounds, seeds and arms, and names
//! retained planted cases, each by its path under `qa/planted/`, with its
//! property, fuzz target and fault class. [`read`] checks each case against
//! the planted catalogue and the fuzz package's target table, and returns it
//! as a pilot [`Case`] whose hashes are those of the tree's files. The
//! runner then checks the same hashes in its checkout of `HEAD`
//! ([`super::check_pins`]), so a case changed but not committed fails
//! before any campaign runs.

use std::{collections::BTreeMap, fs, path::Path};

use serde::Deserialize;

use super::{
    super::{
        cases, harness_of, patched_files, planted_layout::PLANTED_DIR, Case as Planted, Witness,
    },
    file_sha256, Bounds, Case, ConfirmationBounds,
};
use crate::{sim_minimize::fuzz_targets, BoxError};

/// The case list, relative to the repository root.
pub(super) const LIST: &str = "qa/mutation-pilot/feedback.toml";

/// The case list's file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFile {
    bounds: Bounds,
    confirmation: ConfirmationBounds,
    #[serde(rename = "case")]
    cases: Vec<Entry>,
}

/// One listed case.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    /// The planted case's patch, relative to `qa/planted/`, without its
    /// `.patch`, such as `sync/tips-consistent/keep-epoch-on-replaced-selection`.
    pub(super) case: String,
    /// The property the patch must violate: the case's directory.
    pub(super) property: String,
    /// The simulator fuzz target that runs the case's input's harness.
    pub(super) target: String,
    /// The fault the patch plants, for the report's counts.
    pub(super) fault_class: String,
}

/// The case list, checked.
#[derive(Debug)]
pub(super) struct List {
    /// The bounds every campaign shares.
    pub(super) bounds: Bounds,
    /// The confirmation bounds.
    pub(super) confirmation: ConfirmationBounds,
    /// The listed cases, in the list's order.
    pub(super) cases: Vec<Case>,
}

/// Reads and checks the case list in `repo_root`, and returns its bytes,
/// for the report's digest, and its cases ([`resolve`]).
pub(super) fn read(repo_root: &Path) -> Result<(Vec<u8>, List), BoxError> {
    let path = repo_root.join(LIST);
    let bytes = fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let file: ListFile = toml::from_str(std::str::from_utf8(&bytes)?)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let targets = fuzz_targets(repo_root)?;
    if let Some(unknown) = file
        .bounds
        .libfuzzer_args
        .keys()
        .find(|target| !targets.contains_key(*target))
    {
        return Err(
            format!("{LIST}: libFuzzer flags for {unknown}, which isn't a fuzz target").into(),
        );
    }
    let planted = cases(repo_root)?;
    let cases = resolve(repo_root, &file.cases, &planted, &targets)
        .map_err(|error| format!("{LIST}: {error}"))?;
    Ok((
        bytes,
        List {
            bounds: file.bounds,
            confirmation: file.confirmation,
            cases,
        },
    ))
}

/// Each of `entries` as a pilot case, from the `planted` cases under
/// `repo_root` and the fuzz package's `targets`, each mapped to the harness
/// it runs. A case's ID is its path, its production files are those its
/// patch changes, and each hash is of the file in `repo_root`.
///
/// Fails on a case listed twice, one with no planted patch, one whose
/// property isn't its planted case's, a nested case, a typed case, a
/// target that isn't a fuzz target, and a target that runs another harness
/// than the case's input.
pub(super) fn resolve(
    repo_root: &Path,
    entries: &[Entry],
    planted: &[Planted],
    targets: &BTreeMap<String, Option<String>>,
) -> Result<Vec<Case>, BoxError> {
    let mut resolved = Vec::new();
    for (at, entry) in entries.iter().enumerate() {
        let id = &entry.case;
        if entries[..at].iter().any(|earlier| &earlier.case == id) {
            return Err(format!("{id} is listed twice").into());
        }
        let patch = Path::new(PLANTED_DIR).join(format!("{id}.patch"));
        let Some(found) = planted
            .iter()
            .find(|case| case.patch == repo_root.join(&patch))
        else {
            return Err(format!("{id}: no planted case has the patch {}", patch.display()).into());
        };
        if entry.property != found.property {
            return Err(format!(
                "{id}: lists property {}, but the planted case violates {}",
                entry.property, found.property
            )
            .into());
        }
        if !found.set.is_empty() {
            return Err(format!(
                "{id}: a nested case's catch needs its whole set, which a campaign's first \
                 finding needn't show"
            )
            .into());
        }
        let Witness::Input(input) = &found.witness else {
            return Err(format!("{id}: a typed case has no fuzz input for a target to run").into());
        };
        let harness = match targets.get(&entry.target) {
            None => {
                return Err(format!("{id}: {} isn't a simulator fuzz target", entry.target).into())
            }
            Some(None) => {
                return Err(format!("{id}: {} runs no registered harness", entry.target).into())
            }
            Some(Some(harness)) => harness,
        };
        let input_harness = harness_of(repo_root, input)?;
        if &input_harness != harness {
            return Err(format!(
                "{id}: {} runs {harness}, but the case's input runs {input_harness}",
                entry.target
            )
            .into());
        }
        let production_files = patched_files(repo_root, &found.patch)?
            .into_iter()
            .map(|path| {
                let sha256 = file_sha256(&repo_root.join(&path))?;
                let path = path
                    .to_str()
                    .ok_or_else(|| format!("{id}: {} isn't UTF-8", path.display()))?
                    .to_owned();
                Ok((path, sha256))
            })
            .collect::<Result<BTreeMap<String, String>, BoxError>>()?;
        resolved.push(Case {
            id: id.clone(),
            fault_class: entry.fault_class.clone(),
            patch_sha256: file_sha256(&repo_root.join(&patch))?,
            patch,
            production_files,
            property: found.property.clone(),
            also_violates: found.also_violates.clone(),
            harness: input_harness,
            target: entry.target.clone(),
            witness: input.clone(),
            witness_sha256: file_sha256(&repo_root.join(input))?,
            equivalent: None,
            discovery_excluded: None,
        });
    }
    if resolved.is_empty() {
        return Err("the list names no case".into());
    }
    Ok(resolved)
}
