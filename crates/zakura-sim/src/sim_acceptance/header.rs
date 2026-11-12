//! Binding hdr-14's published record to the run that made it, so sim-7's
//! evaluator never accepts a record paired with another run's artifacts
//! (`header-acceptance verify`).

use std::path::Path;

use serde_json::Value;

use crate::harness::header_acceptance::tool::{
    evaluate_artifacts, record_artifacts, Record, RunBinding, RunPlan,
};

/// What [`verify`] reads besides the repository.
pub struct Published<'a> {
    /// The header run's plan, as its `plan.json` holds it.
    pub plan: &'a RunPlan,
    /// The run directory its record's `artifacts` names.
    pub artifacts: &'a Path,
    /// The `--out` the record was published under.
    pub out: &'a Path,
    /// The run's ID: its run directory's name.
    pub run_id: &'a str,
    /// S's source digest, which the caller computes from S itself.
    pub subject_source: &'a str,
    /// The published `record.json`, parsed.
    pub record: &'a Record,
}

/// Rebuilds the header record from `published`'s plan and artifacts with
/// the manifest at `repo`, and returns `Ok` only if it equals the published
/// record, with the published artifact path taken into account. The
/// rebuilt record takes its subject and containment from the published
/// one, so a record from another run, or a result file from another run
/// with the same subject, differs somewhere else and is refused, with the
/// first field that differs.
pub fn verify(repo: &Path, published: &Published<'_>) -> Result<(), String> {
    let run = RunBinding {
        run_id: published.run_id.to_owned(),
        subject: published.record.subject.clone(),
        subject_source: published.subject_source.to_owned(),
        containment: published.record.containment,
    };
    let (_, mut rebuilt) = evaluate_artifacts(repo, published.plan, published.artifacts, &run);
    rebuilt.artifacts = record_artifacts(published.artifacts, published.out);
    let to_value = |record: &Record| {
        serde_json::to_value(record).map_err(|error| format!("serializing a record: {error}"))
    };
    match first_difference(&to_value(published.record)?, &to_value(&rebuilt)?) {
        None => Ok(()),
        Some(at) => Err(format!(
            "the published header record differs from its run's artifacts at `{at}`"
        )),
    }
}

/// The JSON path of the first place `a` and `b` differ, or `None` if
/// they're equal. Objects compare by key in order, arrays by index.
pub fn first_difference(a: &Value, b: &Value) -> Option<String> {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            let keys: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            keys.into_iter()
                .find_map(|key| match (a.get(key), b.get(key)) {
                    (Some(x), Some(y)) => first_difference(x, y).map(|at| join(key, &at)),
                    _ => Some(key.clone()),
                })
        }
        (Value::Array(a), Value::Array(b)) => {
            let shared = a.len().min(b.len());
            (0..shared)
                .find_map(|i| first_difference(&a[i], &b[i]).map(|at| join(&i.to_string(), &at)))
                .or_else(|| (a.len() != b.len()).then(|| shared.to_string()))
        }
        _ => (a != b).then(String::new),
    }
}

/// `key`, then `rest` after a dot if there is one.
fn join(key: &str, rest: &str) -> String {
    if rest.is_empty() {
        key.to_owned()
    } else {
        format!("{key}.{rest}")
    }
}
