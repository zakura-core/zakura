//! Binding hdr-14's published record to the run that made it, so sim-7's
//! evaluator never accepts a record paired with another run's artifacts
//! (`header-acceptance verify`).

use std::path::Path;

use serde_json::Value;

use crate::{
    harness::header_acceptance::{
        evaluate::{Actual, Execution, ResultFile},
        tool::{evaluate_artifacts, record_artifacts, Record, RunBinding, RunPlan},
    },
    trace::{TraceReader, TRACE_FILE},
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
/// the manifest at `repo`, and returns the rebuilt result file only if the
/// record equals the published one, with the published artifact path taken
/// into account. The rebuilt record takes its subject and containment from
/// the published one, so a record from another run, or a result file from
/// another run with the same subject, differs somewhere else and is
/// refused, with the first field that differs.
pub fn verify(repo: &Path, published: &Published<'_>) -> Result<ResultFile, String> {
    // The one check of the run's artifacts before anything reads them, for
    // every caller.
    if let Some(why) = tree_refusal(published.artifacts) {
        return Err(format!(
            "the header run's artifacts aren't safe to read: {why}"
        ));
    }
    let run = RunBinding {
        run_id: published.run_id.to_owned(),
        subject: published.record.subject.clone(),
        subject_source: published.subject_source.to_owned(),
        containment: published.record.containment,
    };
    let (results, mut rebuilt) =
        evaluate_artifacts(repo, published.plan, published.artifacts, &run);
    rebuilt.artifacts = record_artifacts(published.artifacts, published.out);
    let to_value = |record: &Record| {
        serde_json::to_value(record).map_err(|error| format!("serializing a record: {error}"))
    };
    match first_difference(&to_value(published.record)?, &to_value(&rebuilt)?) {
        None => Ok(results),
        Some(at) => Err(format!(
            "the published header record differs from its run's artifacts at `{at}`"
        )),
    }
}

/// The JSON in the regular file at `path`, read without following a link
/// and within [`MAX_ARTIFACT_BYTES`], so a FIFO or a link to `/dev/zero`
/// can't hang or exhaust the reader.
pub fn read_bounded_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let bytes = crate::trace::read::read_bounded(path, MAX_ARTIFACT_BYTES)
        .map_err(|error| format!("{}: {error}", path.display()))?
        .ok_or_else(|| format!("{}: missing", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

/// The text of the regular file at `path`, read as
/// [`read_bounded_json`] reads.
pub fn read_bounded_text(path: &Path) -> Result<String, String> {
    let bytes = crate::trace::read::read_bounded(path, MAX_ARTIFACT_BYTES)
        .map_err(|error| format!("{}: {error}", path.display()))?
        .ok_or_else(|| format!("{}: missing", path.display()))?;
    String::from_utf8(bytes).map_err(|error| format!("{}: {error}", path.display()))
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

/// The trace digest of every simulator run `actual` holds: each
/// `Execution`, and each planted case's companion runs. The match names
/// every variant and every field, so a new variant or field fails to
/// compile until it's walked here (the sim-7 plan's Rule 10).
pub fn executed_traces(actual: &Actual) -> Vec<&str> {
    /// The trace digests of `executions`.
    fn traces<'a>(executions: &[&'a Execution]) -> Vec<&'a str> {
        executions.iter().map(|e| e.trace.as_str()).collect()
    }
    match actual {
        Actual::Run(run) => traces(&[run]),
        Actual::Caught {
            baseline,
            patched,
            draft,
            companions,
        } => {
            let mut all = traces(&[baseline, patched, draft]);
            all.extend(companions.iter().map(|c| c.trace.as_str()));
            all
        }
        Actual::MachineCaught { baseline, patched }
        | Actual::HarnessCaught { baseline, patched } => traces(&[baseline, patched]),
        Actual::IntegrityCaught {
            unpatched,
            patched,
            restored,
        } => traces(&[unpatched, patched, restored]),
        // A replay reruns a run walked under its own result, and a test
        // result or a unit catch is `cargo test` executions with no trace.
        Actual::Replay {
            run: _,
            of: _,
            meta: _,
            exact: _,
            reports_equal: _,
            stores_equal: _,
            fresh: _,
        }
        | Actual::Test {
            ran: _,
            binary_source: _,
            checkout_source: _,
        }
        | Actual::UnitCaught { refused: _ } => Vec::new(),
    }
}

/// The most entries an artifact tree may hold.
const MAX_ARTIFACT_ENTRIES: usize = 1 << 20;

/// The largest file an artifact tree may hold, which bounds every read
/// the evaluator makes from it.
pub const MAX_ARTIFACT_BYTES: u64 = 1 << 30;

/// Why the tree at `dir` isn't safe to read, if it isn't: it and every
/// entry under it must be a directory or a regular file of at most
/// [`MAX_ARTIFACT_BYTES`], with no link, FIFO or device, and at most
/// [`MAX_ARTIFACT_ENTRIES`] entries in all. The evaluator checks each tree
/// once, here, before it reads any file in it, so a link to `/dev/zero` or
/// a FIFO can't exhaust or hang it.
pub fn tree_refusal(dir: &Path) -> Option<String> {
    let mut pending = vec![dir.to_path_buf()];
    let mut entries = 0usize;
    while let Some(path) = pending.pop() {
        entries += 1;
        if entries > MAX_ARTIFACT_ENTRIES {
            return Some(format!("{} holds too many entries", dir.display()));
        }
        let meta = match path.symlink_metadata() {
            Ok(meta) => meta,
            Err(error) => return Some(format!("{}: {error}", path.display())),
        };
        if meta.is_dir() {
            match std::fs::read_dir(&path) {
                // Counted while read, so a directory with too many entries
                // is refused before they're all held.
                Ok(children) => {
                    for child in children.flatten() {
                        if entries + pending.len() >= MAX_ARTIFACT_ENTRIES {
                            return Some(format!("{} holds too many entries", dir.display()));
                        }
                        pending.push(child.path());
                    }
                }
                Err(error) => return Some(format!("{}: {error}", path.display())),
            }
        } else if !meta.is_file() {
            return Some(format!(
                "{} isn't a regular file or directory",
                path.display()
            ));
        } else if meta.len() > MAX_ARTIFACT_BYTES {
            return Some(format!(
                "{} is larger than the artifact limit",
                path.display()
            ));
        }
    }
    None
}

/// Judges the cleanup of every execution in `results` from its trace's end
/// record under `artifacts`, found by trace digest, with the one cleanup
/// rule ([`super::judge::cleanup_fault`]). hdr-14's record exports only a
/// run's `leaked`, so a run whose shutdown panicked with nothing leaked is
/// caught only here. A missing trace refuses too.
pub fn consumed_cleanup(results: &ResultFile, artifacts: &Path) -> Result<(), String> {
    let mut traces = std::collections::BTreeMap::new();
    index_traces(artifacts, &mut traces);
    for result in &results.results {
        for digest in executed_traces(&result.actual) {
            // Every trace with this digest is judged: integrity runs from the
            // same source can leave identical test artifacts beside
            // histories whose cleanup differs.
            let dirs: &Vec<std::path::PathBuf> = traces
                .get(digest)
                .ok_or_else(|| format!("the header run's trace {digest} isn't in its artifacts"))?;
            for dir in dirs {
                let trace = TraceReader::open(dir)
                    .map_err(|error| format!("the header trace in {}: {error}", dir.display()))?;
                let end = trace
                    .end
                    .ok_or_else(|| format!("the header trace in {} has no end", dir.display()))?;
                if let Some(fault) = super::judge::cleanup_fault(&end.cleanup) {
                    return Err(format!("a consumed header run ({digest}): {fault}"));
                }
            }
        }
    }
    Ok(())
}

/// Adds every trace directory under `dir` to `traces`, by its trace's ID,
/// the SHA-256 of its `trace.jsonl`, without following links.
fn index_traces(
    dir: &Path,
    traces: &mut std::collections::BTreeMap<String, Vec<std::path::PathBuf>>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // A link is never followed, so a cyclic link can't recurse forever.
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            index_traces(&path, traces);
        } else if kind.is_file()
            && path
                .file_name()
                .is_some_and(|name| name == crate::harness::header_acceptance::tool::TEST_FILE)
            && dir.join("history").is_dir()
        {
            // An integrity mutation's execution names its `test.json` by
            // digest, and the run it judges is the recorded `history` trace
            // beside it.
            if let Ok(Some(bytes)) =
                crate::trace::read::read_bounded(&path, crate::trace::MAX_TRACE_BYTES)
            {
                use sha2::{Digest, Sha256};
                let id = crate::trace::meta::hex(&Sha256::digest(&bytes));
                traces.entry(id).or_default().push(dir.join("history"));
            }
        } else if kind.is_file() && path.file_name().is_some_and(|name| name == TRACE_FILE) {
            // A regular file only, read without following a link and within
            // the trace limit, so a link to `/dev/zero` or a FIFO can't hang
            // or exhaust the evaluator.
            if let Ok(Some(bytes)) =
                crate::trace::read::read_bounded(&path, crate::trace::MAX_TRACE_BYTES)
            {
                use sha2::{Digest, Sha256};
                let id = crate::trace::meta::hex(&Sha256::digest(&bytes));
                traces.entry(id).or_default().push(dir.to_owned());
            }
        }
    }
}
