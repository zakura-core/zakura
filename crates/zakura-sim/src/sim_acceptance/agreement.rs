//! The schedule-agreement check for S4.4 and S4.7 (the sim-7 plan's
//! `Agreement` result): two recorded runs of one input, on the code before
//! and after a patch, agree on every step up to their first differing
//! step, and that step is at the patch's declared site.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::record::DrawRecord;

/// One hunk of a unified diff, by its old and new line ranges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Hunk {
    old_start: u64,
    old_len: u64,
    new_start: u64,
    new_len: u64,
}

/// How a patch moves each source line, file by file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HunkMap {
    files: BTreeMap<String, Vec<Hunk>>,
}

impl HunkMap {
    /// Reads a unified diff, such as a planted case's `.patch`. Each file's
    /// hunks come from its `@@ -a,b +c,d @@` lines under `+++ b/<file>`.
    pub fn parse(patch: &str) -> Result<Self, String> {
        let mut map = HunkMap::default();
        let mut file: Option<String> = None;
        for line in patch.lines() {
            if let Some(path) = line.strip_prefix("+++ ") {
                let path = path.strip_prefix("b/").unwrap_or(path);
                file = Some(path.split('\t').next().unwrap_or(path).to_owned());
            } else if let Some(rest) = line.strip_prefix("@@ ") {
                let file = file
                    .clone()
                    .ok_or_else(|| format!("a hunk before any file: {line}"))?;
                let hunk = parse_hunk(rest).ok_or_else(|| format!("a bad hunk header: {line}"))?;
                map.files.entry(file).or_default().push(hunk);
            }
        }
        Ok(map)
    }

    /// The map of the patch reversed: lines after it to lines before it,
    /// for comparing a patched run against the code the patch was made
    /// from.
    pub fn inverse(&self) -> Self {
        HunkMap {
            files: self
                .files
                .iter()
                .map(|(file, hunks)| {
                    let reversed = hunks
                        .iter()
                        .map(|hunk| Hunk {
                            old_start: hunk.new_start,
                            old_len: hunk.new_len,
                            new_start: hunk.old_start,
                            new_len: hunk.old_len,
                        })
                        .collect();
                    (file.clone(), reversed)
                })
                .collect(),
        }
    }

    /// Where `line` of `file` before the patch is after it: shifted by the
    /// hunks above it, or `None` if a hunk replaced it, so it can't match
    /// any line after the patch.
    pub fn map(&self, file: &str, line: u64) -> Option<u64> {
        let Some(hunks) = self.files.get(file) else {
            return Some(line);
        };
        let mut shift: i64 = 0;
        for hunk in hunks {
            let old_end = hunk.old_start + hunk.old_len;
            if line < hunk.old_start {
                break;
            }
            if line < old_end {
                return None;
            }
            // Line numbers and counts in a source diff stay far below
            // i64::MAX, so these casts are exact.
            shift += hunk.new_len as i64 - hunk.old_len as i64;
        }
        // As above, `line` fits in i64.
        u64::try_from(line as i64 + shift).ok()
    }
}

/// `-a,b +c,d @@` as a hunk. A missing count is 1.
fn parse_hunk(rest: &str) -> Option<Hunk> {
    let mut parts = rest.split_whitespace();
    let range = |part: &str, sign: char| -> Option<(u64, u64)> {
        let part = part.strip_prefix(sign)?;
        let (start, len) = part.split_once(',').unwrap_or((part, "1"));
        Some((start.parse().ok()?, len.parse().ok()?))
    };
    let (old_start, old_len) = range(parts.next()?, '-')?;
    let (new_start, new_len) = range(parts.next()?, '+')?;
    Some(Hunk {
        old_start,
        old_len,
        new_start,
        new_len,
    })
}

/// What one step of a recorded run shows: its pick and draws from the
/// trace, and its observations from the `--observe` log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepView {
    /// The step, from 0 for setup.
    pub step: u64,
    /// The pick's identity, empty for step 0.
    pub pick: String,
    /// The step's draws.
    pub draws: Vec<DrawRecord>,
    /// Its observations, as the log holds them.
    pub obs: Vec<Value>,
}

/// Where a fix's first observable effect must be: the first differing
/// step's after-patch side must hold an observation whose `site` is
/// `site`, an observation of a task reaching a point, `<task>:<at>`, that
/// is `site`, or a pick that is `site`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Site {
    /// A `file:line` after the patch, a `<task>:<point>` a task reached, or
    /// a pick identity.
    pub at: String,
}

/// `<task>:<at>` for an observation of a task reaching a point.
fn reached(obs: &Value) -> Option<String> {
    let task = obs.get("task")?.as_str()?;
    let at = obs.get("at")?.as_str()?;
    Some(format!("{task}:{at}"))
}

/// The step views of the trace in `dir`, from its steps and the
/// `--observe` log a fresh replay wrote beside it. The log must be bound
/// to the trace: one line per step from 0 for setup, each
/// `{"step":<n>,"obs":<obs>}` for its own step, with `<obs>` hashing to
/// the observation digest the trace records for that step.
pub fn step_views(dir: &std::path::Path) -> Result<Vec<StepView>, String> {
    use sha2::{Digest as _, Sha256};
    let trace = crate::trace::TraceReader::open(dir)
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    let log = crate::sim_acceptance::header::read_bounded_text(
        &dir.join(crate::trace::OBSERVATIONS_FILE),
    )?;
    let lines: Vec<&str> = log.lines().collect();
    if lines.len() != trace.steps.len() + 1 {
        return Err(format!(
            "{}: the observation log has {} lines for {} steps",
            dir.display(),
            lines.len(),
            trace.steps.len()
        ));
    }
    let digests = std::iter::once(trace.init.as_ref().and_then(|init| init.obs.clone()))
        .chain(trace.steps.iter().map(|step| step.obs.clone()));
    let mut obs = Vec::with_capacity(lines.len());
    for (n, (line, digest)) in lines.iter().zip(digests).enumerate() {
        let text = line
            .strip_prefix(&format!(r#"{{"step":{n},"obs":"#))
            .and_then(|rest| rest.strip_suffix('}'))
            .ok_or_else(|| format!("{}: log line {n} isn't step {n}'s", dir.display()))?;
        let hashed: String = Sha256::digest(text.as_bytes())[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if digest.as_deref() != Some(hashed.as_str()) {
            return Err(format!(
                "{}: log line {n} doesn't match the trace's observation digest",
                dir.display()
            ));
        }
        let parsed: Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
        let Value::Array(items) = parsed else {
            return Err(format!(
                "{}: log line {n}'s observations aren't a list",
                dir.display()
            ));
        };
        obs.push(items);
    }
    let mut obs = obs.into_iter();
    let mut views = vec![StepView {
        step: 0,
        pick: String::new(),
        draws: trace.init.map(|init| init.draws).unwrap_or_default(),
        obs: obs.next().unwrap_or_default(),
    }];
    for step in trace.steps {
        views.push(StepView {
            step: step.step,
            pick: step.pick,
            draws: step.draws,
            obs: obs.next().unwrap_or_default(),
        });
    }
    Ok(views)
}

/// `views` without each observation's `site`, for two runs whose fix is a
/// different code path rather than a patch, such as a race fixture's base
/// and fixed variants. Equivalent operations then match whatever lines
/// their two paths are on, and the first difference is in an operation,
/// an object or a pick.
pub fn without_lines(views: Vec<StepView>) -> Vec<StepView> {
    fn strip(value: &Value) -> Value {
        match value {
            Value::Object(fields) => Value::Object(
                fields
                    .iter()
                    .filter(|(key, _)| key.as_str() != "site")
                    .map(|(key, value)| (key.clone(), strip(value)))
                    .collect(),
            ),
            Value::Array(items) => Value::Array(items.iter().map(strip).collect()),
            other => other.clone(),
        }
    }
    views
        .into_iter()
        .map(|view| StepView {
            obs: view.obs.iter().map(strip).collect(),
            ..view
        })
        .collect()
}

/// `obs` with every `site` that names a `file:line` mapped across `map`,
/// and a line a hunk replaced marked so it matches nothing.
fn mapped(obs: &Value, map: &HunkMap) -> Value {
    match obs {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| {
                    let value = match (key.as_str(), value) {
                        ("site", Value::String(site)) => Value::String(map_site(site, map)),
                        _ => mapped(value, map),
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(|v| mapped(v, map)).collect()),
        other => other.clone(),
    }
}

/// `file:line` across `map`, or unchanged if it isn't one.
fn map_site(site: &str, map: &HunkMap) -> String {
    let Some((file, line)) = site.rsplit_once(':') else {
        return site.to_owned();
    };
    let Ok(line) = line.parse::<u64>() else {
        return site.to_owned();
    };
    match map.map(file, line) {
        Some(line) => format!("{file}:{line}"),
        None => format!("{site} (inside a hunk)"),
    }
}

/// The first step where `before`, mapped across `map`, and `after`
/// differ in pick, draws or observations, or where one run ends first.
/// `None` if they agree throughout.
pub fn first_differing_step(before: &[StepView], after: &[StepView], map: &HunkMap) -> Option<u64> {
    for (b, a) in before.iter().zip(after) {
        let obs: Vec<Value> = b.obs.iter().map(|o| mapped(o, map)).collect();
        if b.pick != a.pick || b.draws != a.draws || obs != a.obs {
            return Some(a.step);
        }
    }
    // A run's step count fits in u64.
    (before.len() != after.len()).then(|| before.len().min(after.len()) as u64)
}

/// Checks that `before` and `after` agree up to their first differing
/// step, and that the difference is at `site` on `after`'s side: either an
/// observation `site` names that `before`'s step doesn't hold, or `site`'s
/// pick, with the same draws on both sides, followed by different
/// observations. A site that's merely present beside an unrelated
/// difference doesn't count.
pub fn agreement(
    before: &[StepView],
    after: &[StepView],
    map: &HunkMap,
    site: &Site,
) -> Result<u64, String> {
    let step = first_differing_step(before, after, map)
        .ok_or("the two runs never differ, so the patch changed nothing observable")?;
    let names = |obs: &Value| {
        obs.get("site").and_then(Value::as_str) == Some(site.at.as_str())
            || reached(obs).as_deref() == Some(site.at.as_str())
    };
    let before_view = before.iter().find(|view| view.step == step);
    let before_obs: Vec<Value> = before_view
        .map(|view| view.obs.iter().map(|o| mapped(o, map)).collect())
        .unwrap_or_default();
    let at_site = after
        .iter()
        .find(|view| view.step == step)
        .is_some_and(|view| {
            let changed_obs = view
                .obs
                .iter()
                .any(|obs| names(obs) && !before_obs.contains(obs));
            let after_pick = view.pick == site.at
                && before_view.is_some_and(|b| {
                    b.pick != view.pick || (b.draws == view.draws && before_obs != view.obs)
                });
            changed_obs || after_pick
        });
    if at_site {
        Ok(step)
    } else {
        Err(format!(
            "the runs first differ at step {step}, which isn't at the declared site {}",
            site.at
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::HunkMap;

    /// The inverse map takes a line after the patch back to its line
    /// before it, and a line inside a hunk to nothing, either way.
    #[test]
    fn the_inverse_map_undoes_the_patch() {
        let patch = "+++ b/peer.rs\n@@ -86,18 +86,9 @@\n";
        let map = HunkMap::parse(patch).unwrap();
        let back = map.inverse();
        assert_eq!(map.map("peer.rs", 120), Some(111));
        assert_eq!(back.map("peer.rs", 111), Some(120));
        assert_eq!(back.map("peer.rs", 85), Some(85));
        assert_eq!(map.map("peer.rs", 97), None);
        assert_eq!(back.map("peer.rs", 90), None);
    }
}
