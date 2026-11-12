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
    let (_, new_len) = range(parts.next()?, '+')?;
    Some(Hunk {
        old_start,
        old_len,
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
/// `--observe` log a fresh replay wrote beside it, one line per step from
/// 0 for setup. The log must have exactly one line per step.
pub fn step_views(dir: &std::path::Path) -> Result<Vec<StepView>, String> {
    let trace = crate::trace::TraceReader::open(dir)
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    let log = crate::sim_acceptance::header::read_bounded_text(
        &dir.join(crate::trace::OBSERVATIONS_FILE),
    )?;
    let lines: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
        .collect::<Result<_, _>>()?;
    if lines.len() != trace.steps.len() + 1 {
        return Err(format!(
            "{}: the observation log has {} lines for {} steps",
            dir.display(),
            lines.len(),
            trace.steps.len()
        ));
    }
    let obs = |n: usize| -> Vec<Value> {
        lines[n]
            .get("obs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut views = vec![StepView {
        step: 0,
        pick: String::new(),
        draws: trace.init.map(|init| init.draws).unwrap_or_default(),
        obs: obs(0),
    }];
    for (n, step) in trace.steps.into_iter().enumerate() {
        views.push(StepView {
            step: step.step,
            pick: step.pick,
            draws: step.draws,
            obs: obs(n + 1),
        });
    }
    Ok(views)
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
/// step, and that `after`'s side of that step is at `site`.
pub fn agreement(
    before: &[StepView],
    after: &[StepView],
    map: &HunkMap,
    site: &Site,
) -> Result<u64, String> {
    let step = first_differing_step(before, after, map)
        .ok_or("the two runs never differ, so the patch changed nothing observable")?;
    let at_site = after
        .iter()
        .find(|view| view.step == step)
        .is_some_and(|view| {
            view.pick == site.at
                || view.obs.iter().any(|obs| {
                    obs.get("site").and_then(Value::as_str) == Some(site.at.as_str())
                        || reached(obs).as_deref() == Some(site.at.as_str())
                })
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
