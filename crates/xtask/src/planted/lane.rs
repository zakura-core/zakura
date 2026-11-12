//! The campaign lane that the mutation pilot's discovery experiment and the
//! mutation loop's libFuzzer generator lane share (mutation plan, mut-3 and
//! mut-4.4): building a fuzz target and running one bounded campaign on it
//! ([`campaign_lane`]), reading what its store shows ([`campaign_facts`]),
//! and confirming a finding in fresh processes ([`confirm_and_replay`]).
//! It also reads why a build failed ([`BuildFailure`]).

use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{outcome::NaReason, pilot::INTERRUPTED_ERROR};
use crate::{
    sim_minimize::{run_child, ChildEnd, INTERRUPTED},
    BoxError,
};

/// How long past its exploration budget a campaign's `start` may run, for
/// its startup, snapshot and end record, before the runner kills it.
const CAMPAIGN_GRACE: Duration = Duration::from_secs(300);

/// The file `zakura-sim confirm` writes into its `--out`.
pub(in crate::planted) const CONFIRMATION_FILE: &str = "confirmation.json";

/// The variable the fuzz target's filter reads. A campaign runs without
/// it, so every property's violation counts.
pub(in crate::planted) const EXPECT_VAR: &str = "ZAKURA_SIM_EXPECT";

/// The sanitizer option variables a campaign's manifest records. A
/// campaign lane runs without them.
const SANITIZER_VARS: &[&str] = &[
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "MSAN_OPTIONS",
    "TSAN_OPTIONS",
    "UBSAN_OPTIONS",
];

/// One campaign's libFuzzer bounds, as `zakura-sim campaign start` takes
/// them.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(in crate::planted) struct CampaignBounds {
    pub(in crate::planted) seconds_per_seed: u64,
    pub(in crate::planted) workers: usize,
    pub(in crate::planted) max_len: u64,
    pub(in crate::planted) input_timeout_seconds: u64,
    pub(in crate::planted) rss_limit_mb: u64,
    pub(in crate::planted) max_restarts: u64,
    pub(in crate::planted) abandon_limit: u64,
    pub(in crate::planted) feedback: String,
    pub(in crate::planted) sanitizer: String,
}

/// What one campaign lane runs.
#[derive(Clone, Copy, Debug)]
pub(in crate::planted) struct LaneSpec<'a> {
    /// The fuzz target, from `qa/fuzz/sim`.
    pub(in crate::planted) target: &'a str,
    /// The harness the target runs.
    pub(in crate::planted) harness: &'a str,
    /// The campaign's name in its store.
    pub(in crate::planted) name: &'a str,
    pub(in crate::planted) seed: u32,
    /// The directory the campaign's initial inputs come from.
    pub(in crate::planted) corpus: &'a Path,
}

/// Why a build failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::planted) enum BuildFailure {
    /// The source is invalid: the compiler rejected it with an error at a
    /// source location, or a mutant's patch doesn't apply or changes files
    /// it doesn't declare. For a mutant, it's an invalid mutant.
    Rejected(String),
    /// The build failed without a compile error, such as a compiler killed
    /// for memory, a full disk or a failed link. It's a harness failure.
    Broken(String),
}

impl BuildFailure {
    /// The failure of `what`'s build, said in words.
    pub(in crate::planted) fn of(&self, what: &str) -> BuildFailure {
        match self {
            BuildFailure::Rejected(why) => {
                BuildFailure::Rejected(format!("{what} doesn't compile: {why}"))
            }
            BuildFailure::Broken(why) => {
                BuildFailure::Broken(format!("{what}'s build broke: {why}"))
            }
        }
    }
}

impl std::fmt::Display for BuildFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildFailure::Rejected(why) | BuildFailure::Broken(why) => f.write_str(why),
        }
    }
}

/// Why a campaign lane has no facts.
#[derive(Debug)]
pub(in crate::planted) enum LaneError {
    /// The fuzz target didn't build.
    Build(BuildFailure),
    /// The campaign didn't leave the records the runner reads.
    Run(String),
    /// `campaign start` ran past its deadline, or a campaign process died
    /// from a signal.
    Killed(String),
}

impl std::fmt::Display for LaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneError::Build(failure) => failure.fmt(f),
            LaneError::Run(why) | LaneError::Killed(why) => f.write_str(why),
        }
    }
}

/// The records one campaign left in its store.
#[derive(Clone, Debug)]
pub(in crate::planted) struct Campaign {
    /// The run's end record, if it wrote one.
    pub(in crate::planted) end: Option<Value>,
    /// What `zakura-sim campaign evidence` summed for the run.
    pub(in crate::planted) evidence: Value,
    /// Inputs in the run's initial snapshot.
    pub(in crate::planted) initial_inputs: usize,
    /// The build identity the run's manifest records.
    pub(in crate::planted) identity: Value,
}

impl Campaign {
    /// What the records show about `property` ([`campaign_facts`]).
    pub(in crate::planted) fn facts(&self, property: &str) -> CampaignFacts {
        campaign_facts(
            property,
            self.end.as_ref(),
            &self.evidence,
            self.initial_inputs,
        )
    }
}

/// How long `campaign start` may run with `bounds`: its budget and a grace.
pub(in crate::planted) fn start_deadline(bounds: &CampaignBounds) -> Duration {
    Duration::from_secs(bounds.seconds_per_seed) + CAMPAIGN_GRACE
}

/// Builds `spec`'s fuzz target in `fuzz_target`, and runs one campaign on
/// it with `bounds` and `spec`'s seed, from `spec`'s corpus, in a fresh
/// store under `dir`, with `campaign start` under `deadline`. `sim` makes
/// each child's `zakura-sim` command, such as one under a memory cap.
/// Returns the records the run left.
pub(in crate::planted) fn campaign_lane(
    sim: &dyn Fn() -> Command,
    dir: &Path,
    fuzz_target: &Path,
    spec: &LaneSpec<'_>,
    bounds: &CampaignBounds,
    deadline: Duration,
) -> Result<Result<Campaign, LaneError>, BoxError> {
    let store = dir.join("store");
    if store.exists() {
        return Err(format!("{} isn't a fresh store", store.display()).into());
    }
    fs::create_dir_all(dir)?;
    let mut build = sim();
    build
        .args(["campaign", "build", "--store"])
        .arg(&store)
        .args([
            "--target",
            spec.target,
            "--sanitizer",
            &bounds.sanitizer,
            "--target-dir",
        ])
        .arg(fuzz_target)
        // Uncolored diagnostics, which `fuzz_build_failure` reads.
        .env("CARGO_TERM_COLOR", "never");
    let log = dir.join("build.log");
    match run_lane_child(&mut build, &log, None)? {
        ChildEnd::Exited(0) => {}
        end => {
            let why = format!("`zakura-sim campaign build` {end}; see {}", log.display());
            let text = fs::read(&log).unwrap_or_default();
            return Ok(Err(LaneError::Build(fuzz_build_failure(
                end,
                &String::from_utf8_lossy(&text),
                why,
            ))));
        }
    }
    let Some(build_digest) = only_entry(&store.join("builds"))? else {
        return Ok(Err(LaneError::Run(
            "the store holds no single build".to_owned(),
        )));
    };
    let mut start = sim();
    start.args(start_args(
        bounds,
        spec.name,
        spec.harness,
        &build_digest,
        spec.seed,
        spec.corpus,
        &store,
    ));
    start.env_remove(EXPECT_VAR);
    for var in SANITIZER_VARS {
        start.env_remove(var);
    }
    let log = dir.join("start.log");
    // `start` exits 1 for any run that isn't complete, as a run a finding
    // stopped, so the store's records decide. A run past the deadline, or
    // one a signal killed, may have left records, but it didn't finish.
    match run_lane_child(&mut start, &log, Some(deadline))? {
        ChildEnd::TimedOut => {
            return Ok(Err(LaneError::Killed(format!(
                "`zakura-sim campaign start` ran past {} s; see {}",
                deadline.as_secs(),
                log.display()
            ))))
        }
        end @ ChildEnd::Signaled(_) => {
            return Ok(Err(LaneError::Killed(format!(
                "`zakura-sim campaign start` {end}; see {}",
                log.display()
            ))))
        }
        _ => {}
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
    let mut evidence = sim();
    evidence
        .args(["campaign", "evidence", "--store"])
        .arg(&store)
        .args(["--run", &run, "--out"])
        .arg(&evidence_file);
    let log = dir.join("evidence.log");
    let evidence = match run_lane_child(&mut evidence, &log, None)? {
        ChildEnd::Exited(0) => serde_json::from_slice::<Value>(&fs::read(&evidence_file)?)?,
        end @ ChildEnd::Signaled(_) => {
            return Ok(Err(LaneError::Killed(format!(
                "`zakura-sim campaign evidence` {end}; see {}",
                log.display()
            ))))
        }
        end => {
            return Ok(Err(LaneError::Run(format!(
                "`zakura-sim campaign evidence` {end}; see {}",
                log.display()
            ))))
        }
    };
    Ok(Ok(Campaign {
        end,
        evidence,
        initial_inputs,
        identity: manifest
            .pointer("/build/identity")
            .cloned()
            .unwrap_or(Value::Null),
    }))
}

/// `campaign start`'s arguments for one lane: every bound comes from
/// `bounds`, so paired campaigns with one seed differ only in `store`,
/// `name` and `build`.
pub(in crate::planted) fn start_args(
    bounds: &CampaignBounds,
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

/// What one campaign showed, from its store's records.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(in crate::planted) struct CampaignFacts {
    /// How the run ended: `complete`, `worker-stopped` or `interrupted`, or
    /// `none` without an end record.
    pub(in crate::planted) status: String,
    /// Inputs in the run's initial snapshot.
    pub(in crate::planted) initial_inputs: usize,
    /// The properties a saved input violates, in order, each once, under
    /// any of the `active`, `known` and `draft` buckets.
    pub(in crate::planted) violated: Vec<String>,
    /// The properties of [`CampaignFacts::violated`] that a saved input
    /// violates under the `active` bucket: the violations a fuzz target
    /// stops on. A `known` or `draft` one is a note.
    pub(in crate::planted) active: Vec<String>,
    /// Saved failures that aren't a property violation and that no worker
    /// death explains: a harness failure, crash, abandoned run,
    /// nondeterministic rerun or run without an end that the target saved,
    /// and a libFuzzer OOM or leak.
    pub(in crate::planted) failures: Vec<String>,
    /// libFuzzer's timeout inputs, and its crash inputs no saved violation
    /// explains: a worker that ran past its input timeout or died from a
    /// signal.
    pub(in crate::planted) killed: Vec<String>,
    /// Every way the run's counts fall short, as the evidence lists it.
    pub(in crate::planted) incomplete: Vec<String>,
    /// True if the intended property applied in some complete process.
    pub(in crate::planted) applied: bool,
    /// Inputs the run's processes counted.
    pub(in crate::planted) runs: u64,
    /// Inputs where the intended property applied, in complete processes.
    pub(in crate::planted) applicable_runs: u64,
    /// Inputs where the intended property ended `not-applicable` for an
    /// incomplete reason, such as `assumption-lost`, in complete processes.
    pub(in crate::planted) incomplete_runs: u64,
    /// How long the workers ran, in milliseconds.
    pub(in crate::planted) millis: u64,
    /// The first saved input that violates the intended property, relative
    /// to the store.
    pub(in crate::planted) intended_input: Option<String>,
    /// Why the run's fuzzer build doesn't pair with the catalogue's, if it
    /// doesn't.
    pub(in crate::planted) unpaired: Option<String>,
}

/// What a saved failure file is, from its path in the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::planted) enum Failure {
    /// An input saved as a violation of this property, under an
    /// `active`, `known` or `draft` bucket.
    Violation(String),
    /// libFuzzer's crash input, which a saved violation may explain.
    Crash,
    /// libFuzzer's timeout input: a worker ran past its input timeout.
    Timeout,
    /// Any other failure: a harness failure, crash, abandoned run,
    /// nondeterministic rerun or run without an end that the target saved,
    /// and a libFuzzer OOM or leak.
    Other,
    /// A file that goes with a saved input, such as its trace.
    Companion,
}

/// What the store file at `path` is. A worker's target saves inputs as
/// `.../sim/inputs/<bucket>/[<property>/]<sha256>.bin`, and libFuzzer saves
/// `.../artifacts/<kind>-<sha1>`.
pub(in crate::planted) fn failure_kind(path: &str) -> Failure {
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
        Some(file) if artifact && file.starts_with("timeout-") => Failure::Timeout,
        Some(_) if artifact => Failure::Other,
        _ => Failure::Companion,
    }
}

/// The bucket of a worker target's saved input at `path`, such as
/// `active`, or `None` for another file.
fn bucket(path: &str) -> Option<&str> {
    let segments: Vec<&str> = path.split('/').collect();
    let at = segments
        .windows(2)
        .position(|pair| matches!(pair[0], "sim" | "compact-sim") && pair[1] == "inputs")?;
    segments.get(at + 2).copied()
}

/// Why `campaign` isn't a complete, clean run from the empty corpus, other
/// than a property violation, or `None` if it is: it pairs with its
/// expected build, started from no inputs, left no failure, completed, and
/// its evidence lists nothing incomplete.
pub(in crate::planted) fn campaign_failure(campaign: &CampaignFacts) -> Option<String> {
    if let Some(why) = &campaign.unpaired {
        return Some(why.clone());
    }
    if campaign.initial_inputs > 0 {
        return Some(format!(
            "started from {} inputs, not the frozen empty corpus",
            campaign.initial_inputs
        ));
    }
    if let Some(why) = left_failures(campaign) {
        return Some(why);
    }
    if campaign.status != "complete" || !campaign.incomplete.is_empty() {
        return Some(format!(
            "ended {} with incomplete evidence: {}",
            campaign.status,
            campaign.incomplete.join(", ")
        ));
    }
    None
}

/// The saved failures and worker deaths `campaign` left, said in words, or
/// `None` if it left none.
pub(in crate::planted) fn left_failures(campaign: &CampaignFacts) -> Option<String> {
    let left: Vec<&str> = campaign
        .failures
        .iter()
        .chain(&campaign.killed)
        .map(String::as_str)
        .collect();
    (!left.is_empty()).then(|| format!("left failures: {}", left.join(", ")))
}

/// What a campaign's end record `end` and `evidence` show about
/// `property`, for a run whose initial snapshot held `initial_inputs`.
pub(in crate::planted) fn campaign_facts(
    property: &str,
    end: Option<&Value>,
    evidence: &Value,
    initial_inputs: usize,
) -> CampaignFacts {
    let mut violated = BTreeSet::new();
    let mut active = BTreeSet::new();
    let (mut failures, mut killed, mut crashes, mut intended_input) =
        (Vec::new(), Vec::new(), Vec::new(), None);
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
                if bucket(path) == Some("active") {
                    active.insert(violation.clone());
                }
                violated.insert(violation);
            }
            Failure::Crash => crashes.push(path.to_owned()),
            Failure::Timeout => killed.push(path.to_owned()),
            Failure::Other => failures.push(path.to_owned()),
            Failure::Companion => {}
        }
    }
    if violated.is_empty() {
        killed.extend(crashes);
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
        active: active.into_iter().collect(),
        failures,
        killed,
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
        incomplete_runs: evidence
            .pointer("/complete_totals/properties")
            .and_then(|properties| properties.get(property))
            .and_then(|counts| counts.get("not_applicable"))
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .filter(|(reason, _)| !NaReason::is_benign_name(reason))
            .filter_map(|(_, runs)| runs.as_u64())
            .sum(),
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

/// Confirms `input`'s violation of `property` on `harness` in a fresh
/// process, beside `also_violates`, then replays the confirmed trace in
/// another, writing under `dir`. `sim` makes each child's `zakura-sim`
/// command, such as one under a memory cap. Each child gets `child`.
/// Returns the confirmed trace's directory, or why the input didn't
/// confirm or replay.
pub(in crate::planted) fn confirm_and_replay(
    sim: &dyn Fn() -> Command,
    harness: &str,
    input: &Path,
    property: &str,
    also_violates: &[String],
    dir: &Path,
    child: Duration,
) -> Result<Result<PathBuf, String>, BoxError> {
    let confirm_dir = dir.join("confirm");
    let mut confirm = sim();
    confirm
        .args(["confirm", "--sim", harness, "--input"])
        .arg(input)
        .args(["--property", property, "--out"])
        .arg(&confirm_dir);
    if !also_violates.is_empty() {
        confirm.args(["--also-violates", &also_violates.join(",")]);
    }
    let ended = run_lane_child(&mut confirm, &dir.join("confirm.log"), Some(child))?;
    let confirmation = fs::read(confirm_dir.join(CONFIRMATION_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let trace = match confirmed_trace(ended, confirmation.as_ref()) {
        Ok(trace) => confirm_dir.join(trace),
        Err(why) => return Ok(Err(why)),
    };
    let log = dir.join("replay.log");
    let mut replay = sim();
    replay.arg("replay").arg(&trace);
    Ok(match run_lane_child(&mut replay, &log, Some(child))? {
        ChildEnd::Exited(0) => Ok(trace),
        end => Err(format!(
            "the confirmed trace doesn't replay Exact: `zakura-sim replay` {end}; see {}",
            log.display()
        )),
    })
}

/// The confirmed trace's directory, relative to the confirmation's, from a
/// `zakura-sim confirm` that ended `ended` and wrote `confirmation`, or why
/// it didn't confirm.
pub(in crate::planted) fn confirmed_trace(
    ended: ChildEnd,
    confirmation: Option<&Value>,
) -> Result<PathBuf, String> {
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

/// What a failed `cargo build --message-format=json` that ended `end` and
/// wrote `log` shows, with `why` as its reason. It's a compile error only if
/// cargo exited with a code, rather than a signal, and rustc reported an
/// error at a source location in a path package. A full disk, a killed
/// compiler or a failed link reports no source location.
pub(in crate::planted) fn sim_build_failure(end: ChildEnd, log: &str, why: String) -> BuildFailure {
    let located = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| {
            message.get("reason").and_then(Value::as_str) == Some("compiler-message")
                && message
                    .get("package_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id.contains("path+file://"))
        })
        .filter_map(|message| message.get("message").cloned())
        .find(|message| {
            message.get("level").and_then(Value::as_str) == Some("error")
                && message
                    .get("spans")
                    .and_then(Value::as_array)
                    .is_some_and(|spans| !spans.is_empty())
        })
        .map(|message| {
            let text = message.get("message").and_then(Value::as_str);
            let at = message
                .pointer("/spans/0/file_name")
                .and_then(Value::as_str);
            let line = message
                .pointer("/spans/0/line_start")
                .and_then(Value::as_u64);
            format!(
                "{} at {}:{}",
                text.unwrap_or("an error"),
                at.unwrap_or("?"),
                line.unwrap_or(0)
            )
        });
    compile_error_or_broken(end, located, why)
}

/// What a failed `zakura-sim campaign build` that ended `end` and wrote
/// `log` shows, with `why` as its reason. The pinned simulator runs `cargo
/// fuzz build`, which writes no JSON messages, so the signal is rustc's
/// rendered diagnostic: an `error` line whose next line gives its `-->`
/// source location. A full disk, a killed compiler, a failed link or an
/// internal compiler error renders none.
pub(in crate::planted) fn fuzz_build_failure(
    end: ChildEnd,
    log: &str,
    why: String,
) -> BuildFailure {
    let lines: Vec<&str> = log.lines().collect();
    let located = lines.windows(2).find_map(|pair| {
        let at = pair[1].trim_start().strip_prefix("--> ")?;
        is_rustc_error(pair[0]).then(|| format!("{} at {at}", pair[0].trim_end()))
    });
    compile_error_or_broken(end, located, why)
}

/// True if `line` heads a rustc error, such as `error[E0308]: mismatched
/// types` or `error: cannot find value`, but not an internal compiler
/// error.
fn is_rustc_error(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("error") else {
        return false;
    };
    let rest = match rest.strip_prefix("[E") {
        Some(code) => match code.split_once(']') {
            Some((digits, rest)) if digits.bytes().all(|b| b.is_ascii_digit()) => rest,
            _ => return false,
        },
        None => rest,
    };
    rest.starts_with(": ") && !rest.starts_with(": internal compiler error")
}

/// A rejected build if the build exited with a nonzero code and the
/// compiler `located` an error, otherwise a broken one.
fn compile_error_or_broken(end: ChildEnd, located: Option<String>, why: String) -> BuildFailure {
    match (end, located) {
        (ChildEnd::Exited(code), Some(error)) if code != 0 => {
            BuildFailure::Rejected(format!("{error}; {why}"))
        }
        _ => BuildFailure::Broken(why),
    }
}

/// Runs `command` with [`run_child`] and the flag that the runner's signal
/// handler sets. An interrupt kills the child's group and stops the runner.
pub(in crate::planted) fn run_lane_child(
    command: &mut Command,
    log: &Path,
    deadline: Option<Duration>,
) -> Result<ChildEnd, BoxError> {
    stop_on_interrupt(run_child(command, log, deadline, &INTERRUPTED)?)
}

/// `end`, or the error that stops the runner if an interrupt ended the
/// child.
pub(in crate::planted) fn stop_on_interrupt(end: ChildEnd) -> Result<ChildEnd, BoxError> {
    match end {
        ChildEnd::Interrupted => Err(INTERRUPTED_ERROR.into()),
        end => Ok(end),
    }
}

/// The name of the only entry in `dir`, or `None` if it holds none or
/// several.
pub(in crate::planted) fn only_entry(dir: &Path) -> Result<Option<String>, BoxError> {
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
