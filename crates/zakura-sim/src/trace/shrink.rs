//! The shrinker: reduce a failing trace to fewer non-default picks on the
//! build that recorded it (traces.md, Trace shrinking).
//!
//! [`shrink`] runs five passes:
//!
//! 1. **Confirm.** Replay the trace Exact. A trace that doesn't replay, such
//!    as a nondeterministic one, is refused and kept as it is.
//! 2. **Cut the tail.** Drop every step after the violation step.
//! 3. **Delete external picks.** Delete peer, delivery, drop and storage
//!    picks in chunks, largest first, then halving. Deleting a pick also
//!    deletes every later pick from the same peer, link or storage
//!    operation ([`delete_picks`]).
//! 4. **Default the scheduling picks.** Replace each remaining non-default
//!    pick or draw with the default, one at a time, from last to first.
//!    Repeat until a full pass accepts nothing or a limit is reached.
//! 5. **Re-record.** Rerun the best candidate's choices as a scripted
//!    [`Plan`] in record mode. The result is a fresh Exact trace, whose
//!    `meta.derived_from` names the original.
//!
//! Each candidate runs on the same build with a [`SkipScript`], so a
//! recorded pick that a deletion made unreachable is skipped, and the
//! default pick runs instead. The shrinker accepts a candidate only if it
//! violates the same property and passes that property's trigger-history
//! check, the rule a `violated` witness test applies
//! ([`crate::witness::witness`]). A candidate that violates another
//! property, panics a task, or ends at a harness failure is rejected.
//!
//! A default pick is the Plan's round-robin pick, and a default draw is the
//! one an exhausted input gets. Every other pick or draw is non-default.
//! Each accepted candidate has fewer non-default picks and draws than the
//! one before, so the passes end. Only a pass 4 that reaches a fixed point
//! shows that each remaining non-default pick is needed.
//!
//! The shrinker never changes the original trace. It writes into its output
//! directory:
//!
//! - `original/`: a copy of the original trace;
//! - `best-plan.json`: the best candidate's choices as a scripted [`Plan`],
//!   rewritten after each accepted candidate, so an interrupted or killed
//!   shrink keeps it;
//! - `reductions.jsonl`: one [`Reduction`] per accepted candidate;
//! - `reduced/`: the re-recorded trace;
//! - `report.json`: the [`ShrinkReport`].

use std::{
    fmt, fs,
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value;

use super::{
    failure::{on_new_thread, replay_on_new_thread},
    meta::{Derivation, DerivationTool},
    read::refuse_symlinked_dir,
    ReadError, ReplayError, Trace, BLOBS_DIR, INPUT_FILE, TRACE_FILE,
};
pub use crate::plan::Candidate;
use crate::{
    harness::{self, HarnessEntry, RecordInput},
    plan::{By, SkipScript},
    record::{DrawRecord, EndRecord, Record, StepRecord},
    witness::{Expect, WitnessError},
    Category, MemorySink, Plan, PlanChoice, PlanInput,
};

/// The shrinker's version, as `meta.derived_from` records it.
pub const SHRINK_VERSION: u32 = 1;

/// The copy of the original trace in the output directory.
pub const ORIGINAL_DIR: &str = "original";

/// The re-recorded trace in the output directory.
pub const REDUCED_DIR: &str = "reduced";

/// The best candidate's choices, as a scripted [`Plan`].
pub const BEST_PLAN_FILE: &str = "best-plan.json";

/// One [`Reduction`] per line.
pub const REDUCTIONS_FILE: &str = "reductions.jsonl";

/// The [`ShrinkReport`].
pub const REPORT_FILE: &str = "report.json";

/// When the shrinker stops reducing. explorer.md sets the defaults: 2,000
/// candidates or 10 minutes per failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ShrinkBudget {
    /// The most candidates to run, not counting the run of the trace's own
    /// choices that starts the shrink.
    pub candidates: u32,
    /// The longest the passes may run. A candidate that has started runs to
    /// its end.
    #[serde(rename = "wall_ms", serialize_with = "millis")]
    pub wall: Duration,
}

impl Default for ShrinkBudget {
    fn default() -> Self {
        ShrinkBudget {
            candidates: 2_000,
            wall: Duration::from_secs(10 * 60),
        }
    }
}

/// A trace's size, as the report measures it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Counts {
    /// Picks and draws whose `by` is `bytes`, `init` draws included.
    pub non_default: u64,
    /// Steps the run executed, drain included.
    pub events: u64,
}

impl Counts {
    /// `trace`'s counts. A trace without `end` counts its steps.
    pub fn of(trace: &Trace) -> Counts {
        let init = trace.init.as_ref().map_or(&[][..], |init| &init.draws);
        Counts {
            non_default: non_default(init, &trace.steps),
            // A usize always fits in a u64 on supported targets.
            events: trace
                .end
                .as_ref()
                .map_or(trace.steps.len() as u64, |end| end.steps),
        }
    }
}

/// Why the shrinker stopped before pass 4 reached a fixed point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stop {
    /// It ran [`ShrinkBudget::candidates`] candidates.
    CandidateLimit,
    /// It ran for [`ShrinkBudget::wall`].
    WallClock,
    /// The caller's interrupt flag was set.
    Interrupted,
}

impl fmt::Display for Stop {
    /// What stopped the passes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Stop::CandidateLimit => "the candidate limit",
            Stop::WallClock => "the wall-clock limit",
            Stop::Interrupted => "an interrupt",
        })
    }
}

/// One accepted candidate, as a line of [`REDUCTIONS_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Reduction {
    /// The pass that made it: 2, 3 or 4.
    pub pass: u8,
    /// Its number among the candidates run, from 1.
    pub candidate: u32,
    /// What changed, in the steps of the candidate before it.
    pub change: String,
    /// The candidate's run's non-default picks and draws.
    pub non_default: u64,
    /// The candidate's run's executed steps.
    pub events: u64,
}

/// What a shrink did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ShrinkReport {
    /// The violated property every candidate had to violate.
    pub property: String,
    /// The original trace's ID.
    pub original: String,
    /// The re-recorded trace's ID. `None` if the re-record failed.
    pub reduced: Option<String>,
    /// Why the re-record failed.
    pub rerecord_failure: Option<String>,
    /// The original trace's counts.
    pub before: Counts,
    /// The re-recorded trace's counts. `None` if the re-record failed.
    pub after: Option<Counts>,
    /// True if a full pass 4 accepted no candidate.
    pub fixed_point: bool,
    /// Why the passes stopped early, if they did.
    pub stopped: Option<Stop>,
    /// The candidates run.
    pub candidates: u32,
    /// The candidates accepted.
    pub accepted: u32,
    /// The limits.
    pub budget: ShrinkBudget,
    /// How long the shrink took.
    #[serde(rename = "elapsed_ms", serialize_with = "millis")]
    pub elapsed: Duration,
}

/// Why the shrinker reduced nothing. It wrote nothing, or only the copy of
/// the original and the candidates so far.
#[derive(Debug)]
pub enum ShrinkError {
    /// No harness has `meta.sim`'s name.
    UnknownHarness(String),
    /// Pass 1 failed: the trace doesn't replay Exact on this build. It is
    /// refused and kept as it is.
    Confirm(ReplayError),
    /// The trace's run violated no property.
    NoViolation,
    /// The trace's own choices don't pass the acceptance rule, so no
    /// candidate could: the property has no trigger-history check, or the
    /// run fails it.
    Baseline {
        /// The property.
        property: String,
        /// Why the run fails the rule.
        error: WitnessError,
    },
    /// The output directory isn't empty or is a symlink.
    Out(String),
    /// Writing the output failed.
    Io(io::Error),
}

impl fmt::Display for ShrinkError {
    /// Names the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShrinkError::UnknownHarness(error) => error.fmt(f),
            ShrinkError::Confirm(error) => write!(
                f,
                "the trace doesn't replay Exact, so it can't be shrunk: {error}"
            ),
            ShrinkError::NoViolation => f.write_str("the trace's run violated no property"),
            ShrinkError::Baseline { property, error } => write!(
                f,
                "the trace's own choices fail the acceptance rule for {property}: {error}"
            ),
            ShrinkError::Out(reason) => write!(f, "the output directory {reason}"),
            ShrinkError::Io(error) => write!(f, "writing the shrink output failed: {error}"),
        }
    }
}

impl std::error::Error for ShrinkError {}

impl From<io::Error> for ShrinkError {
    fn from(error: io::Error) -> Self {
        ShrinkError::Io(error)
    }
}

/// Shrinks `trace` on the registered harness its `meta.sim` names, writing
/// into `out`, which must be missing or empty.
///
/// Every run (the confirm replay, each candidate, and the re-record and its
/// replay) builds its `Sim` on a new thread. A run that leaks tasks parks
/// its thread for good, so the tasks' thread-local state never breaks a
/// later run (#1694).
pub fn shrink(
    trace: &Trace,
    budget: ShrinkBudget,
    out: &Path,
) -> Result<ShrinkReport, ShrinkError> {
    let harness = harness::find(&trace.meta.sim)
        .map_err(|error| ShrinkError::UnknownHarness(error.to_string()))?;
    shrink_with(trace, harness, budget, out, &AtomicBool::new(false))
}

/// [`shrink`] on `harness`, whatever `meta.sim` names. The passes stop
/// before the next candidate once `interrupt` is set, and the shrink then
/// re-records the best candidate so far.
pub fn shrink_with(
    trace: &Trace,
    harness: &'static HarnessEntry,
    budget: ShrinkBudget,
    out: &Path,
    interrupt: &AtomicBool,
) -> Result<ShrinkReport, ShrinkError> {
    let started = Instant::now();

    // Pass 1: confirm. The replay also refuses a build that isn't
    // equivalent to the recording one.
    let confirmed = replay_on_new_thread(trace.clone(), harness).map_err(ShrinkError::Confirm)?;
    let end = trace
        .end
        .as_ref()
        .expect("a verified replay read a complete trace");
    let property = target(end).ok_or(ShrinkError::NoViolation)?;
    let violation_step = confirmed
        .properties
        .iter()
        .find(|report| report.property == property)
        .and_then(|report| report.violations.first())
        .map(|violation| violation.step)
        .or_else(|| end.violation.as_ref().map(|violation| violation.step));

    prepare_out(out, trace)?;
    let mut shrinker = Shrinker {
        harness,
        scenario: &trace.meta.scenario,
        property: property.clone(),
        budget,
        started,
        interrupt,
        candidates: 0,
        accepted: 0,
        out,
        write_error: None,
    };
    let init = trace
        .init
        .as_ref()
        .map_or_else(Vec::new, |init| init.draws.clone());
    let mut best = shrinker.baseline(init, trace.steps.clone())?;

    // Passes 2 to 4. A limit or an interrupt stops them, and the best
    // candidate so far is re-recorded.
    let stopped = shrinker.reduce(&mut best, violation_step).err();
    let fixed_point = stopped.is_none();

    // Pass 5: re-record.
    let derivation = Derivation {
        trace: trace.id.clone(),
        tool: DerivationTool::Shrink,
        tool_version: SHRINK_VERSION,
    };
    let (reduced, after, rerecord_failure) = match rerecord(
        harness,
        &trace.meta.scenario,
        &best.plan,
        &derivation,
        &property,
        &out.join(REDUCED_DIR),
    ) {
        Ok(reduced) => (Some(reduced.id.clone()), Some(Counts::of(&reduced)), None),
        Err(reason) => (None, None, Some(reason)),
    };
    let report = ShrinkReport {
        property,
        original: trace.id.clone(),
        reduced,
        rerecord_failure,
        before: Counts::of(trace),
        after,
        fixed_point,
        stopped,
        candidates: shrinker.candidates,
        accepted: shrinker.accepted,
        budget,
        elapsed: started.elapsed(),
    };
    write_json(&out.join(REPORT_FILE), &report)?;
    match shrinker.write_error {
        Some(error) => Err(ShrinkError::Io(error)),
        None => Ok(report),
    }
}

/// The property the shrink keeps violated: the one that ended the run, or
/// else the first violated one in `end.properties`, such as a `Known`
/// property that let the run go on.
fn target(end: &EndRecord) -> Option<String> {
    end.violation
        .as_ref()
        .map(|violation| violation.property.clone())
        .or_else(|| {
            end.properties
                .iter()
                .find(|report| matches!(report.outcome, crate::PropertyOutcome::Violated { .. }))
                .map(|report| report.property.clone())
        })
}

/// Checks that `out` is missing or empty and isn't a symlink, then copies
/// `trace` into its [`ORIGINAL_DIR`].
fn prepare_out(out: &Path, trace: &Trace) -> Result<(), ShrinkError> {
    refuse_symlinked_dir(out).map_err(|error| match error {
        ReadError::Io(error) => ShrinkError::Io(error),
        other => ShrinkError::Out(other.to_string()),
    })?;
    fs::create_dir_all(out)?;
    if fs::read_dir(out)?.next().is_some() {
        return Err(ShrinkError::Out(format!("{} isn't empty", out.display())));
    }
    let original = out.join(ORIGINAL_DIR);
    fs::create_dir(&original)?;
    fs::copy(trace.dir.join(TRACE_FILE), original.join(TRACE_FILE))?;
    if trace.meta.input.is_some() {
        fs::copy(trace.dir.join(INPUT_FILE), original.join(INPUT_FILE))?;
    }
    // Blobs found in the committed store instead stay there.
    for blob in &trace.meta.fixtures {
        let from = trace.dir.join(BLOBS_DIR).join(&blob.sha256);
        if from.is_file() {
            fs::create_dir_all(original.join(BLOBS_DIR))?;
            fs::copy(&from, original.join(BLOBS_DIR).join(&blob.sha256))?;
        }
    }
    Ok(())
}

/// An accepted candidate's run: its choices, ready to be the next
/// candidate's base.
#[derive(Debug, Default)]
struct Run {
    /// The `init` draws.
    init: Vec<DrawRecord>,
    /// The steps, up to the last one with a non-default choice. The steps
    /// after it take the default anyway, as a candidate's missing steps do.
    steps: Vec<StepRecord>,
    /// Non-default picks and draws.
    non_default: u64,
    /// Steps executed.
    events: u64,
    /// The run's choices as a scripted [`Plan`].
    plan: Plan,
}

impl Run {
    /// The run whose records `sink` holds.
    fn from_records(sink: MemorySink) -> Run {
        let mut init = Vec::new();
        let mut steps = Vec::new();
        let mut end = None;
        for record in sink.records {
            match record {
                Record::Init(record) => init = record.draws,
                Record::Step(step) => steps.push(step),
                Record::End(record) => end = Some(record),
                Record::Meta(_) => {}
            }
        }
        let end = end.expect("every run emits its end record");
        let drained = end.drain.map_or(0, |drain| drain.steps);
        let plan = script(&init, &steps, drained);
        let last = steps
            .iter()
            .rposition(|step| step.by == By::Bytes || bytes_draws(&step.draws))
            .map_or(0, |index| index + 1);
        steps.truncate(last);
        Run {
            non_default: non_default(&init, &steps),
            init,
            steps,
            events: end.steps,
            plan,
        }
    }
}

/// True if a draw in `draws` was chosen by bytes.
fn bytes_draws(draws: &[DrawRecord]) -> bool {
    draws.iter().any(|draw| draw.by == By::Bytes)
}

/// The picks and draws among `init` and `steps` that bytes chose.
fn non_default(init: &[DrawRecord], steps: &[StepRecord]) -> u64 {
    let draws = |draws: &[DrawRecord]| draws.iter().filter(|draw| draw.by == By::Bytes).count();
    let total = draws(init)
        + steps
            .iter()
            .map(|step| usize::from(step.by == By::Bytes) + draws(&step.draws))
            .sum::<usize>();
    // A usize always fits in a u64 on supported targets.
    total as u64
}

/// The scripted [`Plan`] that makes the choices `init` and `steps` record,
/// whose last `drained` steps are the drain.
///
/// A script entry answers each pick among more than one action and each
/// draw with a bound other than 1, except the drain's picks, which take
/// round-robin without reading the script. A default choice between two
/// others is a [`PlanChoice::Default`] entry, and defaults after the last
/// other choice need none.
fn script(init: &[DrawRecord], steps: &[StepRecord], drained: u64) -> Plan {
    let mut choices = Vec::new();
    let mut defaults = 0;
    let mut note = |by: By, choice: PlanChoice| match by {
        By::Bytes => {
            choices.extend(std::iter::repeat_n(
                PlanChoice::Default,
                std::mem::take(&mut defaults),
            ));
            choices.push(choice);
        }
        By::Default => defaults += 1,
        By::Forced => {}
    };
    let value = |draw: &DrawRecord| PlanChoice::Draw(draw.value);
    for draw in init {
        note(draw.by, value(draw));
    }
    // The drain is at most every step, so this can't underflow.
    let live = steps
        .len()
        .saturating_sub(usize::try_from(drained).unwrap_or(usize::MAX));
    for (index, step) in steps.iter().enumerate() {
        if index < live {
            note(step.by, PlanChoice::Pick(step.pick.clone()));
        }
        for draw in &step.draws {
            note(draw.by, value(draw));
        }
    }
    Plan { choices }
}

/// The peer, link or storage operation that the external `pick` comes
/// from, or `None` for a task pick or a time advance.
///
/// Peer `p1`'s picks share `peer:p1`, the link `p1->node`'s deliveries and
/// drops share `link:p1->node`, and storage operation 7 of `header-store`
/// shares `storage:header-store:7` across its outcomes.
pub fn source(pick: &str) -> Option<String> {
    let (prefix, rest) = pick.split_once(':')?;
    if prefix == Category::Peer.prefix() {
        let (peer, _action) = rest.split_once(':')?;
        Some(format!("peer:{peer}"))
    } else if prefix == Category::Deliver.prefix() || prefix == Category::Drop.prefix() {
        let (link, _seq) = rest.rsplit_once(':')?;
        Some(format!("link:{link}"))
    } else if prefix == Category::Storage.prefix() {
        let (operation, _outcome) = rest.rsplit_once(':')?;
        Some(format!("storage:{operation}"))
    } else {
        None
    }
}

/// `steps` without the candidates at `picks`, and without every later
/// recorded candidate from the same [`source`] as one of them: pass 3's
/// deletion. A deleted pick's event stays in the scenario, so a later
/// default pick can still take it.
pub fn delete_picks(steps: &[Candidate], picks: &[usize]) -> Vec<Candidate> {
    let recorded_source = |candidate: &Candidate| match candidate {
        Candidate::Recorded(step) => source(&step.pick),
        Candidate::Default => None,
    };
    let mut deleted = vec![false; steps.len()];
    for &index in picks {
        let Some(gone) = steps.get(index).and_then(recorded_source) else {
            continue;
        };
        for (later, candidate) in steps.iter().enumerate().skip(index) {
            if later == index || recorded_source(candidate).as_ref() == Some(&gone) {
                deleted[later] = true;
            }
        }
    }
    steps
        .iter()
        .zip(deleted)
        .filter(|(_, deleted)| !deleted)
        .map(|(candidate, _)| candidate.clone())
        .collect()
}

/// A choice pass 4 can make the default: an `init` draw, a step's pick, or
/// a step's draw. Ordered as the run makes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Choice {
    /// `init` draw `n`.
    Init(usize),
    /// The pick, or with `Some(n)` draw `n`, of the step at this index.
    Step(usize, Option<usize>),
}

/// The state of one shrink.
struct Shrinker<'a> {
    harness: &'static HarnessEntry,
    scenario: &'a Value,
    property: String,
    budget: ShrinkBudget,
    started: Instant,
    interrupt: &'a AtomicBool,
    /// Candidates run, not counting the baseline.
    candidates: u32,
    accepted: u32,
    out: &'a Path,
    /// The first failed write of the reduction log or the best plan.
    write_error: Option<io::Error>,
}

impl Shrinker<'_> {
    /// Runs the trace's own choices as the first candidate, and fails if
    /// they don't pass the acceptance rule.
    fn baseline(
        &mut self,
        init: Vec<DrawRecord>,
        steps: Vec<StepRecord>,
    ) -> Result<Run, ShrinkError> {
        let steps = steps.into_iter().map(Candidate::Recorded).collect();
        let run = self
            .run(init, steps)
            .map_err(|error| ShrinkError::Baseline {
                property: self.property.clone(),
                error,
            })?;
        self.save_best(&run)?;
        Ok(run)
    }

    /// Passes 2 to 4 on `base`, which holds the best candidate so far
    /// even when a limit stops them. `Ok` means pass 4 reached a fixed
    /// point.
    fn reduce(&mut self, base: &mut Run, violation_step: Option<u64>) -> Result<(), Stop> {
        if let Some(step) = violation_step {
            self.cut_tail(base, step)?;
        }
        self.delete_external(base)?;
        self.default_scheduling(base)
    }

    /// Pass 2: drops the steps after `step`, if one of them made a
    /// non-default choice.
    fn cut_tail(&mut self, base: &mut Run, step: u64) -> Result<(), Stop> {
        let keep = usize::try_from(step).unwrap_or(usize::MAX);
        if base.steps.len() <= keep {
            return Ok(());
        }
        let steps = base.steps[..keep]
            .iter()
            .cloned()
            .map(Candidate::Recorded)
            .collect();
        let change = format!("cut the steps after step {step}");
        if let Some(run) = self.attempt(2, base, base.init.clone(), steps, change)? {
            *base = run;
        }
        Ok(())
    }

    /// Pass 3: deletes chunks of non-default external picks, largest first,
    /// then halving down to single picks ([`delete_picks`]).
    fn delete_external(&mut self, base: &mut Run) -> Result<(), Stop> {
        let mut size = external_picks(&base.steps).len();
        while size > 0 {
            let mut start = 0;
            loop {
                let picks = external_picks(&base.steps);
                if start >= picks.len() {
                    break;
                }
                let chunk = &picks[start..picks.len().min(start + size)];
                let recorded: Vec<Candidate> = base
                    .steps
                    .iter()
                    .cloned()
                    .map(Candidate::Recorded)
                    .collect();
                let steps = delete_picks(&recorded, chunk);
                let deleted: Vec<String> = chunk
                    .iter()
                    .map(|&index| {
                        let step = &base.steps[index];
                        format!("step {} {}", step.step, step.pick)
                    })
                    .collect();
                let change = format!(
                    "deleted {}, with the later picks from their sources",
                    deleted.join(", ")
                );
                match self.attempt(3, base, base.init.clone(), steps, change)? {
                    // The chunk's place now holds later picks, so try it
                    // again.
                    Some(run) => *base = run,
                    None => start += size,
                }
            }
            size /= 2;
        }
        Ok(())
    }

    /// Pass 4: makes each non-default choice the default, from last to
    /// first, until a full pass accepts nothing.
    fn default_scheduling(&mut self, base: &mut Run) -> Result<(), Stop> {
        loop {
            let mut accepted = false;
            let mut before: Option<Choice> = None;
            while let Some(choice) = last_choice(base, before) {
                before = Some(choice);
                let (init, steps, change) = defaulted(base, choice);
                if let Some(run) = self.attempt(4, base, init, steps, change)? {
                    *base = run;
                    accepted = true;
                }
            }
            if !accepted {
                return Ok(());
            }
        }
    }

    /// Runs one candidate, and returns its run if it's accepted. Fails with
    /// the limit that stops the passes, before running anything.
    fn attempt(
        &mut self,
        pass: u8,
        base: &Run,
        init: Vec<DrawRecord>,
        steps: Vec<Candidate>,
        change: String,
    ) -> Result<Option<Run>, Stop> {
        if self.interrupt.load(Ordering::Relaxed) {
            return Err(Stop::Interrupted);
        }
        if self.candidates >= self.budget.candidates {
            return Err(Stop::CandidateLimit);
        }
        if self.started.elapsed() >= self.budget.wall {
            return Err(Stop::WallClock);
        }
        self.candidates += 1;
        let Ok(run) = self.run(init, steps) else {
            return Ok(None);
        };
        debug_assert!(
            run.non_default < base.non_default,
            "a candidate drops at least one non-default choice of its base, and its \
             run makes no other"
        );
        self.accepted += 1;
        let reduction = Reduction {
            pass,
            candidate: self.candidates,
            change,
            non_default: run.non_default,
            events: run.events,
        };
        // A failed write doesn't stop the passes, but fails the shrink at
        // its end.
        if let Err(error) = self.log(&reduction).and_then(|()| self.save_best(&run)) {
            self.write_error.get_or_insert(error);
        }
        Ok(Some(run))
    }

    /// Runs `init` and `steps` as a [`SkipScript`], and judges the run as a
    /// `violated` witness test of the property.
    fn run(&self, init: Vec<DrawRecord>, steps: Vec<Candidate>) -> Result<Run, WitnessError> {
        let (witness, scenario, property) = (
            self.harness.witness,
            self.scenario.clone(),
            self.property.clone(),
        );
        on_new_thread(move || {
            let mut sink = MemorySink::default();
            let judged = witness(
                &scenario,
                PlanInput::Skip(SkipScript::new(init, steps)),
                &property,
                Expect::Violated,
                &mut sink,
            );
            let clean = sink
                .records
                .iter()
                .any(|record| matches!(record, Record::End(end) if end.cleanup.leaked == 0));
            (judged.map(|()| Run::from_records(sink)), clean)
        })
    }

    /// Appends `reduction` to [`REDUCTIONS_FILE`].
    fn log(&self, reduction: &Reduction) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.out.join(REDUCTIONS_FILE))?;
        let line = serde_json::to_string(reduction).map_err(io::Error::other)?;
        writeln!(file, "{line}")
    }

    /// Replaces [`BEST_PLAN_FILE`] with `run`'s plan.
    fn save_best(&self, run: &Run) -> io::Result<()> {
        let path = self.out.join(BEST_PLAN_FILE);
        let partial = path.with_extension("json.partial");
        write_json(&partial, &run.plan)?;
        fs::rename(partial, path)
    }
}

/// The indices of `steps` whose pick is external and non-default.
fn external_picks(steps: &[StepRecord]) -> Vec<usize> {
    steps
        .iter()
        .enumerate()
        .filter(|(_, step)| step.by == By::Bytes && source(&step.pick).is_some())
        .map(|(index, _)| index)
        .collect()
}

/// `base`'s last non-default choice before `before`, or its last one if
/// `before` is `None`.
fn last_choice(base: &Run, before: Option<Choice>) -> Option<Choice> {
    let bytes = |draws: &[DrawRecord]| {
        draws
            .iter()
            .enumerate()
            .filter(|(_, draw)| draw.by == By::Bytes)
            .map(|(n, _)| n)
            .collect::<Vec<_>>()
    };
    let init = bytes(&base.init).into_iter().map(Choice::Init);
    let steps = base.steps.iter().enumerate().flat_map(|(index, step)| {
        let pick = (step.by == By::Bytes).then_some(Choice::Step(index, None));
        let draws = bytes(&step.draws)
            .into_iter()
            .map(move |n| Choice::Step(index, Some(n)));
        pick.into_iter().chain(draws)
    });
    init.chain(steps)
        .filter(|choice| before.is_none_or(|before| *choice < before))
        .max()
}

/// The candidate that makes `choice` of `base` the default, and its
/// description.
fn defaulted(base: &Run, choice: Choice) -> (Vec<DrawRecord>, Vec<Candidate>, String) {
    let mut init = base.init.clone();
    let mut steps: Vec<Candidate> = base
        .steps
        .iter()
        .cloned()
        .map(Candidate::Recorded)
        .collect();
    let change = match choice {
        Choice::Init(n) => {
            init[n].by = By::Default;
            format!("defaulted setup draw {}", init[n].draw)
        }
        Choice::Step(index, draw) => {
            let mut step = base.steps[index].clone();
            let change = match draw {
                None => {
                    step.by = By::Default;
                    format!("defaulted step {} {}", step.step, step.pick)
                }
                Some(n) => {
                    step.draws[n].by = By::Default;
                    format!("defaulted step {} draw {}", step.step, step.draws[n].draw)
                }
            };
            steps[index] = if step.by == By::Bytes || bytes_draws(&step.draws) {
                Candidate::Recorded(step)
            } else {
                Candidate::Default
            };
            change
        }
    };
    (init, steps, change)
}

/// Pass 5: records `plan` against `scenario` into `out` as a trace derived
/// by the shrinker, and checks that it violates `property` with its trigger
/// history and replays Exact. Returns the trace, or why it failed.
fn rerecord(
    harness: &'static HarnessEntry,
    scenario: &Value,
    plan: &Plan,
    derivation: &Derivation,
    property: &str,
    out: &Path,
) -> Result<Trace, String> {
    let (scenario, plan, derivation, property, out) = (
        scenario.clone(),
        plan.clone(),
        derivation.clone(),
        property.to_owned(),
        out.to_path_buf(),
    );
    let (trace, judged) = on_new_thread(move || {
        let what = RecordInput::Script {
            scenario: &scenario,
            plan: &plan,
            derived_from: Some(&derivation),
        };
        let recorded = harness::record_witness(harness, what, &out, &property, Expect::Violated);
        let clean = matches!(&recorded, Ok((trace, _))
            if trace.end.as_ref().is_some_and(|end| end.cleanup.leaked == 0));
        (recorded, clean)
    })
    .map_err(|error| format!("recording failed: {error}"))?;
    judged.map_err(|error| format!("the re-recorded run fails the acceptance rule: {error}"))?;
    replay_on_new_thread(trace.clone(), harness)
        .map_err(|error| format!("the re-recorded trace doesn't replay: {error}"))?;
    Ok(trace)
}

/// Writes `value` to `path` as JSON, with a final newline.
fn write_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let mut text = serde_json::to_string_pretty(value).map_err(io::Error::other)?;
    text.push('\n');
    fs::write(path, text)
}

/// Serializes a duration as whole milliseconds, since records hold no
/// floats.
fn millis<S: serde::Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}
