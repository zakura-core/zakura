//! The sink an Exact replay emits into: it compares each record with the
//! trace's.

use super::Trace;
use crate::{
    record::{EndRecord, InitRecord, StepRecord},
    Record, RecordSink,
};

/// Compares a replay's `init` and step records with a trace's, and keeps the
/// replay's `end` record for [`end_difference`]. It can also keep a copy of
/// every record, for evidence.
///
/// The decider has already checked each pick and draw, and echoes the
/// recorded `by` and offset, so a differing record in practice differs in its
/// `obs` digest. The sink can't stop the run, so it keeps the first
/// difference and counts the rest.
pub(crate) struct VerifySink<'t> {
    init: Option<&'t InitRecord>,
    steps: &'t [StepRecord],
    /// Records that differed.
    pub(crate) mismatches: u64,
    /// The first record that differed: its step, 0 for `init`, and field.
    pub(crate) first: Option<(u64, &'static str)>,
    /// Step records received.
    pub(crate) steps_seen: usize,
    /// The replay's `end` record, once emitted.
    pub(crate) end: Option<EndRecord>,
    /// Whether to keep a copy of every record.
    keep: bool,
    /// Every record received, in order, if `keep` is set.
    pub(crate) kept: Vec<Record>,
}

impl<'t> VerifySink<'t> {
    /// A sink that checks records against `trace`, and keeps a copy of
    /// each if `keep` is set.
    pub(crate) fn new(trace: &'t Trace, keep: bool) -> Self {
        VerifySink {
            init: trace.init.as_ref(),
            steps: &trace.steps,
            mismatches: 0,
            first: None,
            steps_seen: 0,
            end: None,
            keep,
            kept: Vec::new(),
        }
    }

    /// Notes that the record at `step` differs in `field`.
    fn mismatch(&mut self, step: u64, field: &'static str) {
        self.mismatches += 1;
        self.first.get_or_insert((step, field));
    }
}

impl RecordSink for VerifySink<'_> {
    fn record(&mut self, record: Record) {
        if self.keep {
            self.kept.push(record.clone());
        }
        match record {
            Record::Init(init) => {
                if self.init != Some(&init) {
                    self.mismatch(0, "init");
                }
            }
            Record::Step(step) => {
                self.steps_seen += 1;
                let recorded = usize::try_from(step.step)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|index| self.steps.get(index));
                if let Some(field) = recorded.map_or(Some("step"), |r| step_difference(r, &step)) {
                    self.mismatch(step.step, field);
                }
            }
            Record::End(end) => self.end = Some(end),
            // A run never emits `meta`; the trace writer adds it.
            Record::Meta(_) => {}
        }
    }
}

/// The first field where step record `produced` differs from `recorded`.
fn step_difference(recorded: &StepRecord, produced: &StepRecord) -> Option<&'static str> {
    // Destructured, so a new field can't be left out of the comparison.
    let StepRecord {
        step,
        pick,
        key,
        options,
        by,
        offset,
        draws,
        obs,
    } = recorded;
    [
        ("step", *step == produced.step),
        ("pick", *pick == produced.pick),
        ("key", *key == produced.key),
        ("options", *options == produced.options),
        ("by", *by == produced.by),
        ("offset", *offset == produced.offset),
        ("draws", *draws == produced.draws),
        ("obs", *obs == produced.obs),
    ]
    .into_iter()
    .find_map(|(field, same)| (!same).then_some(field))
}

/// The first field where `end` record `produced` differs from `recorded`, as
/// `end.<field>`.
pub(crate) fn end_difference(recorded: &EndRecord, produced: &EndRecord) -> Option<&'static str> {
    // Destructured, so a new field can't be left out of the comparison.
    let EndRecord {
        outcome,
        status,
        steps,
        violation,
        drain,
        harness,
        cleanup,
        properties,
    } = recorded;
    [
        ("end.outcome", *outcome == produced.outcome),
        ("end.status", *status == produced.status),
        ("end.steps", *steps == produced.steps),
        ("end.violation", *violation == produced.violation),
        ("end.drain", *drain == produced.drain),
        ("end.harness", *harness == produced.harness),
        ("end.cleanup", *cleanup == produced.cleanup),
        ("end.properties", *properties == produced.properties),
    ]
    .into_iter()
    .find_map(|(field, same)| (!same).then_some(field))
}

/// The first record where trace `other` differs from `trace`: its step, 0
/// for `init`, and field, with an `end` field at the last step `trace`
/// holds. A step one trace lacks differs in `step`.
pub(crate) fn trace_difference(trace: &Trace, other: &Trace) -> Option<(u64, &'static str)> {
    if trace.init != other.init {
        return Some((0, "init"));
    }
    let shared = trace.steps.len().min(other.steps.len());
    if let Some(difference) =
        trace
            .steps
            .iter()
            .zip(&other.steps)
            .find_map(|(recorded, produced)| {
                step_difference(recorded, produced).map(|field| (recorded.step, field))
            })
    {
        return Some(difference);
    }
    if trace.steps.len() != other.steps.len() {
        // A usize always fits in a u64 on supported targets.
        return Some((shared as u64 + 1, "step"));
    }
    let last = trace.steps.last().map_or(0, |step| step.step);
    match (&trace.end, &other.end) {
        (Some(end), Some(other)) => end_difference(end, other).map(|field| (last, field)),
        (None, None) => None,
        _ => Some((last, "end")),
    }
}
