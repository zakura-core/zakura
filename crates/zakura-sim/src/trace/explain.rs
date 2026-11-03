//! `replay --explain`: a trace as readable text (traces.md, Trace shrinking).
//!
//! Each step shows its pick, with a task pick's task name and point, its
//! draws, and the observations the replay made at it. A `*` marks every
//! non-default pick and draw, the choices a witness test's author scripts.

use std::fmt::Write;

use super::Trace;
use crate::{
    plan::By,
    record::{DrawRecord, EndRecord, OutcomeKind},
    ObservationLog,
};

/// `trace` as text, with each step's observations from `log`, the log of
/// a replay of `trace`.
pub fn explain(trace: &Trace, log: &ObservationLog) -> String {
    let mut out = String::new();
    // Writing to a String can't fail.
    let _ = write_explanation(&mut out, trace, log);
    out
}

/// Writes [`explain`]'s text to `out`.
fn write_explanation(out: &mut String, trace: &Trace, log: &ObservationLog) -> std::fmt::Result {
    let non_default = trace
        .steps
        .iter()
        .filter(|step| step.by == By::Bytes)
        .count();
    writeln!(
        out,
        "trace {} of {}: {} steps, {non_default} non-default picks. `*` marks a \
         non-default pick or draw.",
        trace.id,
        trace.meta.sim,
        trace.steps.len()
    )?;
    let init = trace.init.iter().flat_map(|init| &init.draws);
    if init.clone().next().is_some() || log.of_step(0).next().is_some() {
        writeln!(out, "setup")?;
        for draw in init {
            write_draw(out, draw)?;
        }
        write_observations(out, log, 0)?;
    }
    for step in &trace.steps {
        let mark = mark(step.by);
        let by = match step.by {
            By::Bytes => "chosen",
            By::Default => "default",
            By::Forced => "forced",
        };
        writeln!(
            out,
            "step {} {mark} {} [{by}, {} enabled]",
            step.step,
            pick(&step.pick),
            step.options
        )?;
        for draw in &step.draws {
            write_draw(out, draw)?;
        }
        write_observations(out, log, step.step)?;
    }
    if let Some(end) = &trace.end {
        write_end(out, end)?;
    }
    Ok(())
}

/// A pick as text: `task <name> at <point>` for a task, else its identity.
/// Task names never hold a `:`, so the first one after the name ends it.
fn pick(identity: &str) -> String {
    match identity
        .strip_prefix("task:")
        .and_then(|rest| rest.split_once(':'))
    {
        Some((name, at)) => format!("task {name} at {at}"),
        None => identity.to_owned(),
    }
}

/// `*` for a non-default choice, else a space.
fn mark(by: By) -> char {
    match by {
        By::Bytes => '*',
        By::Default | By::Forced => ' ',
    }
}

/// One draw's line.
fn write_draw(out: &mut String, draw: &DrawRecord) -> std::fmt::Result {
    let bound = match draw.bound.0 {
        0 => "any u64".to_owned(),
        _ => format!("below {}", draw.bound),
    };
    writeln!(
        out,
        "    {} draw {} = {} ({bound})",
        mark(draw.by),
        draw.draw,
        draw.value
    )
}

/// One line per observation `log` holds for `step`.
fn write_observations(out: &mut String, log: &ObservationLog, step: u64) -> std::fmt::Result {
    for obs in log.of_step(step) {
        // Observations hold only strings and integers, so this can't fail.
        let json = serde_json::to_string(obs).map_err(|_| std::fmt::Error)?;
        writeln!(out, "      {json}")?;
    }
    Ok(())
}

/// How the run ended.
fn write_end(out: &mut String, end: &EndRecord) -> std::fmt::Result {
    match (&end.outcome, &end.violation) {
        (OutcomeKind::Violation, Some(violation)) => writeln!(
            out,
            "end: {} violated at step {}, evidence steps {:?}: expected {}, actual {}",
            violation.property,
            violation.step,
            violation.witness,
            violation.expected,
            violation.actual
        ),
        (OutcomeKind::Harness, _) => writeln!(
            out,
            "end: harness failure: {}",
            end.harness.as_deref().unwrap_or("unknown")
        ),
        (OutcomeKind::Violation, None) => writeln!(out, "end: violation"),
        (OutcomeKind::Budget, _) => writeln!(out, "end: the budget ran out"),
        (OutcomeKind::Idle, _) => writeln!(out, "end: nothing was enabled"),
    }
}
