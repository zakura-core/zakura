//! Confirming a fuzz input's failure, for `cargo xtask sim-minimize`
//! (traces implementation plan, T2b).
//!
//! `cargo fuzz tmin` keeps any input that crashes the target. Under
//! `ZAKURA_SIM_EXPECT` only the expected property's `Active` violation
//! panics, but an abort, a sanitizer report or a timeout still crashes the
//! process, so `tmin`'s output is only a candidate. [`confirm`] judges one
//! input as a `violated` witness test does, records its trace with
//! [`record_failure`], and says what it found. [`Confirmation::against`]
//! then holds a candidate to the original's contract: the same harness,
//! generator and build, the same declared assumptions with the same
//! parameters, and the same cleanup.
//!
//! Each [`ConfirmStatus`] names one finding. A confirmation never calls a
//! failure unknown.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    failure::on_new_thread,
    meta::{hex, BuildInfo, InputRef},
    record_failure,
};
use crate::{
    explore::{Classified, ExploreOutcome, ExploreRun, Worker},
    harness::{HarnessEntry, RecordError},
    property::{AssumeRecord, PropertyId, Status},
    witness::{Expect, WitnessError},
    NullSink, Plan, PropertyOutcome,
};

/// The file [`confirm`]'s callers write the [`Confirmation`] to.
pub const CONFIRMATION_FILE: &str = "confirmation.json";

/// What a confirmation found. Each status means one thing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfirmStatus {
    /// The input violates the property, the classifier selects it, its
    /// trigger history holds, and its trace replays Exact. Against an
    /// original, it also keeps the original's contract.
    Confirmed,
    /// The run violates no property.
    NotViolated,
    /// The run fails on another `Active` property, not the expected one. A
    /// task panic is one: it violates `sim/no-task-panic`.
    OtherProperty,
    /// The run violates the property, but the fuzz target's filter can't
    /// select it alone: the classifier selects another failure, the
    /// property isn't `Active`, or another property is violated too.
    SelectionUnsupported,
    /// The property has no trigger-history check, so no run can show the
    /// causal witness.
    WitnessUnsupported,
    /// The run violates the property without reaching its trigger history.
    WitnessLost,
    /// The run ended at a harness failure, so its evidence is incomplete.
    IncompleteEvidence,
    /// A rerun of the input didn't do what the first run did. Nondeterminism
    /// isn't a property violation, and nothing minimizes it.
    Nondeterministic,
    /// The candidate declares other assumptions, or other parameters for
    /// them, than the original.
    AssumptionsChanged,
    /// The candidate ran on another harness, generator, trace format or
    /// contract, or on a build that isn't equivalent to the original's.
    ProvenanceChanged,
    /// The candidate's shutdown leaked tasks and the original's didn't, or
    /// the other way round.
    CleanupChanged,
}

impl ConfirmStatus {
    /// The status's name, as the JSON writes it.
    pub fn name(self) -> &'static str {
        match self {
            ConfirmStatus::Confirmed => "confirmed",
            ConfirmStatus::NotViolated => "not-violated",
            ConfirmStatus::OtherProperty => "other-property",
            ConfirmStatus::SelectionUnsupported => "selection-unsupported",
            ConfirmStatus::WitnessUnsupported => "witness-unsupported",
            ConfirmStatus::WitnessLost => "witness-lost",
            ConfirmStatus::IncompleteEvidence => "incomplete-evidence",
            ConfirmStatus::Nondeterministic => "nondeterministic",
            ConfirmStatus::AssumptionsChanged => "assumptions-changed",
            ConfirmStatus::ProvenanceChanged => "provenance-changed",
            ConfirmStatus::CleanupChanged => "cleanup-changed",
        }
    }
}

impl fmt::Display for ConfirmStatus {
    /// Writes [`ConfirmStatus::name`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A property the run reported `Violated`, with its status in the harness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViolatedProperty {
    /// The property's ID.
    pub property: String,
    /// Its status. `None` if the harness doesn't register it.
    pub status: Option<Status>,
}

/// What [`confirm`] found for one input, with the facts a candidate's
/// confirmation is held to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Confirmation {
    /// The finding.
    pub status: ConfirmStatus,
    /// What the finding is about, such as the other property's ID.
    pub detail: Option<String>,
    /// The property the confirmation expects violated.
    pub property: String,
    /// The harness's name.
    pub harness: String,
    /// The harness's generator version.
    pub generator: u32,
    /// The trace format version.
    pub format: u32,
    /// The property contract version.
    pub contract: u32,
    /// The build that ran the input.
    pub build: BuildInfo,
    /// The input's digest and length.
    pub input: InputRef,
    /// The scenario the input's prefix drew.
    pub scenario: Value,
    /// The assumptions the scenario declares, with their parameters.
    pub assumptions: Vec<AssumeRecord>,
    /// The classifier's outcome: `fail`, `harness-failure`, `abandoned` or
    /// `clean`.
    pub outcome: String,
    /// The property the classifier selected, for a `fail` outcome.
    pub selected: Option<String>,
    /// Every property the run reported `Violated`, in report order.
    pub violated: Vec<ViolatedProperty>,
    /// Tasks the run's shutdown leaked.
    pub leaked: usize,
    /// The recorded trace's directory, relative to the confirmation's
    /// directory. Set only for [`ConfirmStatus::Confirmed`] and the
    /// statuses [`Confirmation::against`] adds.
    pub trace: Option<PathBuf>,
    /// The recorded trace's ID, beside [`Confirmation::trace`].
    pub trace_id: Option<String>,
}

impl Confirmation {
    /// This confirmation of a candidate, held to `original`'s contract. A
    /// candidate that isn't confirmed keeps its own status. A confirmed one
    /// becomes [`ConfirmStatus::ProvenanceChanged`],
    /// [`ConfirmStatus::OtherProperty`],
    /// [`ConfirmStatus::AssumptionsChanged`] or
    /// [`ConfirmStatus::CleanupChanged`] at the first difference, checked in
    /// that order.
    pub fn against(mut self, original: &Confirmation) -> Confirmation {
        if self.status != ConfirmStatus::Confirmed {
            return self;
        }
        let change = if self.harness != original.harness
            || self.generator != original.generator
            || self.format != original.format
            || self.contract != original.contract
        {
            Some((
                ConfirmStatus::ProvenanceChanged,
                format!(
                    "the original ran on {} generator {} (format {}, contract {}), the \
                     candidate on {} generator {} (format {}, contract {})",
                    original.harness,
                    original.generator,
                    original.format,
                    original.contract,
                    self.harness,
                    self.generator,
                    self.format,
                    self.contract
                ),
            ))
        } else if !self.build.equivalent(&original.build) {
            Some((
                ConfirmStatus::ProvenanceChanged,
                format!(
                    "the candidate's build (binary {}) isn't equivalent to the original's \
                     (binary {})",
                    self.build.binary, original.build.binary
                ),
            ))
        } else if self.property != original.property {
            Some((
                ConfirmStatus::OtherProperty,
                format!(
                    "the original violates {}, the candidate {}",
                    original.property, self.property
                ),
            ))
        } else if self.assumptions != original.assumptions {
            Some((
                ConfirmStatus::AssumptionsChanged,
                format!(
                    "the original declares {}, the candidate {}",
                    json(&original.assumptions),
                    json(&self.assumptions)
                ),
            ))
        } else if (self.leaked > 0) != (original.leaked > 0) {
            Some((
                ConfirmStatus::CleanupChanged,
                format!(
                    "the original's shutdown leaked {} tasks, the candidate's {}",
                    original.leaked, self.leaked
                ),
            ))
        } else {
            None
        };
        if let Some((status, detail)) = change {
            self.status = status;
            self.detail = Some(detail);
        }
        self
    }
}

/// `value` as compact JSON.
fn json(value: &impl Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| format!("<{error}>"))
}

/// Confirms that the full fuzz `input` makes `entry`'s harness violate
/// `property`, and records its trace under `out`.
///
/// The input runs with a [`NullSink`] on a [`Worker`], and the explorer's
/// classifier reads the run, as in the fuzz target. Then:
///
/// 1. the classifier must select `property`, as `ZAKURA_SIM_EXPECT` does,
///    and no other property may be violated;
/// 2. a rerun on a new thread must pass the `violated` witness test of
///    `property`, which checks its trigger history;
/// 3. [`record_failure`] records the trace under `out/traces/`, checks that
///    it violates `property` at the same step, and replays it Exact.
///
/// The first check that fails sets the status. A run that passes all three
/// is [`ConfirmStatus::Confirmed`], with its trace.
///
/// # Errors
///
/// If the harness can't build the scenario's `meta`, or the trace can't be
/// written. A finding about the run is never an error.
pub fn confirm(
    entry: &'static HarnessEntry,
    input: &[u8],
    property: &str,
    out: &Path,
) -> Result<Confirmation, RecordError> {
    // An ID that isn't valid can't name a registered property.
    let id = PropertyId::generated(property.to_owned())
        .map_err(|_| RecordError::Witness(WitnessError::Unregistered))?;
    let split = entry.scenario_bytes.min(input.len());
    let mut prefix = input[..split].to_vec();
    prefix.resize(entry.scenario_bytes, 0);
    let scenario = (entry.draw)(&prefix);
    let meta = (entry.meta)(&scenario).map_err(RecordError::Harness)?;
    let confirmation = Confirmation {
        status: ConfirmStatus::Confirmed,
        detail: None,
        property: property.to_owned(),
        harness: meta.sim,
        generator: meta.generator,
        format: meta.format,
        contract: meta.contract,
        build: meta.build,
        input: InputRef {
            sha256: hex(&Sha256::digest(input)),
            // A usize always fits in a u64 on supported targets.
            len: input.len() as u64,
        },
        scenario: meta.scenario,
        assumptions: meta.assumptions,
        outcome: String::new(),
        selected: None,
        violated: Vec::new(),
        leaked: 0,
        trace: None,
        trace_id: None,
    };
    let out = out.to_path_buf();
    Worker::new().explore(entry, input, move |input, run, classified| {
        judge(entry, input, &id, run, classified, &out, confirmation)
    })
}

/// [`confirm`]'s checks on the classified `run` of `input`, filling in
/// `confirmation`.
fn judge(
    entry: &'static HarnessEntry,
    input: &[u8],
    property: &PropertyId,
    run: &ExploreRun,
    classified: &Classified,
    out: &Path,
    mut confirmation: Confirmation,
) -> Result<Confirmation, RecordError> {
    let want = property.as_str();
    confirmation.leaked = run.leaked();
    confirmation.violated = run
        .report()
        .map(|report| {
            report
                .properties
                .iter()
                .filter(|report| matches!(report.outcome, PropertyOutcome::Violated { .. }))
                .map(|report| ViolatedProperty {
                    property: report.property.clone(),
                    status: entry.status(&report.property),
                })
                .collect()
        })
        .unwrap_or_default();
    let (outcome, selected) = match &classified.outcome {
        ExploreOutcome::Fail { property } => ("fail", Some(property.clone())),
        ExploreOutcome::HarnessFailure { .. } => ("harness-failure", None),
        ExploreOutcome::Abandoned { .. } => ("abandoned", None),
        ExploreOutcome::Clean => ("clean", None),
    };
    confirmation.outcome = outcome.to_owned();
    confirmation.selected = selected.clone();
    let others: Vec<&str> = confirmation
        .violated
        .iter()
        .map(|violated| violated.property.as_str())
        .filter(|other| *other != want)
        .collect();
    let violates = others.len() < confirmation.violated.len();
    let finding = match (&classified.outcome, selected) {
        (ExploreOutcome::HarnessFailure { reason }, _) => Some((
            ConfirmStatus::IncompleteEvidence,
            format!("the run ended at a harness failure: {reason}"),
        )),
        (_, Some(other)) if !violates => Some((
            ConfirmStatus::OtherProperty,
            format!("the run fails on {other}"),
        )),
        _ if !violates => Some((ConfirmStatus::NotViolated, format!("the run is {outcome}"))),
        (_, Some(other)) if other != want => Some((
            ConfirmStatus::SelectionUnsupported,
            format!("the classifier selects {other}, and the filter keys on it"),
        )),
        (_, None) => Some((
            ConfirmStatus::SelectionUnsupported,
            format!("{want} isn't Active here, and the filter selects only an Active failure"),
        )),
        _ if !others.is_empty() => Some((
            ConfirmStatus::SelectionUnsupported,
            format!("the run also violates {}", others.join(", ")),
        )),
        _ => None,
    };
    if let Some((status, detail)) = finding {
        confirmation.status = status;
        confirmation.detail = Some(detail);
        return Ok(confirmation);
    }

    if let Err(error) = witness(entry, input, want, &confirmation.scenario, run) {
        let status = match error {
            WitnessError::Unregistered | WitnessError::NoTriggerHistory => {
                ConfirmStatus::WitnessUnsupported
            }
            WitnessError::LostTriggerHistory => ConfirmStatus::WitnessLost,
            WitnessError::HarnessEnd(_) | WitnessError::NoEnd => ConfirmStatus::IncompleteEvidence,
            // The first run violated the property alone, so a rerun that
            // finds anything else disagrees with it.
            WitnessError::UnsupportedScenario(_)
            | WitnessError::MissingReport
            | WitnessError::Outcome(_)
            | WitnessError::Unrelated { .. }
            | WitnessError::Fixed { .. } => ConfirmStatus::Nondeterministic,
        };
        confirmation.status = status;
        confirmation.detail = Some(format!("the witness test of {want}: {error}"));
        return Ok(confirmation);
    }

    let report = run
        .report()
        .expect("a run the classifier selected a property of has a report");
    let scenario = confirmation.scenario.clone();
    match record_failure(entry, &scenario, input, property, report, out) {
        Ok(trace) => {
            confirmation.trace = Some(
                trace
                    .dir
                    .strip_prefix(out)
                    .map_or_else(|_| trace.dir.clone(), Path::to_path_buf),
            );
            confirmation.trace_id = Some(trace.id);
            Ok(confirmation)
        }
        Err(RecordError::Nondeterministic { detail }) => {
            confirmation.status = ConfirmStatus::Nondeterministic;
            confirmation.detail = Some(detail);
            Ok(confirmation)
        }
        Err(error) => Err(error),
    }
}

/// Runs the `violated` witness test of `property` on `input` on a new
/// thread. The thread parks for good if the first `run` leaked tasks, as
/// the rerun then likely does too.
fn witness(
    entry: &'static HarnessEntry,
    input: &[u8],
    property: &str,
    scenario: &Value,
    run: &ExploreRun,
) -> Result<(), WitnessError> {
    let plan = input[entry.scenario_bytes.min(input.len())..].to_vec();
    let (property, scenario) = (property.to_owned(), scenario.clone());
    let clean = run.leaked() == 0;
    on_new_thread(move || {
        let result = (entry.witness)(
            &scenario,
            Plan::from_fuzz(&plan),
            &property,
            Expect::Violated,
            &mut NullSink,
        );
        (result, clean)
    })
}
