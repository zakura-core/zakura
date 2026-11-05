//! The pilot's result classes, and the reports that count them (mutation
//! plan, Result classification and Reports and acceptance).
//!
//! Each experiment's runner fills in what each step observed, in order,
//! and leaves a step that didn't run as `None`. [`sensitivity`] and
//! [`discovery`] read those observations and return exactly one
//! [`Class`]. A step that didn't run, or a run whose observations fall
//! short, is a [`Class::HarnessFailure`], never a detection.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use super::super::{baseline_failure, classify as planted_verdict, Run, Verdict};

/// One result, as the plan's table defines it. There is no other class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Class {
    /// The target reports the declared property ID with a valid witness and
    /// complete observations.
    IntendedViolation,
    /// Another property fails.
    OtherViolation,
    /// A valid run or campaign completes without the intended violation.
    SurvivedBudget,
    /// The property never meets its prerequisites.
    NotApplicable,
    /// The unchanged build fails the paired check.
    BaselineFailure,
    /// The patch fails to apply or compile, or changes files it doesn't
    /// declare.
    InvalidMutant,
    /// The run crashes, times out, exhausts memory, leaks state, or loses
    /// required observations.
    HarnessFailure,
    /// Review established that the mutation can't change behavior.
    Equivalent,
}

impl Class {
    /// True if the detection rate leaves this class out of its
    /// denominator: the rate counts valid, non-equivalent cases with a
    /// passing baseline.
    fn excluded(self) -> bool {
        matches!(
            self,
            Class::BaselineFailure | Class::InvalidMutant | Class::Equivalent
        )
    }
}

/// A result and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Classified {
    /// The class.
    pub(super) class: Class,
    /// What showed it.
    pub(super) reason: String,
}

/// `class` because of `reason`.
fn result(class: Class, reason: impl Into<String>) -> Classified {
    Classified {
        class,
        reason: reason.into(),
    }
}

/// A recorded witness run: the planted runner's reading of its `end`
/// record, the pilot's reading of its trace, and whether its build pairs
/// with the catalogue's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Recorded {
    /// The end record and witness test result.
    pub(super) run: Run,
    /// The trace's facts, or `None` if the run wrote no trace.
    pub(super) trace: Option<TraceFacts>,
    /// Why the run's build or input doesn't pair with the catalogue's, if
    /// it doesn't.
    pub(super) unpaired: Option<String>,
}

/// What a trace's `meta` and `end` records say about its run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct TraceFacts {
    /// The build that recorded it.
    pub(super) build: Value,
    /// The harness.
    pub(super) harness: String,
    /// The input's SHA-256.
    pub(super) input_sha256: String,
    /// Steps the run took.
    pub(super) steps: u64,
    /// The scheduler's status at an idle end, such as `quiescent`.
    pub(super) status: Option<String>,
    /// What shutdown did, by counter.
    pub(super) cleanup: BTreeMap<String, u64>,
    /// The intended property's applicable evaluations, for a run where it
    /// held.
    pub(super) applicable: Option<u64>,
}

/// The cleanup counters that mean a run leaked state: a task stuck in
/// cleanup, one that panicked there, or one that caught a forced unwind.
/// A violation stops a run early, so cancelled tasks and pending timers
/// are expected then.
const LEAKS: &[&str] = &["leaked", "panicked", "unwind_failures"];

impl TraceFacts {
    /// The leak counters that aren't zero, as `name n` pairs.
    fn leaks(&self) -> Vec<String> {
        LEAKS
            .iter()
            .filter_map(|name| {
                let n = self.cleanup.get(*name).copied().unwrap_or(0);
                (n > 0).then(|| format!("{name} {n}"))
            })
            .collect()
    }
}

/// What one sensitivity case's steps observed. A step that didn't run is
/// `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct SensitivityRun {
    /// The review's argument that the mutation can't change behavior, from
    /// the catalogue.
    pub(super) equivalent: Option<String>,
    /// The original build's witness run, or why it couldn't run.
    pub(super) baseline: Option<Result<Recorded, String>>,
    /// Whether the patch applied, changed only its declared files, and
    /// compiled.
    pub(super) patch: Option<Result<(), String>>,
    /// The mutant's witness run, or why it couldn't run.
    pub(super) mutant: Option<Result<Recorded, String>>,
    /// The fresh-process confirmation and Exact replay of the mutant's
    /// violation.
    pub(super) confirmation: Option<Result<(), String>>,
}

/// Why `baseline` isn't a passing paired baseline for `property`: it must
/// hold with nonzero applicability and a passing witness test, settle
/// quiescent with zero cleanup, and pair with the catalogue's build.
pub(super) fn sensitivity_baseline(
    property: &str,
    baseline: &Option<Result<Recorded, String>>,
) -> Option<Classified> {
    let failed = |why: String| Some(result(Class::BaselineFailure, why));
    let recorded = match baseline {
        None => return Some(result(Class::HarnessFailure, "the baseline didn't run")),
        Some(Err(why)) => return failed(why.clone()),
        Some(Ok(recorded)) => recorded,
    };
    if let Some(why) = baseline_failure(property, &recorded.run) {
        return failed(why);
    }
    let Some(trace) = &recorded.trace else {
        return failed("the baseline wrote no trace".to_owned());
    };
    match trace.applicable {
        Some(n) if n > 0 => {}
        _ => return failed(format!("{property} held with zero applicability")),
    }
    if trace.status.as_deref() != Some("quiescent") || trace.cleanup.values().any(|n| *n > 0) {
        return failed(format!(
            "the baseline didn't settle cleanly: status {}, cleanup {:?}",
            trace.status.as_deref().unwrap_or("none"),
            trace.cleanup
        ));
    }
    recorded
        .unpaired
        .as_ref()
        .and_then(|why| failed(why.clone()))
}

/// What a sensitivity case shows about `property`: the original build must
/// hold its witness with nonzero applicability, the patch must apply only to
/// its declared files and compile, the mutant must violate exactly
/// `property` with its trigger history, and a fresh process must confirm
/// the violation and replay it Exact.
pub(super) fn sensitivity(property: &str, run: &SensitivityRun) -> Classified {
    let harness = |why: String| result(Class::HarnessFailure, why);
    if let Some(argument) = &run.equivalent {
        return result(Class::Equivalent, argument.clone());
    }
    if let Some(failed) = sensitivity_baseline(property, &run.baseline) {
        return failed;
    }
    match &run.patch {
        None => return harness("the patch wasn't applied".to_owned()),
        Some(Err(why)) => return result(Class::InvalidMutant, why.clone()),
        Some(Ok(())) => {}
    }
    let mutant = match &run.mutant {
        None => return harness("the mutant didn't run".to_owned()),
        Some(Err(why)) => return harness(why.clone()),
        Some(Ok(mutant)) => mutant,
    };
    if let Some(why) = &mutant.unpaired {
        return harness(why.clone());
    }
    if let Some(leaks) = mutant.trace.as_ref().map(TraceFacts::leaks) {
        if !leaks.is_empty() {
            return harness(format!("the mutant run leaked state: {}", leaks.join(", ")));
        }
    }
    match planted_verdict(property, &mutant.run) {
        Verdict::Caught(_) => match &run.confirmation {
            None => harness("no fresh process confirmed the violation".to_owned()),
            Some(Err(why)) => harness(why.clone()),
            Some(Ok(())) => result(
                Class::IntendedViolation,
                format!("the mutant violates {property}; a fresh process confirmed and replayed it Exact"),
            ),
        },
        Verdict::Unrelated(other) => result(
            Class::OtherViolation,
            format!("the mutant violates {other}, not {property}"),
        ),
        Verdict::NotCaught(outcome) => match outcome.as_str() {
            "held" => result(
                Class::SurvivedBudget,
                format!("{property} held on the mutant's witness run"),
            ),
            "not-applicable" => result(
                Class::NotApplicable,
                format!("{property} never applied on the mutant's witness run"),
            ),
            outcome => harness(format!(
                "incomplete observations: {property} is {outcome} on the mutant"
            )),
        },
        Verdict::Unreported => harness(format!(
            "incomplete observations: the mutant run doesn't report {property}"
        )),
        Verdict::WitnessFails(why) => harness(format!("missing witness: {why}")),
        verdict => harness(verdict.to_string()),
    }
}

/// What one discovery campaign showed, from its store's records.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct CampaignFacts {
    /// How the run ended: `complete`, `worker-stopped` or `interrupted`, or
    /// `none` without an end record.
    pub(super) status: String,
    /// Inputs in the run's initial snapshot.
    pub(super) initial_inputs: usize,
    /// The properties a saved input violates, in order, each once.
    pub(super) violated: Vec<String>,
    /// Saved failures that aren't a property violation: a harness failure,
    /// crash, abandoned run, nondeterministic rerun, run without an end, or
    /// libFuzzer timeout or OOM, and a crash no saved violation explains.
    pub(super) failures: Vec<String>,
    /// Every way the run's counts fall short, as the evidence lists it.
    pub(super) incomplete: Vec<String>,
    /// True if the intended property applied in some complete process.
    pub(super) applied: bool,
    /// Inputs the run's processes counted.
    pub(super) runs: u64,
    /// Inputs where the intended property applied, in complete processes.
    pub(super) applicable_runs: u64,
    /// How long the workers ran, in milliseconds.
    pub(super) millis: u64,
    /// The first saved input that violates the intended property, relative
    /// to the store.
    pub(super) intended_input: Option<String>,
    /// Why the run's fuzzer build doesn't pair with the catalogue's, if it
    /// doesn't.
    pub(super) unpaired: Option<String>,
}

/// What one seed's paired discovery campaigns observed. A step that didn't
/// run is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct DiscoveryRun {
    /// The review's argument that the mutation can't change behavior.
    pub(super) equivalent: Option<String>,
    /// The original build's campaign, or why it couldn't run.
    pub(super) baseline: Option<Result<CampaignFacts, String>>,
    /// Whether the patch applied, changed only its declared files, and
    /// compiled.
    pub(super) patch: Option<Result<(), String>>,
    /// The mutant's campaign, or why it couldn't run.
    pub(super) mutant: Option<Result<CampaignFacts, String>>,
    /// The fresh-process confirmation and Exact replay of the mutant
    /// campaign's intended finding.
    pub(super) confirmation: Option<Result<(), String>>,
}

/// What a seed's paired campaigns show about `property`: the original's
/// campaign must complete from an empty corpus with no failure, and the
/// mutant's equal-budget campaign must find exactly `property`, which a
/// fresh process confirms. A completed campaign that never applies the
/// property is [`Class::NotApplicable`], never a survivor.
pub(super) fn discovery(property: &str, run: &DiscoveryRun) -> Classified {
    let harness = |why: String| result(Class::HarnessFailure, why);
    if let Some(argument) = &run.equivalent {
        return result(Class::Equivalent, argument.clone());
    }
    if let Some(failed) = discovery_baseline(&run.baseline) {
        return failed;
    }
    match &run.patch {
        None => return harness("the patch wasn't applied".to_owned()),
        Some(Err(why)) => return result(Class::InvalidMutant, why.clone()),
        Some(Ok(())) => {}
    }
    let mutant = match &run.mutant {
        None => return harness("the mutant campaign didn't run".to_owned()),
        Some(Err(why)) => return harness(why.clone()),
        Some(Ok(mutant)) => mutant,
    };
    if let Some(why) = &mutant.unpaired {
        return harness(why.clone());
    }
    if mutant.initial_inputs > 0 {
        return harness(format!(
            "the mutant campaign started from {} inputs, not the frozen empty corpus",
            mutant.initial_inputs
        ));
    }
    let others: Vec<&String> = mutant.violated.iter().filter(|p| *p != property).collect();
    if let Some(other) = others.first() {
        return result(
            Class::OtherViolation,
            format!("the mutant campaign violates {other}"),
        );
    }
    if mutant.violated.iter().any(|p| p == property) {
        return match &run.confirmation {
            None => harness("no fresh process confirmed the campaign's finding".to_owned()),
            Some(Err(why)) => harness(why.clone()),
            Some(Ok(())) => result(
                Class::IntendedViolation,
                format!(
                    "the mutant campaign found {property} after {} runs and {} ms; a fresh \
                     process confirmed and replayed it Exact",
                    mutant.runs, mutant.millis
                ),
            ),
        };
    }
    if let Some(why) = campaign_failure(mutant) {
        return harness(format!("the mutant campaign {why}"));
    }
    if !mutant.applied {
        return result(
            Class::NotApplicable,
            format!("{property} never applied in {} runs", mutant.runs),
        );
    }
    result(
        Class::SurvivedBudget,
        format!(
            "{property} held through the budget: {} of {} runs applied",
            mutant.applicable_runs, mutant.runs
        ),
    )
}

/// Why `baseline` isn't a passing paired discovery baseline: the original's
/// campaign must complete from the frozen empty corpus, pair with the
/// catalogue's build, and leave no violation or other failure. A baseline
/// whose property never applies passes; the mutant's campaign then shows
/// whether it applies.
pub(super) fn discovery_baseline(
    baseline: &Option<Result<CampaignFacts, String>>,
) -> Option<Classified> {
    let baseline = match baseline {
        None => {
            return Some(result(
                Class::HarnessFailure,
                "the baseline campaign didn't run",
            ))
        }
        Some(Err(why)) => return Some(result(Class::BaselineFailure, why.clone())),
        Some(Ok(baseline)) => baseline,
    };
    if let Some(violated) = baseline.violated.first() {
        return Some(result(
            Class::BaselineFailure,
            format!("the baseline campaign violates {violated}"),
        ));
    }
    campaign_failure(baseline).map(|why| {
        result(
            Class::BaselineFailure,
            format!("the baseline campaign {why}"),
        )
    })
}

/// Why `campaign` isn't a complete, clean, paired run from the empty
/// corpus, other than a property violation, or `None` if it is.
fn campaign_failure(campaign: &CampaignFacts) -> Option<String> {
    if let Some(why) = &campaign.unpaired {
        return Some(why.clone());
    }
    if campaign.initial_inputs > 0 {
        return Some(format!(
            "started from {} inputs, not the frozen empty corpus",
            campaign.initial_inputs
        ));
    }
    if !campaign.failures.is_empty() {
        return Some(format!("left failures: {}", campaign.failures.join(", ")));
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

/// The classes of a set of results, and the intended-detection rate's
/// numerator and denominator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Summary {
    /// Results counted: cases for sensitivity, case-seed pairs for
    /// discovery.
    pub(super) results: usize,
    /// Results by class. Every class appears, with zero if none.
    pub(super) classes: BTreeMap<Class, usize>,
    /// Results the rate counts: every class but baseline failures, invalid
    /// mutants and equivalent mutants. Not-applicable results stay in.
    pub(super) denominator: usize,
    /// Intended violations.
    pub(super) intended: usize,
    /// The same counts per fault class, so easy mutants can't hide weak
    /// coverage of another class.
    pub(super) by_fault_class: BTreeMap<String, ClassSummary>,
}

/// One fault class's counts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ClassSummary {
    /// Its results.
    pub(super) results: usize,
    /// Those the rate counts.
    pub(super) denominator: usize,
    /// Its intended violations.
    pub(super) intended: usize,
}

/// Counts `rows`, each a fault class and its result's class. Fails on no
/// rows: zero selected results is never a report.
pub(super) fn summarize<'a>(
    rows: impl IntoIterator<Item = (&'a str, Class)>,
) -> Result<Summary, String> {
    let mut summary = Summary {
        results: 0,
        classes: [
            Class::IntendedViolation,
            Class::OtherViolation,
            Class::SurvivedBudget,
            Class::NotApplicable,
            Class::BaselineFailure,
            Class::InvalidMutant,
            Class::HarnessFailure,
            Class::Equivalent,
        ]
        .into_iter()
        .map(|class| (class, 0))
        .collect(),
        denominator: 0,
        intended: 0,
        by_fault_class: BTreeMap::new(),
    };
    for (fault_class, class) in rows {
        let counted = !class.excluded();
        let intended = class == Class::IntendedViolation;
        summary.results += 1;
        *summary.classes.entry(class).or_default() += 1;
        summary.denominator += usize::from(counted);
        summary.intended += usize::from(intended);
        let row = summary
            .by_fault_class
            .entry(fault_class.to_owned())
            .or_default();
        row.results += 1;
        row.denominator += usize::from(counted);
        row.intended += usize::from(intended);
    }
    if summary.results == 0 {
        return Err("no result to report: the run selected zero cases".to_owned());
    }
    Ok(summary)
}
