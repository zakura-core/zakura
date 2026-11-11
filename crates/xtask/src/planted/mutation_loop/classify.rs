//! The mutation loop's classes, and how a witness run's end records give
//! one (mutation plan, mut-7.3, Classes).
//!
//! A run's evidence is the end records that `zakura-sim`'s end-record hook
//! wrote ([`Record`]), how the witness process ended, and, for a test
//! witness, how many tests its selector names. The runner never reads the
//! process's output, so it never reads panic text.
//!
//! A generator lane is classified into the same classes ([`generated`]),
//! from the search's report and the one end record it wrote.

use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What a mutant's case ended with, or the run failure that kept it from
/// one. The report keeps the two apart ([`Class::is_result`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum Class {
    /// The named ID is violated, with its trigger history.
    KilledByNamedRule,
    /// Another ID is violated first. [`Classified::other`] names it.
    KilledByOtherRule,
    /// The named ID wasn't violated. [`Classified::reason`] says why.
    Survived,
    /// The patched build fails to compile.
    DidNotCompile,
    /// A run failure: the patch doesn't apply.
    PatchDoesNotApply,
    /// A run failure: the unpatched witness fails, so the mutant didn't run.
    BaselineFails,
    /// A run failure: the witness run couldn't give a result.
    /// [`Classified::reason`] says why.
    HarnessFailure,
}

impl Class {
    /// True for a mutant result, false for a run failure.
    pub(in crate::planted) fn is_result(self) -> bool {
        matches!(
            self,
            Class::KilledByNamedRule
                | Class::KilledByOtherRule
                | Class::Survived
                | Class::DidNotCompile
        )
    }
}

/// Why a mutant survived, or why a run failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum Reason {
    /// Survived: the named ID applied and held.
    Held,
    /// Survived: the named ID never applied.
    NotApplicable,
    /// Survived: the run didn't reach the named ID's trigger history.
    TriggerLost,
    /// Survived: the named ID ended inconclusive, and no run held it with
    /// nonzero applicability.
    Inconclusive,
    /// Harness failure: the run passed its deadline.
    Timeout,
    /// Harness failure: the memory cap killed the run.
    Oom,
    /// Harness failure: the witness failed outside any harness run, or a
    /// signal other than the memory cap's killed it.
    Panic,
    /// Harness failure: no end record shows the named ID.
    NoEndRecord,
    /// Harness failure: the test selector names no test.
    ZeroTests,
    /// Harness failure: a harness run ended at a harness failure.
    HarnessEnd,
    /// Harness failure: the build broke without a compile error, such as a
    /// compiler the memory cap killed or a full disk.
    Build,
    /// Harness failure: a generator's search stopped at its limit of runs
    /// whose shutdown abandoned tasks.
    Abandoned,
    /// Harness failure: a generator's kill didn't confirm, or didn't replay
    /// Exact, in a fresh process.
    Unconfirmed,
}

/// One case's class, and what showed it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct Classified {
    pub(in crate::planted) class: Class,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) reason: Option<Reason>,
    /// The ID violated instead, for [`Class::KilledByOtherRule`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) other: Option<String>,
    /// What showed it, in words.
    pub(in crate::planted) detail: String,
}

impl Classified {
    pub(in crate::planted) fn new(
        class: Class,
        reason: Option<Reason>,
        detail: impl Into<String>,
    ) -> Self {
        Classified {
            class,
            reason,
            other: None,
            detail: detail.into(),
        }
    }

    /// The summary's key: the class, and the reason after a slash if there
    /// is one, such as `survived/held`.
    pub(in crate::planted) fn key(&self) -> String {
        match self.reason {
            Some(reason) => format!("{}/{}", name(&self.class), name(&reason)),
            None => name(&self.class),
        }
    }
}

/// `value`'s serde name, such as `killed-by-named-rule`.
pub(in crate::planted) fn name<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Whether a run reached the named ID's trigger history, as a witness
/// judgement found it. The first four are `zakura-sim`'s `TriggerState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum Trigger {
    /// The run reached it.
    Reached,
    /// The run didn't reach it.
    Lost,
    /// The trigger history reads the view, and no view shows the run's end.
    NoView,
    /// The property has no trigger-history check.
    None,
    /// No witness judgement of the named ID speaks for the run.
    Unjudged,
}

/// One file the end-record hook wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::planted) enum Record {
    /// A run's end.
    End {
        thread: String,
        /// The violation that stopped the run.
        stopped_by: Option<String>,
        /// Every property whose outcome is violated, in report order.
        violated: Vec<String>,
        /// The harness failure the run ended at.
        harness: Option<String>,
        /// Each property's outcome name, such as `held`, and its applicable
        /// evaluations for a held one.
        outcomes: BTreeMap<String, (String, u64)>,
    },
    /// A witness judgement.
    Witness {
        thread: String,
        property: String,
        /// The judgement's error kind, or `None` if it passed.
        error: Option<String>,
        /// Never [`Trigger::Unjudged`].
        trigger_history: Trigger,
    },
}

/// Reads one hook file, written by the build whose source digest is
/// `source`. `Ok(None)` for a record another build wrote.
pub(in crate::planted) fn read_record(text: &str, source: &str) -> Result<Option<Record>, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
    if value.get("source").and_then(Value::as_str) != Some(source) {
        return Ok(None);
    }
    let thread = value
        .get("thread")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let text_at =
        |value: &Value, key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
    if let Some(end) = value.get("end") {
        let mut violated = Vec::new();
        let mut outcomes = BTreeMap::new();
        for report in end
            .get("properties")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = text_at(report, "property").unwrap_or_default();
            let outcome = report.get("outcome");
            let name = outcome
                .and_then(|outcome| {
                    outcome
                        .as_str()
                        .or_else(|| outcome.as_object()?.keys().next().map(String::as_str))
                })
                .unwrap_or_default()
                .to_owned();
            let applicable = outcome
                .and_then(|outcome| outcome.pointer("/held/applicable"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if name == "violated" {
                violated.push(id.clone());
            }
            outcomes.insert(id, (name, applicable));
        }
        return Ok(Some(Record::End {
            thread,
            stopped_by: end
                .get("violation")
                .and_then(|violation| text_at(violation, "property")),
            violated,
            harness: text_at(end, "harness"),
            outcomes,
        }));
    }
    if let Some(witness) = value.get("witness") {
        let trigger_history = witness
            .get("trigger_history")
            .cloned()
            .and_then(|trigger| serde_json::from_value(trigger).ok())
            .filter(|trigger| *trigger != Trigger::Unjudged)
            .ok_or("a witness record has no known `trigger_history`")?;
        return Ok(Some(Record::Witness {
            thread,
            property: text_at(witness, "property").unwrap_or_default(),
            error: text_at(witness, "error"),
            trigger_history,
        }));
    }
    Err("a record holds neither `end` nor `witness`".to_owned())
}

/// How a witness process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::planted) enum Ended {
    /// It exited with this code. 0 is a passing witness.
    Exited(i32),
    /// This signal killed it.
    Signaled(i32),
    /// It passed its deadline, and the runner killed it.
    #[cfg(any(unix, test))]
    TimedOut,
}

/// What one witness run showed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::planted) struct Observed {
    pub(in crate::planted) ended: Ended,
    /// How many tests the selector names, for a test witness.
    pub(in crate::planted) listed: Option<usize>,
    /// The run's records from the case's build, in the order written.
    pub(in crate::planted) records: Vec<Record>,
    /// Records another build wrote, which pair with no case.
    pub(in crate::planted) unpaired: usize,
}

/// The memory cap's kill: the kernel's OOM killer sends SIGKILL.
const SIGKILL: i32 = 9;

/// True if the memory cap killed the witness process.
pub(in crate::planted) fn cap_killed(observed: &Observed) -> bool {
    observed.ended == Ended::Signaled(SIGKILL)
}

/// The run failure, if any, that keeps `observed` from giving a result.
fn run_failure(observed: &Observed) -> Option<Classified> {
    let harness =
        |reason, detail: String| Some(Classified::new(Class::HarnessFailure, Some(reason), detail));
    if cap_killed(observed) {
        return harness(Reason::Oom, "the memory cap killed the run".to_owned());
    }
    match observed.ended {
        #[cfg(any(unix, test))]
        Ended::TimedOut => {
            return harness(Reason::Timeout, "the run passed its deadline".to_owned())
        }
        Ended::Signaled(signal) => {
            return harness(Reason::Panic, format!("signal {signal} killed the run"))
        }
        Ended::Exited(_) => {}
    }
    if observed.listed == Some(0) {
        return harness(Reason::ZeroTests, "the selector names no test".to_owned());
    }
    if observed
        .records
        .iter()
        .all(|r| !matches!(r, Record::End { .. }))
    {
        let unpaired = match observed.unpaired {
            0 => String::new(),
            n => format!("; {n} records came from another build"),
        };
        return harness(
            Reason::NoEndRecord,
            format!("the run wrote no end record{unpaired}"),
        );
    }
    None
}

/// Why the unpatched witness doesn't hold `property` cleanly, or `None` if
/// it does: it passes, every run ends without a violation or a harness
/// failure, and some run holds `property` with nonzero applicability.
pub(in crate::planted) fn baseline(property: &str, observed: &Observed) -> Option<String> {
    if let Some(failed) = run_failure(observed) {
        return Some(failed.detail);
    }
    for record in &observed.records {
        if let Record::End {
            stopped_by,
            violated,
            harness,
            ..
        } = record
        {
            if let Some(failure) = harness {
                return Some(format!("a run ended at a harness failure: {failure}"));
            }
            if let Some(id) = stopped_by.iter().chain(violated).next() {
                return Some(format!("a run violates {id}"));
            }
        }
    }
    if observed.ended != Ended::Exited(0) {
        return Some(format!("the witness fails: {:?}", observed.ended));
    }
    let applied = observed.records.iter().any(|record| {
        matches!(record, Record::End { outcomes, .. }
            if matches!(outcomes.get(property), Some((name, n)) if name == "held" && *n > 0))
    });
    (!applied).then(|| format!("no run holds {property} with nonzero applicability"))
}

/// The ID whose violation decides a kill on the run that ended at `record`:
/// the ID that stopped the run, or else `property` if the run violated it
/// and ended without a harness failure. Another ID's violation that didn't
/// stop its run, such as a `Draft` property's, decides nothing. A run that
/// ended at a harness failure stopped before its end-of-run outcomes were
/// final, so only a violation that stopped it decides.
fn deciding<'a>(property: &str, record: &'a Record) -> Option<&'a str> {
    let Record::End {
        stopped_by,
        violated,
        harness,
        ..
    } = record
    else {
        return None;
    };
    if let Some(id) = stopped_by {
        return Some(id);
    }
    if harness.is_some() {
        return None;
    }
    violated
        .iter()
        .map(String::as_str)
        .find(|id| *id == property)
}

/// Each ID other than `property` that a run violated without stopping, once,
/// in the order the records name them.
fn unstopped<'a>(property: &str, records: &'a [Record]) -> Vec<&'a str> {
    let mut ids: Vec<&str> = Vec::new();
    for record in records {
        if let Record::End {
            stopped_by,
            violated,
            ..
        } = record
        {
            for id in violated {
                if id != property && stopped_by.as_ref() != Some(id) && !ids.contains(&id.as_str())
                {
                    ids.push(id);
                }
            }
        }
    }
    ids
}

/// The class of a mutant's witness run for `property`, allowing
/// `also_violates` beside it.
///
/// The first run with a deciding violation ([`deciding`]) decides a kill.
/// That run kills by the named rule if the deciding ID is `property`, or is
/// in `also_violates` while `property` is violated too, and if the witness
/// judgement after it on its thread didn't lose the trigger history. Any
/// other deciding ID kills by another rule. A run that ended at a harness
/// failure before any deciding violation is a harness failure.
///
/// Without a deciding violation, the mutant survived. If the witness passed,
/// or its judgement failed only at a violation that stopped no run, the end
/// records' outcomes give the reason ([`by_outcomes`]). Otherwise the
/// witness judgement or the last end record gives `trigger-lost`,
/// `not-applicable` or `inconclusive`. A witness that failed with none of
/// these failed outside the harness run.
///
/// The detail names each other ID that a run violated without stopping.
pub(in crate::planted) fn mutant(
    property: &str,
    also_violates: &[String],
    observed: &Observed,
) -> Classified {
    if let Some(failed) = run_failure(observed) {
        return failed;
    }
    let unstopped = unstopped(property, &observed.records);
    let mut classified = decide(property, also_violates, observed, &unstopped);
    if !unstopped.is_empty() {
        classified.detail += &format!(
            "; {} violated without stopping its run",
            unstopped.join(", ")
        );
    }
    classified
}

/// [`mutant`]'s class, for a run with no run failure, where `unstopped`
/// lists the violations that stopped no run.
fn decide(
    property: &str,
    also_violates: &[String],
    observed: &Observed,
    unstopped: &[&str],
) -> Classified {
    let survived = |reason, detail: String| Classified::new(Class::Survived, Some(reason), detail);
    for (at, record) in observed.records.iter().enumerate() {
        let Record::End {
            thread,
            violated,
            harness,
            ..
        } = record
        else {
            continue;
        };
        let Some(first) = deciding(property, record) else {
            if let Some(failure) = harness {
                return Classified::new(
                    Class::HarnessFailure,
                    Some(Reason::HarnessEnd),
                    format!("a run ended at a harness failure: {failure}"),
                );
            }
            continue;
        };
        let named = first == property
            || (also_violates.iter().any(|id| id == first)
                && violated.iter().any(|id| id == property));
        if !named {
            return Classified {
                other: Some(first.to_owned()),
                ..Classified::new(
                    Class::KilledByOtherRule,
                    None,
                    format!("a run violates {first} first"),
                )
            };
        }
        if judgement_after(&observed.records[at + 1..], thread, property)
            .is_some_and(|(_, trigger)| trigger == Trigger::Lost)
        {
            return survived(
                Reason::TriggerLost,
                format!("a run violates {property} without its trigger history"),
            );
        }
        return Classified::new(
            Class::KilledByNamedRule,
            None,
            format!("a run violates {property}"),
        );
    }
    let error = last_judgement(property, &observed.records).and_then(|(error, _)| error);
    if observed.ended == Ended::Exited(0) || (error == Some("unrelated") && !unstopped.is_empty()) {
        return by_outcomes(property, observed);
    }
    let last_outcome = observed
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            Record::End { outcomes, .. } => outcomes.get(property).map(|(name, _)| name.as_str()),
            Record::Witness { .. } => None,
        });
    match (error, last_outcome) {
        (Some("lost-trigger-history"), _) => survived(
            Reason::TriggerLost,
            format!("the witness lost {property}'s trigger history"),
        ),
        (Some("not-applicable"), _) => survived(
            Reason::NotApplicable,
            format!("the witness found {property} not applicable"),
        ),
        (Some("inconclusive"), _) => survived(
            Reason::Inconclusive,
            format!("the witness found {property} inconclusive"),
        ),
        (_, Some("not-applicable")) => survived(
            Reason::NotApplicable,
            format!("the witness failed with {property} not applicable on its last run"),
        ),
        (_, Some("inconclusive")) => survived(
            Reason::Inconclusive,
            format!("the witness failed with {property} inconclusive on its last run"),
        ),
        _ => Classified::new(
            Class::HarnessFailure,
            Some(Reason::Panic),
            format!("the witness failed with no violation: {:?}", observed.ended),
        ),
    }
}

/// A survivor's reason from the end records' outcomes of `property`:
/// `held` if some run held it with nonzero applicability, as a baseline
/// requires, else `inconclusive` if some run ended it inconclusive, else
/// `not-applicable`. A harness failure if no end record reports `property`.
fn by_outcomes(property: &str, observed: &Observed) -> Classified {
    let outcomes: Vec<&(String, u64)> = observed
        .records
        .iter()
        .filter_map(|record| match record {
            Record::End { outcomes, .. } => outcomes.get(property),
            Record::Witness { .. } => None,
        })
        .collect();
    let survived = |reason, detail: String| Classified::new(Class::Survived, Some(reason), detail);
    if outcomes.iter().any(|(name, n)| name == "held" && *n > 0) {
        survived(
            Reason::Held,
            format!("{property} held with nonzero applicability"),
        )
    } else if outcomes.iter().any(|(name, _)| name == "inconclusive") {
        survived(
            Reason::Inconclusive,
            format!("{property} ended inconclusive, and no run held it with nonzero applicability"),
        )
    } else if !outcomes.is_empty() {
        survived(Reason::NotApplicable, format!("{property} never applied"))
    } else {
        Classified::new(
            Class::HarnessFailure,
            Some(Reason::NoEndRecord),
            format!("no end record reports {property}"),
        )
    }
}

/// Whether the run that decided `observed`'s class reached `property`'s
/// trigger history. For a run with a deciding violation ([`deciding`]) or a
/// harness failure, it is the witness judgement after that run on its
/// thread. Otherwise it is the last judgement of `property`.
/// [`Trigger::Unjudged`] if no such judgement exists.
pub(in crate::planted) fn trigger(property: &str, observed: &Observed) -> Trigger {
    let records = &observed.records;
    for (at, record) in records.iter().enumerate() {
        if let Record::End {
            thread, harness, ..
        } = record
        {
            if deciding(property, record).is_some() || harness.is_some() {
                return judgement_after(&records[at + 1..], thread, property)
                    .map_or(Trigger::Unjudged, |(_, trigger)| trigger);
            }
        }
    }
    last_judgement(property, records).map_or(Trigger::Unjudged, |(_, trigger)| trigger)
}

/// The last witness judgement of `property` in `records`: its error and
/// trigger history.
fn last_judgement<'a>(property: &str, records: &'a [Record]) -> Option<(Option<&'a str>, Trigger)> {
    records.iter().rev().find_map(|record| match record {
        Record::Witness {
            property: judged,
            error,
            trigger_history,
            ..
        } if judged == property => Some((error.as_deref(), *trigger_history)),
        _ => None,
    })
}

/// The first witness judgement of `property` on `thread` in `records`,
/// before that thread's next end record: its error and trigger history.
fn judgement_after<'a>(
    records: &'a [Record],
    thread: &str,
    property: &str,
) -> Option<(Option<&'a str>, Trigger)> {
    for record in records {
        match record {
            Record::End { thread: other, .. } if other == thread => return None,
            Record::Witness {
                thread: other,
                property: judged,
                error,
                trigger_history,
            } if other == thread && judged == property => {
                return Some((error.as_deref(), *trigger_history))
            }
            _ => {}
        }
    }
    None
}

/// Why a generator's search stopped, as `zakura-sim generate` writes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(in crate::planted) enum GenerateStop {
    /// No drawn input stopped it.
    Clean,
    /// A run violated an `Active` property.
    Violation { property: String },
    /// A run ended at a harness failure.
    HarnessFailure { reason: String },
    /// Shutdown abandoned the search's limit of runs.
    Abandoned { leaked: u64 },
    /// A run panicked outside any task.
    Panic { message: String },
}

/// One property's outcomes over a search's runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::planted) struct Applicability {
    pub(in crate::planted) held: u64,
    pub(in crate::planted) violated: u64,
    pub(in crate::planted) inconclusive: u64,
    pub(in crate::planted) not_applicable: u64,
}

/// The parts of `zakura-sim generate`'s report the loop reads.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(in crate::planted) struct GenerateFacts {
    pub(in crate::planted) stop: GenerateStop,
    /// The draw index of the input that stopped the search.
    #[serde(default)]
    pub(in crate::planted) kill_run: Option<u64>,
    pub(in crate::planted) executed: u64,
    pub(in crate::planted) millis: u64,
    /// The saved input that stopped the search.
    #[serde(default)]
    pub(in crate::planted) input: Option<PathBuf>,
    #[serde(default)]
    pub(in crate::planted) properties: BTreeMap<String, Applicability>,
}

impl GenerateFacts {
    /// Runs the search took: to its kill, or all it ran.
    pub(in crate::planted) fn runs(&self) -> u64 {
        self.kill_run.map_or(self.executed, |kill| kill + 1)
    }
}

/// The run failure, if any, that keeps a generator process that ended as
/// `observed` says, with report `facts`, from giving a result.
fn generator_failure(observed: &Observed, facts: Option<&GenerateFacts>) -> Option<Classified> {
    let harness =
        |reason, detail: String| Some(Classified::new(Class::HarnessFailure, Some(reason), detail));
    if cap_killed(observed) {
        return harness(Reason::Oom, "the memory cap killed the search".to_owned());
    }
    match (observed.ended, facts) {
        #[cfg(any(unix, test))]
        (Ended::TimedOut, _) => {
            harness(Reason::Timeout, "the search passed its deadline".to_owned())
        }
        (Ended::Signaled(signal), _) => {
            harness(Reason::Panic, format!("signal {signal} killed the search"))
        }
        (Ended::Exited(0), Some(_)) => None,
        (ended, _) => harness(
            Reason::Panic,
            format!("the generator wrote no report: {ended:?}"),
        ),
    }
}

/// Why a generator's search on the unpatched build isn't clean, or `None`
/// if it is: it must run every input with no `Active` violation, harness
/// failure, abandonment limit or panic.
pub(in crate::planted) fn generator_baseline(
    observed: &Observed,
    facts: Option<&GenerateFacts>,
) -> Option<String> {
    if let Some(failed) = generator_failure(observed, facts) {
        return Some(failed.detail);
    }
    let facts = facts.expect("a search with no run failure wrote its report");
    stop_detail(&facts.stop, facts.runs())
}

/// `stop` in words, after `runs` runs, or `None` for a clean search.
pub(in crate::planted) fn stop_detail(stop: &GenerateStop, runs: u64) -> Option<String> {
    match stop {
        GenerateStop::Clean => None,
        GenerateStop::Violation { property } => {
            Some(format!("run {runs} of the search violates {property}"))
        }
        GenerateStop::HarnessFailure { reason } => Some(format!(
            "run {runs} of the search ended at a harness failure: {reason}"
        )),
        GenerateStop::Abandoned { leaked } => Some(format!(
            "run {runs} of the search reached the abandonment limit, leaking {leaked} tasks"
        )),
        GenerateStop::Panic { message } => {
            Some(format!("run {runs} of the search panicked: {message}"))
        }
    }
}

/// The class of a generator's search on a mutant for `property`, allowing
/// `also_violates` beside it, by the witness lane's rules.
///
/// A search that stopped at an `Active` violation kills by the named rule
/// if the violated ID is `property`, or is in `also_violates` while the
/// kill's end record shows `property` violated too. Any other ID kills by
/// another rule. The kill's end record must come from the patched build.
/// A clean search survived: `held` if some run held `property` with an
/// applicable evaluation, else `inconclusive` if some run ended it
/// inconclusive, else `not-applicable`. Any other stop is a harness
/// failure.
pub(in crate::planted) fn generated(
    property: &str,
    also_violates: &[String],
    observed: &Observed,
    facts: Option<&GenerateFacts>,
) -> Classified {
    if let Some(failed) = generator_failure(observed, facts) {
        return failed;
    }
    let facts = facts.expect("a search with no run failure wrote its report");
    let runs = facts.runs();
    let detail = stop_detail(&facts.stop, runs).unwrap_or_default();
    let harness = |reason| Classified::new(Class::HarnessFailure, Some(reason), detail.clone());
    match &facts.stop {
        GenerateStop::Clean => searched(property, facts),
        GenerateStop::Violation { property: id } => {
            let Some(violated) = observed.records.iter().find_map(|record| match record {
                Record::End {
                    stopped_by: Some(stopped),
                    violated,
                    ..
                } if stopped == id => Some(violated),
                _ => None,
            }) else {
                return Classified::new(
                    Class::HarnessFailure,
                    Some(Reason::NoEndRecord),
                    format!(
                        "no end record from this build shows the search's violation of {id}; \
                         {} records came from another build",
                        observed.unpaired
                    ),
                );
            };
            let named = id == property
                || (also_violates.iter().any(|also| also == id)
                    && violated.iter().any(|other| other == property));
            if named {
                Classified::new(Class::KilledByNamedRule, None, detail)
            } else {
                Classified {
                    other: Some(id.clone()),
                    ..Classified::new(Class::KilledByOtherRule, None, detail)
                }
            }
        }
        GenerateStop::HarnessFailure { .. } => harness(Reason::HarnessEnd),
        GenerateStop::Abandoned { .. } => harness(Reason::Abandoned),
        GenerateStop::Panic { .. } => harness(Reason::Panic),
    }
}

/// A clean search's survivor reason for `property`, from its outcome
/// counts, as [`by_outcomes`] gives a passing witness's.
fn searched(property: &str, facts: &GenerateFacts) -> Classified {
    let survived = |reason, detail: String| Classified::new(Class::Survived, Some(reason), detail);
    let Some(counts) = facts.properties.get(property) else {
        return Classified::new(
            Class::HarnessFailure,
            Some(Reason::NoEndRecord),
            format!("no run of the search reports {property}"),
        );
    };
    let runs = facts.executed;
    if counts.held > 0 {
        survived(
            Reason::Held,
            format!(
                "{property} held with nonzero applicability in {} of {runs} runs",
                counts.held
            ),
        )
    } else if counts.inconclusive > 0 {
        survived(
            Reason::Inconclusive,
            format!(
                "{property} ended inconclusive in {} of {runs} runs and never applied",
                counts.inconclusive
            ),
        )
    } else {
        survived(
            Reason::NotApplicable,
            format!("{property} never applied in {runs} runs"),
        )
    }
}
