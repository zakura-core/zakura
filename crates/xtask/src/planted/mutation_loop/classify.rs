//! The mutation loop's classes, and how a witness run's end records give
//! one (mutation plan, mut-7.3, Classes).
//!
//! A run's evidence is the end records that `zakura-sim`'s end-record hook
//! wrote ([`Record`]), how the witness process ended, and, for a test
//! witness, how many tests its selector names. The runner never reads the
//! process's output, so it never reads panic text.

use std::collections::BTreeMap;

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
        /// `reached`, `lost`, `no-view` or `none`.
        trigger_history: String,
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
        return Ok(Some(Record::Witness {
            thread,
            property: text_at(witness, "property").unwrap_or_default(),
            error: text_at(witness, "error"),
            trigger_history: text_at(witness, "trigger_history").unwrap_or_default(),
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

/// The run failure, if any, that keeps `observed` from giving a result.
fn run_failure(observed: &Observed) -> Option<Classified> {
    let harness =
        |reason, detail: String| Some(Classified::new(Class::HarnessFailure, Some(reason), detail));
    match observed.ended {
        Ended::TimedOut => {
            return harness(Reason::Timeout, "the run passed its deadline".to_owned())
        }
        Ended::Signaled(SIGKILL) => {
            return harness(Reason::Oom, "the memory cap killed the run".to_owned())
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

/// The class of a mutant's witness run for `property`, allowing
/// `also_violates` beside it.
///
/// The first run with a violation decides a kill: the ID that stopped it,
/// or its first violated report. That run kills by the named rule if that
/// ID is `property`, or is in `also_violates` while `property` is violated
/// too, and if the witness judgement after it on its thread didn't lose the
/// trigger history. Any other first ID kills by another rule. A run that
/// ended at a harness failure before any violation is a harness failure.
/// Without a violation, the mutant survived: `held` if the witness passed
/// and some run applied `property`, else `not-applicable` or
/// `trigger-lost` as the witness judgement or the last end record says. A
/// witness that failed with none of these failed outside the harness run.
pub(in crate::planted) fn mutant(
    property: &str,
    also_violates: &[String],
    observed: &Observed,
) -> Classified {
    if let Some(failed) = run_failure(observed) {
        return failed;
    }
    let survived = |reason, detail: String| Classified::new(Class::Survived, Some(reason), detail);
    for (at, record) in observed.records.iter().enumerate() {
        let Record::End {
            thread,
            stopped_by,
            violated,
            harness,
            ..
        } = record
        else {
            continue;
        };
        let Some(first) = stopped_by.iter().chain(violated).next() else {
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
            || (also_violates.contains(first) && violated.iter().any(|id| id == property));
        if !named {
            return Classified {
                other: Some(first.clone()),
                ..Classified::new(
                    Class::KilledByOtherRule,
                    None,
                    format!("a run violates {first} first"),
                )
            };
        }
        if judgement_after(&observed.records[at + 1..], thread, property)
            .is_some_and(|(_, trigger)| trigger == "lost")
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
    let last_outcome = observed
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            Record::End { outcomes, .. } => outcomes.get(property).cloned(),
            Record::Witness { .. } => None,
        });
    let judged = observed
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            Record::Witness {
                property: judged,
                error,
                trigger_history,
                ..
            } if judged == property => Some((error.clone(), trigger_history.clone())),
            _ => None,
        });
    if observed.ended == Ended::Exited(0) {
        let applied = observed.records.iter().any(|record| {
            matches!(record, Record::End { outcomes, .. }
                if matches!(outcomes.get(property), Some((name, n)) if name != "not-applicable" && !(name == "held" && *n == 0)))
        });
        return match (applied, last_outcome) {
            (true, _) => survived(Reason::Held, format!("{property} held on every run")),
            (false, Some(_)) => {
                survived(Reason::NotApplicable, format!("{property} never applied"))
            }
            (false, None) => Classified::new(
                Class::HarnessFailure,
                Some(Reason::NoEndRecord),
                format!("no end record reports {property}"),
            ),
        };
    }
    match judged {
        Some((Some(error), _)) if error == "lost-trigger-history" => survived(
            Reason::TriggerLost,
            format!("the witness lost {property}'s trigger history"),
        ),
        Some((Some(error), _)) if error == "not-applicable" => survived(
            Reason::NotApplicable,
            format!("the witness found {property} not applicable"),
        ),
        _ if matches!(&last_outcome, Some((name, _)) if name == "not-applicable") => survived(
            Reason::NotApplicable,
            format!("the witness failed with {property} not applicable on its last run"),
        ),
        _ => Classified::new(
            Class::HarnessFailure,
            Some(Reason::Panic),
            format!("the witness failed with no violation: {:?}", observed.ended),
        ),
    }
}

/// The first witness judgement of `property` on `thread` in `records`,
/// before that thread's next end record: its error and trigger history.
fn judgement_after(
    records: &[Record],
    thread: &str,
    property: &str,
) -> Option<(Option<String>, String)> {
    for record in records {
        match record {
            Record::End { thread: other, .. } if other == thread => return None,
            Record::Witness {
                thread: other,
                property: judged,
                error,
                trigger_history,
            } if other == thread && judged == property => {
                return Some((error.clone(), trigger_history.clone()))
            }
            _ => {}
        }
    }
    None
}
