//! The mutation loop's recorded triage (mutation plan, mut-5).
//!
//! A lane that survived, or that another rule killed, needs a reason before
//! the run counts as triaged. [`TRIAGE_FILE`] records each reason once, by
//! the case's patch and property, the lane (`witness`, or the generator
//! harness, which covers every seed), and the class it explains, with the
//! other rule's ID for a kill by another rule. A record matches a lane, or
//! a seed of a generator lane, only while that lane's class and other rule
//! are the record's, so a lane whose result changed needs triage again. The
//! loop reads the file from the checkout it runs from, and copies each
//! record onto the lanes it matches, so every later report carries it too.
//! A record that matches no lane is stale, and the loop lists it.

use std::{fs, io, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    classify::{Class, Classified},
    CaseReport,
};
use crate::BoxError;

/// The triage records, relative to the repository root.
pub(super) const TRIAGE_FILE: &str = "qa/native-mutations/triage.toml";

/// The lane name a record gives the witness lane.
const WITNESS_LANE: &str = "witness";

/// Where in the causal chain from mutant to violation the lane stopped:
/// the input never drew the bug's trigger, the schedule never ran it, the
/// harness never observed it, the property doesn't cover it, or the harness
/// can't express it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum Stage {
    Generator,
    Schedule,
    Observation,
    Property,
    Unsupported,
}

/// One lane's triage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::planted) struct Triage {
    pub(in crate::planted) stage: Stage,
    /// Why the lane stopped there, in words.
    pub(in crate::planted) reason: String,
    /// The issue or PR that tracks the gap, if there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::planted) issue: Option<String>,
}

/// The class a record explains: one of the classes that need triage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::planted) enum TriagedClass {
    Survived,
    KilledByOtherRule,
}

impl TriagedClass {
    fn class(self) -> Class {
        match self {
            TriagedClass::Survived => Class::Survived,
            TriagedClass::KilledByOtherRule => Class::KilledByOtherRule,
        }
    }
}

/// One record in [`TRIAGE_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::planted) struct Record {
    /// The case's patch, as the report names it.
    pub(in crate::planted) patch: String,
    pub(in crate::planted) property: String,
    /// `witness`, or a generator harness.
    pub(in crate::planted) lane: String,
    /// The lane's class that the record explains.
    pub(in crate::planted) class: TriagedClass,
    /// The other rule's ID, for `killed-by-other-rule` only.
    #[serde(default)]
    pub(in crate::planted) other: Option<String>,
    pub(in crate::planted) stage: Stage,
    pub(in crate::planted) reason: String,
    #[serde(default)]
    pub(in crate::planted) issue: Option<String>,
}

impl Record {
    fn triage(&self) -> Triage {
        Triage {
            stage: self.stage,
            reason: self.reason.clone(),
            issue: self.issue.clone(),
        }
    }

    /// True if the record explains `case`'s `lane`, whose result is
    /// `result`.
    fn names(&self, case: &CaseReport, lane: &str, result: &Classified) -> bool {
        (
            self.patch.as_str(),
            self.property.as_str(),
            self.lane.as_str(),
        ) == (case.patch.as_str(), case.property.as_str(), lane)
            && self.class.class() == result.class
            && self.other == result.other
    }

    /// The record's lane and class, in words.
    pub(super) fn describe(&self) -> String {
        let class = match self.class {
            TriagedClass::Survived => "survived",
            TriagedClass::KilledByOtherRule => "killed-by-other-rule",
        };
        let other = self
            .other
            .as_ref()
            .map_or_else(String::new, |other| format!(" {other}"));
        format!(
            "{} {} {} {class}{other}",
            self.patch, self.property, self.lane
        )
    }

    /// Why the record is malformed: `other` is required for a kill by
    /// another rule and forbidden otherwise.
    fn malformed(&self) -> Option<&'static str> {
        match (self.class, &self.other) {
            (TriagedClass::KilledByOtherRule, None) => {
                Some("is `killed-by-other-rule` and names no `other` rule")
            }
            (TriagedClass::Survived, Some(_)) => Some(
                "is `survived` and names an `other` rule, which only `killed-by-other-rule` takes",
            ),
            _ => None,
        }
    }
}

/// True for a class that needs triage: a survivor, or a kill by another
/// rule.
pub(in crate::planted) fn needs_triage(class: Class) -> bool {
    matches!(class, Class::Survived | Class::KilledByOtherRule)
}

/// The records in `root`'s [`TRIAGE_FILE`], or none if it has no such
/// file. A malformed record is an error.
pub(super) fn read(root: &Path) -> Result<Vec<Record>, BoxError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct File {
        #[serde(default)]
        triage: Vec<Record>,
    }
    let records = match fs::read_to_string(root.join(TRIAGE_FILE)) {
        Ok(text) => {
            toml::from_str::<File>(&text)
                .map_err(|error| format!("{TRIAGE_FILE}: {error}"))?
                .triage
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    for record in &records {
        if let Some(why) = record.malformed() {
            return Err(format!(
                "{TRIAGE_FILE}: the record for {} {} {} {why}",
                record.patch, record.property, record.lane
            )
            .into());
        }
    }
    Ok(records)
}

/// Copies each of `records` onto every lane of `cases` it names that needs
/// triage, with the lane's class and other rule, and clears every other
/// lane's triage. Returns the stale records: those whose case ran and that
/// matched no lane. A record for a case `--only` or `--changed-since` left
/// out isn't judged.
pub(super) fn apply<'a>(cases: &mut [CaseReport], records: &'a [Record]) -> Vec<&'a Record> {
    let mut used = vec![false; records.len()];
    let mut find = |case: &CaseReport, lane: &str, result: &Classified| {
        let index = records
            .iter()
            .position(|record| record.names(case, lane, result))?;
        used[index] = true;
        Some(records[index].triage())
    };
    for case in cases.iter_mut() {
        case.triage = needs_triage(case.result.class)
            .then(|| find(case, WITNESS_LANE, &case.result))
            .flatten();
        let generator = std::mem::take(&mut case.generator);
        case.generator = generator
            .into_iter()
            .map(|mut lane| {
                lane.triage = needs_triage(lane.result.class)
                    .then(|| find(case, &lane.harness, &lane.result))
                    .flatten();
                lane
            })
            .collect();
    }
    let ran = |record: &Record| {
        cases
            .iter()
            .any(|case| (&case.patch, &case.property) == (&record.patch, &record.property))
    };
    records
        .iter()
        .zip(used)
        .filter(|(record, used)| !used && ran(record))
        .map(|(record, _)| record)
        .collect()
}

/// How many lanes of `cases` need triage and have none.
pub(super) fn untriaged(cases: &[CaseReport]) -> usize {
    cases
        .iter()
        .map(|case| {
            usize::from(needs_triage(case.result.class) && case.triage.is_none())
                + case
                    .generator
                    .iter()
                    .filter(|lane| needs_triage(lane.result.class) && lane.triage.is_none())
                    .count()
        })
        .sum()
}

/// Applies `records` to the cases of `report`, a loop report or a merge of
/// several, as [`apply`] does, and changes nothing else in it. The report's
/// `summary` counts classes, not triage, so it stays. Returns the stale
/// records.
pub(super) fn reapply<'a>(
    report: &mut Value,
    records: &'a [Record],
) -> Result<Vec<&'a Record>, BoxError> {
    let values = report
        .get_mut("cases")
        .and_then(Value::as_array_mut)
        .ok_or("the report has no `cases` list")?;
    let mut cases = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            serde_json::from_value::<CaseReport>(value.clone())
                .map_err(|error| format!("case {index}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let stale = apply(&mut cases, records);
    // Only each lane's `triage` changes, so a field this build doesn't read
    // survives.
    for (value, case) in values.iter_mut().zip(&cases) {
        set_triage(value, case.triage.as_ref())?;
        let lanes = value.get_mut("generator").and_then(Value::as_array_mut);
        for (lane, report) in lanes.into_iter().flatten().zip(&case.generator) {
            set_triage(lane, report.triage.as_ref())?;
        }
    }
    Ok(stale)
}

/// Sets the `triage` field of the lane `value`, or removes it for `None`.
fn set_triage(value: &mut Value, triage: Option<&Triage>) -> Result<(), BoxError> {
    let lane = value.as_object_mut().ok_or("a lane isn't a JSON object")?;
    match triage {
        Some(triage) => {
            lane.insert("triage".to_owned(), serde_json::to_value(triage)?);
        }
        None => {
            lane.remove("triage");
        }
    }
    Ok(())
}

/// Runs `cargo xtask planted triage --report <file> [--out <file>]`: applies
/// `repo_root`'s [`TRIAGE_FILE`] to the report afresh ([`reapply`]), writes
/// it to `--out`, or back to `--report` without one, and prints the stale
/// records and the lanes still untriaged.
pub(crate) fn run(
    repo_root: &Path,
    mut args: impl Iterator<Item = String>,
) -> Result<(), BoxError> {
    let (mut report, mut out) = (None, None);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--report" if report.is_none() => report = Some(value),
            "--out" if out.is_none() => out = Some(value),
            _ => return Err(format!("unknown or repeated flag `{flag}`").into()),
        }
    }
    let report = report.ok_or(crate::UsageError("`planted triage` needs --report <file>"))?;
    let out = out.unwrap_or_else(|| report.clone());
    let records = read(repo_root)?;
    let mut value: Value = serde_json::from_slice(&fs::read(&report)?)
        .map_err(|error| format!("{report}: {error}"))?;
    for stale in reapply(&mut value, &records)? {
        println!("stale triage: {}: it matches no lane", stale.describe());
    }
    let cases: Vec<CaseReport> = serde_json::from_value(value["cases"].clone())?;
    fs::write(&out, serde_json::to_string_pretty(&value)? + "\n")?;
    println!(
        "{} lanes untriaged ({TRIAGE_FILE}); report in {out}",
        untriaged(&cases)
    );
    Ok(())
}
