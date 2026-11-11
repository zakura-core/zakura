//! The mutation loop's recorded triage (mutation plan, mut-5).
//!
//! A lane that survived, or that another rule killed, needs a reason before
//! the run counts as triaged. [`TRIAGE_FILE`] records each reason once, by
//! the case's patch and property and the lane: `witness`, or the generator
//! harness, which covers every seed. The loop reads the file from the
//! checkout it runs from, and copies each record onto the lanes it matches,
//! so every later report carries it too. A record whose lane no longer
//! needs triage is stale, and the loop lists it.

use std::{fs, io, path::Path};

use serde::{Deserialize, Serialize};

use super::{classify::Class, CaseReport};
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

/// One record in [`TRIAGE_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::planted) struct Record {
    /// The case's patch, as the report names it.
    pub(in crate::planted) patch: String,
    pub(in crate::planted) property: String,
    /// `witness`, or a generator harness.
    pub(in crate::planted) lane: String,
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

    fn names(&self, case: &CaseReport, lane: &str) -> bool {
        (
            self.patch.as_str(),
            self.property.as_str(),
            self.lane.as_str(),
        ) == (case.patch.as_str(), case.property.as_str(), lane)
    }
}

/// True for a class that needs triage: a survivor, or a kill by another
/// rule.
pub(in crate::planted) fn needs_triage(class: Class) -> bool {
    matches!(class, Class::Survived | Class::KilledByOtherRule)
}

/// The records in `root`'s [`TRIAGE_FILE`], or none if it has no such
/// file.
pub(super) fn read(root: &Path) -> Result<Vec<Record>, BoxError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct File {
        #[serde(default)]
        triage: Vec<Record>,
    }
    match fs::read_to_string(root.join(TRIAGE_FILE)) {
        Ok(text) => Ok(toml::from_str::<File>(&text)?.triage),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

/// Copies each of `records` onto every lane of `cases` it names that needs
/// triage. Returns the stale records: those that named no such lane.
pub(super) fn apply<'a>(cases: &mut [CaseReport], records: &'a [Record]) -> Vec<&'a Record> {
    let mut used = vec![false; records.len()];
    let mut find = |case: &CaseReport, lane: &str| {
        let index = records.iter().position(|record| record.names(case, lane))?;
        used[index] = true;
        Some(records[index].triage())
    };
    for case in cases.iter_mut() {
        if needs_triage(case.result.class) {
            case.triage = find(case, WITNESS_LANE);
        }
        let generator = std::mem::take(&mut case.generator);
        case.generator = generator
            .into_iter()
            .map(|mut lane| {
                if needs_triage(lane.result.class) {
                    lane.triage = find(case, &lane.harness);
                }
                lane
            })
            .collect();
    }
    records
        .iter()
        .zip(used)
        .filter(|(_, used)| !used)
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
