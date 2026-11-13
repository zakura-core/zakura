//! The sim-7 evaluator: each expected result's verdict, each row's verdict
//! from its results, and when the record is accepted (the sim-7 plan,
//! "The evaluator").

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One expected result's, or one row's, verdict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Verdict {
    /// It ran, bound to S, and passed.
    Verified,
    /// An approved exclusion names it.
    Excluded {
        /// The approval, as the exclusion records it.
        approval: String,
    },
    /// It's missing, failed, or isn't bound to S.
    Blocked {
        /// Why, in words.
        why: String,
    },
}

impl Verdict {
    /// How bad the verdict is: blocked over excluded over verified.
    fn rank(&self) -> u8 {
        match self {
            Verdict::Verified => 0,
            Verdict::Excluded { .. } => 1,
            Verdict::Blocked { .. } => 2,
        }
    }
}

/// A row of the manifest, as the evaluator needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowSpec {
    /// The requirement's ID, such as `M7.2` or `S4.4`.
    pub id: String,
    /// The IDs of its expected results.
    pub results: Vec<String>,
    /// Whether an exclusion of any of its results blocks it instead: the
    /// downstream row, the discovery row, the saved-plan row and every
    /// sched-4 row.
    pub never_excluded: bool,
}

/// One row's verdict, with each of its results'.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowVerdict {
    /// The row's ID.
    pub id: String,
    /// Its verdict: its worst result's, with an exclusion turned into a
    /// block on a row that can't be excluded.
    pub verdict: Verdict,
    /// Each result's ID and verdict, in the row's order.
    pub results: Vec<(String, Verdict)>,
}

/// Judges `row` from `verdicts`, the verdict of each expected result the
/// run produced: a result with no verdict is blocked as missing. A row
/// takes its worst result's verdict, and a row with no results is blocked,
/// so an empty row can't pass.
pub fn judge_row(row: &RowSpec, verdicts: &BTreeMap<String, Verdict>) -> RowVerdict {
    let results: Vec<(String, Verdict)> = row
        .results
        .iter()
        .map(|id| {
            let verdict = verdicts
                .get(id)
                .cloned()
                .unwrap_or_else(|| Verdict::Blocked {
                    why: format!("{id} has no result"),
                });
            (id.clone(), verdict)
        })
        .collect();
    let worst = results
        .iter()
        .map(|(_, verdict)| verdict)
        .max_by_key(|verdict| verdict.rank())
        .cloned()
        .unwrap_or_else(|| Verdict::Blocked {
            why: "the row has no expected result".to_owned(),
        });
    let verdict = match worst {
        Verdict::Excluded { approval } if row.never_excluded => Verdict::Blocked {
            why: format!("this row can't be excluded, but a result is ({approval})"),
        },
        verdict => verdict,
    };
    RowVerdict {
        id: row.id.clone(),
        verdict,
        results,
    }
}

/// The rows' verdicts, and whether they make the record accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evaluation {
    /// Each row's verdict, in the manifest's order.
    pub rows: Vec<RowVerdict>,
    /// The applicable evaluations the suite's passing results counted, in
    /// all.
    pub applicable: u64,
    /// No row is blocked, and the suite counted something applicable.
    pub accepted: bool,
    /// Accepted, and no row is excluded either.
    pub fully_accepted: bool,
}

/// Judges every row of `rows` from `verdicts`. The record is accepted when
/// no row is blocked and `applicable`, the suite's applicable count, is
/// positive, and fully accepted when no row is excluded either.
pub fn evaluate(
    rows: &[RowSpec],
    verdicts: &BTreeMap<String, Verdict>,
    applicable: u64,
) -> Evaluation {
    let rows: Vec<RowVerdict> = rows.iter().map(|row| judge_row(row, verdicts)).collect();
    let blocked = rows
        .iter()
        .any(|row| matches!(row.verdict, Verdict::Blocked { .. }));
    let excluded = rows
        .iter()
        .any(|row| matches!(row.verdict, Verdict::Excluded { .. }));
    let accepted = !blocked && applicable > 0;
    Evaluation {
        rows,
        applicable,
        accepted,
        fully_accepted: accepted && !excluded,
    }
}

/// What the evaluator needs from hdr-14's published record before any of
/// its keys count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderRecordFacts {
    /// Its subject commit.
    pub subject: String,
    /// Whether its run was contained under a subreaper.
    pub subreaper: bool,
    /// Its `accepted` field.
    pub accepted: bool,
    /// Its strays.
    pub stray: Vec<String>,
    /// What `header-acceptance verify` said about it.
    pub verified: Result<(), String>,
}

/// Why the header record can't be consumed at `subject`, or `None` if it
/// can: its subject must be `subject`, its run contained under a
/// subreaper, its `accepted` true, its strays empty, and `verify` must have
/// bound it to its run. While it's refused, every header key blocks.
pub fn header_refusal(facts: &HeaderRecordFacts, subject: &str) -> Option<String> {
    if facts.subject != subject {
        return Some(format!(
            "the header record's subject is {}, not {subject}",
            facts.subject
        ));
    }
    if !facts.subreaper {
        return Some("the header run wasn't contained under a subreaper".to_owned());
    }
    if !facts.accepted {
        return Some("the header record isn't accepted".to_owned());
    }
    if let Some(stray) = facts.stray.first() {
        return Some(format!("the header record has a stray: {stray}"));
    }
    facts
        .verified
        .as_ref()
        .err()
        .map(|why| format!("the header record isn't bound to its run: {why}"))
}
