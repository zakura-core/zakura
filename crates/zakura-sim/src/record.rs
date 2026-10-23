//! The records a run emits, in traces.md's shapes.
//!
//! A run emits one `init` record, one record per step, drain included, and
//! one `end` record, in that order, to a [`RecordSink`]. Trace files, `meta`,
//! replay and the reader belong to M7 (traces.md, T1).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zakura_scheduler::CleanupReport;

use crate::{
    check::{PropertyReport, Violation},
    end::{Drain, IdleStatus},
    plan::By,
    HexU64, Observation,
};

/// The record shapes and field encodings. Any field or variant change bumps
/// it.
pub const FORMAT_VERSION: u32 = 1;

/// What records mean: the identity grammar, the default-pick and default-draw
/// rules, when `AdvanceTime` is enabled, and what `kind#n` counts.
pub const CONTRACT_VERSION: u32 = 1;

/// One draw's identity, bound, answer, and what chose the answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrawRecord {
    /// `<task name>:<kind>#<n>`, or `setup:<kind>#<n>` outside any task.
    pub draw: String,
    /// The answer is below this, or any `u64` when it's 0.
    pub bound: HexU64,
    /// The answer production code saw.
    pub value: HexU64,
    /// What chose the answer.
    pub by: By,
}

/// The draws taken before the first step.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitRecord {
    /// The draws, in order.
    pub draws: Vec<DrawRecord>,
}

/// One step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepRecord {
    /// The step, from 1.
    pub step: u64,
    /// The pick's identity.
    pub pick: String,
    /// The pick's match key, when it differs from `pick`. M7 sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// How many actions were enabled.
    pub options: usize,
    /// What chose the pick.
    pub by: By,
    /// Where the pick's bytes start in the input, when bytes chose it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    /// The draws taken during the step, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub draws: Vec<DrawRecord>,
    /// The first 8 bytes of SHA-256 over the step's observations as a JSON
    /// array, in hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obs: Option<String>,
}

/// How a run ended, as the `end` record names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutcomeKind {
    /// A property was violated.
    Violation,
    /// The step budget ran out.
    Budget,
    /// Nothing was enabled. `status` says why.
    Idle,
    /// The simulation itself failed. `harness` says how.
    Harness,
}

/// The counts from the scheduler's [`CleanupReport`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupRecord {
    /// Unfinished tasks cancelled at a poll boundary.
    pub cancelled: usize,
    /// Tasks that panicked during cleanup.
    pub panicked: usize,
    /// Parked tasks force-unwound.
    pub unwound: usize,
    /// Forced unwinds a task caught.
    pub unwind_failures: usize,
    /// Tasks stuck in cleanup, leaked without running their destructors.
    pub leaked: usize,
    /// Timers still registered.
    pub timers_left: usize,
}

impl From<CleanupReport> for CleanupRecord {
    fn from(report: CleanupReport) -> Self {
        CleanupRecord {
            cancelled: report.cancelled,
            panicked: report.panicked,
            unwound: report.unwound,
            unwind_failures: report.unwind_failures,
            leaked: report.leaked,
            timers_left: report.timers_left,
        }
    }
}

/// How the run ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndRecord {
    /// How it ended.
    pub outcome: OutcomeKind,
    /// Why nothing was enabled, for `idle`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<IdleStatus>,
    /// Every step, drain included.
    pub steps: u64,
    /// The violation, for `violation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub violation: Option<Violation>,
    /// The drain, for `budget`. Always present, `null` without a drain.
    pub drain: Option<Drain>,
    /// The failure, for `harness`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// What shutdown cleaned up.
    pub cleanup: CleanupRecord,
    /// Each property's outcome.
    pub properties: Vec<PropertyReport>,
}

/// One line of a run's records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "Line", from = "Line")]
pub enum Record {
    /// Draws before the first step, as `{"init":{...}}`.
    Init(InitRecord),
    /// A step, as a bare object with a `step` field.
    Step(StepRecord),
    /// The run's end, as `{"end":{...}}`.
    End(EndRecord),
}

impl Record {
    /// The record as one JSON line, without the newline.
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).expect("records hold only strings, integers and lists")
    }
}

/// A record's JSON shape. Each variant's own fields tell them apart.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Line {
    Init(InitLine),
    Step(StepRecord),
    End(EndLine),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitLine {
    init: InitRecord,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndLine {
    end: EndRecord,
}

impl From<Record> for Line {
    fn from(record: Record) -> Self {
        match record {
            Record::Init(init) => Line::Init(InitLine { init }),
            Record::Step(step) => Line::Step(step),
            Record::End(end) => Line::End(EndLine { end }),
        }
    }
}

impl From<Line> for Record {
    fn from(line: Line) -> Self {
        match line {
            Line::Init(InitLine { init }) => Record::Init(init),
            Line::Step(step) => Record::Step(step),
            Line::End(EndLine { end }) => Record::End(end),
        }
    }
}

/// Receives a run's records as they're made.
pub trait RecordSink {
    /// Takes the next record.
    fn record(&mut self, record: Record);
}

/// A sink that keeps every record in memory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemorySink {
    /// The records, in order.
    pub records: Vec<Record>,
}

impl MemorySink {
    /// Every record as a JSON line.
    pub fn lines(&self) -> Vec<String> {
        self.records.iter().map(Record::to_line).collect()
    }
}

impl RecordSink for MemorySink {
    fn record(&mut self, record: Record) {
        self.records.push(record);
    }
}

/// The `obs` digest of one step's observations.
pub(crate) fn digest<'a>(observations: impl Iterator<Item = &'a Observation>) -> String {
    let observations: Vec<&Observation> = observations.collect();
    let json =
        serde_json::to_vec(&observations).expect("observations hold only strings and integers");
    Sha256::digest(&json)[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::PropertyOutcome;

    /// One record of each kind, with every optional field set.
    fn samples() -> Vec<Record> {
        vec![
            Record::Init(InitRecord {
                draws: vec![DrawRecord {
                    draw: "setup:hash-seed#1".into(),
                    bound: HexU64(0),
                    value: HexU64(0x9f2c_4e1a_77d0_b351),
                    by: By::Bytes,
                }],
            }),
            Record::Step(StepRecord {
                step: 41,
                pick: "task:root.1:select#3".into(),
                key: None,
                options: 3,
                by: By::Bytes,
                offset: Some(17),
                draws: vec![DrawRecord {
                    draw: "root.1:select#5".into(),
                    bound: HexU64(4),
                    value: HexU64(2),
                    by: By::Default,
                }],
                obs: Some("5d1e09aa3c4f7b21".into()),
            }),
            Record::End(EndRecord {
                outcome: OutcomeKind::Idle,
                status: Some(IdleStatus::Deadlock),
                steps: 42,
                violation: None,
                drain: None,
                harness: None,
                cleanup: CleanupRecord::default(),
                properties: vec![PropertyReport {
                    property: "sim/no-livelock".into(),
                    outcome: PropertyOutcome::Held { applicable: 3 },
                }],
            }),
        ]
    }

    #[test]
    fn records_match_the_trace_shapes() {
        let lines: Vec<String> = samples().iter().map(Record::to_line).collect();
        assert_eq!(
            lines,
            [
                r#"{"init":{"draws":[{"draw":"setup:hash-seed#1","bound":"0x0","value":"0x9f2c4e1a77d0b351","by":"bytes"}]}}"#,
                r#"{"step":41,"pick":"task:root.1:select#3","options":3,"by":"bytes","offset":17,"draws":[{"draw":"root.1:select#5","bound":"0x4","value":"0x2","by":"default"}],"obs":"5d1e09aa3c4f7b21"}"#,
                r#"{"end":{"outcome":"idle","status":"deadlock","steps":42,"drain":null,"cleanup":{"cancelled":0,"panicked":0,"unwound":0,"unwind_failures":0,"leaked":0,"timers_left":0},"properties":[{"property":"sim/no-livelock","outcome":{"held":{"applicable":3}}}]}}"#,
            ]
        );
    }

    #[test]
    fn records_round_trip_and_reject_unknown_fields() {
        for record in samples() {
            let line = record.to_line();
            assert_eq!(serde_json::from_str::<Record>(&line).unwrap(), record);
        }
        for bad in [
            r#"{"init":{"draws":[]},"extra":1}"#,
            r#"{"step":1,"pick":"advance-time:0","options":1,"by":"forced","extra":1}"#,
            r#"{"step":1,"pick":"advance-time:0","options":1,"by":"guessed"}"#,
        ] {
            assert!(serde_json::from_str::<Record>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn digests_are_8_bytes_of_hex() {
        let obs = [Observation::TaskWaiting {
            task: "root.1".into(),
        }];
        let digest = digest(obs.iter());
        assert_eq!(digest.len(), 16);
        assert_eq!(digest, super::digest(obs.iter()));
        assert_ne!(digest, super::digest([].iter()));
    }
}
