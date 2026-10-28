//! The records a run emits, in traces.md's shapes.
//!
//! A run emits one `init` record, one record per step, drain included, and
//! one `end` record, in that order, to a [`RecordSink`]. A trace file puts a
//! `meta` record before them (see [`crate::trace`]).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zakura_scheduler::CleanupReport;

use crate::{
    check::{PropertyReport, Violation},
    end::{Drain, IdleStatus},
    plan::By,
    trace::MetaRecord,
    HexU64, Observation,
};

/// The record shapes and field encodings. Any field or variant change bumps
/// it. Version 2 replaced storage operation events with typed storage facts
/// and states. Version 3 added header-sync reactor events. Version 4 added
/// block storage facts, requests, results and states. Version 5 added
/// block-sync events. Version 6 added the block-sync serving admission.
pub const FORMAT_VERSION: u32 = 6;

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

/// A violation as the `end` record holds it: the checker's [`Violation`],
/// plus a match key for each evidence step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViolationRecord {
    /// The property's ID, such as `sim/no-livelock`.
    pub property: String,
    /// The step the check failed at.
    pub step: u64,
    /// The steps the violation depends on, in order.
    pub witness: Vec<u64>,
    /// The recorded action match key of each `witness` step, in the same
    /// order. Until match keys exist, each is the step's pick identity.
    pub witness_keys: Vec<String>,
    /// What the property expected.
    pub expected: String,
    /// What the run did.
    pub actual: String,
}

impl ViolationRecord {
    /// `violation`'s record, with `witness_keys` as its match keys.
    pub fn new(violation: &Violation, witness_keys: Vec<String>) -> Self {
        ViolationRecord {
            property: violation.property.clone(),
            step: violation.step,
            witness: violation.witness.clone(),
            witness_keys,
            expected: violation.expected.clone(),
            actual: violation.actual.clone(),
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
    pub violation: Option<ViolationRecord>,
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
    /// A trace's line 1, as `{"meta":{...}}`. Boxed, since it's far larger
    /// than the others and written once.
    Meta(Box<MetaRecord>),
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
    Meta(MetaLine),
    Init(InitLine),
    Step(StepRecord),
    End(EndLine),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaLine {
    meta: Box<MetaRecord>,
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
            Record::Meta(meta) => Line::Meta(MetaLine { meta }),
            Record::Init(init) => Line::Init(InitLine { init }),
            Record::Step(step) => Line::Step(step),
            Record::End(end) => Line::End(EndLine { end }),
        }
    }
}

impl From<Line> for Record {
    fn from(line: Line) -> Self {
        match line {
            Line::Meta(MetaLine { meta }) => Record::Meta(meta),
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
    use crate::{check::PropertyOutcome, observe::Exit};

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
                properties: vec![PropertyReport::new(
                    "sim/no-livelock",
                    PropertyOutcome::Held { applicable: 3 },
                )],
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
            ],
            "a record's encoding changed, so bump FORMAT_VERSION and update these lines"
        );
    }

    #[test]
    fn observation_digests_are_pinned() {
        let obs = [
            Observation::TaskSpawned {
                task: "root.1".into(),
            },
            Observation::TaskReached {
                task: "root.1".into(),
                at: "lock#2".into(),
                site: Some("src/lib.rs:7".into()),
            },
            Observation::TaskWaiting {
                task: "root.1".into(),
            },
            Observation::TaskParked {
                task: "root.1".into(),
            },
            Observation::TaskExited {
                task: "root.1".into(),
                exit: Exit::Completed,
            },
            Observation::TimeAdvanced {
                to_ns: 1_000,
                fired: 2,
            },
            Observation::Fake {
                source: "net".into(),
                event: "deliver".into(),
            },
        ];
        let bump = "an observation's encoding changed, which changes every `obs` digest, \
            so bump FORMAT_VERSION and update this test";
        assert_eq!(
            serde_json::to_string(&obs).unwrap(),
            r#"[{"obs":"task-spawned","task":"root.1"},{"obs":"task-reached","task":"root.1","at":"lock#2","site":"src/lib.rs:7"},{"obs":"task-waiting","task":"root.1"},{"obs":"task-parked","task":"root.1"},{"obs":"task-exited","task":"root.1","exit":"completed"},{"obs":"time-advanced","to_ns":1000,"fired":2},{"obs":"fake","source":"net","event":"deliver"}]"#,
            "{bump}"
        );
        assert_eq!(digest(obs.iter()), "917a4440abdd4f53", "{bump}");
    }

    /// Pins the storage observations separately, since their types are
    /// many and change together.
    #[test]
    fn storage_observation_encoding_is_pinned() {
        use crate::observe::{
            HeaderStorageState, StorageBranch, StorageFact, StorageLease, StorageLeaseState,
            StorageOperation, StoragePoint, StorageRequest, StorageResult, StorageState, WorkOwner,
            WorkScope,
        };

        let scope = || WorkScope {
            header_generation: 2,
            branch: StorageBranch {
                anchor: "aa".into(),
                target: "bb".into(),
            },
        };
        let lease = || StorageLease {
            peer: "01".into(),
            session_id: 1,
            lease_id: 3,
        };
        let point = |height| StoragePoint {
            height,
            hash: "cc".into(),
        };
        let storage = |seq, fact| Observation::Storage {
            service: "header".into(),
            instance: 1,
            seq,
            txn: 4,
            fact,
        };
        let request = StorageRequest::ApplyHeaderTarget {
            owner: WorkOwner {
                kind: "body-repair".into(),
                scope: scope(),
                verified_generation: Some(5),
                body_work_epoch: Some(6),
                session_id: 7,
                request_id: 8,
            },
            source: "02".into(),
            parent: Some("dd".into()),
            target: "bb".into(),
        };
        let obs = [
            storage(
                1,
                StorageFact::Submitted {
                    id: 1,
                    request: request.clone(),
                    digest: "ef".into(),
                },
            ),
            storage(
                2,
                StorageFact::Leased {
                    lease: lease(),
                    scope: scope(),
                    target: point(3),
                    common_ancestor: point(0),
                    next: Some(0),
                    capacity: "general".into(),
                    continued_from: None,
                },
            ),
            storage(
                3,
                StorageFact::RetentionEnded {
                    lease: lease(),
                    why: "continued".into(),
                    into: Some(4),
                },
            ),
            storage(
                4,
                StorageFact::WaiterSet {
                    class: "peer".into(),
                    lease: Some(lease()),
                },
            ),
            storage(
                5,
                StorageFact::Completed {
                    id: 1,
                    outcome: "engine".into(),
                    result: StorageResult::Acquired {
                        lease: lease(),
                        common_ancestor: point(0),
                        target: point(3),
                    },
                    delivered: true,
                    generation: 2,
                    state: "ab".into(),
                },
            ),
            storage(
                6,
                StorageFact::ShutDown {
                    dropped: vec![1],
                    deferred: Vec::new(),
                },
            ),
            Observation::StorageState {
                service: "header".into(),
                instance: 1,
                through_seq: 6,
                state: StorageState::Header(HeaderStorageState {
                    operations: vec![StorageOperation {
                        id: 1,
                        request,
                        stage: "replied".into(),
                        caller_waiting: None,
                        result: Some(StorageResult::Busy),
                        caller_alive: true,
                    }],
                    permits: vec![2],
                    leases: vec![StorageLeaseState {
                        lease: lease(),
                        scope: scope(),
                        target: point(3),
                        last: point(0),
                        next: None,
                        capacity: "bounded".into(),
                        waiter: false,
                        leased_txn: Some(4),
                    }],
                    continuations: Vec::new(),
                    general_waiter: false,
                    fallback_waiter: true,
                    last_lease_id: 3,
                    continuation_hits: 1,
                }),
            },
        ];
        assert_eq!(
            serde_json::to_string(&obs).unwrap(),
            r#"[{"obs":"storage","service":"header","instance":1,"seq":1,"txn":4,"fact":{"fact":"submitted","id":1,"request":{"op":"apply_header_target","owner":{"kind":"body-repair","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"source":"02","parent":"dd","target":"bb"},"digest":"ef"}},{"obs":"storage","service":"header","instance":1,"seq":2,"txn":4,"fact":{"fact":"leased","lease":{"peer":"01","session_id":1,"lease_id":3},"scope":{"header_generation":2,"branch":{"anchor":"aa","target":"bb"}},"target":{"height":3,"hash":"cc"},"common_ancestor":{"height":0,"hash":"cc"},"next":0,"capacity":"general"}},{"obs":"storage","service":"header","instance":1,"seq":3,"txn":4,"fact":{"fact":"retention-ended","lease":{"peer":"01","session_id":1,"lease_id":3},"why":"continued","into":4}},{"obs":"storage","service":"header","instance":1,"seq":4,"txn":4,"fact":{"fact":"waiter-set","class":"peer","lease":{"peer":"01","session_id":1,"lease_id":3}}},{"obs":"storage","service":"header","instance":1,"seq":5,"txn":4,"fact":{"fact":"completed","id":1,"outcome":"engine","result":{"result":"acquired","lease":{"peer":"01","session_id":1,"lease_id":3},"common_ancestor":{"height":0,"hash":"cc"},"target":{"height":3,"hash":"cc"}},"delivered":true,"generation":2,"state":"ab"}},{"obs":"storage","service":"header","instance":1,"seq":6,"txn":4,"fact":{"fact":"shut-down","dropped":[1]}},{"obs":"storage-state","service":"header","instance":1,"through_seq":6,"state":{"operations":[{"id":1,"request":{"op":"apply_header_target","owner":{"kind":"body-repair","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"source":"02","parent":"dd","target":"bb"},"stage":"replied","result":{"result":"busy"},"caller_alive":true}],"permits":[2],"leases":[{"lease":{"peer":"01","session_id":1,"lease_id":3},"scope":{"header_generation":2,"branch":{"anchor":"aa","target":"bb"}},"target":{"height":3,"hash":"cc"},"last":{"height":0,"hash":"cc"},"next":null,"capacity":"bounded","waiter":false,"leased_txn":4}],"continuations":[],"general_waiter":false,"fallback_waiter":true,"last_lease_id":3,"continuation_hits":1}}]"#,
            "a storage observation's encoding changed, which changes `obs` digests, \
             so bump FORMAT_VERSION and update this test"
        );
    }

    /// Pins the block storage observations, one of each fact, request,
    /// result and stage, and the format version that added them.
    #[test]
    fn block_storage_observation_encoding_is_pinned() {
        use crate::observe::{
            BlockOperation, BlockStorageState, BodyAvailability, BodyScope, StorageBranch,
            StorageFact, StoragePoint, StorageRequest, StorageResult, StorageState, WorkOwner,
            WorkScope,
        };

        let point = |height| StoragePoint {
            height,
            hash: "cc".into(),
        };
        let owner = WorkOwner {
            kind: "body".into(),
            scope: WorkScope {
                header_generation: 2,
                branch: StorageBranch {
                    anchor: "aa".into(),
                    target: "bb".into(),
                },
            },
            verified_generation: Some(5),
            body_work_epoch: Some(6),
            session_id: 1,
            request_id: 1,
        };
        let requests = [
            StorageRequest::QueryNeededBlocks {
                query_id: 1,
                from: 1,
                limit: 16,
                scope: BodyScope {
                    scope: owner.scope.clone(),
                    verified_generation: 5,
                    body_work_epoch: 6,
                },
                best_header_tip: 3,
            },
            StorageRequest::SubmitBlock {
                owner,
                source: "07".into(),
                token: 1,
                height: Some(1),
                hash: "cc".into(),
                parent: "aa".into(),
                class: "full".into(),
            },
            StorageRequest::RecordBody {
                record: "record_body_invalid".into(),
                expected_version: 1,
                hash: "cc".into(),
                evidence: "ee".into(),
                rule: Some("block.subsidy".into()),
                source: Some("07".into()),
                failure: None,
                availability: None,
            },
            StorageRequest::Misbehavior {
                peer: "03".into(),
                reason: "StatusSpam".into(),
            },
            StorageRequest::ReadRange {
                start_height: 1,
                count: 3,
                max_body_bytes: 1_900,
            },
        ];
        let results = [
            StorageResult::Admitted,
            StorageResult::NeededBlocks {
                anchor: point(0),
                blocks: vec![point(1)],
            },
            StorageResult::NeededBlocksFailed,
            StorageResult::Verdict {
                verdict: "committed".into(),
                reply: "committed".into(),
            },
            StorageResult::Recorded {
                attempts: vec!["stale".into(), "committed".into()],
            },
            StorageResult::RecordFailed,
            StorageResult::Served {
                heights: vec![1, 2],
                bytes: 1_800,
            },
            StorageResult::ReadFailed,
        ];
        let unavailable = StorageRequest::RecordBody {
            record: "record_body_unavailable".into(),
            expected_version: 2,
            hash: "cc".into(),
            evidence: "ef".into(),
            rule: None,
            source: None,
            failure: Some("timeout".into()),
            availability: Some(BodyAvailability {
                started_at: 1_000,
                attempts: 2,
                suppliers: 1,
                supplier_set_digest: "dd".into(),
                alarmed: false,
                next_probe_at: 2_000,
            }),
        };
        let storage = |seq, fact| Observation::Storage {
            service: "block".into(),
            instance: 1,
            seq,
            txn: 3,
            fact,
        };
        let mut obs: Vec<Observation> = (1..)
            .zip(requests.iter().cloned())
            .map(|(id, request)| {
                storage(
                    id,
                    StorageFact::BlockSubmitted {
                        id,
                        request,
                        stage: (id != 4).then(|| "pending".into()),
                    },
                )
            })
            .collect();
        obs.extend((1..).zip(results).map(|(id, result)| {
            storage(
                10 + id,
                StorageFact::BlockCompleted {
                    id,
                    outcome: "answer".into(),
                    result,
                    delivered: true,
                    state_version: 4,
                },
            )
        }));
        obs.extend([
            storage(20, StorageFact::Admitted { id: 2 }),
            storage(21, StorageFact::Coalesced { query_id: 1 }),
            storage(
                22,
                StorageFact::TransitionInstalled {
                    state_version: 4,
                    verified_best: point(1),
                    epoch_before: 1,
                    invalidates_body_work: true,
                    cause: Some("reset".into()),
                },
            ),
            storage(
                23,
                StorageFact::ViewPublished {
                    state_version: 4,
                    epoch: 1,
                    hint_revision: 0,
                    verified_best: point(1),
                    header_best: point(3),
                },
            ),
            storage(24, StorageFact::ReplyDelivered { id: 2 }),
            storage(25, StorageFact::HandleClosed { id: 2 }),
            storage(26, StorageFact::ReadTaken { id: 5 }),
            storage(27, StorageFact::ReadDropped { id: 5 }),
            storage(28, StorageFact::CallerDropped { id: 5 }),
            storage(
                19,
                StorageFact::Deferred {
                    queue: vec![unavailable.clone()],
                },
            ),
            storage(
                29,
                StorageFact::DriverExited {
                    dropped: vec![3],
                    deferred: vec![unavailable.clone()],
                },
            ),
            storage(
                30,
                StorageFact::ShutDown {
                    dropped: vec![2],
                    deferred: vec![unavailable.clone()],
                },
            ),
            Observation::StorageState {
                service: "block".into(),
                instance: 1,
                through_seq: 30,
                state: StorageState::Block(BlockStorageState {
                    operations: vec![
                        BlockOperation {
                            id: 2,
                            request: requests[1].clone(),
                            stage: "admitted".into(),
                            caller_waiting: None,
                            replaced: Some(false),
                            retained: Some("cc".into()),
                        },
                        BlockOperation {
                            id: 3,
                            request: requests[1].clone(),
                            stage: "queued".into(),
                            caller_waiting: None,
                            replaced: None,
                            retained: None,
                        },
                        BlockOperation {
                            id: 5,
                            request: requests[4].clone(),
                            stage: "pending".into(),
                            caller_waiting: Some(true),
                            replaced: None,
                            retained: None,
                        },
                        BlockOperation {
                            id: 6,
                            request: requests[4].clone(),
                            stage: "replied".into(),
                            caller_waiting: None,
                            replaced: None,
                            retained: None,
                        },
                    ],
                    deferred: vec![unavailable],
                }),
            },
        ]);
        let bump = "a block storage observation's encoding changed, which changes `obs` \
             digests, so bump FORMAT_VERSION and update this test";
        assert_eq!(FORMAT_VERSION, 6, "{bump}");
        assert_eq!(
            serde_json::to_string(&obs).unwrap(),
            r#"[{"obs":"storage","service":"block","instance":1,"seq":1,"txn":3,"fact":{"fact":"block-submitted","id":1,"request":{"op":"query_needed_blocks","query_id":1,"from":1,"limit":16,"scope":{"header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6},"best_header_tip":3},"stage":"pending"}},{"obs":"storage","service":"block","instance":1,"seq":2,"txn":3,"fact":{"fact":"block-submitted","id":2,"request":{"op":"submit_block","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":1,"request_id":1},"source":"07","token":1,"height":1,"hash":"cc","parent":"aa","class":"full"},"stage":"pending"}},{"obs":"storage","service":"block","instance":1,"seq":3,"txn":3,"fact":{"fact":"block-submitted","id":3,"request":{"op":"record_body","record":"record_body_invalid","expected_version":1,"hash":"cc","evidence":"ee","rule":"block.subsidy","source":"07"},"stage":"pending"}},{"obs":"storage","service":"block","instance":1,"seq":4,"txn":3,"fact":{"fact":"block-submitted","id":4,"request":{"op":"misbehavior","peer":"03","reason":"StatusSpam"},"stage":null}},{"obs":"storage","service":"block","instance":1,"seq":5,"txn":3,"fact":{"fact":"block-submitted","id":5,"request":{"op":"read_range","start_height":1,"count":3,"max_body_bytes":1900},"stage":"pending"}},{"obs":"storage","service":"block","instance":1,"seq":11,"txn":3,"fact":{"fact":"block-completed","id":1,"outcome":"answer","result":{"result":"admitted"},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":12,"txn":3,"fact":{"fact":"block-completed","id":2,"outcome":"answer","result":{"result":"needed-blocks","anchor":{"height":0,"hash":"cc"},"blocks":[{"height":1,"hash":"cc"}]},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":13,"txn":3,"fact":{"fact":"block-completed","id":3,"outcome":"answer","result":{"result":"needed-blocks-failed"},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":14,"txn":3,"fact":{"fact":"block-completed","id":4,"outcome":"answer","result":{"result":"verdict","verdict":"committed","reply":"committed"},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":15,"txn":3,"fact":{"fact":"block-completed","id":5,"outcome":"answer","result":{"result":"recorded","attempts":["stale","committed"]},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":16,"txn":3,"fact":{"fact":"block-completed","id":6,"outcome":"answer","result":{"result":"record-failed"},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":17,"txn":3,"fact":{"fact":"block-completed","id":7,"outcome":"answer","result":{"result":"served","heights":[1,2],"bytes":1800},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":18,"txn":3,"fact":{"fact":"block-completed","id":8,"outcome":"answer","result":{"result":"read-failed"},"delivered":true,"state_version":4}},{"obs":"storage","service":"block","instance":1,"seq":20,"txn":3,"fact":{"fact":"admitted","id":2}},{"obs":"storage","service":"block","instance":1,"seq":21,"txn":3,"fact":{"fact":"coalesced","query_id":1}},{"obs":"storage","service":"block","instance":1,"seq":22,"txn":3,"fact":{"fact":"transition-installed","state_version":4,"verified_best":{"height":1,"hash":"cc"},"epoch_before":1,"invalidates_body_work":true,"cause":"reset"}},{"obs":"storage","service":"block","instance":1,"seq":23,"txn":3,"fact":{"fact":"view-published","state_version":4,"epoch":1,"hint_revision":0,"verified_best":{"height":1,"hash":"cc"},"header_best":{"height":3,"hash":"cc"}}},{"obs":"storage","service":"block","instance":1,"seq":24,"txn":3,"fact":{"fact":"reply-delivered","id":2}},{"obs":"storage","service":"block","instance":1,"seq":25,"txn":3,"fact":{"fact":"handle-closed","id":2}},{"obs":"storage","service":"block","instance":1,"seq":26,"txn":3,"fact":{"fact":"read-taken","id":5}},{"obs":"storage","service":"block","instance":1,"seq":27,"txn":3,"fact":{"fact":"read-dropped","id":5}},{"obs":"storage","service":"block","instance":1,"seq":28,"txn":3,"fact":{"fact":"caller-dropped","id":5}},{"obs":"storage","service":"block","instance":1,"seq":19,"txn":3,"fact":{"fact":"deferred","queue":[{"op":"record_body","record":"record_body_unavailable","expected_version":2,"hash":"cc","evidence":"ef","failure":"timeout","availability":{"started_at":1000,"attempts":2,"suppliers":1,"supplier_set_digest":"dd","alarmed":false,"next_probe_at":2000}}]}},{"obs":"storage","service":"block","instance":1,"seq":29,"txn":3,"fact":{"fact":"driver-exited","dropped":[3],"deferred":[{"op":"record_body","record":"record_body_unavailable","expected_version":2,"hash":"cc","evidence":"ef","failure":"timeout","availability":{"started_at":1000,"attempts":2,"suppliers":1,"supplier_set_digest":"dd","alarmed":false,"next_probe_at":2000}}]}},{"obs":"storage","service":"block","instance":1,"seq":30,"txn":3,"fact":{"fact":"shut-down","dropped":[2],"deferred":[{"op":"record_body","record":"record_body_unavailable","expected_version":2,"hash":"cc","evidence":"ef","failure":"timeout","availability":{"started_at":1000,"attempts":2,"suppliers":1,"supplier_set_digest":"dd","alarmed":false,"next_probe_at":2000}}]}},{"obs":"storage-state","service":"block","instance":1,"through_seq":30,"state":{"operations":[{"id":2,"request":{"op":"submit_block","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":1,"request_id":1},"source":"07","token":1,"height":1,"hash":"cc","parent":"aa","class":"full"},"stage":"admitted","replaced":false,"retained":"cc"},{"id":3,"request":{"op":"submit_block","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":1,"request_id":1},"source":"07","token":1,"height":1,"hash":"cc","parent":"aa","class":"full"},"stage":"queued"},{"id":5,"request":{"op":"read_range","start_height":1,"count":3,"max_body_bytes":1900},"stage":"pending","caller_waiting":true},{"id":6,"request":{"op":"read_range","start_height":1,"count":3,"max_body_bytes":1900},"stage":"replied"}],"deferred":[{"op":"record_body","record":"record_body_unavailable","expected_version":2,"hash":"cc","evidence":"ef","failure":"timeout","availability":{"started_at":1000,"attempts":2,"suppliers":1,"supplier_set_digest":"dd","alarmed":false,"next_probe_at":2000}}]}}]"#,
            "{bump}"
        );
    }

    /// Pins the reactor observations, one of each event and call kind, and
    /// the format version that added them.
    #[test]
    fn reactor_observation_encoding_is_pinned() {
        use crate::observe::{
            ReactorCall, ReactorFact, ReactorPurpose, ReactorRequest, StorageBranch, StoragePoint,
            WorkOwner, WorkScope,
        };

        let scope = || WorkScope {
            header_generation: 2,
            branch: StorageBranch {
                anchor: "aa".into(),
                target: "bb".into(),
            },
        };
        let owner = |kind: &str| WorkOwner {
            kind: kind.into(),
            scope: scope(),
            verified_generation: (kind == "body-repair").then_some(5),
            body_work_epoch: (kind == "body-repair").then_some(6),
            session_id: 7,
            request_id: 8,
        };
        let request = || ReactorRequest {
            peer: "01".into(),
            session_id: 7,
            request_id: 8,
            owner: owner("header"),
            target: "bb".into(),
        };
        let repair = || ReactorPurpose::SelectedAuxiliaryRepair {
            selected_target: StoragePoint {
                height: 3,
                hash: "cc".into(),
            },
            repair_generation: 4,
        };
        let reactor = |seq, event| Observation::Reactor {
            instance: 0,
            seq,
            storage_seq: seq + 10,
            node: "node".into(),
            event,
        };
        let obs = [
            reactor(
                1,
                ReactorFact::Dispatched {
                    call: 1,
                    port: ReactorCall::Locator {
                        peer: "01".into(),
                        session_id: 7,
                        target: "bb".into(),
                        scope: scope(),
                    },
                },
            ),
            reactor(
                2,
                ReactorFact::Dispatched {
                    call: 2,
                    port: ReactorCall::VctContext {
                        owner: owner("body-repair"),
                        height: 3,
                    },
                },
            ),
            reactor(
                3,
                ReactorFact::Dispatched {
                    call: 3,
                    port: ReactorCall::Prepare {
                        peer: "01".into(),
                        source: "02".into(),
                        owner: owner("header"),
                    },
                },
            ),
            reactor(
                4,
                ReactorFact::Dispatched {
                    call: 4,
                    port: ReactorCall::Apply {
                        peer: "01".into(),
                        source: "02".into(),
                        owner: owner("header"),
                    },
                },
            ),
            reactor(
                5,
                ReactorFact::CallEnded {
                    call: 1,
                    end: "timed-out".into(),
                    op: None,
                },
            ),
            reactor(
                6,
                ReactorFact::CallEnded {
                    call: 3,
                    end: "replied".into(),
                    op: Some(9),
                },
            ),
            reactor(
                7,
                ReactorFact::Handled {
                    call: 1,
                    panicked: false,
                },
            ),
            reactor(
                8,
                ReactorFact::Started {
                    request: request(),
                    purpose: ReactorPurpose::Normal,
                },
            ),
            reactor(
                9,
                ReactorFact::Retired {
                    request: request(),
                    phase: "applying".into(),
                    purpose: repair(),
                    reason: "snapshot_obsolete".into(),
                },
            ),
            reactor(
                10,
                ReactorFact::OfferWaiting {
                    peer: "01".into(),
                    session_id: 7,
                    target: "bb".into(),
                    cause: "staging-full".into(),
                },
            ),
        ];
        let bump = "a reactor observation's encoding changed, which changes `obs` digests, \
             so bump FORMAT_VERSION and update this test";
        assert_eq!(FORMAT_VERSION, 6, "{bump}");
        assert_eq!(
            serde_json::to_string(&obs).unwrap(),
            r#"[{"obs":"reactor","instance":0,"seq":1,"storage_seq":11,"node":"node","event":{"event":"dispatched","call":1,"port":{"kind":"locator","peer":"01","session_id":7,"target":"bb","scope":{"header_generation":2,"branch":{"anchor":"aa","target":"bb"}}}}},{"obs":"reactor","instance":0,"seq":2,"storage_seq":12,"node":"node","event":{"event":"dispatched","call":2,"port":{"kind":"vct-context","owner":{"kind":"body-repair","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"height":3}}},{"obs":"reactor","instance":0,"seq":3,"storage_seq":13,"node":"node","event":{"event":"dispatched","call":3,"port":{"kind":"prepare","peer":"01","source":"02","owner":{"kind":"header","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"session_id":7,"request_id":8}}}},{"obs":"reactor","instance":0,"seq":4,"storage_seq":14,"node":"node","event":{"event":"dispatched","call":4,"port":{"kind":"apply","peer":"01","source":"02","owner":{"kind":"header","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"session_id":7,"request_id":8}}}},{"obs":"reactor","instance":0,"seq":5,"storage_seq":15,"node":"node","event":{"event":"call-ended","call":1,"end":"timed-out","op":null}},{"obs":"reactor","instance":0,"seq":6,"storage_seq":16,"node":"node","event":{"event":"call-ended","call":3,"end":"replied","op":9}},{"obs":"reactor","instance":0,"seq":7,"storage_seq":17,"node":"node","event":{"event":"handled","call":1,"panicked":false}},{"obs":"reactor","instance":0,"seq":8,"storage_seq":18,"node":"node","event":{"event":"started","request":{"peer":"01","session_id":7,"request_id":8,"owner":{"kind":"header","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"session_id":7,"request_id":8},"target":"bb"},"purpose":{"purpose":"normal"}}},{"obs":"reactor","instance":0,"seq":9,"storage_seq":19,"node":"node","event":{"event":"retired","request":{"peer":"01","session_id":7,"request_id":8,"owner":{"kind":"header","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"session_id":7,"request_id":8},"target":"bb"},"phase":"applying","purpose":{"purpose":"selected-auxiliary-repair","selected_target":{"height":3,"hash":"cc"},"repair_generation":4},"reason":"snapshot_obsolete"}},{"obs":"reactor","instance":0,"seq":10,"storage_seq":20,"node":"node","event":{"event":"offer-waiting","peer":"01","session_id":7,"target":"bb","cause":"staging-full"}}]"#,
            "{bump}"
        );
    }

    /// Pins the block-sync observations, one of each event and holder
    /// kind, and the format version that added them.
    #[test]
    fn block_sync_observation_encoding_is_pinned() {
        use crate::observe::{
            BlockSyncBody, BlockSyncClaim, BlockSyncFact, BlockSyncHolder, BlockSyncRange,
            BlockSyncRequest, BlockSyncRequestedHeight, BlockSyncRetained, BlockSyncSubmission,
            BlockSyncWorkItem, StorageBranch, WorkOwner, WorkScope,
        };

        let owner = || WorkOwner {
            kind: "body".into(),
            scope: WorkScope {
                header_generation: 2,
                branch: StorageBranch {
                    anchor: "aa".into(),
                    target: "bb".into(),
                },
            },
            verified_generation: Some(5),
            body_work_epoch: Some(6),
            session_id: 7,
            request_id: 8,
        };
        let body = || BlockSyncBody {
            height: 3,
            owner: owner(),
            bytes: 900,
        };
        let holder = |holder| BlockSyncFact::Holder {
            body: body(),
            holder,
            change: "retained".into(),
        };
        let events = [
            BlockSyncFact::SessionAdmitted {
                peer: "01".into(),
                connection: 4,
                session_id: 7,
                generation: 1,
                direction: "inbound".into(),
            },
            BlockSyncFact::FrameJudged {
                peer: "01".into(),
                connection: 4,
                generation: 1,
                seq: 2,
                message_type: 3,
                flags: 0,
                payload_len: 900,
            },
            BlockSyncFact::Request {
                owner: owner(),
                heights: vec![3, 4],
                estimated_bytes: 2_000,
                step: "dropped-queued".into(),
            },
            BlockSyncFact::Budget {
                change: "reserved".into(),
                before: 0,
                after: 2_000,
            },
            BlockSyncFact::HandoffStarted {
                peer: "01".into(),
                owner: owner(),
                height: 3,
                charge: 1_000,
            },
            BlockSyncFact::HandoffEnded {
                peer: "01".into(),
                owner: owner(),
                height: 3,
                charge: 1_000,
            },
            holder(BlockSyncHolder::Queued { allocation: 1 }),
            holder(BlockSyncHolder::Reorder { allocation: 1 }),
            holder(BlockSyncHolder::Applying { allocation: 1 }),
            holder(BlockSyncHolder::Submitted { token: 9 }),
            holder(BlockSyncHolder::Detached { token: 9 }),
            BlockSyncFact::ServingLease {
                peer: "01".into(),
                connection: 4,
                generation: 1,
                start: 3,
                count: 2,
                step: "acquired".into(),
            },
            BlockSyncFact::ServingAdmission {
                peer: "01".into(),
                start: 3,
                count: 2,
                live: vec![(1, 2)],
            },
            BlockSyncFact::ServingFrame {
                peer: "01".into(),
                connection: 4,
                generation: 1,
                start: 3,
                message_type: 4,
                body_bytes: 900,
            },
            BlockSyncFact::Routine {
                peer: "01".into(),
                connection: 4,
                generation: 1,
                requests: vec![BlockSyncRequest {
                    owner: owner(),
                    write: "written".into(),
                    heights: vec![BlockSyncRequestedHeight {
                        height: 3,
                        estimated_bytes: 1_000,
                        received: true,
                    }],
                }],
                claims: vec![BlockSyncClaim {
                    height: 3,
                    owner: owner(),
                    estimated_bytes: 1_000,
                }],
                serving: vec![BlockSyncRange {
                    first: 3,
                    last: 4,
                    completed: false,
                }],
            },
            BlockSyncFact::WorkQueue {
                items: vec![
                    BlockSyncWorkItem {
                        height: 3,
                        owner: Some(owner()),
                        place: "in-flight".into(),
                        estimated_bytes: 1_000,
                        reserved_bytes: 1_000,
                    },
                    BlockSyncWorkItem {
                        height: 5,
                        owner: None,
                        place: "pending".into(),
                        estimated_bytes: 1_000,
                        reserved_bytes: 0,
                    },
                ],
            },
            BlockSyncFact::Sequencer {
                reorder: vec![BlockSyncRetained {
                    body: body(),
                    admitted_estimate_bytes: Some(1_000),
                    allocation: 1,
                }],
                applying: vec![BlockSyncRetained {
                    body: body(),
                    admitted_estimate_bytes: None,
                    allocation: 2,
                }],
                submissions: vec![BlockSyncSubmission {
                    body: body(),
                    token: 9,
                    detached: false,
                }],
            },
            BlockSyncFact::SequencerClosed,
        ];
        let obs: Vec<Observation> = (1..)
            .zip(events)
            .map(|(seq, event)| Observation::BlockSync {
                instance: 0,
                seq,
                storage_seq: seq + 10,
                node: "node".into(),
                event,
            })
            .collect();
        let bump = "a block-sync observation's encoding changed, which changes `obs` digests, \
             so bump FORMAT_VERSION and update this test";
        assert_eq!(FORMAT_VERSION, 6, "{bump}");
        assert_eq!(
            serde_json::to_string(&obs).unwrap(),
            r#"[{"obs":"block-sync","instance":0,"seq":1,"storage_seq":11,"node":"node","event":{"event":"session-admitted","peer":"01","connection":4,"session_id":7,"generation":1,"direction":"inbound"}},{"obs":"block-sync","instance":0,"seq":2,"storage_seq":12,"node":"node","event":{"event":"frame-judged","peer":"01","connection":4,"generation":1,"seq":2,"message_type":3,"flags":0,"payload_len":900}},{"obs":"block-sync","instance":0,"seq":3,"storage_seq":13,"node":"node","event":{"event":"request","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"heights":[3,4],"estimated_bytes":2000,"step":"dropped-queued"}},{"obs":"block-sync","instance":0,"seq":4,"storage_seq":14,"node":"node","event":{"event":"budget","change":"reserved","before":0,"after":2000}},{"obs":"block-sync","instance":0,"seq":5,"storage_seq":15,"node":"node","event":{"event":"handoff-started","peer":"01","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"height":3,"charge":1000}},{"obs":"block-sync","instance":0,"seq":6,"storage_seq":16,"node":"node","event":{"event":"handoff-ended","peer":"01","owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"height":3,"charge":1000}},{"obs":"block-sync","instance":0,"seq":7,"storage_seq":17,"node":"node","event":{"event":"holder","body":{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900},"holder":{"kind":"queued","allocation":1},"change":"retained"}},{"obs":"block-sync","instance":0,"seq":8,"storage_seq":18,"node":"node","event":{"event":"holder","body":{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900},"holder":{"kind":"reorder","allocation":1},"change":"retained"}},{"obs":"block-sync","instance":0,"seq":9,"storage_seq":19,"node":"node","event":{"event":"holder","body":{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900},"holder":{"kind":"applying","allocation":1},"change":"retained"}},{"obs":"block-sync","instance":0,"seq":10,"storage_seq":20,"node":"node","event":{"event":"holder","body":{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900},"holder":{"kind":"submitted","token":9},"change":"retained"}},{"obs":"block-sync","instance":0,"seq":11,"storage_seq":21,"node":"node","event":{"event":"holder","body":{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900},"holder":{"kind":"detached","token":9},"change":"retained"}},{"obs":"block-sync","instance":0,"seq":12,"storage_seq":22,"node":"node","event":{"event":"serving-lease","peer":"01","connection":4,"generation":1,"start":3,"count":2,"step":"acquired"}},{"obs":"block-sync","instance":0,"seq":13,"storage_seq":23,"node":"node","event":{"event":"serving-admission","peer":"01","start":3,"count":2,"live":[[1,2]]}},{"obs":"block-sync","instance":0,"seq":14,"storage_seq":24,"node":"node","event":{"event":"serving-frame","peer":"01","connection":4,"generation":1,"start":3,"message_type":4,"body_bytes":900}},{"obs":"block-sync","instance":0,"seq":15,"storage_seq":25,"node":"node","event":{"event":"routine","peer":"01","connection":4,"generation":1,"requests":[{"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"write":"written","heights":[{"height":3,"estimated_bytes":1000,"received":true}]}],"claims":[{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"estimated_bytes":1000}],"serving":[{"first":3,"last":4,"completed":false}]}},{"obs":"block-sync","instance":0,"seq":16,"storage_seq":26,"node":"node","event":{"event":"work-queue","items":[{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"place":"in-flight","estimated_bytes":1000,"reserved_bytes":1000},{"height":5,"place":"pending","estimated_bytes":1000,"reserved_bytes":0}]}},{"obs":"block-sync","instance":0,"seq":17,"storage_seq":27,"node":"node","event":{"event":"sequencer","reorder":[{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900,"admitted_estimate_bytes":1000,"allocation":1}],"applying":[{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900,"allocation":2}],"submissions":[{"height":3,"owner":{"kind":"body","header_generation":2,"branch":{"anchor":"aa","target":"bb"},"verified_generation":5,"body_work_epoch":6,"session_id":7,"request_id":8},"bytes":900,"token":9,"detached":false}]}},{"obs":"block-sync","instance":0,"seq":18,"storage_seq":28,"node":"node","event":{"event":"sequencer-closed"}}]"#,
            "{bump}"
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
