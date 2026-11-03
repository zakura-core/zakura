//! Cases that zakurad's real block-sync driver and [`ControlledBlockService`]
//! must both pass (storage.md ST4.6).
//!
//! Each [`Case`] is a fixture chain and a script of block-sync actions and
//! verifier verdicts. After each step, [`Seen`] lists what block sync must
//! see: the submissions the driver sends to the verifier, the attempts each
//! record makes, and the events at the handle. [`run_controlled`] runs a
//! case on the controlled service. zakurad's `block_sync_driver/fidelity.rs`
//! runs it on `drive_block_sync_actions` and checks that both runs see the
//! same events. A case with no verdict runs there over a real ephemeral
//! zakura-state holding the case's headers. A case with verdicts commits
//! fixture bodies, whose v1 coinbase and missing chain history commitment
//! zakura-state refuses, so it runs over state fakes on the case's engine. zakurad's
//! `block_sync_driver/committed_state.rs` covers what those fakes share with
//! the service: it commits [`BlockFixture::committable`] blocks to real
//! zakura-state and to the service, and compares fork selection, queries,
//! range reads and views.
//!
//! The controlled run takes the driver's choices. It sends each step's
//! actions to the driver's channel, a queue here, and runs the driver's
//! steps on the service: the driver waits on its channel between steps, so
//! it receives a step's first action ([`ControlledBlockService::take_received`])
//! and then takes the rest at the top of its loop
//! ([`ControlledBlockService::take_ready`]). The run completes each query
//! or record as soon as the driver takes it, and admits every queued
//! submission the limits allow before the driver takes its next action. A
//! verdict step completes the oldest admitted submission of its block.
//!
//! The driver run compares every event in full, so the verdicts'
//! `BlockApplyOutcome` evidence identities must match the driver's too.
//!
//! # Deliberate differences
//!
//! The cases leave out what the contract in [`super`] excludes, and a few
//! paths the driver's fakes can't reach:
//!
//! | Driver behavior | Why no case covers it |
//! |:--|:--|
//! | Legacy handoff and abandoned applies | A node that runs only the Zakura stack never yields after bootstrap |
//! | Fail-closed shutdown after a `record_body_invalid` exhausts its retries | Shutdown ends the run |
//! | A `record_body_invalid` that goes stale again after its refresh | Another state change must land between the two attempts. A completion runs atomically here, so the refresh always records |
//! | The 30 s deadlines on state and verifier calls | The run picks when each operation completes |
//! | The throughput probe | It's a debug mode |
//! | A query reply that finds the handle closed ends the driver | The detached handle can't close. `block_controlled`'s tests cover the service's exit |
//! | A misbehavior report waits for the supervisor | The simulator doesn't run the supervisor |
//! | A checkpoint verifier error | The verifier fake answers as the full verifier does, so checkpoint-class bodies get only full-verifier verdicts |
//! | The verifier returning another block's hash | The fake returns the submitted hash. The driver reports another hash as a `VerifierUnavailable` retry, the same event as [`Verdict::Retryable`] with that kind |
//! | zakura-state's own `MissingBlockBodyMetadata` read and record writes, in a case with verdicts | The verdict-free cases run both on a real zakura-state. A case with verdicts would need the fixture bodies committed there, and zakura-state refuses their v1 coinbase and missing chain history commitment, so the fakes answer from the engine port's ports of them. `committed_state.rs` compares the reads after real commits |
//!
//! A moved anchor fails a query in the driver when its chunks read
//! different states. The controlled service reads every chunk from one
//! state, so its run fails that query with [`BlockOutcome::Fail`] instead.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU64,
    sync::Arc,
};

use futures::FutureExt;
use zakura_chain::block;
use zakura_header_chain::{
    BodyRuleId, BodySupplierDiscovered, BodyUnavailableSummary, BodyWorkAuthority,
    CommittedHeaderChainView, ConsensusBodyInvalid, EvidenceId, OperatorBodyRetry, SourceId,
    StateVersion, TransientBodyFailure, TransientBodyFailureKind,
};
use zakura_network::zakura::{BlockApplyResult, BlockApplyToken, BlockSyncAction};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};

use super::{
    controlled::{ControlledBlockService, OpId, ReplyLog, Taken},
    evidence::{RecordResult, RequestIdentity, ResultIdentity},
    fixture::{BlockFixture, BodySpec, FixtureBlock},
    ApplyLimits, BlockOp, BlockOutcome, Commitment, HandleEvent, RetryKind, Verdict,
};
use crate::header::adapter::EnginePort;

/// How many fixture headers a case inserts per header target, within the
/// engine's per-transition limit.
const HEADER_BATCH: usize = 2_000;

/// The supplier every submission names.
pub const SOURCE: SourceId = SourceId::from_digest([7; 32]);

/// One step of a case.
#[derive(Clone, Debug)]
pub enum Step {
    /// Block sync queries `limit` heights from `from`.
    Query {
        /// The first height.
        from: u32,
        /// The number of heights.
        limit: u32,
    },
    /// As [`Step::Query`], but state moves its anchor between the query's
    /// chunks, so `limit` must be over 4,000.
    QueryAnchorMoves {
        /// The first height.
        from: u32,
        /// The number of heights.
        limit: u32,
    },
    /// Block sync submits the chain's block at this index.
    Submit(usize),
    /// Block sync records the block at this index invalid, at `version`.
    RecordInvalid {
        /// The block's index.
        block: usize,
        /// The version block sync last saw.
        version: u64,
    },
    /// Block sync records an availability episode of the block at this
    /// index.
    Availability {
        /// Which record.
        record: AvailabilityRecord,
        /// The block's index.
        block: usize,
        /// The episode the record carries.
        episode: Episode,
        /// The version block sync last saw, or `None` for the latest view's.
        version: Option<u64>,
    },
    /// Every action in these steps arrives before the driver takes any.
    Burst(Vec<Step>),
    /// The verifier finishes the oldest admitted submission of the block
    /// at this index.
    Verdict(usize, Verdict),
}

/// The availability records block sync sends.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AvailabilityRecord {
    /// `record_body_unavailable`, after failed deliveries.
    Unavailable,
    /// `restart_body_availability`, when an alarmed episode gains a
    /// supplier.
    Restart,
    /// `retry_body_availability`, an operator's fresh episode.
    Retry,
}

/// A retry episode's summary. Every episode starts at the genesis time and
/// is due for its next probe then, so a probe is never early.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Episode {
    /// The failed attempts.
    pub attempts: u32,
    /// The eligible suppliers, the first `suppliers` of a fixed sequence.
    pub suppliers: u32,
    /// Whether the alarm fired.
    pub alarmed: bool,
}

impl Episode {
    /// The summary a record carries.
    pub fn summary(self) -> BodyUnavailableSummary {
        let start = BlockFixture::regtest().headers().genesis_header().time;
        let suppliers: BTreeSet<SourceId> = (1..=self.suppliers)
            .map(|n| {
                let mut digest = [0; 32];
                digest[..4].copy_from_slice(&n.to_le_bytes());
                SourceId::from_digest(digest)
            })
            .collect();
        BodyUnavailableSummary {
            started_at: start,
            attempts: self.attempts,
            suppliers: self.suppliers,
            supplier_set_digest: BodyUnavailableSummary::supplier_set_digest(&suppliers),
            alarmed: self.alarmed,
            next_probe_at: start,
        }
    }
}

/// An event at the handle, by its query or token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expect {
    /// A query's answer: the anchor's height and the missing heights.
    Needed {
        /// The query.
        query: u64,
        /// The body anchor's height.
        anchor: u32,
        /// The missing bodies' heights.
        heights: Vec<u32>,
    },
    /// A query's failure.
    Failed {
        /// The query.
        query: u64,
    },
    /// A submission's verdict.
    Finished {
        /// The submission's token.
        token: BlockApplyToken,
        /// The verdict's result.
        result: BlockApplyResult,
    },
}

/// What block sync sees during one step.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Seen {
    /// The submissions sent to the verifier, by token.
    pub admitted: Vec<BlockApplyToken>,
    /// Each record attempt's result, in order.
    pub records: Vec<RecordResult>,
    /// The handle's control events in order, then its query failures.
    pub events: Vec<Expect>,
}

impl Seen {
    /// Nothing.
    pub fn nothing() -> Self {
        Seen::default()
    }

    /// `events` only.
    pub fn events(events: impl IntoIterator<Item = Expect>) -> Self {
        Seen {
            events: events.into_iter().collect(),
            ..Seen::default()
        }
    }

    /// `tokens` admitted, and nothing else.
    pub fn admitted(tokens: impl IntoIterator<Item = BlockApplyToken>) -> Self {
        Seen {
            admitted: tokens.into_iter().collect(),
            ..Seen::default()
        }
    }

    /// Also `tokens` admitted.
    pub fn and_admitted(mut self, tokens: impl IntoIterator<Item = BlockApplyToken>) -> Self {
        self.admitted.extend(tokens);
        self
    }
}

/// What one run saw during one step, in full.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Observed {
    /// The submissions sent to the verifier, by token.
    pub admitted: Vec<BlockApplyToken>,
    /// Each record attempt's result, in order.
    pub records: Vec<RecordResult>,
    /// The handle's control events in order, then its query failures in
    /// order. The two travel on separate channels, so their relative order
    /// carries nothing.
    pub events: Vec<HandleEvent>,
}

impl Observed {
    /// Adds `events`, keeping control events before query failures.
    pub fn push_events(&mut self, events: impl IntoIterator<Item = HandleEvent>) {
        self.events.extend(events);
        // A stable sort keeps each channel's order.
        self.events
            .sort_by_key(|event| matches!(event, HandleEvent::NeededBlocksFailed { .. }));
    }

    /// The summary a case's [`Seen`] states.
    pub fn summary(&self) -> Seen {
        Seen {
            admitted: self.admitted.clone(),
            records: self.records.clone(),
            events: self
                .events
                .iter()
                .map(|event| match event {
                    HandleEvent::NeededBlocks {
                        query_id,
                        body_anchor,
                        blocks,
                        ..
                    } => Expect::Needed {
                        query: query_id.get(),
                        anchor: body_anchor.height.0,
                        heights: blocks.iter().map(|meta| meta.height.0).collect(),
                    },
                    HandleEvent::NeededBlocksFailed { query_id, .. } => Expect::Failed {
                        query: query_id.get(),
                    },
                    HandleEvent::ApplyFinished { token, outcome, .. } => Expect::Finished {
                        token: *token,
                        result: outcome.result(),
                    },
                })
                .collect(),
        }
    }
}

/// The apply limits a case configures, before the driver clamps them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Configured {
    /// The checkpoint limit.
    pub checkpoint: usize,
    /// The full limit.
    pub full: usize,
    /// The combined limit.
    pub combined: usize,
    /// The last checkpoint height.
    pub max_checkpoint_height: u32,
}

/// One shared case.
#[derive(Clone, Debug)]
pub struct Case {
    /// Its name.
    pub name: &'static str,
    /// The chain's bodies, one branch from genesis. The body at index `i`
    /// is at height `i + 1`.
    pub bodies: Vec<BodySpec>,
    /// The apply limits.
    pub limits: Configured,
    /// The steps, each with what block sync sees during it.
    pub steps: Vec<(Step, Seen)>,
}

impl Case {
    /// The limits after the driver clamps them, as `drive_block_sync_actions`
    /// does.
    pub fn apply_limits(&self) -> ApplyLimits {
        ApplyLimits::clamped(
            self.limits.checkpoint,
            self.limits.full,
            self.limits.combined,
            block::Height(self.limits.max_checkpoint_height),
        )
    }
}

/// A case's chain and engine, and the actions its steps send.
pub struct Script {
    /// The engine, with the chain's headers inserted.
    pub engine: Arc<EnginePort>,
    /// The chain.
    pub blocks: Vec<FixtureBlock>,
    /// Each submission's block index, by token.
    pub submissions: BTreeMap<BlockApplyToken, usize>,
    next_query: u64,
}

impl Script {
    /// An engine over `case`'s chain.
    pub fn new(case: &Case) -> Self {
        let fixture = BlockFixture::regtest();
        let engine = Arc::new(EnginePort::new(fixture.headers()));
        let blocks = fixture.extend(fixture.anchor(), &case.bodies, 0);
        insert_headers(&*engine, &blocks)
            .now_or_never()
            .expect("the engine port completes each operation on its first poll");
        Script {
            engine,
            blocks,
            submissions: BTreeMap::new(),
            next_query: 0,
        }
    }

    /// The actions `step` sends, in order, with the scope of the engine's
    /// latest view.
    ///
    /// # Panics
    ///
    /// If `step` is a verdict, which sends nothing.
    pub fn actions(&mut self, step: &Step) -> Vec<BlockSyncAction> {
        let view = self.engine.view();
        self.actions_in(step, &view)
    }

    /// The actions `step` sends, in order, with the scope and best header
    /// tip of `view`, the latest view of the state the driver runs on.
    ///
    /// # Panics
    ///
    /// If `step` is a verdict, which sends nothing.
    pub fn actions_in(
        &mut self,
        step: &Step,
        view: &CommittedHeaderChainView,
    ) -> Vec<BlockSyncAction> {
        let scope = BodyWorkAuthority::for_view(view);
        match step {
            Step::Query { from, limit } | Step::QueryAnchorMoves { from, limit } => {
                self.next_query += 1;
                vec![BlockSyncAction::QueryNeededBlocks {
                    query_id: NonZeroU64::new(self.next_query).expect("counts from 1"),
                    from: block::Height(*from),
                    limit: *limit,
                    best_header_tip: view.snapshot.frontiers.header_best.height,
                    scope,
                }]
            }
            Step::Submit(index) => {
                let token = self.submissions.len() as u64 + 1;
                self.submissions.insert(token, *index);
                vec![BlockSyncAction::SubmitBlock {
                    owner: scope.bind(1, NonZeroU64::MIN),
                    source: SOURCE,
                    token,
                    block: self.blocks[*index].block.clone(),
                }]
            }
            Step::RecordInvalid { block, version } => vec![BlockSyncAction::RecordBodyInvalid {
                expected_version: StateVersion::new(*version),
                invalid: ConsensusBodyInvalid {
                    hash: self.blocks[*block].hash(),
                    // Each block's evidence is its own.
                    evidence: EvidenceId::from_digest(self.blocks[*block].hash().0),
                    rule: BodyRuleId::new("block.subsidy"),
                    source: SOURCE,
                },
            }],
            Step::Availability {
                record,
                block,
                episode,
                version,
            } => {
                let expected_version =
                    version.map_or(view.snapshot.state_version, StateVersion::new);
                let hash = self.blocks[*block].hash();
                // A record's evidence is its block, kind and episode, so only
                // a repeat of the same record shares it.
                let mut digest = hash.0;
                digest[0] ^= match record {
                    AvailabilityRecord::Unavailable => 1,
                    AvailabilityRecord::Restart => 2,
                    AvailabilityRecord::Retry => 3,
                };
                digest[1..5].copy_from_slice(&episode.attempts.to_le_bytes());
                digest[5..9].copy_from_slice(&episode.suppliers.to_le_bytes());
                digest[9] = u8::from(episode.alarmed);
                let evidence = EvidenceId::from_digest(digest);
                let availability = episode.summary();
                vec![match record {
                    AvailabilityRecord::Unavailable => BlockSyncAction::RecordBodyUnavailable {
                        expected_version,
                        failure: TransientBodyFailure {
                            hash,
                            evidence,
                            kind: TransientBodyFailureKind::Timeout,
                            availability,
                        },
                    },
                    AvailabilityRecord::Restart => BlockSyncAction::RestartBodyAvailability {
                        expected_version,
                        discovery: BodySupplierDiscovered {
                            hash,
                            evidence,
                            availability,
                        },
                    },
                    AvailabilityRecord::Retry => BlockSyncAction::RetryBodyAvailability {
                        expected_version,
                        retry: OperatorBodyRetry {
                            hash,
                            evidence,
                            availability,
                        },
                    },
                }]
            }
            Step::Burst(steps) => steps
                .iter()
                .flat_map(|step| self.actions_in(step, view))
                .collect(),
            Step::Verdict(..) => panic!("a verdict sends no action"),
        }
    }
}

/// Inserts the headers of `blocks`, a chain on the fixture's anchor, into
/// `port`, in batches within the engine's per-transition limit.
///
/// # Panics
///
/// If `port` refuses a batch.
pub async fn insert_headers(port: &dyn Port, blocks: &[FixtureBlock]) {
    let fixture = BlockFixture::regtest();
    let mut parent = fixture.anchor();
    for (request_id, batch) in (1..).zip(blocks.chunks(HEADER_BATCH)) {
        let snapshot = port
            .selected_chain()
            .borrow()
            .clone()
            .expect("an attached port has a snapshot");
        let request = fixture.target_request(&snapshot, parent, batch, request_id);
        let prepared = port
            .prepare_header_target(request)
            .await
            .expect("fixture headers prepare");
        let applied = port
            .apply_header_target(prepared)
            .await
            .expect("fixture headers apply");
        assert_eq!(applied, ApplyHeaderTargetOutcome::Applied);
        parent = batch.last().expect("a chunk holds a block").frontier();
    }
}

/// Runs `case` on a [`ControlledBlockService`], taking the driver's choices,
/// and returns what block sync saw during each step.
///
/// # Panics
///
/// If the service refuses a choice the case makes, or the driver ends a
/// step with an action it hasn't taken.
pub fn run_controlled(case: &Case) -> Vec<Observed> {
    let mut script = Script::new(case);
    let replies = Arc::new(ReplyLog::default());
    let service =
        ControlledBlockService::new(script.engine.clone(), replies.clone(), case.apply_limits());
    let mut run = ControlledRun {
        service: &service,
        channel: VecDeque::new(),
        failing: BTreeSet::new(),
        admitted: Vec::new(),
    };
    let mut observed = Vec::new();
    for (step, _) in &case.steps {
        let mut seen = Observed::default();
        if let Step::Verdict(index, verdict) = step {
            let position = run
                .admitted
                .iter()
                .position(|(_, block)| block == index)
                .unwrap_or_else(|| panic!("{}: block {index} isn't admitted", case.name));
            let (id, _) = run.admitted.remove(position);
            service
                .complete(id, BlockOutcome::Verdict(*verdict))
                .unwrap_or_else(|error| panic!("{}: {verdict:?}: {error}", case.name));
        } else {
            let actions = script.actions(step);
            if matches!(step, Step::QueryAnchorMoves { .. }) {
                run.failing
                    .extend(actions.iter().filter_map(|action| match action {
                        BlockSyncAction::QueryNeededBlocks { query_id, .. } => Some(*query_id),
                        _ => None,
                    }));
            }
            run.channel.extend(actions);
        }
        run.settle(&script.submissions, &mut seen);
        seen.push_events(replies.take());
        observed.push(seen);
    }
    observed
}

/// The driver's side of a controlled run.
struct ControlledRun<'a> {
    service: &'a ControlledBlockService,
    /// The driver's action channel.
    channel: VecDeque<BlockSyncAction>,
    /// The queries whose state reads see a moved anchor.
    failing: BTreeSet<NonZeroU64>,
    /// Each admitted submission, with its block's index, oldest first.
    admitted: Vec<(OpId, usize)>,
}

impl ControlledRun<'_> {
    /// Runs the driver until it waits on its channel again: it receives the
    /// step's first action, if any, then completes each query or record it
    /// takes and admits what the limits allow before it takes the next.
    /// `submissions` maps each token to its block's index.
    fn settle(&mut self, submissions: &BTreeMap<BlockApplyToken, usize>, seen: &mut Observed) {
        let mut taken = match self.channel.pop_front() {
            Some(first) => self.service.take_received(first, &mut self.channel),
            None => Taken::Idle,
        };
        loop {
            if let Taken::Op(id) = taken {
                self.finish_awaited(id, seen);
            }
            self.admit(submissions, seen);
            taken = self.service.take_ready(&mut self.channel);
            match taken {
                Taken::Op(_) => {}
                Taken::Idle => break,
                Taken::Busy | Taken::Stopped => {
                    unreachable!("the run completes each query and record it takes")
                }
            }
        }
        assert!(self.channel.is_empty(), "the driver takes every action");
    }

    /// Completes `id` at once if the driver awaits it: a query, answered or
    /// failed if its anchor moves, or a record.
    fn finish_awaited(&mut self, id: OpId, seen: &mut Observed) {
        let Some(pending) = self.service.pending_op(id) else {
            return;
        };
        let outcome = match pending.op {
            BlockOp::QueryNeededBlocks => match self.request(id) {
                RequestIdentity::Query { query_id, .. } if self.failing.contains(&query_id) => {
                    BlockOutcome::Fail
                }
                _ => BlockOutcome::Answer,
            },
            op if op.awaited() => BlockOutcome::Apply,
            _ => return,
        };
        let completion = self
            .service
            .complete(id, outcome)
            .expect("the driver's awaited operation completes");
        if let ResultIdentity::Recorded(attempts) = completion.result {
            seen.records.extend(attempts);
        }
    }

    /// Admits every queued submission the limits allow, oldest first.
    fn admit(&mut self, submissions: &BTreeMap<BlockApplyToken, usize>, seen: &mut Observed) {
        while let Some(admit) = self
            .service
            .pending()
            .into_iter()
            .find(|op| op.enabled.contains(&BlockOutcome::Admit))
        {
            let completion = self
                .service
                .complete(admit.id, BlockOutcome::Admit)
                .expect("an enabled admission completes");
            let RequestIdentity::Submit { token, .. } = completion.request else {
                unreachable!("only a submission is admitted");
            };
            seen.admitted.push(token);
            self.admitted.push((admit.id, submissions[&token]));
        }
    }

    /// What held operation `id` asks for.
    fn request(&self, id: OpId) -> RequestIdentity {
        self.service
            .owner_state()
            .operations
            .into_iter()
            .find(|op| op.id == id)
            .map(|op| op.request)
            .expect("the operation is held")
    }
}

/// A body of 900 bytes with an exact hint.
const BODY: BodySpec = BodySpec::exact(900);

/// Limits that admit one full body at a time.
const ONE_FULL: Configured = Configured {
    checkpoint: 1,
    full: 1,
    combined: 3,
    max_checkpoint_height: 0,
};

/// The query answer for `query` at anchor height `anchor`.
fn needed(query: u64, anchor: u32, heights: &[u32]) -> Expect {
    Expect::Needed {
        query,
        anchor,
        heights: heights.to_vec(),
    }
}

/// A record of `episode` for the block at `block`.
fn availability(
    record: AvailabilityRecord,
    block: usize,
    episode: Episode,
    version: Option<u64>,
) -> Step {
    Step::Availability {
        record,
        block,
        episode,
        version,
    }
}

/// An episode of `attempts` failures among `suppliers`.
fn episode(attempts: u32, suppliers: u32, alarmed: bool) -> Episode {
    Episode {
        attempts,
        suppliers,
        alarmed,
    }
}

/// One record attempt with `result`, and nothing else.
fn recorded(result: RecordResult) -> Seen {
    Seen {
        records: vec![result],
        ..Seen::default()
    }
}

/// The verdict event for `token`.
fn finished(token: BlockApplyToken, result: BlockApplyResult) -> Expect {
    Expect::Finished { token, result }
}

/// The shared cases.
pub fn cases() -> Vec<Case> {
    use BlockApplyResult::{Committed, Duplicate, Rejected, Unavailable};

    vec![
        Case {
            name: "queries answer from full state",
            bodies: vec![BODY, BodySpec { size: 900, hint: 0 }, BODY],
            limits: ONE_FULL,
            steps: vec![
                (
                    Step::Query { from: 1, limit: 16 },
                    Seen::events([needed(1, 0, &[1, 2, 3])]),
                ),
                (Step::Submit(0), Seen::admitted([1])),
                (
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]),
                ),
                (
                    Step::Query { from: 1, limit: 16 },
                    Seen::events([needed(2, 1, &[2, 3])]),
                ),
                (
                    Step::Query { from: 1, limit: 0 },
                    Seen::events([Expect::Failed { query: 3 }]),
                ),
            ],
        },
        Case {
            name: "queries chunk, and fail when the anchor moves",
            bodies: vec![BODY; 3],
            limits: ONE_FULL,
            steps: vec![
                (
                    Step::Query {
                        from: 1,
                        limit: 4_001,
                    },
                    Seen::events([needed(1, 0, &[1, 2, 3])]),
                ),
                (
                    Step::QueryAnchorMoves {
                        from: 1,
                        limit: 4_001,
                    },
                    Seen::events([Expect::Failed { query: 2 }]),
                ),
            ],
        },
        Case {
            name: "a query's second chunk lists its bodies",
            bodies: vec![BodySpec::exact(300); 4_002],
            limits: ONE_FULL,
            steps: vec![(
                Step::Query {
                    from: 1,
                    limit: 4_002,
                },
                Seen::events([Expect::Needed {
                    query: 1,
                    anchor: 0,
                    heights: (1..=4_002).collect(),
                }]),
            )],
        },
        Case {
            name: "the latest query coalesces the older ones",
            bodies: vec![BODY; 2],
            limits: ONE_FULL,
            steps: vec![
                (
                    Step::Burst(vec![
                        Step::Query { from: 1, limit: 16 },
                        Step::Query { from: 1, limit: 16 },
                        Step::Query { from: 2, limit: 16 },
                    ]),
                    Seen::events([needed(3, 0, &[2])]),
                ),
                (
                    Step::Burst(vec![
                        Step::Query { from: 1, limit: 16 },
                        Step::Submit(0),
                        Step::Query { from: 1, limit: 16 },
                    ]),
                    Seen::events([needed(5, 0, &[1, 2])]).and_admitted([1]),
                ),
            ],
        },
        Case {
            name: "full admission follows the limits",
            bodies: vec![BODY; 4],
            limits: ONE_FULL,
            steps: vec![
                (
                    Step::Burst((0..4).map(Step::Submit).collect()),
                    Seen::admitted([1]),
                ),
                (
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]).and_admitted([2]),
                ),
                (
                    Step::Verdict(1, Verdict::Retryable(RetryKind::Storage)),
                    Seen::events([finished(2, Unavailable)]).and_admitted([3]),
                ),
            ],
        },
        Case {
            name: "checkpoint admission has its own room",
            bodies: vec![BODY; 4],
            limits: Configured {
                max_checkpoint_height: 2,
                ..ONE_FULL
            },
            steps: vec![
                (
                    Step::Burst((0..4).map(Step::Submit).collect()),
                    Seen::admitted([1, 2, 3]),
                ),
                (
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]),
                ),
                (
                    Step::Verdict(2, Verdict::Retryable(RetryKind::MissingContext)),
                    Seen::events([finished(3, Unavailable)]).and_admitted([4]),
                ),
            ],
        },
        Case {
            name: "the combined limit binds, and admission skips a blocked class",
            bodies: vec![BODY; 4],
            // The driver raises the checkpoint limit to the checkpoint gap,
            // so only the full and combined limits bind.
            limits: Configured {
                checkpoint: 1,
                full: 1,
                combined: 2,
                max_checkpoint_height: 2,
            },
            steps: vec![
                (
                    Step::Burst(vec![Step::Submit(0), Step::Submit(1)]),
                    Seen::admitted([1, 2]),
                ),
                // Two checkpoint applies fill the combined limit, so the
                // full bodies wait, and a checkpoint copy of block 0 goes
                // ahead of them.
                (
                    Step::Burst(vec![Step::Submit(2), Step::Submit(3), Step::Submit(0)]),
                    Seen::admitted([5]),
                ),
                (
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]),
                ),
                (
                    Step::Verdict(1, Verdict::Committed),
                    Seen::events([finished(2, Committed)]).and_admitted([3]),
                ),
                // The full limit holds block 3 until block 2 finishes.
                (
                    Step::Verdict(2, Verdict::Committed),
                    Seen::events([finished(3, Committed)]).and_admitted([4]),
                ),
            ],
        },
        Case {
            name: "checkpoint admission stops at two ranges, and a full body goes ahead",
            bodies: vec![BodySpec::exact(300); 802],
            // Heights 1 to 801 are checkpoint-class. The driver raises the
            // checkpoint limit to the checkpoint gap, so checkpoint applies
            // have room for two ranges, 800 bodies.
            limits: Configured {
                checkpoint: 1,
                full: 1,
                combined: 1_000,
                max_checkpoint_height: 801,
            },
            steps: {
                // The action channel holds 64 actions.
                let mut steps: Vec<_> = (0..801)
                    .collect::<Vec<usize>>()
                    .chunks(64)
                    .map(|indices| {
                        let admitted = indices
                            .iter()
                            .map(|index| *index as u64 + 1)
                            .filter(|token| *token <= 800);
                        (
                            Step::Burst(indices.iter().copied().map(Step::Submit).collect()),
                            Seen::admitted(admitted),
                        )
                    })
                    .collect();
                // Block 800 waits for checkpoint room, so the full body at
                // height 802 goes ahead of it.
                steps.push((Step::Submit(801), Seen::admitted([802])));
                steps.push((
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]).and_admitted([801]),
                ));
                steps
            },
        },
        Case {
            name: "each verifier class",
            bodies: vec![BODY; 2],
            limits: ONE_FULL,
            steps: {
                let mut steps = vec![
                    (Step::Submit(0), Seen::admitted([1])),
                    (
                        Step::Verdict(0, Verdict::Ambiguous),
                        Seen::events([finished(1, Unavailable)]),
                    ),
                    (Step::Submit(0), Seen::admitted([2])),
                    (
                        Step::Verdict(0, Verdict::Duplicate),
                        Seen::events([finished(2, Duplicate)]),
                    ),
                ];
                let mismatches = Commitment::ALL
                    .map(|commitment| (Verdict::PayloadMismatch(commitment), Rejected));
                let verdicts = [
                    (Verdict::ConsensusInvalid, Rejected),
                    (Verdict::Retryable(RetryKind::MissingContext), Unavailable),
                    (Verdict::Retryable(RetryKind::Canceled), Unavailable),
                    (Verdict::Retryable(RetryKind::Storage), Unavailable),
                    (
                        Verdict::Retryable(RetryKind::VerifierUnavailable),
                        Unavailable,
                    ),
                ];
                let verdicts = mismatches
                    .into_iter()
                    .chain(verdicts)
                    .chain([(Verdict::Committed, Committed)]);
                for (token, (verdict, result)) in (3..).zip(verdicts) {
                    steps.push((Step::Submit(1), Seen::admitted([token])));
                    steps.push((
                        Step::Verdict(1, verdict),
                        Seen::events([finished(token, result)]),
                    ));
                }
                steps
            },
        },
        Case {
            name: "a copy admitted before another commits can fail consensus",
            bodies: vec![BODY],
            limits: Configured {
                full: 2,
                ..ONE_FULL
            },
            steps: vec![
                (
                    Step::Burst(vec![Step::Submit(0), Step::Submit(0)]),
                    Seen::admitted([1, 2]),
                ),
                (
                    Step::Verdict(0, Verdict::Committed),
                    Seen::events([finished(1, Committed)]),
                ),
                // The second copy passed the verifier's duplicate check before
                // the first committed.
                (
                    Step::Verdict(0, Verdict::ConsensusInvalid),
                    Seen::events([finished(2, Rejected)]),
                ),
                // A copy admitted after the commit is a duplicate.
                (Step::Submit(0), Seen::admitted([3])),
                (
                    Step::Verdict(0, Verdict::Duplicate),
                    Seen::events([finished(3, Duplicate)]),
                ),
            ],
        },
        Case {
            name: "a driver blocked in a record takes the next record before the coalesced query",
            bodies: vec![BODY; 3],
            limits: ONE_FULL,
            steps: vec![(
                // The driver receives the first record and awaits it while
                // the rest arrive. It then drains its channel: the second
                // query replaces the first, and the second record goes
                // ahead of it.
                Step::Burst(vec![
                    Step::RecordInvalid {
                        block: 2,
                        version: 1,
                    },
                    Step::Query { from: 1, limit: 16 },
                    Step::RecordInvalid {
                        block: 1,
                        version: 1,
                    },
                    Step::Query { from: 1, limit: 16 },
                ]),
                Seen {
                    records: vec![
                        RecordResult::Stale,
                        RecordResult::Committed,
                        RecordResult::Stale,
                        RecordResult::Committed,
                    ],
                    events: vec![needed(2, 0, &[1])],
                    ..Seen::default()
                },
            )],
        },
        Case {
            name: "a stale invalid-body record retries",
            bodies: vec![BODY; 3],
            limits: ONE_FULL,
            steps: vec![
                (
                    Step::RecordInvalid {
                        block: 2,
                        version: 1,
                    },
                    Seen {
                        records: vec![RecordResult::Stale, RecordResult::Committed],
                        ..Seen::default()
                    },
                ),
                (
                    Step::Query { from: 1, limit: 16 },
                    Seen::events([needed(1, 0, &[1, 2])]),
                ),
                (
                    Step::Burst(vec![
                        Step::Query { from: 1, limit: 16 },
                        Step::RecordInvalid {
                            block: 1,
                            version: 1,
                        },
                        Step::Query { from: 1, limit: 16 },
                    ]),
                    Seen {
                        records: vec![RecordResult::Stale, RecordResult::Committed],
                        // The latest query goes ahead of the record.
                        events: vec![needed(3, 0, &[1, 2])],
                        ..Seen::default()
                    },
                ),
                (
                    Step::Burst(vec![
                        Step::RecordInvalid {
                            block: 0,
                            version: 1,
                        },
                        Step::Query { from: 1, limit: 16 },
                    ]),
                    Seen {
                        records: vec![RecordResult::Stale, RecordResult::Committed],
                        events: vec![needed(4, 0, &[])],
                        ..Seen::default()
                    },
                ),
            ],
        },
        Case {
            name: "an alarmed episode restarts with a new supplier, and an operator retries it",
            bodies: vec![BODY; 3],
            limits: ONE_FULL,
            steps: {
                use AvailabilityRecord::{Restart, Retry, Unavailable};
                let tip = 2;
                vec![
                    (
                        availability(Unavailable, tip, episode(3, 1, true), None),
                        recorded(RecordResult::Committed),
                    ),
                    // Only the header-best tip's alarm restarts.
                    (
                        availability(Restart, 1, episode(3, 2, true), None),
                        recorded(RecordResult::Refused),
                    ),
                    (
                        availability(Restart, tip, episode(3, 2, true), None),
                        recorded(RecordResult::Committed),
                    ),
                    // The same record again changes nothing.
                    (
                        availability(Restart, tip, episode(3, 2, true), None),
                        recorded(RecordResult::NoChange),
                    ),
                    // Fewer suppliers bring none new.
                    (
                        availability(Restart, tip, episode(3, 1, true), None),
                        recorded(RecordResult::Refused),
                    ),
                    // Only an invalid-body record refreshes a stale version.
                    (
                        availability(Unavailable, tip, episode(4, 2, true), Some(1)),
                        recorded(RecordResult::Stale),
                    ),
                    (
                        availability(Retry, tip, episode(0, 2, false), None),
                        recorded(RecordResult::Committed),
                    ),
                    // The fresh episode has no alarm to retry or restart.
                    (
                        availability(Retry, tip, episode(0, 3, false), None),
                        recorded(RecordResult::Refused),
                    ),
                    (
                        availability(Restart, tip, episode(0, 3, true), None),
                        recorded(RecordResult::Refused),
                    ),
                    (
                        Step::Query { from: 1, limit: 16 },
                        Seen::events([needed(1, 0, &[1, 2, 3])]),
                    ),
                ]
            },
        },
    ]
}
