//! Cases that zakurad's real block-sync driver and [`ControlledBlockService`]
//! must both pass (storage.md ST4.6).
//!
//! Each [`Case`] is a fixture chain and a script of block-sync actions and
//! verifier verdicts. After each step, [`Seen`] lists what block sync must
//! see: the submissions the driver sends to the verifier, the attempts each
//! record makes, and the events at the handle. [`run_controlled`] runs a
//! case on the controlled service. zakurad's `block_sync_driver/fidelity.rs`
//! runs it on `drive_block_sync_actions`, with state and verifier fakes over
//! the same engine, and checks that both runs see the same events.
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
//! | The 30 s deadlines on state and verifier calls | The run picks when each operation completes |
//! | The throughput probe | It's a debug mode |
//! | A query reply that finds the handle closed ends the driver | The detached handle can't close. `block_controlled`'s tests cover the service's exit |
//! | A misbehavior report waits for the supervisor | The simulator doesn't run the supervisor |
//! | A checkpoint verifier error | Regtest checkpoints only genesis, so every verdict is the full verifier's, and checkpoint-class bodies only commit |
//! | `record_body_unavailable`, `restart_body_availability` and `retry_body_availability` | Their fakes would need retry-episode summaries the fixture engine accepts. The service builds their engine requests with [`record_request`](super::controlled::record_request), from the driver's arms |
//! | zakura-state's own `MissingBlockBodyMetadata` read | The fakes answer from the engine port's port of it. An ephemeral zakura-state would need the fixture headers selected and the fixture bodies committed, and fixture bodies never pass zakura-state's contextual checks |
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
    BodyRuleId, BodyWorkAuthority, ConsensusBodyInvalid, EvidenceId, SourceId, StateVersion,
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
    /// Every action in these steps arrives before the driver takes any.
    Burst(Vec<Step>),
    /// The verifier finishes the oldest admitted submission of the block
    /// at this index.
    Verdict(usize, Verdict),
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
        let anchor = fixture.anchor();
        let blocks = fixture.extend(anchor, &case.bodies, 0);
        let mut parent = anchor;
        for (request_id, batch) in (1..).zip(blocks.chunks(HEADER_BATCH)) {
            let request = fixture.target_request(&engine.snapshot(), parent, batch, request_id);
            let prepared = engine
                .prepare_header_target(request)
                .now_or_never()
                .expect("the engine port completes each operation on its first poll")
                .expect("fixture headers prepare");
            let applied = engine
                .apply_header_target(prepared)
                .now_or_never()
                .expect("the engine port completes each operation on its first poll")
                .expect("fixture headers apply");
            assert_eq!(applied, ApplyHeaderTargetOutcome::Applied);
            parent = batch.last().expect("a chunk holds a block").frontier();
        }
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
        let scope = BodyWorkAuthority::for_view(&self.engine.view());
        match step {
            Step::Query { from, limit } | Step::QueryAnchorMoves { from, limit } => {
                self.next_query += 1;
                vec![BlockSyncAction::QueryNeededBlocks {
                    query_id: NonZeroU64::new(self.next_query).expect("counts from 1"),
                    from: block::Height(*from),
                    limit: *limit,
                    best_header_tip: self.engine.snapshot().frontiers.header_best.height,
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
            Step::Burst(steps) => steps.iter().flat_map(|step| self.actions(step)).collect(),
            Step::Verdict(..) => panic!("a verdict sends no action"),
        }
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
        let Some(pending) = self.service.pending().into_iter().find(|op| op.id == id) else {
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
            self.service
                .complete(admit.id, BlockOutcome::Admit)
                .expect("an enabled admission completes");
            let RequestIdentity::Submit { token, .. } = self.request(admit.id) else {
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
    ]
}
