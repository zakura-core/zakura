//! The controlled block service: operation identity, enumeration and
//! completion, the driver's intake, committed-frontier coherence, stale
//! owners and the owner model (storage.md ST4.3 to ST4.5).

use std::{
    collections::VecDeque,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use zakura_chain::block;
use zakura_header_chain::{
    BodyRuleId, BodyWorkAuthority, BodyWorkEpoch, CommittedHeaderChainView, ConsensusBodyInvalid,
    EvidenceId, Frontier, SourceId, StateVersion, VerifiedChangeCause,
};
use zakura_network::zakura::{
    BlockApplyResult, BlockRangeRead, BlockRangeReadLease, BlockRangeSource, BlockSizeEstimate,
    BlockSyncAction, BlockSyncMisbehavior, ZakuraPeerId,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_storage::{
    block::{
        controlled::{
            CompleteError, Completion, ControlledBlockService, Fact, FactKind, OpId, ReplyLog,
            Taken,
        },
        evidence::{
            ModelFailure, OperationStage, OwnerModel, RecordResult, RequestIdentity,
            ResultIdentity, ViewModel, ViewViolation,
        },
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyClass, ApplyLimits, BlockOutcome, HandleEvent, RetryKind, Verdict,
    },
    header::adapter::EnginePort,
};

/// The committed verdict.
const COMMITTED: BlockOutcome = BlockOutcome::Verdict(Verdict::Committed);

/// Polls `future` once.
fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

/// Polls `future` once and returns its output.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the engine port completes each operation on its first poll"),
    }
}

/// A service over a fresh engine, with its fixture, reply log and the
/// driver's channel.
struct Harness {
    fixture: BlockFixture,
    engine: Arc<EnginePort>,
    replies: Arc<ReplyLog>,
    service: ControlledBlockService,
    /// The actions block sync sent that the driver hasn't taken.
    inbox: VecDeque<BlockSyncAction>,
    model: OwnerModel,
    views: ViewModel,
    next_token: u64,
    next_query: u64,
}

/// Apply limits that admit two full bodies at once.
const LIMITS: ApplyLimits = ApplyLimits {
    checkpoint: 2,
    full: 2,
    combined: 3,
    max_checkpoint_height: block::Height(0),
};

impl Harness {
    fn new() -> Self {
        let fixture = BlockFixture::regtest();
        let engine = EnginePort::new(fixture.headers());
        Self::over(fixture, engine)
    }

    fn over(fixture: BlockFixture, engine: EnginePort) -> Self {
        let engine = Arc::new(engine);
        let replies = Arc::new(ReplyLog::default());
        let service = ControlledBlockService::new(engine.clone(), replies.clone(), LIMITS);
        Harness {
            fixture,
            engine,
            replies,
            service,
            inbox: VecDeque::new(),
            model: OwnerModel::new(),
            views: ViewModel::new(),
            next_token: 0,
            next_query: 0,
        }
    }

    /// Fixture blocks on `parent`, with their headers inserted.
    fn chain(&self, parent: Frontier, specs: &[BodySpec], branch: u8) -> Vec<FixtureBlock> {
        let blocks = self.fixture.extend(parent, specs, branch);
        let request = self.fixture.target_request(
            &self.engine.snapshot(),
            parent,
            &blocks,
            u64::from(branch) + 1,
        );
        let prepared =
            ready(self.engine.prepare_header_target(request)).expect("fixture headers prepare");
        assert_eq!(
            ready(self.engine.apply_header_target(prepared)).expect("fixture headers apply"),
            ApplyHeaderTargetOutcome::Applied
        );
        blocks
    }

    /// The current body-work scope.
    fn scope(&self) -> BodyWorkAuthority {
        BodyWorkAuthority::for_view(&self.engine.view())
    }

    /// A submission of `block`.
    fn submit_action(&mut self, block: &Arc<block::Block>) -> BlockSyncAction {
        self.next_token += 1;
        BlockSyncAction::SubmitBlock {
            owner: self.scope().bind(1, NonZeroU64::MIN),
            source: SourceId::from_digest([7; 32]),
            token: self.next_token,
            block: block.clone(),
        }
    }

    /// A query for `limit` heights from `from`.
    fn query_action(&mut self, from: u32, limit: u32) -> BlockSyncAction {
        self.next_query += 1;
        BlockSyncAction::QueryNeededBlocks {
            query_id: NonZeroU64::new(self.next_query).expect("counts from 1"),
            from: block::Height(from),
            limit,
            best_header_tip: self.engine.snapshot().frontiers.header_best.height,
            scope: self.scope(),
        }
    }

    /// A `record_body_invalid` for `block` at `version`.
    fn record_invalid_action(
        &self,
        block: &FixtureBlock,
        version: StateVersion,
    ) -> BlockSyncAction {
        BlockSyncAction::RecordBodyInvalid {
            expected_version: version,
            invalid: ConsensusBodyInvalid {
                hash: block.hash(),
                evidence: EvidenceId::from_digest(block.hash().0),
                rule: BodyRuleId::new("block.subsidy"),
                source: SourceId::from_digest([7; 32]),
            },
        }
    }

    /// Runs one step of the driver's loop over the inbox.
    fn take(&mut self) -> Taken {
        self.service.take_ready(&mut self.inbox)
    }

    /// Sends `action` and has the driver take it, which must be the only
    /// action waiting.
    fn send_and_take(&mut self, action: BlockSyncAction) -> OpId {
        assert!(self.inbox.is_empty(), "the driver takes the action sent");
        self.inbox.push_back(action);
        match self.take() {
            Taken::Op(id) => id,
            taken => panic!("the driver takes the action: {taken:?}"),
        }
    }

    /// Sends and takes a submission of `block`.
    fn submit(&mut self, block: &FixtureBlock) -> OpId {
        let action = self.submit_action(&block.block);
        self.send_and_take(action)
    }

    /// Sends and takes a query for `limit` heights from `from`.
    fn query(&mut self, from: u32, limit: u32) -> OpId {
        let action = self.query_action(from, limit);
        self.send_and_take(action)
    }

    /// Sends and takes a `record_body_invalid` for `block` at the engine's
    /// version.
    fn record_invalid(&mut self, block: &FixtureBlock) -> OpId {
        let action = self.record_invalid_action(block, self.engine.snapshot().state_version);
        self.send_and_take(action)
    }

    /// The outcomes enabled for `id` now.
    fn enabled(&self, id: OpId) -> Vec<BlockOutcome> {
        self.service
            .pending()
            .into_iter()
            .find(|op| op.id == id)
            .map(|op| op.enabled)
            .unwrap_or_default()
    }

    /// Completes `id` with `outcome`, then checks the owner model and the
    /// latest view.
    fn complete(&mut self, id: OpId, outcome: BlockOutcome) -> Completion {
        let completion = self
            .service
            .complete(id, outcome)
            .unwrap_or_else(|error| panic!("{} completes {id:?}: {error}", outcome.name()));
        self.check();
        completion
    }

    /// Admits `id` and completes it with `verdict`.
    fn apply(&mut self, id: OpId, verdict: Verdict) -> Completion {
        self.complete(id, BlockOutcome::Admit);
        self.complete(id, BlockOutcome::Verdict(verdict))
    }

    /// The facts published since the last check, after the owner model
    /// applies them and matches the actual state.
    fn facts(&mut self) -> Vec<FactKind> {
        let facts = self.service.take_facts();
        self.model
            .check_boundary(&facts, &self.service.owner_state())
            .expect("the owner model matches the service");
        facts.into_iter().map(|fact| fact.kind).collect()
    }

    /// Checks the owner model against the actual state, and the view model
    /// against the latest view and full state.
    fn check(&mut self) {
        self.facts();
        let view = self.engine.view();
        let views = &mut self.views;
        self.engine
            .with_full_state(|chain| views.observe(&view, chain))
            .expect("the views stay coherent");
    }

    /// The request of held operation `id`.
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

/// The view a test fabricates from `view` at `epoch` with `verified_best`.
fn view_at(
    view: &CommittedHeaderChainView,
    epoch: BodyWorkEpoch,
    verified_best: Frontier,
) -> CommittedHeaderChainView {
    let mut snapshot = view.snapshot.clone();
    snapshot.frontiers.verified_best = verified_best;
    CommittedHeaderChainView::new(snapshot, epoch)
}

/// The query ID `id` counts as.
fn query_id(id: u64) -> NonZeroU64 {
    NonZeroU64::new(id).expect("a test's query IDs count from 1")
}

/// ST4.3: IDs count up in the order the driver takes actions, a misbehavior
/// report is recorded when taken, and two services fed the same history
/// list the same operations and outcomes.
#[test]
fn ids_are_unique_and_enumeration_is_deterministic() {
    let run = || {
        let mut harness = Harness::new();
        let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 3], 0);
        let misbehavior = BlockSyncAction::Misbehavior {
            peer: ZakuraPeerId::new([3; 32]).expect("a 32-byte node ID is in bounds"),
            reason: BlockSyncMisbehavior::StatusSpam,
        };
        let ids = vec![
            harness.submit(&blocks[0]),
            harness.submit(&blocks[1]),
            harness.send_and_take(misbehavior),
        ];
        let before_query = harness.service.pending();
        let ids = [ids, vec![harness.query(1, 16)]].concat();
        harness.check();
        let pending = harness.service.pending();
        assert_eq!(pending, harness.service.pending(), "listing is a pure read");
        (ids, before_query, pending)
    };
    let (ids, before_query, pending) = run();
    assert_eq!(ids, (1..=4).map(OpId).collect::<Vec<_>>());
    assert_eq!(
        before_query
            .iter()
            .map(|op| (op.id, op.enabled.clone()))
            .collect::<Vec<_>>(),
        [(OpId(1), vec![BlockOutcome::Admit]), (OpId(2), Vec::new())],
        "only the oldest queued body with room is admissible, and a misbehavior report holds \
         no operation"
    );
    assert_eq!(
        pending
            .iter()
            .map(|op| (op.id, op.enabled.clone()))
            .collect::<Vec<_>>(),
        [
            (OpId(1), Vec::new()),
            (OpId(2), Vec::new()),
            (OpId(4), vec![BlockOutcome::Answer, BlockOutcome::Fail]),
        ],
        "the driver admits nothing while it awaits a query"
    );
    assert_eq!(
        (ids, before_query, pending),
        run(),
        "enumeration is deterministic"
    );
}

/// ST4.3: each operation completes at most once, and a refused completion
/// changes nothing.
#[test]
fn each_operation_completes_once_and_a_refusal_changes_nothing() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 3], 0);
    let second = harness.submit(&blocks[1]);
    let first = harness.submit(&blocks[0]);
    harness.complete(second, BlockOutcome::Admit);
    let query = harness.query(1, 16);
    harness.check();

    let before = (
        harness.service.owner_state(),
        harness.engine.snapshot(),
        harness.engine.view(),
        harness.service.last_seq(),
    );
    let refusals = [
        // The parent isn't committed yet.
        (second, COMMITTED),
        (second, BlockOutcome::Verdict(Verdict::Ambiguous)),
        // Nothing replaced it, and it isn't committed.
        (second, BlockOutcome::Verdict(Verdict::Duplicate)),
        // A full body never reports `ResourceExhausted`.
        (
            second,
            BlockOutcome::Verdict(Verdict::Retryable(RetryKind::ResourceExhausted)),
        ),
        // The driver awaits the query.
        (first, BlockOutcome::Admit),
        (query, BlockOutcome::Apply),
        (first, COMMITTED),
        (OpId(99), BlockOutcome::Answer),
    ];
    for (id, outcome) in refusals {
        let refused = harness.service.complete(id, outcome);
        assert!(
            matches!(
                refused,
                Err(CompleteError::Illegal(_) | CompleteError::NotPending(_))
            ),
            "{} on {id:?} is refused: {refused:?}",
            outcome.name()
        );
    }
    assert_eq!(
        before,
        (
            harness.service.owner_state(),
            harness.engine.snapshot(),
            harness.engine.view(),
            harness.service.last_seq(),
        ),
        "a refusal changes nothing"
    );
    assert!(harness.replies.take().is_empty());

    harness.complete(query, BlockOutcome::Answer);
    assert!(matches!(
        harness.service.complete(query, BlockOutcome::Answer),
        Err(CompleteError::NotPending(_))
    ));
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(first, COMMITTED);
    harness.complete(second, COMMITTED);
    for outcome in [COMMITTED, BlockOutcome::Admit] {
        assert!(matches!(
            harness.service.complete(second, outcome),
            Err(CompleteError::NotPending(_))
        ));
    }
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        blocks[1].frontier()
    );
}

/// ST4.3: while the driver awaits a record, it takes nothing off its
/// channel and admits nothing, and the owner model rejects an action taken
/// then.
#[test]
fn the_driver_takes_nothing_while_it_awaits_a_record() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    let queued = harness.submit(&blocks[0]);
    let record = harness.record_invalid(&blocks[1]);
    let submission = harness.submit_action(&blocks[1].block);
    let query = harness.query_action(1, 16);
    let query_identity = RequestIdentity::of_action(&query, ApplyClass::Full);
    harness.inbox.extend([submission, query]);
    let model = harness.model.clone();
    let facts = harness.service.take_facts();
    let last = facts.last().expect("taking actions publishes").clone();

    assert_eq!(harness.take(), Taken::Busy);
    assert_eq!(harness.inbox.len(), 2, "the actions stay in the channel");
    assert!(harness.enabled(queued).is_empty());

    // A fabricated step that takes the query while the record is pending.
    let mut model_taking = model.clone();
    model_taking
        .apply(&facts)
        .expect("the real facts are coherent");
    let taken = Fact {
        seq: last.seq + 1,
        txn: last.txn + 1,
        at: last.at,
        kind: FactKind::Submitted {
            id: OpId(3),
            request: query_identity,
            stage: Some(zakura_storage::block::Stage::Pending),
        },
    };
    assert_eq!(
        model_taking.apply(&[taken]),
        Err(ModelFailure::UnexpectedHead { txn: last.txn + 1 })
    );

    harness.model = model;
    harness
        .model
        .check_boundary(&facts, &harness.service.owner_state())
        .expect("the owner model follows the real facts");
    harness.complete(record, BlockOutcome::Apply);
    assert_eq!(harness.enabled(queued), [BlockOutcome::Admit]);
    let Taken::Op(next) = harness.take() else {
        panic!("the driver takes the submission");
    };
    assert!(matches!(
        harness.request(next),
        RequestIdentity::Submit { hash, .. } if hash == blocks[1].hash()
    ));
}

/// ST4.3: a driver blocked in record R0 while block sync sends Q1, R1 and
/// Q2 takes R1 next, coalescing Q1 into Q2, and answers Q2 only after R1,
/// as `coalesce_ready_needed_block_queries` orders them.
#[test]
fn a_blocked_driver_takes_the_later_record_before_the_coalesced_query() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 4], 0);
    let first_record = harness.record_invalid(&blocks[3]);
    let version = harness.engine.snapshot().state_version;
    let q1 = harness.query_action(1, 16);
    let r1 = harness.record_invalid_action(&blocks[2], version);
    let q2 = harness.query_action(1, 16);
    harness.inbox.extend([q1, r1, q2]);
    assert_eq!(harness.take(), Taken::Busy);
    harness.complete(first_record, BlockOutcome::Apply);

    let Taken::Op(second_record) = harness.take() else {
        panic!("the driver takes the record");
    };
    let facts = harness.facts();
    assert!(
        matches!(
            facts.as_slice(),
            [
                FactKind::Coalesced { query_id: coalesced },
                FactKind::Deferred { queue },
                FactKind::Submitted { request: RequestIdentity::Record { hash, .. }, .. },
            ] if *coalesced == query_id(1)
                && *hash == blocks[2].hash()
                && matches!(
                    queue.as_slice(),
                    [RequestIdentity::Query { query_id: deferred, .. }] if *deferred == query_id(2)
                )
        ),
        "{facts:?}"
    );
    assert!(harness.inbox.is_empty(), "the driver drained its channel");
    assert_eq!(harness.take(), Taken::Busy);
    harness.complete(second_record, BlockOutcome::Apply);

    let Taken::Op(query) = harness.take() else {
        panic!("the driver takes the deferred query");
    };
    assert!(matches!(
        harness.request(query),
        RequestIdentity::Query {
            query_id,
            from: block::Height(1),
            limit: 16,
            ..
        } if query_id == self::query_id(2)
    ));
    harness.complete(query, BlockOutcome::Answer);
    let replies = harness.replies.take();
    let [HandleEvent::NeededBlocks {
        query_id: answered,
        blocks: metas,
        ..
    }] = replies.as_slice()
    else {
        panic!("only the latest query is answered: {replies:?}");
    };
    assert_eq!(*answered, query_id(2));
    assert!(metas
        .iter()
        .all(|meta| meta.size == BlockSizeEstimate::Advertised(900)));
    assert_eq!(harness.take(), Taken::Idle);
}

/// ST4.3: once the driver takes query Q0, it drains its channel: Q1 replaces
/// Q0 and record R is deferred, as `coalesce_stale_needed_block_queries`
/// does. R follows Q1's answer.
#[test]
fn a_taken_query_folds_in_a_later_query_and_defers_a_record() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    let q0 = harness.query_action(1, 16);
    let record = harness.record_invalid_action(&blocks[1], harness.engine.snapshot().state_version);
    let q1 = harness.query_action(1, 16);
    harness.inbox.extend([record, q1]);

    let Taken::Op(query) = harness.service.take_received(q0, &mut harness.inbox) else {
        panic!("the driver takes the query");
    };
    let facts = harness.facts();
    assert!(
        matches!(
            facts.as_slice(),
            [
                FactKind::Coalesced { query_id: coalesced },
                FactKind::Deferred { queue },
                FactKind::Submitted { request: RequestIdentity::Query { query_id: taken, .. }, .. },
            ] if *coalesced == query_id(1)
                && *taken == query_id(2)
                && matches!(
                    queue.as_slice(),
                    [RequestIdentity::Record { hash, .. }] if *hash == blocks[1].hash()
                )
        ),
        "{facts:?}"
    );
    assert!(harness.inbox.is_empty());
    harness.complete(query, BlockOutcome::Answer);
    assert!(matches!(
        harness.replies.take().as_slice(),
        [HandleEvent::NeededBlocks { query_id: answered, .. }] if *answered == query_id(2)
    ));

    let Taken::Op(deferred) = harness.take() else {
        panic!("the driver takes the deferred record");
    };
    assert!(matches!(
        harness.request(deferred),
        RequestIdentity::Record { hash, .. } if hash == blocks[1].hash()
    ));
}

/// ST4.4: each commit installs its transition before its view goes out,
/// verified tips only grow along their chain within an epoch, and a reset
/// starts the next epoch.
#[test]
fn views_follow_installed_transitions_and_a_reset_bumps_the_epoch() {
    let mut harness = Harness::new();
    let anchor = harness.fixture.anchor();
    let main = harness.chain(anchor, &[BodySpec::exact(900); 2], 0);
    let fork = harness.chain(anchor, &[BodySpec::exact(800); 3], 1);
    let mut views = harness.engine.subscribe_views();
    let start_epoch = harness.engine.view().body_work_epoch;

    let mut commit = |harness: &mut Harness, block: &FixtureBlock| {
        let id = harness.submit(block);
        let completion = harness.apply(id, Verdict::Committed);
        let view = views
            .borrow_and_update()
            .clone()
            .expect("the view watch holds a view");
        assert_eq!(
            view,
            harness.engine.view(),
            "the watch holds the latest view"
        );
        assert_eq!(view.snapshot, harness.engine.snapshot());
        assert_eq!(view.snapshot.state_version, completion.state_version);
        view
    };

    let grown = commit(&mut harness, &main[0]);
    let grown = [grown, commit(&mut harness, &main[1])];
    assert!(grown.iter().all(|view| view.body_work_epoch == start_epoch));
    assert_eq!(
        grown[1].snapshot.frontiers.verified_best,
        main[1].frontier()
    );

    // Equal or less work on the fork leaves the earlier tip verified.
    for block in &fork[..2] {
        let view = commit(&mut harness, block);
        assert_eq!(view.snapshot.frontiers.verified_best, main[1].frontier());
    }
    let reset = commit(&mut harness, &fork[2]);
    assert_eq!(reset.snapshot.frontiers.verified_best, fork[2].frontier());
    assert!(
        reset.body_work_epoch > start_epoch,
        "a verified reset invalidates body work"
    );
    assert_eq!(
        harness.engine.with_full_state(|chain| chain.tips()).len(),
        2,
        "both branches stay committed"
    );

    // The view model accepts the real reset, and rejects a tip that leaves
    // its chain within an epoch, to a lower or a higher height.
    let observe = |views: &[&CommittedHeaderChainView]| {
        let mut model = ViewModel::new();
        harness
            .engine
            .with_full_state(|chain| views.iter().try_for_each(|view| model.observe(view, chain)))
    };
    assert_eq!(observe(&[&grown[1], &reset]), Ok(()));
    let higher = view_at(&grown[1], grown[1].body_work_epoch, fork[2].frontier());
    let lower = view_at(&reset, reset.body_work_epoch, main[1].frontier());
    for (last, next) in [(&grown[1], &higher), (&reset, &lower)] {
        assert!(matches!(
            observe(&[last, next]),
            Err(ViewViolation::VerifiedTipLeftChain { .. })
        ));
    }
}

/// ST4.4: the owner model rejects install facts that disagree with their
/// view or skip the epoch a reset needs.
#[test]
fn the_owner_model_rejects_tampered_install_facts() {
    let mut harness = Harness::new();
    let anchor = harness.fixture.anchor();
    let main = harness.chain(anchor, &[BodySpec::exact(900)], 0);
    let fork = harness.chain(anchor, &[BodySpec::exact(800); 2], 1);
    for block in [&main[0], &fork[0]] {
        let id = harness.submit(block);
        harness.apply(id, Verdict::Committed);
    }
    let id = harness.submit(&fork[1]);
    harness.complete(id, BlockOutcome::Admit);
    harness
        .service
        .complete(id, COMMITTED)
        .expect("the parent is committed");
    let facts = harness.service.take_facts();
    let txn = facts.last().expect("the commit publishes").txn;
    assert!(facts.iter().any(|fact| matches!(
        fact.kind,
        FactKind::TransitionInstalled {
            cause: Some(VerifiedChangeCause::Reset),
            invalidates_body_work: true,
            ..
        }
    )));
    harness
        .model
        .clone()
        .check_boundary(&facts, &harness.service.owner_state())
        .expect("the real facts are coherent");

    let tampers: [fn(&mut FactKind); 3] = [
        |kind| {
            if let FactKind::TransitionInstalled {
                invalidates_body_work,
                ..
            } = kind
            {
                *invalidates_body_work = false;
            }
        },
        |kind| {
            if let FactKind::ViewPublished(view) = kind {
                view.epoch = BodyWorkEpoch::new(0);
            }
        },
        |kind| {
            if let FactKind::TransitionInstalled { verified_best, .. } = kind {
                verified_best.height = block::Height(1);
            }
        },
    ];
    for tamper in tampers {
        let mut tampered = facts.clone();
        tampered.iter_mut().for_each(|fact| tamper(&mut fact.kind));
        assert_eq!(
            harness.model.clone().apply(&tampered),
            Err(ModelFailure::Effects { txn })
        );
    }
}

/// ST4.3: the verifier's receipt order breaks an equal-work tie. A1 was
/// admitted first and B1 committed first, so A1's commit resets the
/// verified chain to A1.
#[test]
fn an_earlier_admission_wins_an_equal_work_tie() {
    let mut harness = Harness::new();
    let anchor = harness.fixture.anchor();
    let a = harness.chain(anchor, &[BodySpec::exact(900)], 0).remove(0);
    let b = harness.chain(anchor, &[BodySpec::exact(900)], 1).remove(0);
    let first = harness.submit(&a);
    let second = harness.submit(&b);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(second, BlockOutcome::Admit);
    let epoch = harness.engine.view().body_work_epoch;

    harness.complete(second, COMMITTED);
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        b.frontier()
    );
    harness
        .service
        .complete(first, COMMITTED)
        .expect("the parent is committed");
    let facts = harness.facts();
    assert!(facts.iter().any(|kind| matches!(
        kind,
        FactKind::TransitionInstalled {
            cause: Some(VerifiedChangeCause::Reset),
            ..
        }
    )));
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        a.frontier()
    );
    assert_eq!(
        harness.engine.view().body_work_epoch,
        epoch.checked_next().expect("the epoch is small")
    );
}

/// ST4.3: a commit the engine stalls on its resource limits commits nothing
/// and reports a `Storage` retry, as zakura-state's `HeaderChainError` does,
/// after installing and publishing the stall alarm.
#[test]
fn a_stalled_commit_reports_a_storage_retry() {
    let fixture = BlockFixture::regtest();
    let engine = EnginePort::new(fixture.headers())
        .with_max_non_finalized_nodes(NonZeroUsize::new(3).expect("3 is nonzero"));
    let mut harness = Harness::over(fixture, engine);
    let anchor = harness.fixture.anchor();
    let main = harness.chain(anchor, &[BodySpec::exact(900); 3], 0);
    for block in &main {
        let id = harness.submit(block);
        harness.apply(id, Verdict::Committed);
    }
    harness.replies.take();
    // Both chains' tips protect their paths, which is four nodes. The fork's
    // header goes to the engine with its commit.
    let fork = harness.fixture.block(anchor, 1, BodySpec::exact(900));
    let id = harness.submit(&fork);
    harness.complete(id, BlockOutcome::Admit);
    let completion = harness
        .service
        .complete(id, COMMITTED)
        .expect("a stall is a verdict");
    let facts = harness.facts();
    assert!(matches!(
        completion.result,
        ResultIdentity::Verdict {
            verdict: Verdict::Retryable(RetryKind::Storage),
            ..
        }
    ));
    assert!(matches!(
        facts.as_slice(),
        [
            FactKind::TransitionInstalled { .. },
            FactKind::ViewPublished(_),
            FactKind::ReplyDelivered { .. },
            FactKind::Completed(_),
        ]
    ));
    assert!(harness.engine.view().snapshot.alarms.resource_stalled);
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        main[2].frontier()
    );
    assert!(!harness
        .engine
        .with_full_state(|chain| chain.contains(fork.hash())));
    let replies = harness.replies.take();
    let [HandleEvent::ApplyFinished { hash, outcome, .. }] = replies.as_slice() else {
        panic!("the verdict replies: {replies:?}");
    };
    assert_eq!(*hash, fork.hash());
    assert_eq!(
        outcome.result(),
        BlockApplyResult::Unavailable,
        "block sync retries a storage failure"
    );
}

/// ST4.5: a body without a coinbase height gets no reply, since the driver
/// reports a body by its height, and full state refuses to commit it, which
/// changes nothing.
#[test]
fn a_body_without_a_coinbase_height_gets_no_reply_and_no_commit() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let mut heightless = (*blocks[0].block).clone();
    heightless.transactions.clear();
    let heightless = Arc::new(heightless);
    let action = harness.submit_action(&heightless);
    let id = harness.send_and_take(action);
    harness.complete(id, BlockOutcome::Admit);

    let before = (harness.service.owner_state(), harness.engine.snapshot());
    let refused = harness.service.complete(id, COMMITTED);
    assert!(
        matches!(refused, Err(CompleteError::State(_))),
        "{refused:?}"
    );
    assert_eq!(
        before,
        (harness.service.owner_state(), harness.engine.snapshot())
    );
    harness.check();

    let completion = harness.complete(id, BlockOutcome::Verdict(Verdict::ConsensusInvalid));
    assert!(!completion.delivered);
    assert!(harness.replies.take().is_empty());
}

/// ST4.4: the missing-body query reads the engine and full state together,
/// and its anchor follows the commits.
#[test]
fn the_missing_body_query_follows_full_state() {
    let mut harness = Harness::new();
    let blocks = harness.chain(
        harness.fixture.anchor(),
        &[
            BodySpec::exact(900),
            BodySpec { size: 900, hint: 0 },
            BodySpec::exact(900),
        ],
        0,
    );
    let first = harness.submit(&blocks[0]);
    harness.apply(first, Verdict::Committed);
    harness.replies.take();
    let query = harness.query(1, 16);
    let completion = harness.complete(query, BlockOutcome::Answer);
    assert_eq!(
        completion.result,
        ResultIdentity::NeededBlocks {
            body_anchor: blocks[0].frontier(),
            blocks: blocks[1..]
                .iter()
                .map(|block| (block.height, block.hash()))
                .collect(),
        }
    );
    let replies = harness.replies.take();
    let [HandleEvent::NeededBlocks { blocks: metas, .. }] = replies.as_slice() else {
        panic!("one answer");
    };
    assert_eq!(
        metas[0].size,
        BlockSizeEstimate::Unknown,
        "a 0 hint is unknown"
    );
    assert_eq!(metas[1].size, BlockSizeEstimate::Advertised(900));

    let zero = harness.query(1, 0);
    harness.complete(zero, BlockOutcome::Answer);
    assert!(matches!(
        harness.replies.take().as_slice(),
        [HandleEvent::NeededBlocksFailed { .. }]
    ));
}

/// ST4.5: an admitted body keeps its owner and block until its verdict, a
/// replaced copy reports a duplicate, and an ambiguous verdict commits.
#[test]
fn an_admitted_body_keeps_its_block_until_its_verdict() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    let block = blocks[0].block.clone();
    let first = harness.submit(&blocks[0]);
    let copy = harness.submit(&blocks[0]);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(copy, BlockOutcome::Admit);
    let held = harness.service.owner_state().retained_blocks();
    assert_eq!(held, [(first, block.hash()), (copy, block.hash())]);
    // The fixture, the test, and each submission hold the block.
    assert_eq!(Arc::strong_count(&block), 4);
    assert_eq!(
        harness.service.owner_state().operations[0].stage,
        OperationStage::Admitted { replaced: true }
    );

    let duplicate = harness.complete(first, BlockOutcome::Verdict(Verdict::Duplicate));
    assert_eq!(
        duplicate.result,
        ResultIdentity::Verdict {
            verdict: Verdict::Duplicate,
            result: BlockApplyResult::Duplicate,
        }
    );
    let ambiguous = harness.complete(copy, BlockOutcome::Verdict(Verdict::Ambiguous));
    assert_eq!(
        ambiguous.result,
        ResultIdentity::Verdict {
            verdict: Verdict::Ambiguous,
            result: BlockApplyResult::Unavailable,
        }
    );
    // The fixture, the test, and full state hold the block.
    assert_eq!(
        Arc::strong_count(&block),
        3,
        "each verdict released its block"
    );
    assert!(harness.service.owner_state().retained_blocks().is_empty());
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        blocks[0].frontier(),
        "an ambiguous verdict commits"
    );
}

/// ST4.5: two admitted copies of a body can't both report a duplicate while
/// neither commits. Full state either replaces the older copy in its queue
/// or refuses the newer one, so once the newer copy reports a duplicate,
/// the older one is unreplaced and must reach a verdict of its own.
#[test]
fn a_refused_copy_leaves_the_older_one_unreplaced() {
    let duplicate = BlockOutcome::Verdict(Verdict::Duplicate);
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let first = harness.submit(&blocks[0]);
    let copy = harness.submit(&blocks[0]);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(copy, BlockOutcome::Admit);
    // Either copy may lose.
    assert!(harness.enabled(first).contains(&duplicate));
    assert!(harness.enabled(copy).contains(&duplicate));

    harness.complete(copy, duplicate);
    assert_eq!(
        harness.service.owner_state().operations[0].stage,
        OperationStage::Admitted { replaced: false }
    );
    let enabled = harness.enabled(first);
    assert!(!enabled.contains(&duplicate), "{enabled:?}");
    assert!(matches!(
        harness.service.complete(first, duplicate),
        Err(CompleteError::Illegal(_))
    ));
    harness.complete(first, COMMITTED);
}

/// ST4.5: a copy that a third admission replaced stays replaced when the
/// copy between them reports a duplicate.
#[test]
fn a_refused_copy_leaves_other_replacements() {
    let duplicate = BlockOutcome::Verdict(Verdict::Duplicate);
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let first = harness.submit(&blocks[0]);
    let second = harness.submit(&blocks[0]);
    let third = harness.submit(&blocks[0]);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(second, BlockOutcome::Admit);
    // The full limit admits two bodies, so the third waits for the second.
    harness.complete(second, duplicate);
    assert!(!harness.enabled(first).contains(&duplicate));
    harness.complete(third, BlockOutcome::Admit);
    assert!(harness.enabled(first).contains(&duplicate));
    harness.complete(first, duplicate);
    // The older copies are gone, so the last must reach its own verdict.
    assert!(!harness.enabled(third).contains(&duplicate));
    harness.complete(third, COMMITTED);
}

/// ST4.5: a resubmission of a committed body can't fail a consensus rule,
/// since the verifier reports the duplicate first. A body admitted before
/// another submission commits it still can.
#[test]
fn consensus_invalid_follows_the_hash_at_admission() {
    let invalid = BlockOutcome::Verdict(Verdict::ConsensusInvalid);
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);

    // Admitted before the commit: the verifier passed its duplicate check.
    let early = harness.submit(&blocks[0]);
    let committer = harness.submit(&blocks[0]);
    harness.complete(early, BlockOutcome::Admit);
    harness.apply(committer, Verdict::Committed);
    let enabled = harness.enabled(early);
    assert!(enabled.contains(&invalid), "{enabled:?}");
    assert!(enabled.contains(&BlockOutcome::Verdict(Verdict::Duplicate)));

    // Admitted after it: only the duplicate and the verifier's retries.
    let late = harness.submit(&blocks[0]);
    harness.complete(late, BlockOutcome::Admit);
    let enabled = harness.enabled(late);
    assert!(!enabled.contains(&invalid), "{enabled:?}");
    assert!(enabled.contains(&BlockOutcome::Verdict(Verdict::Duplicate)));
    assert!(matches!(
        harness.service.complete(late, invalid),
        Err(CompleteError::Illegal(_))
    ));

    harness.complete(early, invalid);
    harness.complete(late, BlockOutcome::Verdict(Verdict::Duplicate));
}

/// ST4.5: a verdict's reply for a closed handle is dropped, and the driver
/// carries on, as it ignores that send's error.
#[test]
fn a_verdict_reply_for_a_closed_handle_is_dropped() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let id = harness.submit(&blocks[0]);
    harness.replies.close();
    let completion = harness.apply(id, Verdict::Retryable(RetryKind::Storage));
    assert!(!completion.delivered);
    assert!(harness.replies.take().is_empty());
    assert_eq!(
        harness.take(),
        Taken::Idle,
        "the driver still takes actions"
    );
}

/// ST4.5: a query's reply for a closed handle ends the driver, which drops
/// its actions and takes no more. Range reads carry on.
#[test]
fn a_query_reply_for_a_closed_handle_ends_the_driver() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let block = blocks[0].block.clone();
    let queued = harness.submit(&blocks[0]);
    let source = harness.service.range_source();
    let (lease, _session) = BlockRangeReadLease::detached();
    let mut call = source.read(BlockRangeRead {
        start_height: block::Height(0),
        count: 8,
        max_body_bytes: 10_000,
        lease,
    });
    assert!(poll(&mut call).is_pending());
    let query = harness.query(1, 4);
    harness.check();

    harness.replies.close();
    let completion = harness.service.complete(query, BlockOutcome::Answer);
    assert!(completion.is_ok_and(|completion| !completion.delivered));
    let facts = harness.facts();
    assert!(
        matches!(
            facts.last(),
            Some(FactKind::DriverExited { dropped, deferred }) if *dropped == [queued] && deferred.is_empty()
        ),
        "{facts:?}"
    );
    assert_eq!(Arc::strong_count(&block), 2, "the exit released the block");
    let late = harness.query_action(1, 4);
    harness.inbox.push_back(late);
    assert_eq!(harness.take(), Taken::Stopped);
    assert_eq!(harness.inbox.len(), 1, "the driver takes nothing more");

    let read = harness.service.owner_state().read_leases()[0];
    harness.complete(read, BlockOutcome::Serve);
    assert!(matches!(poll(&mut call), Poll::Ready(Ok(_))));
}

/// ST4.5: a read keeps its lease until it completes, even after its caller
/// drops the call. A served read stops at the byte budget.
#[test]
fn a_read_keeps_its_lease_until_it_completes() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 3], 0);
    for block in &blocks {
        let id = harness.submit(block);
        harness.apply(id, Verdict::Committed);
    }
    let source = harness.service.range_source();
    let read = |start: u32, max_body_bytes: u32| {
        let (lease, session) = BlockRangeReadLease::detached();
        let call = source.read(BlockRangeRead {
            start_height: block::Height(start),
            count: 8,
            max_body_bytes,
            lease,
        });
        (call, session)
    };

    let (mut call, _session) = read(1, 1_900);
    assert!(poll(&mut call).is_pending());
    harness.check();
    let id = harness.service.owner_state().read_leases()[0];
    assert_eq!(
        harness.enabled(id),
        [BlockOutcome::Serve, BlockOutcome::Error]
    );
    let completion = harness.complete(id, BlockOutcome::Serve);
    assert_eq!(
        completion.result,
        ResultIdentity::Served {
            heights: vec![block::Height(1), block::Height(2)],
            bytes: 1_800,
        },
        "the third body doesn't fit"
    );
    assert_eq!(harness.service.owner_state().read_leases(), [id]);
    let Poll::Ready(Ok(result)) = poll(&mut call) else {
        panic!("the read returns its blocks");
    };
    assert_eq!(result.blocks[0].1, blocks[0].bytes());
    harness.check();
    assert!(harness.service.owner_state().read_leases().is_empty());

    let (mut call, session) = read(0, 10_000);
    assert!(poll(&mut call).is_pending());
    drop(call);
    harness.check();
    let id = harness.service.owner_state().read_leases()[0];
    assert_eq!(
        harness.service.owner_state().operations[0].stage,
        OperationStage::Pending {
            caller_waiting: false
        },
        "a dropped caller leaves its read pending, with its lease"
    );
    drop(session);
    let completion = harness.complete(id, BlockOutcome::Serve);
    assert!(!completion.delivered);
    assert_eq!(
        completion.result,
        ResultIdentity::Served {
            heights: Vec::new(),
            bytes: 0,
        },
        "a cancelled read never starts"
    );
    assert!(harness.service.owner_state().read_leases().is_empty());
}

/// ST4.5: a stale `record_body_invalid` retries with the engine's version,
/// as the driver does.
#[test]
fn a_stale_invalid_body_record_retries_with_the_current_version() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    assert!(harness.engine.snapshot().state_version > StateVersion::new(1));
    let action = harness.record_invalid_action(&blocks[1], StateVersion::new(1));
    let id = harness.send_and_take(action);
    let completion = harness.complete(id, BlockOutcome::Apply);
    assert_eq!(
        completion.result,
        ResultIdentity::Recorded(vec![RecordResult::Stale, RecordResult::Committed])
    );
}

/// ST4.5: shutdown drops every held operation, with its block, and the
/// owner model rejects facts with a gap.
#[test]
fn shutdown_drops_everything_and_the_model_catches_a_gap() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    let block = blocks[0].block.clone();
    let queued = harness.submit(&blocks[0]);
    harness.query(1, 4);

    let facts = harness.service.take_facts();
    assert_eq!(
        OwnerModel::new().apply(&facts[1..]),
        Err(ModelFailure::Gap { seq: 2 })
    );
    harness
        .model
        .check_boundary(&facts, &harness.service.owner_state())
        .expect("the model follows the real facts");

    harness.service.shut_down();
    let facts = harness.facts();
    assert!(matches!(
        facts.last(),
        Some(FactKind::ShutDown { dropped, deferred }) if dropped.len() == 2 && deferred.is_empty()
    ));
    assert!(harness.service.owner_state().operations.is_empty());
    assert_eq!(Arc::strong_count(&block), 2, "shutdown released the block");
    let late = harness.submit_action(&blocks[1].block);
    harness.inbox.push_back(late);
    assert_eq!(harness.take(), Taken::Stopped);
    assert_eq!(harness.inbox.len(), 1, "shutdown takes no new work");
    assert!(matches!(
        harness.service.complete(queued, BlockOutcome::Admit),
        Err(CompleteError::NotPending(_))
    ));
    assert!(harness.engine.subscribe_views().has_changed().is_err());
}

/// ST4.5: the owner model rejects a commit whose view never went out.
#[test]
fn the_owner_model_rejects_a_commit_without_its_view() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let id = harness.submit(&blocks[0]);
    harness.complete(id, BlockOutcome::Admit);
    harness
        .service
        .complete(id, COMMITTED)
        .expect("the parent is committed");
    let mut facts = harness.service.take_facts();
    let published = facts
        .iter()
        .position(|fact| matches!(fact.kind, FactKind::ViewPublished(_)))
        .expect("the commit publishes a view");
    let txn = facts[published].txn;
    facts.remove(published);
    for fact in &mut facts[published..] {
        fact.seq -= 1;
    }
    assert_eq!(
        harness
            .model
            .check_boundary(&facts, &harness.service.owner_state()),
        Err(ModelFailure::Effects { txn })
    );
}

/// How long a test waits for the intake task to end.
#[cfg(not(zakura_sim))]
const TASK_END: std::time::Duration = std::time::Duration::from_secs(10);

/// Lets the intake task run.
#[cfg(not(zakura_sim))]
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// The intake task stops reading the channel while the driver awaits a
/// record, so the channel fills and block sync's sends wait, and it takes
/// the next action once the record completes.
#[cfg(not(zakura_sim))]
#[tokio::test]
async fn the_intake_task_leaves_actions_in_the_channel_while_the_driver_awaits() {
    use zakura_rt::sync::mpsc::{self, error::TrySendError};

    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900); 2], 0);
    let (sender, receiver) = mpsc::channel(1);
    let task = harness.service.spawn(receiver);
    let record = harness.record_invalid_action(&blocks[1], harness.engine.snapshot().state_version);
    sender.send(record).await.expect("the task is running");
    settle().await;
    let query = harness.query_action(1, 16);
    sender.send(query).await.expect("the channel has room");
    settle().await;
    let submission = harness.submit_action(&blocks[0].block);
    let Err(TrySendError::Full(submission)) = sender.try_send(submission) else {
        panic!("the channel is full while the driver awaits the record");
    };
    harness.check();
    assert_eq!(
        harness
            .service
            .owner_state()
            .operations
            .iter()
            .map(|op| op.id)
            .collect::<Vec<_>>(),
        [OpId(1)]
    );

    harness.complete(OpId(1), BlockOutcome::Apply);
    settle().await;
    sender
        .try_send(submission)
        .expect("the driver took the query off the channel");
    harness.check();
    harness.complete(OpId(2), BlockOutcome::Answer);
    settle().await;
    drop(sender);
    tokio::time::timeout(TASK_END, task)
        .await
        .expect("the task ends when the channel closes")
        .expect("the task doesn't panic");
    harness.check();
    assert_eq!(
        harness.service.owner_state().retained_blocks(),
        [(OpId(3), blocks[0].hash())]
    );
}

/// When a query's reply finds the handle closed, the intake task ends and
/// drops its receiver, so block sync's later sends fail.
#[cfg(not(zakura_sim))]
#[tokio::test]
async fn the_intake_task_drops_its_channel_when_the_driver_exits() {
    let mut harness = Harness::new();
    let (sender, receiver) = zakura_rt::sync::mpsc::channel(4);
    let task = harness.service.spawn(receiver);
    let query = harness.query_action(1, 4);
    sender.send(query).await.expect("the task is running");
    settle().await;
    harness.replies.close();
    let completion = harness.complete(OpId(1), BlockOutcome::Fail);
    assert!(!completion.delivered);
    tokio::time::timeout(TASK_END, task)
        .await
        .expect("the task ends when the driver exits")
        .expect("the task doesn't panic");
    let late = harness.query_action(1, 4);
    assert!(sender.send(late).await.is_err(), "the channel is closed");
}

/// Bodies preload only before a block service binds the port: a preload
/// past the service would skip its receipts and facts.
#[test]
#[should_panic(expected = "bodies preload only to a port no block service commits through")]
fn a_preload_after_the_service_binds_panics() {
    let harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(2_000)], 0);
    let _ = harness
        .engine
        .preload_blocks(blocks.iter().map(|block| block.block.clone()));
}

/// A second block service on one port panics: the two would hand out the
/// same operation IDs and receipts.
#[test]
#[should_panic(expected = "one block service commits through a port")]
fn a_second_service_on_a_port_panics() {
    let harness = Harness::new();
    let _ = ControlledBlockService::new(harness.engine.clone(), harness.replies.clone(), LIMITS);
}

/// ST4.3: a reset the engine stalls on its resource limits installs only
/// the stall alarm, reports no verified-chain change, keeps the body-work
/// epoch, and replies with a `Storage` retry, which the owner model
/// accepts.
#[test]
fn a_stalled_reset_completes_as_a_storage_retry() {
    let fixture = BlockFixture::regtest();
    let engine = EnginePort::new(fixture.headers())
        .with_max_non_finalized_nodes(NonZeroUsize::new(1).expect("1 is nonzero"));
    let mut harness = Harness::over(fixture, engine);
    let anchor = harness.fixture.anchor();
    // A's header goes to the engine with its commit, past a one-node graph.
    let a = harness.fixture.block(anchor, 0, BodySpec::exact(900));
    let b = harness.chain(anchor, &[BodySpec::exact(900)], 1).remove(0);
    let first = harness.submit(&a);
    let second = harness.submit(&b);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(second, BlockOutcome::Admit);
    harness.complete(second, COMMITTED);
    let epoch = harness.engine.view().body_work_epoch;

    // A was received first, so its commit would reset to it.
    let completion = harness
        .service
        .complete(first, COMMITTED)
        .expect("a stall is a verdict");
    assert!(matches!(
        completion.result,
        ResultIdentity::Verdict {
            verdict: Verdict::Retryable(RetryKind::Storage),
            ..
        }
    ));
    let facts = harness.facts();
    assert!(
        facts.iter().any(|kind| matches!(
            kind,
            FactKind::TransitionInstalled {
                cause: None,
                invalidates_body_work: false,
                ..
            }
        )),
        "{facts:?}"
    );
    assert_eq!(harness.engine.view().body_work_epoch, epoch);
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        b.frontier()
    );
}

/// ST4.3: overlapping admissions of one body share the first one's
/// receipt, as zakura-consensus's receipt registry does. A was admitted
/// before B, so A's second delivery, admitted after B committed, still wins
/// the equal-work tie and resets the verified chain to A.
#[test]
fn overlapping_admissions_of_a_body_share_its_receipt() {
    let mut harness = Harness::new();
    let anchor = harness.fixture.anchor();
    let a = harness.chain(anchor, &[BodySpec::exact(900)], 0).remove(0);
    let b = harness.chain(anchor, &[BodySpec::exact(900)], 1).remove(0);
    let first = harness.submit(&a);
    let other = harness.submit(&b);
    let again = harness.submit(&a);
    harness.complete(first, BlockOutcome::Admit);
    harness.complete(other, BlockOutcome::Admit);
    harness.complete(other, COMMITTED);
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        b.frontier()
    );
    // The first delivery is still at the verifier.
    harness.complete(again, BlockOutcome::Admit);
    harness
        .service
        .complete(again, COMMITTED)
        .expect("the parent is committed");
    assert!(harness.facts().iter().any(|kind| matches!(
        kind,
        FactKind::TransitionInstalled {
            cause: Some(VerifiedChangeCause::Reset),
            ..
        }
    )));
    assert_eq!(
        harness.engine.snapshot().frontiers.verified_best,
        a.frontier()
    );
}

/// ST4.3: the actions the driver defers hold their blocks, so the owner
/// state lists them, a step that changes them publishes them, and the
/// driver's exit drops them. The owner model rejects a deferred queue that
/// differs from the service's.
#[test]
fn deferred_actions_are_owner_state() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let block = blocks[0].block.clone();
    let query = harness.query_action(1, 4);
    let submission = harness.submit_action(&block);
    let deferred = vec![RequestIdentity::of_action(&submission, ApplyClass::Full)];
    // The driver received the query from an empty channel, and the
    // submission arrived behind it.
    harness.inbox.push_back(submission);
    let Taken::Op(query) = harness.service.take_received(query, &mut harness.inbox) else {
        panic!("the driver takes the query");
    };
    assert_eq!(harness.service.owner_state().deferred, deferred);
    assert_eq!(
        Arc::strong_count(&block),
        3,
        "the deferred action holds the block"
    );
    let facts = harness.service.take_facts();
    assert!(facts.iter().any(|fact| fact.kind
        == FactKind::Deferred {
            queue: deferred.clone()
        }));
    let mut forged = facts.clone();
    for fact in &mut forged {
        if let FactKind::Deferred { queue } = &mut fact.kind {
            queue.clear();
        }
    }
    assert_eq!(
        harness
            .model
            .clone()
            .check_boundary(&forged, &harness.service.owner_state()),
        Err(ModelFailure::Mismatch)
    );
    harness
        .model
        .check_boundary(&facts, &harness.service.owner_state())
        .expect("the model follows the real facts");

    harness.replies.close();
    harness
        .service
        .complete(query, BlockOutcome::Answer)
        .expect("the query is pending");
    let facts = harness.facts();
    assert!(
        matches!(
            facts.last(),
            Some(FactKind::DriverExited { deferred: dropped, .. }) if *dropped == deferred
        ),
        "{facts:?}"
    );
    assert!(harness.service.owner_state().deferred.is_empty());
    assert_eq!(Arc::strong_count(&block), 2, "the exit released the block");
}

/// ST4.3: the driver takes no verdict while it awaits a query, so the
/// service refuses one then, and the owner model rejects a verdict
/// published then.
#[test]
fn no_verdict_completes_while_the_driver_awaits() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let id = harness.submit(&blocks[0]);
    harness.complete(id, BlockOutcome::Admit);
    harness.query(1, 4);
    let facts = harness.service.take_facts();
    harness
        .model
        .check_boundary(&facts, &harness.service.owner_state())
        .expect("the model follows the real facts");
    let last = facts.last().expect("taking the query publishes").clone();
    assert!(harness.enabled(id).is_empty());
    assert!(matches!(
        harness.service.complete(id, COMMITTED),
        Err(CompleteError::Illegal(_))
    ));

    let invalid = BlockOutcome::Verdict(Verdict::ConsensusInvalid);
    let state_version = harness.engine.snapshot().state_version;
    let (seq, last_txn) = (last.seq, last.txn);
    let completion = Completion {
        txn: last_txn + 1,
        id,
        op: zakura_storage::block::BlockOp::SubmitBlock,
        request: harness.request(id),
        outcome: invalid,
        result: ResultIdentity::Verdict {
            verdict: Verdict::ConsensusInvalid,
            result: BlockApplyResult::Rejected,
        },
        delivered: true,
        state_version,
    };
    let at = last.at;
    let published = [
        Fact {
            seq: seq + 1,
            txn: last_txn + 1,
            at,
            kind: FactKind::ReplyDelivered { id },
        },
        Fact {
            seq: seq + 2,
            txn: last_txn + 1,
            at,
            kind: FactKind::Completed(Box::new(completion)),
        },
    ];
    assert_eq!(
        harness.model.clone().apply(&published),
        Err(ModelFailure::UnexpectedHead { txn: last_txn + 1 })
    );
}

/// ST4.5: a `record_body_invalid` that isn't durable is where the driver
/// fails closed, which the contract excludes, so the service panics.
#[test]
#[should_panic(expected = "where the driver fails closed")]
fn a_record_the_driver_would_fail_closed_on_panics() {
    let mut harness = Harness::new();
    let anchor = harness.fixture.anchor();
    // The engine has no header for this body.
    let unknown = harness.fixture.block(anchor, 3, BodySpec::exact(900));
    let record = harness.record_invalid(&unknown);
    harness.service.complete(record, BlockOutcome::Apply).ok();
}

/// ST4.4: the engine refuses to commit a body block sync recorded invalid.
/// zakura-state reports the refusal as `HeaderChainError`, so the verdict
/// completes as a `Storage` retry and commits nothing.
#[test]
fn a_commit_of_a_body_recorded_invalid_reports_a_storage_retry() {
    let mut harness = Harness::new();
    let blocks = harness.chain(harness.fixture.anchor(), &[BodySpec::exact(900)], 0);
    let id = harness.submit(&blocks[0]);
    harness.complete(id, BlockOutcome::Admit);
    let record = harness.record_invalid(&blocks[0]);
    harness.complete(record, BlockOutcome::Apply);
    harness.replies.take();
    let before = (harness.engine.snapshot(), harness.engine.view());

    let completion = harness.complete(id, COMMITTED);
    assert_eq!(
        completion.result,
        ResultIdentity::Verdict {
            verdict: Verdict::Retryable(RetryKind::Storage),
            result: BlockApplyResult::Unavailable,
        }
    );
    assert_eq!(
        before,
        (harness.engine.snapshot(), harness.engine.view()),
        "the refused commit changes no state"
    );
    assert!(harness.service.owner_state().operations.is_empty());
    assert!(matches!(
        harness.replies.take().as_slice(),
        [HandleEvent::ApplyFinished { .. }]
    ));
}

/// ST4.3: a record's identity carries its retry episode's summary, so two
/// records that differ only there are told apart.
#[test]
fn a_record_identity_carries_its_retry_episode() {
    let hash = block::Hash([3; 32]);
    let evidence = EvidenceId::from_digest([4; 32]);
    let availability = zakura_header_chain::BodyUnavailableSummary {
        attempts: 2,
        suppliers: 1,
        supplier_set_digest: [5; 32],
        alarmed: true,
        ..Default::default()
    };
    let unavailable = BlockSyncAction::RecordBodyUnavailable {
        expected_version: StateVersion::new(1),
        failure: zakura_header_chain::TransientBodyFailure {
            hash,
            evidence,
            kind: zakura_header_chain::TransientBodyFailureKind::Timeout,
            availability,
        },
    };
    let RequestIdentity::Record {
        availability: carried,
        ..
    } = RequestIdentity::of_action(&unavailable, ApplyClass::Full)
    else {
        panic!("a record's identity is a record");
    };
    assert_eq!(carried, Some(availability));
}
