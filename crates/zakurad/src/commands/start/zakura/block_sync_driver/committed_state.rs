//! ST4.6: the controlled block service and real zakura-state agree on what
//! committed bodies do: which fork full state selects, the body anchor and
//! the missing bodies a query lists, what a range read serves, and the
//! views each publishes.
//!
//! The shared cases in `fidelity.rs` run the real driver, and its verifier
//! fake commits there too, but the driver only sees what block sync sees.
//! This suite reads both stores directly after each commit. Both sides
//! commit the same [`BlockFixture::committable`] blocks: zakura-state
//! through `CommitSemanticallyVerifiedBlock`, as the full verifier sends
//! them, and the controlled service through a `committed` verdict. Each
//! side's verifier receipt orders the commits, so equal-work forks tie the
//! same way.

use std::collections::VecDeque;

use futures::FutureExt;
use tower::ServiceExt;
use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, CommittedHeaderChainView, Frontier, SourceId, StateVersion,
};
use zakura_network::zakura::{
    BlockRangeRead, BlockRangeReadLease, BlockRangeSource, BlockSyncAction, BlockSyncBlockMeta,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_storage::{
    block::{
        controlled::{ControlledBlockService, ReplyLog, Taken},
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyLimits, BlockOutcome, HandleEvent, Verdict,
    },
    header::adapter::EnginePort,
};

use super::{
    super::{
        block_range_source::StateBlockRangeSource,
        header_sync_driver::engine_comparison::ProductionState,
    },
    query_block_sync_needed_blocks,
};

/// The supplier every submission names.
const SOURCE: SourceId = SourceId::from_digest([7; 32]);

/// Limits that admit any body at once.
const LIMITS: ApplyLimits = ApplyLimits {
    checkpoint: 4,
    full: 4,
    combined: 8,
    max_checkpoint_height: block::Height(0),
};

/// What one side shows after a step: its latest view, a missing-body
/// query's anchor and bodies, and range reads.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Seen {
    /// The view's frontiers and alarms.
    view: ViewShape,
    /// The anchor and missing bodies from height 1.
    needed: (Frontier, Vec<BlockSyncBlockMeta>),
    /// Each range read's blocks, by height, hash and size.
    reads: Vec<Vec<(block::Height, block::Hash, usize)>>,
}

/// The parts of a view both sides compute alike. Versions, generations and
/// epochs are process-local counters, so [`Seen`] keeps only whether the
/// epoch moved.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ViewShape {
    frontiers: zakura_header_chain::FrontierSet,
    header_best_score: zakura_header_chain::ChainScore,
    alarms: zakura_header_chain::AlarmSet,
    /// Whether the body-work epoch moved since the previous step.
    reset: bool,
}

/// The range reads each step makes: from height 1 with room for every
/// body, from height 2, and from height 1 within a two-body budget.
const READS: [(u32, u32, u32); 3] = [(1, 8, 1_000_000), (2, 8, 1_000_000), (1, 8, 1_900)];

/// Real zakura-state, with the epoch of its last view.
struct RealSide {
    state: ProductionState,
    epoch: Option<zakura_header_chain::BodyWorkEpoch>,
}

impl RealSide {
    async fn start() -> Self {
        RealSide {
            state: ProductionState::start().await,
            epoch: None,
        }
    }

    fn view(&self) -> CommittedHeaderChainView {
        self.state
            .read_state
            .subscribe_header_chain_views()
            .borrow()
            .clone()
            .expect("an attached header runtime publishes its view")
    }

    /// Commits `block`, which the verifier received `receipt`th, as the full
    /// verifier does, and waits until the header runtime publishes the view
    /// that follows it.
    async fn commit(&self, block: &FixtureBlock, receipt: u64) {
        let before = self.view().snapshot.state_version;
        let mut verified = zakura_state::SemanticallyVerifiedBlock::from(block.block.clone());
        verified.receipt_order = Some(receipt);
        let response = self
            .state
            .writer()
            .oneshot(zakura_state::Request::CommitSemanticallyVerifiedBlock(
                verified,
            ))
            .await
            .expect("zakura-state commits a committable fixture block");
        assert_eq!(
            response,
            zakura_state::Response::Committed(block.hash()),
            "zakura-state commits the block it was sent"
        );
        published_after(self.state.read_state.subscribe_header_chain_views(), before).await;
    }

    async fn seen(&mut self) -> Seen {
        let view = self.view();
        let reset = self.epoch.replace(view.body_work_epoch) != Some(view.body_work_epoch);
        let needed =
            query_block_sync_needed_blocks(self.state.read_state.clone(), block::Height(1), 16)
                .await
                .expect("zakura-state answers the missing-body query");
        let source = StateBlockRangeSource::new(self.state.read_state.clone());
        let mut reads = Vec::new();
        for (start, count, max_body_bytes) in READS {
            let (lease, _session) = BlockRangeReadLease::detached();
            let read = source
                .read(BlockRangeRead {
                    start_height: block::Height(start),
                    count,
                    max_body_bytes,
                    lease,
                })
                .await
                .expect("zakura-state serves a range read");
            reads.push(served(&read.blocks));
        }
        Seen {
            view: shape(&view, reset),
            needed,
            reads,
        }
    }
}

/// Waits until `views` publishes a view past `before`.
async fn published_after(
    mut views: tokio::sync::watch::Receiver<Option<CommittedHeaderChainView>>,
    before: StateVersion,
) {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        views.wait_for(|view| {
            view.as_ref()
                .is_some_and(|view| view.snapshot.state_version > before)
        }),
    )
    .await
    .expect("the header runtime publishes a commit within 10 s")
    .expect("the view watch stays open");
}

/// The controlled block service over its own engine, as the run drives it.
struct ControlledSide {
    service: ControlledBlockService,
    engine: std::sync::Arc<EnginePort>,
    replies: std::sync::Arc<ReplyLog>,
    epoch: Option<zakura_header_chain::BodyWorkEpoch>,
    next_token: u64,
    next_query: u64,
}

impl ControlledSide {
    fn new() -> Self {
        let engine = std::sync::Arc::new(EnginePort::new(BlockFixture::regtest().headers()));
        let replies = std::sync::Arc::new(ReplyLog::default());
        ControlledSide {
            service: ControlledBlockService::new(engine.clone(), replies.clone(), LIMITS),
            engine,
            replies,
            epoch: None,
            next_token: 0,
            next_query: 0,
        }
    }

    /// Has the driver take `action`, the only one waiting.
    fn take(&self, action: BlockSyncAction) -> zakura_storage::block::controlled::OpId {
        let mut inbox = VecDeque::new();
        match self.service.take_received(action, &mut inbox) {
            Taken::Op(id) => id,
            taken => panic!("the driver takes the action: {taken:?}"),
        }
    }

    /// Submits, admits and commits `block`.
    fn commit(&mut self, block: &FixtureBlock) {
        self.next_token += 1;
        let scope = BodyWorkAuthority::for_view(&self.engine.view());
        let id = self.take(BlockSyncAction::SubmitBlock {
            owner: scope.bind(1, std::num::NonZeroU64::MIN),
            source: SOURCE,
            token: self.next_token,
            block: block.block.clone(),
        });
        self.service
            .complete(id, BlockOutcome::Admit)
            .expect("the limits admit every body");
        self.service
            .complete(id, BlockOutcome::Verdict(Verdict::Committed))
            .expect("a body whose parent is committed commits");
        let replied = self.replies.take();
        assert!(
            matches!(replied.as_slice(), [HandleEvent::ApplyFinished { .. }]),
            "{replied:?}"
        );
    }

    async fn seen(&mut self) -> Seen {
        let view = self.engine.view();
        let reset = self.epoch.replace(view.body_work_epoch) != Some(view.body_work_epoch);
        self.next_query += 1;
        let id = self.take(BlockSyncAction::QueryNeededBlocks {
            query_id: std::num::NonZeroU64::new(self.next_query).expect("counts from 1"),
            from: block::Height(1),
            limit: 16,
            best_header_tip: view.snapshot.frontiers.header_best.height,
            scope: BodyWorkAuthority::for_view(&view),
        });
        self.service
            .complete(id, BlockOutcome::Answer)
            .expect("the driver's query completes");
        let needed = match self.replies.take().as_slice() {
            [HandleEvent::NeededBlocks {
                body_anchor,
                blocks,
                ..
            }] => (*body_anchor, blocks.clone()),
            replied => panic!("the query replies with its bodies: {replied:?}"),
        };
        let source = self.service.range_source();
        let mut reads = Vec::new();
        for (start, count, max_body_bytes) in READS {
            let (lease, _session) = BlockRangeReadLease::detached();
            let mut call = source.read(BlockRangeRead {
                start_height: block::Height(start),
                count,
                max_body_bytes,
                lease,
            });
            assert!((&mut call).now_or_never().is_none(), "a read waits");
            let read = self.service.owner_state().read_leases()[0];
            self.service
                .complete(read, BlockOutcome::Serve)
                .expect("a pending read serves");
            let read = call
                .now_or_never()
                .expect("a served read returns")
                .expect("a served read succeeds");
            reads.push(served(&read.blocks));
        }
        Seen {
            view: shape(&view, reset),
            needed,
            reads,
        }
    }
}

/// The height, hash and size of each block a read served.
fn served(blocks: &[(block::Height, Vec<u8>)]) -> Vec<(block::Height, block::Hash, usize)> {
    use zakura_chain::serialization::ZcashDeserializeInto;

    blocks
        .iter()
        .map(|(height, bytes)| {
            let block: block::Block = bytes
                .zcash_deserialize_into()
                .expect("a read serves serialized blocks");
            (*height, block.hash(), bytes.len())
        })
        .collect()
}

fn shape(view: &CommittedHeaderChainView, reset: bool) -> ViewShape {
    ViewShape {
        frontiers: view.snapshot.frontiers,
        header_best_score: view.snapshot.header_best_score,
        alarms: view.snapshot.alarms.clone(),
        reset,
    }
}

/// Inserts the headers of `blocks`, which extend `parent`, into `port`.
async fn insert(port: &dyn Port, parent: Frontier, blocks: &[FixtureBlock], request_id: u64) {
    let snapshot = port
        .selected_chain()
        .borrow()
        .clone()
        .expect("an attached port has a snapshot");
    let request = BlockFixture::regtest().target_request(&snapshot, parent, blocks, request_id);
    let prepared = port
        .prepare_header_target(request)
        .await
        .expect("fixture headers prepare");
    assert_eq!(
        port.apply_header_target(prepared)
            .await
            .expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
}

/// ST4.6: committing a main chain, then a longer fork past it, selects the
/// same fork, anchors the same missing bodies, serves the same ranges and
/// publishes the same views on both sides. A fork tip with equal work leaves
/// the earlier receipt selected, and passing it resets body work.
#[tokio::test(flavor = "multi_thread")]
async fn committed_bodies_select_query_and_serve_alike() {
    let fixture = BlockFixture::regtest();
    let main = fixture.committable(&[], &[BodySpec::exact(900); 3], 0);
    let fork = fixture.committable(&main[..1], &[BodySpec::exact(800); 4], 1);

    let mut real = RealSide::start().await;
    let mut controlled = ControlledSide::new();
    let ports: [std::sync::Arc<dyn Port>; 2] = [real.state.port(), controlled.engine.clone()];
    for port in ports {
        insert(&*port, fixture.anchor(), &main, 1).await;
        insert(&*port, main[0].frontier(), &fork, 2).await;
    }
    let mut steps = vec![(
        "headers".to_string(),
        real.seen().await,
        controlled.seen().await,
    )];
    // Each side's verifier receives the bodies in this order, so its
    // receipts follow it.
    for (receipt, block) in (1..).zip(main.iter().chain(&fork)) {
        controlled.commit(block);
        real.commit(block, receipt).await;
        steps.push((
            format!("commit {:?} {}", block.height, block.hash()),
            real.seen().await,
            controlled.seen().await,
        ));
    }
    for (step, real, controlled) in &steps {
        assert_eq!(real, controlled, "after {step}");
    }

    // The scenario reaches the tie and the reset it claims.
    let after = |block: &FixtureBlock| {
        let label = format!("{}", block.hash());
        &steps
            .iter()
            .find(|(step, ..)| step.ends_with(&label))
            .expect("each committed block has a step")
            .1
    };
    let tie = after(&fork[1]);
    assert_eq!(
        tie.view.frontiers.verified_best,
        main[2].frontier(),
        "an equal-work tip keeps the earlier receipt"
    );
    assert!(!tie.view.reset);
    let switch = after(&fork[2]);
    assert_eq!(switch.view.frontiers.verified_best, fork[2].frontier());
    assert!(switch.view.reset, "the switch resets body work");
    assert_eq!(switch.needed.0, fork[2].frontier(), "the anchor follows it");
}
