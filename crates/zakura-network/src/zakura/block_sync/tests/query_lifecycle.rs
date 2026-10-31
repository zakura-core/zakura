//! Witnesses of the needed-block query lifecycle on the real reactor task.
//!
//! The machine tests beside `ReactorState` decide what each answer does to the
//! query state. These tests drive the actual action channel, reply routes, and
//! shutdown token, and read each completed transition through the observer.

use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex, PoisonError},
};

use super::super::block_sync_observe::{
    ActionRefusal, BlockEvent, BlockSyncObserver, ObserverSlot, QueryReplyOutcome, ReactorSnapshot,
    ReactorTransition,
};
use super::*;

/// Records the reactor's transitions and the state each left behind.
#[derive(Debug, Default)]
struct Transitions(Mutex<Vec<(ReactorTransition, ReactorSnapshot)>>);

impl BlockSyncObserver for Transitions {
    fn observe(&self, event: BlockEvent) {
        if let BlockEvent::Reactor { transition, state } = event {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((transition, state));
        }
    }
}

impl Transitions {
    /// The first recorded transition `select` picks.
    fn find(
        &self,
        select: impl Fn(&ReactorTransition) -> bool,
    ) -> Option<(ReactorTransition, ReactorSnapshot)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(transition, _)| select(transition))
            .copied()
    }

    fn count(&self, select: impl Fn(&ReactorTransition) -> bool) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(transition, _)| select(transition))
            .count()
    }
}

/// A running reactor that already dispatched its first query, which stays
/// unanswered.
struct Fixture {
    transitions: Arc<Transitions>,
    tips: watch::Sender<(block::Height, block::Hash)>,
    handle: BlockSyncHandle,
    actions: mpsc::Receiver<BlockSyncAction>,
    reactor: zakura_rt::task::JoinHandle<()>,
    shutdown: CancellationToken,
    query_id: NonZeroU64,
    scope: zakura_header_chain::BodyWorkAuthority,
    /// Keeps the committed-view route open.
    _snapshots: watch::Sender<Option<zakura_header_chain::CommittedHeaderChainView>>,
}

impl Fixture {
    async fn accepted_query() -> Self {
        let (snapshots, mut startup) =
            committed_block_sync_startup_at_heights(0, 0, 100, immediate_body_download_config());
        let transitions = Arc::new(Transitions::default());
        startup.observer = ObserverSlot(Some(transitions.clone()));
        let (tips, header_tip) = watch::channel(startup.best_header_tip);
        startup.header_tip = Some(header_tip);
        let shutdown = startup.shutdown.clone();
        let (handle, mut actions, reactor) =
            spawn_block_sync_reactor(startup).expect("valid block-sync config");
        let BlockSyncAction::QueryNeededBlocks {
            query_id, scope, ..
        } = next_action(&mut actions).await
        else {
            panic!("startup queries the needed blocks");
        };
        Self {
            transitions,
            tips,
            handle,
            actions,
            reactor,
            shutdown,
            query_id,
            scope,
            _snapshots: snapshots,
        }
    }

    /// Fill the action channel, so its next required action finds no room.
    fn fill_action_channel(&self) {
        let sender = self
            .handle
            .routine_wiring
            .as_ref()
            .expect("the test reactor exposes routine wiring")
            .actions
            .clone();
        while sender
            .try_send(BlockSyncAction::Misbehavior {
                peer: ZakuraPeerId::new(vec![1; 32]).expect("the test peer ID has its length"),
                reason: BlockSyncMisbehavior::MalformedMessage,
            })
            .is_ok()
        {}
    }

    /// Move the header tip, so the next proposal differs from the pending query.
    fn raise_header_tip(&self) {
        self.tips
            .send((block::Height(120), block::Hash([0x78; 32])))
            .expect("the reactor watches the header tip");
    }

    fn answer(&self) -> BlockSyncEvent {
        BlockSyncEvent::ScopedNeededBlocks {
            query_id: self.query_id,
            scope: self.scope,
            body_anchor: zakura_header_chain::Frontier::new(block::Height(0), block::Hash([0; 32])),
            blocks: vec![BlockSyncBlockMeta {
                height: block::Height(1),
                hash: block::Hash([0x01; 32]),
                size: BlockSizeEstimate::Unknown,
            }],
        }
    }

    async fn await_transition(
        &self,
        description: &str,
        select: impl Fn(&ReactorTransition) -> bool,
    ) -> ReactorSnapshot {
        await_until(description, Duration::from_secs(2), || {
            self.transitions.find(&select).is_some()
        })
        .await
        .expect("the reactor reports the transition");
        self.transitions
            .find(&select)
            .expect("the transition was just recorded")
            .1
    }
}

fn is_refusal(transition: &ReactorTransition, expected: ActionRefusal) -> bool {
    matches!(transition, ReactorTransition::QueryRefused { refusal, .. } if *refusal == expected)
}

/// A full action channel refuses the replacement. The accepted query stays
/// pending, the refused proposal consumes no ID, and the retry uses that ID.
#[tokio::test]
async fn full_action_channel_keeps_the_accepted_query_and_its_next_id() {
    let mut fixture = Fixture::accepted_query().await;
    let accepted = fixture.query_id;
    fixture.fill_action_channel();
    fixture.raise_header_tip();

    let state = fixture
        .await_transition("the full channel refuses the query", |transition| {
            is_refusal(transition, ActionRefusal::Full)
        })
        .await;
    assert_eq!(state.pending.map(|query| query.query_id), Some(accepted));
    let retry_id = accepted.get() + 1;
    assert_eq!(state.next_query_id.map(NonZeroU64::get), Some(retry_id));
    assert!(state.retry_armed);

    // The retry reaches the drained channel with the unconsumed ID.
    let retried = loop {
        match next_action(&mut fixture.actions).await {
            BlockSyncAction::QueryNeededBlocks { query_id, .. } => break query_id,
            BlockSyncAction::Misbehavior { .. } => {}
            action => panic!("unexpected action while draining: {action:?}"),
        }
    };
    assert_eq!(retried.get(), retry_id);

    fixture.reactor.abort();
}

/// A header tip at the floor retires the pending query at the next opening.
/// No input-specific transition names that change, so the reactor reports it
/// as a gate change.
#[tokio::test]
async fn a_range_at_the_floor_reports_the_retired_query_state() {
    let fixture = Fixture::accepted_query().await;
    fixture
        .tips
        .send((block::Height(0), block::Hash([0; 32])))
        .expect("the reactor watches the header tip");

    let state = fixture
        .await_transition("the opening retires the pending query", |transition| {
            matches!(transition, ReactorTransition::QueryGateChanged)
        })
        .await;
    assert_eq!(state.pending, None);
    assert!(!state.retry_armed);

    fixture.reactor.abort();
}

/// The accepted query's reply still completes it while the channel stays full.
#[tokio::test]
async fn reply_completes_the_accepted_query_while_the_channel_is_full() {
    let fixture = Fixture::accepted_query().await;
    fixture.fill_action_channel();
    fixture.raise_header_tip();
    fixture
        .await_transition("the full channel refuses the query", |transition| {
            is_refusal(transition, ActionRefusal::Full)
        })
        .await;

    fixture
        .handle
        .send_control(fixture.answer())
        .expect("the reactor is running");

    let state = fixture
        .await_transition("the reply completes the query", |transition| {
            matches!(
                transition,
                ReactorTransition::QueryReply {
                    outcome: QueryReplyOutcome::Accepted,
                    ..
                }
            )
        })
        .await;
    assert_eq!(state.pending, None);

    fixture.reactor.abort();
}

/// A closed action channel ends dispatch without a retry timer. The accepted
/// query keeps its reply route, and the reactor keeps running until shutdown.
#[tokio::test]
async fn closed_action_channel_arms_no_retry_and_keeps_the_reply_route() {
    let mut fixture = Fixture::accepted_query().await;
    fixture.actions.close();
    fixture.raise_header_tip();

    let state = fixture
        .await_transition("the closed channel refuses the query", |transition| {
            is_refusal(transition, ActionRefusal::Closed)
        })
        .await;
    assert_eq!(
        state.pending.map(|query| query.query_id),
        Some(fixture.query_id)
    );
    assert!(state.dispatch_closed);
    assert!(!state.retry_armed);

    // Twice the retry delay passes without another offer.
    time::sleep(NEEDED_BLOCK_QUERY_RETRY_DELAY * 2).await;
    assert_eq!(
        fixture
            .transitions
            .count(|transition| matches!(transition, ReactorTransition::QueryRefused { .. })),
        1
    );
    assert_eq!(
        fixture
            .transitions
            .count(|transition| matches!(transition, ReactorTransition::RetryElapsed)),
        0
    );

    fixture
        .handle
        .send_control(fixture.answer())
        .expect("the reactor outlives its action driver");
    let state = fixture
        .await_transition("the late reply completes the query", |transition| {
            matches!(
                transition,
                ReactorTransition::QueryReply {
                    outcome: QueryReplyOutcome::Accepted,
                    ..
                }
            )
        })
        .await;
    assert_eq!(state.pending, None);
    assert!(!fixture.reactor.is_finished());

    fixture.reactor.abort();
}

/// Shutdown ends the reactor with an accepted query outstanding. The driver's
/// late answer and failure report fail, which tells the driver to stop.
#[tokio::test]
async fn shutdown_with_an_accepted_query_closes_both_reply_routes() {
    let fixture = Fixture::accepted_query().await;
    let late_answer = fixture.answer();

    fixture.shutdown.cancel();
    time::timeout(Duration::from_secs(2), fixture.reactor)
        .await
        .expect("shutdown does not wait for the accepted query")
        .expect("the reactor exits without a panic");

    assert!(fixture.handle.send_control(late_answer).is_err());
    assert!(fixture
        .handle
        .send_needed_blocks_query_failure(fixture.query_id, fixture.scope)
        .is_err());
}
