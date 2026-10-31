//! Machine tests for [`ReactorState`]'s query, completion, and view transitions.
//!
//! The helpers drive `ReactorState` the way [`super::super::ReactorIO`] does:
//! a refill opens, proposes, and reports the action channel's answer before the
//! next input. The proptest applies each input to an independent model and
//! compares the two after every step. The model never calls `ReactorState`.

use std::time::Duration;

#[cfg(test)]
use proptest::prelude::*;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, ChainScore, EngineMode, FrontierSet, HeaderGeneration,
    SuffixWork, VerifiedGeneration,
};

use super::*;
use crate::zakura::block_sync::sequencer_task::initial_view;

fn hash(domain: u8, raw: u32) -> block::Hash {
    let mut hash = [domain; 32];
    hash[..4].copy_from_slice(&raw.to_le_bytes());
    block::Hash(hash)
}

/// Verified hash at `height` on the test chain.
fn verified_hash(height: u32) -> block::Hash {
    hash(0xA0, height)
}

/// Header hash for the header tip at `height`.
fn header_hash(height: u32) -> block::Hash {
    hash(0xB0, height)
}

/// A committed view whose body-work authority is the `generation`th test authority.
fn authority_view(generation: u8) -> CommittedHeaderChainView {
    let frontier = Frontier {
        height: block::Height(0),
        hash: verified_hash(0),
    };
    CommittedHeaderChainView::new(
        EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(2 + u64::from(generation)),
            verified_generation: VerifiedGeneration::new(3),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), frontier.hash),
            oldest_retained_height: frontier.height,
            alarms: Default::default(),
        },
        BodyWorkEpoch::new(1),
    )
}

/// The `generation`th test authority.
fn authority(generation: u8) -> BodyWorkAuthority {
    BodyWorkAuthority::for_view(&authority_view(generation))
}

/// Make `scope` the authority of `state`'s committed view. The view's other
/// fields stay out of the query policy, and the state's verified tip never
/// diverges from its header tip.
fn set_authority(state: &mut ReactorState, scope: Option<BodyWorkAuthority>) {
    if state.body_work_scope() != scope {
        state.committed_view = scope.map(|scope| {
            let generation = (0..=u8::MAX)
                .find(|generation| authority(*generation) == scope)
                .expect("the tests name only the generated authorities");
            authority_view(generation)
        });
    }
}

/// Classify a reply under the committed authority `current`.
fn complete_query(
    state: &mut ReactorState,
    query_id: NonZeroU64,
    scope: BodyWorkAuthority,
    body_anchor: Frontier,
    current: Option<BodyWorkAuthority>,
) -> NeededReply {
    set_authority(state, current);
    state.complete_needed_query(query_id, scope, body_anchor)
}

fn frontiers(tip: u32) -> BlockSyncFrontiers {
    BlockSyncFrontiers {
        finalized_height: block::Height(0),
        verified_block_tip: block::Height(tip),
        verified_block_hash: verified_hash(tip),
    }
}

/// A started reactor state with verified tip and floor `tip` and header tip `header_tip`.
fn reactor_state(tip: u32, header_tip: u32) -> ReactorState {
    ReactorState::new(
        true,
        ReactorProgress::new(
            frontiers(tip),
            (block::Height(header_tip), header_hash(header_tip)),
            initial_view(frontiers(tip)),
        ),
        BootstrapGate::new(true, None),
        None,
    )
}

fn work(first_unclaimed: Option<u32>, backlog: usize, low_water: usize) -> WorkFacts {
    WorkFacts {
        first_unclaimed: first_unclaimed.map(block::Height),
        backlog,
        low_water,
    }
}

/// What one refill did.
#[derive(Debug, Eq, PartialEq)]
enum Refill {
    /// A gate held the query, or the plan dispatched nothing.
    Held,
    Accepted(PendingNeededQuery),
    Refused(PendingNeededQuery),
}

/// Run one refill the way the reactor does. `accept` selects between an accepted
/// dispatch and a full action channel.
fn refill(
    state: &mut ReactorState,
    force: bool,
    facts: WorkFacts,
    scope: Option<BodyWorkAuthority>,
    accept: bool,
    now: Instant,
) -> Refill {
    let answer = if accept {
        Ok(())
    } else {
        Err(ActionRefusal::Full)
    };
    refill_answered(state, force, facts, scope, answer, now)
}

/// Run one refill with the action channel's exact `answer`, which reaches the
/// state before anything else happens.
fn refill_answered(
    state: &mut ReactorState,
    force: bool,
    facts: WorkFacts,
    scope: Option<BodyWorkAuthority>,
    answer: Result<(), ActionRefusal>,
    now: Instant,
) -> Refill {
    let start = match state.open_query(force) {
        QueryOpening::Closed => return Refill::Held,
        QueryOpening::Refresh(refresh) => QueryStart::Refresh(refresh),
        QueryOpening::NeedsWork => QueryStart::Work(facts),
    };
    set_authority(state, scope);
    let QueryPlan::Dispatch(proposal) = state.propose_query(force, start) else {
        return Refill::Held;
    };
    let query = proposal.query();
    match answer {
        Ok(()) => {
            state.queries.accepted(proposal);
            Refill::Accepted(query)
        }
        Err(refusal) => {
            state.queries.refused(proposal, refusal, now);
            Refill::Refused(query)
        }
    }
}

#[cfg(test)]
fn expect_accepted(refill: Refill) -> PendingNeededQuery {
    match refill {
        Refill::Accepted(query) => query,
        other => panic!("expected an accepted query, got {other:?}"),
    }
}

#[cfg(test)]
fn anchor(height: u32) -> Frontier {
    Frontier {
        height: block::Height(height),
        hash: verified_hash(height),
    }
}

/// A view that moves the Sequencer to `tip` with the given epochs.
fn view(tip: u32, reset_epoch: u64, reaction_epoch: u64) -> SequencerView {
    SequencerView {
        reset_epoch,
        reaction_epoch,
        ..initial_view(frontiers(tip))
    }
}

// ---- seed cases ----

#[test]
fn refused_replacement_keeps_accepted_query() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    let next_id = state.queries.next_id();

    // A changed header tip makes the next proposal differ from `a`.
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));
    let b = refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        false,
        now,
    );

    assert!(
        matches!(b, Refill::Refused(b) if Some(b.query_id) == next_id && b.query_id != a.query_id)
    );
    assert_eq!(state.queries.pending(), Some(a));
    assert_eq!(state.queries.next_id(), next_id);
    assert_eq!(
        state.queries.retry_at(),
        Some(now + NEEDED_BLOCK_QUERY_RETRY_DELAY)
    );
}

#[test]
fn rejected_replacement_then_old_reply_completes_old_query() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));
    let _ = refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        false,
        now,
    );

    let reply = complete_query(
        &mut state,
        a.query_id,
        a.scope,
        anchor(0),
        Some(authority(0)),
    );

    assert_eq!(reply, NeededReply::Accept(a));
    assert_eq!(state.queries.pending(), None);
    assert_eq!(state.queries.retry_at(), None);
}

#[test]
fn unrelated_stale_reply_keeps_replacement_query() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));
    let b = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    assert_ne!(a.query_id, b.query_id);

    let reply = complete_query(
        &mut state,
        a.query_id,
        a.scope,
        anchor(0),
        Some(authority(0)),
    );
    assert_eq!(reply, NeededReply::Stale);
    assert!(!state.queries.fail(a.query_id, a.scope, now));
    assert_eq!(state.queries.pending(), Some(b));
    assert_eq!(state.queries.retry_at(), None);
}

#[test]
fn matching_reply_under_stale_authority_retires_query() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));

    let reply = complete_query(
        &mut state,
        a.query_id,
        a.scope,
        anchor(0),
        Some(authority(1)),
    );

    assert_eq!(reply, NeededReply::StaleAuthority);
    assert_eq!(state.queries.pending(), None);
}

#[test]
fn anchor_change_during_query_takes_frontier_path() {
    let now = Instant::now();
    for (body_anchor, expected) in [
        (
            anchor(6),
            NeededReply::AnchorAdvanced(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(6),
                verified_block_hash: verified_hash(6),
            }),
        ),
        (
            anchor(4),
            NeededReply::AnchorReset(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(4),
                verified_block_hash: verified_hash(4),
            }),
        ),
        // Equal heights with different hashes take the reset path.
        (
            Frontier {
                height: block::Height(5),
                hash: hash(0xCC, 5),
            },
            NeededReply::AnchorReset(BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(5),
                verified_block_hash: hash(0xCC, 5),
            }),
        ),
    ] {
        let mut state = reactor_state(5, 100);
        let a = expect_accepted(refill(
            &mut state,
            false,
            work(Some(6), 0, 10),
            Some(authority(0)),
            true,
            now,
        ));

        let reply = complete_query(
            &mut state,
            a.query_id,
            a.scope,
            body_anchor,
            Some(authority(0)),
        );

        assert_eq!(reply, expected);
        assert_eq!(state.queries.pending(), None);
    }
}

#[test]
fn view_with_unchanged_reaction_epoch_only_refreshes_mirrors() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));

    let mut changed = view(7, 0, 0);
    changed.download_floor = block::Height(3);
    assert_eq!(state.observe_view(changed), ViewReaction::ObserveOnly);

    assert_eq!(state.progress.verified_block_tip, block::Height(7));
    assert_eq!(state.progress.verified_block_hash, verified_hash(7));
    assert_eq!(state.progress.request_floor, block::Height(3));
    assert_eq!(state.progress.servable_high, block::Height(7));
    assert_eq!(state.progress.last_view, changed);
    assert_eq!(state.queries.pending(), Some(a));

    // A later reaction compares against the refreshed tip mirror.
    assert_eq!(
        state.observe_view(view(7, 0, 1)),
        ViewReaction::React {
            reset: false,
            tip_advanced: false,
        }
    );
}

/// A forced refill that the full channel refuses must not lose its force: a
/// stale outstanding snapshot above the low-water target would otherwise hold
/// the retry until an external wake.
#[test]
fn refused_forced_refill_retries_past_low_water() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let refused = refill(
        &mut state,
        true,
        work(Some(1), 50, 10),
        Some(authority(0)),
        false,
        now,
    );
    assert!(matches!(refused, Refill::Refused(query) if query.forced));
    assert!(state.queries.retry_forced());

    state.queries.retry_elapsed();
    let retried = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 50, 10),
        Some(authority(0)),
        true,
        now,
    ));
    assert!(retried.forced);
    assert!(!state.queries.retry_forced());

    // The accepted retry settled the debt, so the low-water gate applies again.
    state
        .queries
        .complete(retried.query_id, retried.scope)
        .expect("the retry is pending");
    assert_eq!(
        refill(
            &mut state,
            false,
            work(Some(1), 50, 10),
            Some(authority(0)),
            true,
            now,
        ),
        Refill::Held
    );
}

/// A forced query that state fails after acceptance owes the same retry.
#[test]
fn failed_forced_query_retries_past_low_water() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let query = expect_accepted(refill(
        &mut state,
        true,
        work(Some(1), 50, 10),
        Some(authority(0)),
        true,
        now,
    ));

    assert!(state.queries.fail(query.query_id, query.scope, now));
    state.queries.retry_elapsed();

    assert!(
        expect_accepted(refill(
            &mut state,
            false,
            work(Some(1), 50, 10),
            Some(authority(0)),
            true,
            now,
        ))
        .forced
    );
}

/// The old query's reply lifts a refused replacement's retry gate. The refused
/// force stays for the next proposal.
#[test]
fn old_reply_keeps_the_force_a_refused_replacement_owes() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let old = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));
    assert!(matches!(
        refill(
            &mut state,
            true,
            work(Some(1), 0, 10),
            Some(authority(0)),
            false,
            now,
        ),
        Refill::Refused(_)
    ));

    assert_eq!(state.queries.complete(old.query_id, old.scope), Some(old));
    assert_eq!(state.queries.retry_at(), None);
    assert!(state.queries.retry_forced());
}

/// A reset supersedes the refused query and the force it owed.
#[test]
fn reset_clears_the_owed_force() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    assert!(matches!(
        refill(
            &mut state,
            true,
            work(Some(1), 0, 10),
            Some(authority(0)),
            false,
            now,
        ),
        Refill::Refused(_)
    ));

    assert!(matches!(
        state.observe_view(view(0, 1, 1)),
        ViewReaction::React { reset: true, .. }
    ));
    assert_eq!(state.queries.retry_at(), None);
    assert!(!state.queries.retry_forced());
}

/// A closed action channel cannot accept a retry, so no timer spins during
/// shutdown. The accepted query stays pending: the driver may still answer it.
#[test]
fn closed_channel_ends_dispatch_and_keeps_the_accepted_query() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let accepted = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    let next_id = state.queries.next_id();
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));

    let refused = refill_answered(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        Err(ActionRefusal::Closed),
        now,
    );

    assert!(matches!(refused, Refill::Refused(_)));
    assert_eq!(state.queries.pending(), Some(accepted));
    assert_eq!(state.queries.next_id(), next_id);
    assert_eq!(state.queries.retry_at(), None);
    assert!(state.queries.dispatch_closed());
    assert_eq!(state.open_query(true), QueryOpening::Closed);

    assert_eq!(
        state.queries.complete(accepted.query_id, accepted.scope),
        Some(accepted)
    );
    assert_eq!(state.open_query(true), QueryOpening::Closed);
}

#[test]
fn reset_view_forces_refill_past_retry_and_low_water() {
    let now = Instant::now();
    let mut state = reactor_state(0, 100);
    let a = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
    // A refused replacement arms the retry gate.
    state
        .progress
        .observe_header_tip(block::Height(120), header_hash(120));
    assert!(matches!(
        refill(
            &mut state,
            false,
            work(Some(1), 0, 10),
            Some(authority(0)),
            false,
            now,
        ),
        Refill::Refused(_)
    ));

    // A reset that arrives without a reaction waits for the next reaction.
    assert_eq!(state.observe_view(view(0, 1, 0)), ViewReaction::ObserveOnly);
    assert_eq!(state.queries.pending(), Some(a));
    let reaction = state.observe_view(view(0, 1, 1));
    assert_eq!(
        reaction,
        ViewReaction::React {
            reset: true,
            tip_advanced: false,
        }
    );
    assert_eq!(state.queries.pending(), None);

    // Stale outstanding work exceeds the low-water target, but the forced
    // refill still dispatches, even right after a no-op view.
    assert_eq!(state.observe_view(view(0, 1, 1)), ViewReaction::ObserveOnly);
    let b = expect_accepted(refill(
        &mut state,
        true,
        work(Some(1), 50, 10),
        Some(authority(0)),
        true,
        now,
    ));
    assert_eq!(b.query_id.get(), a.query_id.get() + 1);
    assert_eq!(state.queries.pending(), Some(b));
}

#[test]
fn header_quiet_holds_queries_until_body_sync_starts() {
    let now = Instant::now();
    let mut state = ReactorState::new(
        true,
        ReactorProgress::new(
            frontiers(0),
            (block::Height(1_000), header_hash(1_000)),
            initial_view(frontiers(0)),
        ),
        BootstrapGate::new(false, None),
        None,
    );
    state
        .bootstrap
        .hold_for_headers(zakura_rt::time::Instant::now() + Duration::from_secs(30));
    assert_eq!(state.open_query(true), QueryOpening::Closed);

    state.bootstrap.start_body_sync();
    assert_eq!(state.open_query(false), QueryOpening::NeedsWork);
    // Body sync never holds for headers again.
    state
        .bootstrap
        .hold_for_headers(zakura_rt::time::Instant::now() + Duration::from_secs(30));
    assert!(!state.bootstrap.is_header_quiet());
    let _ = expect_accepted(refill(
        &mut state,
        false,
        work(Some(1), 0, 10),
        Some(authority(0)),
        true,
        now,
    ));
}

#[test]
fn hint_refresh_walks_its_interval_in_query_batches() {
    let now = Instant::now();
    let mut state = reactor_state(0, 20_000);
    state
        .queries
        .start_hint_refresh(7, Some((block::Height(1), block::Height(5_000))));

    let first = expect_accepted(refill(
        &mut state,
        false,
        work(None, 0, 0),
        Some(authority(0)),
        true,
        now,
    ));
    assert_eq!(
        (first.from, first.limit, first.hint_revision),
        (block::Height(1), NEEDED_BLOCK_REFILL_LIMIT, Some(7))
    );
    let NeededReply::Accept(first) = complete_query(
        &mut state,
        first.query_id,
        first.scope,
        anchor(0),
        Some(authority(0)),
    ) else {
        panic!("the reply matches the pending query and its anchor");
    };
    assert!(state.queries.advance_hint_refresh(&first));

    let second = expect_accepted(refill(
        &mut state,
        false,
        work(None, 0, 0),
        Some(authority(0)),
        true,
        now,
    ));
    assert_eq!((second.from, second.limit), (block::Height(4_001), 1_000));
    let _ = complete_query(
        &mut state,
        second.query_id,
        second.scope,
        anchor(0),
        Some(authority(0)),
    );
    assert!(!state.queries.advance_hint_refresh(&second));
    assert_eq!(state.queries.hint_refresh(), None);
}

// ---- proptest ----

/// Which query a completion names.
#[derive(Copy, Clone, Debug)]
enum Target {
    Pending,
    /// The `n`th most recent accepted query, counting from the pending one.
    Earlier(usize),
    Unknown,
}

/// The body anchor a reply carries, relative to the verified mirror.
#[derive(Copy, Clone, Debug)]
enum AnchorKind {
    Current,
    Ahead,
    Behind,
    OtherHash,
}

#[derive(Clone, Debug)]
enum Input {
    Refill {
        force: bool,
        first_unclaimed: Option<u32>,
        backlog: usize,
        low_water: usize,
        has_authority: bool,
        answer: Result<(), ActionRefusal>,
    },
    Reply {
        target: Target,
        scope: u8,
        anchor: AnchorKind,
    },
    Fail {
        target: Target,
        scope: u8,
    },
    RetryElapsed,
    HeaderTip(u32),
    Authority(u8),
    View {
        tip: u32,
        floor_lag: u32,
        reset: bool,
        react: bool,
        refill_accept: bool,
        refill_backlog: usize,
    },
    Wait(u64),
}

#[cfg(test)]
fn target() -> impl Strategy<Value = Target> {
    prop_oneof![
        3 => Just(Target::Pending),
        2 => (1..4_usize).prop_map(Target::Earlier),
        1 => Just(Target::Unknown),
    ]
}

#[cfg(test)]
fn anchor_kind() -> impl Strategy<Value = AnchorKind> {
    prop_oneof![
        4 => Just(AnchorKind::Current),
        1 => Just(AnchorKind::Ahead),
        1 => Just(AnchorKind::Behind),
        1 => Just(AnchorKind::OtherHash),
    ]
}

#[cfg(test)]
fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        6 => (
            any::<bool>(),
            prop::option::weighted(0.9, 0..40_u32),
            0..20_usize,
            0..20_usize,
            prop::bool::weighted(0.9),
            prop_oneof![
                12 => Just(Ok(())),
                7 => Just(Err(ActionRefusal::Full)),
                1 => Just(Err(ActionRefusal::Closed)),
            ],
        )
            .prop_map(
                |(force, first_unclaimed, backlog, low_water, has_authority, answer)| {
                    Input::Refill {
                        force,
                        first_unclaimed,
                        backlog,
                        low_water,
                        has_authority,
                        answer,
                    }
                }
            ),
        4 => (target(), 0..2_u8, anchor_kind()).prop_map(|(target, scope, anchor)| Input::Reply {
            target,
            scope,
            anchor,
        }),
        2 => (target(), 0..2_u8).prop_map(|(target, scope)| Input::Fail { target, scope }),
        1 => Just(Input::RetryElapsed),
        2 => (0..40_u32).prop_map(Input::HeaderTip),
        1 => (0..2_u8).prop_map(Input::Authority),
        3 => (
            0..30_u32,
            0..3_u32,
            prop::bool::weighted(0.2),
            prop::bool::weighted(0.6),
            any::<bool>(),
            0..20_usize,
        )
            .prop_map(
                |(tip, floor_lag, reset, react, refill_accept, refill_backlog)| Input::View {
                    tip,
                    floor_lag,
                    reset,
                    react,
                    refill_accept,
                    refill_backlog,
                }
            ),
        2 => (0..400_u64).prop_map(Input::Wait),
    ]
}

/// A query as the model records it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ModelQuery {
    id: u64,
    scope: u8,
    from: u32,
    limit: u32,
    header_tip: u32,
    forced: bool,
}

/// The reply the model expects.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ModelReply {
    Stale,
    StaleAuthority,
    Advanced(u32, block::Hash),
    Reset(u32, block::Hash),
    Accept(ModelQuery),
}

/// An independent model of the reactor's query and progress rules.
#[derive(Debug)]
struct Model {
    now_ms: u64,
    pending: Option<ModelQuery>,
    /// Every accepted query, oldest first.
    accepted: Vec<ModelQuery>,
    next_id: u64,
    retry_until_ms: Option<u64>,
    /// A refused or failed forced query owes the next proposal its force.
    retry_forced: bool,
    /// The action channel reported closed.
    closed: bool,
    authority: u8,
    header_tip: u32,
    verified_tip: u32,
    floor: u32,
    /// The Sequencer's epochs and the last ones the reactor reacted to.
    reset_epoch: u64,
    reaction_epoch: u64,
    seen_reset_epoch: u64,
    seen_reaction_epoch: u64,
}

const RETRY_MS: u64 = 250;
const READ_LIMIT: u32 = 4_000;

impl Model {
    fn new(header_tip: u32) -> Self {
        Self {
            now_ms: 0,
            pending: None,
            accepted: Vec::new(),
            next_id: 1,
            retry_until_ms: None,
            retry_forced: false,
            closed: false,
            authority: 0,
            header_tip,
            verified_tip: 0,
            floor: 0,
            reset_epoch: 0,
            reaction_epoch: 0,
            seen_reset_epoch: 0,
            seen_reaction_epoch: 0,
        }
    }

    /// The query a refill dispatches, or `None` when the refill does nothing.
    /// Lifts or retires state the gates touch on the way.
    fn refill(
        &mut self,
        force: bool,
        first_unclaimed: Option<u32>,
        backlog: usize,
        low_water: usize,
        has_authority: bool,
        answer: Result<(), ActionRefusal>,
    ) -> Option<(ModelQuery, bool)> {
        if self.closed {
            return None;
        }
        if force {
            self.retry_until_ms = None;
        }
        if self.retry_until_ms.is_some() {
            return None;
        }
        if self.floor >= self.header_tip {
            self.pending = None;
            self.retry_until_ms = None;
            self.retry_forced = false;
            return None;
        }
        let from = first_unclaimed.filter(|from| *from <= self.header_tip)?;
        let forced = force || self.retry_forced;
        if !forced && backlog >= low_water {
            return None;
        }
        if !has_authority {
            return None;
        }
        let window = u32::try_from((low_water * 2).max(1)).expect("small test window");
        let query = ModelQuery {
            id: self.next_id,
            scope: self.authority,
            from,
            limit: (self.header_tip - from + 1).min(window).min(READ_LIMIT),
            header_tip: self.header_tip,
            forced,
        };
        let same_request = self.pending.is_some_and(|pending| {
            (
                pending.scope,
                pending.from,
                pending.limit,
                pending.header_tip,
            ) == (query.scope, query.from, query.limit, query.header_tip)
        });
        if same_request {
            return None;
        }
        match answer {
            Ok(()) => {
                self.pending = Some(query);
                self.accepted.push(query);
                self.next_id += 1;
                self.retry_until_ms = None;
                self.retry_forced = false;
            }
            Err(ActionRefusal::Closed) => {
                self.closed = true;
                self.retry_until_ms = None;
                self.retry_forced = false;
            }
            Err(_) => {
                self.retry_until_ms = Some(self.now_ms + RETRY_MS);
                self.retry_forced = forced;
            }
        }
        Some((query, answer.is_ok()))
    }

    fn named(&self, target: Target) -> Option<u64> {
        match target {
            Target::Pending => self.pending.map(|query| query.id),
            Target::Earlier(back) => self
                .accepted
                .len()
                .checked_sub(back + 1)
                .map(|index| self.accepted[index].id),
            Target::Unknown => Some(u64::MAX),
        }
    }

    fn matching(&self, id: u64, scope: u8) -> Option<ModelQuery> {
        self.pending
            .filter(|pending| pending.id == id && pending.scope == scope)
    }

    fn reply(&mut self, id: u64, scope: u8, anchor: (u32, block::Hash)) -> ModelReply {
        let Some(query) = self.matching(id, scope) else {
            return ModelReply::Stale;
        };
        self.pending = None;
        self.retry_until_ms = None;
        if scope != self.authority {
            return ModelReply::StaleAuthority;
        }
        if anchor == (self.verified_tip, verified_hash(self.verified_tip)) {
            ModelReply::Accept(query)
        } else if anchor.0 > self.verified_tip {
            ModelReply::Advanced(anchor.0, anchor.1)
        } else {
            ModelReply::Reset(anchor.0, anchor.1)
        }
    }

    fn fail(&mut self, id: u64, scope: u8) -> bool {
        let Some(query) = self.matching(id, scope) else {
            return false;
        };
        self.pending = None;
        self.retry_until_ms = Some(self.now_ms + RETRY_MS);
        self.retry_forced |= query.forced;
        true
    }

    /// Returns `Some(reset)` when the view needs a reaction.
    fn view(&mut self, tip: u32, floor: u32, reset: bool, react: bool) -> Option<(bool, bool)> {
        if reset {
            self.reset_epoch += 1;
        }
        if react {
            self.reaction_epoch += 1;
        }
        let tip_advanced = tip > self.verified_tip;
        self.verified_tip = tip;
        self.floor = floor;
        if self.reaction_epoch == self.seen_reaction_epoch {
            return None;
        }
        self.seen_reaction_epoch = self.reaction_epoch;
        let reset = self.reset_epoch != self.seen_reset_epoch;
        if reset {
            self.seen_reset_epoch = self.reset_epoch;
            self.pending = None;
            self.retry_until_ms = None;
            self.retry_forced = false;
        }
        Some((reset, tip_advanced))
    }
}

fn model_query(query: PendingNeededQuery) -> ModelQuery {
    let scope = (0..2)
        .find(|generation| authority(*generation) == query.scope)
        .expect("the test uses two authorities");
    ModelQuery {
        id: query.query_id.get(),
        scope,
        from: query.from.0,
        limit: query.limit,
        header_tip: query.best_header_tip.0,
        forced: query.forced,
    }
}

fn model_reply(reply: NeededReply) -> ModelReply {
    match reply {
        NeededReply::Stale => ModelReply::Stale,
        NeededReply::StaleAuthority => ModelReply::StaleAuthority,
        NeededReply::AnchorAdvanced(frontiers) => ModelReply::Advanced(
            frontiers.verified_block_tip.0,
            frontiers.verified_block_hash,
        ),
        NeededReply::AnchorReset(frontiers) => ModelReply::Reset(
            frontiers.verified_block_tip.0,
            frontiers.verified_block_hash,
        ),
        NeededReply::Accept(query) => ModelReply::Accept(model_query(query)),
    }
}

fn check(state: &ReactorState, model: &Model, base: Instant) {
    assert_eq!(
        state.queries.pending().map(model_query),
        model.pending,
        "{PROPERTY}"
    );
    assert_eq!(
        state.queries.next_id().map(NonZeroU64::get),
        Some(model.next_id),
        "{PROPERTY}"
    );
    assert_eq!(
        state.queries.retry_at(),
        model
            .retry_until_ms
            .map(|ms| base + Duration::from_millis(ms)),
        "{PROPERTY}"
    );
    assert_eq!(
        state.progress.verified_block_tip.0, model.verified_tip,
        "{PROPERTY}"
    );
    assert_eq!(
        state.queries.retry_forced(),
        model.retry_forced,
        "{PROPERTY}"
    );
    assert_eq!(state.queries.dispatch_closed(), model.closed, "{PROPERTY}");
    assert_eq!(state.progress.request_floor.0, model.floor, "{PROPERTY}");
    assert_eq!(
        state.progress.best_header_tip.0, model.header_tip,
        "{PROPERTY}"
    );
}

const PROPERTY: &str = "block/reactor-policy";

/// Applicable transitions in a decoder-v2 reactor history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BlockReactorReplaySummary {
    /// Inputs decoded.
    pub transitions: usize,
    /// Query dispatches accepted by the action channel.
    pub accepted: usize,
    /// Query dispatches refused by the action channel.
    pub refused: usize,
    /// Named query replies checked.
    pub replies: usize,
    /// Replies rejected for query or authority identity.
    pub stale_replies: usize,
    /// Retry deadlines elapsed.
    pub retry_expiries: usize,
    /// Sequencer views observed.
    pub views: usize,
    /// Views that required a reaction.
    pub reactions: usize,
    /// Reactions that reset pending work.
    pub resets: usize,
}

fn run(
    inputs: impl IntoIterator<Item = Input>,
    mut after_step: impl FnMut(&mut ReactorState),
) -> BlockReactorReplaySummary {
    let mut summary = BlockReactorReplaySummary::default();
    let base = Instant::now();
    let mut state = reactor_state(0, 20);
    let mut model = Model::new(20);
    // The finalized height stays at zero: no generated view raises it.
    for input in inputs {
        summary.transitions += 1;
        let now = base + Duration::from_millis(model.now_ms);
        match input {
            Input::Refill {
                force,
                first_unclaimed,
                backlog,
                low_water,
                has_authority,
                answer,
            } => {
                let scope = has_authority.then(|| authority(model.authority));
                let actual = refill_answered(
                    &mut state,
                    force,
                    work(first_unclaimed, backlog, low_water),
                    scope,
                    answer,
                    now,
                );
                let expected = model.refill(
                    force,
                    first_unclaimed,
                    backlog,
                    low_water,
                    has_authority,
                    answer,
                );
                let actual = match actual {
                    Refill::Held => None,
                    Refill::Accepted(query) => {
                        summary.accepted += 1;
                        Some((model_query(query), true))
                    }
                    Refill::Refused(query) => {
                        summary.refused += 1;
                        Some((model_query(query), false))
                    }
                };
                assert_eq!(actual, expected, "{PROPERTY}");
            }
            Input::Reply {
                target,
                scope,
                anchor,
            } => {
                let Some(id) = model.named(target) else {
                    continue;
                };
                let body_anchor = match anchor {
                    AnchorKind::Current => (model.verified_tip, verified_hash(model.verified_tip)),
                    AnchorKind::Ahead => (
                        model.verified_tip + 1,
                        verified_hash(model.verified_tip + 1),
                    ),
                    AnchorKind::Behind => {
                        let height = model.verified_tip.saturating_sub(1);
                        (height, verified_hash(height))
                    }
                    AnchorKind::OtherHash => (model.verified_tip, hash(0xCC, model.verified_tip)),
                };
                let query_id = NonZeroU64::new(id).expect("model query IDs start at one");
                let actual = complete_query(
                    &mut state,
                    query_id,
                    authority(scope),
                    Frontier {
                        height: block::Height(body_anchor.0),
                        hash: body_anchor.1,
                    },
                    Some(authority(model.authority)),
                );
                let expected = model.reply(id, scope, body_anchor);
                summary.replies += 1;
                summary.stale_replies += usize::from(matches!(
                    expected,
                    ModelReply::Stale | ModelReply::StaleAuthority
                ));
                assert_eq!(model_reply(actual), expected, "{PROPERTY}");
            }
            Input::Fail { target, scope } => {
                let Some(id) = model.named(target) else {
                    continue;
                };
                let query_id = NonZeroU64::new(id).expect("model query IDs start at one");
                let actual = state.queries.fail(query_id, authority(scope), now);
                assert_eq!(actual, model.fail(id, scope), "{PROPERTY}");
            }
            Input::RetryElapsed => {
                if model
                    .retry_until_ms
                    .is_some_and(|until| until <= model.now_ms)
                {
                    summary.retry_expiries += 1;
                    state.queries.retry_elapsed();
                    model.retry_until_ms = None;
                }
            }
            Input::HeaderTip(tip) => {
                state
                    .progress
                    .observe_header_tip(block::Height(tip), header_hash(tip));
                model.header_tip = tip;
            }
            Input::Authority(generation) => model.authority = generation,
            Input::View {
                tip,
                floor_lag,
                reset,
                react,
                refill_accept,
                refill_backlog,
            } => {
                let floor = tip.saturating_sub(floor_lag);
                let expected = model.view(tip, floor, reset, react);
                let mut next = view(tip, model.reset_epoch, model.reaction_epoch);
                next.download_floor = block::Height(floor);
                let actual = state.observe_view(next);
                summary.views += 1;
                summary.reactions += usize::from(expected.is_some());
                summary.resets += usize::from(expected.is_some_and(|(reset, _)| reset));
                match expected {
                    None => assert_eq!(actual, ViewReaction::ObserveOnly, "{PROPERTY}"),
                    Some((reset, tip_advanced)) => {
                        assert_eq!(
                            actual,
                            ViewReaction::React {
                                reset,
                                tip_advanced
                            },
                            "{PROPERTY}"
                        );
                        // The reaction refills, forced after a reset, with
                        // outstanding work over the low-water target.
                        check(&state, &model, base);
                        let scope = Some(authority(model.authority));
                        let actual = refill(
                            &mut state,
                            reset,
                            work(Some(floor + 1), refill_backlog, 10),
                            scope,
                            refill_accept,
                            now,
                        );
                        let expected = model.refill(
                            reset,
                            Some(floor + 1),
                            refill_backlog,
                            10,
                            true,
                            if refill_accept {
                                Ok(())
                            } else {
                                Err(ActionRefusal::Full)
                            },
                        );
                        if reset && floor < model.header_tip && !model.closed {
                            assert!(
                                expected.is_some(),
                                "{PROPERTY}: a reset refill always dispatches"
                            );
                        }
                        let actual = match actual {
                            Refill::Held => None,
                            Refill::Accepted(query) => {
                                summary.accepted += 1;
                                Some((model_query(query), true))
                            }
                            Refill::Refused(query) => {
                                summary.refused += 1;
                                Some((model_query(query), false))
                            }
                        };
                        assert_eq!(actual, expected, "{PROPERTY}");
                    }
                }
            }
            Input::Wait(ms) => model.now_ms += ms,
        }
        after_step(&mut state);
        check(&state, &model, base);
    }

    summary
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]
    #[test]
    fn reactor_state_matches_model(inputs in prop::collection::vec(input(), 1..60)) {
        run(inputs, |_| {});
    }
    #[test]
    fn reactor_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_block_reactor_bytes(&bytes);
    }
}

#[cfg(test)]
mod committed;
#[cfg(test)]
mod status;

/// Replay decoder-v2 quadruples against the existing reactor policy model.
/// Read at most 512 bytes and express deadlines relative to one clock origin.
pub fn replay_block_reactor_bytes(data: &[u8]) -> BlockReactorReplaySummary {
    run(decode(data), |_| {})
}

fn decode(data: &[u8]) -> impl Iterator<Item = Input> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        let target = match b % 6 {
            0..=2 => Target::Pending,
            3..=4 => Target::Earlier(usize::from(b % 6 - 2)),
            _ => Target::Unknown,
        };
        match a % 8 {
            0 => Input::Refill {
                force: b & 1 != 0,
                first_unclaimed: (b & 2 == 0).then_some(u32::from(c % 40)),
                backlog: usize::from(d % 20),
                low_water: usize::from(d / 20) + 1,
                has_authority: b & 4 == 0,
                answer: if b & 8 == 0 {
                    Ok(())
                } else if b & 16 == 0 {
                    Err(ActionRefusal::Full)
                } else {
                    Err(ActionRefusal::Closed)
                },
            },
            1 => Input::Reply {
                target,
                scope: c % 2,
                anchor: match d % 4 {
                    0 => AnchorKind::Current,
                    1 => AnchorKind::Ahead,
                    2 => AnchorKind::Behind,
                    _ => AnchorKind::OtherHash,
                },
            },
            2 => Input::Fail {
                target,
                scope: c % 2,
            },
            3 => Input::RetryElapsed,
            4 => Input::HeaderTip(u32::from(c % 40)),
            5 => Input::Authority(c % 2),
            6 => Input::View {
                tip: u32::from(c % 30),
                floor_lag: u32::from(d % 3),
                reset: b & 1 != 0,
                react: b & 2 != 0,
                refill_accept: b & 4 == 0,
                refill_backlog: usize::from(d / 3 % 20),
            },
            _ => Input::Wait(u64::from(c) * 2),
        }
    })
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 1, 200, 0, 8, 2, 200, 7, 0, 200, 0, 3, 0, 0, 0, 1, 0, 0, 1, 6, 2, 1, 0, 6, 3, 0, 0, 5, 0,
    1, 0, 1, 0, 0, 0, 0, 0, 1, 200, 2, 0, 1, 0, 7, 0, 200, 0, 3, 0, 0, 0, 4, 0, 30, 0, 6, 0, 3, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn reactor_witness_exercises_refusal_retry_reset_and_authority() {
    let summary = replay_block_reactor_bytes(WITNESS);
    assert_eq!(
        summary,
        BlockReactorReplaySummary {
            transitions: 15,
            accepted: 4,
            refused: 1,
            replies: 2,
            stale_replies: 1,
            retry_expiries: 2,
            views: 3,
            reactions: 2,
            resets: 1,
        }
    );
    assert_eq!(
        replay_block_reactor_bytes(&[]),
        BlockReactorReplaySummary::default()
    );
    println!("tier=Machine target=block_reactor decoder=2 {summary:?}");
}

#[test]
#[should_panic(expected = "block/reactor-policy")]
fn reactor_witness_rejects_lost_forced_retry() {
    run(decode(&[0, 9, 1, 200]), |state| {
        state.queries.retry_forced = false;
    });
}

#[test]
#[should_panic(expected = "block/reactor-policy")]
fn reactor_witness_rejects_reopened_dispatch_after_close() {
    run(decode(&[0, 24, 1, 200]), |state| {
        state.queries.dispatch_closed = false;
    });
}

#[test]
#[should_panic(expected = "block/reactor-policy")]
fn reactor_witness_rejects_lost_pending_query() {
    run(decode(WITNESS), |state| {
        state.queries.pending = None;
    });
}

#[test]
#[should_panic(expected = "block/reactor-policy")]
fn reactor_witness_rejects_wrong_request_floor() {
    run(decode(WITNESS), |state| {
        state.progress.request_floor = block::Height(99);
    });
}
