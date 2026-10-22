//! Machine tests for [`ReactorState`]'s query, completion, and view transitions.
//!
//! The helpers drive `ReactorState` the way [`super::super::ReactorIO`] does:
//! a refill opens, proposes, and reports the action channel's answer before the
//! next input. The proptest applies each input to an independent model and
//! compares the two after every step. The model never calls `ReactorState`.

use std::time::Duration;

use proptest::prelude::*;
use zakura_header_chain::{BodyWorkAuthority, HeaderGeneration, HeaderWorkAuthority};

use super::*;
use crate::zakura::block_sync::{sequencer_task::initial_view, test_work_scope};

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

/// The `generation`th authority after the base test authority.
fn authority(generation: u8) -> BodyWorkAuthority {
    let base = test_work_scope();
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(
                base.header.header_generation.get() + u64::from(generation),
            ),
            ..base.header
        },
        ..base
    }
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

/// Run one refill the way the reactor does. `accept` is the action channel's
/// answer, which reaches the state before anything else happens.
fn refill(
    state: &mut ReactorState,
    force: bool,
    facts: WorkFacts,
    scope: Option<BodyWorkAuthority>,
    accept: bool,
    now: Instant,
) -> Refill {
    let start = match state.open_query(force) {
        QueryOpening::Closed => return Refill::Held,
        QueryOpening::Refresh(refresh) => QueryStart::Refresh(refresh),
        QueryOpening::NeedsWork => QueryStart::Work(facts),
    };
    let QueryPlan::Dispatch(proposal) = state.propose_query(force, start, scope) else {
        return Refill::Held;
    };
    let query = proposal.query();
    if accept {
        state.queries.accepted(proposal);
        Refill::Accepted(query)
    } else {
        state.queries.refused(proposal, now);
        Refill::Refused(query)
    }
}

fn expect_accepted(refill: Refill) -> PendingNeededQuery {
    match refill {
        Refill::Accepted(query) => query,
        other => panic!("expected an accepted query, got {other:?}"),
    }
}

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

    let reply = state.complete_needed_query(a.query_id, a.scope, anchor(0), Some(authority(0)));

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

    let reply = state.complete_needed_query(a.query_id, a.scope, anchor(0), Some(authority(0)));
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

    let reply = state.complete_needed_query(a.query_id, a.scope, anchor(0), Some(authority(1)));

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

        let reply =
            state.complete_needed_query(a.query_id, a.scope, body_anchor, Some(authority(0)));

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
    let NeededReply::Accept(first) =
        state.complete_needed_query(first.query_id, first.scope, anchor(0), Some(authority(0)))
    else {
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
    let _ =
        state.complete_needed_query(second.query_id, second.scope, anchor(0), Some(authority(0)));
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
        accept: bool,
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

fn target() -> impl Strategy<Value = Target> {
    prop_oneof![
        3 => Just(Target::Pending),
        2 => (1..4_usize).prop_map(Target::Earlier),
        1 => Just(Target::Unknown),
    ]
}

fn anchor_kind() -> impl Strategy<Value = AnchorKind> {
    prop_oneof![
        4 => Just(AnchorKind::Current),
        1 => Just(AnchorKind::Ahead),
        1 => Just(AnchorKind::Behind),
        1 => Just(AnchorKind::OtherHash),
    ]
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        6 => (
            any::<bool>(),
            prop::option::weighted(0.9, 0..40_u32),
            0..20_usize,
            0..20_usize,
            prop::bool::weighted(0.9),
            prop::bool::weighted(0.6),
        )
            .prop_map(
                |(force, first_unclaimed, backlog, low_water, has_authority, accept)| {
                    Input::Refill {
                        force,
                        first_unclaimed,
                        backlog,
                        low_water,
                        has_authority,
                        accept,
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
        accept: bool,
    ) -> Option<(ModelQuery, bool)> {
        if force {
            self.retry_until_ms = None;
        }
        if self.retry_until_ms.is_some() {
            return None;
        }
        if self.floor >= self.header_tip {
            self.pending = None;
            self.retry_until_ms = None;
            return None;
        }
        let from = first_unclaimed.filter(|from| *from <= self.header_tip)?;
        if !force && backlog >= low_water {
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
        if accept {
            self.pending = Some(query);
            self.accepted.push(query);
            self.next_id += 1;
            self.retry_until_ms = None;
        } else {
            self.retry_until_ms = Some(self.now_ms + RETRY_MS);
        }
        Some((query, accept))
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
        if self.matching(id, scope).is_none() {
            return false;
        }
        self.pending = None;
        self.retry_until_ms = Some(self.now_ms + RETRY_MS);
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

fn check(state: &ReactorState, model: &Model, base: Instant) -> Result<(), TestCaseError> {
    prop_assert_eq!(state.queries.pending().map(model_query), model.pending);
    prop_assert_eq!(
        state.queries.next_id().map(NonZeroU64::get),
        Some(model.next_id)
    );
    prop_assert_eq!(
        state.queries.retry_at(),
        model
            .retry_until_ms
            .map(|ms| base + Duration::from_millis(ms))
    );
    prop_assert_eq!(state.progress.verified_block_tip.0, model.verified_tip);
    prop_assert_eq!(state.progress.request_floor.0, model.floor);
    prop_assert_eq!(state.progress.best_header_tip.0, model.header_tip);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Queries, replies, failures, and views keep the pending query, query ID,
    /// retry gate, and mirrors equal to the model's after every step.
    #[test]
    fn reactor_state_matches_model(inputs in prop::collection::vec(input(), 1..60)) {
        let base = Instant::now();
        let mut state = reactor_state(0, 20);
        let mut model = Model::new(20);
        // The finalized height stays at zero: no generated view raises it.
        for input in inputs {
            let now = base + Duration::from_millis(model.now_ms);
            match input {
                Input::Refill { force, first_unclaimed, backlog, low_water, has_authority, accept } => {
                    let scope = has_authority.then(|| authority(model.authority));
                    let actual = refill(
                        &mut state,
                        force,
                        work(first_unclaimed, backlog, low_water),
                        scope,
                        accept,
                        now,
                    );
                    let expected = model.refill(force, first_unclaimed, backlog, low_water, has_authority, accept);
                    let actual = match actual {
                        Refill::Held => None,
                        Refill::Accepted(query) => Some((model_query(query), true)),
                        Refill::Refused(query) => Some((model_query(query), false)),
                    };
                    prop_assert_eq!(actual, expected);
                }
                Input::Reply { target, scope, anchor } => {
                    let Some(id) = model.named(target) else { continue };
                    let body_anchor = match anchor {
                        AnchorKind::Current => (model.verified_tip, verified_hash(model.verified_tip)),
                        AnchorKind::Ahead => (model.verified_tip + 1, verified_hash(model.verified_tip + 1)),
                        AnchorKind::Behind => {
                            let height = model.verified_tip.saturating_sub(1);
                            (height, verified_hash(height))
                        }
                        AnchorKind::OtherHash => (model.verified_tip, hash(0xCC, model.verified_tip)),
                    };
                    let query_id = NonZeroU64::new(id).expect("model query IDs start at one");
                    let actual = state.complete_needed_query(
                        query_id,
                        authority(scope),
                        Frontier { height: block::Height(body_anchor.0), hash: body_anchor.1 },
                        Some(authority(model.authority)),
                    );
                    prop_assert_eq!(model_reply(actual), model.reply(id, scope, body_anchor));
                }
                Input::Fail { target, scope } => {
                    let Some(id) = model.named(target) else { continue };
                    let query_id = NonZeroU64::new(id).expect("model query IDs start at one");
                    let actual = state.queries.fail(query_id, authority(scope), now);
                    prop_assert_eq!(actual, model.fail(id, scope));
                }
                Input::RetryElapsed => {
                    if model.retry_until_ms.is_some_and(|until| until <= model.now_ms) {
                        state.queries.retry_elapsed();
                        model.retry_until_ms = None;
                    }
                }
                Input::HeaderTip(tip) => {
                    state.progress.observe_header_tip(block::Height(tip), header_hash(tip));
                    model.header_tip = tip;
                }
                Input::Authority(generation) => model.authority = generation,
                Input::View { tip, floor_lag, reset, react, refill_accept, refill_backlog } => {
                    let floor = tip.saturating_sub(floor_lag);
                    let expected = model.view(tip, floor, reset, react);
                    let mut next = view(tip, model.reset_epoch, model.reaction_epoch);
                    next.download_floor = block::Height(floor);
                    let actual = state.observe_view(next);
                    match expected {
                        None => prop_assert_eq!(actual, ViewReaction::ObserveOnly),
                        Some((reset, tip_advanced)) => {
                            prop_assert_eq!(actual, ViewReaction::React { reset, tip_advanced });
                            // The reaction refills, forced after a reset, with
                            // outstanding work over the low-water target.
                            check(&state, &model, base)?;
                            let scope = Some(authority(model.authority));
                            let actual = refill(
                                &mut state,
                                reset,
                                work(Some(floor + 1), refill_backlog, 10),
                                scope,
                                refill_accept,
                                now,
                            );
                            let expected = model.refill(reset, Some(floor + 1), refill_backlog, 10, true, refill_accept);
                            if reset && floor < model.header_tip {
                                prop_assert!(expected.is_some(), "a reset refill always dispatches");
                            }
                            let actual = match actual {
                                Refill::Held => None,
                                Refill::Accepted(query) => Some((model_query(query), true)),
                                Refill::Refused(query) => Some((model_query(query), false)),
                            };
                            prop_assert_eq!(actual, expected);
                        }
                    }
                }
                Input::Wait(ms) => model.now_ms += ms,
            }
            check(&state, &model, base)?;
        }
    }
}
