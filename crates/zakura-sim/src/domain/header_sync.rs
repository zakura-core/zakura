//! Header-sync domain properties (properties.md, Header sync).
//!
//! Each check reads one owner's view-side evidence through a small view
//! trait, so a shared property can take the check over later by
//! registering it against another view.

use zakura_network::zakura::observe::EventQueue;

use crate::{
    harness::header_node::{
        adoption::{AdoptionEvidence, WatchRead},
        authority::AuthorityEvidence,
        capacity::{BoundaryRead, CapacityEvidence},
        chain_effect::ChainEffectEvidence,
        claim::{Claim, ClaimEvidence, SettlementEvidence},
        ends::{EndEvidence, EndFact, EnvironmentAction},
        fork_choice::ForkChoiceEvidence,
        hold_wake::{HoldWakeEvidence, HoldWakeFact},
        outstanding::OutstandingEvidence,
        publication::{PublicationEvidence, PublicationFact, STATUS_DEADLINE},
        retirement::{HandoffFact, RetirementEvidence},
        round_life::RoundLifeEvidence,
        stale::StaleEvidence,
        wake::{WakeEvidence, WakeFact},
    },
    property::{
        Cardinality, CheckCtx, EndViolation, FactAnswer, HasView, Kind, LivenessBound,
        ObligationKey, Pending, Property, PropertyId, Source, Status, Subject, TriggerHistory,
        Unit, Verdict,
    },
    ObservationLog,
};
use zakura_scheduler::SimInstant;

/// The header chunk budget never holds more than its capacity, reserved
/// and owned together.
pub const CHUNK_BUDGET_BOUND: &str = "header-sync/chunk-budget-bound";

/// The model's header chunk budget capacity, from properties.md's
/// Header-sync chunk budget. A unit test pins it to production's.
pub const CHUNK_BUDGET_CAPACITY: usize = 4_000;

/// The model's cached-pass refill interval: a deferred pass runs at most
/// this long after a release's wake opens. A unit test pins it to
/// production's.
pub const CACHED_PASS_REFILL: std::time::Duration = std::time::Duration::from_millis(1);

/// What the header budget checks read: one reactor's probe reads and the
/// claim breaks its tap found.
pub trait BudgetView {
    /// The reactor's probe reads, and how each boundary's read went.
    fn capacity(&self) -> &CapacityEvidence;

    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;
}

/// [`CHUNK_BUDGET_BOUND`], as a harness whose view is a [`BudgetView`]
/// registers it.
pub fn chunk_budget_bound<S>() -> Property<S>
where
    S: HasView,
    S::View: BudgetView,
{
    Property {
        id: PropertyId::new(CHUNK_BUDGET_BOUND),
        claim: "the header chunk budget never holds more than its 4,000 headers, reserved and \
                owned together",
        kind: Kind::Bound {
            unit: Unit::Headers,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_chunk_budget_bound::<S>,
        witness: Some(TriggerHistory::View(budget_claimed::<S::View>)),
    }
}

/// Checks [`CHUNK_BUDGET_BOUND`] at the boundary after a step.
///
/// It's violated by a probe reading over [`CHUNK_BUDGET_CAPACITY`], or by a
/// claim break the tap found at this step: `BudgetOverCapacity` on a read,
/// or `OverBudget` on a reported reservation. A boundary whose read found
/// the budget's lock held judges nothing. With the reactor stopped
/// mid-poll, as it is inside a release, that's allowed, and the boundary
/// stays pending ([`Pending::UnreadBoundary`]). Otherwise no task should
/// hold the lock, so the missing read is missing evidence.
fn check_chunk_budget_bound<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: BudgetView,
{
    let view = cx.view();
    let step = cx.step;
    let boundary = view.capacity().boundary(step);
    match boundary {
        Some(BoundaryRead::UnreadQuiet) => cx.missing_evidence(format!(
            "the header budget's lock was held at the boundary after step {step}, with the \
             reactor not stopped mid-poll"
        )),
        Some(BoundaryRead::UnreadMidPoll) => cx.pending(Pending::UnreadBoundary),
        Some(BoundaryRead::Read) | None => {}
    }
    let records = view
        .claims()
        .at(step, &[Claim::BudgetOverCapacity, Claim::OverBudget]);
    // The tap's `BudgetOverCapacity` judges the read against the probe's
    // capacity, which a wrong production capacity would move.
    let read_judged = records
        .iter()
        .any(|record| record.claim == Claim::BudgetOverCapacity);
    let mut breaks: Vec<String> = records.into_iter().map(|record| record.detail).collect();
    if let Some(reading) = view.capacity().reading_at(step) {
        let claimed = reading.reserved.saturating_add(reading.owned);
        cx.peak(
            "claimed-headers",
            u64::try_from(claimed).unwrap_or(u64::MAX),
        );
        if claimed > CHUNK_BUDGET_CAPACITY && !read_judged {
            breaks.push(format!(
                "the probe reads {} reserved and {} owned",
                reading.reserved, reading.owned
            ));
        }
    }
    let expected = format!("at most {CHUNK_BUDGET_CAPACITY} headers reserved and owned");
    let mut breaks = breaks.into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected.clone(), other);
        }
        return cx.violated(expected, first);
    }
    match boundary {
        Some(BoundaryRead::Read) => Verdict::Holds,
        _ => Verdict::NotApplicable,
    }
}

/// A released round returns its reservation and leases once its calls are
/// handled, and a stopped node's teardown returns every chunk-budget charge
/// and every locator permit no pending storage read still holds.
pub const TEARDOWN_RETURNS_BASELINE: &str = "header-sync/teardown-returns-baseline";

/// The claim rules [`TEARDOWN_RETURNS_BASELINE`] reports: round settlement's,
/// then node settlement's.
const TEARDOWN_CLAIMS: [Claim; 7] = [
    Claim::LeakedReservation,
    Claim::LeakedLease,
    Claim::OpenOwnership,
    Claim::OpenReservation,
    Claim::TeardownReserved,
    Claim::TeardownOwned,
    Claim::TeardownLocatorPermits,
];

/// What the teardown checks read: one reactor's claim breaks and its
/// settlement at each drain.
pub trait TeardownView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// The reactor's released rounds and its own teardown at each drain.
    fn rounds(&self) -> &SettlementEvidence;
}

/// [`TEARDOWN_RETURNS_BASELINE`], as a harness whose view is a
/// [`TeardownView`] registers it.
pub fn teardown_returns_baseline<S>() -> Property<S>
where
    S: HasView,
    S::View: TeardownView,
{
    Property {
        id: PropertyId::new(TEARDOWN_RETURNS_BASELINE),
        claim: "a released header round returns its reservation and leases once its calls are \
                handled, and a stopped node's teardown returns every chunk-budget charge and \
                every locator permit no pending storage read still holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_teardown::<S>,
        witness: Some(TriggerHistory::View(teardown_reached::<S::View>)),
    }
}

/// Checks [`TEARDOWN_RETURNS_BASELINE`] at the boundary after a step, at two
/// levels.
///
/// Round settlement: once the queue released a round, its peer holds no
/// reservation from it, every lease staged for it returns once no call
/// holds it, by its last call's `Handled` at the latest, and every call that
/// held its headers ended that ownership by its `Handled`. The ledger checks
/// those at the events, as `LeakedReservation`, `LeakedLease` and
/// `OpenOwnership`.
///
/// Node settlement: once a reactor the harness stopped is gone, without
/// unwinding, and every call it dropped has its drop receipt, it holds no
/// reservation, ownership or staged lease (`OpenReservation`,
/// `OpenOwnership`, `LeakedLease` in the ledger's teardown check), and the
/// budget holds nothing and every locator permit is free but those storage
/// still holds for pending reads (`TeardownReserved`, `TeardownOwned`,
/// `TeardownLocatorPermits` on the probe reads once it's gone). A reactor
/// that exits by itself, without unwinding, gets the same probe reads and
/// claims, though only a stopped one settles the node.
///
/// Any of those breaks the step's drain found violates. It applies once the
/// queue released a round or the node settled. A run that ends with a
/// released round unsettled stays pending while storage still holds a
/// call with that round's headers or the drain was cut off at its bound,
/// since that call could still settle it. A run that ends at rest, idle or
/// with its drain run dry, with no such call pending can never settle it,
/// so [`teardown_end_violations`] violates it then. A run that ends with a
/// stop begun and the node unsettled stays pending only when its drain
/// was cut off at its bound. Otherwise the tap fails it as
/// `ShutdownIncomplete`. A drain cut off at its bound ends the run as the
/// harness failure `DrainBound`, which keeps only the violations steps
/// found, so a mark still pending there leaves the property
/// `HarnessFailed`. The node half of the claim is about a stopped
/// node, so a run that never stops the node holds once its released rounds
/// settle. Today no committed input can stop the node, so every planted
/// round-level case relies on that. A step with a released round a claim
/// tainted isn't judged, so it never counts as holding.
pub(crate) fn check_teardown<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: TeardownView,
{
    let view = cx.view();
    let step = cx.step;
    let row = view.rounds().at(step);
    if row.is_some_and(|row| row.unsettled > 0 || (row.shutdown && !row.settled)) {
        cx.pending(Pending::TeardownOpen);
    }
    let expected = "a released round, and a stopped node, keeps no reservation, lease or held \
                    headers once settled, and no locator permit a pending storage read doesn't \
                    hold";
    let mut breaks = view.claims().at(step, &TEARDOWN_CLAIMS).into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    // A tainted round's settlement isn't judged, so the step doesn't pass.
    if row.is_some_and(|row| row.tainted == 0 && (row.released > 0 || row.settled)) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// A stale prepare or apply completion's own handling never charges,
/// releases or credits another owner's work, and never starts or
/// dispatches work.
pub const STALE_COMPLETION_INERT: &str = "header-sync/stale-completion-inert";

/// What the stale-completion check reads: one reactor's claim breaks and
/// the stale calls its tap found.
pub trait StaleView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// The reactor's stale prepare and apply calls.
    fn stale_calls(&self) -> &StaleEvidence;
}

/// [`STALE_COMPLETION_INERT`], as a harness whose view is a [`StaleView`]
/// registers it.
pub fn stale_completion_inert<S>() -> Property<S>
where
    S: HasView,
    S::View: StaleView,
{
    Property {
        id: PropertyId::new(STALE_COMPLETION_INERT),
        claim: "a stale header prepare or apply completion's own handling never charges, \
                releases or credits another owner's work, and never starts or dispatches work",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_stale_completion::<S>,
        witness: Some(TriggerHistory::View(stale_ended::<S::View>)),
    }
}

/// Checks [`STALE_COMPLETION_INERT`] at the boundary after a step.
///
/// The tap's stale-completion check
/// ([`crate::harness::header_node::stale`]) finds the stale calls and
/// judges each fact their handlers report, by the cause the reactor
/// records with it. A forbidden effect the step's drain found violates at
/// once. It applies once a drain found a stale call. Whether a maintenance
/// decision in the same turn was right is the maintenance properties' to
/// judge, so facts under any other cause aren't.
fn check_stale_completion<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: StaleView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "a stale completion's handler returns only its own round's budget";
    let mut breaks = view
        .claims()
        .at(step, &[Claim::StaleCompletionEffect])
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if view.stale_calls().found_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// A reset the reactor adopts invalidates its work: no round started under
/// the old generation outlives the unit of work that adopts the new one,
/// every live round's dispatch carries the adopted generation, every reset
/// advances the generation, and every round starts from a locator read of
/// its own generation.
pub const RESET_INVALIDATES_WORK: &str = "header-sync/reset-invalidates-work";

/// What the reset check reads: one reactor's claim breaks and when the
/// check first applied.
pub trait ResetView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// When the reactor's reset check first applied.
    fn round_life(&self) -> &RoundLifeEvidence;
}

/// [`RESET_INVALIDATES_WORK`], as a harness whose view is a [`ResetView`]
/// registers it.
pub fn reset_invalidates_work<S>() -> Property<S>
where
    S: HasView,
    S::View: ResetView,
{
    Property {
        id: PropertyId::new(RESET_INVALIDATES_WORK),
        claim: "a header round started under the adopted generation is retired by the end of \
                the unit of work that adopts a newer one, a live round's prepare or apply \
                carries the adopted generation, every reset and every apply that inserts \
                headers advances the header generation, every round starts from a locator read \
                of its own generation, and every locator read gives the engine's selected path \
                as its durable state then holds it",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_reset_invalidates_work::<S>,
        witness: Some(TriggerHistory::View(reset_applied::<S::View>)),
    }
}

/// Checks [`RESET_INVALIDATES_WORK`] at the boundary after a step.
///
/// The tap's reset check ([`crate::harness::header_node::round_life`])
/// judges the reactor's facts and the node's storage facts as they arrive,
/// and the ownership ledger reports an obsolete round retired before the
/// queue released it. A break the step's drain found violates at once. It
/// applies once a drain found an adoption that changed the generation under
/// a live round, an ordinary start, or a reset.
fn check_reset_invalidates_work<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: ResetView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "the reactor's work follows every reset it adopts";
    let mut breaks = view
        .claims()
        .at(
            step,
            &[Claim::ResetInvalidatesWork, Claim::UnreleasedObsoleteRound],
        )
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if view.round_life().applicable_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`RESET_INVALIDATES_WORK`]'s trigger history: the check applied.
fn reset_applied<V: ResetView>(view: &V, _: &ObservationLog) -> bool {
    view.round_life().any()
}

/// `header-sync/outstanding-work-bounded`: the reactor's growable
/// containers stay within their bounds and keep only live keys.
pub const OUTSTANDING_WORK_BOUNDED: &str = "header-sync/outstanding-work-bounded";

/// A view that keeps the reactor's container peaks, for
/// [`OUTSTANDING_WORK_BOUNDED`].
pub trait OutstandingView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// Each container's peak, and when the rule first applied.
    fn outstanding(&self) -> &OutstandingEvidence;
}

/// [`OUTSTANDING_WORK_BOUNDED`], as a harness whose view is an
/// [`OutstandingView`] registers it.
pub fn outstanding_work_bounded<S>() -> Property<S>
where
    S: HasView,
    S::View: OutstandingView,
{
    Property {
        id: PropertyId::new(OUTSTANDING_WORK_BOUNDED),
        claim: "at the end of each unit of work the reactor's slots, reservations, staged \
                entries, deadlines, closes, locator reads, storage calls, port futures, cached \
                offers, supplier order, strikes and cooldowns stay within their production \
                bounds, and every key kept for a peer or call names one that still has what \
                the entry is for; the serving side's publishers, subscriber cursors, \
                publication containers, serving budgets, serving peer budgets and retained \
                paths stay within theirs and match their owners' facts, and no publisher \
                outlives its session's cancellation",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_outstanding_work_bounded::<S>,
        witness: Some(TriggerHistory::View(outstanding_applied::<S::View>)),
    }
}

/// Checks [`OUTSTANDING_WORK_BOUNDED`] at the boundary after a step: it's
/// violated by an `OutstandingWork` claim break the tap found at this
/// step, or, after a step that leaves the scheduler at rest
/// ([`crate::at_rest`]), by a publisher still running under its session's
/// cancelled token, and holds once a drain judged a `Sizes`.
fn check_outstanding_work_bounded<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: OutstandingView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "the reactor's and the serving side's containers stay within their bounds \
                    and keep only live keys, and no publisher outlives its session";
    let mut breaks: Vec<String> = view
        .claims()
        .at(step, &[Claim::OutstandingWork])
        .into_iter()
        .map(|record| record.detail)
        .collect();
    // Not quiescence: a source action, such as the drain finishing a read
    // the publisher waits on, can still end a publisher that ignored its
    // cancellation, and hide it.
    if crate::at_rest(cx.status) {
        let lingering = view.outstanding().lingering_cancelled();
        if !lingering.is_empty() {
            breaks.push(format!(
                "OutstandingWork: nothing can run after step {step}, yet publishers still run \
                 for the cancelled sessions {lingering:?}"
            ));
        }
    }
    let mut breaks = breaks.into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other);
        }
        return cx.violated(expected, first);
    }
    if view.outstanding().applicable_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`OUTSTANDING_WORK_BOUNDED`]'s trigger history: the rule judged a
/// `Sizes`.
fn outstanding_applied<V: OutstandingView>(view: &V, _: &ObservationLog) -> bool {
    view.outstanding().any()
}

/// A stale apply never changes the node's engine, and the reactor's
/// adoption never moves back.
pub const STALE_CHAIN_EFFECT_INERT: &str = "header-sync/stale-chain-effect-inert";

/// What the stale-chain check reads: one reactor's claim breaks and when
/// the check first applied.
pub trait ChainView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// When the reactor's stale-chain check first applied.
    fn chain_effect(&self) -> &ChainEffectEvidence;
}

/// [`STALE_CHAIN_EFFECT_INERT`], as a harness whose view is a [`ChainView`]
/// registers it.
pub fn stale_chain_effect_inert<S>() -> Property<S>
where
    S: HasView,
    S::View: ChainView,
{
    Property {
        id: PropertyId::new(STALE_CHAIN_EFFECT_INERT),
        claim: "an apply changes the node's header engine only under the engine's own \
                generation and anchor, and the reactor's adopted state version and header \
                generation never decrease",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_stale_chain_effect::<S>,
        witness: Some(TriggerHistory::View(chain_applied::<S::View>)),
    }
}

/// Checks [`STALE_CHAIN_EFFECT_INERT`] at the boundary after a step.
///
/// The tap's stale-chain check ([`crate::harness::header_node::chain_effect`])
/// judges each apply's execution against the engine just before it, and
/// each adoption against the one before it. A break the step's drain found
/// violates at once. It applies once a drain found an apply that changed
/// the engine or a second adoption.
fn check_stale_chain_effect<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: ChainView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "no stale apply changes the engine, and adoption never moves back";
    let mut breaks = view
        .claims()
        .at(step, &[Claim::StaleChainEffect])
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if view.chain_effect().applicable_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`STALE_CHAIN_EFFECT_INERT`]'s trigger history: the check applied.
fn chain_applied<V: ChainView>(view: &V, _: &ObservationLog) -> bool {
    view.chain_effect().any()
}

/// The node's engine selects a max-work eligible tip of the headers it
/// holds, and scores it with its work above the anchor. In the fork
/// fixture, a settled node is on the heaviest mirror chain.
pub const EXPECTED_CHAIN_SELECTED: &str = "header-sync/expected-chain-selected";

/// What the fork-choice check reads: one reactor's claim breaks and when
/// the check first applied.
pub trait ForkView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// When the node's fork-choice check first applied.
    fn fork_choice(&self) -> &ForkChoiceEvidence;
}

/// [`EXPECTED_CHAIN_SELECTED`], as a harness whose view is a [`ForkView`]
/// registers it.
pub fn expected_chain_selected<S>() -> Property<S>
where
    S: HasView,
    S::View: ForkView,
{
    Property {
        id: PropertyId::new(EXPECTED_CHAIN_SELECTED),
        claim: "at every engine snapshot record, the node's engine selects a max-work eligible \
                tip of the headers it holds, by the fixture's work and the resets in force, \
                with any max-work tip allowed on a tie, and scores it with that tip's work \
                above the anchor; and in the fork fixture, at every settled point, the \
                engine is on a max-work tip over all mirrors' chains",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_expected_chain_selected::<S>,
        witness: Some(TriggerHistory::View(fork_choice_applied::<S::View>)),
    }
}

/// Checks [`EXPECTED_CHAIN_SELECTED`] at the boundary after a step.
///
/// The tap's fork-choice check
/// ([`crate::harness::header_node::fork_choice`]) judges the engine before
/// the run and the record each apply's, reset's and harness commit's engine
/// run left. A break the step's drain found violates at once. In the fork
/// fixture, a step after which no task can run, whose drain found every
/// other settled-point condition, is a settled point, and the engine's tip
/// there must be a heaviest mirror tip ([`settled`]). It applies once a
/// drain judged a record, until a drain found a run corrupt.
///
/// [`settled`]: crate::harness::header_node::fork_choice::settled
fn check_expected_chain_selected<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: ForkView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "the engine selects a max-work eligible tip, scored with its work";
    let mut breaks = view
        .claims()
        .at(step, &[Claim::ExpectedChainSelected])
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    // A corrupt engine state is `commit-only-authorized`'s, and nothing
    // after it is judged here, so no later step counts as passed.
    if view.fork_choice().tainted_by(step) {
        return Verdict::NotApplicable;
    }
    // A settled point needs no task able to run after the step, which only
    // the scheduler's status says.
    let at_rest = crate::at_rest(cx.status);
    if let (true, Some(verdict)) = (at_rest, view.fork_choice().settled_at(step)) {
        if let Some(detail) = verdict {
            return cx.violated(
                "a settled node in the fork fixture is on the heaviest mirror chain",
                detail,
            );
        }
        return Verdict::Holds;
    }
    if view.fork_choice().applicable_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`EXPECTED_CHAIN_SELECTED`]'s trigger history: the check judged a
/// record.
fn fork_choice_applied<V: ForkView>(view: &V, _: &ObservationLog) -> bool {
    view.fork_choice().any()
}

/// The node stages, prepares and commits only headers it asked for, and
/// credits only what it reserved: each round's owner is bound to the `Open`
/// the node built for it, each staged page belongs to its round's session
/// and subscription and lies on the requested path, every credit fact,
/// update and acceptance carries its subscription's state faithfully, and
/// each prepare and apply hands on its round's work and reaches storage as
/// dispatched.
pub const COMMIT_ONLY_AUTHORIZED: &str = "header-sync/commit-only-authorized";

/// The claim rules [`COMMIT_ONLY_AUTHORIZED`] reports.
const AUTHORITY_CLAIMS: [Claim; 13] = [
    Claim::CommitOnlyAuthorized,
    Claim::ForeignPage,
    Claim::UnstagedPage,
    Claim::OverReservation,
    Claim::WrongOwner,
    Claim::WrongPayload,
    Claim::BadHandoff,
    Claim::WrongRound,
    Claim::WrongCall,
    Claim::ForeignLease,
    Claim::HeldUnowned,
    Claim::UnstagedHeaders,
    Claim::WrongSource,
];

/// What the authority check reads: one reactor's claim breaks and when the
/// check first applied.
pub trait AuthorityView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// When the reactor's authority check first applied.
    fn authority(&self) -> &AuthorityEvidence;
}

/// [`COMMIT_ONLY_AUTHORIZED`], as a harness whose view is an
/// [`AuthorityView`] registers it.
pub fn commit_only_authorized<S>() -> Property<S>
where
    S: HasView,
    S::View: AuthorityView,
{
    Property {
        id: PropertyId::new(COMMIT_ONLY_AUTHORIZED),
        claim: "each header round's owner is bound to the Open the node built for it, the node \
                admits each session under its link's identity, each staged page belongs to its \
                round's session and subscription and lies on the path it asked for, and the \
                subscriber's credit, updates and acceptances stay within what the node reserved \
                and carry each subscription's state faithfully, and each prepare and apply hands \
                on its round's staged work from its peer's source and reaches storage exactly as \
                dispatched, and each apply or reset changes the engine's durable state only by \
                its own submitted headers and the resets the port made, and each locator read \
                leaves it unchanged",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_commit_only_authorized::<S>,
        witness: Some(TriggerHistory::View(authority_applied::<S::View>)),
    }
}

/// Checks [`COMMIT_ONLY_AUTHORIZED`] at the boundary after a step.
///
/// The tap's authority check ([`crate::harness::header_node::authority`])
/// judges each `Started` and `PageStaged` and, in its credit half, each
/// credit fact, built update and acceptance, and in its submission rule
/// each prepare's and apply's request storage received; the ledger reports
/// a foreign or unstaged page, an over-spent reservation, and a handoff to
/// another owner, payload, source, round or apply, and the session view an
/// admission under another identity than its link's. A break the step's
/// drain found violates at once. It applies once a drain found a start, a
/// staged page, a credit fact or an admission's identity join.
fn check_commit_only_authorized<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: AuthorityView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "the node stages, prepares and applies only headers it asked for, under the \
                    owner it bound and as dispatched, and credits only what it reserved, as each \
                    subscription's state says";
    let mut breaks = view.claims().at(step, &AUTHORITY_CLAIMS).into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if view.authority().applicable_by(step) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`COMMIT_ONLY_AUTHORIZED`]'s trigger history: the check applied.
fn authority_applied<V: AuthorityView>(view: &V, _: &ObservationLog) -> bool {
    view.authority().any()
}

/// The node's port publishes every committed engine state as it was, the
/// reactor adopts only engine records, faithfully, and adopts each commit
/// before quiescence, and every `Status` and tip it publishes names its
/// latest adoption, a changed `Status` in time.
pub const STATE_PUBLICATION_CONSISTENT: &str = "header-sync/state-publication-consistent";

/// What the publication check reads: one reactor's claim breaks and when
/// the check first applied.
pub trait PublicationView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// When the node's publication check first applied, the tip it still
    /// owes, and, armed, its stamped facts.
    fn publication(&self) -> &PublicationEvidence;

    /// The harness's environment actions and the reactor's own exit.
    fn ends(&self) -> &EndEvidence;
}

/// [`STATE_PUBLICATION_CONSISTENT`], as a harness whose view is a
/// [`PublicationView`] registers it.
pub fn state_publication_consistent<S>() -> Property<S>
where
    S: HasView,
    S::View: PublicationView,
{
    Property {
        id: PropertyId::new(STATE_PUBLICATION_CONSISTENT),
        claim: "the node's port publishes every committed engine state on its selected-chain \
                watch as committed, in the step that commits it; the reactor adopts only \
                states some engine record holds, equal to that record, and adopts each \
                commit before quiescence unless the harness began stopping it; and every \
                Status it queues and tip it publishes names its latest adoption, and in a \
                status fixture a changed Status is queued within the deadline",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_state_publication::<S>,
        witness: Some(TriggerHistory::View(publication_applied::<S::View>)),
    }
}

/// Checks [`STATE_PUBLICATION_CONSISTENT`] at the boundary after a step.
///
/// The tap's publication check
/// ([`crate::harness::header_node::publication`]) judges each engine run's
/// publication, each adoption against the engine's records, and each queued
/// `Status` and tip publication against the latest adoption. A break the
/// step's drain found violates at once. A tip still owed at a settled end
/// is the harness's end violation ([`publication_end_violations`]), and
/// one owed at any other end leaves the property `Unsettled` at
/// [`Pending::TipUnpublished`]. It applies once a drain
/// judged an adoption or a commit's publication, until a drain found a run
/// corrupt, which is `commit-only-authorized`'s.
///
/// Where the harness armed the evidence, two rules oblige the node too
/// ([`crate::harness::header_node::publication`] lists the facts):
///
/// - **Adoption happens.** Each commit obliges an adoption of its state
///   version or a later one before quiescence, so watch coalescing is
///   fine. A commit once the harness's `Shutdown` step began, or one an
///   earlier adoption already covers, opens nothing, and the step answers
///   every open one (Rule 13). A reactor that exits by itself answers
///   nothing.
/// - **A changed status is queued in time.** In a status fixture, each
///   session's debt obliges a queued `Status` advertising the newest
///   adoption within [`STATUS_DEADLINE`] of the debt. The harness closing
///   or relinking the session's link, or its `Shutdown` step, answers it,
///   and a debt opened after such a step opens nothing
///   ([`session_left`]), since the node can still adopt or admit before
///   it reads the close or its reactor stops. The node ending the session answers nothing.
///
/// A taint answers every obligation, since a corrupt run's consequences
/// are its owner's.
fn check_state_publication<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: PublicationView,
{
    let view = cx.view();
    let step = cx.step;
    let expected =
        "committed states are published and adopted as recorded, and advertised as adopted";
    let mut breaks = view
        .claims()
        .at(step, &[Claim::StatePublicationConsistent])
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    let publication = view.publication();
    let shutdown = shutdown_stamp(view.ends());
    for stamp in cx.new_stamps().to_vec() {
        let (subject, bound) = match publication.fact(stamp) {
            Some(PublicationFact::Committed { state_version, .. }) => {
                let covered = publication
                    .adopted_before(stamp)
                    .is_some_and(|adopted| adopted >= state_version);
                if covered || shutdown.is_some_and(|shutdown| shutdown < stamp) {
                    continue;
                }
                (adoption_subject(state_version), None)
            }
            Some(PublicationFact::StatusOwed { session_id, .. }) => {
                if session_left(&view, session_id, stamp) {
                    continue;
                }
                (
                    Subject::Session(session_id.to_string()),
                    Some(LivenessBound::VirtualTime(STATUS_DEADLINE)),
                )
            }
            _ => continue,
        };
        let mut obligation =
            cx.fact_obligation(Some(stamp), subject, publication_answer::<S::View>);
        if let Some(bound) = bound {
            obligation.bound = bound;
        }
        // One adoption covers every commit up to its version, and a
        // shutdown or a taint answers every obligation.
        obligation.cardinality = Cardinality::Every;
        cx.oblige(obligation);
    }
    if publication.tainted_by(step) || !publication.applicable_by(step) {
        return Verdict::NotApplicable;
    }
    // The adoption and its tip's publication are one synchronous call with
    // a preemption point between, so a run cut off there hasn't shown it.
    // A settled end judges it ([`PublicationEvidence::unpublished_at_settled_end`]).
    if publication.owed().is_some() {
        cx.pending(Pending::TipUnpublished);
    }
    Verdict::Holds
}

/// The prefix of an adoption obligation's subject, before the committed
/// state version it waits on.
const ADOPTION_OWNER: &str = "adoption-of-state-version-";

/// The subject of the obligation to adopt `state_version` or a later one.
fn adoption_subject(state_version: u64) -> Subject {
    Subject::Owner(format!("{ADOPTION_OWNER}{state_version}"))
}

/// Whether the harness's environment `fact` ended session `session_id`:
/// a `Close` of its connection (a session's ID is its link's connection),
/// a `Relink` of its peer to a newer connection, or a `Shutdown`, which
/// closes every link.
fn ends_session(publication: &PublicationEvidence, fact: &EndFact, session_id: u64) -> bool {
    match fact {
        EndFact::Environment {
            action: EnvironmentAction::Shutdown,
            ..
        } => true,
        EndFact::Environment {
            action: EnvironmentAction::Close,
            connection,
            ..
        } => *connection == Some(session_id),
        EndFact::Environment {
            peer,
            action: EnvironmentAction::Relink,
            connection,
        } => {
            connection.is_some_and(|newer| newer > session_id)
                && publication
                    .owed_peer(session_id)
                    .and_then(|owed| publication.peer_name(&owed))
                    .is_some_and(|name| name == *peer)
        }
        _ => false,
    }
}

/// Whether the harness ended session `session_id` ([`ends_session`]) before
/// the fact with `stamp`, so a debt opened then is on a session the harness
/// already ended, which the node may still adopt into or admit before it
/// reads the close. A relinked peer's new session is a newer connection, so
/// it still owes.
fn session_left<V: PublicationView>(view: &V, session_id: u64, stamp: u64) -> bool {
    let publication = view.publication();
    view.ends()
        .facts()
        .into_iter()
        .any(|(at, fact)| at < stamp && ends_session(publication, &fact, session_id))
}

/// What the fact with `stamp` means for the obligation `key` names, as
/// [`check_state_publication`] describes.
fn publication_answer<V: PublicationView>(key: &ObligationKey, view: &V, stamp: u64) -> FactAnswer {
    let publication = view.publication();
    let owed = match &key.subject {
        Subject::Owner(owner) => owner
            .strip_prefix(ADOPTION_OWNER)
            .and_then(|version| version.parse::<u64>().ok())
            .map(Ok),
        Subject::Session(session) => session.parse::<u64>().ok().map(Err),
        _ => None,
    };
    let Some(owed) = owed else {
        return FactAnswer::Unrelated;
    };
    match (publication.fact(stamp), owed) {
        (Some(PublicationFact::Tainted), _) => return FactAnswer::Discharges,
        (Some(PublicationFact::Adopted { state_version }), Ok(committed))
            if state_version >= committed =>
        {
            return FactAnswer::Discharges
        }
        (Some(PublicationFact::StatusPaid { session_id }), Err(session))
            if session_id == session =>
        {
            return FactAnswer::Discharges
        }
        (Some(_), _) => return FactAnswer::Unrelated,
        (None, _) => {}
    }
    let ended = view.ends().fact(stamp).is_some_and(|fact| match owed {
        // A shutdown ends every adoption obligation, and nothing else does.
        Ok(_) => matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        ),
        Err(session) => ends_session(publication, &fact, session),
    });
    if ended {
        FactAnswer::Discharges
    } else {
        FactAnswer::Unrelated
    }
}

/// [`STATE_PUBLICATION_CONSISTENT`]'s trigger history: the check judged an
/// adoption or a commit's publication.
fn publication_applied<V: PublicationView>(view: &V, _: &ObservationLog) -> bool {
    view.publication().any()
}

/// [`STATE_PUBLICATION_CONSISTENT`]'s end violation: a settled run ended
/// with an adopted tip never published on the reactor's tip watch, though
/// no task was left partway through publishing it (hdr-4's
/// `UnpublishedTip`). A harness whose view is a [`PublicationView`] reports
/// it from [`HasView::end_violations`], after it handed the run's end to
/// [`PublicationEvidence::end`].
pub fn publication_end_violations(publication: &PublicationEvidence) -> Vec<EndViolation> {
    publication
        .unpublished_at_settled_end()
        .map(|tip| EndViolation {
            property: PropertyId::new(STATE_PUBLICATION_CONSISTENT),
            expected: "every adopted tip is published before the run comes to rest".to_owned(),
            actual: format!(
                "UnpublishedTip: the run came to rest with {tip:?} adopted and never published"
            ),
        })
        .into_iter()
        .collect()
}

/// In the wake fixture, a budget return that leaves room for an offer
/// waiting for header room gets that offer's locator read to storage
/// before the reactor goes quiet.
pub const CAPACITY_RELEASE_WAKES: &str = "header-sync/capacity-release-wakes";

/// What the release-wake check reads: one reactor's wake facts.
pub trait WakeView {
    /// The reactor's wake facts.
    fn wake(&self) -> &WakeEvidence;
}

/// [`CAPACITY_RELEASE_WAKES`], as a harness whose view is a [`WakeView`]
/// registers it. It applies only where the harness declared the wake
/// fixture ([`crate::harness::header_node::wake`]).
pub fn capacity_release_wakes<S>() -> Property<S>
where
    S: HasView,
    S::View: WakeView,
{
    Property {
        id: PropertyId::new(CAPACITY_RELEASE_WAKES),
        claim: "in the wake fixture, a header budget return that leaves room for an offer \
                waiting for it gets that offer's locator read to storage before quiescence, \
                or within one cached-pass refill when the pass is deferred, unless the \
                environment ends its session",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_release_wake::<S>,
        witness: Some(TriggerHistory::View(wake_opened::<S::View>)),
    }
}

/// Checks [`CAPACITY_RELEASE_WAKES`] at the boundary after a step.
///
/// Each wake the step's facts opened ([`WakeFact::Opened`]) obliges the
/// waiting peer's locator read to reach storage before quiescence. The
/// read's submission answers it, and so does the waiting session's end when
/// the environment ended it ([`EndedBy`](crate::property::EndedBy)). A
/// deferred cached pass moves its deadline to the pass's refill, but never
/// past [`CACHED_PASS_REFILL`] after the wake opened. Otherwise a budget that
/// never refills would keep deferring it. It applies once a release opened
/// a wake.
fn check_release_wake<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: WakeView,
{
    let view = cx.view();
    for stamp in cx.new_stamps().to_vec() {
        if let Some(WakeFact::Opened {
            peer, session_id, ..
        }) = view.wake().fact(stamp)
        {
            let subject = Subject::Session(wake_subject(&peer, session_id));
            let obligation = cx.fact_obligation(Some(stamp), subject, wake_answer::<S::View>);
            cx.oblige(obligation);
        }
    }
    if view.wake().any_opened() {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// The obligation subject of `peer`'s session `session_id`'s wake.
fn wake_subject(peer: &zakura_network::zakura::ZakuraPeerId, session_id: u64) -> String {
    format!("{}#{session_id}", crate::storage::hex(&peer.digest()))
}

/// What the wake fact with `stamp` means for the wake `key` names, as
/// [`check_release_wake`] describes.
fn wake_answer<V: WakeView>(key: &ObligationKey, view: &V, stamp: u64) -> FactAnswer {
    let Subject::Session(subject) = &key.subject else {
        return FactAnswer::Unrelated;
    };
    match view.wake().fact(stamp) {
        Some(WakeFact::Submitted { peer })
            if subject
                .strip_prefix(&crate::storage::hex(&peer.digest()))
                .is_some_and(|rest| rest.starts_with('#')) =>
        {
            FactAnswer::Discharges
        }
        Some(WakeFact::Ended {
            peer,
            session_id,
            by,
        }) if *subject == wake_subject(&peer, session_id) => by.answer(),
        Some(WakeFact::Deferred { until }) => {
            let cap = opened_at(view.wake(), subject, stamp)
                .and_then(|opened| opened.checked_add(CACHED_PASS_REFILL));
            FactAnswer::DefersTo(cap.map_or(until, |cap| until.min(cap)))
        }
        _ => FactAnswer::Unrelated,
    }
}

/// When the last wake for `subject` before `stamp` opened.
fn opened_at(wake: &WakeEvidence, subject: &str, stamp: u64) -> Option<SimInstant> {
    wake.facts()
        .into_iter()
        .rev()
        .filter(|(at, _)| *at < stamp)
        .find_map(|(_, fact)| match fact {
            WakeFact::Opened {
                peer,
                session_id,
                at,
            } if wake_subject(&peer, session_id) == subject => Some(at),
            _ => None,
        })
}

/// [`CAPACITY_RELEASE_WAKES`]'s trigger history: a release opened a wake.
fn wake_opened<V: WakeView>(view: &V, _: &ObservationLog) -> bool {
    view.wake().any_opened()
}

/// A held storage operation whose reply reached the reactor's waiting call
/// is handled before the reactor goes quiet, and a held serving read's page
/// is handed to the session's writer before the run goes quiet.
pub const HOLD_RELEASE_WAKES: &str = "header-sync/hold-release-wakes";

/// What the hold-release check reads: one node's hold-wake facts and its
/// end facts.
pub trait HoldWakeView {
    /// The node's hold-wake facts.
    fn hold_wake(&self) -> &HoldWakeEvidence;

    /// The harness's environment actions and the reactor's own exit.
    fn ends(&self) -> &EndEvidence;
}

/// [`HOLD_RELEASE_WAKES`], as a harness whose view is a [`HoldWakeView`]
/// registers it. It applies only where a hold picked a reactor call's
/// operation and its reply was delivered, or picked a serving read that
/// completed with a page ([`crate::harness::header_node::hold_wake`]).
pub fn hold_release_wakes<S>() -> Property<S>
where
    S: HasView,
    S::View: HoldWakeView,
{
    Property {
        id: PropertyId::new(HOLD_RELEASE_WAKES),
        claim: "once a held storage operation's reply reaches the reactor's waiting call, \
                whatever its result, the reactor handles that call before quiescence, and once \
                a held serving read's page reaches its page job, the session's publisher hands \
                a page to its writer before quiescence, unless the harness closed or relinked \
                that mirror or began stopping the node",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_hold_release_wake::<S>,
        witness: Some(TriggerHistory::View(hold_wake_opened::<S::View>)),
    }
}

/// Checks [`HOLD_RELEASE_WAKES`] at the boundary after a step.
///
/// Each held reply the step delivered ([`HoldWakeFact::Opened`]) obliges
/// the reactor to handle its call before quiescence. The call's handling
/// answers it ([`HoldWakeFact::Handled`]), and so does the harness's
/// `Shutdown` step, the only environment action that stops the reactor
/// ([`EnvironmentAction::Shutdown`]). Each held serving read the step
/// served ([`HoldWakeFact::Served`]) obliges the session's publisher to
/// queue a page before quiescence ([`HoldWakeFact::Queued`]). The harness
/// closing or relinking that mirror answers it too, and so does the
/// `Shutdown` step, but the node's own end of the session doesn't, and
/// neither does an outcome the publisher picks for itself (Rule 13). A
/// fact made once the shutdown had begun opens nothing. A reactor or
/// publisher that ends by itself answers nothing, so the obligation expires
/// at the next quiescence. It applies once either kind opened before any
/// shutdown began.
fn check_hold_release_wake<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: HoldWakeView,
{
    let view = cx.view();
    let shutdown = shutdown_stamp(view.ends());
    for stamp in cx.new_stamps().to_vec() {
        if let Some(HoldWakeFact::Opened { op, .. } | HoldWakeFact::Served { op, .. }) =
            view.hold_wake().fact(stamp)
        {
            if shutdown.is_some_and(|shutdown| shutdown < stamp) {
                continue;
            }
            let mut obligation = cx.fact_obligation(
                Some(stamp),
                Subject::Operation(op),
                hold_wake_answer::<S::View>,
            );
            // One shutdown ends every reply still waiting. A `Handled` or
            // `Queued` names one operation, so it still answers only its own.
            obligation.cardinality = Cardinality::Every;
            cx.oblige(obligation);
        }
    }
    if hold_wake_obliged(&view) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// The stamp of the harness's `Shutdown` step, if it began stopping the
/// node.
fn shutdown_stamp(ends: &EndEvidence) -> Option<u64> {
    ends.facts().into_iter().find_map(|(at, fact)| {
        matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        )
        .then_some(at)
    })
}

/// Whether a held reply reached the reactor, or a held serving read was
/// served, before any shutdown began, so [`check_hold_release_wake`]
/// obliged its answer.
fn hold_wake_obliged<V: HoldWakeView>(view: &V) -> bool {
    let shutdown = shutdown_stamp(view.ends());
    view.hold_wake().facts().into_iter().any(|(at, fact)| {
        matches!(
            fact,
            HoldWakeFact::Opened { .. } | HoldWakeFact::Served { .. }
        ) && shutdown.is_none_or(|shutdown| at < shutdown)
    })
}

/// What the fact with `stamp` means for the held reply `key` names, as
/// [`check_hold_release_wake`] describes.
fn hold_wake_answer<V: HoldWakeView>(key: &ObligationKey, view: &V, stamp: u64) -> FactAnswer {
    let Subject::Operation(held) = key.subject else {
        return FactAnswer::Unrelated;
    };
    match view.hold_wake().fact(stamp) {
        Some(HoldWakeFact::Handled { op, .. } | HoldWakeFact::Queued { op, .. }) if op == held => {
            return FactAnswer::Discharges
        }
        Some(_) => return FactAnswer::Unrelated,
        None => {}
    }
    match view.ends().fact(stamp) {
        Some(EndFact::Environment {
            action: EnvironmentAction::Shutdown,
            ..
        }) => FactAnswer::Discharges,
        // Only a served read is the mirror's, so only its obligation ends
        // with the mirror's session.
        Some(EndFact::Environment {
            peer,
            action: EnvironmentAction::Close | EnvironmentAction::Relink,
            ..
        }) if view.hold_wake().served_peer(held).as_ref() == Some(&peer) => FactAnswer::Discharges,
        _ => FactAnswer::Unrelated,
    }
}

/// [`HOLD_RELEASE_WAKES`]'s trigger history: a held reply reached the
/// reactor before any shutdown began.
fn hold_wake_opened<V: HoldWakeView>(view: &V, _: &ObservationLog) -> bool {
    hold_wake_obliged(view)
}

/// No work outlives the session it belongs to: once a session ends or is
/// replaced, no slot names it past that unit of work, unless it's a body
/// repair kept for its call in flight, and no round starts and no apply is
/// dispatched for it, unless it's that repair's one apply. In the departure
/// and timeout fixtures, once a slot goes, the one other peer that holds its
/// target gets a locator read for it to storage before quiescence, and a
/// round past its request deadline is handed off within the request
/// timeout and one cached-pass refill.
pub const PEER_RETIREMENT_PROGRESS: &str = "header-sync/peer-retirement-progress";

/// The reactor's request timeout, restated: a round past this long since
/// its deadline last armed is retired. `header_peer_retirement`'s tests pin
/// it to the startup the harness's nodes run.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// What the retirement check reads: one reactor's claim breaks and when the
/// check first applied.
pub trait RetirementView {
    /// The claim rules the reactor's tap found broken.
    fn claims(&self) -> &ClaimEvidence;

    /// The harness's environment actions and the reactor's own exit.
    fn ends(&self) -> &EndEvidence;

    /// When the reactor's departed-work check first applied.
    fn retirement(&self) -> &RetirementEvidence;
}

/// [`PEER_RETIREMENT_PROGRESS`], as a harness whose view is a
/// [`RetirementView`] registers it.
pub fn peer_retirement_progress<S>() -> Property<S>
where
    S: HasView,
    S::View: RetirementView,
{
    Property {
        id: PropertyId::new(PEER_RETIREMENT_PROGRESS),
        claim: "once a session ends or is replaced, no work slot names it past the unit of \
                work it departed in, except a body repair kept until its in-flight calls are \
                handled, and no round starts and no apply is dispatched for it outside a stale \
                call's own handler, except that repair's one apply; and in the departure \
                and timeout fixtures, once a slot goes, another session's locator read for \
                its target reaches storage before quiescence, or within one cached-pass refill \
                when the pass is deferred, unless the node adopted the target or the \
                environment ended the other peer's session; and a round's target is taken up \
                by another session, or adopted, within the request timeout of its deadline's \
                last arm plus one refill",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_peer_retirement::<S>,
        witness: Some(TriggerHistory::View(retirement_applied::<S::View>)),
    }
}

/// Checks [`PEER_RETIREMENT_PROGRESS`] at the boundary after a step.
///
/// The tap's retirement check ([`crate::harness::header_node::retirement`])
/// judges the reactor's facts as they arrive. A departed-work break the
/// step's drain found violates at once. Each hand-off the step's facts
/// opened ([`HandoffFact::Opened`]) obliges another session's locator read
/// for its target to reach storage before quiescence, and each obliging
/// request-deadline arm ([`HandoffFact::Armed`]) obliges, within
/// [`REQUEST_TIMEOUT`] plus one cached-pass refill, the round's next arm or
/// the hand-off of its target, as [`retirement_answer`] says. Once the
/// harness began stopping the node, nothing new opens, and its `Shutdown`
/// answers every open obligation (Rule 13). It applies once a session
/// departed with a slot, or an obligation opened, before that `Shutdown`.
fn check_peer_retirement<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: RetirementView,
{
    let view = cx.view();
    let step = cx.step;
    let shutdown = shutdown_stamp(view.ends());
    for stamp in cx.new_stamps().to_vec() {
        // Once the harness began stopping the node, nothing more is owed.
        if shutdown.is_some_and(|shutdown| shutdown < stamp) {
            continue;
        }
        match view.retirement().fact(stamp) {
            // One read, adoption or shutdown can answer a hand-off and a
            // deadline arm together, so each answers every match. A read
            // names a target and an arm a round, so neither crosses to
            // another obligation's.
            Some(HandoffFact::Opened { target, .. }) => {
                let subject = Subject::Session(crate::storage::hex(&target.0));
                let mut obligation =
                    cx.fact_obligation(Some(stamp), subject, handoff_answer::<S::View>);
                obligation.cardinality = Cardinality::Every;
                cx.oblige(obligation);
            }
            Some(HandoffFact::Armed {
                round,
                arm,
                obliges: true,
                ..
            }) => {
                let subject = Subject::Owner(format!("round {round} arm {arm}"));
                let mut obligation =
                    cx.fact_obligation(Some(stamp), subject, timeout_answer::<S::View>);
                obligation.bound = LivenessBound::VirtualTime(REQUEST_TIMEOUT + CACHED_PASS_REFILL);
                obligation.cardinality = Cardinality::Every;
                cx.oblige(obligation);
            }
            _ => {}
        }
    }
    // A departure the slot rule hasn't judged at a unit boundary yet
    // leaves the run unsettled if it ends here (Rule 2), and a trigger the
    // check couldn't read the fixture for leaves it so wherever it ends
    // (Rule 3).
    if view.retirement().unjudged() {
        cx.pending(Pending::UnjudgedDeparture);
    }
    if view.retirement().unread() {
        cx.pending(Pending::UnreadTrigger);
    }
    let expected = "no work outlives its departed session";
    let mut breaks = view
        .claims()
        .at(
            step,
            &[
                Claim::PeerRetirementProgress,
                Claim::UnreleasedDepartedRound,
            ],
        )
        .into_iter();
    if let Some(first) = breaks.next() {
        for other in breaks {
            cx.record_violation(expected, other.detail);
        }
        return cx.violated(expected, first.detail);
    }
    if view.retirement().applicable_by(step) || view.retirement().opened_before(shutdown) {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// What the fact with `stamp` means for the obligation `opener` opened, a
/// hand-off ([`HandoffFact::Opened`]) or an obliging deadline arm
/// ([`HandoffFact::Armed`]), for a `stamp` after the opener's.
///
/// The harness's `Shutdown`, or any `Environment` fact naming the fixture's
/// taker, discharges either (Rule 13): the taker's readiness relink comes
/// before every obligation. So does a locator read for the target from a
/// session other than the owner's, or the node adopting the target. The
/// round's next arm discharges an arm. A deferred cached pass moves a
/// hand-off's deadline to the pass's refill, but never past
/// [`CACHED_PASS_REFILL`] after it opened.
pub fn retirement_answer(
    opener: &HandoffFact,
    stamp: u64,
    retirement: &RetirementEvidence,
    ends: &EndEvidence,
) -> FactAnswer {
    if let Some(EndFact::Environment { peer, action, .. }) = ends.fact(stamp) {
        let taker = retirement.roles().is_some_and(|roles| roles.taker == peer);
        return if action == EnvironmentAction::Shutdown || taker {
            FactAnswer::Discharges
        } else {
            FactAnswer::Unrelated
        };
    }
    let Some(fact) = retirement.fact(stamp) else {
        return FactAnswer::Unrelated;
    };
    let (target, session) = match opener {
        HandoffFact::Opened {
            target, departed, ..
        } => (target, departed),
        HandoffFact::Armed {
            target, session, ..
        } => (target, session),
        _ => return FactAnswer::Unrelated,
    };
    match (opener, fact) {
        (
            _,
            HandoffFact::Read {
                target: read,
                session: from,
            },
        ) if read == *target && from != *session => FactAnswer::Discharges,
        (_, HandoffFact::Reached { target: reached }) if reached == *target => {
            FactAnswer::Discharges
        }
        (
            HandoffFact::Armed { round, arm, .. },
            HandoffFact::Armed {
                round: rearmed,
                arm: next,
                ..
            },
        ) if rearmed == *round && next > *arm => FactAnswer::Discharges,
        (HandoffFact::Opened { at, .. }, HandoffFact::Deferred { until }) => FactAnswer::DefersTo(
            at.checked_add(CACHED_PASS_REFILL)
                .map_or(until, |cap| until.min(cap)),
        ),
        _ => FactAnswer::Unrelated,
    }
}

/// A [`PEER_RETIREMENT_PROGRESS`] obligation no later fact has answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenRetirement {
    /// The stamp of the fact that opened it.
    pub stamp: u64,
    /// The latest it can conclude: a hand-off one cached-pass refill after
    /// it opened, which a deferred pass can reach and quiescence comes
    /// before, and a deadline arm at its deadline.
    pub deadline: SimInstant,
    /// Whether it's a deadline arm, whose deadline the clock must reach.
    pub timed: bool,
}

/// The obligations [`check_peer_retirement`] opened from `retirement`'s
/// facts that no later fact in `retirement` or `ends` has discharged, by
/// [`retirement_answer`]. The liveness harness ends a run and moves its clock
/// by these, so it and the property can't disagree.
pub fn open_retirement(retirement: &RetirementEvidence, ends: &EndEvidence) -> Vec<OpenRetirement> {
    let shutdown = shutdown_stamp(ends);
    let facts = retirement.facts();
    let mut stamps: Vec<u64> = facts
        .iter()
        .map(|(stamp, _)| *stamp)
        .chain(ends.facts().into_iter().map(|(stamp, _)| stamp))
        .collect();
    stamps.sort_unstable();
    facts
        .iter()
        .filter(|(stamp, _)| shutdown.is_none_or(|shutdown| *stamp < shutdown))
        .filter_map(|(stamp, fact)| {
            let (at, timed) = match fact {
                HandoffFact::Opened { at, .. } => (at.checked_add(CACHED_PASS_REFILL)?, false),
                HandoffFact::Armed {
                    obliges: true, at, ..
                } => (at.checked_add(REQUEST_TIMEOUT + CACHED_PASS_REFILL)?, true),
                _ => return None,
            };
            let answered = stamps.iter().filter(|later| *later > stamp).any(|later| {
                matches!(
                    retirement_answer(fact, *later, retirement, ends),
                    FactAnswer::Discharges
                )
            });
            (!answered).then_some(OpenRetirement {
                stamp: *stamp,
                deadline: at,
                timed,
            })
        })
        .collect()
}

/// What the fact with `stamp` means for the deadline arm `key` names, as
/// [`retirement_answer`] says. Its bound is [`REQUEST_TIMEOUT`] after the
/// arm, plus one cached-pass refill.
fn timeout_answer<V: RetirementView>(key: &ObligationKey, view: &V, stamp: u64) -> FactAnswer {
    let Subject::Owner(subject) = &key.subject else {
        return FactAnswer::Unrelated;
    };
    let Some(opener) = view
        .retirement()
        .facts()
        .into_iter()
        .find_map(|(_, fact)| match fact {
            HandoffFact::Armed {
                round,
                arm,
                obliges: true,
                ..
            } if format!("round {round} arm {arm}") == *subject => Some(fact),
            _ => None,
        })
    else {
        return FactAnswer::Unrelated;
    };
    retirement_answer(&opener, stamp, view.retirement(), view.ends())
}

/// [`PEER_RETIREMENT_PROGRESS`]'s violations at a settled end: the slot
/// rule's breaks for a departure that never reached its unit boundary,
/// since a settled run is judged there (Rule 2).
pub fn retirement_end_violations(retirement: &RetirementEvidence) -> Vec<EndViolation> {
    retirement
        .settled_breaks()
        .into_iter()
        .map(|actual| EndViolation {
            property: PropertyId::new(PEER_RETIREMENT_PROGRESS),
            expected: "no work outlives its departed session".to_owned(),
            actual,
        })
        .collect()
}

/// What the fact with `stamp` means for the hand-off `key` names, the latest
/// before `stamp` for its target, as [`retirement_answer`] says.
fn handoff_answer<V: RetirementView>(key: &ObligationKey, view: &V, stamp: u64) -> FactAnswer {
    let Subject::Session(subject) = &key.subject else {
        return FactAnswer::Unrelated;
    };
    let Some(opener) = view
        .retirement()
        .facts()
        .into_iter()
        .rev()
        .filter(|(at, _)| *at < stamp)
        .find_map(|(_, fact)| match fact {
            HandoffFact::Opened { target, .. } if crate::storage::hex(&target.0) == *subject => {
                Some(fact)
            }
            _ => None,
        })
    else {
        return FactAnswer::Unrelated;
    };
    retirement_answer(&opener, stamp, view.retirement(), view.ends())
}

/// [`PEER_RETIREMENT_PROGRESS`]'s trigger history: a session departed with
/// a slot, or an obligation opened before the harness began stopping the
/// node.
fn retirement_applied<V: RetirementView>(view: &V, _: &ObservationLog) -> bool {
    view.retirement().applicable() || view.retirement().opened_before(shutdown_stamp(view.ends()))
}

/// Under a wire flood, the run loop reads each externally published
/// committed state from its watch before it takes a third wire event.
pub const WATCH_OBSERVED_UNDER_FLOOD: &str = "header-sync/watch-observed-under-flood";

/// Under lifecycle churn, the run loop reads each externally published
/// committed state from its watch before it takes a third lifecycle event.
pub const WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD: &str =
    "header-sync/watch-observed-under-lifecycle-flood";

/// What the flood checks read: one reactor's adoption evidence, which
/// keeps publications, watch reads and taken events in order.
pub trait AdoptionView {
    /// The reactor's adoption evidence.
    fn adoption(&self) -> &AdoptionEvidence;
}

/// [`WATCH_OBSERVED_UNDER_FLOOD`], as a harness whose view is an
/// [`AdoptionView`] registers it.
pub fn watch_observed_under_flood<S>() -> Property<S>
where
    S: HasView,
    S::View: AdoptionView,
{
    watch_observed(
        WATCH_OBSERVED_UNDER_FLOOD,
        "the run loop reads each externally published committed state from its watch \
         before it takes a third wire event",
        check_watch_under_flood::<S>,
        published_under_wire_flood::<S::View>,
    )
}

/// [`WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD`], as a harness whose view is an
/// [`AdoptionView`] registers it.
pub fn watch_observed_under_lifecycle_flood<S>() -> Property<S>
where
    S: HasView,
    S::View: AdoptionView,
{
    watch_observed(
        WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
        "the run loop reads each externally published committed state from its watch \
         before it takes a third lifecycle event",
        check_watch_under_lifecycle_flood::<S>,
        published_under_lifecycle_flood::<S::View>,
    )
}

/// One watch property: `id`, claiming `claim`, checked by `check`, with
/// trigger history `triggered`. A view check that marks its own open
/// obligations, since the registry's bounds can't count taken events.
fn watch_observed<S: HasView>(
    id: &'static str,
    claim: &'static str,
    check: fn(&mut CheckCtx<'_, S>) -> Verdict,
    triggered: fn(&S::View, &ObservationLog) -> bool,
) -> Property<S> {
    Property {
        id: PropertyId::new(id),
        claim,
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check,
        witness: Some(TriggerHistory::View(triggered)),
    }
}

/// Checks [`WATCH_OBSERVED_UNDER_FLOOD`] after each step.
fn check_watch_under_flood<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: AdoptionView,
{
    check_watch_read(cx, EventQueue::Wire)
}

/// Checks [`WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD`] after each step.
fn check_watch_under_lifecycle_flood<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: AdoptionView,
{
    check_watch_read(cx, EventQueue::Lifecycle)
}

/// Checks the watch rule for `queue` over the view's adoption evidence
/// ([`AdoptionEvidence::watch_read`]). It applies once a state is
/// published. A publication still waiting for its read or its third event
/// stays pending ([`Pending::WatchOpen`]) if the run ends there, unless
/// [`watch_end_violations`] breaks it at a settled end.
fn check_watch_read<S>(cx: &mut CheckCtx<'_, S>, queue: EventQueue) -> Verdict
where
    S: HasView,
    S::View: AdoptionView,
{
    match cx.view().adoption().watch_read(queue) {
        WatchRead::Nothing => Verdict::NotApplicable,
        WatchRead::Holds => Verdict::Holds,
        WatchRead::Open => {
            cx.pending(Pending::WatchOpen);
            Verdict::Holds
        }
        WatchRead::Broken(why) => cx.violated(
            "each published state is read from the committed watch before a third event \
             from the flooded queue",
            why,
        ),
    }
}

/// The watch property for the flooded `queue`'s violation at the run's end:
/// a publication still unread when the run settled
/// ([`AdoptionEvidence::unread_at_settled_end`]), unless the harness had started to
/// stop the node (`stopping`). A correct loop with an unread publication
/// always has its watch arm ready, so it can't settle with one. Only a
/// shutdown, which wins the loop's biased `select!`, may leave one unread
/// ([`EndedBy`](crate::property::EndedBy)). A harness whose view is an
/// [`AdoptionView`] reports these from [`HasView::end_violations`], after it
/// handed the run's end to [`AdoptionEvidence::end`].
pub fn watch_end_violations(
    adoption: &AdoptionEvidence,
    queue: EventQueue,
    stopping: bool,
) -> Vec<EndViolation> {
    if stopping || !adoption.unread_at_settled_end(queue) {
        return Vec::new();
    }
    let property = match queue {
        EventQueue::Wire => WATCH_OBSERVED_UNDER_FLOOD,
        EventQueue::Lifecycle => WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
    };
    vec![EndViolation {
        property: PropertyId::new(property),
        expected: "each published state is read from the committed watch before the run \
                   settles"
            .to_owned(),
        actual: format!(
            "the run settled with a published state unread and fewer than {} {queue:?} \
             events taken since, with no shutdown begun",
            crate::harness::header_node::adoption::WATCH_READ_EVENTS + 1
        ),
    }]
}

/// [`WATCH_OBSERVED_UNDER_FLOOD`]'s trigger history: a published state
/// whose wire queue competed with its watch read
/// ([`AdoptionEvidence::published_into_flood`]), or that the run settled
/// with unread ([`AdoptionEvidence::unread_at_settled_end`]).
fn published_under_wire_flood<V: AdoptionView>(view: &V, _: &ObservationLog) -> bool {
    let adoption = view.adoption();
    adoption.published_into_flood(EventQueue::Wire)
        || adoption.unread_at_settled_end(EventQueue::Wire)
}

/// [`WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD`]'s trigger history: a published
/// state whose lifecycle queue competed with its watch read, or that the
/// run settled with unread.
fn published_under_lifecycle_flood<V: AdoptionView>(view: &V, _: &ObservationLog) -> bool {
    let adoption = view.adoption();
    adoption.published_into_flood(EventQueue::Lifecycle)
        || adoption.unread_at_settled_end(EventQueue::Lifecycle)
}

/// [`STALE_COMPLETION_INERT`]'s trigger history: a prepare or apply ended
/// stale.
fn stale_ended<V: StaleView>(view: &V, _: &ObservationLog) -> bool {
    view.stale_calls().any()
}

/// [`TEARDOWN_RETURNS_BASELINE`]'s violation at the run's end: a released
/// round left unsettled when the run came to rest with no call that holds
/// its headers still pending in storage, as [`SettlementEvidence::stuck`]
/// finds. A harness whose view is a [`TeardownView`] reports these from
/// [`HasView::end_violations`], after it handed the run's end to
/// [`SettlementEvidence::end`].
pub fn teardown_end_violations(rounds: &SettlementEvidence) -> Vec<EndViolation> {
    rounds
        .stuck()
        .map(|last| EndViolation {
            property: PropertyId::new(TEARDOWN_RETURNS_BASELINE),
            expected: "a released round settles before the run comes to rest".to_owned(),
            actual: format!(
                "the run came to rest after step {} with {} released rounds unsettled and no \
                 call that holds their headers pending",
                last.step, last.stuck
            ),
        })
        .into_iter()
        .collect()
}

/// [`OUTSTANDING_WORK_BOUNDED`]'s end check: a run that came to rest has
/// no publisher task still running for a session that ended.
pub fn outstanding_end_violations(evidence: &OutstandingEvidence) -> Vec<EndViolation> {
    let lingering = evidence.lingering_at_rest();
    if lingering.is_empty() {
        return Vec::new();
    }
    vec![EndViolation {
        property: PropertyId::new(OUTSTANDING_WORK_BOUNDED),
        expected: "a run at rest runs no publisher for a session that ended".to_owned(),
        actual: format!(
            "OutstandingWork: the run came to rest with publishers still running for the \
             ended sessions {lingering:?}"
        ),
    }]
}

/// [`TEARDOWN_RETURNS_BASELINE`]'s trigger history: the queue released a
/// round, or a stopped node settled.
fn teardown_reached<V: TeardownView>(view: &V, _: &ObservationLog) -> bool {
    view.rounds().any_released() || view.rounds().settled()
}

/// [`CHUNK_BUDGET_BOUND`]'s trigger history: the probe read the budget
/// holding headers.
fn budget_claimed<V: BudgetView>(view: &V, _: &ObservationLog) -> bool {
    view.capacity()
        .readings()
        .iter()
        .any(|reading| reading.reserved.saturating_add(reading.owned) > 0)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, rc::Rc};

    use zakura_network::zakura::observe::{
        HEADER_CACHED_PASS_REFILL, HEADER_CHUNK_BUDGET_CAPACITY,
    };

    use super::{
        chunk_budget_bound, stale_completion_inert, teardown_end_violations,
        teardown_returns_baseline, BudgetView, EndViolation, StaleView, TeardownView,
        CACHED_PASS_REFILL, CHUNK_BUDGET_BOUND, CHUNK_BUDGET_CAPACITY, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    };
    use crate::{
        harness::header_node::{
            capacity::{BoundaryRead, CapacityEvidence, CapacityReading},
            claim::{Claim, ClaimEvidence, ClaimRecord, Settlement, SettlementEvidence},
            stale::StaleEvidence,
        },
        property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
        property::{Pending, Property, Status},
        ActionSource, Boundary, HarnessFailure, Identity, MemorySink, NaReason, ObservationLog,
        Outcome, PlanInput, PropertyOutcome, RunEnd, RunReport, Sim, Violation,
    };

    /// Terra 3: a released round left stuck breaks the teardown rule only at
    /// an end the scheduler settled at, a deadlocked one included, never a
    /// worker-starved one, where the ready task that would settle it never
    /// got to run.
    #[test]
    fn a_stuck_round_breaks_teardown_only_at_a_settled_end() {
        use zakura_scheduler::Status as SchedulerStatus;
        for (rest, breaks) in [
            (SchedulerStatus::Quiescent, true),
            (SchedulerStatus::WaitingOnExternal, true),
            (SchedulerStatus::WaitingOnTime, true),
            (SchedulerStatus::WorkerStarved, false),
            (SchedulerStatus::Deadlock, true),
        ] {
            let rounds = SettlementEvidence::new(&crate::stores::EvidenceStores::default());
            rounds.record(Settlement {
                step: 3,
                released: 1,
                unsettled: 1,
                stuck: 1,
                tainted: 0,
                shutdown: false,
                settled: false,
            });
            rounds.end(&RunEnd {
                outcome: Outcome::Budget,
                steps: 3,
                violation: None,
                drain: Some(crate::end::Drain {
                    steps: 1,
                    exhausted: false,
                }),
                harness: None,
                rest: Some(rest),
            });
            assert_eq!(
                !teardown_end_violations(&rounds).is_empty(),
                breaks,
                "{rest:?}"
            );
        }
    }

    /// The model's capacity and refill interval are production's.
    #[test]
    fn the_model_capacity_is_production_s() {
        assert_eq!(CHUNK_BUDGET_CAPACITY, HEADER_CHUNK_BUDGET_CAPACITY);
        assert_eq!(CACHED_PASS_REFILL, HEADER_CACHED_PASS_REFILL);
    }

    /// A probe's and a ledger's evidence with no tap behind it.
    #[derive(Clone)]
    struct Probe {
        capacity: CapacityEvidence,
        claims: ClaimEvidence,
        rounds: SettlementEvidence,
        stale_calls: StaleEvidence,
    }

    impl Default for Probe {
        /// Empty stores in one handle of their own.
        fn default() -> Self {
            let stores = crate::stores::EvidenceStores::default();
            Probe {
                capacity: CapacityEvidence::new(&stores),
                claims: ClaimEvidence::new(&stores),
                rounds: SettlementEvidence::new(&stores),
                stale_calls: StaleEvidence::new(&stores),
            }
        }
    }

    impl StaleView for Probe {
        fn claims(&self) -> &ClaimEvidence {
            &self.claims
        }

        fn stale_calls(&self) -> &StaleEvidence {
            &self.stale_calls
        }
    }

    impl TeardownView for Probe {
        fn claims(&self) -> &ClaimEvidence {
            &self.claims
        }

        fn rounds(&self) -> &SettlementEvidence {
            &self.rounds
        }
    }

    impl BudgetView for Probe {
        fn capacity(&self) -> &CapacityEvidence {
            &self.capacity
        }

        fn claims(&self) -> &ClaimEvidence {
            &self.claims
        }
    }

    impl HasView for Probe {
        type View = Probe;

        /// A copy sharing the stores.
        fn view(&self) -> Probe {
            self.clone()
        }

        /// Keeps the run's end for the teardown rule.
        fn ended(&self, end: &RunEnd) {
            self.rounds.end(end);
        }

        /// [`teardown_end_violations`].
        fn end_violations(&self, _: &ObservationLog) -> Vec<EndViolation> {
            teardown_end_violations(&self.rounds)
        }
    }

    /// One boundary's scripted evidence: a read of claimed headers, an
    /// unread boundary, the released rounds, a stop and whether the node
    /// settled, or a claim break.
    #[derive(Clone, Copy)]
    enum Scripted {
        Read(usize),
        Unread(BoundaryRead),
        Settlement {
            released: u64,
            unsettled: usize,
            stuck: usize,
        },
        Stop {
            settled: bool,
        },
        /// A released round a claim tainted, unsettled.
        Tainted,
        Broke(Claim),
        Breaks(&'static [Claim]),
    }

    /// Records the next scripted evidence at each boundary, as a tap would.
    struct Boundaries {
        script: VecDeque<Scripted>,
        probe: Probe,
    }

    impl ActionSource for Boundaries {
        fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(Identity, u64)>) {}

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            unreachable!("the script offers no actions");
        }

        /// Records the next scripted read as the boundary after this step,
        /// from step 1, since the run also observes after setup.
        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            let step = log.step();
            if step == 0 {
                return Ok(());
            }
            match self.script.pop_front() {
                Some(Scripted::Read(claimed)) => self.probe.capacity.record(CapacityReading {
                    step,
                    reserved: claimed,
                    owned: 0,
                    locator_permits: 0,
                    compared: false,
                    teardown: false,
                }),
                Some(Scripted::Unread(read)) => self.probe.capacity.unread(step, read),
                Some(Scripted::Settlement {
                    released,
                    unsettled,
                    stuck,
                }) => self.probe.rounds.record(Settlement {
                    step,
                    released,
                    unsettled,
                    stuck,
                    tainted: 0,
                    shutdown: false,
                    settled: false,
                }),
                Some(Scripted::Stop { settled }) => self.probe.rounds.record(Settlement {
                    step,
                    released: 0,
                    unsettled: 0,
                    stuck: 0,
                    tainted: 0,
                    shutdown: true,
                    settled,
                }),
                Some(Scripted::Tainted) => self.probe.rounds.record(Settlement {
                    step,
                    released: 1,
                    unsettled: 1,
                    stuck: 0,
                    tainted: 1,
                    shutdown: false,
                    settled: false,
                }),
                Some(Scripted::Broke(claim)) => {
                    self.probe.claims.record(ClaimRecord {
                        step,
                        claim,
                        detail: format!("{claim:?}"),
                    });
                    self.probe.rounds.record(Settlement {
                        step,
                        released: 1,
                        unsettled: 0,
                        stuck: 0,
                        tainted: 0,
                        shutdown: false,
                        settled: false,
                    });
                }
                Some(Scripted::Breaks(claims)) => {
                    for (at, &claim) in claims.iter().enumerate() {
                        self.probe.claims.record(ClaimRecord {
                            step,
                            claim,
                            detail: format!("{claim:?} #{at}"),
                        });
                    }
                }
                None => {}
            }
            Ok(())
        }

        /// Nothing: it finishes no submitted work.
        fn recovery(&self, _now: zakura_scheduler::SimInstant, _out: &mut crate::RecoveryOffer) {}

        /// Nothing: it has no holds.
        fn enter_drain(&mut self, _log: &mut ObservationLog) {}

        /// None: it holds no submitted work.
        fn holds_work(&self) -> Option<String> {
            None
        }
    }

    /// Runs `chunk-budget-bound` over one task that yields until `script`
    /// is spent, each step's evidence as the script says.
    fn run(script: &[Scripted]) -> RunReport {
        run_property(chunk_budget_bound::<Probe>(), script)
    }

    /// Runs `property` over one task that yields until `script` is spent,
    /// each step's evidence as the script says.
    fn run_property(property: Property<Probe>, script: &[Scripted]) -> RunReport {
        let probe = Probe::default();
        let mut sim = Sim::new(crate::RunConfig::default(), PlanInput::Bytes(Vec::new()));
        sim.add_source(Box::new(Boundaries {
            script: script.iter().copied().collect(),
            probe: probe.clone(),
        }));
        let steps = script.len();
        sim.scheduler().spawn_root("reactor", async move {
            for _ in 1..steps {
                zakura_rt::task::yield_now().await;
            }
        });
        let registry = Registry::new(vec![property]).expect("one property is unique");
        let mut checker = RegistryChecker::new(
            registry,
            Rc::new(probe),
            Assumptions::none(),
            &Sources::observed(&[Source::View]),
        );
        sim.run(&mut checker, &mut MemorySink::default())
    }

    /// Reads within the capacity hold, an unread boundary mid-poll judges
    /// nothing, and a read after it settles the property again.
    #[test]
    fn unread_mid_poll_boundaries_judge_nothing() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
            Scripted::Read(CHUNK_BUDGET_CAPACITY),
        ]);
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 2 }
        );
        assert_eq!(
            report.properties[0].peaks.get("claimed-headers"),
            Some(&4_000)
        );
    }

    /// D4 site 2: a run whose dry end comes at an unread boundary mid-poll
    /// can never read it, so the run fails as missing evidence, never a
    /// verdict, and the property is `HarnessFailed`.
    #[test]
    fn a_run_ending_unread_mid_poll_is_missing_evidence() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
        ]);
        assert!(
            matches!(
                &report.end.harness,
                Some(HarnessFailure::MissingEvidence { property, step: 2, .. })
                    if property == CHUNK_BUDGET_BOUND
            ),
            "{:?}",
            report.end
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::not_applicable(NaReason::HarnessFailed)
        );
    }

    /// An unread boundary with the reactor not mid-poll is missing
    /// evidence, which fails the run.
    #[test]
    fn an_unread_quiet_boundary_is_missing_evidence() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadQuiet),
        ]);
        assert!(
            matches!(
                report.end.harness,
                Some(HarnessFailure::MissingEvidence { step: 2, .. })
            ),
            "{:?}",
            report.end
        );
    }

    /// A read over the model's capacity violates the property, even with
    /// no claim break from the tap.
    #[test]
    fn a_read_over_capacity_violates() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Read(CHUNK_BUDGET_CAPACITY + 1),
        ]);
        assert_eq!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
        let violation = report.end.violation.expect("the run violated");
        assert_eq!(violation.step, 2);
        assert_eq!(
            violation.actual,
            "the probe reads 4001 reserved and 0 owned"
        );
    }

    /// A step whose released round a claim tainted doesn't count as a
    /// passing evaluation: of these two steps only the settled one does.
    #[test]
    fn a_tainted_round_is_not_counted_as_held() {
        let report = run_property(
            teardown_returns_baseline(),
            &[
                Scripted::Tainted,
                Scripted::Settlement {
                    released: 1,
                    unsettled: 0,
                    stuck: 0,
                },
            ],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 1 }
        );
    }

    /// Released rounds that settle hold, the steps before the first release
    /// don't apply, and the property reports nothing unsettled.
    #[test]
    fn settled_rounds_hold_teardown() {
        let report = run_property(
            teardown_returns_baseline(),
            &[
                Scripted::Settlement {
                    released: 0,
                    unsettled: 0,
                    stuck: 0,
                },
                Scripted::Settlement {
                    released: 1,
                    unsettled: 1,
                    stuck: 0,
                },
                Scripted::Settlement {
                    released: 1,
                    unsettled: 0,
                    stuck: 0,
                },
            ],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 2 }
        );
    }

    /// A run that ends with a released round unsettled stays unsettled
    /// while storage still holds a call with its headers, and violated once
    /// it came to rest with a round that has none, since nothing can settle
    /// that round then, whatever another round still waits on.
    #[test]
    fn a_run_ending_with_an_unsettled_round_is_judged_by_its_pending_calls() {
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Settlement {
                released: 1,
                unsettled: 1,
                stuck: 0,
            }],
        );
        assert!(matches!(report.end.outcome, Outcome::Idle { .. }));
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::not_applicable(NaReason::Unsettled {
                site: Pending::TeardownOpen
            })
        );
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Settlement {
                released: 1,
                unsettled: 1,
                stuck: 1,
            }],
        );
        assert!(matches!(
            report.properties[0].outcome,
            PropertyOutcome::Violated { .. }
        ));
        assert_eq!(
            report.properties[0].violations[0].expected,
            "a released round settles before the run comes to rest"
        );
        // One round's call still pending doesn't hide another round that
        // nothing can settle.
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Settlement {
                released: 2,
                unsettled: 2,
                stuck: 1,
            }],
        );
        assert!(matches!(
            report.properties[0].outcome,
            PropertyOutcome::Violated { .. }
        ));
    }

    /// Each round settlement claim violates at the step whose drain found
    /// it, and the budget's claims don't.
    #[test]
    fn round_settlement_claims_violate_teardown() {
        for claim in [
            Claim::LeakedReservation,
            Claim::LeakedLease,
            Claim::OpenOwnership,
        ] {
            let property = Property {
                status: Status::Draft,
                ..teardown_returns_baseline()
            };
            let report = run_property(
                property,
                &[
                    Scripted::Settlement {
                        released: 1,
                        unsettled: 0,
                        stuck: 0,
                    },
                    Scripted::Broke(claim),
                ],
            );
            let violations = &report.properties[0].violations;
            assert_eq!(violations.len(), 1, "{claim:?}: {violations:?}");
            assert_eq!(violations[0].step, 2, "{claim:?}");
        }
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Broke(Claim::OverBudget)],
        );
        assert!(report.properties[0].violations.is_empty());
    }

    /// A stop holds the property unsettled until the node settles, and then
    /// it applies and holds. A run cut off before is unsettled.
    #[test]
    fn node_settlement_settles_teardown() {
        let report = run_property(
            teardown_returns_baseline(),
            &[
                Scripted::Stop { settled: false },
                Scripted::Stop { settled: true },
            ],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Held { applicable: 1 }
        );
        let report = run_property(
            teardown_returns_baseline(),
            &[Scripted::Stop { settled: false }],
        );
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::not_applicable(NaReason::Unsettled {
                site: Pending::TeardownOpen
            })
        );
    }

    /// Each node settlement claim violates at the step whose drain found it.
    #[test]
    fn node_settlement_claims_violate_teardown() {
        for claim in [
            Claim::OpenReservation,
            Claim::TeardownReserved,
            Claim::TeardownOwned,
            Claim::TeardownLocatorPermits,
        ] {
            let property = Property {
                status: Status::Draft,
                ..teardown_returns_baseline()
            };
            let report = run_property(
                property,
                &[Scripted::Stop { settled: false }, Scripted::Broke(claim)],
            );
            let violations = &report.properties[0].violations;
            assert_eq!(violations.len(), 1, "{claim:?}: {violations:?}");
            assert_eq!(violations[0].step, 2, "{claim:?}");
        }
    }

    /// Every claim break one boundary's drain found survives into the
    /// report, not just the first, for each check that reads claim breaks:
    /// the first and third of three at step 1, in the order the tap found
    /// them. The second is another rule's, which the check doesn't report.
    #[test]
    fn every_claim_break_at_a_boundary_is_reported() {
        let budget = format!("at most {CHUNK_BUDGET_CAPACITY} headers reserved and owned");
        let teardown = "a released round, and a stopped node, keeps no reservation, lease or \
                        held headers once settled, and no locator permit a pending storage \
                        read doesn't hold";
        let stale = "a stale completion's handler returns only its own round's budget";
        /// A property, its ID, the claims the boundary broke, what the
        /// property expects, and the two findings it reports.
        type Case<'e> = (
            Property<Probe>,
            &'static str,
            &'static [Claim],
            &'e str,
            [&'static str; 2],
        );
        let cases: [Case<'_>; 3] = [
            (
                chunk_budget_bound(),
                CHUNK_BUDGET_BOUND,
                &[
                    Claim::BudgetOverCapacity,
                    Claim::LeakedLease,
                    Claim::OverBudget,
                ],
                &budget,
                ["BudgetOverCapacity #0", "OverBudget #2"],
            ),
            (
                teardown_returns_baseline(),
                TEARDOWN_RETURNS_BASELINE,
                &[
                    Claim::LeakedReservation,
                    Claim::OverBudget,
                    Claim::LeakedLease,
                ],
                teardown,
                ["LeakedReservation #0", "LeakedLease #2"],
            ),
            (
                stale_completion_inert(),
                STALE_COMPLETION_INERT,
                &[
                    Claim::StaleCompletionEffect,
                    Claim::LeakedLease,
                    Claim::StaleCompletionEffect,
                ],
                stale,
                ["StaleCompletionEffect #0", "StaleCompletionEffect #2"],
            ),
        ];
        for (property, id, claims, expected, [first, second]) in cases {
            let report = run_property(property, &[Scripted::Breaks(claims)]);
            let found = |actual: &str| Violation {
                property: id.to_owned(),
                step: 1,
                witness: vec![1],
                expected: expected.to_owned(),
                actual: actual.to_owned(),
            };
            assert_eq!(
                report.properties[0].violations,
                [found(first), found(second)],
                "{id}"
            );
            assert_eq!(
                report.properties[0].outcome,
                PropertyOutcome::Violated { witness: vec![1] },
                "{id}"
            );
        }
    }
}
