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
        round_life::RoundLifeEvidence,
        stale::StaleEvidence,
        wake::{WakeEvidence, WakeFact},
    },
    property::{
        CheckCtx, EndViolation, FactAnswer, HasView, Inconclusive, Kind, LivenessBound,
        ObligationKey, Property, PropertyId, Source, Status, Subject, TriggerHistory, Unit,
        Verdict,
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
/// mid-poll, as it is inside a release, that's allowed, and a run that ends
/// there is inconclusive. Otherwise no task should hold the lock, so the
/// missing read is missing evidence.
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
        Some(BoundaryRead::UnreadMidPoll) => cx.unsettled(Inconclusive::UnreadBoundary),
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
/// released round unsettled is inconclusive while storage still holds a
/// call with that round's headers or the drain was cut off at its bound,
/// since that call could still settle it. A run that ends at rest, idle or
/// with its drain run dry, with no such call pending can never settle it,
/// so [`teardown_end_violations`] violates it then. A run that ends with a
/// stop begun and the node unsettled is inconclusive only when its drain
/// was cut off at its bound. Otherwise the tap fails it as
/// `ShutdownIncomplete`. The node half of the claim is about a stopped
/// node, so a run that never stops the node holds once its released rounds
/// settle. Today no committed input can stop the node, so every planted
/// round-level case relies on that.
fn check_teardown<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: TeardownView,
{
    let view = cx.view();
    let step = cx.step;
    let row = view.rounds().at(step);
    if row.is_some_and(|row| row.unsettled > 0 || (row.shutdown && !row.settled)) {
        cx.unsettled(Inconclusive::TeardownOpen);
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
    if row.is_some_and(|row| row.released > 0 || row.settled) {
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
        claim: "a header round started under the adopted generation is retired by the end of                 the unit of work that adopts a newer one, a live round's prepare or apply                 carries the adopted generation, every reset advances the header generation,                 and every round starts from a locator read of its own generation",
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

/// The node stages, prepares and commits only headers it asked for: each
/// round's owner is bound to the `Open` the node built for it, and each
/// staged page belongs to its round's session and subscription and lies
/// on the requested path.
pub const COMMIT_ONLY_AUTHORIZED: &str = "header-sync/commit-only-authorized";

/// The claim rules [`COMMIT_ONLY_AUTHORIZED`] reports.
const AUTHORITY_CLAIMS: [Claim; 3] = [
    Claim::CommitOnlyAuthorized,
    Claim::ForeignPage,
    Claim::UnstagedPage,
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
                admits each session under its link's identity, and each staged page belongs to \
                its round's session and subscription and lies on the path it asked for",
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
/// judges each `Started` and `PageStaged`, the ledger reports a foreign or
/// unstaged page, and the session view an admission under another
/// identity than its link's. A break the step's drain found violates at
/// once. It applies once a drain found a start, a staged page or an
/// admission's identity join.
fn check_commit_only_authorized<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: AuthorityView,
{
    let view = cx.view();
    let step = cx.step;
    let expected = "the node stages only headers it asked for, under the owner it bound";
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
                or within one cached-pass refill when the pass is deferred, unless its \
                session ends",
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
/// read's submission or the waiting session's end answers it, and a
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
        Some(WakeFact::Ended { peer, session_id })
            if *subject == wake_subject(&peer, session_id) =>
        {
            FactAnswer::Discharges
        }
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
/// leaves the run inconclusive if it ends there.
fn check_watch_read<S>(cx: &mut CheckCtx<'_, S>, queue: EventQueue) -> Verdict
where
    S: HasView,
    S::View: AdoptionView,
{
    match cx.view().adoption().watch_read(queue) {
        WatchRead::Nothing => Verdict::NotApplicable,
        WatchRead::Holds => Verdict::Holds,
        WatchRead::Open => {
            cx.unsettled(Inconclusive::ObligationOpen);
            Verdict::Holds
        }
        WatchRead::Broken(why) => cx.violated(
            "each published state is read from the committed watch before a third event \
             from the flooded queue",
            why,
        ),
    }
}

/// [`WATCH_OBSERVED_UNDER_FLOOD`]'s trigger history: a published state
/// whose wire queue competed with its watch read
/// ([`AdoptionEvidence::published_into_flood`]).
fn published_under_wire_flood<V: AdoptionView>(view: &V, _: &ObservationLog) -> bool {
    view.adoption().published_into_flood(EventQueue::Wire)
}

/// [`WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD`]'s trigger history: a published
/// state whose lifecycle queue competed with its watch read.
fn published_under_lifecycle_flood<V: AdoptionView>(view: &V, _: &ObservationLog) -> bool {
    view.adoption().published_into_flood(EventQueue::Lifecycle)
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
        property::{Property, Status},
        ActionSource, Boundary, HarnessFailure, Identity, MemorySink, ObservationLog, Outcome,
        PlanInput, PropertyOutcome, RunEnd, RunReport, Sim, Violation,
    };

    /// The model's capacity and refill interval are production's.
    #[test]
    fn the_model_capacity_is_production_s() {
        assert_eq!(CHUNK_BUDGET_CAPACITY, HEADER_CHUNK_BUDGET_CAPACITY);
        assert_eq!(CACHED_PASS_REFILL, HEADER_CACHED_PASS_REFILL);
    }

    /// A probe's and a ledger's evidence with no tap behind it.
    #[derive(Clone, Default)]
    struct Probe {
        capacity: CapacityEvidence,
        claims: ClaimEvidence,
        rounds: SettlementEvidence,
        stale_calls: StaleEvidence,
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
                    shutdown: false,
                    settled: false,
                }),
                Some(Scripted::Stop { settled }) => self.probe.rounds.record(Settlement {
                    step,
                    released: 0,
                    unsettled: 0,
                    stuck: 0,
                    shutdown: true,
                    settled,
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

    /// A run that ends at an unread boundary mid-poll is inconclusive.
    #[test]
    fn a_run_ending_unread_mid_poll_is_inconclusive() {
        let report = run(&[
            Scripted::Read(10),
            Scripted::Unread(BoundaryRead::UnreadMidPoll),
        ]);
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Inconclusive {
                reason: "unread-boundary".to_owned()
            }
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

    /// A run that ends with a released round unsettled is inconclusive
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
            PropertyOutcome::Inconclusive {
                reason: "teardown-open".to_owned()
            }
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
    /// it applies and holds. A run cut off before is inconclusive.
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
            PropertyOutcome::Inconclusive {
                reason: "teardown-open".to_owned()
            }
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
