//! Synchronous header-sync reactor events for simulator tests.
//!
//! The reactor reports its port calls, active-request starts and
//! retirements, refused offers, header reservations, page staging and lease
//! returns, session admissions and ends, the committed state it adopts and
//! what it publishes from that state to an installed [`HeaderSyncObserver`].
//! This module exists only in test and `zakura_sim` builds. Production builds
//! compile none of it, and the reactor's `observe!` macro drops its tokens.

use std::{any::Any, fmt, future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc, task};

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BodyWorkOwner, EngineSnapshot, Frontier, HeaderGeneration, HeaderSyncWorkOwner,
    HeaderWorkAuthority, SourceId, StateVersion, VerifiedGeneration,
};
use zakura_rt::sync::counter::{AtomicU64, Ordering};

#[cfg(zakura_sim)]
pub use super::scheduler::peer_work::{HeaderResourceProbe, HeaderResources};
pub use super::v9::SubscribeHeaders;
use super::{
    events::HeaderPortOperation, ActiveHeaderRequest, HeaderSyncMisbehavior, HeaderTargetPurpose,
    Status,
};
pub use super::{
    reactor::HeaderRequestTerminal,
    scheduler::peer_work::{HeaderTargetPhase, HeaderWorkEnd, StorageCallKind},
};
use crate::zakura::{ServicePeerDirection, SessionOrigin, ZakuraPeerId};

mod ledger;

pub use ledger::{ClaimBreak, ClaimRule, HeaderWorkLedger};

/// The header chunk budget's capacity, so a model can pin its own constant
/// to it.
pub const HEADER_CHUNK_BUDGET_CAPACITY: usize =
    super::scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1;

/// The interval that refills one cached-offer pass, so a model can pin its
/// own constant to it.
pub const HEADER_CACHED_PASS_REFILL: std::time::Duration = super::reactor::CACHED_PASS_REFILL;

/// Receives reactor events as they happen.
///
/// Called synchronously on the reactor task, never while the reactor holds a
/// lock, with two exceptions. The header budget reports a return under its
/// own lock. A session's subscriber reports credit under the session's
/// subscriber lock, and a grant also under its subscription tools' lock.
/// An implementation must not block, yield, take a `zakura_rt` lock, or
/// call into the reactor. A panic in `observe` unwinds the reactor task, even
/// when it is called inside a port future.
pub trait HeaderSyncObserver: Send + Sync + fmt::Debug {
    /// Records one event.
    fn observe(&self, event: ReactorEvent);

    /// The observer the work queue's reservations, leases and budget report
    /// to, or `None` for this one. A leaked reservation or lease keeps its
    /// handle past the reactor, so an observer that reads the reactor's
    /// lifetime from its own handles can return one that doesn't count.
    /// Each admitted session reports its credit here too, and only when this
    /// names a handle, since the service keeps sessions past the reactor.
    fn capacity_observer(&self) -> Option<Arc<dyn HeaderSyncObserver>> {
        None
    }

    /// Takes the reactor's [`HeaderResourceProbe`], which the reactor hands
    /// over once, as it's built. The default drops it.
    #[cfg(zakura_sim)]
    fn resource_probe(&self, probe: HeaderResourceProbe) {
        drop(probe);
    }
}

/// The optional observer installed on a reactor's startup.
#[derive(Clone, Default)]
pub struct ObserverSlot(pub Option<Arc<dyn HeaderSyncObserver>>);

impl fmt::Debug for ObserverSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ObserverSlot")
            .field(&self.0.is_some())
            .finish()
    }
}

/// One reactor event.
///
/// Every `Dispatched { call }` is followed by exactly one `CallEnded { call }`.
/// `Handled { call }` follows unless that end is [`CallEnd::Dropped`] or the
/// reactor task unwinds. The events
/// strictly between `CallEnded { call }` and `Handled { call }` ran while the
/// reactor processed that completion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReactorEvent {
    /// A port future was pushed. `call` counts from 1 per reactor. Accepted
    /// and refused calls share the count.
    Dispatched {
        /// The call's number.
        call: u64,
        /// What the call asks storage for.
        op: PortCall,
    },
    /// The call's future produced its completion, or was caught panicking.
    CallEnded {
        /// The call's number.
        call: u64,
        /// How the future ended.
        end: CallEnd,
    },
    /// The reactor finished processing `call`'s completion: its callback, the
    /// capacity drop, the panic handling if any, and the snapshot refresh.
    Handled {
        /// The call's number.
        call: u64,
        /// Whether the call's future was caught panicking.
        panicked: bool,
    },
    /// A request joined this locator call instead of starting another.
    StorageJoined {
        /// The call in flight.
        call: u64,
    },
    /// The reactor refused a storage call before it started. The call settles
    /// nothing and takes a number no later call reuses.
    StorageRefused {
        /// The refused call's number.
        call: u64,
        /// What the call would have asked storage for.
        kind: StorageCallKind,
        /// Why the reactor refused it.
        cause: RefusalCause,
    },
    /// A prepare or apply call took ownership of a round's retained headers.
    HeadersHeld {
        /// The call's number. Its `Dispatched` came first.
        call: u64,
        /// What the call asks storage for.
        kind: StorageCallKind,
        /// The peer whose work the headers belong to.
        peer: ZakuraPeerId,
        /// The round the headers belong to.
        incarnation: u64,
        /// How many headers the call holds.
        headers: usize,
        /// The round each lease the call holds was staged for, as the lease
        /// records it. Only observed leases record one.
        leases: Vec<u64>,
    },
    /// A call's ownership of retained headers ended, before any lease released.
    HeadersEnded {
        /// The call's number.
        call: u64,
        /// The round the headers belong to.
        incarnation: u64,
        /// How many headers the call held.
        headers: usize,
        /// How the ownership ended.
        end: HeaderWorkEnd,
    },
    /// An active request started.
    Started {
        /// The request.
        request: RequestRef,
        /// Why the request was started.
        purpose: HeaderTargetPurpose,
        /// The locator the reactor handed on for it: the read's entries for
        /// a new `Open`, or the one continuation entry for a resumed
        /// subscription.
        locator: Vec<block::Hash>,
    },
    /// An active request was explicitly retired. Reported once the work
    /// queue has released its round.
    Retired {
        /// The request.
        request: RequestRef,
        /// The request's phase when it was retired.
        phase: HeaderTargetPhase,
        /// Why the request was started.
        purpose: HeaderTargetPurpose,
        /// Why the request was retired.
        reason: HeaderRequestTerminal,
    },
    /// An offer was refused for local capacity and stays unspent.
    OfferWaiting {
        /// The offering peer.
        peer: ZakuraPeerId,
        /// The offering session.
        session_id: u64,
        /// The offered target.
        target_tip_hash: block::Hash,
        /// Which capacity refused it.
        cause: WaitCause,
    },
    /// The work queue reserved header capacity for one wire request to
    /// `peer`: a round's first request, a continuation, or a VCT repair
    /// request. Reported once the reservation succeeds, before the request is
    /// sent, so it doesn't show that the request went out: a failed send or
    /// grant cancels the reservation, and a scheduling point can come in
    /// between.
    ReservationOpened {
        /// The reservation's number. It counts from 1 per reactor.
        reservation: u64,
        /// The peer the request goes to.
        peer: ZakuraPeerId,
        /// What kind of request it reserves for.
        purpose: ReservationPurpose,
        /// How many headers it reserves.
        headers: usize,
        /// The running round a continuation reserves for. A first request's
        /// or a repair's round starts later, and its `ReservationBound` names
        /// it.
        request: Option<RequestRef>,
    },
    /// A reservation opened before its round started now backs that round.
    /// Reported as the round starts, right before its `Started`.
    ReservationBound {
        /// The reservation.
        reservation: u64,
        /// The round it backs.
        request: RequestRef,
        /// The most headers the round asks for in one request, which bounds
        /// each of its reservations.
        max_headers: usize,
    },
    /// A reservation ended, returning its unstaged headers to the budget.
    /// Reported as its last handle drops.
    ReservationEnded {
        /// The reservation.
        reservation: u64,
        /// The headers it still held, which return to the budget.
        released: usize,
        /// Why it ended.
        cause: ReservationEnd,
    },
    /// The work queue staged one admitted page on a round, moving its headers
    /// from the round's reservation into a lease the queue owns.
    PageStaged {
        /// The peer the page arrived from.
        from_peer: ZakuraPeerId,
        /// The session the page arrived on.
        from_session: u64,
        /// The round the page was staged on.
        request: RequestRef,
        /// The reservation the page spent.
        reservation: u64,
        /// The subscription the page belongs to.
        subscription: u64,
        /// The page's position among its stream's reads, as the transport's
        /// `PageClaimed` reports it, or `None` when no transport reader
        /// tagged it, as on an in-memory link.
        frame: Option<u64>,
        /// How many headers the page carried.
        headers: usize,
        /// How many headers the reservation still holds.
        reserved_left: usize,
        /// The common ancestor the page declared, which its first header
        /// extends.
        ancestor: Frontier,
        /// The page's header hashes, in order.
        hashes: Vec<block::Hash>,
        /// The hashes the round holds where the page went, as the queue's
        /// policy stored them.
        stored: Vec<block::Hash>,
        /// How many headers the round holds staged now, as the queue's
        /// policy has them.
        staged_total: usize,
    },
    /// The work queue is removing a started round, after releasing its
    /// capacity: its peer's reservation and the queue's own clones of the
    /// round's leases. Reported before the queue forgets the round, however
    /// the release went.
    RoundReleased {
        /// The round's peer.
        peer: ZakuraPeerId,
        /// The round.
        incarnation: u64,
        /// The headers in leases the queue still holds for the peer, which a
        /// complete release leaves at 0.
        queue_owned: usize,
    },
    /// The work queue's slots changed: a slot was added, given a new target
    /// or priority's target, started, moved into preparation, where a
    /// bounded prefix also narrows its owner, or into apply, or removed.
    /// Lists every slot the queue holds, ordered by peer. Reported after the
    /// change, once per change of any listed field.
    Slots {
        /// Every slot, ordered by peer.
        slots: Vec<WorkSlot>,
    },
    /// The header chunk budget changed: a reservation charged it, a page's
    /// staging moved reserved headers into a lease, or a reservation or a
    /// lease returned headers. The budget reports every change itself,
    /// inside the call that made it, whoever called. A return reports under
    /// the budget's lock, before it wakes waiters.
    BudgetChanged {
        /// What the change was for, or `None` when nothing tagged it.
        owner: Option<BudgetOwner>,
        /// The change in reserved headers.
        reserved_delta: i64,
        /// The change in owned headers.
        owned_delta: i64,
    },
    /// The last handle of a page's lease dropped, returning its headers to
    /// the budget.
    LeaseReturned {
        /// The peer whose round staged the page.
        peer: ZakuraPeerId,
        /// That round.
        incarnation: u64,
        /// How many headers the lease held.
        headers: usize,
    },
    /// The reactor admitted a session from `peer`, replacing any session it
    /// held for that peer. The replaced session leaves the reactor's view
    /// here, since its later disconnect finds another session and does
    /// nothing.
    SessionAdmitted {
        /// The admitted peer.
        peer: ZakuraPeerId,
        /// The admitted session's ID, as its service handed it over.
        session_id: u64,
        /// The direction of the session's connection.
        direction: ServicePeerDirection,
        /// The session this one replaced, if the reactor held one for `peer`.
        replaced: Option<u64>,
        /// Where the transport got the session, if its peer said.
        origin: Option<SessionOrigin>,
    },
    /// The reactor removed `peer`'s current session on its disconnect. A
    /// disconnect of a session the reactor no longer holds reports nothing.
    SessionEnded {
        /// The peer.
        peer: ZakuraPeerId,
        /// The ended session.
        session_id: u64,
        /// Why the service reported the disconnect.
        reason: &'static str,
    },
    /// The reactor dispatched a violation by `peer` to storage.
    Misbehavior {
        /// The violating peer.
        peer: ZakuraPeerId,
        /// What the peer did.
        reason: HeaderSyncMisbehavior,
        /// The session whose message or work showed the violation, if known.
        origin: Option<u64>,
        /// `peer`'s current session when the violation was dispatched.
        current: Option<u64>,
    },
    /// This node's subscriber gave `peer` credit for headers: a
    /// subscription's first credit at its `Open`, or a `Grant` that refills
    /// it. The subscriber reports it as it records the update, whoever
    /// asked for it.
    CreditIssued {
        /// Whether an `Open` or a `Grant` gave it.
        kind: CreditKind,
        /// The peer the credit lets send.
        peer: ZakuraPeerId,
        /// The session the subscription is on.
        session_id: u64,
        /// The subscription.
        subscription: u64,
        /// The headers it adds.
        objects: u32,
        /// The bytes it adds.
        bytes: u32,
        /// The update as the subscriber built it, the last owner boundary
        /// before encoding: its sequence, acknowledgement, target, schema,
        /// and an `Open`'s locators.
        update: Box<SubscribeHeaders>,
        /// The height of the subscription's target, which the update names
        /// only by hash.
        target_height: block::Height,
    },
    /// The subscriber accepted the cursors of `subscription` through
    /// `through`, so its next update acknowledges it.
    CursorsAccepted {
        /// The peer the subscription is with.
        peer: ZakuraPeerId,
        /// The session the subscription is on.
        session_id: u64,
        /// The subscription.
        subscription: u64,
        /// The last header the accepted cursors reach.
        through: Frontier,
    },
    /// The reactor started or finished a unit of work, and `cause` is the
    /// one it's in now. Every fact the reactor's task reports after this,
    /// until the next `CauseSet`, belongs to `cause`.
    CauseSet {
        /// The unit of work the reactor is in.
        cause: ReactorCause,
    },
    /// `peer`'s offer waits for header room: the reactor held it back with
    /// no headroom left for its target, not for a pause, or its locator
    /// found no room to reserve. Reported at each such refusal. The cached
    /// pass skipping an offer with no headroom doesn't report it, so an
    /// offer first held back for another reason can wait for room with no
    /// `HeadroomWait`.
    HeadroomWait {
        /// The offering peer.
        peer: ZakuraPeerId,
        /// The offering session.
        session_id: u64,
        /// The offered target.
        target_tip_hash: block::Hash,
        /// The offered target's height.
        target_height: block::Height,
    },
    /// The cached-offer pass the reactor wanted was refused by its pass
    /// budget. The budget's next refill is `refill_in` from now, and the
    /// reactor arms it as a deadline. A later change can clear that wait
    /// before the pass runs, as a discovery pause does.
    CachedPassDeferred {
        /// How long until the next refill.
        refill_in: std::time::Duration,
    },
    /// The run loop read committed snapshot `state_version` from its
    /// committed watch, at the top of a turn or in the turn's `select!`.
    /// Reported before the reactor handles the snapshot. Not reported for
    /// the reads after a completion or a connect, which refresh the
    /// snapshot without waiting on the watch.
    WatchObserved {
        /// The read snapshot's state version.
        state_version: StateVersion,
    },
    /// The run loop took an event from `queue` at `point`, leaving `queued`
    /// more in that queue. Reported before the event is handled. A take at
    /// [`TakePoint::TurnStart`] that leaves `queued` above zero shows the
    /// queue was ready when the turn's `select!` polled it, since nothing
    /// else takes from it in between.
    EventTaken {
        /// The queue.
        queue: EventQueue,
        /// Where in the turn.
        point: TakePoint,
        /// The events left in the queue.
        queued: usize,
    },
    /// The reactor refused a session from `peer` because `peer`'s readmission
    /// cooldown still holds. Nothing else about the reactor changes.
    SessionRefused {
        /// The refused peer.
        peer: ZakuraPeerId,
        /// The refused session's ID, as its service handed it over.
        session_id: u64,
    },
    /// The reactor installed a readmission cooldown for `peer` after a fault
    /// in session `origin`'s work. Reported right after the `Misbehavior` it
    /// follows, and only when an entry is installed, so a zero cooldown
    /// reports nothing.
    CooldownStarted {
        /// The cooled peer.
        peer: ZakuraPeerId,
        /// The session whose work showed the fault.
        origin: u64,
        /// How long the installed entry refuses `peer`'s sessions from now:
        /// its deadline less the time it was installed at.
        cooldown: std::time::Duration,
    },
    /// The reactor adopted a committed snapshot as the state it schedules and
    /// publishes from: once at construction, then each time it observes a
    /// different one. Reported before the reactor retires the work the
    /// snapshot makes obsolete, so every later event follows from it.
    SnapshotAdopted {
        /// The snapshot's durable state version.
        state_version: StateVersion,
        /// The selected-header work generation that new work captures.
        header_generation: HeaderGeneration,
        /// The finality anchor.
        finalized: Frontier,
        /// The selected header tip.
        header_best: Frontier,
        /// Work after the anchor through `header_best`, as `Status` carries it.
        suffix_work: U256,
        /// The lowest height the node retains headers for.
        oldest_retained: block::Height,
        /// The full-state verified-path generation that new repair work
        /// captures.
        verified_generation: VerifiedGeneration,
        /// The verified body tip.
        verified_best: Frontier,
    },
    /// The reactor queued `status` on `peer`'s session.
    StatusQueued {
        /// The peer.
        peer: ZakuraPeerId,
        /// The session the status went to.
        session_id: u64,
        /// The queued advertisement.
        status: Status,
    },
    /// The session's output queue refused a due `Status`, and the reactor
    /// scheduled a retry of its latest desired status.
    StatusDeferred {
        /// The peer.
        peer: ZakuraPeerId,
        /// The session that refused it.
        session_id: u64,
    },
    /// The reactor published a selected header tip on its tip watch: the
    /// watch's initial value at construction, then each send.
    TipPublished {
        /// The tip's height.
        height: block::Height,
        /// The tip's hash.
        hash: block::Hash,
        /// Whether a receiver saw it. A send with no live receiver is lost.
        delivered: bool,
    },
}

impl ReactorEvent {
    /// A `Started` event for `active`.
    pub(crate) fn started(active: &ActiveHeaderRequest) -> Self {
        Self::Started {
            request: RequestRef::of(active),
            purpose: active.purpose.clone(),
            locator: active
                .sent_locator
                .entries()
                .iter()
                .map(|entry| entry.hash)
                .collect(),
        }
    }

    /// A `SnapshotAdopted` event for `snapshot`.
    pub(crate) fn adopted(snapshot: &EngineSnapshot) -> Self {
        Self::SnapshotAdopted {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            finalized: snapshot.frontiers.finalized,
            header_best: snapshot.frontiers.header_best,
            suffix_work: snapshot.header_best_score.suffix_work.as_u256(),
            oldest_retained: snapshot.oldest_retained_height,
            verified_generation: snapshot.verified_generation,
            verified_best: snapshot.frontiers.verified_best,
        }
    }

    /// A `Retired` event for `active`, retired for `reason`.
    pub(crate) fn retired(active: &ActiveHeaderRequest, reason: HeaderRequestTerminal) -> Self {
        Self::Retired {
            request: RequestRef::of(active),
            phase: active.phase,
            purpose: active.purpose.clone(),
            reason,
        }
    }
}

/// One peer's slot in the work queue, as [`ReactorEvent::Slots`] lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkSlot {
    /// The slot's peer.
    pub peer: ZakuraPeerId,
    /// The session the slot's work belongs to: the awaiting target's, or
    /// the round owner's.
    pub session_id: u64,
    /// The advertised target the slot pursues.
    pub target_tip_hash: block::Hash,
    /// The round's phase, or `None` while the slot awaits its locator.
    pub phase: Option<HeaderTargetPhase>,
    /// The round the queue assigned at start, or 0 while the slot awaits
    /// its locator.
    pub incarnation: u64,
    /// Whether the round is a body repair's.
    pub repair: bool,
    /// The header authority the slot holds now: the awaiting target's
    /// scope, or the round owner's, which a bounded prefix narrows.
    pub authority: HeaderWorkAuthority,
}

/// What a dispatched port call asks storage for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortCall {
    /// A continuation locator read.
    Locator {
        /// The peer whose offer needs the locator.
        peer: ZakuraPeerId,
        /// The offering session.
        session_id: u64,
        /// The offered target.
        target_tip_hash: block::Hash,
        /// The header authority the read was made under.
        scope: HeaderWorkAuthority,
    },
    /// A VCT repair context read.
    VctContext {
        /// The repair's owner.
        owner: BodyWorkOwner,
        /// The height that needs repair.
        height: block::Height,
    },
    /// A target preparation.
    Prepare {
        /// The supplying peer.
        peer: ZakuraPeerId,
        /// The supplier's source identity.
        source: SourceId,
        /// The request that owns the target.
        owner: HeaderSyncWorkOwner,
        /// The target the call names for its headers.
        target: Frontier,
        /// The hashes of the headers the call actually sends, in order.
        hashes: Vec<block::Hash>,
    },
    /// A prepared target's application.
    Apply {
        /// The supplying peer.
        peer: ZakuraPeerId,
        /// The supplier's source identity.
        source: SourceId,
        /// The request that owns the target.
        owner: HeaderSyncWorkOwner,
    },
}

impl PortCall {
    /// The call `action` makes, or `None` for an action that calls no port.
    pub(crate) fn of(action: &HeaderPortOperation) -> Option<Self> {
        Some(match action {
            HeaderPortOperation::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } => Self::Locator {
                peer: peer.clone(),
                session_id: *session_id,
                target_tip_hash: *target_tip_hash,
                scope: *scope,
            },
            HeaderPortOperation::QueryVctRepairContext { owner, height } => Self::VctContext {
                owner: *owner,
                height: *height,
            },
            HeaderPortOperation::PrepareHeaderTarget {
                peer,
                source,
                owner,
                target,
                entries,
                ..
            } => Self::Prepare {
                peer: peer.clone(),
                source: *source,
                owner: *owner,
                target: *target,
                hashes: entries.iter().map(|entry| entry.header.hash()).collect(),
            },
            HeaderPortOperation::ApplyHeaderTarget {
                peer,
                source,
                owner,
                ..
            } => Self::Apply {
                peer: peer.clone(),
                source: *source,
                owner: *owner,
            },
            HeaderPortOperation::Misbehavior { .. } | HeaderPortOperation::DropPeer { .. } => {
                return None
            }
        })
    }
}

/// How a port call's future ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CallEnd {
    /// Storage replied. For a prepare or an apply, this includes a typed
    /// storage failure.
    Replied,
    /// The port returned a `PortError`.
    PortError,
    /// The reactor's request timeout elapsed first.
    TimedOut,
    /// The reactor answered a prepare itself without calling storage.
    NotSubmitted,
    /// The future panicked, and the reactor caught it.
    PortPanicked,
    /// The reactor dropped the future unfinished, as its teardown does. Reported
    /// right before the drop, so any storage receipt the drop leaves comes after
    /// it.
    Dropped,
}

impl CallEnd {
    /// How a port call under the reactor's request timeout ended.
    pub(crate) fn of_timed<T, E, Elapsed>(result: &Result<Result<T, E>, Elapsed>) -> Self {
        match result {
            Ok(Ok(_)) => Self::Replied,
            Ok(Err(_)) => Self::PortError,
            Err(_) => Self::TimedOut,
        }
    }
}

/// Why the reactor refused a storage call.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RefusalCause {
    /// Every locator read permit was taken.
    LocatorCapacity,
    /// Another local VCT repair operation owns the slot.
    LocalOperationBusy,
    /// The external driver's action queue refused the call.
    ExternalQueue,
}

/// Which local capacity refused an offer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WaitCause {
    /// The staging queue was full.
    StagingFull,
    /// Every locator read permit was taken.
    LocatorCapacity,
}

/// What kind of request a reservation backs.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReservationPurpose {
    /// An ordinary round's first request or continuation, bounded by the
    /// per-request fair share.
    Ordinary,
    /// A selected auxiliary repair, bounded by the budget's unclaimed
    /// capacity and the repair's own range.
    Repair,
}

/// Why a reservation ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReservationEnd {
    /// Its pages spent every header it held.
    Staged,
    /// Its request wasn't sent, or its round didn't start.
    Cancelled,
    /// The queue retired or removed its peer's work.
    Released,
    /// It dropped any other way, as the queue's own drop at teardown does.
    Dropped,
}

/// What a [`ReactorEvent::BudgetChanged`] was for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BudgetOwner {
    /// A reservation, by its number: its charge, the headers a page's
    /// staging moves out of it, or its return.
    Reservation(u64),
    /// A page's lease returning its headers.
    Lease {
        /// The reservation the lease's headers came from.
        reservation: u64,
        /// The peer and round the queue staged the page on, once it did.
        round: Option<(ZakuraPeerId, u64)>,
    },
}

/// What gave a [`ReactorEvent::CreditIssued`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CreditKind {
    /// A new subscription's `Open`.
    Open,
    /// A `Grant` on a live subscription.
    Grant,
}

/// One of the reactor's two event queues, as [`ReactorEvent::EventTaken`]
/// names it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EventQueue {
    /// Session connects and disconnects.
    Lifecycle,
    /// Peer messages, such as `Status`.
    Wire,
}

/// Where in a turn of its run loop the reactor took an event, as
/// [`ReactorEvent::EventTaken`] reports it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TakePoint {
    /// The `try_recv` before the turn's `select!`, which takes at most one
    /// event from each queue.
    TurnStart,
    /// The turn's `select!`.
    Select,
}

/// The unit of work the reactor is in, as [`ReactorEvent::CauseSet`]
/// reports it. The reactor sets each one only at the boundary where that
/// work starts, and restores the one before when it ends.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReactorCause {
    /// None: the run loop between units of work, such as while it polls
    /// its pending calls.
    Idle,
    /// The reactor being built.
    Startup,
    /// The handler of the storage call `call`'s completion.
    Completion {
        /// The call's number.
        call: u64,
    },
    /// The committed snapshot refresh after call `call`'s handler.
    Refresh {
        /// The call's number.
        call: u64,
    },
    /// A committed snapshot or repair status the reactor read from its
    /// watches.
    Watch,
    /// A lifecycle or wire event the reactor took.
    Event,
    /// The deadline maintenance: timeouts, close expiry and repair
    /// assignment.
    Maintenance,
    /// A locator permit coming free.
    Capacity,
    /// The cached status pass at the end of each turn.
    Pass,
    /// Retiring every peer's work as the run ends.
    Shutdown,
}

/// Where an admitted page came from, which the reactor hands the work queue
/// to report with the page's staging.
#[derive(Clone, Debug)]
pub(crate) struct PageArrival {
    /// The peer the page arrived from.
    pub(crate) from_peer: ZakuraPeerId,
    /// The session the page arrived on.
    pub(crate) from_session: u64,
    /// The page's subscription.
    pub(crate) subscription: u64,
    /// The page's frame, if a transport reader tagged it.
    pub(crate) frame: Option<u64>,
}

/// An active request's identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestRef {
    /// The peer whose session owns the request.
    pub peer: ZakuraPeerId,
    /// The owning session.
    pub session_id: u64,
    /// The request's correlation ID.
    pub request_id: u64,
    /// The round the queue assigned at start.
    pub incarnation: u64,
    /// The request's work owner.
    pub owner: HeaderSyncWorkOwner,
    /// The advertised target the request pursues.
    pub target_tip_hash: block::Hash,
}

impl RequestRef {
    /// `active`'s identity.
    pub(crate) fn of(active: &ActiveHeaderRequest) -> Self {
        Self {
            peer: active.peer.clone(),
            session_id: active.owner.session_id(),
            request_id: active.request_id.get(),
            incarnation: active.incarnation().get(),
            owner: active.owner,
            target_tip_hash: active.target.status.selected_tip_hash,
        }
    }
}

/// The observer and call number a port future reports its end through.
#[derive(Clone, Debug)]
pub(crate) struct PortTap {
    /// The call's number, or 0 for an action that calls no port.
    pub(crate) call: u64,
    observer: Option<Arc<dyn HeaderSyncObserver>>,
    /// How many ends this call reported, through any clone, so a [`PendingCall`]
    /// dropped after one reports nothing. Only the reactor's task touches it. A
    /// simulator's coordination atomic would be a scheduling point, which would
    /// split a storage receipt from its call's end.
    ended: Arc<AtomicU64>,
}

impl PortTap {
    /// A tap for `call` that reports to `slot`'s observer, if one is installed.
    pub(crate) fn new(call: u64, slot: &ObserverSlot) -> Self {
        Self {
            call,
            observer: slot.0.clone(),
            ended: Arc::default(),
        }
    }

    /// Reports that the call ended with `end`, from inside the port future.
    ///
    /// The port future's panic catch would otherwise report an observer panic
    /// here as the port's own, after the observer may have recorded this end.
    /// So the panic is rethrown wrapped in [`ObserverPanicked`], which
    /// [`rethrow_observer_panic`] unwraps and resumes past the catch.
    pub(crate) fn end(&self, end: CallEnd) {
        if let Err(payload) =
            std::panic::catch_unwind(AssertUnwindSafe(|| self.end_after_catch(end)))
        {
            std::panic::resume_unwind(Box::new(ObserverPanicked(payload)));
        }
    }

    /// Reports that the call ended with `end`, from outside the port future's
    /// panic catch, where an observer panic already unwinds the reactor.
    pub(crate) fn end_after_catch(&self, end: CallEnd) {
        self.ended.fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            observer.observe(ReactorEvent::CallEnded {
                call: self.call,
                end,
            });
        }
    }
}

/// A port call's number, from 1, as [`ReactorEvent::Dispatched`] hands it out.
/// In a simulator build, storage tags each operation a call's poll submits with
/// it (see [`as_call`]), so each storage receipt pairs with exactly the call that
/// submitted it.
pub type CallId = u64;

/// Runs `poll` as call `call`'s poll, so in a simulator build `polling_call` names
/// `call` inside it, on this task only, as a storage call submitted there reads
/// it. The task's previous call is restored after, on unwind too.
pub fn as_call<R>(call: CallId, poll: impl FnOnce() -> R) -> R {
    #[cfg(zakura_sim)]
    let _scope = call_scope::CallScope::enter(call);
    #[cfg(not(zakura_sim))]
    let _ = call;
    poll()
}

/// The call whose poll the current task is running, if any. See [`as_call`].
#[cfg(zakura_sim)]
pub fn polling_call() -> Option<CallId> {
    call_scope::current()
}

/// Each task's polling call. Keyed by run and task, not just by thread, because a
/// poll can reach a scheduling point, and the run then switches to another task
/// on this thread before the poll returns.
#[cfg(zakura_sim)]
mod call_scope {
    use std::cell::RefCell;

    use zakura_rt::sim::{RunId, TaskId};

    use super::CallId;

    std::thread_local! {
        /// Each task's polling call, with `None` for code outside a run or a task.
        static CALLS: RefCell<Vec<(Task, CallId)>> = const { RefCell::new(Vec::new()) };
    }

    /// A run's task.
    type Task = (Option<RunId>, Option<TaskId>);

    /// The task running now.
    fn task() -> Task {
        (
            zakura_rt::sim::run_id(),
            zakura_rt::sim::executor().and_then(|executor| executor.current_task()),
        )
    }

    /// The current task's polling call.
    pub(super) fn current() -> Option<CallId> {
        let task = task();
        CALLS.with_borrow(|calls| {
            calls
                .iter()
                .find(|(t, _)| *t == task)
                .map(|(_, call)| *call)
        })
    }

    /// One task's call, until dropped.
    pub(super) struct CallScope {
        task: Task,
        previous: Option<CallId>,
    }

    impl CallScope {
        /// Makes `call` the current task's polling call.
        pub(super) fn enter(call: CallId) -> Self {
            let task = task();
            let previous =
                CALLS.with_borrow_mut(|calls| match calls.iter_mut().find(|(t, _)| *t == task) {
                    Some((_, current)) => Some(std::mem::replace(current, call)),
                    None => {
                        calls.push((task, call));
                        None
                    }
                });
            Self { task, previous }
        }
    }

    impl Drop for CallScope {
        /// Restores the task's previous polling call, or none.
        fn drop(&mut self) {
            CALLS.with_borrow_mut(|calls| {
                let Some(index) = calls.iter().position(|(t, _)| *t == self.task) else {
                    return;
                };
                match self.previous {
                    Some(previous) => calls[index].1 = previous,
                    None => {
                        calls.swap_remove(index);
                    }
                }
            });
        }
    }
}

/// A port call's whole future, guarded so that dropping it unfinished, as the
/// reactor's teardown does, reports [`CallEnd::Dropped`] through the call's tap.
///
/// The guard reports first and then drops the future: the storage call, whose
/// drop may leave a `CallerDropped` or `ReplyDropped` receipt, and everything else
/// it holds. So for a dropped call alone, the end comes before its receipt. A
/// panicking thread reports nothing, as [`HeaderSyncObserver`] says.
pub(crate) struct PendingCall<F> {
    future: Option<F>,
    tap: PortTap,
    /// Set once a poll returned or unwound, so the call's own end reports it.
    done: bool,
}

impl<F> PendingCall<F> {
    /// Guards `future`, the future of the call `tap` reports for.
    pub(crate) fn new(future: F, tap: PortTap) -> Self {
        Self {
            future: Some(future),
            tap,
            done: false,
        }
    }
}

impl<F: Future + Unpin> Future for PendingCall<F> {
    type Output = F::Output;

    /// Polls the guarded future as its call's (see [`as_call`]).
    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<F::Output> {
        let call = self.tap.call;
        let future = self
            .future
            .as_mut()
            .expect("a pending call's future lives until its guard drops");
        let polled = std::panic::catch_unwind(AssertUnwindSafe(|| {
            as_call(call, || Pin::new(future).poll(cx))
        }));
        match polled {
            Ok(task::Poll::Pending) => task::Poll::Pending,
            Ok(ready) => {
                self.done = true;
                ready
            }
            Err(payload) => {
                self.done = true;
                std::panic::resume_unwind(payload)
            }
        }
    }
}

impl<F> Drop for PendingCall<F> {
    /// Reports the call dropped unless a poll finished, its end was already
    /// reported, or the thread is unwinding, then drops the future.
    fn drop(&mut self) {
        let reported = self.done || self.tap.ended.load(Ordering::Relaxed) > 0;
        if !reported && !std::thread::panicking() {
            self.tap.end_after_catch(CallEnd::Dropped);
        }
        drop(self.future.take());
    }
}

/// An observer panic's original payload, carried through a port future's
/// panic catch. See [`PortTap::end`].
struct ObserverPanicked(Box<dyn Any + Send>);

/// Resumes `result`'s panic with the observer's original payload if a port
/// future's observer raised it, so it unwinds the reactor instead of being
/// reported as a port panic.
pub(crate) fn rethrow_observer_panic<T>(
    result: Result<T, Box<dyn Any + Send>>,
) -> Result<T, Box<dyn Any + Send>> {
    result.map_err(|payload| match payload.downcast::<ObserverPanicked>() {
        Ok(observer_panic) => std::panic::resume_unwind(observer_panic.0),
        Err(payload) => payload,
    })
}
