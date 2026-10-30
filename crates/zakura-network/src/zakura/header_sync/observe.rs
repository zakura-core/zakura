//! Synchronous header-sync reactor events for simulator tests.
//!
//! The reactor reports its port calls, active-request starts and
//! retirements, refused offers, and its settled state at each wait to an
//! installed [`HeaderSyncObserver`].
//! This module exists only in test and `zakura_sim` builds. Production builds
//! compile none of it, and the reactor's `observe!` macro drops its tokens.

use std::{any::Any, fmt, panic::AssertUnwindSafe, sync::Arc};

use zakura_rt::sync::counter;

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, Frontier, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId,
};

use super::{
    events::HeaderPortOperation, v9::Cursor, ActiveHeaderRequest, HeaderSyncMisbehavior,
    HeaderTargetPurpose,
};
pub use super::{reactor::HeaderRequestTerminal, scheduler::peer_work::HeaderTargetPhase};
use crate::zakura::{ServicePeerDirection, ZakuraPeerId};

/// Receives reactor events as they happen.
///
/// The reactor's own state is private to its task, so the reactor reports
/// it itself (rule 3 of the M6a observation contract): a
/// [`ReactorEvent::Settled`] snapshot right before each await that can't
/// change it, and a [`ReactorEvent::Resumed`] marker as the first statement
/// after that await. Between the two, its state is in transition.
///
/// Every event reaches it through its slot's counted delivery path,
/// [`ObserverSlot::emit`], which counts it first, so a reader can tell an
/// event the observer dropped from one never sent.
///
/// An implementation must never block, never reach a scheduling point, and
/// never call back into an observed owner. It may take only diagnostic
/// locks, such as a std mutex around its own buffer, and never hold one
/// across a scheduling point. Today the reactor calls it on its own task,
/// with no lock held, but later owners will call it from any task and
/// under their own locks, so it mustn't rely on either. A panic in
/// `observe` unwinds the caller, even when it's called inside a port
/// future.
pub trait HeaderSyncObserver: Send + Sync + fmt::Debug {
    /// Records one event.
    fn observe(&self, event: ReactorEvent);
}

/// The optional observer installed on a reactor's startup, with its
/// delivery count.
///
/// The count is shared by every clone, and a reader that holds it, such as
/// a simulator tap, keeps it after the reactor and its observer are gone.
#[derive(Clone, Default)]
pub struct ObserverSlot {
    observer: Option<Arc<dyn HeaderSyncObserver>>,
    /// Events [`emit`](Self::emit) handed to the observer, counted before
    /// each call.
    emitted: Arc<counter::AtomicU64>,
}

impl fmt::Debug for ObserverSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObserverSlot")
            .field("installed", &self.observer.is_some())
            .field("emitted", &self.emitted())
            .finish()
    }
}

impl ObserverSlot {
    /// A slot that delivers to `observer`, with nothing emitted yet.
    pub fn new(observer: Arc<dyn HeaderSyncObserver>) -> Self {
        Self::counting(observer, Arc::default())
    }

    /// A slot that delivers to `observer` and counts into `emitted`, which
    /// the caller keeps, so it can read the count after the reactor and its
    /// observer are gone.
    pub fn counting(
        observer: Arc<dyn HeaderSyncObserver>,
        emitted: Arc<counter::AtomicU64>,
    ) -> Self {
        ObserverSlot {
            observer: Some(observer),
            emitted,
        }
    }

    /// A slot that delivers to what `wrap` makes of this slot's observer and
    /// shares this slot's count, so a reader of the count also sees what the
    /// wrapper drops. A slot with no observer stays empty.
    pub fn wrapped(
        &self,
        wrap: impl FnOnce(Arc<dyn HeaderSyncObserver>) -> Arc<dyn HeaderSyncObserver>,
    ) -> Self {
        ObserverSlot {
            observer: self.observer.clone().map(wrap),
            emitted: self.emitted.clone(),
        }
    }

    /// Whether an observer is installed.
    pub fn is_installed(&self) -> bool {
        self.observer.is_some()
    }

    /// The events emitted so far, through any clone.
    pub fn emitted(&self) -> u64 {
        self.emitted.load(counter::Ordering::Relaxed)
    }

    /// Delivers `event` to the observer, if one is installed, counting it
    /// first. The count is a plain std atomic, so this adds no scheduling
    /// point. Every reactor event goes through here (rule 7 of the M6a
    /// observation contract).
    pub fn emit(&self, event: ReactorEvent) {
        if let Some(observer) = &self.observer {
            self.emitted.fetch_add(1, counter::Ordering::Relaxed);
            observer.observe(event);
        }
    }
}

/// One reactor event.
///
/// Every `Dispatched { call }` is followed by exactly one `CallEnded { call }`
/// and then one `Handled { call }`, unless the reactor task unwinds or stops
/// with the call still pending. The events
/// strictly between `CallEnded { call }` and `Handled { call }` ran while the
/// reactor processed that completion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReactorEvent {
    /// A port future was pushed. `call` counts from 1 per reactor, once per
    /// physical storage call.
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
    /// An active request started.
    Started {
        /// The request.
        request: RequestRef,
        /// Why the request was started.
        purpose: HeaderTargetPurpose,
    },
    /// An active request was explicitly retired.
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
    /// The reactor is about to wait at an await whose future can't change
    /// its state, so `snapshot` holds until the next `Resumed`. Reported
    /// once at build with turn 0, before each of the loop's waits, and,
    /// with `last` set, after the loop retires its work on the way out.
    /// The final one is reported even when nothing changed. The build's
    /// own effects, such as a startup repair's `Dispatched`, come before
    /// its turn 0 `Settled`.
    Settled {
        /// The turn that ends here: 0 at build, then the turn of the
        /// `Resumed` it follows.
        turn: u64,
        /// Whether the loop has ended, so no `Resumed` follows.
        last: bool,
        /// The reactor's state.
        snapshot: Box<ReactorSnapshot>,
    },
    /// The reactor is being dropped: its loop returned, its task was
    /// cancelled, or it was dropped before it ran. Reported from its
    /// `Drop`, before any of its fields drop, so the only events after it
    /// are its pending calls' `CallEnded{end: Dropped}`. Neither is
    /// reported while the thread unwinds from a panic, since an observer
    /// that panicked once could panic again and abort the process.
    Dropping {
        /// Its turn when it was dropped.
        turn: u64,
    },
    /// The reactor resumed from a wait, or started. It's the first thing
    /// the turn reports, and the reactor's state is in transition until
    /// the next `Settled`.
    Resumed {
        /// The turn that starts here, one more than the last `Settled`'s.
        turn: u64,
        /// Where it resumed.
        arm: ResumeArm,
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
    /// The work queue reserved header capacity for `peer`'s work (M6a-4).
    /// Reported by the queue itself, so no reservation goes unplaced. When
    /// the peer's work is already active, a [`Bound`](Self::Bound) to its
    /// incarnation follows at once. Otherwise `Bound` comes when its
    /// request starts, or never, if the open fails.
    Placed {
        /// The reservation's ID.
        reservation: ResourceId,
        /// The peer whose work it's for.
        peer: ZakuraPeerId,
        /// The headers it reserves.
        count: usize,
        /// The work the queue held for `peer` then, if any.
        placement: Option<Placement>,
    },
    /// A placed reservation now belongs to a work incarnation, which spends
    /// it on its pages (M6a-4).
    Bound {
        /// The reservation's ID.
        reservation: ResourceId,
        /// The incarnation it belongs to.
        incarnation: IncarnationId,
    },
    /// A prepare or apply call captured clones of its peer's staged
    /// capacity leases, which its future holds until its completion or
    /// drop (M6a-4). Reported right after the call's `Dispatched`.
    LeasesCaptured {
        /// The call.
        call: CallId,
        /// The incarnation the peer's active work had at dispatch.
        incarnation: Option<IncarnationId>,
        /// The leases' IDs, in the queue's order.
        leases: Vec<ResourceId>,
    },
    /// The reactor moved an incarnation's staged headers into a prepare,
    /// as transfer `transfer` (M6a-4). Reported by [`TransferHandle::taken`],
    /// the transfer's only constructor.
    EntriesTaken {
        /// The incarnation the headers left.
        incarnation: IncarnationId,
        /// The transfer's number.
        transfer: TransferId,
        /// How many headers it took.
        count: usize,
    },
    /// A transfer's last handle dropped, ending it (M6a-4). Reported once
    /// per transfer, by its tag's `Drop`, from whichever task dropped it.
    TransferEnded {
        /// The transfer's number.
        transfer: TransferId,
        /// How it ended.
        disposition: TransferDisposition,
    },
}

impl ReactorEvent {
    /// A `Started` event for `active`.
    pub(crate) fn started(active: &ActiveHeaderRequest) -> Self {
        Self::Started {
            request: RequestRef::of(active),
            purpose: active.purpose.clone(),
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

/// Where the reactor resumed, for [`ReactorEvent::Resumed`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResumeArm {
    /// The first statement of the loop, before its first wait.
    Start,
    /// After the shutdown wait that follows a fatal storage operation.
    FatalWait,
    /// The loop's shutdown arm.
    Shutdown,
    /// A port call's completion.
    PortCompletion,
    /// A freed locator read permit.
    LocatorPermit,
    /// A released chunk of header capacity.
    CapacityReleased,
    /// The maintenance deadline.
    Maintenance,
    /// A lifecycle event, or its channel's end.
    Lifecycle,
    /// A peer event, or its channel's end.
    Event,
    /// The yield after a turn that handled an event.
    Yield,
    /// A new committed snapshot, or its watch's end.
    CommittedSnapshot,
    /// A new VCT repair status, or its watch's end.
    VctRepairs,
}

/// The reactor's state at a [`ReactorEvent::Settled`]: the fields it
/// schedules from, read on its own task. Every list is in peer order, so
/// equal states compare equal.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReactorSnapshot {
    /// The committed snapshot the reactor last adopted, if any. It can lag
    /// the watch, which the reactor reads only at its own points.
    pub adopted: Option<AdoptedSnapshot>,
    /// Each admitted peer's current session.
    pub sessions: Vec<SessionEntry>,
    /// Peers with a readmission cooldown, expired or not, until maintenance
    /// prunes it.
    pub cooldowns: Vec<ZakuraPeerId>,
    /// Peers with a continuation-locator read in flight.
    pub pending_locators: Vec<ZakuraPeerId>,
    /// Port calls dispatched and not yet handled, in call order.
    pub unhandled_calls: Vec<u64>,
    /// Each peer's queued work.
    pub work: Vec<WorkEntry>,
    /// The reservation and leases the queue holds for each peer, read from
    /// its own maps, apart from the peer's work (M6a-4).
    pub holdings: Vec<HoldingEntry>,
}

/// The header capacity the work queue holds for one peer (M6a-4).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HoldingEntry {
    /// The peer.
    pub peer: ZakuraPeerId,
    /// Its reservation for pages still to come, if any.
    pub reservation: Option<ResourceId>,
    /// The leases of its staged pages, in staging order.
    pub leases: Vec<LeaseEntry>,
}

/// One staged page's capacity lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseEntry {
    /// The lease's ID.
    pub lease: ResourceId,
    /// The headers it owns.
    pub count: usize,
}

/// The work the queue held for a peer when it placed a reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    /// The session that offered the work.
    pub session_id: u64,
    /// The header authority the work runs under.
    pub scope: HeaderWorkAuthority,
    /// The advertised target it pursues.
    pub target_tip_hash: block::Hash,
}

/// A work incarnation's number, from 1 per work queue (M6a-4). `PeerWorkQueue::start`
/// hands one out each time a request starts, so it names one round of work
/// apart from its subscription and request IDs, which a resumed
/// subscription reuses.
pub type IncarnationId = u64;

/// A reservation's or a lease's number, from 1 per chunk budget, taken
/// under the budget's lock (M6a-4).
pub type ResourceId = u64;

/// A transfer's number, from 1 per reactor (M6a-4). A transfer is the
/// headers a prepare took out of their incarnation.
pub type TransferId = u64;

/// How a transfer ended (M6a-4): what last decided its staged headers'
/// fate before its last handle dropped.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransferDisposition {
    /// No dispatch took the prepare: a repair returned before it, or the
    /// dispatch refused it.
    NotDispatched,
    /// Its port future was dropped unfinished, as the reactor's teardown
    /// drops its pending calls.
    Dropped,
    /// It was sent to the external test port, which dropped it.
    External,
    /// Its port future panicked, and the reactor caught it.
    Panicked,
    /// The completion found no matching active request.
    Stale,
    /// The completion's work lost its authority.
    NoAuthority,
    /// The prepared target names another owner or source.
    OwnerMismatch,
    /// The prepared repair target no longer matches its repair.
    RepairInvalid,
    /// The prepared target went to apply call `call`.
    HandedToApply {
        /// The apply call.
        call: CallId,
    },
    /// The apply's dispatch refused it.
    ApplyRefused,
    /// The preparation failed.
    PrepareFailed,
}

impl TransferDisposition {
    /// Every kind but `HandedToApply`, in code order, so a code is its
    /// index.
    const PLAIN: [Self; 10] = [
        Self::NotDispatched,
        Self::Dropped,
        Self::External,
        Self::Panicked,
        Self::Stale,
        Self::NoAuthority,
        Self::OwnerMismatch,
        Self::RepairInvalid,
        Self::ApplyRefused,
        Self::PrepareFailed,
    ];

    /// The disposition as one `u64`: a plain kind's index, or one past the
    /// last index with the apply call in the bits above the low byte.
    fn code(self) -> u64 {
        // Indexes below `PLAIN.len()`, 10, fit u64, and a run makes far
        // fewer than 2^56 calls, so the shift keeps the call whole.
        match self {
            Self::HandedToApply { call } => (call << 8) | Self::PLAIN.len() as u64,
            plain => Self::PLAIN
                .iter()
                .position(|kind| *kind == plain)
                .expect("every other kind is listed") as u64,
        }
    }

    /// The disposition [`code`](Self::code) encoded.
    fn of(code: u64) -> Self {
        // The low byte fits usize.
        match Self::PLAIN.get((code & 0xff) as usize) {
            Some(plain) => *plain,
            None => Self::HandedToApply { call: code >> 8 },
        }
    }
}

/// One transfer, shared by every clone of the prepare that carries it, so
/// its end is reported once (M6a-4). A cfg-only field of
/// `HeaderPortOperation::PrepareHeaderTarget`.
#[derive(Clone)]
pub struct TransferHandle(Arc<TransferTag>);

/// What a transfer's handles share. Its `Drop`, which runs when the last
/// handle drops, reports the transfer's end.
struct TransferTag {
    transfer: TransferId,
    /// The disposition's [`code`](TransferDisposition::code). A plain std
    /// atomic, so setting it adds no scheduling point, from any task.
    disposition: counter::AtomicU64,
    slot: ObserverSlot,
}

impl TransferHandle {
    /// A new transfer of `count` headers out of `incarnation`, numbered
    /// one past `last`, which it advances. Reports `EntriesTaken` through
    /// `slot`, which its end reports through too. It starts
    /// `NotDispatched`.
    pub(crate) fn taken(
        last: &mut TransferId,
        slot: &ObserverSlot,
        incarnation: IncarnationId,
        count: usize,
    ) -> Self {
        *last += 1;
        let transfer = *last;
        slot.emit(ReactorEvent::EntriesTaken {
            incarnation,
            transfer,
            count,
        });
        Self(Arc::new(TransferTag {
            transfer,
            disposition: counter::AtomicU64::new(TransferDisposition::NotDispatched.code()),
            slot: slot.clone(),
        }))
    }

    /// The transfer's number.
    pub(crate) fn id(&self) -> TransferId {
        self.0.transfer
    }

    /// Records `disposition` as how the transfer ends, unless a later call
    /// replaces it before its last handle drops.
    pub(crate) fn dispose(&self, disposition: TransferDisposition) {
        self.0
            .disposition
            .store(disposition.code(), counter::Ordering::Relaxed);
    }
}

impl fmt::Debug for TransferHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TransferHandle")
            .field(&self.0.transfer)
            .finish()
    }
}

impl Drop for TransferTag {
    /// Reports the transfer's end, unless the thread is unwinding (see
    /// [`ReactorEvent::Dropping`]).
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let code = self.disposition.load(counter::Ordering::Relaxed);
            self.slot.emit(ReactorEvent::TransferEnded {
                transfer: self.transfer,
                disposition: TransferDisposition::of(code),
            });
        }
    }
}

/// The committed snapshot fields the reactor schedules from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AdoptedSnapshot {
    /// The selected-header generation.
    pub header_generation: u64,
    /// The finalized anchor.
    pub finalized: Frontier,
    /// The selected header tip.
    pub header_best: Frontier,
}

/// One admitted peer's current session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionEntry {
    /// The peer.
    pub peer: ZakuraPeerId,
    /// Its session.
    pub session_id: u64,
    /// Which side opened the connection.
    pub direction: ServicePeerDirection,
}

/// One peer's queued work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkEntry {
    /// The peer whose session owns it.
    pub peer: ZakuraPeerId,
    /// The owning session.
    pub session_id: u64,
    /// The advertised target it pursues.
    pub target_tip_hash: block::Hash,
    /// Whether it waits for its locator or is active.
    pub phase: WorkPhase,
}

/// Where one peer's work stands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkPhase {
    /// Staged, waiting for its continuation locator.
    AwaitingLocator {
        /// The header authority it was staged under.
        scope: HeaderWorkAuthority,
    },
    /// An active request.
    Active {
        /// The request's work incarnation.
        incarnation: IncarnationId,
        /// The request's correlation ID.
        request_id: u64,
        /// The request's work owner, with its current authority, which a
        /// bounded prefix rebinds.
        owner: HeaderSyncWorkOwner,
        /// Why it was started.
        purpose: HeaderTargetPurpose,
        /// Its target-processing phase.
        phase: HeaderTargetPhase,
        /// How many staged headers it holds.
        held: usize,
        /// The cursor of its last staged page.
        cursor: Option<Cursor>,
    },
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
        /// The transfer that carries its staged headers (M6a-4).
        transfer: TransferId,
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
                transfer,
                ..
            } => Self::Prepare {
                peer: peer.clone(),
                source: *source,
                owner: *owner,
                transfer: transfer.id(),
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
    /// The future was dropped unfinished, as the reactor's teardown drops
    /// its pending calls. Reported right before the drop, so any storage
    /// receipt the drop leaves comes after it.
    Dropped,
}

/// Which local capacity refused an offer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WaitCause {
    /// The staging queue was full.
    StagingFull,
    /// Every locator read permit was taken.
    LocatorCapacity,
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
    /// The request's work incarnation.
    pub incarnation: IncarnationId,
    /// The request's work owner.
    pub owner: HeaderSyncWorkOwner,
    /// The advertised target the request pursues.
    pub target_tip_hash: block::Hash,
}

impl RequestRef {
    /// `active`'s identity.
    fn of(active: &ActiveHeaderRequest) -> Self {
        Self {
            peer: active.peer.clone(),
            session_id: active.owner.session_id(),
            incarnation: active.incarnation,
            request_id: active.request_id.get(),
            owner: active.owner,
            target_tip_hash: active.target.status.selected_tip_hash,
        }
    }
}

/// The observer slot and call number a port future reports its end through.
#[derive(Clone, Debug)]
pub(crate) struct PortTap {
    /// The call's number, or 0 for an action that calls no port.
    pub(crate) call: u64,
    slot: ObserverSlot,
    /// Set once the call's end is reported, through any clone, so a
    /// [`PendingCall`] dropped after it reports nothing.
    ended: Arc<counter::AtomicU64>,
}

impl PortTap {
    /// A tap for `call` that reports through `slot`.
    pub(crate) fn new(call: u64, slot: &ObserverSlot) -> Self {
        Self {
            call,
            slot: slot.clone(),
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
        self.ended.store(1, counter::Ordering::Relaxed);
        self.slot.emit(ReactorEvent::CallEnded {
            call: self.call,
            end,
        });
    }
}

/// A port call's number, from 1, as [`ReactorEvent::Dispatched`] hands it
/// out. In a simulator build, storage tags each operation a call's poll
/// submits with it (see [`as_call`]), so each storage receipt pairs with exactly the call that
/// submitted it. [`ReactorEvent::LeasesCaptured`] joins a prepare or apply
/// call to its work incarnation and the lease IDs its dispatch captures.
pub type CallId = u64;

/// Runs `poll` as call `call`'s poll, so in a simulator build
/// `polling_call` names `call` inside it, on this task only, as a storage
/// call submitted there reads it. The task's previous call is restored
/// after, on unwind too.
pub fn as_call<R>(call: CallId, poll: impl FnOnce() -> R) -> R {
    #[cfg(zakura_sim)]
    let _scope = call_scope::CallScope::enter(call);
    #[cfg(not(zakura_sim))]
    let _ = call;
    poll()
}

/// The call whose poll the current task is running, if any. See
/// [`as_call`].
#[cfg(zakura_sim)]
pub fn polling_call() -> Option<CallId> {
    call_scope::current()
}

/// Each task's polling call. Keyed by run and task, not just by thread,
/// because a poll can reach a scheduling point, and the run then switches to
/// another task on this thread before the poll returns.
#[cfg(zakura_sim)]
mod call_scope {
    use std::cell::RefCell;

    use zakura_rt::sim::{RunId, TaskId};

    use super::CallId;

    std::thread_local! {
        /// Each task's polling call, with `None` for code outside a run or
        /// a task.
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

/// A port call's whole future, guarded so that dropping it unfinished, as
/// the reactor's teardown drops its pending calls, reports
/// `CallEnded{end: Dropped}` through the call's tap. It reports first,
/// then drops the future: the storage call, whose drop may leave a
/// `CallerDropped` or `ReplyDropped` receipt, and everything else it
/// holds, permits and capacity leases included, whose release can reach
/// scheduling points. So for a dropped call alone, the end comes before
/// its receipt, one to one.
pub(crate) struct PendingCall<F> {
    future: Option<F>,
    tap: PortTap,
    /// Set once a poll returned or unwound, so the call's own end, or the
    /// panic arm's, reports it.
    done: bool,
}

impl<F> PendingCall<F> {
    /// Guards `future`, call `tap`'s future.
    pub(crate) fn new(future: F, tap: PortTap) -> Self {
        Self {
            future: Some(future),
            tap,
            done: false,
        }
    }
}

impl<F: std::future::Future + Unpin> std::future::Future for PendingCall<F> {
    type Output = F::Output;

    /// Polls the guarded future as its call's (see [`as_call`]).
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let call = self.tap.call;
        let future = self
            .future
            .as_mut()
            .expect("a pending call's future lives until its guard drops");
        let polled = std::panic::catch_unwind(AssertUnwindSafe(|| {
            as_call(call, || std::pin::Pin::new(future).poll(cx))
        }));
        match polled {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
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
    /// Reports the call dropped unless a poll finished or its end was
    /// already reported, or the thread is unwinding (see
    /// [`ReactorEvent::Dropping`]), then drops the future.
    fn drop(&mut self) {
        let reported = self.done || self.tap.ended.load(counter::Ordering::Relaxed) != 0;
        if self.tap.call != 0 && !reported && !std::thread::panicking() {
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
