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
    /// The final one is reported even when nothing changed.
    Settled {
        /// The turn that ends here: 0 at build, then the turn of the
        /// `Resumed` it follows.
        turn: u64,
        /// Whether the loop has ended, so no `Resumed` follows.
        last: bool,
        /// The reactor's state.
        snapshot: Box<ReactorSnapshot>,
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
        /// The request's correlation ID.
        request_id: u64,
        /// The request's work owner, with its authority.
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
                ..
            } => Self::Prepare {
                peer: peer.clone(),
                source: *source,
                owner: *owner,
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
}

impl PortTap {
    /// A tap for `call` that reports through `slot`.
    pub(crate) fn new(call: u64, slot: &ObserverSlot) -> Self {
        Self {
            call,
            slot: slot.clone(),
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
        self.slot.emit(ReactorEvent::CallEnded {
            call: self.call,
            end,
        });
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
