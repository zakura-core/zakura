//! Synchronous header-sync reactor events for simulator tests.
//!
//! The reactor reports its port calls, active-request starts and
//! retirements, and refused offers to an installed [`HeaderSyncObserver`].
//! This module exists only in test and `zakura_sim` builds. Production builds
//! compile none of it, and the reactor's `observe!` macro drops its tokens.

use std::{any::Any, fmt, panic::AssertUnwindSafe, sync::Arc};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkOwner, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId};

use super::{
    events::HeaderPortOperation, ActiveHeaderRequest, HeaderSyncMisbehavior, HeaderTargetPurpose,
};
pub use super::{
    reactor::HeaderRequestTerminal,
    scheduler::peer_work::{HeaderTargetPhase, HeaderWorkEnd, StorageCallKind},
};
use crate::zakura::ZakuraPeerId;

mod ledger;

pub use ledger::HeaderWorkLedger;

/// Receives reactor events as they happen.
///
/// Called synchronously on the reactor task, never while the reactor holds a
/// lock. An implementation must not block, yield, take a `zakura_rt` lock, or
/// call into the reactor. A panic in `observe` unwinds the reactor task, even
/// when it is called inside a port future.
pub trait HeaderSyncObserver: Send + Sync + fmt::Debug {
    /// Records one event.
    fn observe(&self, event: ReactorEvent);
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
    /// The reactor dropped the future unfinished, as its teardown does.
    Dropped,
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
    fn of(active: &ActiveHeaderRequest) -> Self {
        Self {
            peer: active.peer.clone(),
            session_id: active.owner.session_id(),
            request_id: active.request_id.get(),
            incarnation: active.incarnation.get(),
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
}

impl PortTap {
    /// A tap for `call` that reports to `slot`'s observer, if one is installed.
    pub(crate) fn new(call: u64, slot: &ObserverSlot) -> Self {
        Self {
            call,
            observer: slot.0.clone(),
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
        if let Some(observer) = &self.observer {
            observer.observe(ReactorEvent::CallEnded {
                call: self.call,
                end,
            });
        }
    }
}

/// Reports the end of a port future that drops before it finishes.
///
/// The reactor keeps one beside each port future. A panicking thread reports
/// nothing, as [`HeaderSyncObserver`] says.
#[derive(Debug)]
pub(crate) struct PendingCall {
    tap: PortTap,
    done: bool,
}

impl PendingCall {
    /// A guard on the call `tap` reports for.
    pub(crate) fn new(tap: PortTap) -> Self {
        Self { tap, done: false }
    }

    /// The call's future finished, and reported its own end.
    pub(crate) fn finished(mut self) {
        self.done = true;
    }

    /// The reactor caught the call's future panicking.
    pub(crate) fn panicked(mut self) {
        self.done = true;
        self.tap.end_after_catch(CallEnd::PortPanicked);
    }
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        if !self.done && !std::thread::panicking() {
            self.tap.end_after_catch(CallEnd::Dropped);
        }
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
