//! Transport-facing Zakura service types.
//!
//! This package owns the base types between bounded QUIC stream handling and
//! protocol services.

mod clock;
mod frame;
mod guard;
mod io;
mod message_rule;
#[cfg(any(test, zakura_sim))]
mod observe;
mod pipe;
mod registry;
mod service;
mod session;
#[cfg(any(test, zakura_sim))]
mod sim_reader;

pub use clock::{Clock, RealClock};
pub use frame::{Frame, StreamPrelude, ZakuraTrace};
// `SessionGuard` and `ByteBudget` are imported through this re-export; `Admit`
// and `PeerMeters` are reached directly via the `guard` module path, so they are
// unused *here* until a later service imports them through `crate::zakura`.
#[cfg(any(test, zakura_sim))]
pub(crate) use guard::BudgetObserver;
#[allow(unused_imports)]
pub(crate) use guard::{Admit, ByteBudget, PeerMeters, SessionGuard};
#[cfg(any(test, zakura_sim))]
pub use guard::{BudgetChange, BudgetChangeKind};
pub use io::{framed_channel, FramedRecv, FramedSend};
#[allow(unused_imports)] // guarded producers are activated by the next chunks
pub(crate) use io::{
    worker_framed_channel, FrameGuard, FrameWriteClaim, FramedWorkerRecv, GuardedFrameSlot,
    GuardedReserveError, OrderedStreamFailure, OrderedStreamFailureCause, QueuedFrame,
    ResponseFrameSlot,
};
pub(crate) use message_rule::frame_filter::{FrameFilter, InboundReader};
#[cfg(test)]
pub(crate) use message_rule::frame_suite::check_frame_filter;
pub use message_rule::{
    frame_filter::FrameRejection, layout::LayoutError, Cadence, Credit, MessageRole, MessageRule,
    PayloadLen,
};
#[cfg(any(test, zakura_sim))]
pub(crate) use observe::{ConnTap, RegulationTap, StreamTap};
#[cfg(any(test, zakura_sim))]
pub use observe::{HeaderCheck, Transition, TransportEvent, TransportObserver};
#[cfg(feature = "internal-bench")]
pub use pipe::supervised_spawn_bench;
pub(crate) use pipe::{
    handle_pipe_exit, spawn_supervised_peer_task, spawn_supervised_peer_task_unboxed,
    spawn_supervised_pipe, CloseCause, Edge, Flow, Node, NodeKind, Pipe, PipeCx, PipeShape,
};
pub use registry::{RegistryError, ServiceRegistry, SessionLayout};
#[cfg(any(test, zakura_sim))]
pub use service::CloseCauseReader;
pub(crate) use service::ServiceStream;
pub use service::{
    BoxRunFuture, Peer, RequestResponseService, Service, SessionDemand, SessionFull,
    SessionOpening, SessionPolicy, SessionResources, Sink, SinkReject, Source, Stream, StreamMode,
    StreamQueueDepths, StreamWritePolicy,
};
pub use session::{OrderedSendError, PeerStreamSession};
#[cfg(any(test, zakura_sim))]
pub use sim_reader::{read_framed_channel, StreamReader};

/// Reports the event that `$event` builds to `$observer`, an
/// `Option<Arc<dyn TransportObserver>>`.
///
/// `$event` is evaluated only when an observer is installed. Outside test and
/// simulator builds the macro expands to nothing.
#[cfg(any(test, zakura_sim))]
macro_rules! transport_observe {
    ($observer:expr, $event:expr) => {
        if let Some(observer) = &$observer {
            observer.observe($event)
        }
    };
}

/// Reports the event that `$event` builds to `$observer`.
///
/// Outside test and simulator builds the macro expands to nothing.
#[cfg(not(any(test, zakura_sim)))]
macro_rules! transport_observe {
    ($observer:expr, $event:expr) => {};
}
pub(crate) use transport_observe;

/// Calls `$f` with `$extra` after its arguments in test and simulator builds,
/// where `$f` takes an observer argument, and without it otherwise.
#[cfg(any(test, zakura_sim))]
macro_rules! sim_arg {
    ($($f:ident).+ ($($arg:expr),* $(,)?); $extra:expr) => {
        $($f).+($($arg,)* $extra)
    };
    ($ty:ident :: $f:ident :: <$($g:ty),+> ($($arg:expr),* $(,)?); $extra:expr) => {
        $ty::$f::<$($g),+>($($arg,)* $extra)
    };
}

/// Calls `$f` with its arguments alone.
///
/// The `$extra` argument only exists in test and simulator builds.
#[cfg(not(any(test, zakura_sim)))]
macro_rules! sim_arg {
    ($($f:ident).+ ($($arg:expr),* $(,)?); $extra:expr) => {
        $($f).+($($arg),*)
    };
    ($ty:ident :: $f:ident :: <$($g:ty),+> ($($arg:expr),* $(,)?); $extra:expr) => {
        $ty::$f::<$($g),+>($($arg),*)
    };
}
pub(crate) use sim_arg;

/// Calls `$method` on `$value` in test and simulator builds, and leaves
/// `$value` alone otherwise.
#[cfg(any(test, zakura_sim))]
macro_rules! sim_chain {
    ($value:expr => $method:ident($($arg:expr),* $(,)?)) => {
        $value.$method($($arg),*)
    };
}

/// Leaves `$value` alone.
///
/// The method only exists in test and simulator builds.
#[cfg(not(any(test, zakura_sim)))]
macro_rules! sim_chain {
    ($value:expr => $method:ident($($arg:expr),* $(,)?)) => {
        $value
    };
}
pub(crate) use sim_chain;
