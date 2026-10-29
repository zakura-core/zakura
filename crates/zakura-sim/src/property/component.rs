//! The components of the node's actual state a harness reads at each step
//! boundary (M6a), such as the chunk budget or a session table.

/// A component a harness's view reads (M6a, Components). A property that
/// needs one names it as [`Source::Component`](super::Source::Component).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ComponentKind {
    /// The header-sync reactor's settled snapshot.
    Reactor,
    /// The header chunk budget and its objects.
    Budget,
    /// The port calls that hold lease clones.
    CallHolders,
    /// Headers moved out of a work incarnation.
    Transfers,
    /// A session's header publisher.
    Publisher,
    /// The engine's committed snapshot.
    SelectedChain,
    /// Each stream's output lifetimes.
    Output,
    /// The supervisor's map of live connections.
    SupervisorMap,
    /// The header service's installed sessions.
    HeaderSessions,
    /// A connection loop's settled snapshot.
    Connection,
    /// A connection's session tasks and their members.
    SessionTasks,
    /// A connection's stream permits.
    StreamPermits,
    /// `SessionCapacity`'s slots.
    SessionSlots,
    /// The node's gauges.
    Gauges,
}
