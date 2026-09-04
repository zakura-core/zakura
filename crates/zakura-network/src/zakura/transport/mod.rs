//! Transport-facing Zakura service types.
//!
//! This package owns the base types between bounded QUIC stream handling and
//! protocol services.

mod clock;
mod frame;
mod guard;
mod io;
mod pipe;
mod registry;
mod service;
mod session;

pub use clock::{Clock, RealClock};
pub use frame::{Frame, StreamPrelude, ZakuraTrace};
// `SessionGuard` and `ByteBudget` are imported through this re-export; `Admit`
// is unused *here* until a later service imports it through `crate::zakura`.
#[allow(unused_imports)]
pub(crate) use crate::zakura::regulation::OutstandingByteBudget as ByteBudget;
pub(crate) use guard::{Admit, SessionGuard};
pub use io::{framed_channel, FramedRecv, FramedSend};
#[allow(unused_imports)] // leased errors are consumed by the GetBlocks policy follow-up
pub(crate) use io::{worker_framed_channel, FramedWorkerRecv, LeasedSendError, QueuedFrame};
pub(crate) use pipe::{
    handle_pipe_exit, spawn_supervised_peer_task, spawn_supervised_pipe, CloseCause, Edge, Flow,
    Node, NodeKind, Pipe, PipeCx, PipeShape,
};
pub use registry::{RegistryError, ServiceRegistry};
pub(crate) use service::ServiceStream;
pub use service::{
    BoxRunFuture, OrderedSessionDemand, OrderedStreamOpening, OrderedStreamPolicy, Peer,
    RequestResponseService, Service, Sink, SinkReject, Source, Stream, StreamMode,
};
pub use session::{OrderedSendError, PeerStreamSession};
