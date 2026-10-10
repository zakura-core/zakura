//! The async driver over `quinn-proto` (SPEC §6a).
//!
//! The driver follows the P2P stack's routine and reactor pattern:
//!
//! - The endpoint task ([`endpoint`]) is the reactor. It owns the
//!   `quinn_proto::Endpoint`, the socket's receive side, the connection table
//!   and admission. It routes each datagram to its connection over a bounded
//!   queue that drops when full.
//! - The connection task ([`connection`]) is the peer routine. It owns one
//!   `quinn_proto::Connection`, its timer and every stream's transport state,
//!   and sends its own datagrams.
//! - Handles ([`ConnRef`], the stream types) talk to the connection task in
//!   order over one command channel per connection. Each read or write is a
//!   request with a one-shot reply, so a stream has at most one operation in
//!   flight and the peer's flow control limits what the driver buffers.
//!
//! Nothing shares a `quinn-proto` object, so no lock guards one.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use quinn_proto::{ConnectionError, StreamId, VarInt};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    error::{ReadError, WriteError},
    key::NodeId,
};

pub(crate) mod connection;
pub(crate) mod endpoint;

/// Bytes one read request moves from quinn-proto to a stream handle.
pub(crate) const READ_BATCH_BYTES: usize = 256 * 1024;
/// Datagrams the endpoint queues for one connection before it drops more
/// (SPEC §6a). The peer retransmits anything dropped.
pub(crate) const CONNECTION_QUEUE_DATAGRAMS: usize = 1024;

/// A request from a handle to its connection task.
#[derive(Debug)]
pub(crate) enum ConnCmd {
    OpenBi {
        reply: oneshot::Sender<Result<StreamId, ConnectionError>>,
    },
    AcceptBi {
        reply: oneshot::Sender<Result<StreamId, ConnectionError>>,
    },
    Write {
        id: StreamId,
        data: Bytes,
        reply: oneshot::Sender<Result<(), WriteError>>,
    },
    Finish {
        id: StreamId,
    },
    Reset {
        id: StreamId,
        code: VarInt,
    },
    Read {
        id: StreamId,
        reply: oneshot::Sender<ReadReply>,
    },
    Stop {
        id: StreamId,
        code: VarInt,
    },
    /// A send handle dropped without finishing or resetting: finish it.
    DropSend {
        id: StreamId,
    },
    /// A receive handle dropped before the stream ended: stop it.
    DropRecv {
        id: StreamId,
    },
    Close {
        code: VarInt,
        reason: Bytes,
    },
    Stats {
        reply: oneshot::Sender<crate::conn::ConnStats>,
    },
}

/// Chunks in stream order, `None` once the peer finished the stream.
pub(crate) type ReadReply = Result<Option<Vec<Bytes>>, ReadError>;

/// State every handle of one connection shares with its task.
#[derive(Debug)]
pub(crate) struct ConnShared {
    /// Set once, when the connection closes.
    pub(crate) closed: watch::Sender<Option<ConnectionError>>,
    pub(crate) stable_id: usize,
}

impl ConnShared {
    fn new() -> Arc<Self> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        Arc::new(Self {
            closed: watch::Sender::new(None),
            stable_id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Records the close reason unless one is already set.
    pub(crate) fn set_closed(&self, reason: ConnectionError) {
        self.closed.send_if_modified(|closed| {
            if closed.is_some() {
                return false;
            }
            *closed = Some(reason);
            true
        });
    }

    /// The close reason, or `LocallyClosed` if the task ended without one.
    pub(crate) fn close_reason(&self) -> ConnectionError {
        self.closed
            .borrow()
            .clone()
            .unwrap_or(ConnectionError::LocallyClosed)
    }
}

/// A handle to a connection task.
///
/// Every clone, including the ones inside stream handles, keeps the
/// connection open. When the last one drops, the command channel closes and
/// the task closes the connection with code 0 (API-4).
#[derive(Clone, Debug)]
pub(crate) struct ConnRef {
    pub(crate) cmds: mpsc::UnboundedSender<ConnCmd>,
    pub(crate) shared: Arc<ConnShared>,
}

impl ConnRef {
    /// Sends a command, or returns the close reason if the task has ended.
    pub(crate) fn send(&self, cmd: ConnCmd) -> Result<(), ConnectionError> {
        self.cmds.send(cmd).map_err(|_| self.shared.close_reason())
    }
}

/// What a completed handshake proved (TLS-8).
#[derive(Clone, Debug)]
pub(crate) struct Handshake {
    pub(crate) remote_id: Option<NodeId>,
    pub(crate) alpn: Option<Vec<u8>>,
}

/// A connection whose handshake is in progress. Dropping it closes the
/// connection.
#[derive(Debug)]
pub(crate) struct Connecting {
    pub(crate) conn: ConnRef,
    pub(crate) handshake: oneshot::Receiver<Result<Handshake, ConnectionError>>,
    pub(crate) remote: SocketAddr,
}

impl Connecting {
    /// Waits for the handshake to finish.
    pub(crate) async fn established(self) -> Result<(ConnRef, Handshake), ConnectionError> {
        match self.handshake.await {
            Ok(Ok(handshake)) => Ok((self.conn, handshake)),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(self.conn.shared.close_reason()),
        }
    }
}
