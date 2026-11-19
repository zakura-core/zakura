//! The stall rule every family shares.
//!
//! A stall is a session that held outstanding work past its liveness deadline
//! while the local side could make progress. The first stall on a connection
//! ends the session: its [`WriterFence`](super::WriterFence) resets the
//! session's streams, its reservations drop with it, and the family parks its
//! service on that connection for a cooldown. The connection and its other
//! services keep running. A second stall on the same connection closes the
//! connection through [`StallClose`].
//!
//! A slow or silent peer is not provably faulty, so the close records the
//! neutral cause [`STALL_POLICY`] and charges no fault.

use zakura_rt::sync::CancellationToken;

use crate::zakura::CloseCause;

/// The close cause of a connection closed after a repeated stall.
pub(crate) const STALL_POLICY: &str = "stall_policy";

/// Closes one connection after a repeated stall. Every family closes through
/// this type, so the stall-policy cause has one owner.
#[derive(Clone, Debug)]
pub(crate) struct StallClose {
    connection: CancellationToken,
    close_cause: CloseCause,
}

impl StallClose {
    /// A close of the connection that `connection` cancels, recording its
    /// cause in `close_cause`.
    pub(crate) fn new(connection: CancellationToken, close_cause: CloseCause) -> Self {
        Self {
            connection,
            close_cause,
        }
    }

    /// Close the connection under the stall policy. The first recorded cause
    /// wins, as for every close.
    pub(crate) fn close(&self) {
        self.close_cause.record(STALL_POLICY);
        self.connection.cancel();
    }
}
