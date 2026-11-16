//! Per-connection compact block relay state, from the draft Compact Block Relay ZIP.
//!
//! Only connections that negotiated compact blocks (`early_block_relay`) use this state.

use std::sync::Arc;

use futures::Sink;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::Service;

use zakura_chain::{chain_tip::ChainTip, serialization::SerializationError};

use super::Connection;
use crate::{protocol::external::Message, BoxError, Request, Response};

/// The compact block encoding version that the draft Compact Block Relay ZIP defines.
const COMPACT_VERSION: u64 = 1;

/// The compact block relay state of one connection.
#[derive(Debug, Default)]
pub(super) struct CompactRelay {
    /// The peer's most recent `sendcmpct` asked for version 1 `cmpctblock` announcements.
    // Nothing sends `cmpctblock` yet, so only tests read this field.
    #[allow(dead_code)]
    pub(super) send: bool,

    /// This connection's high-bandwidth receive slot.
    ///
    /// The connection holds it from its `sendcmpct(1, 1)` write until its `sendcmpct(0, 1)`
    /// write completes, or the connection fails.
    pub(super) receive_permit: Option<OwnedSemaphorePermit>,
}

impl CompactRelay {
    /// Records the peer's `sendcmpct` message. Any version other than 1 turns announcements
    /// off. Neither case is misbehavior.
    pub(super) fn received_send_compact(&mut self, announce: bool, version: u64) {
        self.send = announce && version == COMPACT_VERSION;
    }
}

impl<S, Tx, C> Connection<S, Tx, C>
where
    S: Service<Request, Response = Response, Error = BoxError>,
    C: ChainTip,
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Takes a free high-bandwidth slot, if the peer set selected this connection and a slot
    /// is free.
    pub(super) fn try_take_receive_slot(&mut self) -> Option<OwnedSemaphorePermit> {
        self.compact_receive_rx
            .borrow_and_update()
            .as_ref()
            .and_then(|slots| Arc::clone(slots).try_acquire_owned().ok())
    }

    /// Writes `sendcmpct(announce = permit.is_some(), 1)`, then keeps `permit` as this
    /// connection's receive slot. A demotion therefore releases the old slot only after its
    /// write completes.
    ///
    /// A failed write releases `permit` and fails the connection.
    pub(super) async fn set_compact_receive(&mut self, permit: Option<OwnedSemaphorePermit>) {
        let result = self
            .peer_tx
            .send(Message::SendCompact {
                announce: permit.is_some(),
                version: COMPACT_VERSION,
            })
            .await;
        match result {
            Ok(()) => self.compact.receive_permit = permit,
            Err(error) => {
                drop(permit);
                self.fail_with(error).await;
            }
        }
    }
}

/// Waits for a high-bandwidth receive slot from `slots`, or forever if `slots` is `None`.
pub(super) async fn acquire_receive_slot(slots: Option<Arc<Semaphore>>) -> OwnedSemaphorePermit {
    match slots {
        Some(slots) => slots
            .acquire_owned()
            .await
            .expect("the inventory registry owns the receive slots and never closes them"),
        None => futures::future::pending().await,
    }
}
