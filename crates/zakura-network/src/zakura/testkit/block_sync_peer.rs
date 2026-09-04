//! In-memory stream-6 peers for block-sync harnesses.
//!
//! These peers attach through `BlockSyncService::add_peer`, so the node side still
//! runs the real per-peer routine, WorkQueue, byte budget, and Sequencer path.

use std::collections::HashMap;

use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use crate::zakura::{
    framed_channel, BlockSyncHandle, BlockSyncMessage, BlockSyncService, BlockSyncStatus,
    CloseCause, FramedRecv, FramedSend, Peer, Service, ServicePeerDirection, ServiceStream,
    ZakuraBlockSyncConfig, ZakuraConnId, ZakuraPeerId, ZAKURA_BLOCK_SYNC_STREAM_VERSION,
    ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC,
};

/// Result of waiting for one node-to-peer block-sync message.
#[derive(Debug)]
pub enum SyntheticBlockSyncReceive {
    /// A complete frame was received and decoded.
    Message(BlockSyncMessage),
    /// No frame arrived before the requested deadline.
    TimedOut,
    /// The node closed its outbound stream.
    Closed,
}

/// A connected synthetic block-sync peer backed by in-memory stream channels.
#[derive(Debug)]
pub struct SyntheticBlockSyncPeer {
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    direction: ServicePeerDirection,
    inbound: FramedSend,
    outbound: FramedRecv,
    cancel: CancellationToken,
}

impl SyntheticBlockSyncPeer {
    /// Synthetic peer identity.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Synthetic transport connection identity.
    pub fn conn_id(&self) -> ZakuraConnId {
        self.conn_id
    }

    /// Direction used for service admission.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Cancellation token for observing session ownership and teardown.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Queue a real stream-6 message as inbound peer traffic to the node.
    pub async fn send(&self, msg: BlockSyncMessage) -> Result<(), crate::BoxError> {
        let frame = msg.encode_frame()?;
        self.inbound.send(frame).await?;
        Ok(())
    }

    /// Queue a real stream-6 message without yielding to the node runtime.
    pub fn try_send(&self, msg: BlockSyncMessage) -> Result<(), crate::BoxError> {
        let frame = msg.encode_frame()?;
        self.inbound.try_send(frame)?;
        Ok(())
    }

    /// Wait until the real peer routine has handled all messages queued before
    /// this call and returned to its inbound receive loop.
    #[cfg(test)]
    pub(crate) async fn barrier_for_test(&self) -> Result<(), crate::BoxError> {
        self.inbound.barrier_for_test().await.map_err(Into::into)
    }

    /// Receive the next real stream-6 message sent by the node to this peer.
    pub async fn recv(&mut self) -> Result<Option<BlockSyncMessage>, crate::BoxError> {
        let Some(frame) = self.outbound.recv().await else {
            return Ok(None);
        };
        Ok(Some(BlockSyncMessage::decode_frame(frame)?))
    }

    /// Receive the next node-to-peer message, bounded by `duration`.
    pub async fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<SyntheticBlockSyncReceive, crate::BoxError> {
        match timeout(duration, self.outbound.recv()).await {
            Ok(Some(frame)) => Ok(SyntheticBlockSyncReceive::Message(
                BlockSyncMessage::decode_frame(frame)?,
            )),
            Ok(None) => Ok(SyntheticBlockSyncReceive::Closed),
            Err(_) => Ok(SyntheticBlockSyncReceive::TimedOut),
        }
    }

    /// Disconnect this synthetic peer.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// Owner for a `BlockSyncService` plus synthetic peers attached to it.
#[derive(Debug)]
pub struct SyntheticBlockSyncPeers {
    service: BlockSyncService,
    queue_depth: usize,
}

impl SyntheticBlockSyncPeers {
    /// Attach synthetic peers to an already-spawned block-sync reactor handle.
    pub fn new(config: ZakuraBlockSyncConfig, handle: BlockSyncHandle, queue_depth: usize) -> Self {
        Self {
            service: BlockSyncService::new_with_handle(config, handle),
            queue_depth: queue_depth.max(1),
        }
    }

    /// Add one outbound peer and send its initial `Status`.
    pub async fn add_peer(
        &self,
        peer_id: ZakuraPeerId,
        status: BlockSyncStatus,
    ) -> Result<SyntheticBlockSyncPeer, crate::BoxError> {
        let peer = self.connect_peer(peer_id, 0, ServicePeerDirection::Outbound)?;
        peer.send(BlockSyncMessage::Status(status)).await?;
        Ok(peer)
    }

    /// Attach a synthetic peer without sending its first `Status`.
    ///
    /// The explicit connection id and direction let lifecycle tests distinguish
    /// a current session from a stale predecessor.
    pub fn connect_peer(
        &self,
        peer_id: ZakuraPeerId,
        conn_id: ZakuraConnId,
        direction: ServicePeerDirection,
    ) -> Result<SyntheticBlockSyncPeer, crate::BoxError> {
        let (inbound_tx, inbound_rx) = framed_channel(self.queue_depth);
        let (outbound_tx, outbound_rx) = framed_channel(self.queue_depth);
        let connection_cancel = CancellationToken::new();
        let service_cancel = connection_cancel.child_token();
        let streams = HashMap::from([(
            ZAKURA_STREAM_BLOCK_SYNC,
            ServiceStream::new(
                0,
                ZAKURA_BLOCK_SYNC_STREAM_VERSION,
                inbound_rx,
                outbound_tx,
                service_cancel.clone(),
            ),
        )]);

        self.service.add_peer(Peer::new_with_service_cancel_token(
            conn_id,
            peer_id.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC,
            direction,
            streams,
            connection_cancel,
            service_cancel.clone(),
            CloseCause::default(),
        ));

        Ok(SyntheticBlockSyncPeer {
            peer_id,
            conn_id,
            direction,
            inbound: inbound_tx,
            outbound: outbound_rx,
            cancel: service_cancel,
        })
    }

    /// Remove exactly one synthetic connection from the service.
    pub fn remove_peer(&self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.service.remove_peer(peer_id, conn_id);
    }
}
