//! In-memory stream-6 peers for block-sync harnesses.
//!
//! These peers attach through `BlockSyncService::add_peer`, so the node side still
//! runs the real per-peer routine, WorkQueue, byte budget, and Sequencer path.

use std::collections::HashMap;

use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use crate::zakura::{
    framed_channel, transport::ServiceStream, BlockSyncHandle, BlockSyncMessage, BlockSyncService,
    BlockSyncStatus, CloseCause, FramedRecv, FramedSend, Peer, Service, ServicePeerDirection,
    ZakuraBlockSyncConfig, ZakuraPeerId, ZAKURA_BLOCK_SYNC_STREAM_VERSION, ZAKURA_CAP_BLOCK_SYNC,
    ZAKURA_STREAM_BLOCK_REQUESTS, ZAKURA_STREAM_BLOCK_SYNC,
};

/// A connected synthetic block-sync peer backed by in-memory stream channels.
#[derive(Debug)]
pub struct SyntheticBlockSyncPeer {
    peer_id: ZakuraPeerId,
    inbound: FramedSend,
    requests: FramedSend,
    outbound: FramedRecv,
    outgoing_requests: FramedRecv,
    cancel: CancellationToken,
}

impl SyntheticBlockSyncPeer {
    /// Synthetic peer identity.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Queue a real stream-6 message as inbound peer traffic to the node.
    pub async fn send(&self, msg: BlockSyncMessage) -> Result<(), crate::BoxError> {
        let frame = msg.encode_frame()?;
        let sender = if matches!(msg, BlockSyncMessage::GetBlocks { .. }) {
            &self.requests
        } else {
            &self.inbound
        };
        sender.send(frame).await?;
        Ok(())
    }

    /// Receive the next real stream-6 message sent by the node to this peer.
    pub async fn recv(&mut self) -> Result<Option<BlockSyncMessage>, crate::BoxError> {
        let frame = tokio::select! {
            frame = self.outbound.recv() => frame,
            frame = self.outgoing_requests.recv() => frame,
        };
        let Some(frame) = frame else {
            return Ok(None);
        };
        Ok(Some(BlockSyncMessage::decode_frame(frame)?))
    }

    /// Receive the next node-to-peer message, bounded by `duration`.
    pub async fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Option<BlockSyncMessage>, crate::BoxError> {
        match timeout(duration, self.recv()).await {
            Ok(result) => result,
            Err(_) => Ok(None),
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
        let (inbound_tx, inbound_rx) = framed_channel(self.queue_depth);
        let (outbound_tx, outbound_rx) = framed_channel(self.queue_depth);
        // These channels also stand in for QUIC's receive buffering. Real QUIC
        // gates separately check the one-frame application request queues.
        let (requests, request_recv) = framed_channel(self.queue_depth);
        let (request_send, outgoing_requests) = framed_channel(self.queue_depth);
        let cancel = CancellationToken::new();
        let session_cancel = cancel.child_token();
        let streams = HashMap::from([
            (
                ZAKURA_STREAM_BLOCK_SYNC,
                ServiceStream::new(
                    0,
                    ZAKURA_BLOCK_SYNC_STREAM_VERSION,
                    inbound_rx,
                    outbound_tx,
                    session_cancel.clone(),
                ),
            ),
            (
                ZAKURA_STREAM_BLOCK_REQUESTS,
                ServiceStream::new(0, 1, request_recv, request_send, session_cancel),
            ),
        ]);

        self.service.add_peer(Peer::new_with_service_streams(
            0,
            peer_id.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC,
            ServicePeerDirection::Outbound,
            streams,
            cancel.clone(),
            CloseCause::new(),
            crate::zakura::regulation::ResponseMemory::default()
                .try_connection()
                .expect("default response budget funds a connection"),
        ));

        let peer = SyntheticBlockSyncPeer {
            peer_id,
            inbound: inbound_tx,
            requests,
            outbound: outbound_rx,
            outgoing_requests,
            cancel,
        };
        peer.send(BlockSyncMessage::Status(status)).await?;
        Ok(peer)
    }
}
