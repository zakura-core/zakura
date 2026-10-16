//! Status cadence and the latest received advertisement live with the connection.

use super::wire::Message;
use crate::zakura::{
    block_sync::BlockSyncStatus,
    regulation::{CadenceSendError, CadenceSender},
    FramedSend, OrderedSendError,
};
use std::{
    sync::{Mutex, PoisonError},
    time::Instant,
};

#[derive(Debug)]
pub(crate) struct StatusSender {
    sender: Mutex<CadenceSender<Message>>,
    received: Mutex<Option<BlockSyncStatus>>,
    max_inflight_requests: u32,
}

impl StatusSender {
    /// A connection's sender, advertising at most `max_inflight_requests`; see
    /// [`super::session::serving_max_inflight_requests`].
    pub(crate) fn new(max_inflight_requests: u32) -> Self {
        Self {
            sender: Mutex::new(CadenceSender::new()),
            received: Mutex::new(None),
            max_inflight_requests,
        }
    }

    /// `status` as this connection sends it, with the serving in-flight limit applied.
    pub(crate) fn advertised(&self, status: BlockSyncStatus) -> BlockSyncStatus {
        BlockSyncStatus {
            max_inflight_requests: status.max_inflight_requests.min(self.max_inflight_requests),
            ..status
        }
    }

    pub(crate) fn record_received(&self, status: BlockSyncStatus) {
        *self.received.lock().unwrap_or_else(PoisonError::into_inner) = Some(status);
    }

    pub(crate) fn received(&self) -> Option<BlockSyncStatus> {
        *self.received.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn try_send(
        &self,
        status: BlockSyncStatus,
        send: &FramedSend,
    ) -> Result<(), OrderedSendError> {
        let mut sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        sender
            .update(Message::Status(self.advertised(status)))
            .map_err(|error| OrderedSendError::Encode(error.into()))?;
        match sender.send_due(send) {
            Ok(0) => Err(OrderedSendError::Full),
            Ok(_) => Ok(()),
            Err(CadenceSendError::Closed) => Err(OrderedSendError::Closed),
            Err(error) => Err(OrderedSendError::Encode(error.into())),
        }
    }

    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_due()
            .map(|instant| instant.into_std())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{block_sync::BlockSyncMessage, framed_channel};
    use std::{sync::Arc, time::Duration};
    use zakura_chain::block;

    #[tokio::test(start_paused = true)]
    async fn replacements_and_range_corrections_share_the_connection_cadence() {
        let sender = Arc::new(StatusSender::new(u32::MAX));
        let (old_send, mut old_recv) = framed_channel(4);
        let original = BlockSyncStatus {
            servable_high: block::Height(10),
            ..BlockSyncStatus::default()
        };
        sender.try_send(original, &old_send).unwrap();
        old_recv.recv().await.unwrap();
        let replacement = sender.clone();
        let (new_send, mut new_recv) = framed_channel(4);
        let correction = BlockSyncStatus {
            servable_high: block::Height(3),
            ..original
        };
        assert!(matches!(
            replacement.try_send(correction, &new_send),
            Err(OrderedSendError::Full)
        ));
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(matches!(
            replacement.try_send(correction, &new_send),
            Err(OrderedSendError::Full)
        ));
        assert!(new_recv.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        replacement.try_send(correction, &new_send).unwrap();
        assert_eq!(
            BlockSyncMessage::decode_frame(new_recv.recv().await.unwrap()).unwrap(),
            BlockSyncMessage::Status(correction)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_status_writer_retains_only_one_frame_and_the_latest_update() {
        let sender = StatusSender::new(u32::MAX);
        let (send, mut recv) = framed_channel(8);
        sender.try_send(BlockSyncStatus::default(), &send).unwrap();
        for height in 1..=50 {
            tokio::time::advance(Duration::from_secs(30)).await;
            assert!(matches!(
                sender.try_send(
                    BlockSyncStatus {
                        servable_high: block::Height(height),
                        ..BlockSyncStatus::default()
                    },
                    &send
                ),
                Err(OrderedSendError::Full)
            ));
        }
        assert_eq!(
            send.capacity(),
            7,
            "one unwritten frame survives repeated updates"
        );
        recv.recv().await.unwrap();
        let latest = BlockSyncStatus {
            servable_high: block::Height(50),
            ..BlockSyncStatus::default()
        };
        sender.try_send(latest, &send).unwrap();
        assert_eq!(
            BlockSyncMessage::decode_frame(recv.recv().await.unwrap()).unwrap(),
            BlockSyncMessage::Status(latest)
        );
    }

    /// Regulated Status carries the serving limit without lowering smaller advertisements.
    #[tokio::test]
    async fn status_advertises_at_most_the_serving_limit() {
        let sender = StatusSender::new(2816);
        let (send, mut recv) = framed_channel(4);
        let status = BlockSyncStatus {
            max_inflight_requests: 32_000,
            ..BlockSyncStatus::default()
        };
        sender.try_send(status, &send).unwrap();
        assert_eq!(
            BlockSyncMessage::decode_frame(recv.recv().await.unwrap()).unwrap(),
            BlockSyncMessage::Status(BlockSyncStatus {
                max_inflight_requests: 2816,
                ..status
            })
        );
        let small = BlockSyncStatus {
            max_inflight_requests: 100,
            ..status
        };
        assert_eq!(sender.advertised(small), small);
    }
}
