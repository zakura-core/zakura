//! One coalescing Status sender for the lifetime of a connection.

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
pub(crate) struct StatusSender(Mutex<CadenceSender<Message>>);

impl Default for StatusSender {
    fn default() -> Self {
        Self(Mutex::new(CadenceSender::new()))
    }
}

impl StatusSender {
    pub(crate) fn try_send(
        &self,
        status: BlockSyncStatus,
        send: &FramedSend,
    ) -> Result<(), OrderedSendError> {
        let mut sender = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        sender
            .update(Message::Status(status))
            .map_err(|error| OrderedSendError::Encode(error.into()))?;
        match sender.send_due(send) {
            Ok(0) => Err(OrderedSendError::Full),
            Ok(_) => Ok(()),
            Err(CadenceSendError::Closed) => Err(OrderedSendError::Closed),
            Err(error) => Err(OrderedSendError::Encode(error.into())),
        }
    }

    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.0
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
        let sender = Arc::new(StatusSender::default());
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
        let sender = StatusSender::default();
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
}
