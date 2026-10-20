//! Status cadence and the latest received advertisement live with the connection.

use super::wire::Message;
use crate::zakura::{
    block_sync::{BlockSyncStatus, MSG_BS_STATUS},
    regulation::{CadenceSendError, CadenceSender, RowSend},
    FramedSend, OrderedSendError,
};
use std::{
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

/// What one attempt did with the latest local Status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum StatusSend {
    Queued,
    /// The connection's cadence allows the next Status at this instant.
    /// This is legal pacing, not queue pressure.
    Deferred(Instant),
    /// The previous Status frame has not finished its transport write.
    Unwritten,
    /// The stream's outbound queue has no free slot.
    QueueFull,
}

#[derive(Debug)]
pub(crate) struct StatusSender {
    sender: Mutex<CadenceSender<Message>>,
    received: Mutex<Option<BlockSyncStatus>>,
    max_inflight_requests: u32,
}

impl StatusSender {
    /// A connection's sender, advertising at most `max_inflight_requests`; see
    /// [`super::session::serving_max_inflight_requests`]. Updates are at least
    /// `interval` apart, or the row's cadence if that is longer.
    pub(crate) fn new(max_inflight_requests: u32, interval: Duration) -> Self {
        let sender = CadenceSender::new()
            .with_min_interval(u16::from(MSG_BS_STATUS), interval)
            .expect("the Status row declares a cadence");
        Self {
            sender: Mutex::new(sender),
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

    /// Replace the pending Status and queue it if the cadence allows.
    pub(crate) fn try_send(
        &self,
        status: BlockSyncStatus,
        send: &FramedSend,
    ) -> Result<StatusSend, OrderedSendError> {
        let mut sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        sender
            .update(Message::Status(self.advertised(status)))
            .map_err(|error| OrderedSendError::Encode(error.into()))?;
        match sender.send_row(u16::from(MSG_BS_STATUS), send) {
            Ok(RowSend::Queued) => Ok(StatusSend::Queued),
            Ok(RowSend::DeferredUntil(at)) => Ok(StatusSend::Deferred(at.into_std())),
            Ok(RowSend::Unwritten) => Ok(StatusSend::Unwritten),
            Ok(RowSend::QueueFull) => Ok(StatusSend::QueueFull),
            Ok(RowSend::Idle) => unreachable!("the Status row was updated above"),
            Err(CadenceSendError::Closed) => Err(OrderedSendError::Closed),
            Err(error) => Err(OrderedSendError::Encode(error.into())),
        }
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
        let sender = Arc::new(StatusSender::new(u32::MAX, Duration::ZERO));
        let (old_send, mut old_recv) = framed_channel(4);
        let original = BlockSyncStatus {
            servable_high: block::Height(10),
            ..BlockSyncStatus::default()
        };
        let sent_at = tokio::time::Instant::now().into_std();
        assert_eq!(
            sender.try_send(original, &old_send).unwrap(),
            StatusSend::Queued
        );
        old_recv.recv().await.unwrap();
        let replacement = sender.clone();
        let (new_send, mut new_recv) = framed_channel(4);
        let correction = BlockSyncStatus {
            servable_high: block::Height(3),
            ..original
        };
        // A cadence deferral is legal pacing, not queue pressure.
        let due = StatusSend::Deferred(sent_at + Duration::from_secs(30));
        assert_eq!(replacement.try_send(correction, &new_send).unwrap(), due);
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(replacement.try_send(correction, &new_send).unwrap(), due);
        assert!(new_recv.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            replacement.try_send(correction, &new_send).unwrap(),
            StatusSend::Queued
        );
        assert_eq!(
            BlockSyncMessage::decode_frame(new_recv.recv().await.unwrap()).unwrap(),
            BlockSyncMessage::Status(correction)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_status_writer_retains_only_one_frame_and_the_latest_update() {
        let sender = StatusSender::new(u32::MAX, Duration::ZERO);
        let (send, mut recv) = framed_channel(8);
        sender.try_send(BlockSyncStatus::default(), &send).unwrap();
        for height in 1..=50 {
            tokio::time::advance(Duration::from_secs(30)).await;
            assert_eq!(
                sender
                    .try_send(
                        BlockSyncStatus {
                            servable_high: block::Height(height),
                            ..BlockSyncStatus::default()
                        },
                        &send
                    )
                    .unwrap(),
                StatusSend::Unwritten
            );
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

    /// A local interval slower than the row's cadence paces Status, and a
    /// faster one cannot speed it up.
    #[tokio::test(start_paused = true)]
    async fn a_configured_interval_only_slows_the_status_cadence() {
        for (configured, interval) in [
            (Duration::from_secs(1), Duration::from_secs(30)),
            (Duration::from_secs(60), Duration::from_secs(60)),
        ] {
            let sender = StatusSender::new(u32::MAX, configured);
            let (send, mut recv) = framed_channel(4);
            let sent_at = tokio::time::Instant::now().into_std();
            let status = BlockSyncStatus::default();
            assert_eq!(sender.try_send(status, &send).unwrap(), StatusSend::Queued);
            recv.recv().await.unwrap();
            assert_eq!(
                sender.try_send(status, &send).unwrap(),
                StatusSend::Deferred(sent_at + interval)
            );
            tokio::time::advance(interval).await;
            assert_eq!(sender.try_send(status, &send).unwrap(), StatusSend::Queued);
        }
    }

    /// Regulated Status carries the serving limit without lowering smaller advertisements.
    #[tokio::test]
    async fn status_advertises_at_most_the_serving_limit() {
        let sender = StatusSender::new(2816, Duration::ZERO);
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
