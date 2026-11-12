//! Status cadence and the latest received advertisement live with the connection.

use zakura_rt::{
    sync::{Mutex, PoisonError},
    time::Instant,
};

use super::wire::{Message, RULES};
use crate::zakura::{
    block_sync::{state::RateMeter, BlockSyncStatus, MSG_BS_STATUS},
    regulation::{CadenceSendError, CadenceSender, RowSend},
    FramedSend, MessageRole, OrderedSendError,
};
use std::time::Duration;

/// The soonest a Status announcing a new tip follows the previous Status. A
/// peer applies at most one Status a second with one extra range change, so
/// a faster announcement would wait there instead.
const TIP_MIN_GAP: Duration = Duration::from_secs(1);

/// The Status row's bucket capacity, which every receiver observes.
const STATUS_BUCKET_CAPACITY: u32 = match RULES[0].role {
    MessageRole::Announcement { cadence } if RULES[0].message_type == 1 => cadence.capacity,
    _ => panic!("the first block-sync row is the Status announcement"),
};

/// Tokens of the receiver's Status bucket that a prompt tip announcement
/// leaves unspent: half, so a burst after a short outage still fits.
///
/// The sender charges its copy of the bucket when it queues a Status, and the
/// receiver charges its bucket when the Status arrives. While frames arrive
/// promptly the receiver's bucket never empties. A transport stall that buffers
/// Statuses delivers them together, and one longer than about three minutes of
/// tips every second can exceed the bucket; the receiver records that and does
/// not treat it as a violation.
const TIP_RESERVE_TOKENS: u32 = STATUS_BUCKET_CAPACITY / 2;

/// Whether `latest` announces blocks that `previous` did not: a higher
/// servable tip. A lower tip or a pruned low end is a correction and keeps
/// the Status interval.
///
/// A different tip block at the same height is not a new tip. Receivers do not
/// act on `tip_hash`, but every Status they apply still makes their reactor
/// recompute candidate state, so sending hash-only changes early would cost
/// peers work for nothing.
pub(in crate::zakura::block_sync) fn announces_new_tip(
    previous: Option<&BlockSyncStatus>,
    latest: &BlockSyncStatus,
) -> bool {
    previous.is_none_or(|previous| latest.servable_high > previous.servable_high)
}

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

/// How a connection paces the Status frames it receives. A stream-6
/// replacement resumes this pacing, so a reopened stream gets no fresh
/// Status allowance.
#[derive(Clone, Debug)]
pub(in crate::zakura::block_sync) struct InboundStatusPacing {
    /// Paces Status application, with one extra range change per window.
    pub(in crate::zakura::block_sync) apply_meter: RateMeter,
    /// Paces our Status replies.
    pub(in crate::zakura::block_sync) reply_meter: RateMeter,
    /// Whether the current window still allows one immediate range change.
    pub(in crate::zakura::block_sync) range_bypass_available: bool,
    /// The latest received Status that the apply meter deferred.
    pub(in crate::zakura::block_sync) pending: Option<BlockSyncStatus>,
}

/// The inbound Status state that outlives one stream-6 session.
#[derive(Clone, Debug)]
pub(in crate::zakura::block_sync) struct ReceivedStatus {
    /// The last Status that a routine applied. It was paced when it arrived,
    /// so a replacement routine applies it again without spending a meter.
    pub(in crate::zakura::block_sync) applied: BlockSyncStatus,
    pub(in crate::zakura::block_sync) pacing: InboundStatusPacing,
}

#[derive(Debug)]
pub(crate) struct StatusSender {
    sender: Mutex<OutboundStatus>,
    received: Mutex<StoredStatus>,
    max_inflight_requests: u32,
}

/// The outbound Status cadence and the tip of the last Status it queued.
#[derive(Debug)]
struct OutboundStatus {
    cadence: CadenceSender<Message>,
    /// The last Status queued, as given before the serving limit applied.
    queued: Option<BlockSyncStatus>,
}

/// The stored inbound Status state and the routine generation that owns it.
#[derive(Debug, Default)]
struct StoredStatus {
    /// The newest routine generation that read or wrote the state. A
    /// replacement cancels its predecessor without waiting for it, so the
    /// state refuses writes from an older generation.
    owner: u64,
    status: Option<ReceivedStatus>,
}

impl StatusSender {
    /// A connection's sender, advertising at most `max_inflight_requests`; see
    /// [`super::session::serving_max_inflight_requests`]. Updates are at least
    /// `interval` apart, or the row's cadence if that is longer.
    pub(crate) fn new(max_inflight_requests: u32, interval: Duration) -> Self {
        let cadence = CadenceSender::new()
            .with_min_interval(u16::from(MSG_BS_STATUS), interval)
            .expect("the Status row declares a cadence");
        Self {
            sender: Mutex::new(OutboundStatus {
                cadence,
                queued: None,
            }),
            received: Mutex::new(StoredStatus::default()),
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

    /// Store this connection's inbound Status pacing for a later stream-6
    /// session, with the Status just applied, if any. Pacing without an
    /// applied Status has nothing to resume, so it is not kept. A write from
    /// a `generation` older than the state's owner changes nothing, so a
    /// cancelled routine cannot restore pacing its replacement already spent.
    pub(in crate::zakura::block_sync) fn record_received(
        &self,
        generation: u64,
        applied: Option<BlockSyncStatus>,
        pacing: InboundStatusPacing,
    ) {
        let mut stored = self.received.lock().unwrap_or_else(PoisonError::into_inner);
        if generation < stored.owner {
            return;
        }
        stored.owner = generation;
        match (applied, stored.status.as_mut()) {
            (Some(applied), _) => stored.status = Some(ReceivedStatus { applied, pacing }),
            (None, Some(received)) => received.pacing = pacing,
            (None, None) => {}
        }
    }

    /// The inbound Status state that an earlier stream-6 session stored.
    /// Reading makes `generation` the owner, so later writes from older
    /// routines are refused. An older `generation` reads nothing.
    pub(in crate::zakura::block_sync) fn received(
        &self,
        generation: u64,
    ) -> Option<ReceivedStatus> {
        let mut stored = self.received.lock().unwrap_or_else(PoisonError::into_inner);
        if generation < stored.owner {
            return None;
        }
        stored.owner = generation;
        stored.status.clone()
    }

    /// Replace the pending Status and queue it if the cadence allows.
    ///
    /// A Status that [announces a new tip](announces_new_tip) over the last
    /// queued one goes out before the send interval ends, at least [`TIP_MIN_GAP`] after the previous Status,
    /// while the receiver's Status bucket keeps more than
    /// [`TIP_RESERVE_TOKENS`]. Other changes, such as pruning, keep the interval.
    pub(crate) fn try_send(
        &self,
        status: BlockSyncStatus,
        send: &FramedSend,
    ) -> Result<StatusSend, OrderedSendError> {
        let mut sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        let OutboundStatus { cadence, queued } = &mut *sender;
        cadence
            .update(Message::Status(self.advertised(status)))
            .map_err(|error| OrderedSendError::Encode(error.into()))?;
        let row = u16::from(MSG_BS_STATUS);
        let sent = if announces_new_tip(queued.as_ref(), &status) {
            cadence.send_row_early(row, send, TIP_MIN_GAP, TIP_RESERVE_TOKENS)
        } else {
            cadence.send_row(row, send)
        };
        if sent == Ok(RowSend::Queued) {
            *queued = Some(status);
        }
        match sent {
            Ok(RowSend::Queued) => Ok(StatusSend::Queued),
            Ok(RowSend::DeferredUntil(at)) => Ok(StatusSend::Deferred(at)),
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
        let sent_at = Instant::now();
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
            let sent_at = Instant::now();
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

    fn at(high: u32, hash: u8) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_high: block::Height(high),
            tip_hash: block::Hash([hash; 32]),
            ..BlockSyncStatus::default()
        }
    }

    /// A new tip goes out a second after the previous Status instead of waiting
    /// for the 30 s interval; corrections that announce no new blocks still wait.
    #[tokio::test(start_paused = true)]
    async fn a_new_tip_skips_the_interval_and_corrections_do_not() {
        let sender = StatusSender::new(u32::MAX, Duration::from_secs(30));
        let (send, mut recv) = framed_channel(8);
        let first_at = Instant::now();
        assert_eq!(
            sender.try_send(at(10, 1), &send).unwrap(),
            StatusSend::Queued
        );
        recv.recv().await.unwrap();

        // Too soon after the previous Status: the receiver would only defer it,
        // so it is due when the gap ends, not at the 30 s interval.
        tokio::time::advance(Duration::from_millis(500)).await;
        let due = StatusSend::Deferred(first_at + TIP_MIN_GAP);
        assert_eq!(sender.try_send(at(11, 2), &send).unwrap(), due);
        tokio::time::advance(Duration::from_millis(500)).await;
        let tip_at = Instant::now();
        assert_eq!(
            sender.try_send(at(11, 2), &send).unwrap(),
            StatusSend::Queued
        );
        assert_eq!(
            BlockSyncMessage::decode_frame(recv.recv().await.unwrap()).unwrap(),
            BlockSyncMessage::Status(at(11, 2))
        );

        // A different block at the same height, a lower tip and a pruned low
        // end announce no new height, so they wait for the interval.
        let due = StatusSend::Deferred(tip_at + Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(sender.try_send(at(11, 3), &send).unwrap(), due);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(sender.try_send(at(10, 1), &send).unwrap(), due);
        let pruned = BlockSyncStatus {
            servable_low: block::Height(5),
            ..at(11, 3)
        };
        assert_eq!(sender.try_send(pruned, &send).unwrap(), due);
        assert!(recv.try_recv().is_err());
    }

    /// Announcing a new tip every second spends at most half of the receiver's
    /// Status bucket, then follows its refill: while frames arrive promptly, a
    /// receiver never sees the bucket exhausted, and its reserve for bursts
    /// after an outage stays.
    #[tokio::test(start_paused = true)]
    async fn prompt_tip_announcements_never_exhaust_the_receivers_status_bucket() {
        use crate::zakura::regulation::{CadenceBuckets, CadenceCharge};

        let sender = StatusSender::new(u32::MAX, Duration::from_secs(30));
        let (send, mut recv) = framed_channel(8);
        let mut receiver = CadenceBuckets::new(crate::zakura::RealClock);
        let rule = &RULES[0];
        let started = Instant::now();
        let mut queued = Vec::new();
        for height in 1..=600 {
            if sender.try_send(at(height, 0), &send).unwrap() == StatusSend::Queued {
                recv.recv().await.unwrap();
                assert_eq!(receiver.charge(0, rule), CadenceCharge::Admit);
                queued.push(Instant::now().saturating_duration_since(started));
            }
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        // The first Status, then one per second while more than the reserve is left.
        let burst = usize::try_from(STATUS_BUCKET_CAPACITY - TIP_RESERVE_TOKENS).unwrap();
        assert!(queued[..burst]
            .windows(2)
            .all(|pair| pair[1] - pair[0] == TIP_MIN_GAP));
        assert!(queued[burst] - queued[burst - 1] > TIP_MIN_GAP);
        // Once the reserve is reached, a refused tip is due at the next refill,
        // not at the 30 s interval. The gap has passed on each attempt here.
        let mut deferred = None;
        for offset in 0..20 {
            tokio::time::advance(TIP_MIN_GAP).await;
            let now = Instant::now();
            match sender.try_send(at(10_000 + offset, 0), &send).unwrap() {
                StatusSend::Queued => {
                    recv.recv().await.unwrap();
                }
                StatusSend::Deferred(due) => {
                    deferred = Some((now, due));
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        let (refused_at, due) = deferred.expect("the bucket copy stops a burst at the reserve");
        assert!(due > refused_at && due <= refused_at + Duration::from_secs(15));
        // After the burst, one per refill interval (15 s), not per send interval (30 s).
        let after = queued.len() - burst;
        let refills = (600 - burst) / 15;
        assert!(
            after.abs_diff(refills) <= 2,
            "{after} announcements after the burst for {refills} refills"
        );
    }

    /// Statuses queued during a transport stall all arrive when it ends, so the
    /// receiver charges them together. The sender's copy of the bucket was
    /// charged as each was queued, so it cannot see the stall.
    ///
    /// With a tip every second, the burst fits the receiver's bucket up to a
    /// 165 s stall (11 early sends, then one per 15 s refill) and exceeds it
    /// after that. At the NU7 block spacing of 25 s, a ten-minute stall exceeds
    /// it by a few frames, which the 30 s cadence's sizing alone would have
    /// fit. Exhaustion only records a trace on the receiver.
    #[tokio::test(start_paused = true)]
    async fn statuses_buffered_by_a_stall_arrive_together() {
        use crate::zakura::regulation::{CadenceBuckets, CadenceCharge};

        async fn stalled(stall_secs: u32, tip_every_secs: u32) -> (usize, usize) {
            let sender = StatusSender::new(u32::MAX, Duration::from_secs(30));
            let (send, mut recv) = framed_channel(8);
            let mut buffered = 0;
            for second in 0..=stall_secs {
                if second % tip_every_secs == 0
                    && sender.try_send(at(second + 1, 0), &send).unwrap() == StatusSend::Queued
                {
                    // The transport accepts the write, but the frame waits there.
                    recv.recv().await.unwrap();
                    buffered += 1;
                }
                tokio::time::advance(Duration::from_secs(1)).await;
            }
            let mut receiver = CadenceBuckets::new(crate::zakura::RealClock);
            let exhausted = (0..buffered)
                .filter(|_| receiver.charge(0, &RULES[0]) == CadenceCharge::Exhausted)
                .count();
            (buffered, exhausted)
        }

        let capacity = usize::try_from(STATUS_BUCKET_CAPACITY).unwrap();
        assert_eq!(stalled(165, 1).await, (capacity, 0));
        let (buffered, exhausted) = stalled(180, 1).await;
        assert_eq!((buffered, exhausted), (capacity + 1, 1));
        let (buffered, exhausted) = stalled(600, 25).await;
        assert_eq!(exhausted, buffered - capacity);
        assert!(
            (1..=4).contains(&exhausted),
            "{buffered} Statuses buffered over ten minutes of 25 s blocks"
        );
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
