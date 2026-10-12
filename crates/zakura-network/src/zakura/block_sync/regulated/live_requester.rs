//! Production request authorization, kept separate from the download scheduler.

use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use zakura_chain::{block, serialization::ZcashDeserialize};

use super::{
    requester::{Requester, ResponseError},
    wire::{Message, Range, RULES},
};
use crate::zakura::{
    block_sync::{config::MAX_BS_INFLIGHT_REQUESTS, work_queue::RequestWrite, BlockSyncMessage},
    regulation::{
        ClaimRefused, Exchange, ExchangeWriter, PoolEntry, ReservationPool, ResponsePrecheck,
        WriterFence,
    },
    transport::FrameWriteClaim,
    wire_codec::decode_frame,
    Frame, FrameRejection, FramedRecv, FramedSend, MessageRule, SinkReject,
};

/// Leave at least half the node pool for other sessions, including draining ones.
// The protocol ceiling fits usize on supported targets.
pub(crate) const SESSION_RESERVATIONS: usize = MAX_BS_INFLIGHT_REQUESTS as usize / 2;

/// The precheck may outlive the routine. Its sender retains session capacity
/// until the authorization maps and transport reader have both been dropped.
#[derive(Debug)]
struct Shared {
    requester: Mutex<Requester>,
    _session: FramedSend,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Requester> {
        self.requester
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl ResponsePrecheck for Shared {
    fn check(&self, tag: u16, len: usize) -> Result<(), FrameRejection> {
        if !(3..=5).contains(&tag) {
            return Ok(());
        }
        self.lock().precheck(tag, len).map_err(|error| match error {
            ClaimRefused::OverBytes { bytes } => FrameRejection::AboveReservation { bytes },
            _ => FrameRejection::Unsolicited,
        })
    }
}

pub(crate) enum Authorization {
    Control,
    Block(block::Height),
    IncompatibleHeader,
}

pub(crate) struct LiveRequester {
    shared: Arc<Shared>,
    pub(crate) pool: ReservationPool,
    fence: WriterFence,
}

/// One local wait in the session reader. These waits must not overlap.
pub(crate) struct ReadPause {
    shared: Arc<Shared>,
    started: tokio::time::Instant,
}

impl Drop for ReadPause {
    fn drop(&mut self) {
        self.shared.lock().credit_read_pause(self.started.elapsed());
    }
}

impl LiveRequester {
    /// Exclude one local read wait from ending deadlines. Guards must not overlap.
    pub(crate) fn pause_reading(&self) -> ReadPause {
        ReadPause {
            shared: self.shared.clone(),
            started: tokio::time::Instant::now(),
        }
    }

    pub(crate) fn new(
        pool: ReservationPool,
        fence: WriterFence,
        recv: &FramedRecv,
        send: FramedSend,
    ) -> Self {
        let shared = Arc::new(Shared {
            requester: Mutex::new(Requester::new(SESSION_RESERVATIONS)),
            _session: send,
        });
        // In-process channels have no transport precheck. The routine applies
        // the same authorization before decoding their frames.
        recv.attach_precheck(shared.clone());
        Self {
            shared,
            pool,
            fence,
        }
    }

    /// Snapshot every unclaimed body in all live exchanges, including later positions.
    pub(crate) fn unreceived_expected(&self) -> Vec<(block::Height, block::Hash)> {
        self.shared.lock().unreceived_expected()
    }

    pub(crate) fn len(&self) -> usize {
        self.shared.lock().len()
    }

    pub(crate) fn at_capacity(&self) -> bool {
        self.shared.lock().at_capacity()
    }

    pub(crate) fn retirement_deadline(&self, grace: Duration) -> Option<Instant> {
        self.shared.lock().retirement_deadline(grace)
    }

    /// End this connection locally before releasing any unanswered authorization.
    pub(crate) fn check_liveness(&self, now: Instant, grace: Duration) -> Result<(), SinkReject> {
        let requester = self.shared.lock();
        if requester.endings_at_capacity()
            || requester
                .retirement_deadline(grace)
                .is_some_and(|deadline| now >= deadline)
        {
            self.fence.retire();
            return Err(SinkReject::local("GetBlocks exchanges did not finish"));
        }
        Ok(())
    }

    pub(crate) fn blocks_retry(&self, height: block::Height) -> bool {
        self.shared.lock().blocks_retry(height)
    }

    pub(crate) fn open(&self) -> Option<Exchange> {
        self.fence.open()
    }

    pub(crate) fn reserve(
        &self,
        start: block::Height,
        expected: &[block::Hash],
        max_bytes: u32,
        entry: PoolEntry,
        exchange: Exchange,
    ) -> Result<ExchangeWriter, SinkReject> {
        let count = u32::try_from(expected.len()).map_err(SinkReject::local)?;
        let range = Range::new(start, count).map_err(SinkReject::local)?;
        self.shared
            .lock()
            .reserve(range, expected, max_bytes, entry, exchange)
            .map_err(SinkReject::local)
    }

    pub(crate) fn retire(&self, start: block::Height, unwritten: bool) {
        if unwritten {
            self.shared.lock().retract(start);
        } else {
            self.shared.lock().abandon(start);
        }
    }

    /// Authorize the exact header before decoding the block's transactions.
    /// Endings are small and must validate before their reservation is removed.
    pub(crate) fn authorize(&self, frame: &Frame) -> Result<Authorization, SinkReject> {
        let rule = MessageRule::find(RULES, frame.message_type)
            .ok_or_else(|| SinkReject::protocol("unknown GetBlocks message"))?;
        if frame.flags != 0
            || frame.payload.len() < rule.payload.min()
            || frame.payload.len() > rule.payload.max()
            || frame.payload.first().copied().map(u16::from) != Some(frame.message_type)
        {
            return Err(SinkReject::protocol("invalid GetBlocks frame envelope"));
        }
        if frame.message_type == 3 {
            self.shared
                .check(frame.message_type, frame.payload.len())
                .map_err(SinkReject::protocol)?;
            let header = block::Header::zcash_deserialize(&frame.payload[1..])
                .map_err(SinkReject::protocol)?;
            let result = self
                .shared
                .lock()
                .claim_block(header.hash(), frame.payload.len());
            return match result {
                Ok((height, _)) => Ok(Authorization::Block(height)),
                Err(ResponseError::Identity) => {
                    self.fence.retire();
                    Ok(Authorization::IncompatibleHeader)
                }
                Err(error) => Err(SinkReject::protocol(error)),
            };
        }
        let message: Message = decode_frame(frame).map_err(SinkReject::protocol)?;
        if matches!(frame.message_type, 4 | 5) {
            self.shared
                .lock()
                .finish(&message)
                .map_err(SinkReject::protocol)?;
        }
        Ok(Authorization::Control)
    }

    pub(crate) fn ending_start(message: &BlockSyncMessage) -> Option<block::Height> {
        match message {
            BlockSyncMessage::BlocksDone { start_height, .. }
            | BlockSyncMessage::RangeUnavailable { start_height, .. } => Some(*start_height),
            _ => None,
        }
    }
}

/// Fence first, then claim the scheduler's work. Both publication and the
/// first transport write use this lock order.
#[derive(Debug)]
pub(crate) struct FencedRequest {
    pub(crate) exchange: ExchangeWriter,
    pub(crate) work: Arc<RequestWrite>,
}

impl FrameWriteClaim for FencedRequest {
    fn try_start(&self) -> bool {
        self.exchange.try_start(|| self.work.try_start())
    }
    fn written(&self) {
        self.work.written();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{framed_channel, regulation::UNFINISHED_EXCHANGE, CloseCause};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn missing_endings_close_without_peer_fault_and_keep_entries_through_reader_cleanup() {
        let pool = ReservationPool::new(3).unwrap();
        let connection = CancellationToken::new();
        let cause = CloseCause::default();
        let fence = WriterFence::new(connection.clone(), cause.clone());
        let (sender, _recv) = framed_channel(1);
        let requester = LiveRequester {
            shared: Arc::new(Shared {
                requester: Mutex::new(Requester::new(2)),
                _session: sender,
            }),
            pool: pool.clone(),
            fence,
        };
        // The transport reader can retain the authorization after the routine exits.
        let reader = requester.shared.clone();
        for start in 1..=2 {
            assert!(requester
                .check_liveness(Instant::now(), Duration::from_secs(32))
                .is_ok());
            let byte = u8::try_from(start).unwrap();
            let hash = block::Hash([byte; 32]);
            let writer = requester
                .reserve(
                    block::Height(start),
                    &[hash],
                    100,
                    pool.try_entry().unwrap(),
                    requester.open().unwrap(),
                )
                .unwrap();
            assert!(writer.publish(|| {}));
            assert!(writer.try_start(|| true));
            requester.shared.lock().claim_block(hash, 2).unwrap();
            requester.retire(block::Height(start), false);
        }
        assert!(matches!(
            requester.check_liveness(Instant::now(), Duration::from_secs(32)),
            Err(SinkReject::Local(_))
        ));
        assert!(connection.is_cancelled());
        assert_eq!(cause.get_or("unset"), UNFINISHED_EXCHANGE);
        assert_eq!(pool.held(), 2);
        assert!(
            pool.try_entry().is_some(),
            "another peer still has capacity"
        );
        drop(requester);
        assert_eq!(pool.held(), 2, "the reader still owns its reservations");
        drop(reader);
        assert_eq!(pool.held(), 0);
    }
}
