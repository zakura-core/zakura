//! Production request authorization, kept separate from the download scheduler.

use std::{sync::Arc, time::Duration};
use zakura_chain::{block, serialization::ZcashDeserialize};
use zakura_rt::{
    sync::{Mutex, MutexGuard, PoisonError},
    time::{self, Instant},
};

use super::{
    requester::{CompletedRange, Requester, ResponseError},
    wire::{decode_control, Control, Range, RULES},
};
use crate::zakura::{
    block_sync::{
        config::MAX_BS_INFLIGHT_REQUESTS, work_queue::RequestWrite, BlockSyncStatus, MSG_BS_BLOCK,
    },
    regulation::{
        ClaimRefused, Exchange, ExchangeWriter, PoolEntry, ReservationPool, ResponsePrecheck,
        WriterFence,
    },
    transport::FrameWriteClaim,
    Frame, FrameRejection, FramedRecv, FramedSend, MessageRule, SinkReject,
};

/// Live GetBlocks exchanges one session may hold from the node pool: an eighth
/// of the pool, so the share follows the pool size.
///
/// Two sessions hold at most a quarter of the pool, so peers that grow large
/// windows and then stop answering cannot starve every other session. Normal
/// sync stays below it: a default Zakura server advertises 2,816 in-flight
/// requests (`serving_max_inflight_requests`), and the integrated header lead
/// keeps distinct missing heights within 4,000
/// (`INTEGRATED_HEADER_BODY_WINDOW_V1`). Exchanges abandoned after GC or a
/// reorg, or a larger custom advertisement, can briefly reach it, and the
/// session then waits for endings before it requests more.
// The protocol ceiling fits usize on supported targets.
pub(crate) const SESSION_RESERVATIONS: usize = MAX_BS_INFLIGHT_REQUESTS as usize / 8;

/// Fully received ranges one session may hold while their endings are owed.
///
/// A peer that sends every body but withholds the ending still looks
/// productive, so without this bound it could keep taking work until its
/// unended exchanges fill [`SESSION_RESERVATIONS`]. An honest server queues
/// each ending right after its body. Its ready-order output interleaves at most
/// one body per running read-and-encode job (`PEER_EXECUTION`, 12 per peer),
/// and one stream keeps frames in that order. So an honest session stays well
/// below this bound, while a withholding one holds at most this many pool
/// entries beyond the requests its window already has in flight.
pub(crate) const MAX_PENDING_ENDINGS: usize = 64;

/// One consistent read of a session's requester limits.
#[derive(Copy, Clone, Debug)]
pub(crate) struct RequesterOccupancy {
    /// Whether the session holds its whole [`SESSION_RESERVATIONS`] share.
    pub(crate) at_capacity: bool,
    /// Live exchanges, including abandoned ones still awaiting an ending.
    pub(crate) len: usize,
    /// Fully received ranges still owing an ending.
    pub(crate) pending_endings: usize,
}

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

/// The transport reader's gate. Every response it queues stays charged until
/// [`LiveRequester::authorize`] takes it, so the queue never holds more than
/// the live ranges owe.
impl ResponsePrecheck for Shared {
    fn check(&self, tag: u16, len: usize) -> Result<(), FrameRejection> {
        if !(3..=5).contains(&tag) {
            return Ok(());
        }
        self.lock()
            .admit(tag, len)
            .map_err(ClaimRefused::frame_rejection)
    }
}

/// A frame that passed its checks, carrying the values that its handler uses.
pub(crate) enum Authorization {
    Status(BlockSyncStatus),
    GetBlocks(Range),
    /// The claimed body's height. Its transactions are still undecoded.
    Block(block::Height),
    /// The ending already consumed its reservation.
    Ended(CompletedRange),
    /// A local incompatibility, not a protocol fault.
    IncompatibleHeader,
}

pub(crate) struct LiveRequester {
    shared: Arc<Shared>,
    pub(crate) pool: ReservationPool,
    fence: WriterFence,
    /// Whether a transport reader admits responses through [`Shared`].
    transport_admits: bool,
}

/// One local wait in the session reader. These waits must not overlap.
pub(crate) struct ReadPause {
    shared: Arc<Shared>,
    started: time::Instant,
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
            started: time::Instant::now(),
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
        let transport_admits = recv.attach_precheck(shared.clone());
        Self {
            shared,
            pool,
            fence,
            transport_admits,
        }
    }

    /// Snapshot every unclaimed body in all live exchanges, including later positions.
    pub(crate) fn unreceived_expected(&self, limit: usize) -> Vec<(block::Height, block::Hash)> {
        self.shared.lock().unreceived_expected(limit)
    }

    /// The share, live exchange count and owed endings, read under one lock.
    pub(crate) fn occupancy(&self) -> RequesterOccupancy {
        let requester = self.shared.lock();
        RequesterOccupancy {
            at_capacity: requester.at_capacity(),
            len: requester.len(),
            pending_endings: requester.pending_endings(),
        }
    }

    pub(crate) fn retirement_deadline(&self, grace: Duration) -> Option<Instant> {
        self.shared.lock().retirement_deadline(grace)
    }

    /// End this connection locally before releasing any unanswered authorization.
    pub(crate) fn check_liveness(&self, now: Instant, grace: Duration) -> Result<(), SinkReject> {
        let requester = self.shared.lock();
        if requester
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
    /// Control frames are decoded here, once.
    pub(crate) fn authorize(&self, frame: &Frame) -> Result<Authorization, SinkReject> {
        if self.transport_admits && (3..=5).contains(&frame.message_type) {
            self.shared
                .lock()
                .take_admitted(frame.message_type, frame.payload.len());
        }
        let rule = MessageRule::find(RULES, frame.message_type)
            .ok_or_else(|| SinkReject::protocol("unknown GetBlocks message"))?;
        if frame.flags != 0
            || frame.payload.len() < rule.payload.min()
            || frame.payload.len() > rule.payload.max()
            || frame.payload.first().copied().map(u16::from) != Some(frame.message_type)
        {
            return Err(SinkReject::protocol("invalid GetBlocks frame envelope"));
        }
        if frame.message_type == u16::from(MSG_BS_BLOCK) {
            self.shared
                .lock()
                .precheck(frame.message_type, frame.payload.len())
                .map_err(|error| SinkReject::protocol(error.frame_rejection()))?;
            let header = block::Header::zcash_deserialize_from_slice(&mut &frame.payload[1..])
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
        Ok(
            match decode_control(&frame.payload).map_err(SinkReject::protocol)? {
                Control::Status(status) => Authorization::Status(status),
                Control::GetBlocks(range) => Authorization::GetBlocks(range),
                Control::End(ending) => Authorization::Ended(
                    self.shared
                        .lock()
                        .finish(ending)
                        .map_err(SinkReject::protocol)?,
                ),
            },
        )
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
    use crate::zakura::{
        framed_channel,
        regulation::{PrecheckSlot, UNFINISHED_EXCHANGE},
        CloseCause,
    };
    use tokio_util::sync::CancellationToken;

    /// The reader may queue the whole response it was asked for, and a frame
    /// past that is refused before its payload is read. The routine releases
    /// each frame it takes, so a conformant peer is never refused.
    #[test]
    fn the_transport_queue_holds_only_what_the_live_ranges_owe() {
        let pool = ReservationPool::new(2).unwrap();
        let connection = CancellationToken::new();
        let fence = WriterFence::new(connection.clone(), CloseCause::default());
        let (session, _) = framed_channel(1);
        let (_, recv) = framed_channel(1);
        let slot = PrecheckSlot::default();
        let recv = recv.with_precheck(slot.clone());
        let requester = LiveRequester::new(pool.clone(), fence, &recv, session);
        let reader = slot.get().expect("the requester attaches its precheck");

        let hash = block::Hash([7; 32]);
        let writer = requester
            .reserve(
                block::Height(1),
                &[hash],
                100,
                pool.try_entry().unwrap(),
                requester.open().unwrap(),
            )
            .unwrap();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));

        // One body and one ending are owed, and the reader queues both.
        assert_eq!(reader.check(3, 50), Ok(()));
        assert_eq!(reader.check(4, 9), Ok(()));
        assert_eq!(
            reader.check(4, 9),
            Err(FrameRejection::AboveOwed {
                frames: 2,
                bytes: 110
            })
        );
        // Status is not a response, so it never charges the queue.
        assert_eq!(reader.check(1, 53), Ok(()));

        // The routine takes and claims the body, then the ending.
        requester.shared.lock().take_admitted(3, 50);
        requester.shared.lock().claim_block(hash, 50).unwrap();
        let mut ending = vec![4];
        ending.extend_from_slice(&1u32.to_le_bytes());
        ending.extend_from_slice(&1u32.to_le_bytes());
        let ending = Frame {
            message_type: 4,
            flags: 0,
            payload: ending,
        };
        assert!(matches!(
            requester.authorize(&ending),
            Ok(Authorization::Ended(_))
        ));
        assert_eq!(pool.held(), 0);
        assert_eq!(reader.check(4, 9), Err(FrameRejection::Unsolicited));
        assert!(!connection.is_cancelled());
    }

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
            transport_admits: false,
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
        // A full session waiting only for endings may still have them queued.
        assert!(requester
            .check_liveness(Instant::now(), Duration::from_secs(32))
            .is_ok());
        assert!(matches!(
            requester.check_liveness(
                Instant::now() + Duration::from_secs(33),
                Duration::from_secs(32)
            ),
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
