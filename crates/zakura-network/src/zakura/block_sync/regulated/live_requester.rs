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
use crate::zakura::CloseRow;
use crate::zakura::{
    block_sync::{
        config::MAX_BS_INFLIGHT_REQUESTS, work_queue::RequestWrite, BlockSyncStatus, MSG_BS_BLOCK,
        MSG_BS_GET_BLOCKS, MSG_BS_STATUS,
    },
    regulation::{
        ClaimRefused, Exchange, ExchangeWriter, PoolEntry, ReservationPool, ResponsePrecheck,
        StallClose, WriterFence,
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
    /// A body that answers a live exchange from another chain, such as a
    /// peer's answer after a fork (#2303). It is a local incompatibility, not
    /// a protocol fault: the fence has already reset the session, and the
    /// connection stays. A body with no live exchange fails the precheck as
    /// unsolicited instead.
    IncompatibleHeader,
}

pub(crate) struct LiveRequester {
    shared: Arc<Shared>,
    pub(crate) pool: ReservationPool,
    fence: WriterFence,
    /// Closes the connection after a repeated stall.
    stall: StallClose,
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
        stall: StallClose,
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
            stall,
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

    /// Body bytes the peer may still send under its live ranges, up to one
    /// protocol-maximum body.
    pub(crate) fn owed_body_bytes(&self) -> u64 {
        self.shared.lock().owed_body_bytes()
    }

    pub(crate) fn retirement_deadline(&self, grace: Duration) -> Option<Instant> {
        self.shared.lock().retirement_deadline(grace)
    }

    /// Whether an exchange's ending is overdue at `now`: an abandoned or
    /// fully received range waited `grace` of read-active time for it. A
    /// peer that withholds endings stalls even while its bodies arrive.
    pub(crate) fn endings_overdue(&self, now: Instant, grace: Duration) -> bool {
        self.shared
            .lock()
            .retirement_deadline(grace)
            .is_some_and(|deadline| now >= deadline)
    }

    /// Close the connection after its second stall.
    pub(crate) fn close_after_repeated_stall(&self) {
        self.stall.close();
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
        let rule = MessageRule::find(RULES, frame.message_type).ok_or_else(|| {
            SinkReject::protocol(CloseRow::BSYNC_CLOSE_2, "unknown GetBlocks message")
        })?;
        let envelope = if frame.flags != 0 {
            Some(CloseRow::BSYNC_CLOSE_3)
        } else if frame.payload.len() < rule.payload.min()
            || frame.payload.len() > rule.payload.max()
        {
            Some(CloseRow::BSYNC_CLOSE_4)
        } else if frame.payload.first().copied().map(u16::from) != Some(frame.message_type) {
            Some(CloseRow::BSYNC_CLOSE_5)
        } else {
            None
        };
        if let Some(row) = envelope {
            return Err(SinkReject::protocol(
                row,
                "invalid GetBlocks frame envelope",
            ));
        }
        if frame.message_type == u16::from(MSG_BS_BLOCK) {
            self.shared
                .lock()
                .precheck(frame.message_type, frame.payload.len())
                .map_err(|error| {
                    SinkReject::protocol(error.close_row(), error.frame_rejection())
                })?;
            let header = block::Header::zcash_deserialize_from_slice(&mut &frame.payload[1..])
                .map_err(|error| SinkReject::protocol(CloseRow::BSYNC_CLOSE_13, error))?;
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
                Err(error) => Err(SinkReject::protocol(error.close_row(), error)),
            };
        }
        Ok(
            match decode_control(&frame.payload).map_err(|error| {
                SinkReject::protocol(control_decode_row(frame.message_type), error)
            })? {
                Control::Status(status) => Authorization::Status(status),
                Control::GetBlocks(range) => Authorization::GetBlocks(range),
                Control::End(ending) => Authorization::Ended(
                    self.shared
                        .lock()
                        .finish(ending)
                        .map_err(|error| SinkReject::protocol(error.close_row(), error))?,
                ),
            },
        )
    }
}

/// The row a control frame of `message_type` breaks when it fails decode.
fn control_decode_row(message_type: u16) -> CloseRow {
    if message_type == u16::from(MSG_BS_STATUS) {
        CloseRow::BSYNC_CLOSE_6
    } else if message_type == u16::from(MSG_BS_GET_BLOCKS) {
        CloseRow::BSYNC_CLOSE_7
    } else {
        CloseRow::BSYNC_CLOSE_11
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

#[cfg(all(test, not(zakura_sim)))]
mod tests {
    use super::*;
    use crate::zakura::{framed_channel, regulation::PrecheckSlot, CloseCause};
    use tokio_util::sync::CancellationToken;

    /// The reader may queue the whole response it was asked for, and a frame
    /// past that is refused before its payload is read. The routine releases
    /// each frame it takes, so a conformant peer is never refused.
    #[test]
    fn the_transport_queue_holds_only_what_the_live_ranges_owe() {
        let pool = ReservationPool::new(2).unwrap();
        let connection = CancellationToken::new();
        let fence = WriterFence::new(connection.clone());
        let (session, _) = framed_channel(1);
        let (_, recv) = framed_channel(1);
        let slot = PrecheckSlot::default();
        let recv = recv.with_precheck(slot.clone());
        let stall = StallClose::new(connection.clone(), CloseCause::default());
        let requester = LiveRequester::new(pool.clone(), fence, stall, &recv, session);
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

    /// An envelope fault names its block-sync row: an undeclared type breaks
    /// `bsync-close-2`, and a set flag breaks `bsync-close-3`.
    #[test]
    fn envelope_faults_name_their_rows() {
        let (sender, _recv) = framed_channel(1);
        let requester = LiveRequester {
            shared: Arc::new(Shared {
                requester: Mutex::new(Requester::new(1)),
                _session: sender,
            }),
            pool: ReservationPool::new(1).unwrap(),
            fence: WriterFence::new(CancellationToken::new()),
            stall: StallClose::new(CancellationToken::new(), CloseCause::default()),
            transport_admits: false,
        };
        let row = |message_type: u16, flags: u16| match requester.authorize(&Frame {
            message_type,
            flags,
            payload: vec![MSG_BS_STATUS],
        }) {
            Err(SinkReject::Protocol(row, _)) => Some(row),
            _ => None,
        };
        assert_eq!(row(99, 0), Some(CloseRow::BSYNC_CLOSE_2));
        assert_eq!(
            row(u16::from(MSG_BS_STATUS), 1),
            Some(CloseRow::BSYNC_CLOSE_3)
        );
    }

    /// A peer fault needs certainty: bytes past what this session asked for, or a
    /// response with nothing owed. A body that exactly uses its reservation passes.
    #[test]
    fn only_bytes_past_the_request_identify_an_over_sender() {
        use zakura_chain::serialization::ZcashDeserializeInto;

        let frame_payload: Vec<u8> = std::iter::once(MSG_BS_BLOCK)
            .chain(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.iter().copied())
            .collect();
        let body_bytes = u32::try_from(frame_payload.len() - 1).unwrap();
        let block: block::Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let hash = block.hash();

        let authorize_with_reservation = |reserved: Option<u32>| {
            let pool = ReservationPool::new(1).unwrap();
            let fence = WriterFence::new(CancellationToken::new());
            let (sender, _recv) = framed_channel(1);
            let requester = LiveRequester {
                shared: Arc::new(Shared {
                    requester: Mutex::new(Requester::new(1)),
                    _session: sender,
                }),
                pool: pool.clone(),
                fence,
                stall: StallClose::new(CancellationToken::new(), CloseCause::default()),
                transport_admits: false,
            };
            if let Some(reserved) = reserved {
                let writer = requester
                    .reserve(
                        block::Height(1),
                        &[hash],
                        reserved,
                        pool.try_entry().unwrap(),
                        requester.open().unwrap(),
                    )
                    .unwrap();
                assert!(writer.publish(|| {}));
                assert!(writer.try_start(|| true));
            }
            requester.authorize(&Frame {
                message_type: u16::from(MSG_BS_BLOCK),
                flags: 0,
                payload: frame_payload.clone(),
            })
        };

        assert!(matches!(
            authorize_with_reservation(Some(body_bytes)),
            Ok(Authorization::Block(block::Height(1)))
        ));
        let row = |reserved| match authorize_with_reservation(reserved) {
            Err(SinkReject::Protocol(row, _)) => Some(row),
            _ => None,
        };
        assert_eq!(row(Some(body_bytes - 1)), Some(CloseRow::BSYNC_CLOSE_14));
        assert_eq!(row(None), Some(CloseRow::REG_CLOSE_6));
    }

    #[test]
    fn overdue_endings_stall_and_keep_entries_through_reader_cleanup() {
        let pool = ReservationPool::new(3).unwrap();
        let session = CancellationToken::new();
        let fence = WriterFence::new(session.clone());
        let (sender, _recv) = framed_channel(1);
        let requester = LiveRequester {
            shared: Arc::new(Shared {
                requester: Mutex::new(Requester::new(2)),
                _session: sender,
            }),
            pool: pool.clone(),
            fence,
            stall: StallClose::new(CancellationToken::new(), CloseCause::default()),
            transport_admits: false,
        };
        // The transport reader can retain the authorization after the routine exits.
        let reader = requester.shared.clone();
        for start in 1..=2 {
            assert!(!requester.endings_overdue(Instant::now(), Duration::from_secs(32)));
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
        assert!(!requester.endings_overdue(Instant::now(), Duration::from_secs(32)));
        assert!(requester.endings_overdue(
            Instant::now() + Duration::from_secs(33),
            Duration::from_secs(32)
        ));
        // The stalled session's teardown retires its fence, which resets it.
        assert!(!requester.fence.retire());
        assert!(session.is_cancelled(), "the session is reset");
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
