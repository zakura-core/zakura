//! Production request authorization, kept separate from the download scheduler.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use zakura_chain::{block, serialization::ZcashDeserialize};

use super::{
    requester::Requester,
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

pub(crate) struct LiveRequester {
    shared: Arc<Shared>,
    pub(crate) pool: ReservationPool,
    fence: WriterFence,
}

impl LiveRequester {
    pub(crate) fn new(
        pool: ReservationPool,
        fence: WriterFence,
        recv: &FramedRecv,
        send: FramedSend,
    ) -> Self {
        let shared = Arc::new(Shared {
            // The protocol ceiling fits usize on supported targets.
            requester: Mutex::new(Requester::new(MAX_BS_INFLIGHT_REQUESTS as usize)),
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

    pub(crate) fn len(&self) -> usize {
        self.shared.lock().len()
    }

    pub(crate) fn contains_height(&self, height: block::Height) -> bool {
        self.shared.lock().contains_height(height)
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
    pub(crate) fn authorize(&self, frame: &Frame) -> Result<Option<block::Height>, SinkReject> {
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
            let (height, _) = self
                .shared
                .lock()
                .claim_block(header.hash(), frame.payload.len())
                .map_err(SinkReject::protocol)?;
            return Ok(Some(height));
        }
        let message: Message = decode_frame(frame).map_err(SinkReject::protocol)?;
        if matches!(frame.message_type, 4 | 5) {
            self.shared
                .lock()
                .finish(&message)
                .map_err(SinkReject::protocol)?;
        }
        Ok(None)
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
