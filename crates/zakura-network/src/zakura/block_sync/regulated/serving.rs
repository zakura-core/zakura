//! Storage and body-byte accounting. Shared `Serve` owns scheduling and output.

use std::sync::Arc;

use zakura_chain::{block, diagnostic::task::CheckForPanics};

use super::wire::{Message, Range, RangeEnding};
use crate::zakura::{
    block_sync::{MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES},
    regulation::{
        Produce, Responded, ResponseCap, ResponseSink, ServeEnd, SinkProgress, WorkLease,
    },
    wire_codec::WireError,
};

use super::super::source::BlockRangeReadLease;
pub(super) use super::super::source::{
    BlockRangeRead as Read, BlockRangeReadResult as ReadResult, BlockRangeSource as Source,
};

#[derive(Debug)]
pub(super) struct Server<S: ?Sized> {
    source: Arc<S>,
    session: Option<crate::zakura::FramedSend>,
    max_blocks: u32,
    max_body_bytes: u32,
}

impl<S: ?Sized> Server<S> {
    pub(super) fn new(
        source: Arc<S>,
        max_blocks: u32,
        max_body_bytes: u32,
    ) -> Result<Self, WireError> {
        if !(1..=MAX_BS_BLOCKS_PER_REQUEST).contains(&max_blocks)
            || !(1..=MAX_BS_RESPONSE_BYTES).contains(&max_body_bytes)
        {
            return Err(WireError::OutOfRange("GetBlocks serving limits"));
        }
        Ok(Self {
            source,
            session: None,
            max_blocks,
            max_body_bytes,
        })
    }
}

impl<S: ?Sized> Server<S> {
    pub(super) fn with_session(mut self, send: crate::zakura::FramedSend) -> Self {
        self.session = Some(send);
        self
    }
}

impl<S: Source + ?Sized> Produce for Server<S> {
    type Request = Range;
    type Message = Message;

    fn response_cap(&self, range: &Range) -> ResponseCap {
        range
            .truncated(self.max_blocks)
            .response_cap(self.max_body_bytes)
    }

    async fn produce(
        &self,
        range: &Range,
        lease: WorkLease,
        sink: ResponseSink<Message>,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let read_range = range.truncated(self.max_blocks);
        let result = self
            .source
            .read(Read {
                start_height: read_range.start(),
                count: read_range.count(),
                max_body_bytes: self.max_body_bytes,
                lease: BlockRangeReadLease {
                    work: lease,
                    _session: self.session.clone(),
                },
            })
            .await
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        let requested = *range;
        let max_body_bytes = self.max_body_bytes;
        // Copying up to the response cap into frames can still take a while.
        // Await the actual job, even after session cancellation, and keep the
        // source's lease with its retained blocks.
        tokio::task::spawn_blocking(move || {
            encode_response(requested, read_range.count(), max_body_bytes, result, sink)
        })
        .await
        .check_for_panics_with(false)
        .map_err(|_| ServeEnd::Cancelled)?
    }

    fn local_failure(&self, range: &Range, sent: SinkProgress) -> Message {
        ending(*range, sent.frames)
    }
}

/// Send the source's serialized blocks unchanged as a contiguous prefix of
/// `requested`, within `max_blocks` and `max_body_bytes`, then end the response.
fn encode_response(
    requested: Range,
    max_blocks: u32,
    max_body_bytes: u32,
    result: ReadResult,
    mut sink: ResponseSink<Message>,
) -> Result<Responded, ServeEnd> {
    let ReadResult { blocks, lease } = result;
    let mut returned = 0;
    let mut body_bytes = 0_u64;
    // Consuming the blocks drops each one once it is framed, and any unsent
    // remainder when the loop ends, all before `lease` is released.
    for (height, bytes) in blocks {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        if returned == max_blocks || height.0 != requested.start().0 + returned {
            break;
        }
        // usize fits u64 on the supported 64-bit targets.
        let len = bytes.len() as u64;
        if len > block::MAX_BLOCK_BYTES {
            return Err(ServeEnd::LocalFault(
                "stored block exceeds the block byte limit".into(),
            ));
        }
        if len > u64::from(max_body_bytes).saturating_sub(body_bytes) {
            break;
        }
        sink.send(&Message::Block(bytes))
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        metrics::counter!("sync.block.body.served").increment(1);
        body_bytes += len;
        returned += 1;
    }
    if lease.is_cancelled() {
        return Err(ServeEnd::Cancelled);
    }
    sink.finish(&ending(requested, returned))
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))
}

fn ending(range: Range, returned: u32) -> Message {
    Message::End(if returned == 0 {
        RangeEnding::unavailable(range.start(), range.count())
    } else {
        RangeEnding::done(range.start(), returned)
    })
}
